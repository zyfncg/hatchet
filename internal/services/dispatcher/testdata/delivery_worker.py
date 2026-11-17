"""Run a real listener subprocess and the SDK runner on opposite ends of its queues."""

import asyncio
import multiprocessing
import sys

import grpc
from hatchet_sdk import Context, EmptyModel, Hatchet
from hatchet_sdk.clients.dispatcher.action_listener import ActionListener
from hatchet_sdk.clients.dispatcher.dispatcher import DispatcherClient
from hatchet_sdk.config import ClientConfig, ClientTLSConfig
from hatchet_sdk.utils.typing import STOP_LOOP
from hatchet_sdk.worker.action_listener_process import WorkerActionListenerProcess
from hatchet_sdk.worker.runner.run_loop_manager import WorkerActionRunLoopManager


def config_for(address):
    return ClientConfig.model_construct(
        host_port=address,
        server_url="http://" + address,
        tls_config=ClientTLSConfig(strategy="none"),
        token="test",
        disable_log_capture=True,
    )


def listen(
    address, worker, scenario, target, actions, events, stopping, fault, running
):
    async def serve():
        config = config_for(address)
        process = WorkerActionListenerProcess(
            name="delivery-test",
            actions=["test:execute"],
            slot_config={"default": 3},
            config=config,
            action_queue=actions,
            event_queue=events,
            labels=[],
            debug=False,
            worker_id_queue=None,
            stop_event=stopping,
            handle_kill=False,
        )
        process.dispatcher_client = DispatcherClient(config)
        client = ActionListener(config, worker)
        client.recover_deliveries = True
        process.listener = client
        connect = client.get_listen_client
        opens = 0

        async def subscribe():
            nonlocal opens
            stream = await connect()
            opens += 1
            read = stream.read

            async def receive():
                msg = await read()
                if msg is not grpc.aio.EOF and msg.task_run_external_id == target:
                    assert await asyncio.to_thread(running.wait, 5)
                if (
                    msg is not grpc.aio.EOF
                    and msg.task_run_external_id == target
                    and not fault.is_set()
                    and scenario in ("lost_start", "claim_outage", "heartbeat_replay")
                ):
                    assert running.is_set()
                    fault.set()
                    if scenario in ("lost_start", "claim_outage"):
                        stream.cancel()
                        return grpc.aio.EOF
                    return await read()
                return msg

            stream.read = receive
            return stream

        client.get_listen_client = subscribe
        if scenario == "queue_failure":
            put = actions.put

            def fail_first_enqueue(action):
                if action.step_run_id == target and not fault.is_set():
                    assert running.is_set()
                    fault.set()
                    raise OSError("injected IPC write failure")
                put(action)

            actions.put = fail_first_enqueue

        reader = asyncio.create_task(process.start_action_loop())
        await process.start_event_send_loop()
        client.stop_stream()
        await asyncio.wait_for(reader, 6)
        while process.step_action_events:
            await asyncio.gather(*process.step_action_events)
        if client.heartbeat_task:
            client.heartbeat_task.cancel()
        if scenario in ("lost_start", "claim_outage"):
            assert opens >= 2

    asyncio.run(serve())


async def main(address, worker, scenario, long_id, target, barrier):
    mp = multiprocessing.get_context("spawn")
    actions, events = mp.Queue(), mp.Queue()
    fault, running, stopping = mp.Event(), mp.Event(), mp.Event()
    child = mp.Process(
        target=listen,
        args=(
            address,
            worker,
            scenario,
            target,
            actions,
            events,
            stopping,
            fault,
            running,
        ),
    )
    child.start()
    config = config_for(address)
    finished = asyncio.Event()
    executed = []

    @Hatchet(config=config).task(name="execute")
    async def execute(input: EmptyModel, ctx: Context) -> None:
        executed.append(ctx.task_run_id)
        if ctx.task_run_id == long_id:
            running.set()
            await finished.wait()
        elif ctx.task_run_id == target:
            assert running.is_set() and not finished.is_set()
            await finished.wait()
        elif ctx.task_run_id == barrier:
            finished.set()

    manager = WorkerActionRunLoopManager(
        name="delivery-test",
        event_queue=events,
        action_queue=actions,
        config=config,
        slots=3,
        durable_slots=0,
        loop=asyncio.get_running_loop(),
        handle_kill=False,
        action_registry={"test:execute": execute._task},
        labels=[],
        lifespan_context=None,
        debug=False,
    )
    try:
        await asyncio.wait_for(finished.wait(), 80)
        actions.put(STOP_LOOP)
        await manager.start_loop_manager_task
        await asyncio.gather(*manager.runner.running_tasks)
        assert executed == [long_id, target, barrier], executed
        assert not any(manager.runner.used_slots_by_pool.values())
        if scenario in (
            "lost_start",
            "claim_outage",
            "heartbeat_replay",
            "queue_failure",
        ):
            assert fault.is_set()
        events.put(STOP_LOOP)
        await asyncio.to_thread(child.join, 10)
        assert child.exitcode == 0, child.exitcode
        print(scenario, "handler calls", len(executed), "including one long task")
    finally:
        if child.is_alive():
            child.kill()
            child.join()
        manager.log_sender.stop()
        actions.close()
        events.close()


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
