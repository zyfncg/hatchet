package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/hatchet/internal/msgqueue"
	"github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts"
	"github.com/hatchet-dev/hatchet/internal/services/dispatcher/contracts/contractsconnect"
	tasktypes "github.com/hatchet-dev/hatchet/internal/services/shared/tasktypes/v1"
	"github.com/hatchet-dev/hatchet/pkg/analytics"
	"github.com/hatchet-dev/hatchet/pkg/config/limits"
	"github.com/hatchet-dev/hatchet/pkg/repository"
	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// Exercise the real lifecycle RPCs and completion transaction with an in-process
// broker. Capture the events the dispatcher actually publishes to OLAP.
type deliveryMonitor struct {
	msgqueue.MessageQueue
	msgqueue.PubSub
	repo   repository.TaskRepository
	mu     sync.Mutex
	events map[int64]map[sqlcv1.V1EventTypeOlap]int
}

func (m *deliveryMonitor) IsReady() bool { return true }

func (m *deliveryMonitor) Pub(context.Context, msgqueue.Topic, *msgqueue.Message) error { return nil }

func (m *deliveryMonitor) SendMessage(ctx context.Context, _ msgqueue.Queue, msg *msgqueue.Message) error {
	for _, data := range msg.Payloads {
		switch msg.ID {
		case msgqueue.MsgIDTaskCompleted:
			var event tasktypes.CompletedTaskPayload
			if err := json.Unmarshal(data, &event); err != nil {
				return err
			}
			_, err := m.repo.CompleteTasks(ctx, msg.TenantID, []repository.CompleteTaskOpts{{
				TaskIdInsertedAtRetryCount: &repository.TaskIdInsertedAtRetryCount{Id: event.TaskId, InsertedAt: event.InsertedAt, RetryCount: event.RetryCount}, Output: event.Output,
			}})
			if err != nil {
				return err
			}
		case msgqueue.MsgIDCreateMonitoringEvent:
			var event tasktypes.CreateMonitoringEventPayload
			if err := json.Unmarshal(data, &event); err != nil {
				return err
			}
			m.mu.Lock()
			if m.events[event.TaskId] == nil {
				m.events[event.TaskId] = map[sqlcv1.V1EventTypeOlap]int{}
			}
			m.events[event.TaskId][event.EventType]++
			m.mu.Unlock()
		default:
			return fmt.Errorf("unexpected message %s", msg.ID)
		}
	}
	return nil
}

func (m *deliveryMonitor) count(id int64, event sqlcv1.V1EventTypeOlap) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events[id][event]
}

type deliveryTestServer struct {
	*DispatcherImpl
	pool *pgxpool.Pool

	scenario string
	target   int64
	mu       sync.Mutex

	firstReceipt string
	firstFailure time.Time
	denied       int
	heartbeats   []time.Time
}

func (s *deliveryTestServer) Heartbeat(ctx context.Context, req *contracts.HeartbeatRequest) (*contracts.HeartbeatResponse, error) {
	resp, err := s.DispatcherImpl.Heartbeat(ctx, req)
	if err == nil {
		s.mu.Lock()
		s.heartbeats = append(s.heartbeats, time.Now())
		s.mu.Unlock()
	}
	return resp, err
}

func (s *deliveryTestServer) ClaimDelivery(ctx context.Context, req *contracts.ClaimDeliveryRequest) (*contracts.ClaimDeliveryResponse, error) {
	var id int64
	err := s.pool.QueryRow(ctx, "SELECT task_id FROM v1_worker_delivery WHERE delivery_id=$1", req.DeliveryId).Scan(&id)
	if err != nil || id != s.target {
		return s.DispatcherImpl.ClaimDelivery(ctx, req)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstReceipt == "" {
		s.firstReceipt = req.ReceiptId
		s.firstFailure = time.Now()
	}
	firstTry := req.ReceiptId == s.firstReceipt
	outage := s.scenario == "claim_outage" && time.Since(s.firstFailure) < 32*time.Second
	unavailable := connect.NewError(connect.CodeUnavailable, fmt.Errorf("injected claim outage"))
	if firstTry && (outage || s.scenario == "lost_receipt" && s.denied == 0) {
		s.denied++
		return nil, unavailable
	}
	resp, err := s.DispatcherImpl.ClaimDelivery(ctx, req)
	if err == nil && firstTry && resp.Accepted && s.scenario == "lost_reply" && s.denied == 0 {
		s.denied++
		return nil, unavailable
	}
	return resp, err
}

// Seed the task and the assignment the scheduler commits before dispatch.
func seedDeliveryTask(t *testing.T, pool *pgxpool.Pool, tenant, worker uuid.UUID) *sqlcv1.V1Task {
	t.Helper()
	task := &sqlcv1.V1Task{
		ExternalID: uuid.New(), TenantID: tenant, WorkflowRunID: uuid.New(), StepID: uuid.New(),
		ActionID: "test:execute", StepReadableID: "execute", Queue: "test",
	}
	ctx := t.Context()
	err := pool.QueryRow(ctx, `
        INSERT INTO v1_task(tenant_id, queue, action_id, step_id, step_readable_id,
            workflow_id, workflow_version_id, workflow_run_id, schedule_timeout,
            sticky, external_id, display_name, input, step_index)
        VALUES ($1,'test','test:execute',$2,'execute',gen_random_uuid(),gen_random_uuid(),
            $3,'3m','NONE',$4,'execute','{}',0)
        RETURNING id, inserted_at`, tenant, task.StepID, task.WorkflowRunID, task.ExternalID).Scan(&task.ID, &task.InsertedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
        INSERT INTO v1_task_runtime(task_id,task_inserted_at,retry_count,tenant_id,worker_id,timeout_at)
        VALUES ($1,$2,0,$3,$4,now()+interval '3 minutes')`, task.ID, task.InsertedAt, tenant, worker)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
        INSERT INTO v1_task_runtime_slot(tenant_id,task_id,task_inserted_at,retry_count,worker_id,slot_type,units)
        VALUES ($1,$2,$3,0,$4,'default',1)`, tenant, task.ID, task.InsertedAt, worker)
	require.NoError(t, err)
	return task
}

// Run against a migrated disposable database and a Python interpreter with the SDK
// under test installed. The listener and runner communicate across process boundaries.
func TestRecoverWorkerStart(t *testing.T) {
	dsn, python := os.Getenv("HATCHET_TEST_DATABASE_URL"), os.Getenv("HATCHET_TEST_PYTHON")
	if dsn == "" || python == "" {
		t.Skip("set HATCHET_TEST_DATABASE_URL and HATCHET_TEST_PYTHON")
	}
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	log := zerolog.Nop()
	repo, cleanup := repository.NewRepository(pool, pool, &log, time.Minute, time.Hour, time.Hour, 3,
		repository.TaskOperationLimits{}, repository.PayloadStoreRepositoryOpts{}, repository.StatusUpdateBatchSizeLimits{}, limits.LimitConfigFile{}, false, repository.DurableEventBufferOpts{})
	defer cleanup()
	tenant, dispatcher := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO "Tenant"(id,name,slug) VALUES($1,'delivery-test',$2)`, tenant, tenant.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO "Dispatcher"(id,"lastHeartbeatAt") VALUES($1,now())`, dispatcher)
	require.NoError(t, err)
	engine := &DispatcherImpl{repov1: repo, workers: &workers{}, dispatcherId: dispatcher, l: &log, analytics: analytics.NoOpAnalytics{}, defaultMaxWorkerLockAcquisitionTime: time.Second}
	_, err = sqlcv1.New().CreatePartitions(ctx, pool, pgtype.Date{Time: time.Now().UTC(), Valid: true})
	require.NoError(t, err)
	monitor := &deliveryMonitor{repo: repo.Tasks(), events: map[int64]map[sqlcv1.V1EventTypeOlap]int{}}
	engine.mqv1 = monitor
	engine.pubsub = monitor
	engine.pubBuffer = msgqueue.NewMQPubBuffer(monitor)
	defer engine.pubBuffer.Stop()
	for _, scenario := range []string{"lost_start", "lost_reply", "lost_receipt", "heartbeat_replay", "queue_failure", "write_failure", "claim_outage"} {
		t.Run(scenario, func(t *testing.T) {
			worker := uuid.New()
			_, err := pool.Exec(ctx, `INSERT INTO "Worker"(id,"tenantId","dispatcherId",name,"lastHeartbeatAt") VALUES($1,$2,$3,'delivery-test',now())`, worker, tenant, dispatcher)
			require.NoError(t, err)
			api := &deliveryTestServer{DispatcherImpl: engine, pool: pool, scenario: scenario}
			long := seedDeliveryTask(t, pool, tenant, worker)
			target := seedDeliveryTask(t, pool, tenant, worker)
			barrier := seedDeliveryTask(t, pool, tenant, worker)
			api.target = target.ID
			_, handler := contractsconnect.NewDispatcherHandler(api)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), "tenant", &sqlcv1.Tenant{ID: tenant})))
			}))
			server.Config.Protocols = new(http.Protocols)
			server.Config.Protocols.SetUnencryptedHTTP2(true)
			server.Start()
			defer server.Close()
			childCtx, stopChild := context.WithTimeout(t.Context(), 90*time.Second)
			defer stopChild()
			child := exec.CommandContext(childCtx, python, "testdata/delivery_worker.py", strings.TrimPrefix(server.URL, "http://"), worker.String(), scenario, long.ExternalID.String(), target.ExternalID.String(), barrier.ExternalID.String())
			child.WaitDelay = 5 * time.Second
			var output strings.Builder
			child.Stdout = &output
			child.Stderr = &output
			require.NoError(t, child.Start())
			defer func() {
				if child.ProcessState == nil {
					_ = child.Process.Kill()
					_ = child.Wait()
				}
				t.Log(output.String())
			}()
			var active *subscribedWorker
			require.Eventually(t, func() bool {
				listeners, err := engine.workers.Get(worker)
				if err == nil && len(listeners) == 1 {
					active = listeners[0]
					return true
				}
				return false
			}, 10*time.Second, 10*time.Millisecond)
			send := func(task *sqlcv1.V1Task) {
				t.Helper()
				tasks := map[int64]*V1TaskWithPayloadAndInvocationCount{
					task.ID: {V1TaskWithPayload: &repository.V1TaskWithPayload{V1Task: task, Payload: []byte("{}")}},
				}
				err := engine.sendTasksToWorker(ctx, func(*sqlcv1.V1Task) { t.Error("unexpected requeue") }, tenant, worker, []int64{task.ID}, tasks)
				require.NoError(t, err)
			}
			started := func(task *sqlcv1.V1Task) bool {
				return monitor.count(task.ID, sqlcv1.V1EventTypeOlapSTARTED) > 0
			}
			send(long)
			require.Eventually(t, func() bool { return started(long) }, 10*time.Second, 20*time.Millisecond)
			if scenario == "write_failure" {
				require.True(t, active.sendLock.Acquire())
			}
			send(target)
			if scenario == "write_failure" {
				// Keep the write failing long enough for a false buffered SENT to be published.
				require.Never(t, func() bool {
					return monitor.count(target.ID, sqlcv1.V1EventTypeOlapSENTTOWORKER) > 0 || started(target)
				}, 200*time.Millisecond, 10*time.Millisecond)
				active.sendLock.Release()
			}
			require.Eventually(t, func() bool { return started(target) }, 50*time.Second, 25*time.Millisecond)
			var delivery, receipt, session uuid.UUID
			err = pool.QueryRow(ctx, `SELECT delivery_id,receipt_id,"lastListenerSessionId" FROM v1_worker_delivery JOIN "Worker" ON worker_id=id WHERE task_id=$1`, target.ID).Scan(&delivery, &receipt, &session)
			require.NoError(t, err)
			// Redeliver an already claimed attempt while both handlers are running.
			send(target)
			send(barrier)
			require.NoError(t, child.Wait())
			api.mu.Lock()
			if scenario == "claim_outage" {
				assert.Greater(t, api.denied, 1)
				assert.Greater(t, len(api.heartbeats), 2)
				for i := 1; i < len(api.heartbeats); i++ {
					assert.Less(t, api.heartbeats[i].Sub(api.heartbeats[i-1]), 30*time.Second)
				}
				if len(api.heartbeats) > 0 {
					assert.Greater(t, api.heartbeats[len(api.heartbeats)-1].Sub(api.firstFailure), 25*time.Second)
				}
			} else if scenario == "lost_reply" || scenario == "lost_receipt" {
				assert.Equal(t, 1, api.denied)
			}
			if api.denied > 0 {
				assert.Equal(t, receipt.String(), api.firstReceipt)
			}
			api.mu.Unlock()
			for _, task := range []*sqlcv1.V1Task{long, target, barrier} {
				require.Eventually(t, func() bool { return monitor.count(task.ID, sqlcv1.V1EventTypeOlapFINISHED) > 0 }, time.Second, 10*time.Millisecond)
				monitor.mu.Lock()
				assert.Equal(t, map[sqlcv1.V1EventTypeOlap]int{sqlcv1.V1EventTypeOlapSTARTED: 1, sqlcv1.V1EventTypeOlapFINISHED: 1, sqlcv1.V1EventTypeOlapSENTTOWORKER: 1}, monitor.events[task.ID])
				monitor.mu.Unlock()
				var completed int
				err = pool.QueryRow(ctx, `SELECT count(*) FROM v1_task_event WHERE task_id=$1 AND task_inserted_at=$2 AND event_type='COMPLETED'`, task.ID, task.InsertedAt).Scan(&completed)
				require.NoError(t, err)
				require.Equal(t, 1, completed)
			}
			var remaining int
			err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM v1_task_runtime_slot WHERE worker_id=$1)+(SELECT count(*) FROM v1_worker_delivery WHERE worker_id=$1)`, worker).Scan(&remaining)
			require.NoError(t, err)
			require.Zero(t, remaining)
			claim, err := repo.Workers().ClaimWorkerDelivery(ctx, tenant, worker, session, delivery, receipt)
			require.NoError(t, err)
			require.False(t, claim.Accepted)
		})
	}
}
