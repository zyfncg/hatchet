package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/hatchet-dev/hatchet/pkg/repository/sqlcv1"
)

// WorkerDeliveries is the delivery state of individual, non-durable task attempts.
type WorkerDeliveries interface {
	SaveWorkerDelivery(context.Context, uuid.UUID, uuid.UUID, *sqlcv1.V1Task, []byte) (uuid.UUID, error)
	PendingWorkerDeliveries(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) ([]WorkerDelivery, error)
	ClaimWorkerDelivery(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID) (WorkerClaim, error)
}

// WorkerClaim distinguishes the first successful claim from a confirmed retry.
type WorkerClaim struct {
	Accepted   bool
	First      bool
	TaskID     int64
	RetryCount int32
	Queue      string
}

type WorkerDelivery struct {
	ID     uuid.UUID
	Action []byte
}

func (w *workerRepository) SaveWorkerDelivery(ctx context.Context, tenant, worker uuid.UUID, task *sqlcv1.V1Task, action []byte) (uuid.UUID, error) {
	var id uuid.UUID
	err := w.pool.QueryRow(ctx, `
		INSERT INTO v1_worker_delivery AS d
			(task_id, task_inserted_at, retry_count, tenant_id, worker_id, action)
		SELECT task_id, task_inserted_at, retry_count, tenant_id, worker_id, $6
		FROM v1_task_runtime
		WHERE tenant_id = $1 AND worker_id = $2 AND task_id = $3
			AND task_inserted_at = $4 AND retry_count = $5 AND timeout_at > now()
		ON CONFLICT (task_id, task_inserted_at, retry_count) DO UPDATE
			SET delivery_id = d.delivery_id WHERE d.worker_id = excluded.worker_id
		RETURNING delivery_id`, tenant, worker, task.ID, task.InsertedAt, task.RetryCount, action).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, nil
	}
	return id, err
}

func (w *workerRepository) PendingWorkerDeliveries(ctx context.Context, tenant, worker, session uuid.UUID) ([]WorkerDelivery, error) {
	rows, err := w.pool.Query(ctx, `
		SELECT d.delivery_id, d.action FROM v1_worker_delivery d
		JOIN v1_task_runtime r USING (task_id, task_inserted_at, retry_count, tenant_id, worker_id)
		JOIN "Worker" w ON w.id = d.worker_id AND w."tenantId" = d.tenant_id
		WHERE d.tenant_id = $1 AND d.worker_id = $2 AND w."lastListenerSessionId" = $3
			AND w."isActive" AND d.receipt_id IS NULL AND r.timeout_at > now()
		ORDER BY d.task_id LIMIT 32`, tenant, worker, session)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[WorkerDelivery])
}

func (w *workerRepository) ClaimWorkerDelivery(ctx context.Context, tenant, worker, session, delivery, receipt uuid.UUID) (WorkerClaim, error) {
	var claim WorkerClaim
	err := w.pool.QueryRow(ctx, `
		WITH owned AS MATERIALIZED (
			SELECT d.delivery_id, d.receipt_id, d.task_id, d.retry_count, t.queue,
				w."isActive" AND w."lastListenerSessionId" = $3 AS current_session
			FROM v1_worker_delivery d
			JOIN v1_task_runtime r USING (task_id, task_inserted_at, retry_count, tenant_id, worker_id)
			JOIN v1_task t ON (t.id, t.inserted_at) = (d.task_id, d.task_inserted_at)
			JOIN "Worker" w ON w.id = d.worker_id AND w."tenantId" = d.tenant_id
			WHERE d.tenant_id = $1 AND d.worker_id = $2 AND d.delivery_id = $4
				AND r.timeout_at > now()
			FOR UPDATE OF d
		), claimed AS (
			UPDATE v1_worker_delivery d SET receipt_id = $5, action = NULL
			FROM owned o WHERE d.delivery_id = o.delivery_id
				AND o.receipt_id IS NULL AND o.current_session
			RETURNING d.delivery_id
		)
		SELECT EXISTS (SELECT FROM claimed) OR receipt_id IS NOT DISTINCT FROM $5,
			EXISTS (SELECT FROM claimed), task_id, retry_count, queue FROM owned`,
		tenant, worker, session, delivery, receipt).Scan(&claim.Accepted, &claim.First, &claim.TaskID, &claim.RetryCount, &claim.Queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkerClaim{}, nil
	}
	return claim, err
}
