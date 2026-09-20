-- name: InsertOutboxEvent :one
INSERT INTO outbox_events (
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    payload,
    status,
    retry_count,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
RETURNING 
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    payload,
    status,
    retry_count,
    created_at,
    processed_at;

-- name: FetchPendingOutboxEvents :many
SELECT 
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    payload,
    status,
    retry_count,
    created_at,
    processed_at
FROM outbox_events
WHERE status = 'PENDING'
ORDER BY created_at ASC
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: MarkOutboxEventProcessed :exec
UPDATE outbox_events
SET 
    status = 'PROCESSED',
    processed_at = $2
WHERE id = $1;

-- name: IncrementOutboxEventRetry :exec
UPDATE outbox_events
SET 
    retry_count = retry_count + 1,
    status = $2
WHERE id = $1;
