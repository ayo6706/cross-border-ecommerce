-- name: CreateOutboxEvent :exec
INSERT INTO outbox_events (
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    payload
) VALUES (
    $1, $2, $3, $4, $5
);

-- name: CreateReplayOutboxEvent :exec
INSERT INTO outbox_events (
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    payload,
    replay_of_event_id,
    target_group
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
);

-- name: ClaimOutboxBatch :many
WITH candidate AS (
    SELECT id
    FROM outbox_events
    WHERE status = 'PENDING'
      AND available_at <= NOW()
    ORDER BY available_at, id
    LIMIT @batch_size::int
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox_events o
SET claim_token = @claim_token::uuid,
    available_at = NOW() + @lease_duration::interval
FROM candidate
WHERE o.id = candidate.id
RETURNING o.id, o.aggregate_type, o.aggregate_id, o.event_type, o.payload, o.retry_count, o.created_at,
          COALESCE(o.replay_of_event_id, o.id::text)::varchar AS event_id,
          COALESCE(o.target_group, '')::varchar AS target_group;

-- name: MarkOutboxPublished :execrows
UPDATE outbox_events
SET status = 'PROCESSED',
    processed_at = NOW(),
    claim_token = NULL,
    last_error = NULL
WHERE id = ANY(@ids::uuid[])
  AND claim_token = @claim_token::uuid
  AND status = 'PENDING';

-- name: RecordOutboxPublishFailure :execrows
UPDATE outbox_events
SET retry_count = retry_count + 1,
    last_error = @last_error,
    claim_token = NULL,
    available_at = NOW() + @backoff::interval,
    status = CASE
        WHEN retry_count + 1 >= @max_attempts::int THEN 'FAILED'
        ELSE 'PENDING'
    END
WHERE id = @id
  AND claim_token = @claim_token::uuid;

-- name: ReleaseOutboxClaims :execrows
UPDATE outbox_events
SET claim_token = NULL,
    available_at = NOW()
WHERE id = ANY(@ids::uuid[])
  AND claim_token = @claim_token::uuid;

-- name: CopyOutboxEvents :copyfrom
INSERT INTO outbox_events (
    id,
    aggregate_type,
    aggregate_id,
    event_type,
    payload
) VALUES (
    $1, $2, $3, $4, $5
);

