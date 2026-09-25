-- name: InsertDLQMessage :exec
INSERT INTO dlq_messages (
    stream,
    consumer_group,
    stream_message_id,
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    correlation_id,
    payload,
    failure_class,
    last_error,
    stack,
    attempts,
    consumer_name,
    event_created_at,
    first_failed_at,
    replayable
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
)
ON CONFLICT (stream, consumer_group, stream_message_id) DO NOTHING;

-- name: GetDLQReplaySource :one
SELECT
    consumer_group,
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    payload,
    replayable
FROM dlq_messages
WHERE id = $1;

-- name: MarkDLQMessageReplayed :execrows
UPDATE dlq_messages
SET status = 'REPLAYED',
    replayed_at = NOW(),
    replay_outbox_id = @replay_outbox_id::uuid
WHERE id = @id::uuid
  AND status = 'DEAD';
