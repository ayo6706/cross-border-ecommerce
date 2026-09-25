CREATE TABLE dlq_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream VARCHAR(128) NOT NULL,
    consumer_group VARCHAR(128) NOT NULL,
    stream_message_id VARCHAR(64) NOT NULL,
    -- Same width as idempotency_keys.key: replay must reuse the exact event_id.
    event_id VARCHAR(256),
    event_type VARCHAR(64) NOT NULL DEFAULT '',
    aggregate_type VARCHAR(64) NOT NULL DEFAULT '',
    aggregate_id VARCHAR(128) NOT NULL DEFAULT '',
    correlation_id VARCHAR(128) NOT NULL DEFAULT '',
    payload BYTEA NOT NULL,
    failure_class VARCHAR(64) NOT NULL,
    last_error TEXT NOT NULL,
    stack TEXT,
    attempts INT NOT NULL CHECK (attempts >= 1),
    consumer_name VARCHAR(128) NOT NULL,
    event_created_at TIMESTAMPTZ,
    first_failed_at TIMESTAMPTZ NOT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(32) NOT NULL DEFAULT 'DEAD' CHECK (status IN ('DEAD', 'REPLAYED')),
    replayed_at TIMESTAMPTZ,
    replay_outbox_id UUID,
    -- FALSE when the envelope was corrupt or a replayed field had to be altered to fit.
    replayable BOOLEAN NOT NULL,
    CHECK (NOT replayable OR event_id IS NOT NULL),
    CONSTRAINT uq_dlq_messages_stream_group_msg UNIQUE (stream, consumer_group, stream_message_id),
    CHECK (
        (status = 'DEAD' AND replayed_at IS NULL AND replay_outbox_id IS NULL)
        OR
        (status = 'REPLAYED' AND replayed_at IS NOT NULL AND replay_outbox_id IS NOT NULL)
    )
);

CREATE INDEX idx_dlq_messages_status ON dlq_messages(status, dead_lettered_at);
CREATE INDEX idx_dlq_messages_lookup ON dlq_messages(stream, consumer_group, dead_lettered_at);

ALTER TABLE outbox_events
    ADD COLUMN replay_of_event_id VARCHAR(256),
    ADD COLUMN target_group VARCHAR(128),
    ADD CONSTRAINT chk_outbox_replay_group CHECK (
        (replay_of_event_id IS NULL AND target_group IS NULL)
        OR
        (replay_of_event_id IS NOT NULL AND target_group IS NOT NULL)
    );

