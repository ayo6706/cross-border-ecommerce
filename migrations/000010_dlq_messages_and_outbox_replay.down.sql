ALTER TABLE outbox_events
    DROP CONSTRAINT IF EXISTS chk_outbox_replay_group,
    DROP COLUMN IF EXISTS target_group,
    DROP COLUMN IF EXISTS replay_of_event_id;

DROP TABLE IF EXISTS dlq_messages;
