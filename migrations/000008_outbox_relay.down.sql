DROP INDEX IF EXISTS idx_outbox_claimable;

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (created_at) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_outbox_status_retry ON outbox_events (status, retry_count);

ALTER TABLE outbox_events
    DROP CONSTRAINT IF EXISTS chk_outbox_processed_at,
    DROP CONSTRAINT IF EXISTS chk_outbox_retry_count,
    DROP CONSTRAINT IF EXISTS chk_outbox_status,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS claim_token,
    DROP COLUMN IF EXISTS available_at;
