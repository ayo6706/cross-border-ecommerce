ALTER TABLE outbox_events
    ADD COLUMN available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN claim_token UUID,
    ADD COLUMN last_error TEXT,
    ADD CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'PROCESSED', 'FAILED')),
    ADD CONSTRAINT chk_outbox_retry_count CHECK (retry_count >= 0),
    ADD CONSTRAINT chk_outbox_processed_at CHECK ((status = 'PROCESSED') = (processed_at IS NOT NULL));

UPDATE outbox_events SET available_at = created_at;

DROP INDEX IF EXISTS idx_outbox_pending;
DROP INDEX IF EXISTS idx_outbox_status_retry;

CREATE INDEX IF NOT EXISTS idx_outbox_claimable ON outbox_events (available_at) WHERE status = 'PENDING';
