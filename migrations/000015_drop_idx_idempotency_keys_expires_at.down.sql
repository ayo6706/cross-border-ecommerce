-- migrate:no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_idempotency_keys_expires_at ON idempotency_keys (lease_expires_at) WHERE status = 'IN_PROGRESS';
