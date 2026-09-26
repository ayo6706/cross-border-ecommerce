-- migrate:no-transaction
-- No query scans idempotency_keys by lease expiry; claims resolve the row by primary key (ADR 0010).
DROP INDEX CONCURRENTLY IF EXISTS idx_idempotency_keys_expires_at;
