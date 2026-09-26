-- migrate:no-transaction
-- No query filters outbox_events by aggregate; the index only taxes every outbox insert (ADR 0010).
DROP INDEX CONCURRENTLY IF EXISTS idx_outbox_aggregate;
