-- migrate:no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_outbox_aggregate ON outbox_events (aggregate_type, aggregate_id);
