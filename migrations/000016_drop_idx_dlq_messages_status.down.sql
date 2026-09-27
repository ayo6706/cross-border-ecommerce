-- migrate:no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dlq_messages_status ON dlq_messages (status, dead_lettered_at);
