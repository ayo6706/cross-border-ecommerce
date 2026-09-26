-- migrate:no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dlq_messages_lookup ON dlq_messages (stream, consumer_group, dead_lettered_at);
