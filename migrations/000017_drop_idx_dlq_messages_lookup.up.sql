-- migrate:no-transaction
-- No query lists dlq_messages by stream and group; uq_dlq_messages_stream_group_msg covers that prefix (ADR 0010).
DROP INDEX CONCURRENTLY IF EXISTS idx_dlq_messages_lookup;
