-- migrate:no-transaction
-- No query lists dlq_messages by status (ADR 0010).
DROP INDEX CONCURRENTLY IF EXISTS idx_dlq_messages_status;
