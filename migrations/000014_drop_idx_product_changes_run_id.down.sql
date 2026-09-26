-- migrate:no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_product_changes_run_id ON product_changes (ingestion_run_id);
