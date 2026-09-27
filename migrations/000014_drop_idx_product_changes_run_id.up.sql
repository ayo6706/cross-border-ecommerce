-- migrate:no-transaction
-- No query filters product_changes by run, and ingestion runs are never deleted; re-add with a run-deletion path (ADR 0010).
DROP INDEX CONCURRENTLY IF EXISTS idx_product_changes_run_id;
