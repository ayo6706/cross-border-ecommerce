-- migrate:no-transaction
-- Redundant: uq_product_versions_product_version (product_id, version_number) serves every product_id lookup (ADR 0010).
DROP INDEX CONCURRENTLY IF EXISTS idx_product_versions_product_id;
