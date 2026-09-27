-- migrate:no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_product_versions_product_id ON product_versions (product_id);
