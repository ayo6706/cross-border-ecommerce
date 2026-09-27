-- migrate:no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_product_sources_product_id ON product_sources (product_id);
