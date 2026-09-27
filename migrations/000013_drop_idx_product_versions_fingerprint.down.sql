-- migrate:no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_product_versions_fingerprint ON product_versions (fingerprint);
