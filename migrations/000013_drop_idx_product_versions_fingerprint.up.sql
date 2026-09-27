-- migrate:no-transaction
-- No query looks up product_versions by fingerprint (ADR 0010).
DROP INDEX CONCURRENTLY IF EXISTS idx_product_versions_fingerprint;
