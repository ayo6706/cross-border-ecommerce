-- migrate:no-transaction
-- Only served the ON DELETE RESTRICT check of products(id); no query deletes products (ADR 0010, ENG-017).
DROP INDEX CONCURRENTLY IF EXISTS idx_product_sources_product_id;
