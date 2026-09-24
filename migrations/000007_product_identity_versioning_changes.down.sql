-- Revert Migration 000007

-- 1. Restore old raw_records index and drop keyset index
DROP INDEX IF EXISTS idx_raw_records_run_keyset;
CREATE INDEX IF NOT EXISTS idx_raw_records_run_id ON raw_records(ingestion_run_id);

-- 2. Drop run processing table
DROP TABLE IF EXISTS ingestion_run_processing;

-- 3. Drop product changes table
DROP TABLE IF EXISTS product_changes;

-- 4. Drop product sources table
DROP TABLE IF EXISTS product_sources;

-- 5. Restore product_versions and products columns/constraints
ALTER TABLE product_versions DROP COLUMN IF EXISTS ingestion_run_id;
-- Note: Reverting VARCHAR(128) back to VARCHAR(64) on non-empty tables may truncate stored "v1:<64 hex>" values.
ALTER TABLE product_versions ALTER COLUMN fingerprint TYPE VARCHAR(64);
ALTER TABLE products ALTER COLUMN current_fingerprint TYPE VARCHAR(64);

ALTER TABLE raw_records DROP CONSTRAINT IF EXISTS raw_records_ingestion_run_id_fkey;
ALTER TABLE raw_records ADD CONSTRAINT raw_records_ingestion_run_id_fkey 
    FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;

ALTER TABLE product_versions DROP CONSTRAINT IF EXISTS product_versions_product_id_fkey;
ALTER TABLE product_versions ADD CONSTRAINT product_versions_product_id_fkey 
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE;
