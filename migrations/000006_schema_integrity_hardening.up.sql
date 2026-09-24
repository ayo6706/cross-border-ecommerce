-- Migration 000006: Schema integrity, strict foreign keys, raw payload columns, and index pruning

-- 1. Correct default product status to DRAFT
ALTER TABLE products ALTER COLUMN status SET DEFAULT 'DRAFT';

-- 2. Add raw byte payload and content hash to raw_records
ALTER TABLE raw_records ADD COLUMN IF NOT EXISTS payload_raw BYTEA;
ALTER TABLE raw_records ADD COLUMN IF NOT EXISTS payload_sha256 VARCHAR(64) NOT NULL DEFAULT '';

-- 3. Replace ON DELETE CASCADE with ON DELETE RESTRICT to preserve audit provenance
ALTER TABLE ingestion_runs DROP CONSTRAINT IF EXISTS ingestion_runs_source_id_fkey;
ALTER TABLE ingestion_runs ADD CONSTRAINT ingestion_runs_source_id_fkey 
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE RESTRICT;

ALTER TABLE raw_records DROP CONSTRAINT IF EXISTS raw_records_source_id_fkey;
ALTER TABLE raw_records ADD CONSTRAINT raw_records_source_id_fkey 
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE RESTRICT;

ALTER TABLE raw_records DROP CONSTRAINT IF EXISTS raw_records_ingestion_run_id_fkey;
ALTER TABLE raw_records ADD CONSTRAINT raw_records_ingestion_run_id_fkey 
    FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE RESTRICT;

-- 4. Drop redundant/unused indexes to optimize high-throughput write path
DROP INDEX IF EXISTS idx_ingestion_runs_source_id;
DROP INDEX IF EXISTS idx_ingestion_runs_status;
DROP INDEX IF EXISTS idx_ingestion_runs_created_at;
DROP INDEX IF EXISTS idx_products_origin_country;
DROP INDEX IF EXISTS idx_products_fingerprint;
DROP INDEX IF EXISTS idx_raw_records_received_at_id;
