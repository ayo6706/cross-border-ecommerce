-- Migration 000006 Down: Revert schema integrity changes

-- 1. Re-add dropped indexes
CREATE INDEX IF NOT EXISTS idx_raw_records_received_at_id ON raw_records(received_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_products_fingerprint ON products(current_fingerprint);
CREATE INDEX IF NOT EXISTS idx_products_origin_country ON products(origin_country);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_created_at ON ingestion_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status ON ingestion_runs(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_source_id ON ingestion_runs(source_id);

-- 2. Restore CASCADE foreign keys
ALTER TABLE raw_records DROP CONSTRAINT IF EXISTS raw_records_ingestion_run_id_fkey;
ALTER TABLE raw_records ADD CONSTRAINT raw_records_ingestion_run_id_fkey 
    FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;

ALTER TABLE raw_records DROP CONSTRAINT IF EXISTS raw_records_source_id_fkey;
ALTER TABLE raw_records ADD CONSTRAINT raw_records_source_id_fkey 
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE;

ALTER TABLE ingestion_runs DROP CONSTRAINT IF EXISTS ingestion_runs_source_id_fkey;
ALTER TABLE ingestion_runs ADD CONSTRAINT ingestion_runs_source_id_fkey 
    FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE;

-- 3. Drop payload_sha256 and payload_raw columns
ALTER TABLE raw_records DROP COLUMN IF EXISTS payload_sha256;
ALTER TABLE raw_records DROP COLUMN IF EXISTS payload_raw;

-- 4. Revert default product status
ALTER TABLE products ALTER COLUMN status SET DEFAULT 'ACTIVE';
