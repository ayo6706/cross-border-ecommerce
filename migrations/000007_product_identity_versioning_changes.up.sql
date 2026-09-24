-- Migration 000007: Product Identity, Versioning, Changes, and Run Processing State

-- 1. Tighten foreign keys to ON DELETE RESTRICT
ALTER TABLE product_versions DROP CONSTRAINT IF EXISTS product_versions_product_id_fkey;
ALTER TABLE product_versions ADD CONSTRAINT product_versions_product_id_fkey 
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE RESTRICT;

ALTER TABLE raw_records DROP CONSTRAINT IF EXISTS raw_records_ingestion_run_id_fkey;
ALTER TABLE raw_records ADD CONSTRAINT raw_records_ingestion_run_id_fkey 
    FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE RESTRICT;

-- 2. Widen fingerprint columns for "v1:<64 hex>" (67 chars)
ALTER TABLE products ALTER COLUMN current_fingerprint TYPE VARCHAR(128);
ALTER TABLE product_versions ALTER COLUMN fingerprint TYPE VARCHAR(128);
ALTER TABLE product_versions ADD COLUMN IF NOT EXISTS ingestion_run_id UUID 
    REFERENCES ingestion_runs(id) ON DELETE RESTRICT;

-- 3. Product Sources (Identity resolution & watermark tracking)
-- fillfactor=90 leaves space in data pages for HOT updates on watermark bumps
CREATE TABLE IF NOT EXISTS product_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
    source_id VARCHAR(64) NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    external_product_id VARCHAR(255) NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_source_updated_at TIMESTAMPTZ,
    last_received_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_product_sources_source_external UNIQUE (source_id, external_product_id)
) WITH (fillfactor = 90);
CREATE INDEX IF NOT EXISTS idx_product_sources_product_id ON product_sources(product_id);

-- 4. Product Changes (Audit record of version transitions)
CREATE TABLE IF NOT EXISTS product_changes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
    from_version_id UUID REFERENCES product_versions(id) ON DELETE RESTRICT,
    to_version_id UUID NOT NULL REFERENCES product_versions(id) ON DELETE RESTRICT,
    change_type VARCHAR(32) NOT NULL CHECK (change_type IN ('NEW', 'CHANGED')),
    changed_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
    ingestion_run_id UUID REFERENCES ingestion_runs(id) ON DELETE RESTRICT,
    raw_record_id UUID, -- No FK constraint: raw_records will be partitioned in ENG-054
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_product_changes_to_version UNIQUE (to_version_id)
);
CREATE INDEX IF NOT EXISTS idx_product_changes_product_detected 
    ON product_changes(product_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_product_changes_run_id 
    ON product_changes(ingestion_run_id);

-- 5. Ingestion Run Processing State (Cursor, lease claim, and progress counters)
CREATE TABLE IF NOT EXISTS ingestion_run_processing (
    run_id UUID PRIMARY KEY REFERENCES ingestion_runs(id) ON DELETE RESTRICT,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    claim_token UUID,
    lease_expires_at TIMESTAMPTZ,
    cursor_raw_record_id UUID,
    records_seen INT NOT NULL DEFAULT 0,
    records_new INT NOT NULL DEFAULT 0,
    records_changed INT NOT NULL DEFAULT 0,
    records_unchanged INT NOT NULL DEFAULT 0,
    records_failed INT NOT NULL DEFAULT 0,
    error_summary TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Immutable partial index for claim queries
CREATE INDEX IF NOT EXISTS idx_ingestion_run_processing_claim 
    ON ingestion_run_processing (status, lease_expires_at, created_at)
    WHERE status IN ('PENDING', 'RUNNING');

-- 6. Index Pruning & Keyset index
DROP INDEX IF EXISTS idx_raw_records_run_id;
CREATE INDEX IF NOT EXISTS idx_raw_records_run_keyset 
    ON raw_records(ingestion_run_id, id ASC);
