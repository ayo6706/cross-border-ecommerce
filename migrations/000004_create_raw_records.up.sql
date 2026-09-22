CREATE TABLE IF NOT EXISTS raw_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id VARCHAR(64) NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    external_product_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    source_version VARCHAR(128) NOT NULL DEFAULT '',
    etag VARCHAR(128) NOT NULL DEFAULT '',
    source_updated_at TIMESTAMPTZ,
    ingestion_run_id UUID REFERENCES ingestion_runs(id) ON DELETE SET NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_records_source_external_received 
    ON raw_records(source_id, external_product_id, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_records_run_id 
    ON raw_records(ingestion_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_records_received_at_id 
    ON raw_records(received_at DESC, id DESC);
