CREATE TABLE IF NOT EXISTS ingestion_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id VARCHAR(64) NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    checkpoint VARCHAR(1024) NOT NULL DEFAULT '',
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

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_source_id ON ingestion_runs(source_id);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_status ON ingestion_runs(status);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_created_at ON ingestion_runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingestion_runs_source_created ON ingestion_runs(source_id, created_at DESC);
