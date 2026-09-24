CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_runs_active_source 
ON ingestion_runs(source_id) 
WHERE status IN ('PENDING', 'RUNNING');
