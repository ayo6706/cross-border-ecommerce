-- Runs are queued for product processing when they finish (UpdateIngestionRunStatus). This queues
-- runs that finished earlier and have no processing row; ON CONFLICT keeps it safe beside a worker
-- that still runs the old scan during a rolling deploy.
INSERT INTO ingestion_run_processing (run_id)
SELECT r.id
FROM ingestion_runs r
WHERE r.status IN ('COMPLETED', 'PARTIAL')
  AND NOT EXISTS (SELECT 1 FROM ingestion_run_processing p WHERE p.run_id = r.id)
ON CONFLICT (run_id) DO NOTHING;
