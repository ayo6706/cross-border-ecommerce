-- name: SeedPendingRunProcessing :exec
INSERT INTO ingestion_run_processing (run_id)
SELECT id FROM ingestion_runs r
WHERE (status = 'COMPLETED' OR status = 'PARTIAL')
  AND NOT EXISTS (
      SELECT 1 FROM ingestion_run_processing p WHERE p.run_id = r.id
  )
ON CONFLICT (run_id) DO NOTHING;

-- name: ClaimNextRunProcessing :one
WITH candidate AS (
    SELECT run_id FROM ingestion_run_processing
    WHERE status = 'PENDING' OR (status = 'RUNNING' AND lease_expires_at < NOW())
    ORDER BY created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE ingestion_run_processing
SET status = 'RUNNING',
    claim_token = @claim_token::uuid,
    lease_expires_at = NOW() + @lease_duration::interval,
    started_at = COALESCE(ingestion_run_processing.started_at, NOW()),
    updated_at = NOW()
FROM candidate
WHERE ingestion_run_processing.run_id = candidate.run_id
RETURNING ingestion_run_processing.*;

-- name: ClaimSpecificRunProcessing :one
UPDATE ingestion_run_processing
SET status = 'RUNNING',
    claim_token = @claim_token::uuid,
    lease_expires_at = NOW() + @lease_duration::interval,
    started_at = COALESCE(started_at, NOW()),
    updated_at = NOW()
WHERE run_id = @run_id::uuid
  AND (status = 'PENDING' OR status = 'FAILED' OR status = 'COMPLETED' OR lease_expires_at < NOW() OR claim_token = @claim_token::uuid)
RETURNING *;

-- name: GetRunProcessingByID :one
SELECT * FROM ingestion_run_processing WHERE run_id = $1;

-- name: EnsureRunProcessingExists :one
INSERT INTO ingestion_run_processing (run_id)
VALUES ($1)
ON CONFLICT (run_id) DO UPDATE SET updated_at = NOW()
RETURNING *;

-- name: UpdateRunProcessingProgress :one
UPDATE ingestion_run_processing
SET cursor_raw_record_id = @cursor_id::uuid,
    records_seen = records_seen + @seen_inc::int,
    records_new = records_new + @new_inc::int,
    records_changed = records_changed + @changed_inc::int,
    records_unchanged = records_unchanged + @unchanged_inc::int,
    records_failed = records_failed + @failed_inc::int,
    lease_expires_at = NOW() + @lease_duration::interval,
    updated_at = NOW()
WHERE run_id = @run_id::uuid AND status = 'RUNNING' AND claim_token = @claim_token::uuid
RETURNING *;

-- name: ReleaseRunProcessingClaim :one
UPDATE ingestion_run_processing
SET status = 'PENDING',
    claim_token = NULL,
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE run_id = @run_id::uuid AND claim_token = @claim_token::uuid
RETURNING *;

-- name: CompleteRunProcessing :one
UPDATE ingestion_run_processing
SET status = 'COMPLETED',
    claim_token = NULL,
    lease_expires_at = NULL,
    completed_at = NOW(),
    updated_at = NOW()
WHERE run_id = @run_id::uuid AND claim_token = @claim_token::uuid
RETURNING *;

-- name: FailRunProcessing :one
UPDATE ingestion_run_processing
SET status = 'FAILED',
    error_summary = @error_summary::text,
    claim_token = NULL,
    lease_expires_at = NULL,
    completed_at = NOW(),
    updated_at = NOW()
WHERE run_id = @run_id::uuid AND (claim_token = @claim_token::uuid OR @claim_token::uuid IS NULL)
RETURNING *;

-- name: ResetRunProcessingFromStart :one
UPDATE ingestion_run_processing
SET status = 'PENDING',
    cursor_raw_record_id = NULL,
    records_seen = 0,
    records_new = 0,
    records_changed = 0,
    records_unchanged = 0,
    records_failed = 0,
    error_summary = '',
    started_at = NULL,
    completed_at = NULL,
    claim_token = NULL,
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE run_id = @run_id::uuid
  AND (status = 'PENDING' OR status = 'FAILED' OR status = 'COMPLETED' OR lease_expires_at < NOW())
RETURNING *;
