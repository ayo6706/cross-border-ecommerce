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
RETURNING ingestion_run_processing.run_id, ingestion_run_processing.status,
    ingestion_run_processing.claim_token, ingestion_run_processing.lease_expires_at,
    ingestion_run_processing.cursor_raw_record_id, ingestion_run_processing.records_seen,
    ingestion_run_processing.records_new, ingestion_run_processing.records_changed,
    ingestion_run_processing.records_unchanged, ingestion_run_processing.records_failed,
    ingestion_run_processing.error_summary, ingestion_run_processing.started_at,
    ingestion_run_processing.completed_at, ingestion_run_processing.created_at,
    ingestion_run_processing.updated_at;

-- name: ClaimSpecificRunProcessing :one
UPDATE ingestion_run_processing
SET status = 'RUNNING',
    claim_token = @claim_token::uuid,
    lease_expires_at = NOW() + @lease_duration::interval,
    started_at = COALESCE(started_at, NOW()),
    updated_at = NOW()
WHERE run_id = @run_id::uuid
  AND (status = 'PENDING' OR status = 'FAILED' OR status = 'COMPLETED' OR lease_expires_at < NOW() OR claim_token = @claim_token::uuid)
RETURNING run_id, status, claim_token, lease_expires_at, cursor_raw_record_id, records_seen, records_new,
    records_changed, records_unchanged, records_failed, error_summary, started_at, completed_at,
    created_at, updated_at;

-- name: GetRunProcessingByID :one
SELECT run_id, status, claim_token, lease_expires_at, cursor_raw_record_id, records_seen, records_new,
    records_changed, records_unchanged, records_failed, error_summary, started_at, completed_at,
    created_at, updated_at
FROM ingestion_run_processing WHERE run_id = $1;

-- name: EnsureRunProcessingExists :one
INSERT INTO ingestion_run_processing (run_id)
VALUES ($1)
ON CONFLICT (run_id) DO UPDATE SET updated_at = NOW()
RETURNING run_id, status, claim_token, lease_expires_at, cursor_raw_record_id, records_seen, records_new,
    records_changed, records_unchanged, records_failed, error_summary, started_at, completed_at,
    created_at, updated_at;

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
RETURNING run_id, status, claim_token, lease_expires_at, cursor_raw_record_id, records_seen, records_new,
    records_changed, records_unchanged, records_failed, error_summary, started_at, completed_at,
    created_at, updated_at;

-- name: ReleaseRunProcessingClaim :one
UPDATE ingestion_run_processing
SET status = 'PENDING',
    claim_token = NULL,
    lease_expires_at = NULL,
    updated_at = NOW()
WHERE run_id = @run_id::uuid AND claim_token = @claim_token::uuid
RETURNING run_id, status, claim_token, lease_expires_at, cursor_raw_record_id, records_seen, records_new,
    records_changed, records_unchanged, records_failed, error_summary, started_at, completed_at,
    created_at, updated_at;

-- name: CompleteRunProcessing :one
UPDATE ingestion_run_processing
SET status = 'COMPLETED',
    claim_token = NULL,
    lease_expires_at = NULL,
    completed_at = NOW(),
    updated_at = NOW()
WHERE run_id = @run_id::uuid AND claim_token = @claim_token::uuid
RETURNING run_id, status, claim_token, lease_expires_at, cursor_raw_record_id, records_seen, records_new,
    records_changed, records_unchanged, records_failed, error_summary, started_at, completed_at,
    created_at, updated_at;

-- name: FailRunProcessing :one
UPDATE ingestion_run_processing
SET status = 'FAILED',
    error_summary = @error_summary::text,
    claim_token = NULL,
    lease_expires_at = NULL,
    completed_at = NOW(),
    updated_at = NOW()
WHERE run_id = @run_id::uuid AND (claim_token = @claim_token::uuid OR @claim_token::uuid IS NULL)
RETURNING run_id, status, claim_token, lease_expires_at, cursor_raw_record_id, records_seen, records_new,
    records_changed, records_unchanged, records_failed, error_summary, started_at, completed_at,
    created_at, updated_at;

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
RETURNING run_id, status, claim_token, lease_expires_at, cursor_raw_record_id, records_seen, records_new,
    records_changed, records_unchanged, records_failed, error_summary, started_at, completed_at,
    created_at, updated_at;
