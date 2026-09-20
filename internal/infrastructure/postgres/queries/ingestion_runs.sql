-- name: CreateIngestionRun :one
INSERT INTO ingestion_runs (
    id,
    source_id,
    status,
    checkpoint,
    records_seen,
    records_new,
    records_changed,
    records_unchanged,
    records_failed,
    error_summary,
    started_at,
    completed_at,
    created_at,
    updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
) RETURNING 
    id,
    source_id,
    status,
    checkpoint,
    records_seen,
    records_new,
    records_changed,
    records_unchanged,
    records_failed,
    error_summary,
    started_at,
    completed_at,
    created_at,
    updated_at;

-- name: GetIngestionRunByID :one
SELECT 
    id,
    source_id,
    status,
    checkpoint,
    records_seen,
    records_new,
    records_changed,
    records_unchanged,
    records_failed,
    error_summary,
    started_at,
    completed_at,
    created_at,
    updated_at
FROM ingestion_runs
WHERE id = $1;

-- name: UpdateIngestionRunProgress :one
UPDATE ingestion_runs
SET 
    records_seen = records_seen + @seen_increment::int,
    records_new = records_new + @new_increment::int,
    records_changed = records_changed + @changed_increment::int,
    records_unchanged = records_unchanged + @unchanged_increment::int,
    records_failed = records_failed + @failed_increment::int,
    checkpoint = CASE 
        WHEN @checkpoint::varchar != '' THEN @checkpoint::varchar 
        ELSE checkpoint 
    END,
    updated_at = @updated_at::timestamptz
WHERE id = @id::uuid
RETURNING 
    id,
    source_id,
    status,
    checkpoint,
    records_seen,
    records_new,
    records_changed,
    records_unchanged,
    records_failed,
    error_summary,
    started_at,
    completed_at,
    created_at,
    updated_at;

-- name: UpdateIngestionRunStatus :one
UPDATE ingestion_runs
SET 
    status = @status::varchar,
    error_summary = @error_summary::text,
    checkpoint = CASE 
        WHEN @checkpoint::varchar != '' THEN @checkpoint::varchar 
        ELSE checkpoint 
    END,
    completed_at = @completed_at::timestamptz,
    updated_at = @updated_at::timestamptz
WHERE id = @id::uuid
RETURNING 
    id,
    source_id,
    status,
    checkpoint,
    records_seen,
    records_new,
    records_changed,
    records_unchanged,
    records_failed,
    error_summary,
    started_at,
    completed_at,
    created_at,
    updated_at;

-- name: ListIngestionRunsBySource :many
SELECT 
    id,
    source_id,
    status,
    checkpoint,
    records_seen,
    records_new,
    records_changed,
    records_unchanged,
    records_failed,
    error_summary,
    started_at,
    completed_at,
    created_at,
    updated_at
FROM ingestion_runs
WHERE source_id = $1
ORDER BY created_at DESC, id DESC
LIMIT $2;

-- name: GetLatestIngestionRunBySource :one
SELECT 
    id,
    source_id,
    status,
    checkpoint,
    records_seen,
    records_new,
    records_changed,
    records_unchanged,
    records_failed,
    error_summary,
    started_at,
    completed_at,
    created_at,
    updated_at
FROM ingestion_runs
WHERE source_id = $1
ORDER BY created_at DESC, id DESC
LIMIT 1;
