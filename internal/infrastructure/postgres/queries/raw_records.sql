-- name: CreateRawRecord :one
INSERT INTO raw_records (
    id,
    source_id,
    external_product_id,
    payload,
    payload_raw,
    payload_sha256,
    source_version,
    etag,
    source_updated_at,
    ingestion_run_id,
    received_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
) RETURNING
    id,
    source_id,
    external_product_id,
    payload,
    source_version,
    etag,
    source_updated_at,
    ingestion_run_id,
    received_at,
    payload_raw,
    payload_sha256;

-- name: GetRawRecordByID :one
SELECT
    id,
    source_id,
    external_product_id,
    payload,
    source_version,
    etag,
    source_updated_at,
    ingestion_run_id,
    received_at,
    payload_raw,
    payload_sha256
FROM raw_records
WHERE id = $1;

-- name: GetLatestRawRecordBySourceAndExternalID :one
SELECT
    id,
    source_id,
    external_product_id,
    payload,
    source_version,
    etag,
    source_updated_at,
    ingestion_run_id,
    received_at,
    payload_raw,
    payload_sha256
FROM raw_records
WHERE source_id = $1 AND external_product_id = $2
ORDER BY received_at DESC, id DESC
LIMIT 1;

-- name: ListRawRecordsBySourceAndExternalID :many
SELECT
    id,
    source_id,
    external_product_id,
    payload,
    source_version,
    etag,
    source_updated_at,
    ingestion_run_id,
    received_at,
    payload_raw,
    payload_sha256
FROM raw_records
WHERE source_id = $1 AND external_product_id = $2
ORDER BY received_at DESC, id DESC
LIMIT $3;

-- name: ListRawRecordsByRunID :many
SELECT
    id,
    source_id,
    external_product_id,
    payload,
    source_version,
    etag,
    source_updated_at,
    ingestion_run_id,
    received_at,
    payload_raw,
    payload_sha256
FROM raw_records
WHERE ingestion_run_id = $1
ORDER BY received_at DESC, id DESC
LIMIT $2;

-- name: ListRawRecordsByRunIDKeysetFirstPage :many
SELECT
    id,
    source_id,
    external_product_id,
    payload,
    source_version,
    etag,
    source_updated_at,
    ingestion_run_id,
    received_at,
    payload_raw,
    payload_sha256
FROM raw_records
WHERE ingestion_run_id = $1
ORDER BY id ASC
LIMIT $2;

-- name: ListRawRecordsByRunIDKeysetAfterCursor :many
SELECT
    id,
    source_id,
    external_product_id,
    payload,
    source_version,
    etag,
    source_updated_at,
    ingestion_run_id,
    received_at,
    payload_raw,
    payload_sha256
FROM raw_records
WHERE ingestion_run_id = $1
  AND id > @cursor_id::uuid
ORDER BY id ASC
LIMIT $2;


