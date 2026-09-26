-- name: GetProductByID :one
SELECT 
    id,
    canonical_name,
    description,
    brand,
    origin_country,
    status,
    current_version_id,
    current_fingerprint,
    created_at,
    updated_at
FROM products
WHERE id = $1;

-- name: UpsertProduct :one
INSERT INTO products (
    id,
    canonical_name,
    description,
    brand,
    origin_country,
    status,
    current_version_id,
    current_fingerprint,
    created_at,
    updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
ON CONFLICT (id) DO UPDATE SET
    canonical_name = EXCLUDED.canonical_name,
    description = EXCLUDED.description,
    brand = EXCLUDED.brand,
    origin_country = EXCLUDED.origin_country,
    status = EXCLUDED.status,
    current_version_id = EXCLUDED.current_version_id,
    current_fingerprint = EXCLUDED.current_fingerprint,
    updated_at = EXCLUDED.updated_at
RETURNING 
    id,
    canonical_name,
    description,
    brand,
    origin_country,
    status,
    current_version_id,
    current_fingerprint,
    created_at,
    updated_at;

-- name: ListProductsFirstPage :many
SELECT 
    id,
    canonical_name,
    description,
    brand,
    origin_country,
    status,
    current_version_id,
    current_fingerprint,
    created_at,
    updated_at
FROM products
ORDER BY created_at DESC, id DESC
LIMIT $1;

-- name: ListProductsAfterCursor :many
SELECT 
    id,
    canonical_name,
    description,
    brand,
    origin_country,
    status,
    current_version_id,
    current_fingerprint,
    created_at,
    updated_at
FROM products
WHERE (created_at, id) < (@cursor_created_at::timestamptz, @cursor_id::uuid)
ORDER BY created_at DESC, id DESC
LIMIT $1;

-- name: DeleteProduct :exec
DELETE FROM products
WHERE id = $1;

-- name: CreateProductVersion :one
INSERT INTO product_versions (
    id,
    product_id,
    version_number,
    fingerprint,
    canonical_name,
    description,
    brand,
    origin_country,
    attributes,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
RETURNING 
    id,
    product_id,
    version_number,
    fingerprint,
    canonical_name,
    description,
    brand,
    origin_country,
    attributes,
    created_at;

-- name: GetLatestProductVersion :one
SELECT 
    id,
    product_id,
    version_number,
    fingerprint,
    canonical_name,
    description,
    brand,
    origin_country,
    attributes,
    created_at
FROM product_versions
WHERE product_id = $1
ORDER BY version_number DESC
LIMIT 1;

-- name: ListProductVersions :many
SELECT 
    id,
    product_id,
    version_number,
    fingerprint,
    canonical_name,
    description,
    brand,
    origin_country,
    attributes,
    ingestion_run_id,
    created_at
FROM product_versions
WHERE product_id = $1
ORDER BY version_number DESC;

-- name: CopyProducts :copyfrom
INSERT INTO products (
    id,
    canonical_name,
    description,
    brand,
    origin_country,
    status,
    current_version_id,
    current_fingerprint,
    created_at,
    updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
);

-- name: CopyProductVersions :copyfrom
INSERT INTO product_versions (
    id,
    product_id,
    version_number,
    fingerprint,
    canonical_name,
    description,
    brand,
    origin_country,
    attributes,
    ingestion_run_id,
    created_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
);

-- name: BatchGuardedUpdateProductVersion :many
UPDATE products p
SET current_version_id = u.to_version_id,
    current_fingerprint = u.current_fingerprint,
    canonical_name = u.canonical_name,
    description = u.description,
    brand = u.brand,
    origin_country = u.origin_country,
    updated_at = u.updated_at
FROM (
    SELECT
        unnest(@ids::uuid[]) AS id,
        unnest(@to_version_ids::uuid[]) AS to_version_id,
        unnest(@current_fingerprints::varchar[]) AS current_fingerprint,
        unnest(@canonical_names::varchar[]) AS canonical_name,
        unnest(@descriptions::text[]) AS description,
        unnest(@brands::varchar[]) AS brand,
        unnest(@origin_countries::varchar[]) AS origin_country,
        unnest(@updated_ats::timestamptz[]) AS updated_at,
        unnest(@expected_version_ids::uuid[]) AS expected_version_id
) u
WHERE p.id = u.id
  AND p.current_version_id IS NOT DISTINCT FROM u.expected_version_id
RETURNING p.id;

-- name: BatchGuardedUpdateProductFingerprintOnly :many
UPDATE products p
SET current_fingerprint = u.current_fingerprint,
    updated_at = u.updated_at
FROM (
    SELECT
        unnest(@ids::uuid[]) AS id,
        unnest(@current_fingerprints::varchar[]) AS current_fingerprint,
        unnest(@updated_ats::timestamptz[]) AS updated_at,
        unnest(@expected_version_ids::uuid[]) AS expected_version_id
) u
WHERE p.id = u.id
  AND p.current_version_id IS NOT DISTINCT FROM u.expected_version_id
RETURNING p.id;
