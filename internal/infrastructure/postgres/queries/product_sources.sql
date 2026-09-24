-- name: GetProductWithSourceByIdentity :one
SELECT 
    ps.id AS product_source_id,
    ps.product_id,
    ps.source_id,
    ps.external_product_id,
    ps.first_seen_at,
    ps.last_changed_at,
    ps.last_source_updated_at,
    ps.last_received_at,
    p.canonical_name,
    p.description,
    p.brand,
    p.origin_country,
    p.status AS product_status,
    p.current_version_id,
    p.current_fingerprint,
    p.created_at AS product_created_at,
    p.updated_at AS product_updated_at,
    pv.id AS version_id,
    pv.version_number,
    pv.fingerprint AS version_fingerprint,
    pv.canonical_name AS version_canonical_name,
    pv.description AS version_description,
    pv.brand AS version_brand,
    pv.origin_country AS version_origin_country,
    pv.attributes AS version_attributes,
    pv.ingestion_run_id AS version_ingestion_run_id,
    pv.created_at AS version_created_at
FROM product_sources ps
JOIN products p ON ps.product_id = p.id
LEFT JOIN product_versions pv ON p.current_version_id = pv.id
WHERE ps.source_id = $1 AND ps.external_product_id = $2;

-- name: CreateProductSource :one
INSERT INTO product_sources (
    id,
    product_id,
    source_id,
    external_product_id,
    first_seen_at,
    last_changed_at,
    last_source_updated_at,
    last_received_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
) RETURNING *;

-- name: UpdateProductSourceWatermark :one
UPDATE product_sources
SET last_source_updated_at = GREATEST(last_source_updated_at, @source_updated_at::timestamptz),
    last_received_at       = GREATEST(last_received_at, @received_at::timestamptz)
WHERE id = @id::uuid
  AND (last_source_updated_at IS DISTINCT FROM GREATEST(last_source_updated_at, @source_updated_at::timestamptz)
       OR last_received_at < @received_at::timestamptz)
RETURNING *;

-- name: UpdateProductSourceOnChanged :one
UPDATE product_sources
SET last_changed_at        = @last_changed_at::timestamptz,
    last_source_updated_at = GREATEST(last_source_updated_at, @source_updated_at::timestamptz),
    last_received_at       = GREATEST(last_received_at, @received_at::timestamptz)
WHERE id = @id::uuid
RETURNING *;
