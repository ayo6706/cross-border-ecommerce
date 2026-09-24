-- name: CreateProductChange :one
INSERT INTO product_changes (
    id,
    product_id,
    from_version_id,
    to_version_id,
    change_type,
    changed_fields,
    ingestion_run_id,
    raw_record_id,
    detected_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
) RETURNING *;

-- name: ListProductChangesByProductID :many
SELECT *
FROM product_changes
WHERE product_id = $1
ORDER BY detected_at DESC, id DESC
LIMIT $2;
