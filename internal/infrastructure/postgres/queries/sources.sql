-- name: GetSourceByID :one
SELECT 
    id,
    name,
    type,
    config,
    rate_limit,
    enabled,
    created_at,
    updated_at
FROM sources
WHERE id = $1;

-- name: ListActiveSources :many
SELECT 
    id,
    name,
    type,
    config,
    rate_limit,
    enabled,
    created_at,
    updated_at
FROM sources
WHERE enabled = TRUE
ORDER BY id ASC;

-- name: ListSources :many
SELECT 
    id,
    name,
    type,
    config,
    rate_limit,
    enabled,
    created_at,
    updated_at
FROM sources
ORDER BY id ASC;

-- name: UpsertSource :one
INSERT INTO sources (
    id,
    name,
    type,
    config,
    rate_limit,
    enabled,
    created_at,
    updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    type = EXCLUDED.type,
    config = EXCLUDED.config,
    rate_limit = EXCLUDED.rate_limit,
    enabled = EXCLUDED.enabled,
    updated_at = EXCLUDED.updated_at
RETURNING 
    id,
    name,
    type,
    config,
    rate_limit,
    enabled,
    created_at,
    updated_at;

-- name: DeleteSource :exec
DELETE FROM sources
WHERE id = $1;
