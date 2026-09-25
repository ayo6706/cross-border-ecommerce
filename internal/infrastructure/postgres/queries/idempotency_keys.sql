-- name: ClaimIdempotencyKey :one
INSERT INTO idempotency_keys (
    scope,
    key,
    status,
    payload_hash,
    lease_token,
    lease_expires_at,
    attempts,
    created_at
) VALUES (
    $1,
    $2,
    'IN_PROGRESS',
    $3,
    $4,
    NOW() + (sqlc.arg('lease_duration_ms')::bigint * INTERVAL '1 millisecond'),
    1,
    NOW()
)
ON CONFLICT (scope, key) DO UPDATE
SET lease_token = EXCLUDED.lease_token,
    lease_expires_at = EXCLUDED.lease_expires_at,
    attempts = idempotency_keys.attempts + 1
WHERE idempotency_keys.status = 'IN_PROGRESS'
  AND idempotency_keys.lease_expires_at <= NOW()
  AND idempotency_keys.payload_hash = EXCLUDED.payload_hash
RETURNING scope, key, status, payload_hash, lease_token, lease_expires_at, attempts, created_at, completed_at;



-- name: GetIdempotencyKey :one
SELECT scope, key, status, payload_hash, lease_token, lease_expires_at, attempts, created_at, completed_at
FROM idempotency_keys
WHERE scope = $1 AND key = $2;

-- name: CompleteIdempotencyKey :execrows
UPDATE idempotency_keys
SET status = 'COMPLETED',
    completed_at = NOW(),
    lease_token = NULL,
    lease_expires_at = NULL
WHERE scope = $1
  AND key = $2
  AND lease_token = $3
  AND status = 'IN_PROGRESS';

-- name: ReleaseIdempotencyKey :execrows
UPDATE idempotency_keys
SET lease_expires_at = NOW()
WHERE scope = $1
  AND key = $2
  AND lease_token = $3
  AND status = 'IN_PROGRESS';
