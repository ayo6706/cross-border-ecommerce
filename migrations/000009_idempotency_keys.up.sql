CREATE TABLE idempotency_keys (
    scope VARCHAR(128) NOT NULL,
    key VARCHAR(256) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED')),
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    attempts INT NOT NULL DEFAULT 1 CHECK (attempts >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (scope, key),
    CHECK (
        (status = 'IN_PROGRESS' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL AND completed_at IS NULL)
        OR
        (status = 'COMPLETED' AND completed_at IS NOT NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX idx_idempotency_keys_expires_at ON idempotency_keys(lease_expires_at) WHERE status = 'IN_PROGRESS';
