package idempotency

import (
	"context"
	"errors"
	"time"
)

var (
	ErrKeyInProgress      = errors.New("idempotency key is currently in progress")
	ErrLeaseLost          = errors.New("idempotency lease lost")
	ErrPayloadMismatch    = errors.New("idempotency payload hash mismatch")
	ErrInvalidScope       = errors.New("invalid idempotency scope")
	ErrInvalidKey         = errors.New("invalid idempotency key")
	ErrInvalidLeaseTTL    = errors.New("invalid idempotency lease TTL")
	ErrInvalidPayloadHash = errors.New("invalid payload hash: must be 32 bytes")
	ErrNotCompletedInTx   = errors.New("idempotency key was not completed within transaction")
	ErrKeyNotFound        = errors.New("idempotency key not found")
	ErrNilLogger          = errors.New("logger cannot be nil")
)

type ClaimStatus string

const (
	ClaimAcquired         ClaimStatus = "ACQUIRED"
	ClaimAlreadyCompleted ClaimStatus = "ALREADY_COMPLETED"
)

type Claim struct {
	Scope      string
	Key        string
	Status     ClaimStatus
	LeaseToken string
	ExpiresAt  time.Time
	Attempts   int
	completed  bool
}

func (c *Claim) IsCompleted() bool {
	return c.completed
}

func (c *Claim) MarkCompleted() {
	c.completed = true
}

type Record struct {
	Scope          string
	Key            string
	Status         string // "IN_PROGRESS" or "COMPLETED"
	PayloadHash    []byte
	LeaseToken     *string
	LeaseExpiresAt *time.Time
	Attempts       int
	CreatedAt      time.Time
	CompletedAt    *time.Time
}

// Completer marks an idempotency key COMPLETED within a transaction or standalone.
type Completer interface {
	CompleteKey(ctx context.Context, scope, key string, token string) error
}

// Store is the storage port for idempotency keys.
type Store interface {
	Completer
	ClaimKey(ctx context.Context, scope, key string, payloadHash []byte, token string, ttl time.Duration) (Claim, error)
	ReleaseKey(ctx context.Context, scope, key string, token string) error
}
