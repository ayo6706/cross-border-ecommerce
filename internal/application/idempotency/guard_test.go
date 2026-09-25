package idempotency_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/idempotency"
	"github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/stretchr/testify/require"
)

type memoryRecord struct {
	scope          string
	key            string
	status         string
	payloadHash    []byte
	leaseToken     *string
	leaseExpiresAt *time.Time
	attempts       int
	createdAt      time.Time
	completedAt    *time.Time
}

type memoryStore struct {
	mu      sync.Mutex
	records map[string]*memoryRecord
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		records: make(map[string]*memoryRecord),
	}
}

func (s *memoryStore) key(scope, k string) string {
	return scope + ":" + k
}

func (s *memoryStore) ClaimKey(
	ctx context.Context,
	scope, key string,
	payloadHash []byte,
	token string,
	ttl time.Duration,
) (idempotency.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := s.key(scope, key)
	rec, exists := s.records[k]
	now := time.Now()

	if !exists {
		exp := now.Add(ttl)
		tokenCopy := token
		s.records[k] = &memoryRecord{
			scope:          scope,
			key:            key,
			status:         "IN_PROGRESS",
			payloadHash:    append([]byte(nil), payloadHash...),
			leaseToken:     &tokenCopy,
			leaseExpiresAt: &exp,
			attempts:       1,
			createdAt:      now,
		}
		return idempotency.Claim{
			Scope:      scope,
			Key:        key,
			Status:     idempotency.ClaimAcquired,
			LeaseToken: token,
			ExpiresAt:  exp,
			Attempts:   1,
		}, nil
	}

	// Record exists
	if !bytes.Equal(rec.payloadHash, payloadHash) {
		return idempotency.Claim{}, idempotency.ErrPayloadMismatch
	}

	if rec.status == "COMPLETED" {
		return idempotency.Claim{
			Scope:  scope,
			Key:    key,
			Status: idempotency.ClaimAlreadyCompleted,
		}, nil
	}

	// Status is IN_PROGRESS
	if rec.leaseExpiresAt != nil && rec.leaseExpiresAt.After(now) {
		return idempotency.Claim{}, idempotency.ErrKeyInProgress
	}

	// Lease expired - reclaim
	exp := now.Add(ttl)
	tokenCopy := token
	rec.leaseToken = &tokenCopy
	rec.leaseExpiresAt = &exp
	rec.attempts++

	return idempotency.Claim{
		Scope:      scope,
		Key:        key,
		Status:     idempotency.ClaimAcquired,
		LeaseToken: token,
		ExpiresAt:  exp,
		Attempts:   rec.attempts,
	}, nil
}

func (s *memoryStore) CompleteKey(ctx context.Context, scope, key string, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.records[s.key(scope, key)]
	if !exists || rec.status != "IN_PROGRESS" || rec.leaseToken == nil || *rec.leaseToken != token {
		return idempotency.ErrLeaseLost
	}

	now := time.Now()
	rec.status = "COMPLETED"
	rec.completedAt = &now
	rec.leaseToken = nil
	rec.leaseExpiresAt = nil
	return nil
}

func (s *memoryStore) ReleaseKey(ctx context.Context, scope, key string, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.records[s.key(scope, key)]
	if !exists || rec.status != "IN_PROGRESS" || rec.leaseToken == nil || *rec.leaseToken != token {
		return nil
	}

	now := time.Now().Add(-1 * time.Millisecond)
	rec.leaseExpiresAt = &now
	return nil
}

func (s *memoryStore) GetKey(ctx context.Context, scope, key string) (idempotency.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, exists := s.records[s.key(scope, key)]
	if !exists {
		return idempotency.Record{}, errors.New("not found")
	}

	return idempotency.Record{
		Scope:          rec.scope,
		Key:            rec.key,
		Status:         rec.status,
		PayloadHash:    rec.payloadHash,
		LeaseToken:     rec.leaseToken,
		LeaseExpiresAt: rec.leaseExpiresAt,
		Attempts:       rec.attempts,
		CreatedAt:      rec.createdAt,
		CompletedAt:    rec.completedAt,
	}, nil
}

func TestNewGuard_Validation(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	_, err := idempotency.NewGuard(nil, 10*time.Second, testLogger)
	require.Error(t, err)

	_, err = idempotency.NewGuard(store, 0, testLogger)
	require.ErrorIs(t, err, idempotency.ErrInvalidLeaseTTL)

	_, err = idempotency.NewGuard(store, -5*time.Second, testLogger)
	require.ErrorIs(t, err, idempotency.ErrInvalidLeaseTTL)

	_, err = idempotency.NewGuard(store, 10*time.Second, nil)
	require.ErrorIs(t, err, idempotency.ErrNilLogger)

	guard, err := idempotency.NewGuard(store, 10*time.Second, testLogger)
	require.NoError(t, err)
	require.NotNil(t, guard)
}

func TestGuard_Wrap_InputValidation(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 10*time.Second, testLogger)
	require.NoError(t, err)

	ctx := context.Background()

	// Empty scope at Wrap time
	_, err = guard.Wrap("", func(ctx context.Context, msg messaging.Message) error { return nil })
	require.ErrorIs(t, err, idempotency.ErrInvalidScope)

	// Whitespace scope at Wrap time
	_, err = guard.Wrap("   ", func(ctx context.Context, msg messaging.Message) error { return nil })
	require.ErrorIs(t, err, idempotency.ErrInvalidScope)

	// Nil handler at Wrap time
	_, err = guard.Wrap("grp", nil)
	require.Error(t, err)

	// Valid wrap, but empty key at invoke time
	h, err := guard.Wrap("grp", func(ctx context.Context, msg messaging.Message) error { return nil })
	require.NoError(t, err)
	err = h(ctx, messaging.Message{EventID: ""})
	require.ErrorIs(t, err, idempotency.ErrInvalidKey)
}

func TestGuard_A1_FirstDeliveryCompletes(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 10*time.Second, testLogger)
	require.NoError(t, err)

	called := false
	handler, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		called = true
		claim, ok := idempotency.FromContext(ctx)
		require.True(t, ok)
		require.Equal(t, "test-group", claim.Scope)
		require.Equal(t, "evt-1", claim.Key)
		require.Equal(t, 1, claim.Attempts)
		return idempotency.CompleteInTx(ctx, store)
	})
	require.NoError(t, err)

	msg := messaging.Message{EventID: "evt-1", Payload: []byte(`{"hello":"world"}`)}
	err = handler(context.Background(), msg)
	require.NoError(t, err)
	require.True(t, called)

	// Verify in store
	rec, err := store.GetKey(context.Background(), "test-group", "evt-1")
	require.NoError(t, err)
	require.Equal(t, "COMPLETED", rec.Status)
	require.NotNil(t, rec.CompletedAt)
	require.Nil(t, rec.LeaseToken)
	require.Nil(t, rec.LeaseExpiresAt)
}

func TestGuard_A2_RedeliverySkipped(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 10*time.Second, testLogger)
	require.NoError(t, err)

	callCount := 0
	handler, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		callCount++
		return idempotency.CompleteInTx(ctx, store)
	})
	require.NoError(t, err)

	msg := messaging.Message{EventID: "evt-1", Payload: []byte(`{"hello":"world"}`)}

	// First delivery
	err = handler(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, 1, callCount)

	// Second delivery (redelivery of identical message)
	err = handler(context.Background(), msg)
	require.NoError(t, err)        // Returns nil so it is ACKed
	require.Equal(t, 1, callCount) // Handler was NOT called a second time!
}

func TestGuard_A3_DifferentEventIDs_NotSkipped(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 10*time.Second, testLogger)
	require.NoError(t, err)

	processed := make([]string, 0)
	handler, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		processed = append(processed, msg.EventID)
		return idempotency.CompleteInTx(ctx, store)
	})
	require.NoError(t, err)

	// Revert scenario A -> B -> A: Event 1 has fingerprint A, Event 2 has B, Event 3 has A
	payloadA := []byte(`{"name":"item A"}`)
	payloadB := []byte(`{"name":"item B"}`)

	err = handler(context.Background(), messaging.Message{EventID: "evt-1", Payload: payloadA})
	require.NoError(t, err)

	err = handler(context.Background(), messaging.Message{EventID: "evt-2", Payload: payloadB})
	require.NoError(t, err)

	err = handler(context.Background(), messaging.Message{EventID: "evt-3", Payload: payloadA})
	require.NoError(t, err)

	require.Equal(t, []string{"evt-1", "evt-2", "evt-3"}, processed)
}

func TestGuard_F1_ConcurrentClaim_KeyInProgress(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 10*time.Second, testLogger)
	require.NoError(t, err)

	started := make(chan struct{})
	block := make(chan struct{})

	h, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		close(started)
		<-block
		return idempotency.CompleteInTx(ctx, store)
	})
	require.NoError(t, err)

	msg := messaging.Message{EventID: "evt-1", Payload: []byte(`{"data":"test"}`)}

	go func() {
		_ = h(context.Background(), msg)
	}()

	<-started

	// Second worker tries to process same key while first is still running
	err2 := h(context.Background(), msg)
	require.ErrorIs(t, err2, idempotency.ErrKeyInProgress)

	close(block)
}

func TestGuard_F2_ExpiredLeaseReclaimable(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// TTL of 20ms
	guard, err := idempotency.NewGuard(store, 20*time.Millisecond, testLogger)
	require.NoError(t, err)

	// Worker 1 claims key but crashes (does not complete)
	hash := sha256.Sum256([]byte("payload"))
	token1, err := uuid.NewString()
	require.NoError(t, err)
	claim1, err := store.ClaimKey(context.Background(), "test-group", "evt-1", hash[:], token1, 20*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, idempotency.ClaimAcquired, claim1.Status)
	require.Equal(t, 1, claim1.Attempts)

	// Wait for TTL to expire
	time.Sleep(30 * time.Millisecond)

	// Worker 2 runs via Guard
	ran := false
	h, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		ran = true
		c, ok := idempotency.FromContext(ctx)
		require.True(t, ok)
		require.Equal(t, 2, c.Attempts) // Attempts incremented!
		return idempotency.CompleteInTx(ctx, store)
	})
	require.NoError(t, err)

	err = h(context.Background(), messaging.Message{EventID: "evt-1", Payload: []byte("payload")})
	require.NoError(t, err)
	require.True(t, ran)

	rec, err := store.GetKey(context.Background(), "test-group", "evt-1")
	require.NoError(t, err)
	require.Equal(t, "COMPLETED", rec.Status)
	require.Equal(t, 2, rec.Attempts)
}

func TestGuard_F3_TransactionalCompletion_CrashBeforeACK(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 10*time.Second, testLogger)
	require.NoError(t, err)

	executedCount := 0
	h, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		executedCount++
		// Simulate transactional completion inside handler
		return idempotency.CompleteInTx(ctx, store)
	})
	require.NoError(t, err)

	msg := messaging.Message{EventID: "evt-1", Payload: []byte("payload")}

	// First execution completes transactionally
	err = h(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, 1, executedCount)

	// Simulate crash before ACK -> message redelivered from stream
	err = h(context.Background(), msg)
	require.NoError(t, err) // Handled as already completed -> ACKed
	require.Equal(t, 1, executedCount)
}

func TestGuard_F4_StaleHolderCannotComplete(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 20*time.Millisecond, testLogger)
	require.NoError(t, err)

	// Worker A claims lease
	hash := sha256.Sum256([]byte("payload"))
	tokenA, err := uuid.NewString()
	require.NoError(t, err)
	claimA, err := store.ClaimKey(context.Background(), "test-group", "evt-1", hash[:], tokenA, 20*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 1, claimA.Attempts)

	// Wait for Worker A's lease to expire
	time.Sleep(30 * time.Millisecond)

	// Worker B claims key
	tokenB, err := uuid.NewString()
	require.NoError(t, err)
	claimB, err := store.ClaimKey(context.Background(), "test-group", "evt-1", hash[:], tokenB, 10*time.Second)
	require.NoError(t, err)
	require.Equal(t, 2, claimB.Attempts)

	// Worker A tries to complete with stale tokenA
	err = store.CompleteKey(context.Background(), "test-group", "evt-1", tokenA)
	require.ErrorIs(t, err, idempotency.ErrLeaseLost)

	// Worker B successfully completes with tokenB
	err = store.CompleteKey(context.Background(), "test-group", "evt-1", tokenB)
	require.NoError(t, err)

	_ = guard
}

func TestGuard_F5_PayloadMismatch_FailsLoudly(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 10*time.Second, testLogger)
	require.NoError(t, err)

	h, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		return idempotency.CompleteInTx(ctx, store)
	})
	require.NoError(t, err)

	// First message
	err = h(context.Background(), messaging.Message{EventID: "evt-1", Payload: []byte("original payload")})
	require.NoError(t, err)

	// Corrupted redelivery with different payload
	err = h(context.Background(), messaging.Message{EventID: "evt-1", Payload: []byte("tampered payload")})
	require.ErrorIs(t, err, idempotency.ErrPayloadMismatch)
}

func TestGuard_F6_HandlerError_ReleasesLeaseImmediately(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 30*time.Second, testLogger)
	require.NoError(t, err)

	failOnce := true
	h, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		if failOnce {
			failOnce = false
			return errors.New("transient database connection timeout")
		}
		return idempotency.CompleteInTx(ctx, store)
	})
	require.NoError(t, err)

	msg := messaging.Message{EventID: "evt-1", Payload: []byte("data")}

	// First run fails
	err = h(context.Background(), msg)
	require.Error(t, err)

	// Because lease was released immediately, the immediate next delivery succeeds without waiting 30s!
	err = h(context.Background(), msg)
	require.NoError(t, err)

	rec, err := store.GetKey(context.Background(), "test-group", "evt-1")
	require.NoError(t, err)
	require.Equal(t, "COMPLETED", rec.Status)
	require.Equal(t, 2, rec.Attempts)
}

func TestGuard_HandlerPanic_ReleasesLease(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 30*time.Second, testLogger)
	require.NoError(t, err)

	h, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		panic("unexpected nil pointer panic")
	})
	require.NoError(t, err)

	msg := messaging.Message{EventID: "evt-1", Payload: []byte("data")}

	require.Panics(t, func() {
		_ = h(context.Background(), msg)
	})

	// Ensure lease was expired so next attempt can claim immediately
	rec, err := store.GetKey(context.Background(), "test-group", "evt-1")
	require.NoError(t, err)
	require.Equal(t, "IN_PROGRESS", rec.Status)
	require.True(t, rec.LeaseExpiresAt.Before(time.Now()) || rec.LeaseExpiresAt.Equal(time.Now()))
}

func TestGuard_F7_HandlerReturnsNilWithoutCompleteInTx_FailsWithErrNotCompletedInTx(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 10*time.Second, testLogger)
	require.NoError(t, err)

	// Handler returns nil without calling CompleteInTx
	h, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		return nil
	})
	require.NoError(t, err)

	msg := messaging.Message{EventID: "evt-1", Payload: []byte("payload")}
	err = h(context.Background(), msg)
	require.ErrorIs(t, err, idempotency.ErrNotCompletedInTx)

	// Verify lease was released so retry can immediately re-claim
	rec, err := store.GetKey(context.Background(), "test-group", "evt-1")
	require.NoError(t, err)
	require.Equal(t, "IN_PROGRESS", rec.Status)
	require.True(t, rec.LeaseExpiresAt.Before(time.Now()) || rec.LeaseExpiresAt.Equal(time.Now()))
}

func TestGuard_F8_ErrorAfterCompleteInTx_ReleasesLeaseImmediately(t *testing.T) {
	store := newMemoryStore()
	testLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	guard, err := idempotency.NewGuard(store, 30*time.Second, testLogger)
	require.NoError(t, err)

	simulatedCommitErr := errors.New("simulated commit failure / rollback")

	// Handler calls CompleteInTx, but then commit fails and handler returns error
	firstAttempt := true
	h, err := guard.Wrap("test-group", func(ctx context.Context, msg messaging.Message) error {
		if firstAttempt {
			firstAttempt = false
			// Simulate calling CompleteInTx before transaction aborts
			claim, ok := idempotency.FromContext(ctx)
			require.True(t, ok)
			claim.MarkCompleted()
			// Transaction aborts!
			return simulatedCommitErr
		}
		// Second attempt succeeds and completes properly
		return idempotency.CompleteInTx(ctx, store)
	})
	require.NoError(t, err)

	msg := messaging.Message{EventID: "evt-1", Payload: []byte("payload")}

	// First attempt fails
	err = h(context.Background(), msg)
	require.ErrorIs(t, err, simulatedCommitErr)

	// Verify that ReleaseKey was called and immediate retry succeeds without waiting 30s!
	err = h(context.Background(), msg)
	require.NoError(t, err)

	rec, err := store.GetKey(context.Background(), "test-group", "evt-1")
	require.NoError(t, err)
	require.Equal(t, "COMPLETED", rec.Status)
	require.Equal(t, 2, rec.Attempts)
}

// failingReleaseStore behaves like memoryStore but every ReleaseKey fails.
type failingReleaseStore struct {
	*memoryStore
}

func (s failingReleaseStore) ReleaseKey(context.Context, string, string, string) error {
	return errors.New("release unavailable")
}

func TestGuard_ReleaseFailureIsLoggedOnEveryPath(t *testing.T) {
	cases := map[string]messaging.Handler{
		"handler error": func(context.Context, messaging.Message) error {
			return errors.New("boom")
		},
		"handler panic": func(context.Context, messaging.Message) error {
			panic("boom")
		},
		"handler returned without CompleteInTx": func(context.Context, messaging.Message) error {
			return nil
		},
	}
	for reason, next := range cases {
		t.Run(reason, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			guard, err := idempotency.NewGuard(failingReleaseStore{newMemoryStore()}, 10*time.Second, logger)
			require.NoError(t, err)
			h, err := guard.Wrap("test-group", next)
			require.NoError(t, err)

			func() {
				defer func() { _ = recover() }()
				_ = h(context.Background(), messaging.Message{EventID: "evt-1", Payload: []byte(`{}`)})
			}()

			require.Contains(t, logs.String(), "failed to release idempotency lease")
			require.Contains(t, logs.String(), "release unavailable")
		})
	}
}
