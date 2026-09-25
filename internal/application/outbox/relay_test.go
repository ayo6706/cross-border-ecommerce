package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStore struct {
	mu             sync.Mutex
	events         []Event
	claimedTokens  map[string][]string
	publishedIDs   []string
	recordedFails  map[string]string
	releasedTokens map[string][]string
	claimErr       error
	markErr        error
	recordErr      error
	releaseErr     error
	onClaim        func()
}

func newMockStore(events []Event) *mockStore {
	return &mockStore{
		events:         events,
		claimedTokens:  make(map[string][]string),
		recordedFails:  make(map[string]string),
		releasedTokens: make(map[string][]string),
	}
}

func (m *mockStore) ClaimBatch(ctx context.Context, claimToken string, limit int, lease time.Duration) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.onClaim != nil {
		m.onClaim()
	}
	if m.claimErr != nil {
		return nil, m.claimErr
	}
	if len(m.events) == 0 {
		return nil, nil
	}
	n := limit
	if len(m.events) < n {
		n = len(m.events)
	}
	batch := make([]Event, n)
	copy(batch, m.events[:n])
	m.events = m.events[n:]
	ids := make([]string, len(batch))
	for i, e := range batch {
		ids[i] = e.ID
	}
	m.claimedTokens[claimToken] = ids
	return batch, nil
}

func (m *mockStore) MarkPublished(ctx context.Context, claimToken string, ids []string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.markErr != nil {
		return 0, m.markErr
	}
	m.publishedIDs = append(m.publishedIDs, ids...)
	return int64(len(ids)), nil
}

func (m *mockStore) RecordFailure(ctx context.Context, claimToken, id, cause string, maxAttempts int, backoff time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recordErr != nil {
		return m.recordErr
	}
	m.recordedFails[id] = cause
	return nil
}

func (m *mockStore) Release(ctx context.Context, claimToken string, ids []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.releaseErr != nil {
		return m.releaseErr
	}
	m.releasedTokens[claimToken] = append(m.releasedTokens[claimToken], ids...)
	return nil
}

type mockPublisher struct {
	mu         sync.Mutex
	published  []Event
	publishFn  func(ctx context.Context, e Event) error
	publishErr error
}

func (p *mockPublisher) Publish(ctx context.Context, e Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.publishFn != nil {
		return p.publishFn(ctx, e)
	}
	if p.publishErr != nil {
		return p.publishErr
	}
	p.published = append(p.published, e)
	return nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// U1: Backoff grows exponentially up to the cap without overflowing at attempt 63
// U2: Invalid config (batch 0, lease <= 0, base > max, attempts < 1) is rejected
func TestNewRelay_InvalidConfig(t *testing.T) {
	t.Parallel()
	store := newMockStore(nil)
	pub := &mockPublisher{}
	logger := testLogger()

	validCfg := RelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		Lease:        30 * time.Second,
		BaseBackoff:  1 * time.Second,
		MaxBackoff:   5 * time.Minute,
		MaxAttempts:  5,
	}

	t.Run("nil store", func(t *testing.T) {
		r, err := NewRelay(nil, pub, validCfg, logger)
		assert.Nil(t, r)
		assert.ErrorIs(t, err, ErrNilStore)
	})

	t.Run("nil publisher", func(t *testing.T) {
		r, err := NewRelay(store, nil, validCfg, logger)
		assert.Nil(t, r)
		assert.ErrorIs(t, err, ErrNilPublisher)
	})

	t.Run("nil logger", func(t *testing.T) {
		r, err := NewRelay(store, pub, validCfg, nil)
		assert.Nil(t, r)
		assert.ErrorIs(t, err, ErrNilLogger)
	})

	t.Run("batch size 0", func(t *testing.T) {
		cfg := validCfg
		cfg.BatchSize = 0
		_, err := NewRelay(store, pub, cfg, logger)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("batch size > 1000", func(t *testing.T) {
		cfg := validCfg
		cfg.BatchSize = 1001
		_, err := NewRelay(store, pub, cfg, logger)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("poll interval 0", func(t *testing.T) {
		cfg := validCfg
		cfg.PollInterval = 0
		_, err := NewRelay(store, pub, cfg, logger)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("lease <= 0", func(t *testing.T) {
		cfg := validCfg
		cfg.Lease = 0
		_, err := NewRelay(store, pub, cfg, logger)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("base backoff <= 0", func(t *testing.T) {
		cfg := validCfg
		cfg.BaseBackoff = 0
		_, err := NewRelay(store, pub, cfg, logger)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("base > max backoff", func(t *testing.T) {
		cfg := validCfg
		cfg.BaseBackoff = 10 * time.Minute
		cfg.MaxBackoff = 5 * time.Minute
		_, err := NewRelay(store, pub, cfg, logger)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("max attempts < 1", func(t *testing.T) {
		cfg := validCfg
		cfg.MaxAttempts = 0
		_, err := NewRelay(store, pub, cfg, logger)
		assert.ErrorIs(t, err, ErrInvalidConfig)
	})

	t.Run("valid config succeeds", func(t *testing.T) {
		r, err := NewRelay(store, pub, validCfg, logger)
		require.NoError(t, err)
		assert.NotNil(t, r)
	})
}

// U3: Broker unavailable: batch released, RecordFailure never called, error wraps ErrBrokerUnavailable
func TestRunOnce_BrokerUnavailable(t *testing.T) {
	t.Parallel()
	events := []Event{
		{ID: "e1", AggregateType: "product", AggregateID: "p1", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e2", AggregateType: "product", AggregateID: "p2", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e3", AggregateType: "product", AggregateID: "p3", EventType: "product.changed", Payload: []byte("{}")},
	}
	store := newMockStore(events)
	pub := &mockPublisher{
		publishFn: func(ctx context.Context, e Event) error {
			if e.ID == "e2" {
				return appMessaging.ErrBrokerUnavailable
			}
			return nil
		},
	}
	cfg := RelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		Lease:        30 * time.Second,
		BaseBackoff:  1 * time.Second,
		MaxBackoff:   5 * time.Minute,
		MaxAttempts:  5,
	}
	relay, err := NewRelay(store, pub, cfg, testLogger())
	require.NoError(t, err)

	claimed, published, err := relay.RunOnce(context.Background())
	assert.Equal(t, 3, claimed)
	assert.Equal(t, 1, published)
	require.Error(t, err)
	assert.ErrorIs(t, err, appMessaging.ErrBrokerUnavailable)

	assert.Equal(t, []string{"e1"}, store.publishedIDs)
	var releasedAll []string
	for _, ids := range store.releasedTokens {
		releasedAll = append(releasedAll, ids...)
	}
	assert.ElementsMatch(t, []string{"e2", "e3"}, releasedAll)
	assert.Empty(t, store.recordedFails)
}

// U3b: Mixed batch (poison -> ok -> broker-down) correctly marks published and releases only remaining
func TestRunOnce_MixedBatch_PoisonOkBrokerDown(t *testing.T) {
	t.Parallel()
	events := []Event{
		{ID: "e1", AggregateType: "product", AggregateID: "p1", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e2", AggregateType: "product", AggregateID: "p2", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e3", AggregateType: "product", AggregateID: "p3", EventType: "product.changed", Payload: []byte("{}")},
	}
	store := newMockStore(events)
	errPoison := errors.New("malformed event schema")
	pub := &mockPublisher{
		publishFn: func(ctx context.Context, e Event) error {
			if e.ID == "e1" {
				return errPoison
			}
			if e.ID == "e3" {
				return appMessaging.ErrBrokerUnavailable
			}
			return nil
		},
	}
	cfg := RelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		Lease:        30 * time.Second,
		BaseBackoff:  1 * time.Second,
		MaxBackoff:   5 * time.Minute,
		MaxAttempts:  5,
	}
	relay, err := NewRelay(store, pub, cfg, testLogger())
	require.NoError(t, err)

	claimed, published, err := relay.RunOnce(context.Background())
	assert.Equal(t, 3, claimed)
	assert.Equal(t, 1, published)
	require.Error(t, err)
	assert.ErrorIs(t, err, appMessaging.ErrBrokerUnavailable)

	assert.Equal(t, []string{"e2"}, store.publishedIDs)
	assert.Equal(t, "malformed event schema", store.recordedFails["e1"])

	var releasedAll []string
	for _, ids := range store.releasedTokens {
		releasedAll = append(releasedAll, ids...)
	}
	assert.ElementsMatch(t, []string{"e3"}, releasedAll)
	assert.NotContains(t, releasedAll, "e2")
}

// U4: Poison event: RecordFailure for that event only, the others still published
func TestRunOnce_PoisonEvent(t *testing.T) {
	t.Parallel()
	events := []Event{
		{ID: "e1", AggregateType: "product", AggregateID: "p1", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e2", AggregateType: "product", AggregateID: "p2", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e3", AggregateType: "product", AggregateID: "p3", EventType: "product.changed", Payload: []byte("{}")},
	}
	store := newMockStore(events)
	errPoison := errors.New("malformed event payload schema")
	pub := &mockPublisher{
		publishFn: func(ctx context.Context, e Event) error {
			if e.ID == "e2" {
				return errPoison
			}
			return nil
		},
	}
	cfg := RelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		Lease:        30 * time.Second,
		BaseBackoff:  1 * time.Second,
		MaxBackoff:   5 * time.Minute,
		MaxAttempts:  5,
	}
	relay, err := NewRelay(store, pub, cfg, testLogger())
	require.NoError(t, err)

	claimed, published, err := relay.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, claimed)
	assert.Equal(t, 2, published)

	assert.ElementsMatch(t, []string{"e1", "e3"}, store.publishedIDs)
	assert.Equal(t, "malformed event payload schema", store.recordedFails["e2"])
	assert.Len(t, store.recordedFails, 1)
}

// U5: Cancelled mid-batch: only published ids marked, returns ctx.Err()
func TestRunOnce_Cancelled(t *testing.T) {
	t.Parallel()
	events := []Event{
		{ID: "e1", AggregateType: "product", AggregateID: "p1", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e2", AggregateType: "product", AggregateID: "p2", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e3", AggregateType: "product", AggregateID: "p3", EventType: "product.changed", Payload: []byte("{}")},
	}
	store := newMockStore(events)
	ctx, cancel := context.WithCancel(context.Background())

	pub := &mockPublisher{
		publishFn: func(pCtx context.Context, e Event) error {
			if e.ID == "e1" {
				cancel() // cancel after first event published
				return nil
			}
			return nil
		},
	}
	cfg := RelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		Lease:        30 * time.Second,
		BaseBackoff:  1 * time.Second,
		MaxBackoff:   5 * time.Minute,
		MaxAttempts:  5,
	}
	relay, err := NewRelay(store, pub, cfg, testLogger())
	require.NoError(t, err)

	claimed, published, err := relay.RunOnce(ctx)
	assert.Equal(t, 3, claimed)
	assert.Equal(t, 1, published)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, []string{"e1"}, store.publishedIDs)
	assert.Equal(t, []string{"e2", "e3"}, releasedIDs(store))
}

func releasedIDs(m *mockStore) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for _, released := range m.releasedTokens {
		ids = append(ids, released...)
	}
	return ids
}

// U5c: On cancellation, mark and release are both attempted even when one fails or has nothing to do
func TestRunOnce_Cancelled_MarkAndReleaseBothAttempted(t *testing.T) {
	t.Parallel()
	cfg := RelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		Lease:        30 * time.Second,
		BaseBackoff:  1 * time.Second,
		MaxBackoff:   5 * time.Minute,
		MaxAttempts:  5,
	}
	newEvents := func() []Event {
		return []Event{{ID: "e1"}, {ID: "e2"}, {ID: "e3"}}
	}

	t.Run("mark fails, release still runs", func(t *testing.T) {
		t.Parallel()
		store := newMockStore(newEvents())
		store.markErr = errors.New("mark db down")
		ctx, cancel := context.WithCancel(context.Background())
		pub := &mockPublisher{publishFn: func(_ context.Context, e Event) error {
			if e.ID == "e1" {
				cancel()
			}
			return nil
		}}
		relay, err := NewRelay(store, pub, cfg, testLogger())
		require.NoError(t, err)

		_, _, err = relay.RunOnce(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		assert.ErrorIs(t, err, store.markErr)
		assert.Equal(t, []string{"e2", "e3"}, releasedIDs(store))
	})

	t.Run("nothing published, release fails", func(t *testing.T) {
		t.Parallel()
		store := newMockStore(newEvents())
		store.releaseErr = errors.New("release db down")
		ctx, cancel := context.WithCancel(context.Background())
		pub := &mockPublisher{publishFn: func(_ context.Context, _ Event) error {
			cancel()
			return ctx.Err()
		}}
		relay, err := NewRelay(store, pub, cfg, testLogger())
		require.NoError(t, err)

		_, published, err := relay.RunOnce(ctx)
		assert.Equal(t, 0, published)
		assert.ErrorIs(t, err, context.Canceled)
		assert.ErrorIs(t, err, store.releaseErr)
		assert.Empty(t, store.publishedIDs)
		assert.Empty(t, store.recordedFails)
	})
}

// U5b: Cancellation during publish returns ctx.Err() and never records failure (Gotcha #17)
func TestRunOnce_CancellationDuringPublish_DoesNotRecordFailure(t *testing.T) {
	t.Parallel()
	events := []Event{
		{ID: "e1", AggregateType: "product", AggregateID: "p1", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e2", AggregateType: "product", AggregateID: "p2", EventType: "product.changed", Payload: []byte("{}")},
	}
	store := newMockStore(events)
	ctx, cancel := context.WithCancel(context.Background())

	pub := &mockPublisher{
		publishFn: func(pCtx context.Context, e Event) error {
			if e.ID == "e1" {
				return nil
			}
			if e.ID == "e2" {
				cancel()
				return ctx.Err()
			}
			return nil
		},
	}
	cfg := RelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		Lease:        30 * time.Second,
		BaseBackoff:  1 * time.Second,
		MaxBackoff:   5 * time.Minute,
		MaxAttempts:  5,
	}
	relay, err := NewRelay(store, pub, cfg, testLogger())
	require.NoError(t, err)

	claimed, published, err := relay.RunOnce(ctx)
	assert.Equal(t, 2, claimed)
	assert.Equal(t, 1, published)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, []string{"e1"}, store.publishedIDs)
	assert.Equal(t, []string{"e2"}, releasedIDs(store))
	assert.Empty(t, store.recordedFails)
}

func TestRunOnce_RecordFailureError_MultiEvent_MarksPublishedAndReturnsError(t *testing.T) {
	t.Parallel()
	events := []Event{
		{ID: "e1", AggregateType: "product", AggregateID: "p1", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e2", AggregateType: "product", AggregateID: "p2", EventType: "product.changed", Payload: []byte("{}")},
		{ID: "e3", AggregateType: "product", AggregateID: "p3", EventType: "product.changed", Payload: []byte("{}")},
	}
	store := newMockStore(events)
	store.recordErr = errors.New("database error during record failure")
	pub := &mockPublisher{
		publishFn: func(ctx context.Context, e Event) error {
			if e.ID == "e2" {
				return errors.New("per-event poison error on e2")
			}
			return nil
		},
	}

	cfg := RelayConfig{
		BatchSize:    10,
		PollInterval: 100 * time.Millisecond,
		Lease:        30 * time.Second,
		BaseBackoff:  1 * time.Second,
		MaxBackoff:   5 * time.Minute,
		MaxAttempts:  5,
	}
	relay, err := NewRelay(store, pub, cfg, testLogger())
	require.NoError(t, err)

	claimed, published, err := relay.RunOnce(context.Background())
	assert.Equal(t, 3, claimed)
	assert.Equal(t, 2, published)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database error during record failure")

	assert.Equal(t, []string{"e1", "e3"}, store.publishedIDs)
}
