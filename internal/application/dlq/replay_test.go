package dlq_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memTx is an in-memory TxRunner. Writes made inside fn are applied only if fn returns nil,
// matching a PostgreSQL transaction that rolls back on error.
type memTx struct {
	sources   map[string]dlq.ReplaySource
	replayed  map[string]string // dlq id -> replay outbox id
	outbox    []dlq.ReplayEvent
	outboxErr error
}

func newMemTx(sources map[string]dlq.ReplaySource) *memTx {
	return &memTx{sources: sources, replayed: map[string]string{}}
}

func (m *memTx) WithinTx(_ context.Context, fn func(dlq.TxRepos) error) error {
	view := &memTxView{parent: m, replayed: map[string]string{}}
	if err := fn(dlq.TxRepos{DLQ: view, Outbox: view}); err != nil {
		return err
	}
	for id, outboxID := range view.replayed {
		m.replayed[id] = outboxID
	}
	m.outbox = append(m.outbox, view.outbox...)
	return nil
}

type memTxView struct {
	parent   *memTx
	replayed map[string]string
	outbox   []dlq.ReplayEvent
}

func (v *memTxView) GetReplaySource(_ context.Context, id string) (dlq.ReplaySource, error) {
	src, ok := v.parent.sources[id]
	if !ok {
		return dlq.ReplaySource{}, dlq.ErrDLQNotFound
	}
	return src, nil
}

func (v *memTxView) MarkReplayed(_ context.Context, id, outboxID string) (bool, error) {
	if _, done := v.parent.replayed[id]; done {
		return false, nil
	}
	if _, done := v.replayed[id]; done {
		return false, nil
	}
	v.replayed[id] = outboxID
	return true, nil
}

func (v *memTxView) CreateReplayEvent(_ context.Context, e dlq.ReplayEvent) error {
	if v.parent.outboxErr != nil {
		return v.parent.outboxErr
	}
	v.outbox = append(v.outbox, e)
	return nil
}

func replayableSource() dlq.ReplaySource {
	return dlq.ReplaySource{
		ConsumerGroup: "product-classifier",
		EventID:       "evt-1",
		EventType:     "product.changed",
		AggregateType: "Product",
		AggregateID:   "prod-1",
		Payload:       []byte(`{"id":"prod-1"}`),
		Replayable:    true,
	}
}

func newService(t *testing.T, tx dlq.TxRunner) *dlq.ReplayService {
	t.Helper()
	svc, err := dlq.NewReplayService(tx)
	require.NoError(t, err)
	return svc
}

func TestNewReplayService_NilTx(t *testing.T) {
	_, err := dlq.NewReplayService(nil)
	require.Error(t, err)
}

func TestReplayService_Replay(t *testing.T) {
	ctx := context.Background()

	t.Run("publishes original event to its group and marks replayed", func(t *testing.T) {
		tx := newMemTx(map[string]dlq.ReplaySource{"d1": replayableSource()})
		outboxID, err := newService(t, tx).Replay(ctx, "d1")
		require.NoError(t, err)

		require.Len(t, tx.outbox, 1)
		e := tx.outbox[0]
		assert.Equal(t, outboxID, e.ID)
		assert.Equal(t, "evt-1", e.ReplayOfEventID, "replay must reuse the original event_id")
		assert.Equal(t, "product-classifier", e.TargetGroup)
		assert.Equal(t, "product.changed", e.EventType)
		assert.Equal(t, "Product", e.AggregateType)
		assert.Equal(t, "prod-1", e.AggregateID)
		assert.Equal(t, []byte(`{"id":"prod-1"}`), e.Payload)
		assert.Equal(t, outboxID, tx.replayed["d1"])
	})

	t.Run("second replay is refused and writes nothing", func(t *testing.T) {
		tx := newMemTx(map[string]dlq.ReplaySource{"d1": replayableSource()})
		svc := newService(t, tx)
		_, err := svc.Replay(ctx, "d1")
		require.NoError(t, err)

		_, err = svc.Replay(ctx, "d1")
		require.ErrorIs(t, err, dlq.ErrAlreadyReplayed)
		assert.Len(t, tx.outbox, 1, "the losing replay's outbox write must roll back")
	})

	t.Run("not replayable is refused before any write", func(t *testing.T) {
		src := replayableSource()
		src.Replayable = false
		tx := newMemTx(map[string]dlq.ReplaySource{"d1": src})

		_, err := newService(t, tx).Replay(ctx, "d1")
		require.ErrorIs(t, err, dlq.ErrNotReplayable)
		assert.Empty(t, tx.outbox)
		assert.Empty(t, tx.replayed)
	})

	t.Run("unknown id", func(t *testing.T) {
		_, err := newService(t, newMemTx(nil)).Replay(ctx, "missing")
		require.ErrorIs(t, err, dlq.ErrDLQNotFound)
	})

	t.Run("empty id", func(t *testing.T) {
		_, err := newService(t, newMemTx(nil)).Replay(ctx, "  ")
		require.ErrorIs(t, err, dlq.ErrInvalidDLQID)
	})

	t.Run("outbox failure leaves message dead", func(t *testing.T) {
		tx := newMemTx(map[string]dlq.ReplaySource{"d1": replayableSource()})
		tx.outboxErr = errors.New("connection reset")

		_, err := newService(t, tx).Replay(ctx, "d1")
		require.ErrorIs(t, err, tx.outboxErr)
		assert.Empty(t, tx.replayed)
	})
}
