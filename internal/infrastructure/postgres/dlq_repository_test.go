package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDLQRepository_ConstructorValidation(t *testing.T) {
	t.Parallel()

	repo, err := postgres.NewDLQRepository(nil)
	assert.Error(t, err)
	assert.Nil(t, repo)
}

func setupLiveDLQDB(t *testing.T) (*pgxpool.Pool, *postgres.DLQRepository) {
	t.Helper()

	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		t.Skip("skipping live database test: TEST_DATABASE_URL not set")
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, connStr,
		postgres.WithConnectTimeout(3*time.Second),
		postgres.WithMaxConns(20),
		postgres.WithMinConns(2),
	)
	if err != nil {
		t.Skipf("skipping live database test: unable to connect to %s: %v", connStr, err)
		return nil, nil
	}
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_, _ = pool.Exec(cleanCtx, "TRUNCATE dlq_messages, outbox_events CASCADE")
		pool.Close()
	})

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("failed to initialize migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("failed to run migrations: %v", err)
	}

	repo, err := postgres.NewDLQRepository(pool)
	if err != nil {
		t.Fatalf("failed to initialize DLQ repository: %v", err)
	}

	return pool, repo
}

// dlqRow is the persisted state of a dlq_messages row, read with plain SQL.
type dlqRow struct {
	Status         string
	EventID        *string
	EventType      string
	AggregateType  string
	AggregateID    string
	CorrelationID  string
	Payload        []byte
	FailureClass   string
	LastError      string
	Stack          *string
	Attempts       int
	ConsumerName   string
	Replayable     bool
	ReplayOutboxID *string
	ReplayedAt     *time.Time
}

// insertDLQ inserts msg and returns the id PostgreSQL assigned to it.
func insertDLQ(t *testing.T, pool *pgxpool.Pool, repo *postgres.DLQRepository, msg dlq.Message) string {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, repo.Insert(ctx, msg))
	var id string
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT id::text FROM dlq_messages WHERE stream = $1 AND consumer_group = $2 AND stream_message_id = $3",
		msg.Stream, msg.ConsumerGroup, msg.StreamMessageID).Scan(&id))
	return id
}

func readDLQ(t *testing.T, pool *pgxpool.Pool, id string) dlqRow {
	t.Helper()
	var r dlqRow
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT status, event_id, event_type, aggregate_type, aggregate_id, correlation_id, payload,
		       failure_class, last_error, stack, attempts, consumer_name, replayable,
		       replay_outbox_id::text, replayed_at
		FROM dlq_messages WHERE id = $1::uuid`, id).Scan(
		&r.Status, &r.EventID, &r.EventType, &r.AggregateType, &r.AggregateID, &r.CorrelationID, &r.Payload,
		&r.FailureClass, &r.LastError, &r.Stack, &r.Attempts, &r.ConsumerName, &r.Replayable,
		&r.ReplayOutboxID, &r.ReplayedAt))
	return r
}

// replayableMessage returns a well-formed dead-letter message for the given stream message ID.
func replayableMessage(t *testing.T, streamMsgID string) dlq.Message {
	t.Helper()
	eventID, err := uuid.NewString()
	require.NoError(t, err)
	return dlq.Message{
		Stream:          "catalog.products",
		ConsumerGroup:   "product-classifier",
		StreamMessageID: streamMsgID,
		EventID:         &eventID,
		EventType:       "ProductCatalogUpdated",
		AggregateType:   "Product",
		AggregateID:     "prod-1",
		Payload:         []byte(`{"product_id":"prod-1"}`),
		FailureClass:    dlq.ClassPermanent,
		LastError:       "contract violation",
		Attempts:        1,
		ConsumerName:    "worker-1",
		FirstFailedAt:   time.Now(),
	}
}

func TestDLQ_Live(t *testing.T) {
	pool, repo := setupLiveDLQDB(t)
	ctx := context.Background()

	t.Run("Insert_PersistsAllFields", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		msg := replayableMessage(t, "1710000000000-0")
		msg.CorrelationID = "corr-xyz"
		msg.FailureClass = dlq.ClassRetriesExhausted
		msg.LastError = "upstream connection refused"
		msg.Stack = "goroutine 1 [running]:\n..."
		msg.Attempts = 5
		msg.EventCreatedAt = &now

		got := readDLQ(t, pool, insertDLQ(t, pool, repo, msg))

		require.NotNil(t, got.EventID)
		assert.Equal(t, *msg.EventID, *got.EventID)
		assert.Equal(t, msg.EventType, got.EventType)
		assert.Equal(t, msg.AggregateType, got.AggregateType)
		assert.Equal(t, msg.AggregateID, got.AggregateID)
		assert.Equal(t, msg.CorrelationID, got.CorrelationID)
		assert.Equal(t, msg.Payload, got.Payload)
		assert.Equal(t, string(dlq.ClassRetriesExhausted), got.FailureClass)
		assert.Equal(t, msg.LastError, got.LastError)
		require.NotNil(t, got.Stack)
		assert.Equal(t, msg.Stack, *got.Stack)
		assert.Equal(t, 5, got.Attempts)
		assert.Equal(t, "worker-1", got.ConsumerName)
		assert.Equal(t, "DEAD", got.Status)
		assert.True(t, got.Replayable)
		assert.Nil(t, got.ReplayedAt)
		assert.Nil(t, got.ReplayOutboxID)
	})

	t.Run("Insert_Idempotent_ConflictIgnored", func(t *testing.T) {
		msg := replayableMessage(t, "1710000000001-0")
		require.NoError(t, repo.Insert(ctx, msg))

		msg.LastError = "second error"
		require.NoError(t, repo.Insert(ctx, msg), "a second insert of the same stream message is a no-op")

		var count int
		require.NoError(t, pool.QueryRow(ctx,
			"SELECT count(*) FROM dlq_messages WHERE stream = $1 AND consumer_group = $2 AND stream_message_id = $3",
			msg.Stream, msg.ConsumerGroup, msg.StreamMessageID).Scan(&count))
		assert.Equal(t, 1, count)
	})

	t.Run("Insert_Rejects_Invalid_Input", func(t *testing.T) {
		cases := map[string]func(*dlq.Message){
			"zero attempts":          func(m *dlq.Message) { m.Attempts = 0 },
			"empty stream":           func(m *dlq.Message) { m.Stream = "" },
			"empty group":            func(m *dlq.Message) { m.ConsumerGroup = "" },
			"empty stream msg id":    func(m *dlq.Message) { m.StreamMessageID = "" },
			"empty consumer name":    func(m *dlq.Message) { m.ConsumerName = "" },
			"empty failure class":    func(m *dlq.Message) { m.FailureClass = "" },
			"zero first failed at":   func(m *dlq.Message) { m.FirstFailedAt = time.Time{} },
			"stream over 128":        func(m *dlq.Message) { m.Stream = strings.Repeat("s", 129) },
			"group over 128":         func(m *dlq.Message) { m.ConsumerGroup = strings.Repeat("g", 129) },
			"consumer name over 128": func(m *dlq.Message) { m.ConsumerName = strings.Repeat("c", 129) },
		}
		for name, alter := range cases {
			t.Run(name, func(t *testing.T) {
				msg := replayableMessage(t, "1710000000008-0")
				alter(&msg)
				require.ErrorIs(t, repo.Insert(ctx, msg), dlq.ErrInvalidDLQInput,
					"identity fields are validated, never truncated")
			})
		}
	})

	t.Run("Insert_Sanitizes_Descriptive_Fields", func(t *testing.T) {
		msg := replayableMessage(t, "1710000000006-0")
		msg.EventType = strings.Repeat("X", 200)
		msg.AggregateType = strings.Repeat("Y", 200)
		msg.AggregateID = strings.Repeat("Z", 300)
		msg.CorrelationID = strings.Repeat("C", 300)
		msg.Payload = []byte("\xff\xfe\x00\x01binary payload")
		msg.FailureClass = dlq.ClassCorruptEnvelope
		msg.LastError = "invalid utf8: \xff\xfe and nul byte: \x00 inside error"
		msg.Stack = "stack with \x00 nul byte and \xff\xfe invalid utf8"

		got := readDLQ(t, pool, insertDLQ(t, pool, repo, msg))
		assert.LessOrEqual(t, len([]rune(got.EventType)), 64)
		assert.Equal(t, "invalid utf8: � and nul byte:  inside error", got.LastError,
			"invalid bytes become U+FFFD and NUL bytes are dropped")
		require.NotNil(t, got.Stack)
		assert.NotContains(t, *got.Stack, "\x00")
		assert.Equal(t, msg.Payload, got.Payload, "payload is stored byte for byte")
		assert.False(t, got.Replayable)
	})

	t.Run("Replay_HappyPath", func(t *testing.T) {
		msg := replayableMessage(t, "1710000000002-0")
		dlqID := insertDLQ(t, pool, repo, msg)

		outboxID, err := repo.Replay(ctx, dlqID)
		require.NoError(t, err)

		got := readDLQ(t, pool, dlqID)
		assert.Equal(t, "REPLAYED", got.Status)
		require.NotNil(t, got.ReplayedAt)
		require.NotNil(t, got.ReplayOutboxID)
		assert.Equal(t, outboxID, *got.ReplayOutboxID)

		var targetGroup, replayOfEventID, outboxStatus string
		var outboxPayload []byte
		require.NoError(t, pool.QueryRow(ctx,
			"SELECT target_group, replay_of_event_id, status, payload FROM outbox_events WHERE id = $1",
			outboxID).Scan(&targetGroup, &replayOfEventID, &outboxStatus, &outboxPayload))
		assert.Equal(t, msg.ConsumerGroup, targetGroup)
		assert.Equal(t, *msg.EventID, replayOfEventID)
		assert.Equal(t, "PENDING", outboxStatus)
		assert.JSONEq(t, string(msg.Payload), string(outboxPayload))
	})

	t.Run("Replay_AlreadyReplayed", func(t *testing.T) {
		dlqID := insertDLQ(t, pool, repo, replayableMessage(t, "1710000000003-0"))

		_, err := repo.Replay(ctx, dlqID)
		require.NoError(t, err)

		_, err = repo.Replay(ctx, dlqID)
		require.ErrorIs(t, err, dlq.ErrAlreadyReplayed)
	})

	t.Run("Replay_NotReplayable_Corrupt", func(t *testing.T) {
		msg := replayableMessage(t, "1710000000004-0")
		msg.EventID = nil
		msg.EventType = ""
		msg.Payload = []byte("not valid json at all")
		msg.FailureClass = dlq.ClassCorruptEnvelope
		dlqID := insertDLQ(t, pool, repo, msg)

		_, err := repo.Replay(ctx, dlqID)
		require.ErrorIs(t, err, dlq.ErrNotReplayable)
	})

	t.Run("Replay_NotFound", func(t *testing.T) {
		nonExistentID, err := uuid.NewString()
		require.NoError(t, err)

		_, err = repo.Replay(ctx, nonExistentID)
		require.ErrorIs(t, err, dlq.ErrDLQNotFound)
	})

	t.Run("Replay_InvalidUUID", func(t *testing.T) {
		_, err := repo.Replay(ctx, "not-a-valid-uuid")
		require.ErrorIs(t, err, dlq.ErrInvalidDLQID)
	})

	t.Run("Replay_Concurrent_OnlyOneSucceeds", func(t *testing.T) {
		dlqID := insertDLQ(t, pool, repo, replayableMessage(t, "1710000000005-0"))

		var wg sync.WaitGroup
		var mu sync.Mutex
		successCount, conflictCount := 0, 0
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := repo.Replay(ctx, dlqID)
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					successCount++
				} else if errors.Is(err, dlq.ErrAlreadyReplayed) {
					conflictCount++
				}
			}()
		}
		wg.Wait()

		assert.Equal(t, 1, successCount, "exactly one replay must succeed")
		assert.Equal(t, 4, conflictCount, "all other concurrent replays must return ErrAlreadyReplayed")

		var outboxRows int
		require.NoError(t, pool.QueryRow(ctx,
			"SELECT count(*) FROM outbox_events WHERE replay_of_event_id = (SELECT event_id FROM dlq_messages WHERE id = $1::uuid)",
			dlqID).Scan(&outboxRows))
		assert.Equal(t, 1, outboxRows, "rolled-back replays must not leave outbox rows")
	})

	t.Run("Replay_NonUUID_EventID", func(t *testing.T) {
		msg := replayableMessage(t, "1710000000007-0")
		eventID := "order-42-custom-string"
		msg.EventID = &eventID
		dlqID := insertDLQ(t, pool, repo, msg)

		outboxID, err := repo.Replay(ctx, dlqID)
		require.NoError(t, err)

		var replayOf string
		require.NoError(t, pool.QueryRow(ctx,
			"SELECT replay_of_event_id FROM outbox_events WHERE id = $1", outboxID).Scan(&replayOf))
		assert.Equal(t, eventID, replayOf)
	})

	// Regression: event_id was truncated to 128, so a replay published a different event_id and
	// the idempotency guard (keyed on event_id, VARCHAR(256)) no longer recognised the event.
	t.Run("Replay_Preserves_Long_EventID_Verbatim", func(t *testing.T) {
		msg := replayableMessage(t, "1710000000009-0")
		eventID := strings.Repeat("e", 200)
		msg.EventID = &eventID
		dlqID := insertDLQ(t, pool, repo, msg)

		outboxID, err := repo.Replay(ctx, dlqID)
		require.NoError(t, err)

		var replayOf string
		require.NoError(t, pool.QueryRow(ctx,
			"SELECT replay_of_event_id FROM outbox_events WHERE id = $1", outboxID).Scan(&replayOf))
		assert.Equal(t, eventID, replayOf, "replay must reuse the exact event_id so the idempotency guard applies")
	})

	// Regression: sanitised or truncated replay fields were replayed as if they were the original.
	t.Run("Replay_Refused_When_Replayed_Fields_Were_Altered", func(t *testing.T) {
		cases := []struct {
			name  string
			alter func(*dlq.Message)
		}{
			{"event_id over 256", func(m *dlq.Message) { s := strings.Repeat("e", 257); m.EventID = &s }},
			{"event_id invalid utf8", func(m *dlq.Message) { s := "evt-\xff"; m.EventID = &s }},
			{"event_type over 64", func(m *dlq.Message) { m.EventType = strings.Repeat("t", 65) }},
			{"aggregate_type over 64", func(m *dlq.Message) { m.AggregateType = strings.Repeat("a", 65) }},
			{"aggregate_id over 128", func(m *dlq.Message) { m.AggregateID = strings.Repeat("a", 129) }},
		}
		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				msg := replayableMessage(t, fmt.Sprintf("1710000000010-%d", i))
				tc.alter(&msg)
				dlqID := insertDLQ(t, pool, repo, msg)

				_, err := repo.Replay(ctx, dlqID)
				require.ErrorIs(t, err, dlq.ErrNotReplayable)
			})
		}
	})
}
