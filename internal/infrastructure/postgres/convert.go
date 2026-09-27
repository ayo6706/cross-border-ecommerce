package postgres

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/jackc/pgx/v5/pgtype"
)

const maxListLimit = 1000

// toInt32 converts an int to int32, failing instead of silently truncating.
func toInt32(n int) (int32, error) {
	if n > math.MaxInt32 || n < math.MinInt32 {
		return 0, fmt.Errorf("integer value %d overflows int32", n)
	}
	return int32(n), nil
}

// listLimit applies the default for non-positive limits and caps the result at
// maxListLimit, so the returned value always fits in int32.
func listLimit(limit, defaultLimit int) int32 {
	if limit <= 0 {
		limit = defaultLimit
	}
	return int32(min(limit, maxListLimit))
}

// runCounters holds ingestion run counters converted for the int4 columns.
type runCounters struct {
	seen, newRecords, changed, unchanged, failed int32
}

func toRunCounters(m ingestion.BatchMetrics) (runCounters, error) {
	var c runCounters
	fields := []struct {
		dst  *int32
		val  int
		name string
	}{
		{&c.seen, m.Seen, "seen"},
		{&c.newRecords, m.New, "new"},
		{&c.changed, m.Changed, "changed"},
		{&c.unchanged, m.Unchanged, "unchanged"},
		{&c.failed, m.Failed, "failed"},
	}
	for _, f := range fields {
		v, err := toInt32(f.val)
		if err != nil {
			return runCounters{}, fmt.Errorf("records %s counter: %w", f.name, err)
		}
		*f.dst = v
	}
	return c, nil
}

// toTimestamptz converts a *time.Time to pgtype.Timestamptz.
func toTimestamptz(t *time.Time) pgtype.Timestamptz {
	if t == nil || t.IsZero() {
		return pgtype.Timestamptz{Valid: false}
	}
	return pgtype.Timestamptz{Time: t.UTC(), Valid: true}
}

var ErrZeroTimestamp = errors.New("timestamp cannot be zero")

// requiredTimestamptz converts a time.Time to a valid UTC pgtype.Timestamptz or returns an error on zero time.
func requiredTimestamptz(t time.Time) (pgtype.Timestamptz, error) {
	if t.IsZero() {
		return pgtype.Timestamptz{}, ErrZeroTimestamp
	}
	return pgtype.Timestamptz{Time: t.UTC(), Valid: true}, nil
}

// requiredAuditTimestamps converts the created_at/updated_at pair every aggregate row carries.
func requiredAuditTimestamps(createdAt, updatedAt time.Time) (created, updated pgtype.Timestamptz, err error) {
	if created, err = requiredTimestamptz(createdAt); err != nil {
		return created, updated, fmt.Errorf("created_at: %w", err)
	}
	if updated, err = requiredTimestamptz(updatedAt); err != nil {
		return created, updated, fmt.Errorf("updated_at: %w", err)
	}
	return created, updated, nil
}

// fromTimestamptz converts a pgtype.Timestamptz to *time.Time in UTC.
func fromTimestamptz(tz pgtype.Timestamptz) *time.Time {
	if !tz.Valid {
		return nil
	}
	utc := tz.Time.UTC()
	return &utc
}
