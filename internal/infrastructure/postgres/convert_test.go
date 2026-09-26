package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequiredTimestamptz(t *testing.T) {
	t.Parallel()

	t.Run("zero time returns ErrZeroTimestamp", func(t *testing.T) {
		tz, err := requiredTimestamptz(time.Time{})
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrZeroTimestamp))
		assert.False(t, tz.Valid)
	})

	t.Run("valid time returns UTC timestamptz", func(t *testing.T) {
		loc, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)

		localTime := time.Date(2026, 9, 26, 12, 0, 0, 0, loc)
		tz, err := requiredTimestamptz(localTime)
		require.NoError(t, err)
		assert.True(t, tz.Valid)
		assert.Equal(t, time.UTC, tz.Time.Location())
		assert.True(t, localTime.Equal(tz.Time))
	})
}

func TestToTimestamptz(t *testing.T) {
	t.Parallel()

	t.Run("nil time returns invalid timestamptz", func(t *testing.T) {
		tz := toTimestamptz(nil)
		assert.False(t, tz.Valid)
	})

	t.Run("valid time returns valid UTC timestamptz", func(t *testing.T) {
		now := time.Now()
		tz := toTimestamptz(&now)
		assert.True(t, tz.Valid)
		assert.Equal(t, time.UTC, tz.Time.Location())
	})
}

func TestFromTimestamptz(t *testing.T) {
	t.Parallel()

	t.Run("invalid returns nil", func(t *testing.T) {
		assert.Nil(t, fromTimestamptz(pgtype.Timestamptz{Valid: false}))
	})

	t.Run("valid returns UTC pointer", func(t *testing.T) {
		now := time.Date(2026, 9, 26, 14, 0, 0, 0, time.UTC)
		res := fromTimestamptz(pgtype.Timestamptz{Time: now, Valid: true})
		require.NotNil(t, res)
		assert.Equal(t, now, *res)
	})
}
