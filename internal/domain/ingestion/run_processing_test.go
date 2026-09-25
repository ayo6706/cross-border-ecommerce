package ingestion

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunProcessing_Transitions(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	runID := "9f81a7b8-1b7c-4824-9b2f-2d74a2ff7ea4"

	t.Run("NewRunProcessing initializes with PENDING status", func(t *testing.T) {
		rp, err := NewRunProcessing(runID, now)
		require.NoError(t, err)
		assert.Equal(t, runID, rp.RunID)
		assert.Equal(t, ProcessingPending, rp.Status)
		assert.Equal(t, 0, rp.RecordsSeen)
		assert.Nil(t, rp.ClaimToken)
	})

	t.Run("Claim transitions PENDING to RUNNING and sets lease", func(t *testing.T) {
		rp, err := NewRunProcessing(runID, now)
		require.NoError(t, err)

		token := "claim-1"
		leaseDur := 30 * time.Second
		err = rp.Claim(token, leaseDur, now)
		require.NoError(t, err)

		assert.Equal(t, ProcessingRunning, rp.Status)
		require.NotNil(t, rp.ClaimToken)
		assert.Equal(t, token, *rp.ClaimToken)
		require.NotNil(t, rp.LeaseExpiresAt)
		assert.Equal(t, now.Add(leaseDur), *rp.LeaseExpiresAt)
	})

	t.Run("Claim fails if already RUNNING with unexpired lease", func(t *testing.T) {
		rp, err := NewRunProcessing(runID, now)
		require.NoError(t, err)

		require.NoError(t, rp.Claim("claim-1", 30*time.Second, now))

		err = rp.Claim("claim-2", 30*time.Second, now.Add(10*time.Second))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "active lease")
	})

	t.Run("Claim succeeds on RUNNING if lease is expired", func(t *testing.T) {
		rp, err := NewRunProcessing(runID, now)
		require.NoError(t, err)

		require.NoError(t, rp.Claim("claim-1", 30*time.Second, now))

		err = rp.Claim("claim-2", 30*time.Second, now.Add(35*time.Second))
		require.NoError(t, err)
		assert.Equal(t, "claim-2", *rp.ClaimToken)
	})

	t.Run("RecordProgress updates counters and cursor", func(t *testing.T) {
		rp, err := NewRunProcessing(runID, now)
		require.NoError(t, err)
		require.NoError(t, rp.Claim("claim-1", 30*time.Second, now))

		cursor := "raw-1"
		err = rp.RecordProgress("claim-1", 1, 1, 0, 0, 0, &cursor, 30*time.Second, now.Add(5*time.Second))
		require.NoError(t, err)

		assert.Equal(t, 1, rp.RecordsSeen)
		assert.Equal(t, 1, rp.RecordsNew)
		require.NotNil(t, rp.CursorRawRecordID)
		assert.Equal(t, cursor, *rp.CursorRawRecordID)
	})

	t.Run("RecordProgress fails with invalid claim token", func(t *testing.T) {
		rp, err := NewRunProcessing(runID, now)
		require.NoError(t, err)
		require.NoError(t, rp.Claim("claim-1", 30*time.Second, now))

		cursor := "raw-1"
		err = rp.RecordProgress("wrong-token", 1, 1, 0, 0, 0, &cursor, 30*time.Second, now.Add(5*time.Second))
		require.Error(t, err)
	})

	t.Run("Complete transitions RUNNING to COMPLETED", func(t *testing.T) {
		rp, err := NewRunProcessing(runID, now)
		require.NoError(t, err)
		require.NoError(t, rp.Claim("claim-1", 30*time.Second, now))

		err = rp.Complete("claim-1", now.Add(10*time.Second))
		require.NoError(t, err)
		assert.Equal(t, ProcessingCompleted, rp.Status)
		assert.Nil(t, rp.ClaimToken)
		assert.Nil(t, rp.LeaseExpiresAt)
		assert.NotNil(t, rp.CompletedAt)
	})

	t.Run("Fail transitions to FAILED with error summary", func(t *testing.T) {
		rp, err := NewRunProcessing(runID, now)
		require.NoError(t, err)
		require.NoError(t, rp.Claim("claim-1", 30*time.Second, now))

		err = rp.Fail("claim-1", "normalization error budget exceeded", now.Add(10*time.Second))
		require.NoError(t, err)
		assert.Equal(t, ProcessingFailed, rp.Status)
		assert.Equal(t, "normalization error budget exceeded", rp.ErrorSummary)
	})

	t.Run("Restart resets cursor and counters", func(t *testing.T) {
		rp, err := NewRunProcessing(runID, now)
		require.NoError(t, err)
		require.NoError(t, rp.Claim("claim-1", 30*time.Second, now))
		cursor := "raw-10"
		require.NoError(t, rp.RecordProgress("claim-1", 10, 5, 3, 2, 0, &cursor, 30*time.Second, now))
		require.NoError(t, rp.Complete("claim-1", now.Add(10*time.Second)))

		err = rp.Restart(now.Add(20 * time.Second))
		require.NoError(t, err)
		assert.Equal(t, ProcessingPending, rp.Status)
		assert.Equal(t, 0, rp.RecordsSeen)
		assert.Equal(t, 0, rp.RecordsNew)
		assert.Nil(t, rp.CursorRawRecordID)
	})
}
