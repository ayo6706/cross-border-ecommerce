package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/stretchr/testify/require"
)

// A guarded write that matches no row must say why: the run has no processing state, or
// another worker's lease refused it. Neither may look like success.
func TestRunProcessingRepository_GuardFailures_Live(t *testing.T) {
	pool, runRepo := setupLiveIngestionDB(t)
	ctx := context.Background()
	repo, err := postgres.NewRunProcessingRepository(pool)
	require.NoError(t, err)

	newUUID := func() string {
		id, err := uuid.NewString()
		require.NoError(t, err)
		return id
	}
	// claimedRun returns a run whose processing state is leased to a fresh token for a minute.
	claimedRun := func(t *testing.T) (runID, token string) {
		run, err := ingestion.NewRun("", newTestSource(t, pool), "")
		require.NoError(t, err)
		require.NoError(t, runRepo.CreateRun(ctx, run))
		_, err = repo.EnsureExists(ctx, run.ID)
		require.NoError(t, err)
		token = newUUID()
		_, err = repo.ClaimSpecific(ctx, run.ID, token, time.Minute)
		require.NoError(t, err)
		return run.ID, token
	}

	t.Run("ClaimSpecific_UnknownRun_NotFound", func(t *testing.T) {
		_, err := repo.ClaimSpecific(ctx, newUUID(), newUUID(), time.Minute)
		require.ErrorIs(t, err, ingestion.ErrRunProcessingNotFound)
	})

	t.Run("ClaimSpecific_ActiveLease_LeaseHeld", func(t *testing.T) {
		runID, _ := claimedRun(t)
		_, err := repo.ClaimSpecific(ctx, runID, newUUID(), time.Minute)
		require.ErrorIs(t, err, ingestion.ErrLeaseHeld)
	})

	t.Run("ResetFromStart_UnknownRun_NotFound", func(t *testing.T) {
		_, err := repo.ResetFromStart(ctx, newUUID())
		require.ErrorIs(t, err, ingestion.ErrRunProcessingNotFound)
	})

	t.Run("ResetFromStart_ActiveLease_LeaseHeld", func(t *testing.T) {
		runID, _ := claimedRun(t)
		_, err := repo.ResetFromStart(ctx, runID)
		require.ErrorIs(t, err, ingestion.ErrLeaseHeld)
	})

	t.Run("Fail_UnknownRun_NotFound", func(t *testing.T) {
		err := repo.Fail(ctx, newUUID(), newUUID(), "boom")
		require.ErrorIs(t, err, ingestion.ErrRunProcessingNotFound)
	})

	t.Run("Fail_WithLostLease_LeaseLost", func(t *testing.T) {
		runID, owner := claimedRun(t)
		err := repo.Fail(ctx, runID, newUUID(), "boom")
		require.ErrorIs(t, err, ingestion.ErrLeaseLost, "a worker that lost its lease must not report the run failed")

		state, err := repo.GetByID(ctx, runID)
		require.NoError(t, err)
		require.Equal(t, ingestion.ProcessingRunning, state.Status)
		require.NotNil(t, state.ClaimToken)
		require.Equal(t, owner, *state.ClaimToken)
	})
}
