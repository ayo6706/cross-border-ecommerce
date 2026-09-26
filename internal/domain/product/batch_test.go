package product_test

import (
	"testing"
	"time"

	domainProduct "github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecideBatch_Empty(t *testing.T) {
	plan, err := domainProduct.DecideBatch(nil, nil, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, 0, plan.RecordsSeen)
	assert.Empty(t, plan.ProductsToInsert)
	assert.Empty(t, plan.ProductVersionsToInsert)
}

func TestDecideBatch_ZeroNowReturnsError(t *testing.T) {
	_, err := domainProduct.DecideBatch(nil, nil, time.Time{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "now timestamp cannot be zero")
}

func TestDecideBatch_AllNew(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	srcUpdated := now.Add(-time.Hour)

	incoming := []domainProduct.BatchIncomingRecord{
		{
			RawRecordID:       "raw-1",
			SourceID:          "src-1",
			ExternalProductID: "ext-1",
			Normalized: &domainProduct.NormalizedProduct{
				CanonicalName: "Product 1",
				Description:   "Description 1",
				Brand:         "Brand 1",
				OriginCountry: "US",
				Attributes:    map[string]string{"color": "red"},
			},
			Fingerprint:     "fp-1",
			SourceUpdatedAt: &srcUpdated,
			ReceivedAt:      now,
			IngestionRunID:  "run-1",
		},
		{
			RawRecordID:       "raw-2",
			SourceID:          "src-1",
			ExternalProductID: "ext-2",
			Normalized: &domainProduct.NormalizedProduct{
				CanonicalName: "Product 2",
				Description:   "Description 2",
				Brand:         "Brand 2",
				OriginCountry: "CA",
				Attributes:    map[string]string{"color": "blue"},
			},
			Fingerprint:     "fp-2",
			SourceUpdatedAt: &srcUpdated,
			ReceivedAt:      now,
			IngestionRunID:  "run-1",
		},
	}

	plan, err := domainProduct.DecideBatch(nil, incoming, now)
	require.NoError(t, err)

	assert.Equal(t, 2, plan.RecordsSeen)
	assert.Equal(t, 2, plan.RecordsNew)
	assert.Equal(t, 0, plan.RecordsChanged)
	assert.Equal(t, 0, plan.RecordsUnchanged)
	assert.Len(t, plan.ProductsToInsert, 2)
	assert.Len(t, plan.ProductSourcesToInsert, 2)
	assert.Len(t, plan.ProductVersionsToInsert, 2)
	assert.Len(t, plan.ProductChangesToInsert, 2)
	assert.Len(t, plan.Events, 2)
	assert.Empty(t, plan.ProductsToUpdate)

	assert.Equal(t, "Product 1", plan.ProductsToInsert[0].CanonicalName)
	assert.Equal(t, "Product 2", plan.ProductsToInsert[1].CanonicalName)
}

func TestDecideBatch_NewThenChangedInSameBatch(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	t1 := now.Add(-2 * time.Hour)
	t2 := now.Add(-1 * time.Hour)

	incoming := []domainProduct.BatchIncomingRecord{
		{
			RawRecordID:       "raw-1",
			SourceID:          "src-1",
			ExternalProductID: "ext-1",
			Normalized: &domainProduct.NormalizedProduct{
				CanonicalName: "Original Title",
				Description:   "Description 1",
				Brand:         "Brand",
				OriginCountry: "US",
				Attributes:    map[string]string{"color": "red"},
			},
			Fingerprint:     "fp-v1",
			SourceUpdatedAt: &t1,
			ReceivedAt:      t1,
			IngestionRunID:  "run-1",
		},
		{
			RawRecordID:       "raw-2",
			SourceID:          "src-1",
			ExternalProductID: "ext-1",
			Normalized: &domainProduct.NormalizedProduct{
				CanonicalName: "Updated Title",
				Description:   "Description 1",
				Brand:         "Brand",
				OriginCountry: "US",
				Attributes:    map[string]string{"color": "blue"},
			},
			Fingerprint:     "fp-v2",
			SourceUpdatedAt: &t2,
			ReceivedAt:      t2,
			IngestionRunID:  "run-1",
		},
	}

	plan, err := domainProduct.DecideBatch(nil, incoming, now)
	require.NoError(t, err)

	assert.Equal(t, 2, plan.RecordsSeen)
	assert.Equal(t, 1, plan.RecordsNew)
	assert.Equal(t, 1, plan.RecordsChanged)
	assert.Equal(t, 0, plan.RecordsUnchanged)

	// 1 product inserted with final state pointing to v2
	require.Len(t, plan.ProductsToInsert, 1)
	assert.Equal(t, "Updated Title", plan.ProductsToInsert[0].CanonicalName)
	assert.Equal(t, "fp-v2", plan.ProductsToInsert[0].CurrentFingerprint)

	// 2 versions inserted (v1 and v2)
	require.Len(t, plan.ProductVersionsToInsert, 2)
	assert.Equal(t, 1, plan.ProductVersionsToInsert[0].VersionNumber)
	assert.Equal(t, "Original Title", plan.ProductVersionsToInsert[0].CanonicalName)
	assert.Equal(t, 2, plan.ProductVersionsToInsert[1].VersionNumber)
	assert.Equal(t, "Updated Title", plan.ProductVersionsToInsert[1].CanonicalName)

	// 2 changes inserted (NEW and CHANGED)
	require.Len(t, plan.ProductChangesToInsert, 2)
	assert.Equal(t, domainProduct.ChangeTypeNew, plan.ProductChangesToInsert[0].ChangeType)
	assert.Equal(t, domainProduct.ChangeTypeChanged, plan.ProductChangesToInsert[1].ChangeType)

	// 2 ProductChanged events
	require.Len(t, plan.Events, 2)
	assert.Empty(t, plan.ProductsToUpdate, "no DB product update because product is new in this batch")
}

func TestDecideBatch_ExistingProductChangedTwice(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	t0 := now.Add(-3 * time.Hour)
	t1 := now.Add(-2 * time.Hour)
	t2 := now.Add(-1 * time.Hour)

	initialV1ID := "v1-uuid"
	snapshots := map[string]*domainProduct.Snapshot{
		domainProduct.IdentityKey("src-1", "ext-1"): {
			ProductSourceID:     "ps-1",
			ProductID:           "prod-1",
			CurrentVersionID:    &initialV1ID,
			CurrentFingerprint:  "fp-v1",
			LastSourceUpdatedAt: &t0,
			LastReceivedAt:      t0,
			StoredCurrentVersion: &domainProduct.ProductVersion{
				ID:            initialV1ID,
				ProductID:     "prod-1",
				VersionNumber: 1,
				Fingerprint:   "fp-v1",
				CanonicalName: "Title v1",
			},
		},
	}

	incoming := []domainProduct.BatchIncomingRecord{
		{
			RawRecordID:       "raw-1",
			SourceID:          "src-1",
			ExternalProductID: "ext-1",
			Normalized: &domainProduct.NormalizedProduct{
				CanonicalName: "Title v2",
				Description:   "Desc v2",
				Brand:         "Brand",
				OriginCountry: "US",
			},
			Fingerprint:     "fp-v2",
			SourceUpdatedAt: &t1,
			ReceivedAt:      t1,
			IngestionRunID:  "run-1",
		},
		{
			RawRecordID:       "raw-2",
			SourceID:          "src-1",
			ExternalProductID: "ext-1",
			Normalized: &domainProduct.NormalizedProduct{
				CanonicalName: "Title v3",
				Description:   "Desc v3",
				Brand:         "Brand",
				OriginCountry: "US",
			},
			Fingerprint:     "fp-v3",
			SourceUpdatedAt: &t2,
			ReceivedAt:      t2,
			IngestionRunID:  "run-1",
		},
	}

	plan, err := domainProduct.DecideBatch(snapshots, incoming, now)
	require.NoError(t, err)

	assert.Equal(t, 2, plan.RecordsSeen)
	assert.Equal(t, 0, plan.RecordsNew)
	assert.Equal(t, 2, plan.RecordsChanged)

	assert.Empty(t, plan.ProductsToInsert)
	require.Len(t, plan.ProductVersionsToInsert, 2)
	assert.Equal(t, 2, plan.ProductVersionsToInsert[0].VersionNumber)
	assert.Equal(t, 3, plan.ProductVersionsToInsert[1].VersionNumber)

	require.Len(t, plan.ProductsToUpdate, 1)
	assert.Equal(t, domainProduct.ID("prod-1"), plan.ProductsToUpdate[0].ProductID)
	assert.Equal(t, &initialV1ID, plan.ProductsToUpdate[0].ExpectedVersionID, "must guard against initial DB version")
	assert.Equal(t, plan.ProductVersionsToInsert[1].ID, plan.ProductsToUpdate[0].ToVersionID)
	assert.Equal(t, "Title v3", plan.ProductsToUpdate[0].CanonicalName)

	require.Len(t, plan.SourcesToUpdateChanged, 1)
	assert.Equal(t, "ps-1", plan.SourcesToUpdateChanged[0].ProductSourceID)
	assert.Equal(t, &t2, plan.SourcesToUpdateChanged[0].SourceUpdatedAt)
}

func TestDecideBatch_OutOfOrderStaleRecord(t *testing.T) {
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	tNew := now.Add(-1 * time.Hour)
	tOld := now.Add(-3 * time.Hour)

	initialV1ID := "v1-uuid"
	snapshots := map[string]*domainProduct.Snapshot{
		domainProduct.IdentityKey("src-1", "ext-1"): {
			ProductSourceID:     "ps-1",
			ProductID:           "prod-1",
			CurrentVersionID:    &initialV1ID,
			CurrentFingerprint:  "fp-v1",
			LastSourceUpdatedAt: &tOld,
			LastReceivedAt:      tOld,
			StoredCurrentVersion: &domainProduct.ProductVersion{
				ID:            initialV1ID,
				ProductID:     "prod-1",
				VersionNumber: 1,
				Fingerprint:   "fp-v1",
				CanonicalName: "Title v1",
			},
		},
	}

	// Batch arrives out of order: newer record first, older record second
	incoming := []domainProduct.BatchIncomingRecord{
		{
			RawRecordID:       "raw-2",
			SourceID:          "src-1",
			ExternalProductID: "ext-1",
			Normalized: &domainProduct.NormalizedProduct{
				CanonicalName: "Title v2 (Newer)",
			},
			Fingerprint:     "fp-v2",
			SourceUpdatedAt: &tNew,
			ReceivedAt:      tNew,
			IngestionRunID:  "run-1",
		},
		{
			RawRecordID:       "raw-1",
			SourceID:          "src-1",
			ExternalProductID: "ext-1",
			Normalized: &domainProduct.NormalizedProduct{
				CanonicalName: "Title v1 (Older Replay)",
			},
			Fingerprint:     "fp-v1",
			SourceUpdatedAt: &tOld,
			ReceivedAt:      tOld,
			IngestionRunID:  "run-1",
		},
	}

	plan, err := domainProduct.DecideBatch(snapshots, incoming, now)
	require.NoError(t, err)

	assert.Equal(t, 2, plan.RecordsSeen)
	assert.Equal(t, 1, plan.RecordsChanged)
	assert.Equal(t, 1, plan.RecordsUnchanged) // Older record sorted first -> evaluated against v1 as unchanged, newer evaluated as changed

	require.Len(t, plan.ProductVersionsToInsert, 1)
	assert.Equal(t, 2, plan.ProductVersionsToInsert[0].VersionNumber)
	assert.Equal(t, "Title v2 (Newer)", plan.ProductVersionsToInsert[0].CanonicalName)
}
