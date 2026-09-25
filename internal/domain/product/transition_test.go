package product

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecideTransition(t *testing.T) {
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	t1 := now.Add(-2 * time.Hour)
	t2 := now.Add(-1 * time.Hour)

	norm1 := &NormalizedProduct{
		CanonicalName: "Sony Bravia 4K TV",
		Description:   "Ultra HD Smart LED TV",
		Brand:         "Sony",
		OriginCountry: "JP",
		Attributes: map[string]string{
			"color": "Black",
			"size":  "65",
		},
	}
	fp1 := Fingerprint(*norm1)

	ver1 := &ProductVersion{
		ID:            "ver-1",
		ProductID:     "prod-1",
		VersionNumber: 1,
		Fingerprint:   fp1,
		CanonicalName: norm1.CanonicalName,
		Description:   norm1.Description,
		Brand:         norm1.Brand,
		OriginCountry: norm1.OriginCountry,
		Attributes:    norm1.Attributes,
		CreatedAt:     t1,
	}

	snap1 := &Snapshot{
		ProductSourceID:      "ps-1",
		ProductID:            "prod-1",
		CurrentVersionID:     &ver1.ID,
		CurrentFingerprint:   fp1,
		LastSourceUpdatedAt:  &t1,
		LastReceivedAt:       t1,
		StoredCurrentVersion: ver1,
	}

	t.Run("New product when snapshot is nil", func(t *testing.T) {
		incoming := IncomingRecord{
			Normalized:      norm1,
			Fingerprint:     fp1,
			SourceUpdatedAt: &t1,
			ReceivedAt:      t1,
			RawRecordID:     "raw-1",
		}
		res := DecideTransition(nil, incoming)
		assert.Equal(t, TransitionNew, res.Type)
		assert.Empty(t, res.ChangedFields)
	})

	t.Run("Unchanged record when fingerprint matches and timestamps are newer", func(t *testing.T) {
		incoming := IncomingRecord{
			Normalized:      norm1,
			Fingerprint:     fp1,
			SourceUpdatedAt: &t2,
			ReceivedAt:      t2,
			RawRecordID:     "raw-2",
		}
		res := DecideTransition(snap1, incoming)
		assert.Equal(t, TransitionUnchanged, res.Type)
		assert.Empty(t, res.ChangedFields)
	})

	t.Run("Changed record with field diffs", func(t *testing.T) {
		norm2 := &NormalizedProduct{
			CanonicalName: "Sony Bravia 4K OLED TV", // changed
			Description:   norm1.Description,
			Brand:         norm1.Brand,
			OriginCountry: norm1.OriginCountry,
			Attributes: map[string]string{
				"color": "Silver", // changed
				"size":  "65",
			},
		}
		fp2 := Fingerprint(*norm2)

		incoming := IncomingRecord{
			Normalized:      norm2,
			Fingerprint:     fp2,
			SourceUpdatedAt: &t2,
			ReceivedAt:      t2,
			RawRecordID:     "raw-2",
		}
		res := DecideTransition(snap1, incoming)
		assert.Equal(t, TransitionChanged, res.Type)
		assert.ElementsMatch(t, []string{"canonical_name", "attributes"}, res.ChangedFields)
	})

	t.Run("Stale record when source_updated_at is older", func(t *testing.T) {
		tOlder := t1.Add(-1 * time.Hour)
		normDifferent := &NormalizedProduct{
			CanonicalName: "Old Name",
			Description:   norm1.Description,
		}
		fpDifferent := Fingerprint(*normDifferent)

		incoming := IncomingRecord{
			Normalized:      normDifferent,
			Fingerprint:     fpDifferent,
			SourceUpdatedAt: &tOlder,
			ReceivedAt:      t2,
			RawRecordID:     "raw-old",
		}
		res := DecideTransition(snap1, incoming)
		assert.Equal(t, TransitionStale, res.Type)
		assert.Empty(t, res.ChangedFields)
	})

	t.Run("Stale record when mixed nulls and received_at is older", func(t *testing.T) {
		snapNoSourceTs := &Snapshot{
			ProductSourceID:      "ps-1",
			ProductID:            "prod-1",
			CurrentVersionID:     &ver1.ID,
			CurrentFingerprint:   fp1,
			LastSourceUpdatedAt:  nil,
			LastReceivedAt:       t2,
			StoredCurrentVersion: ver1,
		}

		incoming := IncomingRecord{
			Normalized:      norm1,
			Fingerprint:     "v1:different",
			SourceUpdatedAt: nil,
			ReceivedAt:      t1, // older than t2
			RawRecordID:     "raw-old",
		}
		res := DecideTransition(snapNoSourceTs, incoming)
		assert.Equal(t, TransitionStale, res.Type)
	})

	t.Run("Equal source_updated_at tie breaks with received_at", func(t *testing.T) {
		normDifferent := &NormalizedProduct{
			CanonicalName: "Updated Name",
			Description:   norm1.Description,
		}
		fpDifferent := Fingerprint(*normDifferent)

		// Older received_at on equal source_updated_at -> Stale
		incomingStale := IncomingRecord{
			Normalized:      normDifferent,
			Fingerprint:     fpDifferent,
			SourceUpdatedAt: &t1, // equal to snap1.LastSourceUpdatedAt
			ReceivedAt:      t1.Add(-5 * time.Minute),
			RawRecordID:     "raw-stale",
		}
		resStale := DecideTransition(snap1, incomingStale)
		assert.Equal(t, TransitionStale, resStale.Type)

		// Newer received_at on equal source_updated_at -> Changed
		incomingNewer := IncomingRecord{
			Normalized:      normDifferent,
			Fingerprint:     fpDifferent,
			SourceUpdatedAt: &t1,
			ReceivedAt:      t1.Add(5 * time.Minute),
			RawRecordID:     "raw-newer",
		}
		resNewer := DecideTransition(snap1, incomingNewer)
		assert.Equal(t, TransitionChanged, resNewer.Type)
	})

	t.Run("Fingerprint version mismatch updates fingerprint only", func(t *testing.T) {
		legacySnap := &Snapshot{
			ProductSourceID:      "ps-1",
			ProductID:            "prod-1",
			CurrentVersionID:     &ver1.ID,
			CurrentFingerprint:   "v0:legacy_hash_format_12345", // v0 prefix
			LastSourceUpdatedAt:  &t1,
			LastReceivedAt:       t1,
			StoredCurrentVersion: ver1, // version attributes match norm1
		}

		incoming := IncomingRecord{
			Normalized:      norm1,
			Fingerprint:     fp1, // v1:...
			SourceUpdatedAt: &t2,
			ReceivedAt:      t2,
			RawRecordID:     "raw-upgrade",
		}
		res := DecideTransition(legacySnap, incoming)
		assert.Equal(t, TransitionVersionMismatch, res.Type)
		assert.Empty(t, res.ChangedFields)
	})
}

func TestDetectFieldChanges(t *testing.T) {
	current := ProductVersion{
		CanonicalName: "Apple iPhone 15 Pro",
		Description:   "Flagship smartphone with A17 Pro chip",
		Brand:         "Apple",
		OriginCountry: "US",
		Attributes: map[string]string{
			"color":   "Titanium Blue",
			"storage": "256GB",
		},
	}

	t.Run("No changes", func(t *testing.T) {
		next := NormalizedProduct{
			CanonicalName: current.CanonicalName,
			Description:   current.Description,
			Brand:         current.Brand,
			OriginCountry: current.OriginCountry,
			Attributes:    current.Attributes,
		}
		diffs := DetectFieldChanges(current, next)
		assert.Empty(t, diffs)
	})

	t.Run("Brand case-insensitivity produces no diff", func(t *testing.T) {
		next := NormalizedProduct{
			CanonicalName: current.CanonicalName,
			Description:   current.Description,
			Brand:         "apple", // different case
			OriginCountry: current.OriginCountry,
			Attributes:    current.Attributes,
		}
		diffs := DetectFieldChanges(current, next)
		assert.Empty(t, diffs)
	})

	t.Run("Multiple field modifications", func(t *testing.T) {
		next := NormalizedProduct{
			CanonicalName: "Apple iPhone 15 Pro Max",
			Description:   "Flagship smartphone with larger display",
			Brand:         current.Brand,
			OriginCountry: "VN",
			Attributes: map[string]string{
				"color":   "Natural Titanium",
				"storage": "256GB",
			},
		}
		diffs := DetectFieldChanges(current, next)
		require.ElementsMatch(t, []string{"canonical_name", "description", "origin_country", "attributes"}, diffs)
	})
}
