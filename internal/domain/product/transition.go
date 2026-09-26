package product

import (
	"strings"
	"time"
)

type TransitionType string

const (
	TransitionNew             TransitionType = "NEW"
	TransitionChanged         TransitionType = "CHANGED"
	TransitionUnchanged       TransitionType = "UNCHANGED"
	TransitionStale           TransitionType = "STALE"
	TransitionVersionMismatch TransitionType = "VERSION_MISMATCH"
)

type TransitionResult struct {
	Type          TransitionType
	ChangedFields []string
}

type Snapshot struct {
	ProductSourceID      string
	ProductID            ID
	CurrentVersionID     *string
	CurrentFingerprint   string
	LastSourceUpdatedAt  *time.Time
	LastReceivedAt       time.Time
	StoredCurrentVersion *ProductVersion
}

type IncomingRecord struct {
	Normalized      *NormalizedProduct
	Fingerprint     string
	SourceUpdatedAt *time.Time
	ReceivedAt      time.Time
	RawRecordID     string
}

// DecideTransition evaluates an incoming normalized record against the existing product snapshot.
// It enforces out-of-order protection, version mismatch upgrades, and identity resolution.
func DecideTransition(current *Snapshot, in IncomingRecord) TransitionResult {
	if current == nil {
		return TransitionResult{
			Type:          TransitionNew,
			ChangedFields: nil,
		}
	}

	if in.SourceUpdatedAt != nil && current.LastSourceUpdatedAt != nil {
		if in.SourceUpdatedAt.Before(*current.LastSourceUpdatedAt) {
			return TransitionResult{Type: TransitionStale}
		}
		if in.SourceUpdatedAt.Equal(*current.LastSourceUpdatedAt) {
			if in.ReceivedAt.Before(current.LastReceivedAt) {
				return TransitionResult{Type: TransitionStale}
			}
		}
	} else {
		if in.ReceivedAt.Before(current.LastReceivedAt) {
			return TransitionResult{Type: TransitionStale}
		}
	}

	if !strings.HasPrefix(current.CurrentFingerprint, FingerprintV1Prefix) && current.StoredCurrentVersion != nil {
		recomputed := FingerprintFromVersion(*current.StoredCurrentVersion)
		if recomputed == in.Fingerprint {
			return TransitionResult{
				Type:          TransitionVersionMismatch,
				ChangedFields: nil,
			}
		}
	}

	if current.CurrentFingerprint == in.Fingerprint {
		return TransitionResult{
			Type:          TransitionUnchanged,
			ChangedFields: nil,
		}
	}

	var diffs []string
	if current.StoredCurrentVersion != nil && in.Normalized != nil {
		diffs = DetectFieldChanges(*current.StoredCurrentVersion, *in.Normalized)
	}

	return TransitionResult{
		Type:          TransitionChanged,
		ChangedFields: diffs,
	}
}
