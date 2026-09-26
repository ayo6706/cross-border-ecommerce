package product

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

func IdentityKey(sourceID, externalProductID string) string {
	return strings.TrimSpace(sourceID) + "\x00" + strings.TrimSpace(externalProductID)
}

type IdentityRef struct {
	SourceID          string
	ExternalProductID string
}

type BatchIncomingRecord struct {
	RawRecordID       string
	SourceID          string
	ExternalProductID string
	Normalized        *NormalizedProduct
	Fingerprint       string
	SourceUpdatedAt   *time.Time
	ReceivedAt        time.Time
	IngestionRunID    string
}

type ProductGuardedUpdate struct {
	ProductID          ID
	ExpectedVersionID  *string
	ToVersionID        string
	CurrentFingerprint string
	CanonicalName      string
	Description        string
	Brand              string
	OriginCountry      string
	UpdatedAt          time.Time
}

type ProductFingerprintUpdate struct {
	ProductID          ID
	ExpectedVersionID  *string
	CurrentFingerprint string
	UpdatedAt          time.Time
}

type SourceWatermarkUpdate struct {
	ProductSourceID string
	SourceUpdatedAt *time.Time
	ReceivedAt      time.Time
}

type SourceChangedUpdate struct {
	ProductSourceID string
	LastChangedAt   time.Time
	SourceUpdatedAt *time.Time
	ReceivedAt      time.Time
}

type BatchPlan struct {
	ProductsToInsert        []*Product
	ProductSourcesToInsert  []*ProductSource
	ProductVersionsToInsert []*ProductVersion
	ProductChangesToInsert  []*ProductChange
	Events                  []ProductChanged

	ProductsToUpdate            []ProductGuardedUpdate
	ProductsToUpdateFingerprint []ProductFingerprintUpdate
	SourcesToUpdateWatermark    []SourceWatermarkUpdate
	SourcesToUpdateChanged      []SourceChangedUpdate

	RecordsSeen      int
	RecordsNew       int
	RecordsChanged   int
	RecordsUnchanged int
	RecordsFailed    int
}

// DecideBatch folds a page of normalized records into one BatchPlan. Each identity's records
// are folded oldest first against a running snapshot, exactly as if each record were processed
// alone; the rows to write are then derived once, from the identity's state before and after.
func DecideBatch(snapshots map[string]*Snapshot, incoming []BatchIncomingRecord, now time.Time) (*BatchPlan, error) {
	if now.IsZero() {
		return nil, errors.New("now timestamp cannot be zero")
	}
	plan := &BatchPlan{RecordsSeen: len(incoming)}
	for _, group := range groupByIdentity(incoming) {
		fold := newIdentityFold(plan, group, snapshots[group.key], now)
		for _, rec := range group.records {
			if err := fold.apply(rec); err != nil {
				return nil, err
			}
		}
		fold.emit()
	}
	return plan, nil
}

type identityGroup struct {
	key               string
	sourceID          string
	externalProductID string
	records           []BatchIncomingRecord
}

// groupByIdentity splits a page by identity, keeping first-seen order across identities and
// sorting each identity's records into processing order.
func groupByIdentity(records []BatchIncomingRecord) []identityGroup {
	index := make(map[string]int)
	var groups []identityGroup
	for _, rec := range records {
		key := IdentityKey(rec.SourceID, rec.ExternalProductID)
		i, ok := index[key]
		if !ok {
			i = len(groups)
			index[key] = i
			groups = append(groups, identityGroup{key: key, sourceID: rec.SourceID, externalProductID: rec.ExternalProductID})
		}
		groups[i].records = append(groups[i].records, rec)
	}
	for _, g := range groups {
		slices.SortStableFunc(g.records, compareProcessingOrder)
	}
	return groups
}

// compareProcessingOrder orders one identity's records oldest first: by source_updated_at
// (records without one first), then received_at, then raw record ID.
func compareProcessingOrder(a, b BatchIncomingRecord) int {
	switch {
	case a.SourceUpdatedAt == nil && b.SourceUpdatedAt != nil:
		return -1
	case a.SourceUpdatedAt != nil && b.SourceUpdatedAt == nil:
		return 1
	case a.SourceUpdatedAt != nil:
		if c := a.SourceUpdatedAt.Compare(*b.SourceUpdatedAt); c != 0 {
			return c
		}
	}
	if c := a.ReceivedAt.Compare(b.ReceivedAt); c != 0 {
		return c
	}
	return strings.Compare(a.RawRecordID, b.RawRecordID)
}

// identityFold tracks one identity while its records are folded. Versions, changes and events
// are appended to the plan as they happen; product and source rows are emitted once at the end,
// because only the final state is written.
type identityFold struct {
	plan      *BatchPlan
	group     identityGroup
	now       time.Time
	start     *Snapshot // stored state; nil when the identity is first seen in this page
	cur       *Snapshot // state after the records folded so far
	versioned bool      // a record created a version
	touched   bool      // a non-stale record was folded, so the watermarks must be written
}

func newIdentityFold(plan *BatchPlan, group identityGroup, start *Snapshot, now time.Time) *identityFold {
	f := &identityFold{plan: plan, group: group, now: now, start: start}
	if start != nil {
		cur := *start
		f.cur = &cur
	}
	return f
}

func (f *identityFold) apply(rec BatchIncomingRecord) error {
	transition := DecideTransition(f.cur, IncomingRecord{
		Normalized:      rec.Normalized,
		Fingerprint:     rec.Fingerprint,
		SourceUpdatedAt: rec.SourceUpdatedAt,
		ReceivedAt:      rec.ReceivedAt,
		RawRecordID:     rec.RawRecordID,
	})

	switch transition.Type {
	case TransitionStale:
		f.plan.RecordsUnchanged++
		return nil
	case TransitionNew, TransitionChanged:
		if err := f.addVersion(rec, transition); err != nil {
			return err
		}
	case TransitionVersionMismatch:
		f.cur.CurrentFingerprint = rec.Fingerprint
		f.plan.RecordsUnchanged++
	case TransitionUnchanged:
		f.plan.RecordsUnchanged++
	default:
		return fmt.Errorf("%w: unknown transition type %s", ErrInvalidTransition, transition.Type)
	}

	f.cur.LastSourceUpdatedAt = greatestTimestamptz(f.cur.LastSourceUpdatedAt, rec.SourceUpdatedAt)
	f.cur.LastReceivedAt = greatestTime(f.cur.LastReceivedAt, rec.ReceivedAt)
	f.touched = true
	return nil
}

// addVersion records a NEW or CHANGED transition (the next version, its change row and its
// ProductChanged event) and moves the running state to that version.
func (f *identityFold) addVersion(rec BatchIncomingRecord, transition TransitionResult) error {
	if f.cur == nil {
		ids, err := newIDs(2)
		if err != nil {
			return err
		}
		f.cur = &Snapshot{ProductID: ID(ids[0]), ProductSourceID: ids[1], LastReceivedAt: rec.ReceivedAt}
	}
	ids, err := newIDs(2)
	if err != nil {
		return err
	}
	versionID, changeID := ids[0], ids[1]

	changeType, changedFields := ChangeTypeChanged, transition.ChangedFields
	if transition.Type == TransitionNew {
		changeType, changedFields = ChangeTypeNew, []string{}
	}
	number := 1
	if f.cur.StoredCurrentVersion != nil {
		number = f.cur.StoredCurrentVersion.VersionNumber + 1
	}
	runID := optionalString(rec.IngestionRunID)
	rawRecordID := rec.RawRecordID

	version := &ProductVersion{
		ID:             versionID,
		ProductID:      f.cur.ProductID,
		VersionNumber:  number,
		Fingerprint:    rec.Fingerprint,
		CanonicalName:  rec.Normalized.CanonicalName,
		Description:    rec.Normalized.Description,
		Brand:          rec.Normalized.Brand,
		OriginCountry:  rec.Normalized.OriginCountry,
		Attributes:     rec.Normalized.Attributes,
		IngestionRunID: runID,
		CreatedAt:      f.now,
	}
	change := &ProductChange{
		ID:             changeID,
		ProductID:      f.cur.ProductID,
		FromVersionID:  f.cur.CurrentVersionID,
		ToVersionID:    versionID,
		ChangeType:     changeType,
		ChangedFields:  changedFields,
		IngestionRunID: runID,
		RawRecordID:    &rawRecordID,
		DetectedAt:     f.now,
	}

	f.plan.ProductVersionsToInsert = append(f.plan.ProductVersionsToInsert, version)
	f.plan.ProductChangesToInsert = append(f.plan.ProductChangesToInsert, change)
	f.plan.Events = append(f.plan.Events, ProductChanged{
		ProductID:     version.ProductID,
		VersionID:     version.ID,
		VersionNumber: version.VersionNumber,
		Fingerprint:   version.Fingerprint,
		ChangeType:    changeType,
		ChangedFields: changedFields,
	})
	if changeType == ChangeTypeNew {
		f.plan.RecordsNew++
	} else {
		f.plan.RecordsChanged++
	}

	f.cur.CurrentVersionID = &version.ID
	f.cur.CurrentFingerprint = version.Fingerprint
	f.cur.StoredCurrentVersion = version
	f.versioned = true
	return nil
}

// emit writes the identity's final state: an insert when the identity was first seen in this
// page, otherwise the guarded updates that move the stored row from start to cur.
func (f *identityFold) emit() {
	switch {
	case f.start == nil:
		f.emitInsert()
	case f.versioned:
		f.emitVersionUpdate()
	case f.touched:
		// A fingerprint that moved without a new version is an algorithm upgrade (VERSION_MISMATCH).
		if f.cur.CurrentFingerprint != f.start.CurrentFingerprint {
			f.plan.ProductsToUpdateFingerprint = append(f.plan.ProductsToUpdateFingerprint, ProductFingerprintUpdate{
				ProductID:          f.cur.ProductID,
				ExpectedVersionID:  f.start.CurrentVersionID,
				CurrentFingerprint: f.cur.CurrentFingerprint,
				UpdatedAt:          f.now,
			})
		}
		f.plan.SourcesToUpdateWatermark = append(f.plan.SourcesToUpdateWatermark, SourceWatermarkUpdate{
			ProductSourceID: f.cur.ProductSourceID,
			SourceUpdatedAt: f.cur.LastSourceUpdatedAt,
			ReceivedAt:      f.cur.LastReceivedAt,
		})
	}
}

func (f *identityFold) emitInsert() {
	v := f.cur.StoredCurrentVersion
	f.plan.ProductsToInsert = append(f.plan.ProductsToInsert, &Product{
		ID:                 f.cur.ProductID,
		CanonicalName:      v.CanonicalName,
		Description:        v.Description,
		Brand:              v.Brand,
		OriginCountry:      v.OriginCountry,
		Status:             StatusDraft,
		CurrentVersionID:   f.cur.CurrentVersionID,
		CurrentFingerprint: f.cur.CurrentFingerprint,
		CreatedAt:          f.now,
		UpdatedAt:          f.now,
	})
	f.plan.ProductSourcesToInsert = append(f.plan.ProductSourcesToInsert, &ProductSource{
		ID:                  f.cur.ProductSourceID,
		ProductID:           f.cur.ProductID,
		SourceID:            f.group.sourceID,
		ExternalProductID:   f.group.externalProductID,
		FirstSeenAt:         f.now,
		LastChangedAt:       f.now,
		LastSourceUpdatedAt: f.cur.LastSourceUpdatedAt,
		LastReceivedAt:      f.cur.LastReceivedAt,
	})
}

// emitVersionUpdate moves an existing product to its newest version, guarded on the version
// it had when the page was read; a concurrent writer makes the update miss and the page retry.
func (f *identityFold) emitVersionUpdate() {
	v := f.cur.StoredCurrentVersion
	f.plan.ProductsToUpdate = append(f.plan.ProductsToUpdate, ProductGuardedUpdate{
		ProductID:          f.cur.ProductID,
		ExpectedVersionID:  f.start.CurrentVersionID,
		ToVersionID:        v.ID,
		CurrentFingerprint: f.cur.CurrentFingerprint,
		CanonicalName:      v.CanonicalName,
		Description:        v.Description,
		Brand:              v.Brand,
		OriginCountry:      v.OriginCountry,
		UpdatedAt:          f.now,
	})
	f.plan.SourcesToUpdateChanged = append(f.plan.SourcesToUpdateChanged, SourceChangedUpdate{
		ProductSourceID: f.cur.ProductSourceID,
		LastChangedAt:   f.now,
		SourceUpdatedAt: f.cur.LastSourceUpdatedAt,
		ReceivedAt:      f.cur.LastReceivedAt,
	})
}

func newIDs(n int) ([]string, error) {
	ids := make([]string, n)
	for i := range ids {
		id, err := uuid.NewString()
		if err != nil {
			return nil, fmt.Errorf("generate id: %w", err)
		}
		ids[i] = id
	}
	return ids, nil
}

func optionalString(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return &s
}

func greatestTimestamptz(a, b *time.Time) *time.Time {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.After(*b) {
		return a
	}
	return b
}

func greatestTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
