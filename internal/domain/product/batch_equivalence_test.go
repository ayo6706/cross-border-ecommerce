package product_test

import (
	"fmt"
	"testing"
	"time"

	domainProduct "github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/stretchr/testify/require"
)

// memStore applies a BatchPlan the way ApplyBatch's SQL does: inserts, guarded updates that
// fail on a version mismatch, and GREATEST() watermarks. It lets a page-sized plan be compared
// with the same records applied one page per record.
type memStore struct {
	t        *testing.T
	byKey    map[string]*domainProduct.Snapshot
	products map[domainProduct.ID]string // product id → identity key
	sources  map[string]string           // product source id → identity key
	versions map[string]*domainProduct.ProductVersion
	changes  map[string]int // identity key → product_changes rows
	events   map[string]int // identity key → ProductChanged events
}

func newMemStore(t *testing.T) *memStore {
	return &memStore{
		t: t, byKey: map[string]*domainProduct.Snapshot{}, products: map[domainProduct.ID]string{},
		sources: map[string]string{}, versions: map[string]*domainProduct.ProductVersion{},
		changes: map[string]int{}, events: map[string]int{},
	}
}

func (s *memStore) seed(key string, snap domainProduct.Snapshot) {
	s.byKey[key] = &snap
	s.products[snap.ProductID] = key
	s.sources[snap.ProductSourceID] = key
	if snap.StoredCurrentVersion != nil {
		s.versions[snap.StoredCurrentVersion.ID] = snap.StoredCurrentVersion
	}
}

func (s *memStore) snapshots() map[string]*domainProduct.Snapshot {
	out := make(map[string]*domainProduct.Snapshot, len(s.byKey))
	for k, v := range s.byKey {
		c := *v
		out[k] = &c
	}
	return out
}

func (s *memStore) apply(plan *domainProduct.BatchPlan) {
	t := s.t
	for _, v := range plan.ProductVersionsToInsert {
		s.versions[v.ID] = v
	}
	for i, p := range plan.ProductsToInsert {
		ps := plan.ProductSourcesToInsert[i]
		require.Equal(t, p.ID, ps.ProductID, "products and sources are inserted pairwise")
		key := domainProduct.IdentityKey(ps.SourceID, ps.ExternalProductID)
		require.NotContains(t, s.byKey, key, "duplicate identity insert")
		s.byKey[key] = &domainProduct.Snapshot{
			ProductSourceID: ps.ID, ProductID: p.ID, CurrentVersionID: p.CurrentVersionID,
			CurrentFingerprint: p.CurrentFingerprint, LastSourceUpdatedAt: ps.LastSourceUpdatedAt,
			LastReceivedAt: ps.LastReceivedAt, StoredCurrentVersion: s.versions[*p.CurrentVersionID],
		}
		s.products[p.ID] = key
		s.sources[ps.ID] = key
	}
	for _, u := range plan.ProductsToUpdate {
		snap := s.byKey[s.products[u.ProductID]]
		require.Equal(t, deref(snap.CurrentVersionID), deref(u.ExpectedVersionID), "guarded update would conflict")
		snap.CurrentVersionID = &u.ToVersionID
		snap.CurrentFingerprint = u.CurrentFingerprint
		snap.StoredCurrentVersion = s.versions[u.ToVersionID]
	}
	for _, u := range plan.ProductsToUpdateFingerprint {
		snap := s.byKey[s.products[u.ProductID]]
		require.Equal(t, deref(snap.CurrentVersionID), deref(u.ExpectedVersionID), "guarded update would conflict")
		snap.CurrentFingerprint = u.CurrentFingerprint
	}
	for _, u := range plan.SourcesToUpdateChanged {
		s.bump(s.byKey[s.sources[u.ProductSourceID]], u.SourceUpdatedAt, u.ReceivedAt)
	}
	for _, u := range plan.SourcesToUpdateWatermark {
		s.bump(s.byKey[s.sources[u.ProductSourceID]], u.SourceUpdatedAt, u.ReceivedAt)
	}
	for _, c := range plan.ProductChangesToInsert {
		require.Contains(t, s.versions, c.ToVersionID, "change must reference an inserted version")
		s.changes[s.products[c.ProductID]]++
	}
	for _, e := range plan.Events {
		s.events[s.products[e.ProductID]]++
	}
}

func (s *memStore) bump(snap *domainProduct.Snapshot, sua *time.Time, ra time.Time) {
	if sua != nil && (snap.LastSourceUpdatedAt == nil || sua.After(*snap.LastSourceUpdatedAt)) {
		snap.LastSourceUpdatedAt = sua
	}
	if ra.After(snap.LastReceivedAt) {
		snap.LastReceivedAt = ra
	}
}

type identityOutcome struct {
	Version, Versions, Changes, Events int
	Name, Fingerprint                  string
	SourceUpdatedAt                    string
	ReceivedAt                         time.Time
}

func (s *memStore) outcome() map[string]identityOutcome {
	perProduct := map[domainProduct.ID]int{}
	for _, v := range s.versions {
		perProduct[v.ProductID]++
	}
	out := map[string]identityOutcome{}
	for key, snap := range s.byKey {
		o := identityOutcome{
			Versions: perProduct[snap.ProductID], Changes: s.changes[key], Events: s.events[key],
			Fingerprint: snap.CurrentFingerprint, ReceivedAt: snap.LastReceivedAt,
		}
		if snap.StoredCurrentVersion != nil {
			o.Version, o.Name = snap.StoredCurrentVersion.VersionNumber, snap.StoredCurrentVersion.CanonicalName
		}
		if snap.LastSourceUpdatedAt != nil {
			o.SourceUpdatedAt = snap.LastSourceUpdatedAt.UTC().Format(time.RFC3339Nano)
		}
		out[key] = o
	}
	return out
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

type counters struct{ seen, new, changed, unchanged int }

func (c *counters) add(p *domainProduct.BatchPlan) {
	c.seen += p.RecordsSeen
	c.new += p.RecordsNew
	c.changed += p.RecordsChanged
	c.unchanged += p.RecordsUnchanged
}

// TestDecideBatch_PageEqualsOneRecordAtATime is the refactoring safety net for DecideBatch:
// folding a whole page in memory must leave the store exactly as processing the same records
// one page per record does.
func TestDecideBatch_PageEqualsOneRecordAtATime(t *testing.T) {
	base := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	at := func(minutes int) *time.Time { ts := base.Add(time.Duration(minutes) * time.Minute); return &ts }
	product := func(name string) *domainProduct.NormalizedProduct {
		return &domainProduct.NormalizedProduct{CanonicalName: name, Brand: "B", OriginCountry: "US"}
	}
	rec := func(ext, name string, sua *time.Time, ra int) domainProduct.BatchIncomingRecord {
		n := product(name)
		return domainProduct.BatchIncomingRecord{
			RawRecordID: fmt.Sprintf("raw-%s-%03d", ext, ra), SourceID: "src", ExternalProductID: ext,
			Normalized: n, Fingerprint: domainProduct.Fingerprint(*n), SourceUpdatedAt: sua,
			ReceivedAt: *at(ra), IngestionRunID: "00000000-0000-4000-8000-000000000001",
		}
	}
	existing := func(ext, name, fingerprint string, sua *time.Time, ra int) (string, domainProduct.Snapshot) {
		v := &domainProduct.ProductVersion{
			ID: "ver-" + ext, ProductID: domainProduct.ID("prod-" + ext), VersionNumber: 1,
			CanonicalName: name, Brand: "B", OriginCountry: "US", Fingerprint: fingerprint,
		}
		return domainProduct.IdentityKey("src", ext), domainProduct.Snapshot{
			ProductSourceID: "ps-" + ext, ProductID: v.ProductID, CurrentVersionID: &v.ID,
			CurrentFingerprint: fingerprint, LastSourceUpdatedAt: sua, LastReceivedAt: *at(ra),
			StoredCurrentVersion: v,
		}
	}
	fp := func(name string) string { return domainProduct.Fingerprint(*product(name)) }

	type seeded struct {
		key  string
		snap domainProduct.Snapshot
	}
	seed := func(key string, snap domainProduct.Snapshot) seeded { return seeded{key, snap} }

	cases := []struct {
		name    string
		seeds   []seeded
		records []domainProduct.BatchIncomingRecord // in (source_updated_at, received_at, id) order
	}{
		{"new only", nil, []domainProduct.BatchIncomingRecord{rec("a", "A1", at(1), 1)}},
		{"new then changed", nil, []domainProduct.BatchIncomingRecord{rec("a", "A1", at(1), 1), rec("a", "A2", at(2), 2)}},
		{"new then unchanged", nil, []domainProduct.BatchIncomingRecord{rec("a", "A1", at(1), 1), rec("a", "A1", at(2), 3)}},
		{"new, changed, unchanged, changed", nil, []domainProduct.BatchIncomingRecord{
			rec("a", "A1", at(1), 1), rec("a", "A2", at(2), 2), rec("a", "A2", at(3), 3), rec("a", "A3", at(4), 4),
		}},
		{"existing changed twice", []seeded{seed(existing("a", "A1", fp("A1"), at(0), 0))},
			[]domainProduct.BatchIncomingRecord{rec("a", "A2", at(1), 1), rec("a", "A3", at(2), 2)}},
		{"existing unchanged", []seeded{seed(existing("a", "A1", fp("A1"), at(0), 0))},
			[]domainProduct.BatchIncomingRecord{rec("a", "A1", at(1), 1)}},
		{"existing stale", []seeded{seed(existing("a", "A1", fp("A1"), at(5), 5))},
			[]domainProduct.BatchIncomingRecord{rec("a", "A2", at(1), 1)}},
		{"existing unchanged then changed", []seeded{seed(existing("a", "A1", fp("A1"), at(0), 0))},
			[]domainProduct.BatchIncomingRecord{rec("a", "A1", at(1), 1), rec("a", "A2", at(2), 2)}},
		{"existing changed then unchanged", []seeded{seed(existing("a", "A1", fp("A1"), at(0), 0))},
			[]domainProduct.BatchIncomingRecord{rec("a", "A2", at(1), 1), rec("a", "A2", at(2), 2)}},
		{"legacy fingerprint upgraded", []seeded{seed(existing("a", "A1", "legacy-fp", at(0), 0))},
			[]domainProduct.BatchIncomingRecord{rec("a", "A1", at(1), 1)}},
		{"legacy fingerprint upgraded then changed", []seeded{seed(existing("a", "A1", "legacy-fp", at(0), 0))},
			[]domainProduct.BatchIncomingRecord{rec("a", "A1", at(1), 1), rec("a", "A2", at(2), 2)}},
		{"no source_updated_at, received_at order", []seeded{seed(existing("a", "A1", fp("A1"), nil, 0))},
			[]domainProduct.BatchIncomingRecord{rec("a", "A1", nil, 1), rec("a", "A2", nil, 2)}},
		{"several identities in one page", []seeded{seed(existing("b", "B1", fp("B1"), at(0), 0))},
			[]domainProduct.BatchIncomingRecord{
				rec("a", "A1", at(1), 1), rec("b", "B2", at(1), 1), rec("c", "C1", at(1), 1),
				rec("a", "A2", at(2), 2), rec("b", "B2", at(2), 2),
			}},
	}

	now := base.Add(24 * time.Hour)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paged, single := newMemStore(t), newMemStore(t)
			for _, s := range tc.seeds {
				paged.seed(s.key, s.snap)
				single.seed(s.key, s.snap)
			}

			var pagedCounts, singleCounts counters
			plan, err := domainProduct.DecideBatch(paged.snapshots(), tc.records, now)
			require.NoError(t, err)
			paged.apply(plan)
			pagedCounts.add(plan)
			if tc.name == "legacy fingerprint upgraded" {
				require.Len(t, plan.ProductsToUpdateFingerprint, 1, "case must exercise VERSION_MISMATCH")
			}

			for _, r := range tc.records {
				plan, err := domainProduct.DecideBatch(single.snapshots(), []domainProduct.BatchIncomingRecord{r}, now)
				require.NoError(t, err)
				single.apply(plan)
				singleCounts.add(plan)
			}

			require.Equal(t, singleCounts, pagedCounts, "record counters")
			require.Equal(t, single.outcome(), paged.outcome(), "final state per identity")
		})
	}
}
