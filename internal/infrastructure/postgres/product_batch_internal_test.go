package postgres

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

const (
	testIDA = "00000000-0000-4000-8000-00000000000a"
	testIDB = "00000000-0000-4000-8000-00000000000b"
)

// The repository has no database: any SQL call would panic, so these tests also prove
// that invalid plans are rejected before a statement is sent.
func TestApplyBatch_RejectsInvalidPlanBeforeSQL(t *testing.T) {
	t.Parallel()
	repo := &ProductRepository{}
	bad := "not-a-uuid"

	tests := []struct {
		name  string
		plan  *product.BatchPlan
		field string
	}{
		{"nil plan", nil, "nil batch plan"},
		{"product id", &product.BatchPlan{ProductsToInsert: []*product.Product{{ID: product.ID(bad)}}}, "product id"},
		{"optional version id", &product.BatchPlan{ProductsToInsert: []*product.Product{{ID: testIDA, CurrentVersionID: &bad}}}, "current version id"},
		{"version number overflow", &product.BatchPlan{ProductVersionsToInsert: []*product.ProductVersion{
			{ID: testIDA, ProductID: testIDB, VersionNumber: math.MaxInt32 + 1},
		}}, "version number"},
		{"update expected version", &product.BatchPlan{ProductsToUpdate: []product.ProductGuardedUpdate{
			{ProductID: testIDA, ToVersionID: testIDB, ExpectedVersionID: &bad},
		}}, "expected_version_id"},
		{"source watermark id", &product.BatchPlan{SourcesToUpdateWatermark: []product.SourceWatermarkUpdate{
			{ProductSourceID: bad},
		}}, "product source id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := repo.ApplyBatch(context.Background(), tc.plan)
			if !errors.Is(err, product.ErrInvalidProductState) {
				t.Fatalf("err = %v, want ErrInvalidProductState", err)
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("err = %q, want it to name %q", err, tc.field)
			}
		})
	}
}

func TestPlanIDs_BlankOptionalIsNull(t *testing.T) {
	t.Parallel()
	var ids planIDs
	blank := "  "
	if u := ids.optional("from_version_id", nil); u.Valid {
		t.Fatal("nil optional id must map to NULL")
	}
	if u := ids.optional("from_version_id", &blank); u.Valid {
		t.Fatal("blank optional id must map to NULL")
	}
	if ids.err != nil {
		t.Fatalf("unexpected error: %v", ids.err)
	}
}

func TestSortedByKey_SortsCopyOnly(t *testing.T) {
	t.Parallel()
	in := []string{testIDB, testIDA}
	got := sortedByKey(in, func(s string) string { return s })
	if got[0] != testIDA || got[1] != testIDB {
		t.Fatalf("sorted = %v, want ascending", got)
	}
	if in[0] != testIDB {
		t.Fatal("sortedByKey must not reorder the caller's slice")
	}
}

func TestRequireAllUpdated(t *testing.T) {
	t.Parallel()
	if err := requireAllUpdated(3, 3); err != nil {
		t.Fatalf("all rows updated: %v", err)
	}
	err := requireAllUpdated(2, 3)
	if !errors.Is(err, product.ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
	if !strings.Contains(err.Error(), "1 of 3") {
		t.Fatalf("err = %q, want the conflict count", err)
	}
}
