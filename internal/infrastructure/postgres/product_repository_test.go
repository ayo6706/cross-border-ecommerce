package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/testsupport"
	"github.com/jackc/pgx/v5/pgxpool"
)

const productTables = "products, product_versions, product_sources, product_changes, ingestion_run_processing"

func TestProductRepository_ConstructorValidation(t *testing.T) {
	t.Parallel()

	repo, err := postgres.NewProductRepository(nil)
	if err == nil {
		t.Fatal("expected error when db is nil, got nil")
	}
	if repo != nil {
		t.Fatalf("expected nil repo, got %v", repo)
	}
}

func TestProductRepository_LiveIntegration(t *testing.T) {
	pool := testsupport.LiveDB(t)
	repo, err := postgres.NewProductRepository(pool)
	if err != nil {
		t.Fatalf("create product repository: %v", err)
	}
	ctx := context.Background()
	base := time.Date(2026, 9, 27, 10, 0, 0, 123456000, time.UTC)

	t.Run("find_by_id", func(t *testing.T) {
		testsupport.Truncate(t, pool, productTables)
		id := testsupport.InsertProduct(t, pool, "Logitech MX Master 3S", base)

		found, err := repo.FindByID(ctx, product.ID(id))
		if err != nil {
			t.Fatalf("FindByID: %v", err)
		}
		if found.CanonicalName != "Logitech MX Master 3S" || !found.CreatedAt.Equal(base) {
			t.Errorf("found %+v, want name Logitech MX Master 3S created %v", found, base)
		}
		if found.Status != product.StatusDraft || found.CurrentVersionID != nil {
			t.Errorf("status %q version %v, want DRAFT and no version", found.Status, found.CurrentVersionID)
		}
	})

	t.Run("find_unknown_or_non_uuid_id_is_not_found", func(t *testing.T) {
		for _, id := range []product.ID{"00000000-0000-0000-0000-000000000000", "not-a-uuid"} {
			if _, err := repo.FindByID(ctx, id); !errors.Is(err, product.ErrProductNotFound) {
				t.Errorf("FindByID(%q) = %v, want ErrProductNotFound", id, err)
			}
		}
	})

	t.Run("keyset_ties_on_created_at", func(t *testing.T) {
		testsupport.Truncate(t, pool, productTables)
		// One ingestion batch stamps every product with the same created_at; the id tie-break
		// must split them across pages without dropping or repeating any.
		want := seedNewestFirst(t, pool, base, []int{0, 0, 0, 0, 0, -1, 1})

		got := pageThrough(ctx, t, repo, 2)

		assertIDs(t, got, want)
	})

	t.Run("full_last_page_has_no_next_cursor", func(t *testing.T) {
		testsupport.Truncate(t, pool, productTables)
		want := seedNewestFirst(t, pool, base, []int{0, 1, 2})

		page, err := repo.List(ctx, product.ListParams{Limit: 3})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if page.Next != nil {
			t.Errorf("Next = %+v, want nil when exactly limit products remain", page.Next)
		}
		assertIDs(t, idsOf(page.Items), want)
	})

	t.Run("empty_catalogue_is_an_empty_last_page", func(t *testing.T) {
		testsupport.Truncate(t, pool, productTables)

		page, err := repo.List(ctx, product.ListParams{Limit: 10})
		if err != nil || len(page.Items) != 0 || page.Next != nil {
			t.Fatalf("List = (%+v, %v), want empty page without cursor", page, err)
		}
	})

	t.Run("invalid_params_rejected", func(t *testing.T) {
		for _, params := range []product.ListParams{
			{Limit: 0},
			{Limit: product.MaxListLimit + 1},
			{Limit: 1, After: &product.Cursor{ID: "00000000-0000-0000-0000-000000000000"}},
		} {
			if _, err := repo.List(ctx, params); !errors.Is(err, product.ErrInvalidListParams) {
				t.Errorf("List(%+v) = %v, want ErrInvalidListParams", params, err)
			}
		}
	})

	t.Run("non_uuid_cursor_id_is_invalid_params", func(t *testing.T) {
		params := product.ListParams{Limit: 1, After: &product.Cursor{CreatedAt: base, ID: "not-a-uuid"}}
		if _, err := repo.List(ctx, params); !errors.Is(err, product.ErrInvalidListParams) {
			t.Fatalf("List = %v, want ErrInvalidListParams", err)
		}
	})

	t.Run("domain_limits_match_schema", func(t *testing.T) {
		for column, want := range map[string]int{
			"canonical_name": product.MaxCanonicalNameChars,
			"brand":          product.MaxBrandChars,
		} {
			for _, table := range []string{"products", "product_versions"} {
				var got int
				err := pool.QueryRow(ctx, `SELECT character_maximum_length FROM information_schema.columns
					WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`,
					table, column).Scan(&got)
				if err != nil {
					t.Fatalf("read width of %s.%s: %v", table, column, err)
				}
				if got != want {
					t.Errorf("%s.%s is VARCHAR(%d), domain limit is %d", table, column, got, want)
				}
			}
		}
	})
}

// seedNewestFirst inserts one product per offset (seconds from base) and returns their ids in
// the order the catalogue must list them: created_at DESC, id DESC.
func seedNewestFirst(t *testing.T, pool *pgxpool.Pool, base time.Time, offsets []int) []product.ID {
	t.Helper()
	for _, off := range offsets {
		testsupport.InsertProduct(t, pool, "Product", base.Add(time.Duration(off)*time.Second))
	}
	var ids []product.ID
	for _, id := range testsupport.CatalogueOrder(t, pool) {
		ids = append(ids, product.ID(id))
	}
	return ids
}

// pageThrough follows Next until the last page and fails if paging does not terminate.
func pageThrough(ctx context.Context, t *testing.T, repo *postgres.ProductRepository, limit int) []product.ID {
	t.Helper()
	var ids []product.ID
	params := product.ListParams{Limit: limit}
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatal("paging did not terminate")
		}
		page, err := repo.List(ctx, params)
		if err != nil {
			t.Fatalf("List page %d: %v", pages, err)
		}
		if len(page.Items) > limit {
			t.Fatalf("page %d has %d items, limit %d", pages, len(page.Items), limit)
		}
		ids = append(ids, idsOf(page.Items)...)
		if page.Next == nil {
			return ids
		}
		params.After = page.Next
	}
}

func idsOf(items []*product.Product) []product.ID {
	ids := make([]product.ID, 0, len(items))
	for _, p := range items {
		ids = append(ids, p.ID)
	}
	return ids
}

func assertIDs(t *testing.T, got, want []product.ID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d ids %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("position %d: got %s, want %s (got %v, want %v)", i, got[i], want[i], got, want)
		}
	}
}
