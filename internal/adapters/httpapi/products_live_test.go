package httpapi_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/httpapi"
	productApp "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/testsupport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProductsAPI_Live drives the catalogue routes through the wiring cmd/api uses:
// router → product service → PostgreSQL repository.
func TestProductsAPI_Live(t *testing.T) {
	pool := testsupport.LiveDB(t)
	testsupport.Truncate(t, pool, "products, product_versions, product_sources, product_changes")
	repo, err := postgres.NewProductRepository(pool)
	require.NoError(t, err)
	svc, err := productApp.NewService(repo)
	require.NoError(t, err)
	h := httpapi.NewRouter(httpapi.RouterConfig{Products: svc})

	// Microseconds, as PostgreSQL stores them: the JSON cursor must carry them exactly.
	base := time.Date(2026, 9, 27, 10, 0, 0, 123456000, time.UTC)
	var firstID string
	// Order: 1, 0, 0 | 0, 0, -1 | -1. The tie at 0 straddles the first page boundary, as one
	// ingestion batch (a single created_at) does when it is larger than a page.
	for i, off := range []int{0, 0, 0, 0, 1, -1, -1} {
		id := testsupport.InsertProduct(t, pool, "Product", base.Add(time.Duration(off)*time.Second))
		if i == 0 {
			firstID = id
		}
	}

	t.Run("pages_through_catalogue_once", func(t *testing.T) {
		var got []string
		target := "/v1/products?limit=3"
		requests := 0
		for {
			requests++
			require.LessOrEqual(t, requests, 10, "paging did not terminate")
			rec := serve(t, h, target)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			body := decodeBody[catalogResponse](t, rec)
			require.LessOrEqual(t, len(body.Items), 3)
			for _, item := range body.Items {
				got = append(got, item["id"].(string))
			}
			if body.NextCursor == nil {
				break
			}
			target = "/v1/products?limit=3&cursor=" + url.QueryEscape(*body.NextCursor)
		}

		assert.Equal(t, testsupport.CatalogueOrder(t, pool), got)
		assert.Equal(t, 3, requests, "7 products at limit 3 are exactly 3 pages")
	})

	t.Run("get_by_id", func(t *testing.T) {
		rec := serve(t, h, "/v1/products/"+firstID)

		require.Equal(t, http.StatusOK, rec.Code)
		body := decodeBody[map[string]any](t, rec)
		assert.Equal(t, firstID, body["id"])
		assert.Equal(t, base.Format(time.RFC3339Nano), body["created_at"])
	})

	t.Run("get_unknown_id_is_404", func(t *testing.T) {
		for _, id := range []string{"00000000-0000-0000-0000-000000000000", "not-a-uuid"} {
			assert.Equal(t, http.StatusNotFound, serve(t, h, "/v1/products/"+id).Code, id)
		}
	})

	// The handler test injects ErrInvalidListParams; this proves the real repository's error keeps
	// its identity through the service wrap and reaches the client as 400, not 500.
	t.Run("non_uuid_cursor_id_is_400", func(t *testing.T) {
		rec := serve(t, h, "/v1/products?cursor="+cursorToken(`{"created_at":"2026-09-27T10:00:00Z","id":"not-a-uuid"}`))

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
