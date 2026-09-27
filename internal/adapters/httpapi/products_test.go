package httpapi_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/httpapi"
	productApp "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeCatalog validates List params with ListParams.Validate, as the real repository does, and
// returns ErrProductNotFound for unknown ids. It does not parse ids (the live tests cover the
// non-UUID cases). The embedded interface panics on any other method: the handlers never call one.
type fakeCatalog struct {
	product.Repository
	products  map[product.ID]*product.Product
	page      product.Page
	err       error
	gotParams []product.ListParams
}

func (f *fakeCatalog) FindByID(_ context.Context, id product.ID) (*product.Product, error) {
	if f.err != nil {
		return nil, f.err
	}
	p, ok := f.products[id]
	if !ok {
		return nil, product.ErrProductNotFound
	}
	cp := *p
	return &cp, nil
}

func (f *fakeCatalog) List(_ context.Context, params product.ListParams) (product.Page, error) {
	f.gotParams = append(f.gotParams, params)
	if err := params.Validate(); err != nil {
		return product.Page{}, err
	}
	if f.err != nil {
		return product.Page{}, f.err
	}
	return f.page, nil
}

type catalogResponse struct {
	Items      []map[string]any `json:"items"`
	NextCursor *string          `json:"next_cursor"`
}

func newCatalogRouter(t *testing.T, repo *fakeCatalog) (http.Handler, *bytes.Buffer) {
	t.Helper()
	svc, err := productApp.NewService(repo)
	require.NoError(t, err)
	logs := &bytes.Buffer{}
	return httpapi.NewRouter(httpapi.RouterConfig{
		Logger:   slog.New(slog.NewJSONHandler(logs, nil)),
		Products: svc,
	}), logs
}

func serve(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var body T
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	return body
}

// cursorToken builds a token from raw JSON: the handler must reject each malformed shape.
func cursorToken(rawJSON string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(rawJSON))
}

func testProduct(id string, createdAt time.Time) *product.Product {
	versionID := "7a2d6f2e-4c1b-4b8e-9d3a-1f0e2c3b4a59"
	return &product.Product{
		ID:                 product.ID(id),
		CanonicalName:      "Sony WH-1000XM5",
		Description:        "Noise cancelling headphones",
		Brand:              "Sony",
		OriginCountry:      "JP",
		Status:             product.StatusDraft,
		CurrentVersionID:   &versionID,
		CurrentFingerprint: "v1:abc",
		CreatedAt:          createdAt,
		UpdatedAt:          createdAt.Add(time.Minute),
	}
}

func TestListProducts_Handler(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 9, 27, 10, 0, 0, 123456000, time.UTC)
	next := &product.Cursor{CreatedAt: createdAt, ID: "3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"}

	t.Run("first_page_uses_default_limit_and_returns_next_cursor", func(t *testing.T) {
		t.Parallel()
		repo := &fakeCatalog{page: product.Page{
			Items: []*product.Product{testProduct(string(next.ID), createdAt)},
			Next:  next,
		}}
		h, _ := newCatalogRouter(t, repo)

		rec := serve(t, h, "/v1/products")

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.Len(t, repo.gotParams, 1)
		assert.Equal(t, product.ListParams{Limit: product.DefaultListLimit}, repo.gotParams[0])
		body := decodeBody[catalogResponse](t, rec)
		require.Len(t, body.Items, 1)
		require.NotNil(t, body.NextCursor)
		assert.NotEmpty(t, *body.NextCursor)
	})

	t.Run("next_cursor_round_trips_to_the_same_keyset_position", func(t *testing.T) {
		t.Parallel()
		repo := &fakeCatalog{page: product.Page{Next: next}}
		h, _ := newCatalogRouter(t, repo)

		first := decodeBody[catalogResponse](t, serve(t, h, "/v1/products?limit=7"))
		require.NotNil(t, first.NextCursor)
		rec := serve(t, h, "/v1/products?limit=7&cursor="+*first.NextCursor)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Len(t, repo.gotParams, 2)
		got := repo.gotParams[1]
		assert.Equal(t, 7, got.Limit)
		require.NotNil(t, got.After)
		assert.True(t, next.CreatedAt.Equal(got.After.CreatedAt), "created_at %v != %v", got.After.CreatedAt, next.CreatedAt)
		assert.Equal(t, next.ID, got.After.ID)
	})

	t.Run("last_page_has_null_next_cursor_and_empty_items_is_an_array", func(t *testing.T) {
		t.Parallel()
		h, _ := newCatalogRouter(t, &fakeCatalog{})

		rec := serve(t, h, "/v1/products")

		require.Equal(t, http.StatusOK, rec.Code)
		assert.JSONEq(t, `{"items":[],"next_cursor":null}`, rec.Body.String())
	})

	t.Run("item_fields", func(t *testing.T) {
		t.Parallel()
		p := testProduct(string(next.ID), createdAt)
		h, _ := newCatalogRouter(t, &fakeCatalog{page: product.Page{Items: []*product.Product{p}}})

		body := decodeBody[catalogResponse](t, serve(t, h, "/v1/products"))

		require.Len(t, body.Items, 1)
		assertProductJSON(t, p, body.Items[0])
	})

	// Rejected while parsing the query: the repository is never called.
	unparsable := map[string]string{
		"limit_not_a_number":   "limit=abc",
		"limit_not_an_integer": "limit=1.5",
		"cursor_not_base64":    "cursor=!!!not-base64!!!",
		"cursor_padded_std_base64": "cursor=" + base64.StdEncoding.EncodeToString(
			[]byte(`{"created_at":"2026-09-27T10:00:00Z","id":"a"}`)),
		"cursor_not_json":      "cursor=" + cursorToken("not json"),
		"cursor_unknown_field": "cursor=" + cursorToken(`{"created_at":"2026-09-27T10:00:00Z","id":"a","offset":5}`),
		"cursor_bad_time":      "cursor=" + cursorToken(`{"created_at":"yesterday","id":"a"}`),
		"cursor_trailing_data": "cursor=" + cursorToken(`{"created_at":"2026-09-27T10:00:00Z","id":"a"} {}`),
	}
	for name, query := range unparsable {
		t.Run("unparsable_query_is_400_before_the_repository/"+name, func(t *testing.T) {
			t.Parallel()
			repo := &fakeCatalog{}
			h, _ := newCatalogRouter(t, repo)

			rec := serve(t, h, "/v1/products?"+query)

			assertInvalidListParams(t, rec)
			assert.Empty(t, repo.gotParams, "repository must not be called")
		})
	}

	// Parsed but out of range: ListParams.Validate rejects them inside List, before any SQL.
	outOfRange := map[string]string{
		"limit_zero":          "limit=0",
		"limit_negative":      "limit=-5",
		"limit_over_max":      "limit=" + strconv.Itoa(product.MaxListLimit+1),
		"cursor_without_time": "cursor=" + cursorToken(`{"id":"3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"}`),
		"cursor_without_id":   "cursor=" + cursorToken(`{"created_at":"2026-09-27T10:00:00Z"}`),
	}
	for name, query := range outOfRange {
		t.Run("out_of_range_params_are_400/"+name, func(t *testing.T) {
			t.Parallel()
			repo := &fakeCatalog{}
			h, _ := newCatalogRouter(t, repo)

			assertInvalidListParams(t, serve(t, h, "/v1/products?"+query))
			require.Len(t, repo.gotParams, 1, "List must be reached and reject the params itself")
		})
	}

	t.Run("invalid_list_params_from_repository_is_400", func(t *testing.T) {
		t.Parallel()
		repo := &fakeCatalog{err: fmt.Errorf("%w: cursor id: parse uuid", product.ErrInvalidListParams)}
		h, _ := newCatalogRouter(t, repo)

		rec := serve(t, h, "/v1/products?cursor="+cursorToken(`{"created_at":"2026-09-27T10:00:00Z","id":"not-a-uuid"}`))

		assertInvalidListParams(t, rec)
	})

	t.Run("repository_error_is_500", func(t *testing.T) {
		t.Parallel()
		h, logs := newCatalogRouter(t, &fakeCatalog{err: errors.New("connection refused")})

		rec := serve(t, h, "/v1/products")

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.JSONEq(t, `{"error":"internal server error"}`, rec.Body.String())
		assert.Contains(t, logs.String(), "connection refused")
	})

	t.Run("unconfigured_service_is_503", func(t *testing.T) {
		t.Parallel()
		rec := serve(t, httpapi.NewRouter(httpapi.RouterConfig{}), "/v1/products")

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}

func TestGetProduct_Handler(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	p := testProduct("3f2b1c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d", createdAt)

	t.Run("found_is_200", func(t *testing.T) {
		t.Parallel()
		h, _ := newCatalogRouter(t, &fakeCatalog{products: map[product.ID]*product.Product{p.ID: p}})

		rec := serve(t, h, "/v1/products/"+string(p.ID))

		require.Equal(t, http.StatusOK, rec.Code)
		assertProductJSON(t, p, decodeBody[map[string]any](t, rec))
	})

	t.Run("not_found_is_404", func(t *testing.T) {
		t.Parallel()
		h, _ := newCatalogRouter(t, &fakeCatalog{})

		rec := serve(t, h, "/v1/products/"+string(p.ID))

		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.JSONEq(t, `{"error":"product not found"}`, rec.Body.String())
	})

	t.Run("repository_error_is_500", func(t *testing.T) {
		t.Parallel()
		h, logs := newCatalogRouter(t, &fakeCatalog{err: errors.New("connection refused")})

		rec := serve(t, h, "/v1/products/"+string(p.ID))

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Contains(t, logs.String(), "connection refused")
	})

	t.Run("unconfigured_service_is_503", func(t *testing.T) {
		t.Parallel()
		rec := serve(t, httpapi.NewRouter(httpapi.RouterConfig{}), "/v1/products/"+string(p.ID))

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}

// assertInvalidListParams checks that every rejected list request gets the same 400 body.
func assertInvalidListParams(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	want := fmt.Sprintf("invalid list parameters: limit must be 1..%d and cursor a next_cursor value",
		product.MaxListLimit)
	assert.Equal(t, want, decodeBody[map[string]string](t, rec)["error"])
}

func assertProductJSON(t *testing.T, want *product.Product, got map[string]any) {
	t.Helper()
	assert.Equal(t, map[string]any{
		"id":                 string(want.ID),
		"canonical_name":     want.CanonicalName,
		"description":        want.Description,
		"brand":              want.Brand,
		"origin_country":     want.OriginCountry,
		"status":             string(want.Status),
		"current_version_id": *want.CurrentVersionID,
		"created_at":         want.CreatedAt.Format(time.RFC3339Nano),
		"updated_at":         want.UpdatedAt.Format(time.RFC3339Nano),
	}, got)
}
