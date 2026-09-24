package sources_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type dummyAdapter struct{}

func (dummyAdapter) Fetch(ctx context.Context, req ingestion.FetchRequest) (ingestion.FetchResult, error) {
	return ingestion.FetchResult{}, nil
}

type dummyFuncAdapter func(ctx context.Context, checkpoint string) ([]*ingestion.RawRecord, string, error)

func (f dummyFuncAdapter) Fetch(ctx context.Context, req ingestion.FetchRequest) (ingestion.FetchResult, error) {
	records, nextCP, err := f(ctx, req.Checkpoint)
	return ingestion.FetchResult{
		Records:        records,
		NextCheckpoint: nextCP,
		HasMore:        nextCP != "",
	}, err
}

func TestRegistry_RegisterAndResolve(t *testing.T) {
	t.Run("ExplicitRegistrationTakesPrecedence", func(t *testing.T) {
		registry := sources.NewRegistry()
		custom := dummyAdapter{}

		srcID := source.ID("custom-src")
		if err := registry.Register(srcID, custom); err != nil {
			t.Fatalf("unexpected register error: %v", err)
		}

		src, err := source.NewSource(srcID, "Custom Source", source.TypeAPI, map[string]any{"base_url": "https://custom.com"}, 10)
		if err != nil {
			t.Fatalf("unexpected source error: %v", err)
		}

		resolved, err := registry.Resolve(context.Background(), src)
		if err != nil {
			t.Fatalf("unexpected resolve error: %v", err)
		}
		if _, ok := resolved.(dummyAdapter); !ok {
			t.Errorf("expected custom dummyAdapter, got %T", resolved)
		}
	})

	t.Run("BuildDefaultAPIAdapter", func(t *testing.T) {
		t.Setenv("TEST_REGISTRY_TOKEN", "token-123")
		registry := sources.NewRegistry()
		src, err := source.NewSource(
			source.ID("api-src"),
			"Supplier API",
			source.TypeAPI,
			map[string]any{
				"base_url":  "https://api.supplier.com/v1",
				"auth_kind": "bearer",
				"auth_ref":  "env:TEST_REGISTRY_TOKEN",
				"page_size": 50,
			},
			5,
		)
		if err != nil {
			t.Fatalf("unexpected source creation error: %v", err)
		}

		resolved, err := registry.Resolve(context.Background(), src)
		if err != nil {
			t.Fatalf("unexpected resolve error: %v", err)
		}
		if _, ok := resolved.(*sources.RESTAdapter); !ok {
			t.Errorf("expected *RESTAdapter, got %T", resolved)
		}
	})

	t.Run("BuildDefaultFeedAdapter", func(t *testing.T) {
		registry := sources.NewRegistry()
		src, err := source.NewSource(
			source.ID("feed-src"),
			"Feed File",
			source.TypeFeed,
			map[string]any{
				"file_path":  "/tmp/feed.csv",
				"format":     "CSV",
				"batch_size": 200,
			},
			1,
		)
		if err != nil {
			t.Fatalf("unexpected source error: %v", err)
		}

		resolved, err := registry.Resolve(context.Background(), src)
		if err != nil {
			t.Fatalf("unexpected resolve error: %v", err)
		}
		if _, ok := resolved.(*sources.FeedFileAdapter); !ok {
			t.Errorf("expected *FeedFileAdapter, got %T", resolved)
		}
	})

	t.Run("ValidationErrors", func(t *testing.T) {
		registry := sources.NewRegistry()

		err := registry.Register("", dummyAdapter{})
		if !errors.Is(err, ingestion.ErrInvalidSourceID) {
			t.Errorf("expected ErrInvalidSourceID on empty id, got %v", err)
		}

		err = registry.Register("id-1", nil)
		if err == nil {
			t.Error("expected error on nil adapter, got nil")
		}

		var typedNilAdapter *sources.RESTAdapter
		err = registry.Register("id-typed-nil", typedNilAdapter)
		if err == nil {
			t.Error("expected error on typed nil adapter, got nil")
		}

		var nilFuncAdapter dummyFuncAdapter
		err = registry.Register("id-nil-func", nilFuncAdapter)
		if err == nil {
			t.Error("expected error on nil func adapter, got nil")
		}

		_, err = registry.Resolve(context.Background(), nil)
		if !errors.Is(err, source.ErrInvalidSourceState) {
			t.Errorf("expected ErrInvalidSourceState on nil source, got %v", err)
		}

		_, err = source.NewSource("bad-src", "Bad Format", source.TypeFeed, map[string]any{
			"format":    "UNSUPPORTED_XML",
			"file_path": "/tmp/test.xml",
		}, 1)
		if !errors.Is(err, source.ErrInvalidSourceConfig) {
			t.Errorf("expected ErrInvalidSourceConfig on unsupported feed format, got %v", err)
		}
	})

	t.Run("RateLimiterSharedPerSource", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[]`))
		}))
		defer server.Close()

		registry := sources.NewRegistry()
		// Rate limit 1 req/sec (burst 1)
		src, _ := source.NewSource("shared-rl-src", "API with RL", source.TypeAPI, map[string]any{
			"base_url": server.URL,
		}, 1)

		adapter1, err := registry.Resolve(context.Background(), src)
		if err != nil {
			t.Fatalf("unexpected resolve error: %v", err)
		}
		adapter2, err := registry.Resolve(context.Background(), src)
		if err != nil {
			t.Fatalf("unexpected resolve error 2: %v", err)
		}

		// First adapter consumes the 1 burst token
		ctx := context.Background()
		_, err = adapter1.Fetch(ctx, ingestion.FetchRequest{})
		if err != nil {
			t.Fatalf("adapter1 fetch failed: %v", err)
		}

		// Second adapter on the same source should be throttled because token was consumed by adapter1
		shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err = adapter2.Fetch(shortCtx, ingestion.FetchRequest{})
		if err == nil {
			t.Fatal("expected adapter2 to be throttled due to shared rate limiter budget, but call succeeded immediately")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded from throttle wait, got: %v", err)
		}
	})

	t.Run("CacheInvalidationOnUpdatedAtChange", func(t *testing.T) {
		registry := sources.NewRegistry()
		t1 := time.Now().UTC()
		src, err := source.NewSource("cache-src", "API V1", source.TypeAPI, map[string]any{
			"base_url": "https://api.v1.com",
		}, 10)
		if err != nil {
			t.Fatalf("unexpected source error: %v", err)
		}
		src.UpdatedAt = t1

		ad1, err := registry.Resolve(context.Background(), src)
		if err != nil {
			t.Fatalf("unexpected resolve error: %v", err)
		}

		// Resolve with same UpdatedAt should return same adapter pointer
		ad2, err := registry.Resolve(context.Background(), src)
		if err != nil {
			t.Fatalf("unexpected resolve error 2: %v", err)
		}
		if ad1 != ad2 {
			t.Errorf("expected same adapter instance, got %p vs %p", ad1, ad2)
		}

		// Update source configuration and UpdatedAt
		src.Config = map[string]any{"base_url": "https://api.v2.com"}
		src.UpdatedAt = t1.Add(time.Minute)

		ad3, err := registry.Resolve(context.Background(), src)
		if err != nil {
			t.Fatalf("unexpected resolve error 3: %v", err)
		}
		if ad1 == ad3 {
			t.Errorf("expected different adapter instance after UpdatedAt bump, got same pointer %p", ad1)
		}
	})
}

func TestRegistry_ResolvesCredentialReferences(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	newBearerSource := func(t *testing.T, ref string) *source.Source {
		t.Helper()
		src, err := source.NewSource("bearer-src", "Bearer API", source.TypeAPI, map[string]any{
			"base_url":  server.URL,
			"auth_kind": "bearer",
			"auth_ref":  ref,
		}, 100)
		if err != nil {
			t.Fatalf("unexpected source error: %v", err)
		}
		return src
	}

	t.Run("BearerTokenResolvedFromEnvironment", func(t *testing.T) {
		t.Setenv("TEST_BEARER_TOKEN", "s3cret")

		adapter, err := sources.NewRegistry().Resolve(context.Background(), newBearerSource(t, "env:TEST_BEARER_TOKEN"))
		if err != nil {
			t.Fatalf("unexpected resolve error: %v", err)
		}
		if _, err := adapter.Fetch(context.Background(), ingestion.FetchRequest{}); err != nil {
			t.Fatalf("unexpected fetch error: %v", err)
		}
		if gotAuth != "Bearer s3cret" {
			t.Errorf("expected Authorization header from env reference, got %q", gotAuth)
		}
	})

	t.Run("MissingSecretFailsResolve", func(t *testing.T) {
		_, err := sources.NewRegistry().Resolve(context.Background(), newBearerSource(t, "env:TEST_BEARER_TOKEN_UNSET"))
		if err == nil {
			t.Fatal("expected resolve to fail when the referenced secret is not set")
		}
	})

	t.Run("LiteralSecretRejectedAtSourceCreation", func(t *testing.T) {
		_, err := source.NewSource("literal-src", "Literal Secret", source.TypeAPI, map[string]any{
			"base_url":  server.URL,
			"auth_kind": "bearer",
			"auth_ref":  "plain-text-token",
		}, 100)
		if !errors.Is(err, source.ErrInvalidSourceConfig) || !errors.Is(err, source.ErrInvalidSecretRef) {
			t.Fatalf("expected ErrInvalidSourceConfig wrapping ErrInvalidSecretRef, got %v", err)
		}
	})
}
