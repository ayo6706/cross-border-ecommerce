package sources_test

import (
	"context"
	"errors"
	"testing"

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
	records, nextCP, err := f(ctx, req.Checkpoint.String())
	return ingestion.FetchResult{
		Records:        records,
		NextCheckpoint: ingestion.NewCheckpoint(nextCP),
		HasMore:        nextCP != "",
	}, err
}

func (f dummyFuncAdapter) FetchRecords(ctx context.Context, checkpoint string) ([]*ingestion.RawRecord, string, error) {
	return f(ctx, checkpoint)
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

		resolved, err := registry.Resolve(src)
		if err != nil {
			t.Fatalf("unexpected resolve error: %v", err)
		}
		if _, ok := resolved.(dummyAdapter); !ok {
			t.Errorf("expected custom dummyAdapter, got %T", resolved)
		}
	})

	t.Run("BuildDefaultAPIAdapter", func(t *testing.T) {
		registry := sources.NewRegistry()
		src, err := source.NewSource(
			source.ID("api-src"),
			"Supplier API",
			source.TypeAPI,
			map[string]any{
				"base_url":     "https://api.supplier.com/v1",
				"bearer_token": "token-123",
				"page_size":    50,
			},
			5,
		)
		if err != nil {
			t.Fatalf("unexpected source creation error: %v", err)
		}

		resolved, err := registry.Resolve(src)
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

		resolved, err := registry.Resolve(src)
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

		_, err = registry.Resolve(nil)
		if !errors.Is(err, source.ErrInvalidSourceState) {
			t.Errorf("expected ErrInvalidSourceState on nil source, got %v", err)
		}

		badFormatSrc, _ := source.NewSource("bad-src", "Bad Format", source.TypeFeed, map[string]any{
			"format":    "UNSUPPORTED_XML",
			"file_path": "/tmp/test.xml",
		}, 1)
		_, err = registry.Resolve(badFormatSrc)
		if err == nil {
			t.Error("expected error on unsupported feed format, got nil")
		}
	})
}
