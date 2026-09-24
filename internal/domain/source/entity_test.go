package source_test

import (
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

func TestSource_NewSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		id        source.ID
		srcName   string
		srcType   source.Type
		config    map[string]any
		rateLimit int
		wantErr   error
	}{
		{
			name:      "valid API source",
			id:        "src-api-01",
			srcName:   "Supplier API",
			srcType:   source.TypeAPI,
			config:    map[string]any{"base_url": "https://api.supplier.com"},
			rateLimit: 120,
			wantErr:   nil,
		},
		{
			name:      "valid FEED source",
			id:        "src-feed-01",
			srcName:   "Product CSV Feed",
			srcType:   source.TypeFeed,
			config:    map[string]any{"file_path": "/data/feed.csv", "format": "csv"},
			rateLimit: 60,
			wantErr:   nil,
		},
		{
			name:      "FEED source without file_path",
			id:        "src-feed-02",
			srcName:   "Product CSV Feed",
			srcType:   source.TypeFeed,
			config:    nil,
			rateLimit: 60,
			wantErr:   source.ErrInvalidSourceConfig,
		},
		{
			name:      "valid API source with bearer secret reference",
			id:        "src-api-02",
			srcName:   "Supplier API",
			srcType:   source.TypeAPI,
			config:    map[string]any{"base_url": "https://api.supplier.com", "auth_kind": "bearer", "auth_ref": "env:SUPPLIER_TOKEN"},
			rateLimit: 120,
			wantErr:   nil,
		},
		{
			name:      "API source with literal secret",
			id:        "src-api-03",
			srcName:   "Supplier API",
			srcType:   source.TypeAPI,
			config:    map[string]any{"base_url": "https://api.supplier.com", "auth_kind": "bearer", "auth_ref": "raw-token"},
			rateLimit: 120,
			wantErr:   source.ErrInvalidSecretRef,
		},
		{
			name:      "API source with auth_ref but no auth_kind",
			id:        "src-api-04",
			srcName:   "Supplier API",
			srcType:   source.TypeAPI,
			config:    map[string]any{"base_url": "https://api.supplier.com", "auth_ref": "env:SUPPLIER_TOKEN"},
			rateLimit: 120,
			wantErr:   source.ErrInvalidSourceConfig,
		},
		{
			name:      "API source with unsupported auth_kind",
			id:        "src-api-05",
			srcName:   "Supplier API",
			srcType:   source.TypeAPI,
			config:    map[string]any{"base_url": "https://api.supplier.com", "auth_kind": "oauth"},
			rateLimit: 120,
			wantErr:   source.ErrInvalidSourceConfig,
		},
		{
			name:      "API source with relative base_url",
			id:        "src-api-06",
			srcName:   "Supplier API",
			srcType:   source.TypeAPI,
			config:    map[string]any{"base_url": "api.supplier.com/v1"},
			rateLimit: 120,
			wantErr:   source.ErrInvalidSourceConfig,
		},
		{
			name:      "empty ID error",
			id:        "",
			srcName:   "Supplier API",
			srcType:   source.TypeAPI,
			config:    nil,
			rateLimit: 100,
			wantErr:   source.ErrInvalidSourceID,
		},
		{
			name:      "empty name error",
			id:        "src-02",
			srcName:   "",
			srcType:   source.TypeAPI,
			config:    nil,
			rateLimit: 100,
			wantErr:   source.ErrInvalidSourceName,
		},
		{
			name:      "unrecognized type error",
			id:        "src-03",
			srcName:   "Unknown Source",
			srcType:   source.Type("INVALID"),
			config:    nil,
			rateLimit: 100,
			wantErr:   source.ErrInvalidSourceType,
		},
		{
			name:      "zero rate limit error",
			id:        "src-04",
			srcName:   "Supplier API",
			srcType:   source.TypeAPI,
			config:    nil,
			rateLimit: 0,
			wantErr:   source.ErrInvalidRateLimit,
		},
		{
			name:      "negative rate limit error",
			id:        "src-05",
			srcName:   "Supplier API",
			srcType:   source.TypeAPI,
			config:    nil,
			rateLimit: -10,
			wantErr:   source.ErrInvalidRateLimit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src, err := source.NewSource(tt.id, tt.srcName, tt.srcType, tt.config, tt.rateLimit)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				if src != nil {
					t.Fatalf("expected nil source on error, got %v", src)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if src.ID != tt.id {
				t.Fatalf("expected ID %s, got %s", tt.id, src.ID)
			}
			if !src.Enabled {
				t.Fatalf("expected new source to be enabled")
			}
			if src.Config == nil {
				t.Fatalf("expected config map to be initialized")
			}
		})
	}
}

func TestSource_Mutations(t *testing.T) {
	t.Parallel()

	src, err := source.NewSource("src-01", "Supplier 1", source.TypeAPI, map[string]any{"base_url": "https://api.example.com"}, 100)
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}

	src.Disable()
	if src.Enabled {
		t.Fatalf("expected source to be disabled")
	}

	src.Enable()
	if !src.Enabled {
		t.Fatalf("expected source to be enabled")
	}

	if err := src.SetRateLimit(250); err != nil {
		t.Fatalf("unexpected error setting rate limit: %v", err)
	}
	if src.RateLimitPerSecond != 250 {
		t.Fatalf("expected rate limit 250, got %d", src.RateLimitPerSecond)
	}

	if err := src.SetRateLimit(0); !errors.Is(err, source.ErrInvalidRateLimit) {
		t.Fatalf("expected ErrInvalidRateLimit for 0, got %v", err)
	}
	if err := src.SetRateLimit(-50); !errors.Is(err, source.ErrInvalidRateLimit) {
		t.Fatalf("expected ErrInvalidRateLimit for negative, got %v", err)
	}
}

func TestSource_FieldMappingValidation(t *testing.T) {
	t.Parallel()

	validCfg := map[string]any{
		"base_url": "https://api.example.com",
		"field_mapping": map[string]any{
			"name_path": "title",
			"attribute_paths": map[string]any{
				"color": "details.color",
			},
		},
	}
	src, err := source.NewSource("src-01", "Supplier 1", source.TypeAPI, validCfg, 100)
	if err != nil {
		t.Fatalf("expected valid source with field_mapping, got: %v", err)
	}
	apiCfg, err := src.ParseAPIConfig()
	if err != nil {
		t.Fatalf("expected ParseAPIConfig to succeed, got: %v", err)
	}
	if apiCfg.FieldMapping == nil || apiCfg.FieldMapping.NamePath != "title" {
		t.Fatalf("expected FieldMapping to be parsed with name_path 'title'")
	}

	missingNameCfg := map[string]any{
		"base_url": "https://api.example.com",
		"field_mapping": map[string]any{
			"description_path": "desc",
		},
	}
	_, err = source.NewSource("src-02", "Supplier 2", source.TypeAPI, missingNameCfg, 100)
	if err == nil {
		t.Fatal("expected error for field_mapping with missing name_path")
	}
	if !errors.Is(err, source.ErrInvalidSourceConfig) {
		t.Fatalf("expected ErrInvalidSourceConfig, got: %v", err)
	}

	duplicateAttrCfg := map[string]any{
		"base_url": "https://api.example.com",
		"field_mapping": map[string]any{
			"name_path": "title",
			"attribute_paths": map[string]any{
				"Color": "details.color1",
				"color": "details.color2",
			},
		},
	}
	_, err = source.NewSource("src-03", "Supplier 3", source.TypeAPI, duplicateAttrCfg, 100)
	if err == nil {
		t.Fatal("expected error for field_mapping with colliding attribute keys")
	}
	if !errors.Is(err, source.ErrInvalidSourceConfig) {
		t.Fatalf("expected ErrInvalidSourceConfig, got: %v", err)
	}

	invalidPathCfg := map[string]any{
		"base_url": "https://api.example.com",
		"field_mapping": map[string]any{
			"name_path": "title[0",
		},
	}
	_, err = source.NewSource("src-04", "Supplier 4", source.TypeAPI, invalidPathCfg, 100)
	if err == nil {
		t.Fatal("expected error for field_mapping with invalid path syntax")
	}
	if !errors.Is(err, source.ErrInvalidSourceConfig) {
		t.Fatalf("expected ErrInvalidSourceConfig, got: %v", err)
	}

	nonStringAttrCfg := map[string]any{
		"base_url": "https://api.example.com",
		"field_mapping": map[string]any{
			"name_path": "title",
			"attribute_paths": map[string]any{
				"color": 123,
			},
		},
	}
	_, err = source.NewSource("src-05", "Supplier 5", source.TypeAPI, nonStringAttrCfg, 100)
	if err == nil {
		t.Fatal("expected error for non-string entry in attribute_paths")
	}
	if !errors.Is(err, source.ErrInvalidSourceConfig) {
		t.Fatalf("expected ErrInvalidSourceConfig, got: %v", err)
	}

	nonObjectAttrCfg := map[string]any{
		"base_url": "https://api.example.com",
		"field_mapping": map[string]any{
			"name_path":       "title",
			"attribute_paths": "not-an-object",
		},
	}
	_, err = source.NewSource("src-06", "Supplier 6", source.TypeAPI, nonObjectAttrCfg, 100)
	if err == nil {
		t.Fatal("expected error for non-object attribute_paths")
	}
	if !errors.Is(err, source.ErrInvalidSourceConfig) {
		t.Fatalf("expected ErrInvalidSourceConfig, got: %v", err)
	}
}
