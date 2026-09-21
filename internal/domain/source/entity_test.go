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
			name:      "valid FEED source with nil config initialized",
			id:        "src-feed-01",
			srcName:   "Product CSV Feed",
			srcType:   source.TypeFeed,
			config:    nil,
			rateLimit: 60,
			wantErr:   nil,
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

	src, err := source.NewSource("src-01", "Supplier 1", source.TypeAPI, nil, 100)
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}

	// Disable
	src.Disable()
	if src.Enabled {
		t.Fatalf("expected source to be disabled")
	}

	// Enable
	src.Enable()
	if !src.Enabled {
		t.Fatalf("expected source to be enabled")
	}

	// Set valid rate limit
	if err := src.SetRateLimit(250); err != nil {
		t.Fatalf("unexpected error setting rate limit: %v", err)
	}
	if src.RateLimit != 250 {
		t.Fatalf("expected rate limit 250, got %d", src.RateLimit)
	}

	// Set invalid rate limit
	if err := src.SetRateLimit(0); !errors.Is(err, source.ErrInvalidRateLimit) {
		t.Fatalf("expected ErrInvalidRateLimit for 0, got %v", err)
	}
	if err := src.SetRateLimit(-50); !errors.Is(err, source.ErrInvalidRateLimit) {
		t.Fatalf("expected ErrInvalidRateLimit for negative, got %v", err)
	}
}
