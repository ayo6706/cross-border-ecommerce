package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
)

func validPoolConfig(url string) config.DatabaseConfig {
	return config.DatabaseConfig{
		URL:             url,
		MaxConns:        5,
		MinConns:        1,
		MaxConnIdleTime: time.Minute,
		MaxConnLifetime: time.Hour,
		ConnectTimeout:  100 * time.Millisecond,
	}
}

// NewPool has no fallbacks: every invalid setting is rejected by config's rules.
func TestNewPool_RejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modify  func(*config.DatabaseConfig)
		wantErr error // nil: any error (pgx parse error)
	}{
		{"empty url", func(c *config.DatabaseConfig) { c.URL = "" }, config.ErrEmptyDatabaseURL},
		{"zero max conns", func(c *config.DatabaseConfig) { c.MaxConns = 0 }, config.ErrInvalidPoolLimits},
		{"negative min conns", func(c *config.DatabaseConfig) { c.MinConns = -1 }, config.ErrInvalidPoolLimits},
		{"min above max", func(c *config.DatabaseConfig) { c.MinConns = 10 }, config.ErrInvalidPoolLimits},
		{"zero connect timeout", func(c *config.DatabaseConfig) { c.ConnectTimeout = 0 }, config.ErrInvalidTimeout},
		{"zero idle time", func(c *config.DatabaseConfig) { c.MaxConnIdleTime = 0 }, config.ErrInvalidTimeout},
		{"zero lifetime", func(c *config.DatabaseConfig) { c.MaxConnLifetime = 0 }, config.ErrInvalidTimeout},
		{"malformed url", func(c *config.DatabaseConfig) { c.URL = "invalid-conn-string://" }, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validPoolConfig("postgres://localhost:5432/test")
			tc.modify(&cfg)

			pool, err := postgres.NewPool(context.Background(), cfg)
			if pool != nil {
				pool.Close()
				t.Fatal("expected no pool")
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error wrapping %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestNewPool_UnreachableHostFailsWithinConnectTimeout(t *testing.T) {
	t.Parallel()

	start := time.Now()
	pool, err := postgres.NewPool(context.Background(),
		validPoolConfig("postgres://user:pass@127.0.0.1:54329/nonexistent?sslmode=disable"))
	if pool != nil {
		pool.Close()
		t.Fatal("expected no pool on unreachable host")
	}
	if err == nil {
		t.Fatal("expected ping error on unreachable host, got nil")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("NewPool took %v; the 100ms connect timeout was not applied", elapsed)
	}
}
