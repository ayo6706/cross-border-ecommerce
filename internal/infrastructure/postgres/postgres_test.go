package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
)

func TestNewPool_ConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ctx         context.Context
		connString  string
		opts        []postgres.Option
		expectedErr error
	}{
		{
			name:        "nil context",
			ctx:         nil,
			connString:  "postgres://localhost:5432/test",
			expectedErr: postgres.ErrNilContext,
		},
		{
			name:        "empty connection string",
			ctx:         context.Background(),
			connString:  "",
			expectedErr: postgres.ErrEmptyConnString,
		},
		{
			name:       "invalid max conns",
			ctx:        context.Background(),
			connString: "postgres://localhost:5432/test",
			opts: []postgres.Option{
				postgres.WithMaxConns(0),
			},
			expectedErr: postgres.ErrInvalidConfig,
		},
		{
			name:       "negative min conns",
			ctx:        context.Background(),
			connString: "postgres://localhost:5432/test",
			opts: []postgres.Option{
				postgres.WithMinConns(-1),
			},
			expectedErr: postgres.ErrInvalidConfig,
		},
		{
			name:       "min conns exceeds max conns",
			ctx:        context.Background(),
			connString: "postgres://localhost:5432/test",
			opts: []postgres.Option{
				postgres.WithMaxConns(10),
				postgres.WithMinConns(20),
			},
			expectedErr: postgres.ErrMinConnsExceedsMax,
		},
		{
			name:       "invalid connect timeout",
			ctx:        context.Background(),
			connString: "postgres://localhost:5432/test",
			opts: []postgres.Option{
				postgres.WithConnectTimeout(0),
			},
			expectedErr: postgres.ErrInvalidConfig,
		},
		{
			name:        "malformed connection string",
			ctx:         context.Background(),
			connString:  "invalid-conn-string://",
			expectedErr: nil, // pgxpool returns a parse error
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pool, err := postgres.NewPool(tc.ctx, tc.connString, tc.opts...)
			if pool != nil {
				pool.Close()
				t.Fatalf("expected pool to be nil, got %v", pool)
			}
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if tc.expectedErr != nil && !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestNewPool_UnreachableHost(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	connStr := "postgres://user:pass@127.0.0.1:54329/nonexistent?sslmode=disable"
	pool, err := postgres.NewPool(ctx, connStr,
		postgres.WithConnectTimeout(100*time.Millisecond),
		postgres.WithMaxConnIdleTime(1*time.Minute),
		postgres.WithMaxConnLifetime(5*time.Minute),
		postgres.WithHealthCheckPeriod(30*time.Second),
	)

	if pool != nil {
		pool.Close()
		t.Fatalf("expected pool to be nil on unreachable host, got %v", pool)
	}
	if err == nil {
		t.Fatalf("expected ping error on unreachable host, got nil")
	}
}
