// Package testsupport holds setup shared by live tests in several packages.
package testsupport

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const liveSetupTimeout = 30 * time.Second

// LiveDB returns a pool on TEST_DATABASE_URL with every migration applied, closed at cleanup.
// Unset skips the test (verify.sh counts a skip as a failure); set but unreachable fails it.
// Messages never print the URL; pgx redacts the password in its own errors (best effort).
func LiveDB(t testing.TB) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), liveSetupTimeout)
	defer cancel()

	pool, err := postgres.NewPool(ctx, PoolConfig(url, 5))
	if err != nil {
		t.Fatalf("connect to TEST_DATABASE_URL: %v", err)
	}
	t.Cleanup(pool.Close)

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("create migrator: %v", err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return pool
}

// PoolConfig is a small valid pool configuration for live tests; maxConns is the one setting
// tests vary (concurrency tests need more than the default).
func PoolConfig(url string, maxConns int32) config.DatabaseConfig {
	return config.DatabaseConfig{
		URL:             url,
		MaxConns:        maxConns,
		MinConns:        1,
		MaxConnIdleTime: time.Minute,
		MaxConnLifetime: time.Hour,
		ConnectTimeout:  5 * time.Second,
	}
}

// Truncate empties tables now and again at cleanup, so a test starts from a known state and
// leaves none behind. Cleanup runs before the pool closes (cleanups run last-in first-out).
func Truncate(t testing.TB, pool *pgxpool.Pool, tables string) {
	t.Helper()
	truncate := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), liveSetupTimeout)
		defer cancel()
		_, err := pool.Exec(ctx, "TRUNCATE "+tables+" CASCADE")
		return err
	}
	if err := truncate(); err != nil {
		t.Fatalf("truncate %s: %v", tables, err)
	}
	t.Cleanup(func() {
		if err := truncate(); err != nil {
			t.Errorf("truncate %s at cleanup: %v", tables, err)
		}
	})
}

// InsertProduct stores a minimal product row created at createdAt and returns its id.
// Read-side tests use it to control created_at, including ties, which the ingestion path cannot.
func InsertProduct(t testing.TB, pool *pgxpool.Pool, name string, createdAt time.Time) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveSetupTimeout)
	defer cancel()
	var id string
	err := pool.QueryRow(ctx,
		`INSERT INTO products (canonical_name, created_at, updated_at) VALUES ($1, $2, $2) RETURNING id::text`,
		name, createdAt).Scan(&id)
	if err != nil {
		t.Fatalf("insert product %q: %v", name, err)
	}
	return id
}

// CatalogueOrder returns every product id in the order the catalogue lists them, computed
// independently of the code under test: created_at DESC, id DESC.
func CatalogueOrder(t testing.TB, pool *pgxpool.Pool) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), liveSetupTimeout)
	defer cancel()
	rows, err := pool.Query(ctx, `SELECT id::text FROM products ORDER BY created_at DESC, id DESC`)
	if err != nil {
		t.Fatalf("read catalogue order: %v", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read catalogue order: %v", err)
	}
	return ids
}
