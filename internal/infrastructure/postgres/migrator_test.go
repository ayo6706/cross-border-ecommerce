package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrator_ConstructorValidation(t *testing.T) {
	t.Parallel()

	memFS := fstest.MapFS{
		"000001_init.up.sql":   &fstest.MapFile{Data: []byte("SELECT 1;")},
		"000001_init.down.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
	}

	migrator, err := postgres.NewMigrator(nil, memFS)
	if migrator != nil {
		t.Fatalf("expected nil migrator with nil pool, got %v", migrator)
	}
	if !errors.Is(err, postgres.ErrNilPool) {
		t.Fatalf("expected ErrNilPool, got %v", err)
	}

	dummyPool := &pgxpool.Pool{}
	migrator2, err2 := postgres.NewMigrator(dummyPool, nil)
	if migrator2 != nil {
		t.Fatalf("expected nil migrator with nil fs, got %v", migrator2)
	}
	if !errors.Is(err2, postgres.ErrNilFS) {
		t.Fatalf("expected ErrNilFS, got %v", err2)
	}
}

func TestMigrator_DiscoverMigrations(t *testing.T) {
	t.Parallel()

	memFS := fstest.MapFS{
		"000002_create_outbox.up.sql":      &fstest.MapFile{Data: []byte("SELECT 2;")},
		"000002_create_outbox.down.sql":    &fstest.MapFile{Data: []byte("SELECT 2;")},
		"000001_create_catalogue.up.sql":   &fstest.MapFile{Data: []byte("SELECT 1;")},
		"000001_create_catalogue.down.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		"embed.go":                         &fstest.MapFile{Data: []byte("package migrations")},
		"README.txt":                       &fstest.MapFile{Data: []byte("migrations docs")},
	}

	dummyPool := &pgxpool.Pool{}
	migrator, err := postgres.NewMigrator(dummyPool, memFS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	discovered, err := migrator.DiscoverMigrations()
	if err != nil {
		t.Fatalf("unexpected error discovering migrations: %v", err)
	}

	if len(discovered) != 4 {
		t.Fatalf("expected 4 discovered migrations, got %d", len(discovered))
	}

	// Verify numeric sorting
	if discovered[0].Version != 1 || discovered[1].Version != 1 {
		t.Fatalf("expected first two migrations to be version 1, got %d and %d", discovered[0].Version, discovered[1].Version)
	}
	if discovered[2].Version != 2 || discovered[3].Version != 2 {
		t.Fatalf("expected last two migrations to be version 2, got %d and %d", discovered[2].Version, discovered[3].Version)
	}
}

func TestMigrator_DownStepValidation(t *testing.T) {
	t.Parallel()

	dummyPool := &pgxpool.Pool{}
	migrator, err := postgres.NewMigrator(dummyPool, migrations.FS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	errZero := migrator.Down(ctx, 0)
	if !errors.Is(errZero, postgres.ErrInvalidStep) {
		t.Fatalf("expected ErrInvalidStep for step 0, got %v", errZero)
	}

	errNeg := migrator.Down(ctx, -5)
	if !errors.Is(errNeg, postgres.ErrInvalidStep) {
		t.Fatalf("expected ErrInvalidStep for step -5, got %v", errNeg)
	}
}

func getTestDatabaseURL() string {
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://postgres:postgres@127.0.0.1:5433/crossborder_test?sslmode=disable"
}

func TestMigrator_LiveLifecycle(t *testing.T) {
	connStr := getTestDatabaseURL()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, connStr,
		postgres.WithConnectTimeout(3*time.Second),
		postgres.WithMaxConns(5),
		postgres.WithMinConns(1),
	)
	if err != nil {
		t.Skipf("skipping live database test: unable to connect to %s: %v", connStr, err)
		return
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("expected successful pool ping, got %v", err)
	}

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatalf("failed to create migrator: %v", err)
	}

	discovered, err := migrator.DiscoverMigrations()
	if err != nil {
		t.Fatalf("failed to discover migrations: %v", err)
	}
	if len(discovered) < 4 {
		t.Fatalf("expected at least 4 migration files (2 up, 2 down), got %d", len(discovered))
	}

	// Clean slate: rollback any existing migrations
	_ = migrator.Down(ctx, 100)

	// Step 1: Migrate Up to latest
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("migrator.Up failed: %v", err)
	}

	version, dirty, err := migrator.Version(ctx)
	if err != nil {
		t.Fatalf("migrator.Version failed: %v", err)
	}
	if dirty {
		t.Fatalf("expected clean state, got dirty=true")
	}
	if version != 2 {
		t.Fatalf("expected version 2, got %d", version)
	}

	// Step 2: Verify required tables exist
	requiredTables := []string{"sources", "products", "product_versions", "outbox_events", "schema_migrations"}
	for _, table := range requiredTables {
		var exists bool
		query := `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' AND table_name = $1
		);`
		if err := pool.QueryRow(ctx, query, table).Scan(&exists); err != nil {
			t.Fatalf("query table %s existence failed: %v", table, err)
		}
		if !exists {
			t.Fatalf("expected table %s to exist after migration", table)
		}
	}

	// Step 3: Run Up again (idempotent)
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("re-running migrator.Up should succeed idempotently, got: %v", err)
	}

	// Step 4: Rollback 1 step (outbox_events)
	if err := migrator.Down(ctx, 1); err != nil {
		t.Fatalf("migrator.Down(1) failed: %v", err)
	}

	version, dirty, err = migrator.Version(ctx)
	if err != nil {
		t.Fatalf("migrator.Version after rollback failed: %v", err)
	}
	if dirty || version != 1 {
		t.Fatalf("expected clean version 1 after 1 rollback step, got version=%d dirty=%v", version, dirty)
	}

	// Verify outbox_events was dropped, but products and sources remain
	var outboxExists bool
	_ = pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'outbox_events');`).Scan(&outboxExists)
	if outboxExists {
		t.Fatalf("expected outbox_events to be dropped after rollback")
	}

	var productsExists bool
	_ = pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'products');`).Scan(&productsExists)
	if !productsExists {
		t.Fatalf("expected products table to still exist after rolling back outbox_events")
	}

	// Step 5: Rollback remaining step
	if err := migrator.Down(ctx, 1); err != nil {
		t.Fatalf("migrator.Down(1) remaining failed: %v", err)
	}

	version, dirty, err = migrator.Version(ctx)
	if err != nil {
		t.Fatalf("migrator.Version after full rollback failed: %v", err)
	}
	if version != 0 {
		t.Fatalf("expected version 0 after full rollback, got %d", version)
	}

	_ = pool.QueryRow(ctx, `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'products');`).Scan(&productsExists)
	if productsExists {
		t.Fatalf("expected products table to be dropped after full rollback")
	}

	// Step 6: Re-apply all migrations to leave database in migrated state
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("re-applying all migrations failed: %v", err)
	}

	version, _, err = migrator.Version(ctx)
	if err != nil || version != 2 {
		t.Fatalf("expected final version 2, got %d (err: %v)", version, err)
	}
}
