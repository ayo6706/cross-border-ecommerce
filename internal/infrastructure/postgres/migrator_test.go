package postgres_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/testsupport"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
	"github.com/jackc/pgx/v5"
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

func getTestDatabaseURL(t *testing.T) string {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping live postgres migration test")
	}
	return url
}

func TestMigrator_LiveLifecycle(t *testing.T) {
	connStr := getTestDatabaseURL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(ctx, testsupport.PoolConfig(connStr, 5))
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
	if len(discovered) < 14 {
		t.Fatalf("expected at least 14 migration files (7 up, 7 down), got %d", len(discovered))
	}
	latestVersion := discovered[len(discovered)-1].Version

	// Clean slate: rollback any existing migrations
	_ = migrator.Down(ctx, 100)

	// Step 1: Migrate Up to latest
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("migrator.Up failed: %v", err)
	}
	assertMigrationVersion(ctx, t, migrator, latestVersion)

	indexesAfterUp := indexDefinitions(ctx, t, pool)

	// Step 2: Verify required tables exist
	requiredTables := []string{"sources", "products", "product_versions", "outbox_events", "ingestion_runs", "raw_records", "schema_migrations", "idempotency_keys", "dlq_messages"}
	for _, table := range requiredTables {
		if !tableExists(ctx, t, pool, table) {
			t.Fatalf("expected table %s to exist after migration", table)
		}
	}

	// Step 3: Run Up again (idempotent)
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("re-running migrator.Up should succeed idempotently, got: %v", err)
	}
	assertMigrationVersion(ctx, t, migrator, latestVersion)

	// Step 4: Roll back to version 3 (drops raw_records and everything after it)
	if err := migrator.Down(ctx, int(latestVersion-3)); err != nil {
		t.Fatalf("migrator.Down to version 3 failed: %v", err)
	}
	assertMigrationVersion(ctx, t, migrator, 3)

	if tableExists(ctx, t, pool, "raw_records") {
		t.Fatalf("expected raw_records to be dropped after rollback")
	}
	if !tableExists(ctx, t, pool, "ingestion_runs") {
		t.Fatalf("expected ingestion_runs table to still exist after rolling back raw_records")
	}

	// Step 5: Rollback remaining steps
	if err := migrator.Down(ctx, 3); err != nil {
		t.Fatalf("migrator.Down(3) remaining failed: %v", err)
	}
	assertMigrationVersion(ctx, t, migrator, 0)

	if tableExists(ctx, t, pool, "products") {
		t.Fatalf("expected products table to be dropped after full rollback")
	}

	// Step 6: Re-apply all migrations to leave database in migrated state
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("re-applying all migrations failed: %v", err)
	}
	assertMigrationVersion(ctx, t, migrator, latestVersion)
	if got := indexDefinitions(ctx, t, pool); !slices.Equal(got, indexesAfterUp) {
		t.Fatalf("index set after up -> down -> up differs:\nfirst: %v\nagain: %v", indexesAfterUp, got)
	}
}

// indexDefinitions returns every index definition in the public schema, sorted, and fails the
// test if any index is invalid (a half-built concurrent index).
func indexDefinitions(ctx context.Context, t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	var invalid int
	const countInvalid = `SELECT count(*) FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public' AND NOT i.indisvalid`
	if err := pool.QueryRow(ctx, countInvalid).Scan(&invalid); err != nil {
		t.Fatalf("count invalid indexes: %v", err)
	}
	if invalid != 0 {
		t.Fatalf("%d invalid index(es) in public schema", invalid)
	}
	rows, err := pool.Query(ctx, "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexdef")
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	defs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("scan indexes: %v", err)
	}
	return defs
}

func assertMigrationVersion(ctx context.Context, t *testing.T, migrator *postgres.Migrator, want int64) {
	t.Helper()

	got, err := migrator.Version(ctx)
	if err != nil {
		t.Fatalf("migrator.Version failed: %v", err)
	}
	if got != want {
		t.Fatalf("expected migration version %d, got %d", want, got)
	}
}

func tableExists(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) bool {
	t.Helper()

	const query = `SELECT EXISTS (
		SELECT FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = $1
	)`
	var exists bool
	if err := pool.QueryRow(ctx, query, table).Scan(&exists); err != nil {
		t.Fatalf("query table %s existence failed: %v", table, err)
	}
	return exists
}

func TestMigrator_DiscoverMigrations_RejectsMalformedSQLFileName(t *testing.T) {
	t.Parallel()

	memFS := fstest.MapFS{
		"000001_init.up.sql":    &fstest.MapFile{Data: []byte("SELECT 1;")},
		"000001_init.down.sql":  &fstest.MapFile{Data: []byte("SELECT 1;")},
		"0002-add index.up.sql": &fstest.MapFile{Data: []byte("SELECT 2;")},
	}
	migrator, err := postgres.NewMigrator(&pgxpool.Pool{}, memFS)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = migrator.DiscoverMigrations()
	if !errors.Is(err, postgres.ErrMalformedMigrationName) {
		t.Fatalf("a .sql file that does not match the naming pattern must fail discovery, got %v", err)
	}
}

func TestMigrator_DiscoverMigrations_NoTransactionDirective(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		script   string
		wantErr  error
		wantNoTx bool
	}{
		"single concurrent statement": {
			script:   "-- migrate:no-transaction\nCREATE INDEX CONCURRENTLY IF NOT EXISTS idx_a ON t (a);\n",
			wantNoTx: true,
		},
		"semicolon inside a string and a comment is not a separator": {
			script:   "-- migrate:no-transaction\n-- drops; nothing else\nCOMMENT ON TABLE t IS 'a;b';",
			wantNoTx: true,
		},
		"two statements": {
			script:  "-- migrate:no-transaction\nCREATE INDEX CONCURRENTLY idx_a ON t (a);\nCREATE INDEX CONCURRENTLY idx_b ON t (b);",
			wantErr: postgres.ErrInvalidNoTxMigration,
		},
		"empty body": {
			script:  "-- migrate:no-transaction\n-- nothing here\n",
			wantErr: postgres.ErrInvalidNoTxMigration,
		},
		"dollar quoting is refused": {
			script:  "-- migrate:no-transaction\nDO $$ BEGIN PERFORM 1; END $$;",
			wantErr: postgres.ErrInvalidNoTxMigration,
		},
		"CRLF line endings": {
			script:   "-- migrate:no-transaction\r\nDROP INDEX CONCURRENTLY IF EXISTS idx_a;\r\n",
			wantNoTx: true,
		},
		"leading byte order mark": {
			script:   "\ufeff-- migrate:no-transaction\nDROP INDEX CONCURRENTLY IF EXISTS idx_a;",
			wantNoTx: true,
		},
		"doubled quotes and quoted identifiers": {
			script:   "-- migrate:no-transaction\nCOMMENT ON COLUMN \"we;ird\".c IS 'it''s; fine';",
			wantNoTx: true,
		},
		"semicolons in nested block comments": {
			script:   "-- migrate:no-transaction\n/* a; /* b; */ c; */ DROP INDEX CONCURRENTLY IF EXISTS idx_a;",
			wantNoTx: true,
		},
		"a second statement after a block comment": {
			script:  "-- migrate:no-transaction\nDROP INDEX idx_a; /* ; */ DROP INDEX idx_b;",
			wantErr: postgres.ErrInvalidNoTxMigration,
		},
		"directive not on the first line is a normal migration": {
			script: "SELECT 1;\n-- migrate:no-transaction\nSELECT 2;",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			memFS := fstest.MapFS{"000001_idx.up.sql": &fstest.MapFile{Data: []byte(tc.script)}}
			migrator, err := postgres.NewMigrator(&pgxpool.Pool{}, memFS)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got, err := migrator.DiscoverMigrations()
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("want %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got[0].NoTransaction != tc.wantNoTx {
				t.Fatalf("NoTransaction = %v, want %v", got[0].NoTransaction, tc.wantNoTx)
			}
		})
	}
}

// TestMigrator_NoTransaction_Live runs no-transaction migrations in a private schema so the
// shared schema_migrations table is untouched. The steps depend on each other and run in order.
func TestMigrator_NoTransaction_Live(t *testing.T) {
	connStr := getTestDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const schema = "migrator_notx_test"
	admin := openLivePool(ctx, t, connStr)
	mustExec(ctx, t, admin, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	mustExec(ctx, t, admin, "CREATE SCHEMA "+schema)
	t.Cleanup(func() { mustExec(context.Background(), t, admin, "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })

	pool := openLivePool(ctx, t, withSearchPath(connStr, schema))
	mustExec(ctx, t, pool, "CREATE TABLE t (a INT NOT NULL)")
	mustExec(ctx, t, pool, "INSERT INTO t (a) VALUES (1), (1)")

	up := func(script string) error {
		migrator, err := postgres.NewMigrator(pool, fstest.MapFS{
			"000001_uq_t_a.up.sql": &fstest.MapFile{Data: []byte("-- migrate:no-transaction\n" + script)},
		})
		if err != nil {
			t.Fatalf("new migrator: %v", err)
		}
		return migrator.Up(ctx)
	}
	version := func() int64 {
		migrator, err := postgres.NewMigrator(pool, fstest.MapFS{})
		if err != nil {
			t.Fatalf("new migrator: %v", err)
		}
		v, err := migrator.Version(ctx)
		if err != nil {
			t.Fatalf("version: %v", err)
		}
		return v
	}

	// A failing statement (duplicate keys) is not recorded, so it can be retried after a fix.
	// PostgreSQL leaves the half-built index behind as INVALID.
	err := up("CREATE UNIQUE INDEX CONCURRENTLY uq_t_a ON t (a);")
	if !errors.Is(err, postgres.ErrMigrationFailed) {
		t.Fatalf("want ErrMigrationFailed, got %v", err)
	}
	if v := version(); v != 0 {
		t.Fatalf("failed no-transaction migration must not be recorded, version = %d", v)
	}

	// IF NOT EXISTS sees the INVALID leftover and succeeds without building anything; the
	// migrator must notice and refuse to record it.
	const idempotent = "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_t_a ON t (a);"
	if err := up(idempotent); !errors.Is(err, postgres.ErrInvalidIndex) {
		t.Fatalf("want ErrInvalidIndex, got %v", err)
	}
	if v := version(); v != 0 {
		t.Fatalf("migration that left an invalid index must not be recorded, version = %d", v)
	}

	// After the data and the leftover are fixed, the same migration applies and is recorded.
	mustExec(ctx, t, pool, "DELETE FROM t WHERE ctid NOT IN (SELECT min(ctid) FROM t)")
	mustExec(ctx, t, pool, "DROP INDEX uq_t_a")
	if err := up(idempotent); err != nil {
		t.Fatalf("retry after fix: %v", err)
	}
	if v := version(); v != 1 {
		t.Fatalf("version = %d, want 1", v)
	}
}

func openLivePool(ctx context.Context, t *testing.T, connStr string) *pgxpool.Pool {
	t.Helper()
	pool, err := postgres.NewPool(ctx, testsupport.PoolConfig(connStr, 2))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func mustExec(ctx context.Context, t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func withSearchPath(connStr, schema string) string {
	sep := "?"
	if strings.Contains(connStr, "?") {
		sep = "&"
	}
	return connStr + sep + "search_path=" + schema
}
