package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNilPool         = errors.New("nil postgres pool")
	ErrNilFS           = errors.New("nil filesystem")
	ErrNoChange        = errors.New("no migrations to apply")
	ErrInvalidStep     = errors.New("invalid migration step count")
	ErrMigrationFailed = errors.New("migration execution failed")

	ErrMalformedMigrationName = errors.New("malformed migration file name")
	ErrInvalidNoTxMigration   = errors.New("invalid no-transaction migration")
	ErrInvalidIndex           = errors.New("migration left an invalid index")
)

// noTransactionDirective, as a migration's first line, runs it outside a transaction. CREATE and
// DROP INDEX CONCURRENTLY require this: PostgreSQL refuses them inside a transaction block. Such a
// file holds exactly one statement, because without a transaction a failure after the first of
// several statements would leave it applied but unrecorded.
const noTransactionDirective = "-- migrate:no-transaction"

const (
	migrationLockID     int64 = 8472918471928471
	advisoryUnlockGrace       = 5 * time.Second
)

var migrationFilePattern = regexp.MustCompile(`^(\d+)_([a-zA-Z0-9_\-]+)\.(up|down)\.sql$`)

type MigrationDirection string

const (
	DirectionUp   MigrationDirection = "up"
	DirectionDown MigrationDirection = "down"
)

type Migration struct {
	Version       int64
	Name          string
	Direction     MigrationDirection
	Path          string
	NoTransaction bool
}

// Migrator applies embedded SQL migrations. Each migration runs in its own
// transaction together with its schema_migrations bookkeeping, so a failed
// migration leaves no partial state behind and there is no "dirty" version.
type Migrator struct {
	pool *pgxpool.Pool
	fsys fs.FS
}

const schemaMigrationsInitSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version BIGINT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

func NewMigrator(pool *pgxpool.Pool, fsys fs.FS) (*Migrator, error) {
	if pool == nil {
		return nil, ErrNilPool
	}
	if fsys == nil {
		return nil, ErrNilFS
	}

	return &Migrator{
		pool: pool,
		fsys: fsys,
	}, nil
}

func (m *Migrator) withLock(ctx context.Context, fn func(conn *pgxpool.Conn) error) error {
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for migration lock: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), advisoryUnlockGrace)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	if _, err := conn.Exec(ctx, schemaMigrationsInitSQL); err != nil {
		return fmt.Errorf("initialize schema_migrations table: %w", err)
	}

	return fn(conn)
}

func (m *Migrator) DiscoverMigrations() ([]Migration, error) {
	entries, err := fs.ReadDir(m.fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations directory: %w", err)
	}

	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".sql" {
			continue
		}
		mig, err := m.parseMigration(entry.Name())
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, mig)
	}

	slices.SortFunc(migrations, func(a, b Migration) int {
		if a.Version != b.Version {
			return cmp.Compare(a.Version, b.Version)
		}
		return strings.Compare(string(a.Direction), string(b.Direction))
	})

	return migrations, nil
}

// Version returns the highest applied migration version, or 0 when no
// migration has been applied. It never writes to the database.
func (m *Migrator) Version(ctx context.Context) (int64, error) {
	const q = `
	SELECT CASE
		WHEN to_regclass('schema_migrations') IS NULL THEN 0
		ELSE (SELECT COALESCE(MAX(version), 0) FROM schema_migrations)
	END`

	var version int64
	if err := m.pool.QueryRow(ctx, q).Scan(&version); err != nil {
		return 0, fmt.Errorf("query current migration version: %w", err)
	}
	return version, nil
}

// parseMigration reads one migration file. A .sql file that does not match the naming pattern
// is an error, not skipped: skipping it would mean the migration silently never runs.
func (m *Migrator) parseMigration(name string) (Migration, error) {
	matches := migrationFilePattern.FindStringSubmatch(name)
	if len(matches) != 4 {
		return Migration{}, fmt.Errorf("%w: %s (want <version>_<name>.up.sql or .down.sql)", ErrMalformedMigrationName, name)
	}
	v, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return Migration{}, fmt.Errorf("parse migration version %s: %w", matches[1], err)
	}
	script, err := fs.ReadFile(m.fsys, name)
	if err != nil {
		return Migration{}, fmt.Errorf("read migration file %s: %w", name, err)
	}
	noTx, err := parseNoTransaction(string(script))
	if err != nil {
		return Migration{}, fmt.Errorf("migration %s: %w", name, err)
	}
	return Migration{Version: v, Name: matches[2], Direction: MigrationDirection(matches[3]), Path: name, NoTransaction: noTx}, nil
}

func parseNoTransaction(script string) (bool, error) {
	firstLine, body, _ := strings.Cut(script, "\n")
	if strings.TrimSpace(firstLine) != noTransactionDirective {
		return false, nil
	}
	n, err := countStatements(body)
	if err != nil {
		return true, err
	}
	if n != 1 {
		return true, fmt.Errorf("%w: must hold exactly one statement, found %d", ErrInvalidNoTxMigration, n)
	}
	return true, nil
}

// countStatements counts the non-empty statements in a script, ignoring comments and quoted text.
// Dollar quoting is refused rather than parsed: a no-transaction migration is a single index
// statement and never needs it.
func countStatements(sql string) (int, error) {
	count, pending := 0, false
	for i := 0; i < len(sql); i++ {
		switch c := sql[i]; {
		case strings.HasPrefix(sql[i:], "--"):
			i = indexPast(sql, "\n", i) - 1
		case strings.HasPrefix(sql[i:], "/*"):
			i = indexPast(sql, "*/", i+2) - 1
		case c == '\'' || c == '"':
			i = closingQuote(sql, i)
			pending = true
		case c == '$':
			return 0, fmt.Errorf("%w: dollar quoting is not supported", ErrInvalidNoTxMigration)
		case c == ';':
			if pending {
				count++
			}
			pending = false
		case !unicode.IsSpace(rune(c)):
			pending = true
		}
	}
	if pending {
		count++
	}
	return count, nil
}

// indexPast returns the index just after the first terminator at or after from, or len(s).
func indexPast(s, terminator string, from int) int {
	j := strings.Index(s[from:], terminator)
	if j < 0 {
		return len(s)
	}
	return from + j + len(terminator)
}

// closingQuote returns the index of the quote closing the one at s[open]; a doubled quote is an
// escaped quote, not a close.
func closingQuote(s string, open int) int {
	q := s[open]
	for j := open + 1; j < len(s); j++ {
		if s[j] != q {
			continue
		}
		if j+1 < len(s) && s[j+1] == q {
			j++
			continue
		}
		return j
	}
	return len(s) - 1
}

func (m *Migrator) Up(ctx context.Context) error {
	upMigrations, err := m.migrationsByDirection(DirectionUp)
	if err != nil {
		return err
	}
	if len(upMigrations) == 0 {
		return ErrNoChange
	}

	return m.withLock(ctx, func(conn *pgxpool.Conn) error {
		applied, err := appliedVersionsDesc(ctx, conn)
		if err != nil {
			return err
		}
		appliedSet := make(map[int64]struct{}, len(applied))
		for _, v := range applied {
			appliedSet[v] = struct{}{}
		}

		for _, mig := range upMigrations {
			if _, ok := appliedSet[mig.Version]; ok {
				continue
			}

			script, err := fs.ReadFile(m.fsys, path.Clean(mig.Path))
			if err != nil {
				return fmt.Errorf("read migration file %s: %w", mig.Path, err)
			}

			const record = `INSERT INTO schema_migrations (version) VALUES ($1)`
			if err := runMigration(ctx, conn, mig, string(script), record); err != nil {
				return fmt.Errorf("apply migration %d (%s): %w", mig.Version, mig.Name, err)
			}
		}

		return nil
	})
}

func (m *Migrator) Down(ctx context.Context, steps int) error {
	if steps <= 0 {
		return fmt.Errorf("%w: step count must be > 0", ErrInvalidStep)
	}

	downMigrations, err := m.migrationsByDirection(DirectionDown)
	if err != nil {
		return err
	}
	downByVersion := make(map[int64]Migration, len(downMigrations))
	for _, mig := range downMigrations {
		downByVersion[mig.Version] = mig
	}

	return m.withLock(ctx, func(conn *pgxpool.Conn) error {
		applied, err := appliedVersionsDesc(ctx, conn)
		if err != nil {
			return err
		}
		if len(applied) == 0 {
			return ErrNoChange
		}

		for _, version := range applied[:min(steps, len(applied))] {
			mig, ok := downByVersion[version]
			if !ok {
				return fmt.Errorf("down migration file not found for version %d", version)
			}

			script, err := fs.ReadFile(m.fsys, path.Clean(mig.Path))
			if err != nil {
				return fmt.Errorf("read down migration file %s: %w", mig.Path, err)
			}

			const unrecord = `DELETE FROM schema_migrations WHERE version = $1`
			if err := runMigration(ctx, conn, mig, string(script), unrecord); err != nil {
				return fmt.Errorf("rollback migration %d (%s): %w", version, mig.Name, err)
			}
		}

		return nil
	})
}

func (m *Migrator) migrationsByDirection(dir MigrationDirection) ([]Migration, error) {
	all, err := m.DiscoverMigrations()
	if err != nil {
		return nil, err
	}

	filtered := make([]Migration, 0, len(all)/2)
	for _, mig := range all {
		if mig.Direction == dir {
			filtered = append(filtered, mig)
		}
	}
	return filtered, nil
}

func runMigration(ctx context.Context, conn *pgxpool.Conn, mig Migration, script, bookkeeping string) error {
	if mig.NoTransaction {
		return runWithoutTx(ctx, conn, script, bookkeeping, mig.Version)
	}
	return runInTx(ctx, conn, script, bookkeeping, mig.Version)
}

// runWithoutTx executes a single-statement no-transaction migration, then refuses to record it if
// it left an invalid index. A failed CREATE INDEX CONCURRENTLY leaves its index behind as INVALID,
// and a rerun with IF NOT EXISTS then succeeds without building anything.
func runWithoutTx(ctx context.Context, conn *pgxpool.Conn, script, bookkeeping string, version int64) error {
	if _, err := conn.Exec(ctx, script); err != nil {
		return fmt.Errorf("%w: %w", ErrMigrationFailed, err)
	}
	if err := checkNoInvalidIndexes(ctx, conn); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, bookkeeping, version); err != nil {
		return fmt.Errorf("update schema_migrations: %w", err)
	}
	return nil
}

// checkNoInvalidIndexes looks only at the schemas on the search path, so it sees the indexes
// migrations create and not those of unrelated schemas.
func checkNoInvalidIndexes(ctx context.Context, conn *pgxpool.Conn) error {
	const q = `
	SELECT string_agg(c.relname, ', ' ORDER BY c.relname)
	FROM pg_index i
	JOIN pg_class c ON c.oid = i.indexrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE NOT i.indisvalid AND n.nspname = ANY (current_schemas(false))`

	var invalid *string
	if err := conn.QueryRow(ctx, q).Scan(&invalid); err != nil {
		return fmt.Errorf("check for invalid indexes: %w", err)
	}
	if invalid != nil {
		return fmt.Errorf("%w: %s (drop it and rerun the migration)", ErrInvalidIndex, *invalid)
	}
	return nil
}

// runInTx executes a migration script and its schema_migrations bookkeeping
// statement atomically.
func runInTx(ctx context.Context, conn *pgxpool.Conn, script, bookkeeping string, version int64) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, script); err != nil {
		return fmt.Errorf("%w: %w", ErrMigrationFailed, err)
	}
	if _, err := tx.Exec(ctx, bookkeeping, version); err != nil {
		return fmt.Errorf("update schema_migrations: %w", err)
	}

	return tx.Commit(ctx)
}

func appliedVersionsDesc(ctx context.Context, conn *pgxpool.Conn) ([]int64, error) {
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version DESC`)
	if err != nil {
		return nil, fmt.Errorf("query applied migration versions: %w", err)
	}
	defer rows.Close()

	versions := make([]int64, 0, 16)
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan migration version: %w", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate migration versions: %w", err)
	}

	return versions, nil
}
