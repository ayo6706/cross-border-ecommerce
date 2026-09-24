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

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNilPool         = errors.New("nil postgres pool")
	ErrNilFS           = errors.New("nil filesystem")
	ErrNoChange        = errors.New("no migrations to apply")
	ErrInvalidStep     = errors.New("invalid migration step count")
	ErrMigrationFailed = errors.New("migration execution failed")
)

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
	Version   int64
	Name      string
	Direction MigrationDirection
	Path      string
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
		if entry.IsDir() {
			continue
		}

		matches := migrationFilePattern.FindStringSubmatch(entry.Name())
		if len(matches) != 4 {
			continue
		}

		v, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version %s: %w", matches[1], err)
		}

		migrations = append(migrations, Migration{
			Version:   v,
			Name:      matches[2],
			Direction: MigrationDirection(matches[3]),
			Path:      entry.Name(),
		})
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
			if err := runInTx(ctx, conn, string(script), record, mig.Version); err != nil {
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
			if err := runInTx(ctx, conn, string(script), unrecord, version); err != nil {
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
