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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNilPool         = errors.New("nil postgres pool")
	ErrNilFS           = errors.New("nil filesystem")
	ErrDirtyMigration  = errors.New("database migration is in a dirty state")
	ErrNoChange        = errors.New("no migrations to apply")
	ErrInvalidStep     = errors.New("invalid migration step count")
	ErrMigrationFailed = errors.New("migration execution failed")
)

const migrationLockID int64 = 8472918471928471

var migrationFilePattern = regexp.MustCompile(`^(\d+)_([a-zA-Z0-9_\-]+)\.(up|down)\.sql$`)

type Migration struct {
	Version   int64
	Name      string
	Direction string
	Path      string
}

type Migrator struct {
	pool *pgxpool.Pool
	fsys fs.FS
}

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
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	return fn(conn)
}

func (m *Migrator) Init(ctx context.Context) error {
	const initQuery = `
	CREATE TABLE IF NOT EXISTS schema_migrations (
		version BIGINT PRIMARY KEY,
		dirty BOOLEAN NOT NULL DEFAULT FALSE,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`
	if _, err := m.pool.Exec(ctx, initQuery); err != nil {
		return fmt.Errorf("initialize schema_migrations table: %w", err)
	}
	return nil
}

func (m *Migrator) initOnConn(ctx context.Context, conn *pgxpool.Conn) error {
	const initQuery = `
	CREATE TABLE IF NOT EXISTS schema_migrations (
		version BIGINT PRIMARY KEY,
		dirty BOOLEAN NOT NULL DEFAULT FALSE,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`
	if _, err := conn.Exec(ctx, initQuery); err != nil {
		return fmt.Errorf("initialize schema_migrations table on connection: %w", err)
	}
	return nil
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
			Direction: matches[3],
			Path:      entry.Name(),
		})
	}

	slices.SortFunc(migrations, func(a, b Migration) int {
		if a.Version != b.Version {
			return cmp.Compare(a.Version, b.Version)
		}
		return strings.Compare(a.Direction, b.Direction)
	})

	return migrations, nil
}

func (m *Migrator) Version(ctx context.Context) (int64, bool, error) {
	if err := m.Init(ctx); err != nil {
		return 0, false, err
	}
	return m.queryVersion(ctx, m.pool.QueryRow)
}

func (m *Migrator) versionOnConn(ctx context.Context, conn *pgxpool.Conn) (int64, bool, error) {
	return m.queryVersion(ctx, conn.QueryRow)
}

func (m *Migrator) queryVersion(ctx context.Context, queryRow func(ctx context.Context, sql string, args ...any) pgx.Row) (int64, bool, error) {
	const q = `SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 1`
	var (
		version int64
		dirty   bool
	)

	err := queryRow(ctx, q).Scan(&version, &dirty)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("query current migration version: %w", err)
	}

	return version, dirty, nil
}

func (m *Migrator) Up(ctx context.Context) error {
	return m.withLock(ctx, func(conn *pgxpool.Conn) error {
		if err := m.initOnConn(ctx, conn); err != nil {
			return err
		}

		allMigrations, err := m.DiscoverMigrations()
		if err != nil {
			return err
		}

		upMigrations := make([]Migration, 0, len(allMigrations)/2)
		for _, mig := range allMigrations {
			if mig.Direction == "up" {
				upMigrations = append(upMigrations, mig)
			}
		}

		if len(upMigrations) == 0 {
			return ErrNoChange
		}

		currentVersion, dirty, err := m.versionOnConn(ctx, conn)
		if err != nil {
			return err
		}
		if dirty {
			return fmt.Errorf("%w: version %d is dirty", ErrDirtyMigration, currentVersion)
		}

		appliedMap, err := m.getAppliedVersionsOnConn(ctx, conn)
		if err != nil {
			return err
		}

		for _, mig := range upMigrations {
			if appliedMap[mig.Version] {
				continue
			}

			content, err := fs.ReadFile(m.fsys, path.Clean(mig.Path))
			if err != nil {
				return fmt.Errorf("read migration file %s: %w", mig.Path, err)
			}

			if err := m.applyMigration(ctx, conn, mig.Version, string(content)); err != nil {
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

	return m.withLock(ctx, func(conn *pgxpool.Conn) error {
		if err := m.initOnConn(ctx, conn); err != nil {
			return err
		}

		currentVersion, dirty, err := m.versionOnConn(ctx, conn)
		if err != nil {
			return err
		}
		if dirty {
			return fmt.Errorf("%w: version %d is dirty", ErrDirtyMigration, currentVersion)
		}

		allMigrations, err := m.DiscoverMigrations()
		if err != nil {
			return err
		}

		downMap := make(map[int64]Migration, len(allMigrations))
		for _, mig := range allMigrations {
			if mig.Direction == "down" {
				downMap[mig.Version] = mig
			}
		}

		appliedVersions, err := m.getAppliedVersionsListDescOnConn(ctx, conn)
		if err != nil {
			return err
		}

		if len(appliedVersions) == 0 {
			return ErrNoChange
		}

		rollbackCount := min(steps, len(appliedVersions))

		for i := 0; i < rollbackCount; i++ {
			version := appliedVersions[i]
			mig, ok := downMap[version]
			if !ok {
				return fmt.Errorf("down migration file not found for version %d", version)
			}

			content, err := fs.ReadFile(m.fsys, path.Clean(mig.Path))
			if err != nil {
				return fmt.Errorf("read down migration file %s: %w", mig.Path, err)
			}

			if err := m.rollbackMigration(ctx, conn, version, string(content)); err != nil {
				return fmt.Errorf("rollback migration %d (%s): %w", version, mig.Name, err)
			}
		}

		return nil
	})
}

func (m *Migrator) applyMigration(ctx context.Context, conn *pgxpool.Conn, version int64, script string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	const markDirty = `
	INSERT INTO schema_migrations (version, dirty, applied_at)
	VALUES ($1, TRUE, NOW())
	ON CONFLICT (version) DO UPDATE SET dirty = TRUE, applied_at = NOW();
	`
	if _, err := tx.Exec(ctx, markDirty, version); err != nil {
		return fmt.Errorf("mark version dirty: %w", err)
	}

	if _, err := tx.Exec(ctx, script); err != nil {
		return fmt.Errorf("%w: %w", ErrMigrationFailed, err)
	}

	const markClean = `UPDATE schema_migrations SET dirty = FALSE WHERE version = $1;`
	if _, err := tx.Exec(ctx, markClean, version); err != nil {
		return fmt.Errorf("mark version clean: %w", err)
	}

	return tx.Commit(ctx)
}

func (m *Migrator) rollbackMigration(ctx context.Context, conn *pgxpool.Conn, version int64, script string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	const markDirty = `UPDATE schema_migrations SET dirty = TRUE WHERE version = $1;`
	if _, err := tx.Exec(ctx, markDirty, version); err != nil {
		return fmt.Errorf("mark version dirty for rollback: %w", err)
	}

	if _, err := tx.Exec(ctx, script); err != nil {
		return fmt.Errorf("%w: %w", ErrMigrationFailed, err)
	}

	const deleteVersion = `DELETE FROM schema_migrations WHERE version = $1;`
	if _, err := tx.Exec(ctx, deleteVersion, version); err != nil {
		return fmt.Errorf("delete migrated version: %w", err)
	}

	return tx.Commit(ctx)
}

func (m *Migrator) getAppliedVersionsOnConn(ctx context.Context, conn *pgxpool.Conn) (map[int64]bool, error) {
	const q = `SELECT version FROM schema_migrations WHERE dirty = FALSE`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query applied migration versions: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]bool, 16)
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("scan migration version: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate migration versions: %w", err)
	}

	return applied, nil
}

func (m *Migrator) getAppliedVersionsListDescOnConn(ctx context.Context, conn *pgxpool.Conn) ([]int64, error) {
	const q = `SELECT version FROM schema_migrations WHERE dirty = FALSE ORDER BY version DESC`
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query applied migration versions desc: %w", err)
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
