package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "migrate error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		dbURLFlag string
	)
	flag.StringVar(&dbURLFlag, "database-url", "", "PostgreSQL database connection URL (or set DATABASE_URL)")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		return fmt.Errorf("missing command: specify 'up', 'down <steps>', or 'version'")
	}

	command := args[0]

	dbURL := dbURLFlag
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		return fmt.Errorf("database URL is required: pass -database-url or set DATABASE_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(
		ctx,
		dbURL,
		postgres.WithMaxConns(5),
		postgres.WithMinConns(1),
		postgres.WithMaxConnLifetime(time.Hour),
		postgres.WithMaxConnIdleTime(30*time.Minute),
		postgres.WithConnectTimeout(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		return fmt.Errorf("initialize migrator: %w", err)
	}

	switch command {
	case "up":
		slog.Info("running pending database migrations")
		if err := migrator.Up(ctx); err != nil {
			return fmt.Errorf("migration up failed: %w", err)
		}
	case "down":
		steps := 1
		if len(args) > 1 {
			parsed, err := strconv.Atoi(args[1])
			if err != nil || parsed <= 0 {
				return fmt.Errorf("invalid step count: %s (must be positive integer)", args[1])
			}
			steps = parsed
		}
		slog.Info("rolling back database migrations", "steps", steps)
		if err := migrator.Down(ctx, steps); err != nil {
			return fmt.Errorf("migration down failed: %w", err)
		}
	case "version":
	default:
		return fmt.Errorf("unknown command %q: use 'up', 'down', or 'version'", command)
	}

	version, err := migrator.Version(ctx)
	if err != nil {
		return fmt.Errorf("check version: %w", err)
	}
	slog.Info("current schema version", "version", version)
	return nil
}
