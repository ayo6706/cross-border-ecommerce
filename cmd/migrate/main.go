package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "migrate error: %v\n", err)
		os.Exit(1)
	}
}

// command is a parsed migrate invocation: up, down <steps> or version.
type command struct {
	name  string
	steps int
}

// run has no deadline: a CREATE INDEX CONCURRENTLY migration on a large table can outlast any
// fixed bound, so the deploy job owns the timeout and a signal cancels the run.
func run() error {
	dbURL := flag.String("database-url", "", "PostgreSQL database URL (default: DATABASE_URL)")
	flag.Parse()
	cmd, err := parseCommand(flag.Args())
	if err != nil {
		return err
	}

	dbCfg, err := config.LoadDatabase(func(key string) string {
		if key == "DATABASE_URL" && *dbURL != "" {
			return *dbURL
		}
		return os.Getenv(key)
	})
	if err != nil {
		return fmt.Errorf("load database configuration: %w", err)
	}
	logger := slog.Default()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.NewPool(ctx, dbCfg)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	migrator, err := postgres.NewMigrator(pool, migrations.FS)
	if err != nil {
		return fmt.Errorf("initialize migrator: %w", err)
	}
	if err := apply(ctx, migrator, cmd, logger); err != nil {
		return err
	}

	version, err := migrator.Version(ctx)
	if err != nil {
		return fmt.Errorf("check version: %w", err)
	}
	logger.Info("current schema version", slog.Int64("version", version))
	return nil
}

func parseCommand(args []string) (command, error) {
	if len(args) == 0 {
		return command{}, errors.New("missing command: specify 'up', 'down <steps>', or 'version'")
	}
	switch args[0] {
	case "up", "version":
		return command{name: args[0]}, nil
	case "down":
		if len(args) == 1 {
			return command{name: "down", steps: 1}, nil
		}
		steps, err := strconv.Atoi(args[1])
		if err != nil {
			return command{}, fmt.Errorf("invalid step count %q: %w", args[1], err)
		}
		if steps <= 0 {
			return command{}, fmt.Errorf("invalid step count %d: must be a positive integer", steps)
		}
		return command{name: "down", steps: steps}, nil
	default:
		return command{}, fmt.Errorf("unknown command %q: use 'up', 'down', or 'version'", args[0])
	}
}

func apply(ctx context.Context, migrator *postgres.Migrator, cmd command, logger *slog.Logger) error {
	switch cmd.name {
	case "up":
		logger.Info("running pending database migrations")
		if err := migrator.Up(ctx); err != nil {
			return fmt.Errorf("migration up failed: %w", err)
		}
	case "down":
		logger.Info("rolling back database migrations", slog.Int("steps", cmd.steps))
		if err := migrator.Down(ctx, cmd.steps); err != nil {
			return fmt.Errorf("migration down failed: %w", err)
		}
	}
	return nil
}
