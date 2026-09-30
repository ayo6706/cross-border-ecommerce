// Command regload manages regulatory dataset versions: load a curated file, review, activate or
// reject a version, and report coverage.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	adapter "github.com/ayo6706/cross-border-ecommerce/internal/adapters/regulatory"
	appRegulatory "github.com/ayo6706/cross-border-ecommerce/internal/application/regulatory"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
	"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure/postgres"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/wiring"
)

const usage = "usage: regload <load-curated|review|activate|reject|coverage> [options]"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Getenv, os.Stdout)
	stop()
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "regload error: %v\n", err)
		os.Exit(1)
	}
}

// A command parses its flags and returns the action to run against the service.
type (
	action  func(ctx context.Context, svc *appRegulatory.Service, out io.Writer) error
	command func(args []string) (action, error)
)

var commands = map[string]command{
	"load-curated": loadCurated,
	"review":       review,
	"activate":     activate,
	"reject":       reject,
	"coverage":     coverage,
}

// run parses the command and its flags before connecting, so a typo never touches the database.
func run(ctx context.Context, args []string, lookup func(string) string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	cmd, ok := commands[args[0]]
	if !ok {
		return fmt.Errorf("unknown command %q; %s", args[0], usage)
	}
	act, err := cmd(args[1:])
	if err != nil {
		return err
	}
	cfg, err := config.LoadRegulatory(lookup)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	pool, err := postgres.NewPool(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()
	svc, err := wiring.RegulatoryService(pool, cfg.SLAs)
	if err != nil {
		return err
	}
	return act(ctx, svc, out)
}

// flags wraps a FlagSet whose string flags are all required unless declared optional.
type flags struct {
	set      *flag.FlagSet
	values   map[string]*string
	optional map[string]bool
}

func newFlags(name string, required ...string) *flags {
	f := &flags{
		set:      flag.NewFlagSet(name, flag.ContinueOnError),
		values:   map[string]*string{},
		optional: map[string]bool{},
	}
	for _, n := range required {
		f.values[n] = f.set.String(n, "", "required")
	}
	return f
}

func (f *flags) withOptional(name, usage string) *flags {
	f.values[name] = f.set.String(name, "", usage)
	f.optional[name] = true
	return f
}

func (f *flags) parse(args []string) error {
	if err := f.set.Parse(args); err != nil {
		return fmt.Errorf("parse %s flags: %w", f.set.Name(), err)
	}
	var missing []string
	for name, v := range f.values {
		if !f.optional[name] && strings.TrimSpace(*v) == "" {
			missing = append(missing, "--"+name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return fmt.Errorf("%s: missing required %s", f.set.Name(), strings.Join(missing, ", "))
	}
	return nil
}

func (f *flags) get(name string) string { return strings.TrimSpace(*f.values[name]) }

func loadCurated(args []string) (action, error) {
	f := newFlags("load-curated", "file", "jurisdiction", "category", "source", "version", "fetched-at",
		"licence", "attribution", "loaded-by")
	if err := f.parse(args); err != nil {
		return nil, err
	}
	category, err := regulatory.ParseCategory(f.get("category"))
	if err != nil {
		return nil, err
	}
	fetchedAt, err := time.Parse(time.RFC3339, f.get("fetched-at"))
	if err != nil {
		return nil, fmt.Errorf("--fetched-at must be RFC 3339 (2026-09-29T10:00:00Z): %w", err)
	}
	content, err := os.ReadFile(f.get("file"))
	if err != nil {
		return nil, fmt.Errorf("read curated file: %w", err)
	}
	rules, err := adapter.ParseCurated(category, bytes.NewReader(content))
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", f.get("file"), err)
	}
	load := appRegulatory.CuratedLoad{
		Dataset: regulatory.NewDatasetParams{
			Jurisdiction: f.get("jurisdiction"), Category: category, Source: f.get("source"),
			Version: f.get("version"), FetchedAt: fetchedAt, ContentSHA256: sha256.Sum256(content),
			Licence: f.get("licence"), Attribution: f.get("attribution"), LoadedBy: f.get("loaded-by"),
		},
		Rules: rules,
	}
	return func(ctx context.Context, svc *appRegulatory.Service, out io.Writer) error {
		return printResult(out)(svc.LoadCurated(ctx, load))
	}, nil
}

func review(args []string) (action, error) {
	f := newFlags("review", "dataset", "reviewer", "note")
	if err := f.parse(args); err != nil {
		return nil, err
	}
	return func(ctx context.Context, svc *appRegulatory.Service, out io.Writer) error {
		return printResult(out)(svc.Review(ctx, f.get("dataset"), f.get("reviewer"), f.get("note")))
	}, nil
}

func activate(args []string) (action, error) {
	f := newFlags("activate", "dataset")
	if err := f.parse(args); err != nil {
		return nil, err
	}
	return func(ctx context.Context, svc *appRegulatory.Service, out io.Writer) error {
		return printResult(out)(svc.Activate(ctx, f.get("dataset")))
	}, nil
}

func reject(args []string) (action, error) {
	f := newFlags("reject", "dataset", "reason")
	if err := f.parse(args); err != nil {
		return nil, err
	}
	return func(ctx context.Context, svc *appRegulatory.Service, out io.Writer) error {
		return printResult(out)(svc.Reject(ctx, f.get("dataset"), f.get("reason")))
	}, nil
}

// coverage prints the datasets in force and, when a decision must not rely on them, the HOLD reason
// code; that case also returns the error, so the exit status is non-zero.
func coverage(args []string) (action, error) {
	f := newFlags("coverage", "jurisdiction", "category").withOptional("at", "RFC 3339 time (default: now)")
	if err := f.parse(args); err != nil {
		return nil, err
	}
	category, err := regulatory.ParseCategory(f.get("category"))
	if err != nil {
		return nil, err
	}
	var at time.Time
	if raw := f.get("at"); raw != "" {
		if at, err = time.Parse(time.RFC3339, raw); err != nil {
			return nil, fmt.Errorf("--at must be RFC 3339: %w", err)
		}
	}
	return func(ctx context.Context, svc *appRegulatory.Service, out io.Writer) error {
		if at.IsZero() {
			at = time.Now()
		}
		active, err := svc.Coverage(ctx, f.get("jurisdiction"), category, at)
		for _, d := range active {
			if perr := printDataset(out, d); perr != nil {
				return perr
			}
		}
		if reason, ok := regulatory.HoldReason(err); ok {
			if _, perr := fmt.Fprintf(out, "HOLD %s\n", reason); perr != nil {
				return perr
			}
		}
		return err
	}, nil
}

func printResult(out io.Writer) func(*regulatory.Dataset, error) error {
	return func(d *regulatory.Dataset, err error) error {
		if err != nil {
			return err
		}
		return printDataset(out, d)
	}
}

func printDataset(out io.Writer, d *regulatory.Dataset) error {
	_, err := fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%s\n",
		d.ID, d.Status, d.Jurisdiction, d.Category, d.Source, d.Version)
	return err
}
