package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidConfig      = errors.New("invalid postgres pool configuration")
	ErrEmptyConnString    = errors.New("empty connection string")
	ErrNilContext         = errors.New("nil context provided")
	ErrMinConnsExceedsMax = errors.New("min conns cannot exceed max conns")
)

// Fallbacks for callers that do not pass options (e.g. tests). The API and
// migrate commands pass explicit values; platform/config owns the app defaults.
const (
	defaultMaxConns          int32         = 25
	defaultMinConns          int32         = 5
	defaultMaxConnIdleTime   time.Duration = 15 * time.Minute
	defaultMaxConnLifetime   time.Duration = 1 * time.Hour
	defaultHealthCheckPeriod time.Duration = 1 * time.Minute
	defaultConnectTimeout    time.Duration = 5 * time.Second
)

type config struct {
	maxConns          int32
	minConns          int32
	maxConnIdleTime   time.Duration
	maxConnLifetime   time.Duration
	healthCheckPeriod time.Duration
	connectTimeout    time.Duration
}

type Option func(*config)

func WithMaxConns(max int32) Option {
	return func(c *config) {
		c.maxConns = max
	}
}

func WithMinConns(min int32) Option {
	return func(c *config) {
		c.minConns = min
	}
}

func WithMaxConnIdleTime(d time.Duration) Option {
	return func(c *config) {
		c.maxConnIdleTime = d
	}
}

func WithMaxConnLifetime(d time.Duration) Option {
	return func(c *config) {
		c.maxConnLifetime = d
	}
}

func WithHealthCheckPeriod(d time.Duration) Option {
	return func(c *config) {
		c.healthCheckPeriod = d
	}
}

func WithConnectTimeout(d time.Duration) Option {
	return func(c *config) {
		c.connectTimeout = d
	}
}

func NewPool(ctx context.Context, connString string, opts ...Option) (*pgxpool.Pool, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if connString == "" {
		return nil, ErrEmptyConnString
	}

	cfg := config{
		maxConns:          defaultMaxConns,
		minConns:          defaultMinConns,
		maxConnIdleTime:   defaultMaxConnIdleTime,
		maxConnLifetime:   defaultMaxConnLifetime,
		healthCheckPeriod: defaultHealthCheckPeriod,
		connectTimeout:    defaultConnectTimeout,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	if cfg.maxConns <= 0 {
		return nil, fmt.Errorf("%w: max conns must be positive, got %d", ErrInvalidConfig, cfg.maxConns)
	}
	if cfg.minConns < 0 {
		return nil, fmt.Errorf("%w: min conns cannot be negative, got %d", ErrInvalidConfig, cfg.minConns)
	}
	if cfg.minConns > cfg.maxConns {
		return nil, fmt.Errorf("%w: min conns (%d) > max conns (%d)", ErrMinConnsExceedsMax, cfg.minConns, cfg.maxConns)
	}
	if cfg.connectTimeout <= 0 {
		return nil, fmt.Errorf("%w: connect timeout must be positive, got %s", ErrInvalidConfig, cfg.connectTimeout)
	}

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse postgres pool config: %w", err)
	}

	poolConfig.MaxConns = cfg.maxConns
	poolConfig.MinConns = cfg.minConns
	poolConfig.MaxConnIdleTime = cfg.maxConnIdleTime
	poolConfig.MaxConnLifetime = cfg.maxConnLifetime
	poolConfig.HealthCheckPeriod = cfg.healthCheckPeriod

	connectCtx, cancelConnect := context.WithTimeout(ctx, cfg.connectTimeout)
	defer cancelConnect()

	pool, err := pgxpool.NewWithConfig(connectCtx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, cancelPing := context.WithTimeout(ctx, cfg.connectTimeout)
	defer cancelPing()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres pool: %w", err)
	}

	return pool, nil
}
