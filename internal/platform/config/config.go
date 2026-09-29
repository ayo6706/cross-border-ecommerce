package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/backoff"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
)

var (
	ErrNilLookup                  = errors.New("config lookup function is nil")
	ErrInvalidPort                = errors.New("server port must be a valid integer between 1 and 65535")
	ErrInvalidTimeout             = errors.New("timeout values must be strictly positive")
	ErrEmptyDatabaseURL           = errors.New("DATABASE_URL is required")
	ErrEmptyRedisURL              = errors.New("REDIS_URL is required")
	ErrInvalidPoolLimits          = errors.New("database connection pool minimum cannot exceed maximum")
	ErrInvalidStreamConfig        = errors.New("invalid stream configuration")
	ErrInvalidWorkerConfig        = errors.New("invalid worker configuration")
	ErrInvalidRunProcessingConfig = errors.New("invalid run processing configuration")
)

type ServerConfig struct {
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

func (s ServerConfig) Validate() error {
	portNum, err := strconv.Atoi(s.Port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return fmt.Errorf("%w: '%s'", ErrInvalidPort, s.Port)
	}
	if s.ReadTimeout <= 0 || s.WriteTimeout <= 0 || s.IdleTimeout <= 0 || s.ShutdownTimeout <= 0 {
		return fmt.Errorf("%w for server configuration", ErrInvalidTimeout)
	}
	return nil
}

type DatabaseConfig struct {
	URL             string
	MaxConns        int32
	MinConns        int32
	MaxConnIdleTime time.Duration
	MaxConnLifetime time.Duration
	ConnectTimeout  time.Duration
}

func (d DatabaseConfig) Validate() error {
	if strings.TrimSpace(d.URL) == "" {
		return ErrEmptyDatabaseURL
	}
	if d.MinConns < 0 || d.MaxConns <= 0 || d.MinConns > d.MaxConns {
		return fmt.Errorf("%w: min=%d, max=%d", ErrInvalidPoolLimits, d.MinConns, d.MaxConns)
	}
	if d.ConnectTimeout <= 0 || d.MaxConnIdleTime <= 0 || d.MaxConnLifetime <= 0 {
		return fmt.Errorf("%w for database configuration", ErrInvalidTimeout)
	}
	return nil
}

func (d DatabaseConfig) RedactedURL() string {
	if d.URL == "" {
		return ""
	}
	u, err := url.Parse(d.URL)
	if err != nil {
		return "[malformed database URL]"
	}
	return u.Redacted()
}

type RedisConfig struct {
	URL string
}

func (r RedisConfig) Validate() error {
	if strings.TrimSpace(r.URL) == "" {
		return ErrEmptyRedisURL
	}
	return nil
}

type StreamConfig struct {
	Retention        time.Duration
	ConsumerBlock    time.Duration
	ClaimMinIdle     time.Duration
	ClaimInterval    time.Duration
	ConsumerBatch    int
	HandlerTimeout   time.Duration
	RetryMaxAttempts int
	RetryBaseBackoff time.Duration
	RetryMaxBackoff  time.Duration
}

func (s StreamConfig) Validate() error {
	if s.Retention <= 0 || s.ConsumerBlock <= 0 || s.ClaimMinIdle <= 0 || s.ClaimInterval <= 0 || s.HandlerTimeout <= 0 {
		return fmt.Errorf("%w for stream configuration", ErrInvalidTimeout)
	}
	if err := backoff.ValidateRetryPolicy(s.RetryMaxAttempts, s.RetryBaseBackoff, s.RetryMaxBackoff,
		s.HandlerTimeout, s.ClaimMinIdle); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidStreamConfig, err)
	}
	return nil
}

type WorkerConfig struct {
	Concurrency  int
	QueueSize    int
	DrainTimeout time.Duration
}

func (w WorkerConfig) Validate() error {
	if w.Concurrency <= 0 {
		return fmt.Errorf("%w: worker concurrency must be strictly positive, got %d", ErrInvalidWorkerConfig, w.Concurrency)
	}
	if w.QueueSize < 0 {
		return fmt.Errorf("%w: worker queue size cannot be negative, got %d", ErrInvalidWorkerConfig, w.QueueSize)
	}
	if w.DrainTimeout <= 0 {
		return fmt.Errorf("%w for worker drain timeout", ErrInvalidTimeout)
	}
	return nil
}

// OutboxConfig is validated by outbox.NewRelay, which owns its bounds.
type OutboxConfig struct {
	BatchSize    int
	PollInterval time.Duration
	Lease        time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	MaxAttempts  int
}

type IdempotencyConfig struct {
	LeaseTTL time.Duration
}

func (i IdempotencyConfig) Validate() error {
	if i.LeaseTTL <= 0 {
		return fmt.Errorf("%w for idempotency lease TTL", ErrInvalidTimeout)
	}
	return nil
}

type AppConfig struct {
	Environment string
	ServiceName string
}

// RunProcessingConfig drives product processing of finished runs, by the worker loop and by
// cmd/ingest. Lease and BatchSize are the defaults of the ingest flags.
type RunProcessingConfig struct {
	PollInterval time.Duration
	Lease        time.Duration
	BatchSize    int
	ErrorBudget  ingestion.ErrorBudget
}

func (r RunProcessingConfig) Validate() error {
	if r.PollInterval <= 0 || r.Lease <= 0 {
		return fmt.Errorf("%w for run processing poll interval and lease", ErrInvalidTimeout)
	}
	if r.BatchSize <= 0 {
		return fmt.Errorf("%w: batch size must be strictly positive, got %d",
			ErrInvalidRunProcessingConfig, r.BatchSize)
	}
	if err := r.ErrorBudget.Validate(); err != nil {
		return fmt.Errorf("INGESTION_ERROR_BUDGET_*: %w", err)
	}
	return nil
}

type Config struct {
	Server        ServerConfig
	Database      DatabaseConfig
	Log           logging.Options
	App           AppConfig
	Redis         RedisConfig
	Stream        StreamConfig
	Outbox        OutboxConfig
	Worker        WorkerConfig
	Idempotency   IdempotencyConfig
	RunProcessing RunProcessingConfig
}

func Load() (*Config, error) {
	return LoadFromLookup(os.Getenv)
}

// LoadFromLookup reads every variable through lookup, reports all malformed ones together, then
// validates.
func LoadFromLookup(lookup func(string) string) (*Config, error) {
	if lookup == nil {
		return nil, ErrNilLookup
	}
	e := &env{lookup: lookup}
	cfg := &Config{
		Server:        loadServer(e),
		Database:      loadDatabase(e),
		Log:           loadLog(e),
		App:           AppConfig{Environment: e.str("APP_ENV", "development"), ServiceName: e.str("SERVICE_NAME", "cross-border-api")},
		Redis:         RedisConfig{URL: e.str("REDIS_URL", "")},
		Stream:        loadStream(e),
		Outbox:        loadOutbox(e),
		Worker:        loadWorker(e),
		Idempotency:   IdempotencyConfig{LeaseTTL: e.duration("IDEMPOTENCY_LEASE_TTL", 30*time.Second)},
		RunProcessing: loadRunProcessing(e),
	}
	if err := errors.Join(e.errs...); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

// LoadDatabase reads and validates only DATABASE_URL and DB_*, for a binary that needs nothing else
// (cmd/migrate): an unrelated invalid variable such as PORT must not block a migration.
func LoadDatabase(lookup func(string) string) (DatabaseConfig, error) {
	if lookup == nil {
		return DatabaseConfig{}, ErrNilLookup
	}
	e := &env{lookup: lookup}
	db := loadDatabase(e)
	if err := errors.Join(e.errs...); err != nil {
		return DatabaseConfig{}, err
	}
	if err := db.Validate(); err != nil {
		return DatabaseConfig{}, fmt.Errorf("validate database config: %w", err)
	}
	return db, nil
}

// Validate reports every invalid section, not only the first.
func (c *Config) Validate() error {
	return errors.Join(
		c.Server.Validate(),
		c.Database.Validate(),
		c.Stream.Validate(),
		c.Worker.Validate(),
		c.Idempotency.Validate(),
		c.RunProcessing.Validate(),
		c.validateIdempotencyLease(),
	)
}

// validateIdempotencyLease keeps a claim alive longer than the handler it protects.
func (c *Config) validateIdempotencyLease() error {
	if c.Idempotency.LeaseTTL <= c.Stream.HandlerTimeout {
		return fmt.Errorf("idempotency lease TTL (%v) must be strictly greater than stream handler timeout (%v)",
			c.Idempotency.LeaseTTL, c.Stream.HandlerTimeout)
	}
	return nil
}

func loadServer(e *env) ServerConfig {
	return ServerConfig{
		Port:            e.str("PORT", "8080"),
		ReadTimeout:     e.duration("SERVER_READ_TIMEOUT", 10*time.Second),
		WriteTimeout:    e.duration("SERVER_WRITE_TIMEOUT", 10*time.Second),
		IdleTimeout:     e.duration("SERVER_IDLE_TIMEOUT", 60*time.Second),
		ShutdownTimeout: e.duration("SERVER_SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

func loadDatabase(e *env) DatabaseConfig {
	return DatabaseConfig{
		URL:             e.str("DATABASE_URL", ""),
		MaxConns:        read(e, "DB_MAX_CONNS", int32(25), parseInt32),
		MinConns:        read(e, "DB_MIN_CONNS", int32(5), parseInt32),
		MaxConnIdleTime: e.duration("DB_MAX_CONN_IDLE_TIME", 15*time.Minute),
		MaxConnLifetime: e.duration("DB_MAX_CONN_LIFETIME", time.Hour),
		ConnectTimeout:  e.duration("DB_CONNECT_TIMEOUT", 5*time.Second),
	}
}

func loadLog(e *env) logging.Options {
	return logging.Options{
		Level:     read(e, "LOG_LEVEL", slog.LevelInfo, logging.ParseLevel),
		Format:    read(e, "LOG_FORMAT", logging.FormatJSON, logging.ParseFormat),
		AddSource: read(e, "LOG_ADD_SOURCE", false, strconv.ParseBool),
	}
}

func loadStream(e *env) StreamConfig {
	return StreamConfig{
		Retention:        e.duration("STREAM_RETENTION", 168*time.Hour),
		ConsumerBlock:    e.duration("STREAM_CONSUMER_BLOCK", 2*time.Second),
		ClaimMinIdle:     e.duration("STREAM_CLAIM_MIN_IDLE", 60*time.Second),
		ClaimInterval:    e.duration("STREAM_CLAIM_INTERVAL", 10*time.Second),
		ConsumerBatch:    e.int("STREAM_CONSUMER_BATCH", 10),
		HandlerTimeout:   e.duration("STREAM_HANDLER_TIMEOUT", 5*time.Second),
		RetryMaxAttempts: e.int("STREAM_RETRY_MAX_ATTEMPTS", 5),
		RetryBaseBackoff: e.duration("STREAM_RETRY_BASE_BACKOFF", 200*time.Millisecond),
		RetryMaxBackoff:  e.duration("STREAM_RETRY_MAX_BACKOFF", 2*time.Second),
	}
}

func loadOutbox(e *env) OutboxConfig {
	return OutboxConfig{
		BatchSize:    e.int("OUTBOX_BATCH_SIZE", 100),
		PollInterval: e.duration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		Lease:        e.duration("OUTBOX_LEASE", 30*time.Second),
		BaseBackoff:  e.duration("OUTBOX_BASE_BACKOFF", time.Second),
		MaxBackoff:   e.duration("OUTBOX_MAX_BACKOFF", 5*time.Minute),
		MaxAttempts:  e.int("OUTBOX_MAX_ATTEMPTS", 10),
	}
}

func loadWorker(e *env) WorkerConfig {
	return WorkerConfig{
		Concurrency:  e.int("WORKER_CONCURRENCY", 10),
		QueueSize:    e.int("WORKER_QUEUE_SIZE", 10),
		DrainTimeout: e.duration("WORKER_DRAIN_TIMEOUT", 10*time.Second),
	}
}

func loadRunProcessing(e *env) RunProcessingConfig {
	return RunProcessingConfig{
		PollInterval: e.duration("RUN_PROCESSING_POLL_INTERVAL", 2*time.Second),
		Lease:        e.duration("RUN_PROCESSING_LEASE", 30*time.Second),
		BatchSize:    e.int("RUN_PROCESSING_BATCH_SIZE", 500),
		ErrorBudget: ingestion.ErrorBudget{
			MaxErrorRate:  read(e, "INGESTION_ERROR_BUDGET_MAX_RATE", 0.05, parseFloat64),
			MinSampleRows: e.int("INGESTION_ERROR_BUDGET_MIN_ROWS", 100),
		},
	}
}

type env struct {
	lookup func(string) string
	errs   []error
}

// read returns def when key is unset or blank, and the parsed value otherwise. A value that does
// not parse is recorded as an error, never replaced by the default.
func read[T any](e *env, key string, def T, parse func(string) (T, error)) T {
	raw := strings.TrimSpace(e.lookup(key))
	if raw == "" {
		return def
	}
	v, err := parse(raw)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("invalid %s: %w", key, err))
	}
	return v
}

func (e *env) str(key, def string) string {
	return read(e, key, def, func(s string) (string, error) { return s, nil })
}

func (e *env) duration(key string, def time.Duration) time.Duration {
	return read(e, key, def, time.ParseDuration)
}

func (e *env) int(key string, def int) int {
	return read(e, key, def, strconv.Atoi)
}

func parseInt32(s string) (int32, error) {
	i, err := strconv.ParseInt(s, 10, 32)
	return int32(i), err
}

func parseFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}
