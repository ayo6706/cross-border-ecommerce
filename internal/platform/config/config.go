package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/backoff"
)

var (
	ErrInvalidPort         = errors.New("server port must be a valid integer between 1 and 65535")
	ErrInvalidTimeout      = errors.New("timeout values must be strictly positive")
	ErrEmptyDatabaseURL    = errors.New("DATABASE_URL is required")
	ErrEmptyRedisURL       = errors.New("REDIS_URL is required")
	ErrInvalidPoolLimits   = errors.New("database connection pool minimum cannot exceed maximum")
	ErrInvalidLogLevel     = errors.New("log level must be one of 'debug', 'info', 'warn', 'error'")
	ErrInvalidLogFormat    = errors.New("log format must be one of 'json' or 'text'")
	ErrInvalidStreamConfig = errors.New("invalid stream configuration")
	ErrInvalidWorkerConfig = errors.New("invalid worker configuration")
)

type ServerConfig struct {
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

type DatabaseConfig struct {
	URL             string
	MaxConns        int32
	MinConns        int32
	MaxConnIdleTime time.Duration
	MaxConnLifetime time.Duration
	ConnectTimeout  time.Duration
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
	if s.ConsumerBatch <= 0 || s.ConsumerBatch > 1000 {
		return fmt.Errorf("%w: consumer batch must be between 1 and 1000, got %d", ErrInvalidStreamConfig, s.ConsumerBatch)
	}
	if s.ClaimMinIdle <= s.HandlerTimeout {
		return fmt.Errorf("%w: claim min idle (%v) must be strictly greater than handler timeout (%v)",
			ErrInvalidStreamConfig, s.ClaimMinIdle, s.HandlerTimeout)
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

func (w WorkerConfig) ValidateAgainstDBPool(dbMaxConns int32) error {
	if err := w.Validate(); err != nil {
		return err
	}
	if dbMaxConns <= 0 {
		return fmt.Errorf("%w: database max connections must be strictly positive, got %d", ErrInvalidWorkerConfig, dbMaxConns)
	}
	maxAllowed := int(float64(dbMaxConns) * 0.8)
	if w.Concurrency > maxAllowed {
		return fmt.Errorf("%w: WORKER_CONCURRENCY (%d) exceeds 80%% of DB_MAX_CONNS (%d, max %d)",
			ErrInvalidWorkerConfig, w.Concurrency, dbMaxConns, maxAllowed)
	}
	return nil
}

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

type LogConfig struct {
	Level     string
	Format    string
	AddSource bool
}

type AppConfig struct {
	Environment string
	ServiceName string
}

type Config struct {
	Server      ServerConfig
	Database    DatabaseConfig
	Log         LogConfig
	App         AppConfig
	Redis       RedisConfig
	Stream      StreamConfig
	Outbox      OutboxConfig
	Worker      WorkerConfig
	Idempotency IdempotencyConfig
}

func Load() (*Config, error) {
	return LoadFromLookup(os.Getenv)
}

func LoadFromLookup(lookup func(string) string) (*Config, error) {
	if lookup == nil {
		lookup = os.Getenv
	}

	readTimeout, err := getEnvDuration(lookup, "SERVER_READ_TIMEOUT", 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid SERVER_READ_TIMEOUT: %w", err)
	}

	writeTimeout, err := getEnvDuration(lookup, "SERVER_WRITE_TIMEOUT", 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid SERVER_WRITE_TIMEOUT: %w", err)
	}

	idleTimeout, err := getEnvDuration(lookup, "SERVER_IDLE_TIMEOUT", 60*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid SERVER_IDLE_TIMEOUT: %w", err)
	}

	shutdownTimeout, err := getEnvDuration(lookup, "SERVER_SHUTDOWN_TIMEOUT", 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid SERVER_SHUTDOWN_TIMEOUT: %w", err)
	}

	maxConns, err := getEnvInt32(lookup, "DB_MAX_CONNS", 25)
	if err != nil {
		return nil, fmt.Errorf("invalid DB_MAX_CONNS: %w", err)
	}

	minConns, err := getEnvInt32(lookup, "DB_MIN_CONNS", 5)
	if err != nil {
		return nil, fmt.Errorf("invalid DB_MIN_CONNS: %w", err)
	}

	maxConnIdleTime, err := getEnvDuration(lookup, "DB_MAX_CONN_IDLE_TIME", 15*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("invalid DB_MAX_CONN_IDLE_TIME: %w", err)
	}

	maxConnLifetime, err := getEnvDuration(lookup, "DB_MAX_CONN_LIFETIME", 1*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("invalid DB_MAX_CONN_LIFETIME: %w", err)
	}

	connectTimeout, err := getEnvDuration(lookup, "DB_CONNECT_TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid DB_CONNECT_TIMEOUT: %w", err)
	}

	outboxBatchSize, err := getEnvInt(lookup, "OUTBOX_BATCH_SIZE", 100)
	if err != nil {
		return nil, fmt.Errorf("invalid OUTBOX_BATCH_SIZE: %w", err)
	}

	outboxPollInterval, err := getEnvDuration(lookup, "OUTBOX_POLL_INTERVAL", 500*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("invalid OUTBOX_POLL_INTERVAL: %w", err)
	}

	outboxLease, err := getEnvDuration(lookup, "OUTBOX_LEASE", 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid OUTBOX_LEASE: %w", err)
	}

	outboxBaseBackoff, err := getEnvDuration(lookup, "OUTBOX_BASE_BACKOFF", 1*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid OUTBOX_BASE_BACKOFF: %w", err)
	}

	outboxMaxBackoff, err := getEnvDuration(lookup, "OUTBOX_MAX_BACKOFF", 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("invalid OUTBOX_MAX_BACKOFF: %w", err)
	}

	outboxMaxAttempts, err := getEnvInt(lookup, "OUTBOX_MAX_ATTEMPTS", 10)
	if err != nil {
		return nil, fmt.Errorf("invalid OUTBOX_MAX_ATTEMPTS: %w", err)
	}

	streamRetention, err := getEnvDuration(lookup, "STREAM_RETENTION", 168*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("invalid STREAM_RETENTION: %w", err)
	}

	streamConsumerBlock, err := getEnvDuration(lookup, "STREAM_CONSUMER_BLOCK", 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid STREAM_CONSUMER_BLOCK: %w", err)
	}

	streamClaimMinIdle, err := getEnvDuration(lookup, "STREAM_CLAIM_MIN_IDLE", 60*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid STREAM_CLAIM_MIN_IDLE: %w", err)
	}

	streamClaimInterval, err := getEnvDuration(lookup, "STREAM_CLAIM_INTERVAL", 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid STREAM_CLAIM_INTERVAL: %w", err)
	}

	streamConsumerBatch, err := getEnvInt(lookup, "STREAM_CONSUMER_BATCH", 10)
	if err != nil {
		return nil, fmt.Errorf("invalid STREAM_CONSUMER_BATCH: %w", err)
	}

	streamHandlerTimeout, err := getEnvDuration(lookup, "STREAM_HANDLER_TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid STREAM_HANDLER_TIMEOUT: %w", err)
	}

	streamRetryMaxAttempts, err := getEnvInt(lookup, "STREAM_RETRY_MAX_ATTEMPTS", 5)
	if err != nil {
		return nil, fmt.Errorf("invalid STREAM_RETRY_MAX_ATTEMPTS: %w", err)
	}

	streamRetryBaseBackoff, err := getEnvDuration(lookup, "STREAM_RETRY_BASE_BACKOFF", 200*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("invalid STREAM_RETRY_BASE_BACKOFF: %w", err)
	}

	streamRetryMaxBackoff, err := getEnvDuration(lookup, "STREAM_RETRY_MAX_BACKOFF", 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid STREAM_RETRY_MAX_BACKOFF: %w", err)
	}

	workerConcurrency, err := getEnvInt(lookup, "WORKER_CONCURRENCY", 10)
	if err != nil {
		return nil, fmt.Errorf("invalid WORKER_CONCURRENCY: %w", err)
	}

	workerQueueSize, err := getEnvInt(lookup, "WORKER_QUEUE_SIZE", 10)
	if err != nil {
		return nil, fmt.Errorf("invalid WORKER_QUEUE_SIZE: %w", err)
	}

	workerDrainTimeout, err := getEnvDuration(lookup, "WORKER_DRAIN_TIMEOUT", 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid WORKER_DRAIN_TIMEOUT: %w", err)
	}

	addSource, err := getEnvBool(lookup, "LOG_ADD_SOURCE", false)
	if err != nil {
		return nil, fmt.Errorf("invalid LOG_ADD_SOURCE: %w", err)
	}

	appEnv := getEnvString(lookup, "APP_ENV", "development")

	logLevel := strings.ToLower(strings.TrimSpace(getEnvString(lookup, "LOG_LEVEL", "info")))
	logFormat := strings.ToLower(strings.TrimSpace(getEnvString(lookup, "LOG_FORMAT", "json")))

	idempotencyLeaseTTL, err := getEnvDuration(lookup, "IDEMPOTENCY_LEASE_TTL", 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("invalid IDEMPOTENCY_LEASE_TTL: %w", err)
	}

	cfg := &Config{
		Server: ServerConfig{
			Port:            getEnvString(lookup, "PORT", "8080"),
			ReadTimeout:     readTimeout,
			WriteTimeout:    writeTimeout,
			IdleTimeout:     idleTimeout,
			ShutdownTimeout: shutdownTimeout,
		},
		Database: DatabaseConfig{
			URL:             getEnvString(lookup, "DATABASE_URL", ""),
			MaxConns:        maxConns,
			MinConns:        minConns,
			MaxConnIdleTime: maxConnIdleTime,
			MaxConnLifetime: maxConnLifetime,
			ConnectTimeout:  connectTimeout,
		},
		Log: LogConfig{
			Level:     logLevel,
			Format:    logFormat,
			AddSource: addSource,
		},
		App: AppConfig{
			Environment: appEnv,
			ServiceName: getEnvString(lookup, "SERVICE_NAME", "cross-border-api"),
		},
		Redis: RedisConfig{
			URL: getEnvString(lookup, "REDIS_URL", ""),
		},
		Stream: StreamConfig{
			Retention:        streamRetention,
			ConsumerBlock:    streamConsumerBlock,
			ClaimMinIdle:     streamClaimMinIdle,
			ClaimInterval:    streamClaimInterval,
			ConsumerBatch:    streamConsumerBatch,
			HandlerTimeout:   streamHandlerTimeout,
			RetryMaxAttempts: streamRetryMaxAttempts,
			RetryBaseBackoff: streamRetryBaseBackoff,
			RetryMaxBackoff:  streamRetryMaxBackoff,
		},
		Worker: WorkerConfig{
			Concurrency:  workerConcurrency,
			QueueSize:    workerQueueSize,
			DrainTimeout: workerDrainTimeout,
		},
		Idempotency: IdempotencyConfig{
			LeaseTTL: idempotencyLeaseTTL,
		},
		Outbox: OutboxConfig{
			BatchSize:    outboxBatchSize,
			PollInterval: outboxPollInterval,
			Lease:        outboxLease,
			BaseBackoff:  outboxBaseBackoff,
			MaxBackoff:   outboxMaxBackoff,
			MaxAttempts:  outboxMaxAttempts,
		},
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	portNum, err := strconv.Atoi(c.Server.Port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return fmt.Errorf("%w: '%s'", ErrInvalidPort, c.Server.Port)
	}

	if c.Server.ReadTimeout <= 0 || c.Server.WriteTimeout <= 0 || c.Server.IdleTimeout <= 0 || c.Server.ShutdownTimeout <= 0 {
		return fmt.Errorf("%w for server configuration", ErrInvalidTimeout)
	}

	if strings.TrimSpace(c.Database.URL) == "" {
		return ErrEmptyDatabaseURL
	}

	if c.Database.MinConns < 0 || c.Database.MaxConns <= 0 || c.Database.MinConns > c.Database.MaxConns {
		return fmt.Errorf("%w: min=%d, max=%d", ErrInvalidPoolLimits, c.Database.MinConns, c.Database.MaxConns)
	}

	if c.Database.ConnectTimeout <= 0 || c.Database.MaxConnIdleTime <= 0 || c.Database.MaxConnLifetime <= 0 {
		return fmt.Errorf("%w for database configuration", ErrInvalidTimeout)
	}

	switch strings.ToLower(strings.TrimSpace(c.Log.Level)) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("%w: '%s'", ErrInvalidLogLevel, c.Log.Level)
	}

	switch strings.ToLower(strings.TrimSpace(c.Log.Format)) {
	case "json", "text":
	default:
		return fmt.Errorf("%w: '%s'", ErrInvalidLogFormat, c.Log.Format)
	}

	if err := c.Stream.Validate(); err != nil {
		return err
	}

	if err := c.Worker.Validate(); err != nil {
		return err
	}

	if err := c.Idempotency.Validate(); err != nil {
		return err
	}

	if c.Idempotency.LeaseTTL <= c.Stream.HandlerTimeout {
		return fmt.Errorf("idempotency lease TTL (%v) must be strictly greater than stream handler timeout (%v)",
			c.Idempotency.LeaseTTL, c.Stream.HandlerTimeout)
	}

	return nil
}

func getEnvString(lookup func(string) string, key, defaultVal string) string {
	if val := strings.TrimSpace(lookup(key)); val != "" {
		return val
	}
	return defaultVal
}

func getEnvDuration(lookup func(string) string, key string, defaultVal time.Duration) (time.Duration, error) {
	val := strings.TrimSpace(lookup(key))
	if val == "" {
		return defaultVal, nil
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("parse duration '%s': %w", val, err)
	}
	return d, nil
}

func getEnvInt(lookup func(string) string, key string, defaultVal int) (int, error) {
	val := strings.TrimSpace(lookup(key))
	if val == "" {
		return defaultVal, nil
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("parse int '%s': %w", val, err)
	}
	return i, nil
}

func getEnvInt32(lookup func(string) string, key string, defaultVal int32) (int32, error) {
	val := strings.TrimSpace(lookup(key))
	if val == "" {
		return defaultVal, nil
	}
	i, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse int32 '%s': %w", val, err)
	}
	return int32(i), nil
}

func getEnvBool(lookup func(string) string, key string, defaultVal bool) (bool, error) {
	val := strings.TrimSpace(lookup(key))
	if val == "" {
		return defaultVal, nil
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return false, fmt.Errorf("parse bool '%s': %w", val, err)
	}
	return b, nil
}
