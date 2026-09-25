package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	ErrInvalidPort       = errors.New("server port must be a valid integer between 1 and 65535")
	ErrInvalidTimeout    = errors.New("timeout values must be strictly positive")
	ErrEmptyDatabaseURL  = errors.New("DATABASE_URL is required")
	ErrEmptyRedisURL     = errors.New("REDIS_URL is required")
	ErrInvalidPoolLimits = errors.New("database connection pool minimum cannot exceed maximum")
	ErrInvalidLogLevel   = errors.New("log level must be one of 'debug', 'info', 'warn', 'error'")
	ErrInvalidLogFormat  = errors.New("log format must be one of 'json' or 'text'")
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

type OutboxConfig struct {
	BatchSize    int
	PollInterval time.Duration
	Lease        time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
	MaxAttempts  int
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
	Server   ServerConfig
	Database DatabaseConfig
	Log      LogConfig
	App      AppConfig
	Redis    RedisConfig
	Outbox   OutboxConfig
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

	addSource, err := getEnvBool(lookup, "LOG_ADD_SOURCE", false)
	if err != nil {
		return nil, fmt.Errorf("invalid LOG_ADD_SOURCE: %w", err)
	}

	appEnv := getEnvString(lookup, "APP_ENV", "development")

	logLevel := strings.ToLower(strings.TrimSpace(getEnvString(lookup, "LOG_LEVEL", "info")))
	logFormat := strings.ToLower(strings.TrimSpace(getEnvString(lookup, "LOG_FORMAT", "json")))

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
