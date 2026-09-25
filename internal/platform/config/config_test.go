package config_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
)

func TestLoad_Defaults(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFromLookup(onlyDatabaseURL)
	if err != nil {
		t.Fatalf("unexpected error loading default config: %v", err)
	}

	if cfg.Server.Port != "8080" {
		t.Errorf("expected default Port 8080, got %s", cfg.Server.Port)
	}
	if cfg.Server.ReadTimeout != 10*time.Second {
		t.Errorf("expected default ReadTimeout 10s, got %v", cfg.Server.ReadTimeout)
	}
	if cfg.Server.WriteTimeout != 10*time.Second {
		t.Errorf("expected default WriteTimeout 10s, got %v", cfg.Server.WriteTimeout)
	}
	if cfg.Server.IdleTimeout != 60*time.Second {
		t.Errorf("expected default IdleTimeout 60s, got %v", cfg.Server.IdleTimeout)
	}
	if cfg.Server.ShutdownTimeout != 15*time.Second {
		t.Errorf("expected default ShutdownTimeout 15s, got %v", cfg.Server.ShutdownTimeout)
	}

	if cfg.Database.MaxConns != 25 {
		t.Errorf("expected default MaxConns 25, got %d", cfg.Database.MaxConns)
	}
	if cfg.Database.MinConns != 5 {
		t.Errorf("expected default MinConns 5, got %d", cfg.Database.MinConns)
	}
	if cfg.Database.MaxConnIdleTime != 15*time.Minute {
		t.Errorf("expected default MaxConnIdleTime 15m, got %v", cfg.Database.MaxConnIdleTime)
	}
	if cfg.Database.MaxConnLifetime != 1*time.Hour {
		t.Errorf("expected default MaxConnLifetime 1h, got %v", cfg.Database.MaxConnLifetime)
	}
	if cfg.Database.ConnectTimeout != 5*time.Second {
		t.Errorf("expected default ConnectTimeout 5s, got %v", cfg.Database.ConnectTimeout)
	}

	if cfg.Log.Level != "info" {
		t.Errorf("expected default Log Level info, got %s", cfg.Log.Level)
	}
	if cfg.Log.Format != "json" {
		t.Errorf("expected default Log Format json, got %s", cfg.Log.Format)
	}
	if cfg.Log.AddSource {
		t.Errorf("expected default Log AddSource false, got true")
	}

	if cfg.App.Environment != "development" {
		t.Errorf("expected default App Environment development, got %s", cfg.App.Environment)
	}
	if cfg.App.ServiceName != "cross-border-api" {
		t.Errorf("expected default App ServiceName cross-border-api, got %s", cfg.App.ServiceName)
	}

	if cfg.Redis.URL != "" {
		t.Errorf("expected default Redis URL empty, got %s", cfg.Redis.URL)
	}
	if cfg.Stream.Retention != 168*time.Hour {
		t.Errorf("expected default Stream Retention 168h, got %v", cfg.Stream.Retention)
	}
	if cfg.Stream.ConsumerBlock != 2*time.Second {
		t.Errorf("expected default Stream ConsumerBlock 2s, got %v", cfg.Stream.ConsumerBlock)
	}
	if cfg.Stream.ClaimMinIdle != 30*time.Second {
		t.Errorf("expected default Stream ClaimMinIdle 30s, got %v", cfg.Stream.ClaimMinIdle)
	}
	if cfg.Stream.ClaimInterval != 10*time.Second {
		t.Errorf("expected default Stream ClaimInterval 10s, got %v", cfg.Stream.ClaimInterval)
	}
	if cfg.Stream.ConsumerBatch != 10 {
		t.Errorf("expected default Stream ConsumerBatch 10, got %d", cfg.Stream.ConsumerBatch)
	}
	if cfg.Stream.HandlerTimeout != 5*time.Second {
		t.Errorf("expected default Stream HandlerTimeout 5s, got %v", cfg.Stream.HandlerTimeout)
	}
	if cfg.Worker.Concurrency != 10 {
		t.Errorf("expected default Worker Concurrency 10, got %d", cfg.Worker.Concurrency)
	}
	if cfg.Worker.QueueSize != 10 {
		t.Errorf("expected default Worker QueueSize 10, got %d", cfg.Worker.QueueSize)
	}
	if cfg.Worker.DrainTimeout != 10*time.Second {
		t.Errorf("expected default Worker DrainTimeout 10s, got %v", cfg.Worker.DrainTimeout)
	}
	if cfg.Outbox.BatchSize != 100 {
		t.Errorf("expected default Outbox BatchSize 100, got %d", cfg.Outbox.BatchSize)
	}
	if cfg.Outbox.PollInterval != 500*time.Millisecond {
		t.Errorf("expected default Outbox PollInterval 500ms, got %v", cfg.Outbox.PollInterval)
	}
	if cfg.Outbox.Lease != 30*time.Second {
		t.Errorf("expected default Outbox Lease 30s, got %v", cfg.Outbox.Lease)
	}
	if cfg.Outbox.BaseBackoff != 1*time.Second {
		t.Errorf("expected default Outbox BaseBackoff 1s, got %v", cfg.Outbox.BaseBackoff)
	}
	if cfg.Outbox.MaxBackoff != 5*time.Minute {
		t.Errorf("expected default Outbox MaxBackoff 5m, got %v", cfg.Outbox.MaxBackoff)
	}
	if cfg.Outbox.MaxAttempts != 10 {
		t.Errorf("expected default Outbox MaxAttempts 10, got %d", cfg.Outbox.MaxAttempts)
	}
}

func TestLoad_CustomOverrides(t *testing.T) {
	t.Parallel()

	env := map[string]string{
		"PORT":                    "9000",
		"SERVER_READ_TIMEOUT":     "5s",
		"SERVER_WRITE_TIMEOUT":    "5s",
		"SERVER_IDLE_TIMEOUT":     "30s",
		"SERVER_SHUTDOWN_TIMEOUT": "10s",
		"DATABASE_URL":            "postgres://user:pass@dbhost:5432/testdb",
		"DB_MAX_CONNS":            "50",
		"DB_MIN_CONNS":            "10",
		"DB_MAX_CONN_IDLE_TIME":   "10m",
		"DB_MAX_CONN_LIFETIME":    "30m",
		"DB_CONNECT_TIMEOUT":      "3s",
		"LOG_LEVEL":               "DEBUG",
		"LOG_FORMAT":              "TEXT",
		"LOG_ADD_SOURCE":          "true",
		"APP_ENV":                 "production",
		"SERVICE_NAME":            "trade-api",
		"REDIS_URL":               "redis://redis.internal:6379",
		"STREAM_RETENTION":        "72h",
		"STREAM_CONSUMER_BLOCK":   "5s",
		"STREAM_CLAIM_MIN_IDLE":   "2m",
		"STREAM_CLAIM_INTERVAL":   "15s",
		"STREAM_CONSUMER_BATCH":   "25",
		"STREAM_HANDLER_TIMEOUT":  "45s",
		"IDEMPOTENCY_LEASE_TTL":   "60s",
		"WORKER_CONCURRENCY":      "30",
		"WORKER_QUEUE_SIZE":       "50",
		"WORKER_DRAIN_TIMEOUT":    "20s",
		"OUTBOX_BATCH_SIZE":       "200",
		"OUTBOX_POLL_INTERVAL":    "1s",
		"OUTBOX_LEASE":            "45s",
		"OUTBOX_BASE_BACKOFF":     "2s",
		"OUTBOX_MAX_BACKOFF":      "10m",
		"OUTBOX_MAX_ATTEMPTS":     "20",
	}

	cfg, err := config.LoadFromLookup(func(k string) string {
		return env[k]
	})
	if err != nil {
		t.Fatalf("unexpected error loading overridden config: %v", err)
	}

	if cfg.Server.Port != "9000" {
		t.Errorf("expected Port 9000, got %s", cfg.Server.Port)
	}
	if cfg.Server.ReadTimeout != 5*time.Second {
		t.Errorf("expected ReadTimeout 5s, got %v", cfg.Server.ReadTimeout)
	}
	if cfg.Database.MaxConns != 50 {
		t.Errorf("expected MaxConns 50, got %d", cfg.Database.MaxConns)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("expected normalized Log Level debug, got %s", cfg.Log.Level)
	}
	if cfg.Log.Format != "text" {
		t.Errorf("expected normalized Log Format text, got %s", cfg.Log.Format)
	}
	if !cfg.Log.AddSource {
		t.Errorf("expected Log AddSource true, got false")
	}
	if cfg.App.Environment != "production" {
		t.Errorf("expected App Environment production, got %s", cfg.App.Environment)
	}
	if cfg.Redis.URL != "redis://redis.internal:6379" {
		t.Errorf("expected Redis URL redis://redis.internal:6379, got %s", cfg.Redis.URL)
	}
	if cfg.Stream.Retention != 72*time.Hour {
		t.Errorf("expected Stream Retention 72h, got %v", cfg.Stream.Retention)
	}
	if cfg.Stream.ConsumerBlock != 5*time.Second {
		t.Errorf("expected Stream ConsumerBlock 5s, got %v", cfg.Stream.ConsumerBlock)
	}
	if cfg.Stream.ClaimMinIdle != 2*time.Minute {
		t.Errorf("expected Stream ClaimMinIdle 2m, got %v", cfg.Stream.ClaimMinIdle)
	}
	if cfg.Stream.ClaimInterval != 15*time.Second {
		t.Errorf("expected Stream ClaimInterval 15s, got %v", cfg.Stream.ClaimInterval)
	}
	if cfg.Stream.ConsumerBatch != 25 {
		t.Errorf("expected Stream ConsumerBatch 25, got %d", cfg.Stream.ConsumerBatch)
	}
	if cfg.Stream.HandlerTimeout != 45*time.Second {
		t.Errorf("expected Stream HandlerTimeout 45s, got %v", cfg.Stream.HandlerTimeout)
	}
	if cfg.Worker.Concurrency != 30 {
		t.Errorf("expected Worker Concurrency 30, got %d", cfg.Worker.Concurrency)
	}
	if cfg.Worker.QueueSize != 50 {
		t.Errorf("expected Worker QueueSize 50, got %d", cfg.Worker.QueueSize)
	}
	if cfg.Worker.DrainTimeout != 20*time.Second {
		t.Errorf("expected Worker DrainTimeout 20s, got %v", cfg.Worker.DrainTimeout)
	}
	if cfg.Outbox.BatchSize != 200 {
		t.Errorf("expected Outbox BatchSize 200, got %d", cfg.Outbox.BatchSize)
	}
	if cfg.Outbox.PollInterval != 1*time.Second {
		t.Errorf("expected Outbox PollInterval 1s, got %v", cfg.Outbox.PollInterval)
	}
	if cfg.Outbox.Lease != 45*time.Second {
		t.Errorf("expected Outbox Lease 45s, got %v", cfg.Outbox.Lease)
	}
	if cfg.Outbox.BaseBackoff != 2*time.Second {
		t.Errorf("expected Outbox BaseBackoff 2s, got %v", cfg.Outbox.BaseBackoff)
	}
	if cfg.Outbox.MaxBackoff != 10*time.Minute {
		t.Errorf("expected Outbox MaxBackoff 10m, got %v", cfg.Outbox.MaxBackoff)
	}
	if cfg.Outbox.MaxAttempts != 20 {
		t.Errorf("expected Outbox MaxAttempts 20, got %d", cfg.Outbox.MaxAttempts)
	}
	if cfg.Idempotency.LeaseTTL != 60*time.Second {
		t.Errorf("expected Idempotency LeaseTTL 60s, got %v", cfg.Idempotency.LeaseTTL)
	}
}

func TestConfig_ValidationFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		modify      func(*config.Config)
		expectedErr error
	}{
		{
			name: "invalid port non-numeric",
			modify: func(c *config.Config) {
				c.Server.Port = "abc"
			},
			expectedErr: config.ErrInvalidPort,
		},
		{
			name: "invalid port out of range",
			modify: func(c *config.Config) {
				c.Server.Port = "70000"
			},
			expectedErr: config.ErrInvalidPort,
		},
		{
			name: "negative server timeout",
			modify: func(c *config.Config) {
				c.Server.ReadTimeout = -1 * time.Second
			},
			expectedErr: config.ErrInvalidTimeout,
		},
		{
			name: "empty database URL",
			modify: func(c *config.Config) {
				c.Database.URL = "   "
			},
			expectedErr: config.ErrEmptyDatabaseURL,
		},
		{
			name: "min conns exceeds max conns",
			modify: func(c *config.Config) {
				c.Database.MinConns = 30
				c.Database.MaxConns = 20
			},
			expectedErr: config.ErrInvalidPoolLimits,
		},
		{
			name: "max conns is zero",
			modify: func(c *config.Config) {
				c.Database.MaxConns = 0
			},
			expectedErr: config.ErrInvalidPoolLimits,
		},
		{
			name: "negative connect timeout",
			modify: func(c *config.Config) {
				c.Database.ConnectTimeout = 0
			},
			expectedErr: config.ErrInvalidTimeout,
		},
		{
			name: "invalid log level",
			modify: func(c *config.Config) {
				c.Log.Level = "verbose"
			},
			expectedErr: config.ErrInvalidLogLevel,
		},
		{
			name: "invalid log format",
			modify: func(c *config.Config) {
				c.Log.Format = "xml"
			},
			expectedErr: config.ErrInvalidLogFormat,
		},
		{
			name: "negative stream retention",
			modify: func(c *config.Config) {
				c.Stream.Retention = 0
			},
			expectedErr: config.ErrInvalidTimeout,
		},
		{
			name: "worker concurrency zero",
			modify: func(c *config.Config) {
				c.Worker.Concurrency = 0
			},
			expectedErr: config.ErrInvalidWorkerConfig,
		},
		{
			name: "worker queue size negative",
			modify: func(c *config.Config) {
				c.Worker.QueueSize = -1
			},
			expectedErr: config.ErrInvalidWorkerConfig,
		},
		{
			name: "worker drain timeout negative",
			modify: func(c *config.Config) {
				c.Worker.DrainTimeout = 0
			},
			expectedErr: config.ErrInvalidTimeout,
		},
		{
			name: "stream consumer batch zero",
			modify: func(c *config.Config) {
				c.Stream.ConsumerBatch = 0
			},
			expectedErr: config.ErrInvalidStreamConfig,
		},
		{
			name: "stream consumer batch exceeds 1000",
			modify: func(c *config.Config) {
				c.Stream.ConsumerBatch = 1001
			},
			expectedErr: config.ErrInvalidStreamConfig,
		},
		{
			name: "stream claim interval zero",
			modify: func(c *config.Config) {
				c.Stream.ClaimInterval = 0
			},
			expectedErr: config.ErrInvalidTimeout,
		},
		{
			name: "stream claim min idle less than or equal to handler timeout",
			modify: func(c *config.Config) {
				c.Stream.ClaimMinIdle = 5 * time.Second
				c.Stream.HandlerTimeout = 10 * time.Second
			},
			expectedErr: config.ErrInvalidStreamConfig,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := config.LoadFromLookup(onlyDatabaseURL)
			if err != nil {
				t.Fatalf("failed to load base config: %v", err)
			}

			tc.modify(cfg)
			err = cfg.Validate()
			if err == nil {
				t.Fatalf("expected error containing %v, got nil", tc.expectedErr)
			}
			if !errors.Is(err, tc.expectedErr) {
				t.Errorf("expected error wrapping %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestWorkerConfig_ValidateWithDBMaxConns(t *testing.T) {
	t.Parallel()

	w := config.WorkerConfig{
		Concurrency:  25,
		QueueSize:    10,
		DrainTimeout: 5 * time.Second,
	}

	if err := w.Validate(); err != nil {
		t.Fatalf("expected valid WorkerConfig, got %v", err)
	}

	// dbMaxConns <= 0 must fail loudly
	if err := w.ValidateAgainstDBPool(0); !errors.Is(err, config.ErrInvalidWorkerConfig) {
		t.Fatalf("expected ErrInvalidWorkerConfig for maxConns=0, got %v", err)
	}

	// 25 exceeds 80% of 25 (20)
	err := w.ValidateAgainstDBPool(25)
	if !errors.Is(err, config.ErrInvalidWorkerConfig) {
		t.Fatalf("expected ErrInvalidWorkerConfig, got %v", err)
	}

	// 25 is within 80% of 50 (40)
	err = w.ValidateAgainstDBPool(50)
	if err != nil {
		t.Fatalf("expected nil error when concurrency <= 80%% of db max conns, got %v", err)
	}
}

func TestLoad_InvalidEnvironmentValues(t *testing.T) {
	t.Parallel()

	invalidEnvTests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "malformed read timeout",
			env:  map[string]string{"SERVER_READ_TIMEOUT": "invalid-time"},
		},
		{
			name: "malformed max conns",
			env:  map[string]string{"DB_MAX_CONNS": "twenty-five"},
		},
		{
			name: "malformed add source bool",
			env:  map[string]string{"LOG_ADD_SOURCE": "not-a-bool"},
		},
		{
			name: "malformed outbox batch size",
			env:  map[string]string{"OUTBOX_BATCH_SIZE": "invalid-int"},
		},
		{
			name: "malformed outbox poll interval",
			env:  map[string]string{"OUTBOX_POLL_INTERVAL": "invalid-duration"},
		},
		{
			name: "malformed stream retention",
			env:  map[string]string{"STREAM_RETENTION": "invalid-duration"},
		},
	}

	for _, tc := range invalidEnvTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.LoadFromLookup(func(k string) string {
				if k == "DATABASE_URL" {
					return "postgres://user:pass@dbhost:5432/testdb"
				}
				return tc.env[k]
			})
			if err == nil {
				t.Fatalf("expected error for invalid env %s, got nil", tc.name)
			}
		})
	}
}

func TestDatabaseConfig_RedactedURL(t *testing.T) {
	t.Parallel()

	dbCfg := config.DatabaseConfig{
		URL: "postgres://custom_user:super_secret_pw@db.internal:5432/trade_prod?sslmode=require",
	}

	redacted := dbCfg.RedactedURL()
	if redacted != "postgres://custom_user:xxxxx@db.internal:5432/trade_prod?sslmode=require" {
		t.Errorf("expected redacted password, got: %s", redacted)
	}

	emptyCfg := config.DatabaseConfig{URL: ""}
	if emptyCfg.RedactedURL() != "" {
		t.Errorf("expected empty string for empty URL, got: %s", emptyCfg.RedactedURL())
	}

	malformedCfg := config.DatabaseConfig{URL: "postgres://%invalid-url%:pass@/db"}
	if malformedCfg.RedactedURL() != "[malformed database URL]" {
		t.Errorf("expected [malformed database URL] on parse error, got: %s", malformedCfg.RedactedURL())
	}
}

func TestRedisConfig_Validation(t *testing.T) {
	t.Parallel()

	rEmpty := config.RedisConfig{URL: "   "}
	if !errors.Is(rEmpty.Validate(), config.ErrEmptyRedisURL) {
		t.Errorf("expected ErrEmptyRedisURL, got: %v", rEmpty.Validate())
	}

	rValid := config.RedisConfig{URL: "redis://:secretpass@redis.internal:6379"}
	if err := rValid.Validate(); err != nil {
		t.Errorf("expected nil error for valid redis URL, got: %v", err)
	}
}

func TestLoad_RequiresDatabaseURL(t *testing.T) {
	t.Parallel()

	_, err := config.LoadFromLookup(func(string) string { return "" })
	if !errors.Is(err, config.ErrEmptyDatabaseURL) {
		t.Fatalf("expected ErrEmptyDatabaseURL when DATABASE_URL is unset, got %v", err)
	}
}

func TestIdempotencyConfig_Validation(t *testing.T) {
	t.Parallel()

	iZero := config.IdempotencyConfig{LeaseTTL: 0}
	if !errors.Is(iZero.Validate(), config.ErrInvalidTimeout) {
		t.Errorf("expected ErrInvalidTimeout for zero lease TTL, got: %v", iZero.Validate())
	}

	iNeg := config.IdempotencyConfig{LeaseTTL: -1 * time.Second}
	if !errors.Is(iNeg.Validate(), config.ErrInvalidTimeout) {
		t.Errorf("expected ErrInvalidTimeout for negative lease TTL, got: %v", iNeg.Validate())
	}

	iValid := config.IdempotencyConfig{LeaseTTL: 30 * time.Second}
	if err := iValid.Validate(); err != nil {
		t.Errorf("expected nil for valid lease TTL, got: %v", err)
	}
}

func TestConfig_IdempotencyLeaseTTLMustExceedHandlerTimeout(t *testing.T) {
	t.Parallel()

	// Default stream handler timeout is 5s, idempotency lease TTL set to 5s (equal -> invalid)
	_, err := config.LoadFromLookup(func(key string) string {
		switch key {
		case "DATABASE_URL":
			return "postgres://user:pass@dbhost:5432/testdb"
		case "IDEMPOTENCY_LEASE_TTL":
			return "5s"
		case "STREAM_HANDLER_TIMEOUT":
			return "5s"
		default:
			return ""
		}
	})
	if err == nil {
		t.Fatal("expected error when IDEMPOTENCY_LEASE_TTL <= STREAM_HANDLER_TIMEOUT, got nil")
	}

	// Lease TTL 4s < Handler timeout 5s -> invalid
	_, err = config.LoadFromLookup(func(key string) string {
		switch key {
		case "DATABASE_URL":
			return "postgres://user:pass@dbhost:5432/testdb"
		case "IDEMPOTENCY_LEASE_TTL":
			return "4s"
		case "STREAM_HANDLER_TIMEOUT":
			return "5s"
		default:
			return ""
		}
	})
	if err == nil {
		t.Fatal("expected error when IDEMPOTENCY_LEASE_TTL < STREAM_HANDLER_TIMEOUT, got nil")
	}

	// Lease TTL 10s > Handler timeout 5s -> valid
	cfg, err := config.LoadFromLookup(func(key string) string {
		switch key {
		case "DATABASE_URL":
			return "postgres://user:pass@dbhost:5432/testdb"
		case "IDEMPOTENCY_LEASE_TTL":
			return "10s"
		case "STREAM_HANDLER_TIMEOUT":
			return "5s"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("expected valid config when LeaseTTL > HandlerTimeout, got: %v", err)
	}
	if cfg.Idempotency.LeaseTTL != 10*time.Second {
		t.Errorf("expected 10s lease TTL, got: %v", cfg.Idempotency.LeaseTTL)
	}
}

// onlyDatabaseURL sets the one required variable and leaves everything else at its default.
func onlyDatabaseURL(key string) string {
	if key == "DATABASE_URL" {
		return "postgres://user:pass@dbhost:5432/testdb"
	}
	return ""
}
