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
	}

	for _, tc := range invalidEnvTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := config.LoadFromLookup(func(k string) string { return tc.env[k] })
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

func TestLoad_RequiresDatabaseURL(t *testing.T) {
	t.Parallel()

	_, err := config.LoadFromLookup(func(string) string { return "" })
	if !errors.Is(err, config.ErrEmptyDatabaseURL) {
		t.Fatalf("expected ErrEmptyDatabaseURL when DATABASE_URL is unset, got %v", err)
	}
}

// onlyDatabaseURL sets the one required variable and leaves everything else at its default.
func onlyDatabaseURL(key string) string {
	if key == "DATABASE_URL" {
		return "postgres://user:pass@dbhost:5432/testdb"
	}
	return ""
}
