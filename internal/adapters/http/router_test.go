package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	httpAdapter "github.com/ayo6706/cross-border-ecommerce/internal/adapters/http"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
)

func TestRouter_RequestIDMiddleware(t *testing.T) {
	t.Parallel()

	router := httpAdapter.NewRouter(httpAdapter.RouterConfig{})

	t.Run("generates request ID if not provided", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
		rr := httptest.NewRecorder()

		router.ServeHTTP(rr, req)

		reqID := rr.Header().Get("X-Request-ID")
		if reqID == "" {
			t.Errorf("expected X-Request-ID header in response, got empty")
		}
	})

	t.Run("preserves incoming request and correlation ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
		req.Header.Set("X-Request-ID", "custom-req-id-123")
		req.Header.Set("X-Correlation-ID", "custom-corr-id-456")
		rr := httptest.NewRecorder()

		router.ServeHTTP(rr, req)

		if rr.Header().Get("X-Request-ID") != "custom-req-id-123" {
			t.Errorf("expected X-Request-ID 'custom-req-id-123', got %s", rr.Header().Get("X-Request-ID"))
		}
		if rr.Header().Get("X-Correlation-ID") != "custom-corr-id-456" {
			t.Errorf("expected X-Correlation-ID 'custom-corr-id-456', got %s", rr.Header().Get("X-Correlation-ID"))
		}
	})
}

func TestRouter_LoggingAndPanicRecovery(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	cfg := config.LogConfig{Level: "info", Format: "json"}
	logger := logging.NewLogger(cfg, &logBuf)

	router := httpAdapter.NewRouter(httpAdapter.RouterConfig{
		Logger: logger,
	})

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	req.Header.Set("X-Request-ID", "req-log-verify-001")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var logEntry map[string]any
	if err := json.Unmarshal(logBuf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to decode log entry JSON: %v, raw: %s", err, logBuf.String())
	}

	if logEntry["request_id"] != "req-log-verify-001" {
		t.Errorf("expected log request_id 'req-log-verify-001', got %v", logEntry["request_id"])
	}
	if logEntry["method"] != "GET" {
		t.Errorf("expected log method GET, got %v", logEntry["method"])
	}
	if logEntry["path"] != "/health/live" {
		t.Errorf("expected log path /health/live, got %v", logEntry["path"])
	}
	if logEntry["status"] != float64(200) {
		t.Errorf("expected log status 200, got %v", logEntry["status"])
	}
}

func TestPanicRecovery(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	cfg := config.LogConfig{Level: "info", Format: "json"}
	logger := logging.NewLogger(cfg, &logBuf)

	panickingPinger := &panickingPingerMock{}
	router := httpAdapter.NewRouter(httpAdapter.RouterConfig{
		Logger: logger,
		DB:     panickingPinger,
	})

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	req.Header.Set("X-Request-ID", "req-panic-tracing-999")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 status on panic, got %d", rr.Code)
	}

	var logEntry map[string]any
	if err := json.Unmarshal(logBuf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to decode log JSON: %v, raw: %s", err, logBuf.String())
	}

	if logEntry["request_id"] != "req-panic-tracing-999" {
		t.Errorf("expected panic log to retain request_id 'req-panic-tracing-999', got: %v", logEntry["request_id"])
	}
	if logEntry["level"] != "ERROR" {
		t.Errorf("expected ERROR level log, got: %v", logEntry["level"])
	}
	if logEntry["panic"] != "catastrophic db hardware failure" {
		t.Errorf("expected panic details in log, got: %v", logEntry["panic"])
	}
}

type panickingPingerMock struct{}

func (p *panickingPingerMock) Ping(ctx context.Context) error {
	panic("catastrophic db hardware failure")
}
