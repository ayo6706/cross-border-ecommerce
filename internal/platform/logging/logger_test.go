package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
)

func TestLogger_ContextPropagation(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	opts := logging.Options{
		Level:  "info",
		Format: "json",
	}

	logger := logging.NewLogger(&buf, opts)

	ctx := context.Background()
	ctx = logging.WithRequestID(ctx, "req-12345")
	ctx = logging.WithCorrelationID(ctx, "corr-67890")

	logger.InfoContext(ctx, "processing order event", slog.String("source", "shopify"))

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON log line: %v, raw: %s", err, buf.String())
	}

	if entry["msg"] != "processing order event" {
		t.Errorf("expected msg 'processing order event', got %v", entry["msg"])
	}
	if entry["level"] != "INFO" {
		t.Errorf("expected level 'INFO', got %v", entry["level"])
	}
	if entry["request_id"] != "req-12345" {
		t.Errorf("expected request_id 'req-12345', got %v", entry["request_id"])
	}
	if entry["correlation_id"] != "corr-67890" {
		t.Errorf("expected correlation_id 'corr-67890', got %v", entry["correlation_id"])
	}
	if entry["source"] != "shopify" {
		t.Errorf("expected source 'shopify', got %v", entry["source"])
	}
}

func TestLogger_LevelFiltering(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	opts := logging.Options{
		Level:  "warn",
		Format: "json",
	}

	logger := logging.NewLogger(&buf, opts)

	ctx := context.Background()
	logger.DebugContext(ctx, "debug line")
	logger.InfoContext(ctx, "info line")

	if buf.Len() != 0 {
		t.Errorf("expected buffer to be empty for debug and info logs when level is warn, got: %s", buf.String())
	}

	logger.WarnContext(ctx, "warning line")
	if !strings.Contains(buf.String(), "warning line") {
		t.Errorf("expected warning line in output, got: %s", buf.String())
	}
}

func TestLogger_WithAttrsAndGroup(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	opts := logging.Options{
		Level:  "info",
		Format: "json",
	}

	logger := logging.NewLogger(&buf, opts)
	childLogger := logger.With(slog.String("component", "ingestion_worker"))

	ctx := context.Background()
	ctx = logging.WithRequestID(ctx, "req-group-test")

	childLogger.InfoContext(ctx, "worker batch completed", slog.Group("metrics", slog.Int("count", 100)))

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v, raw: %s", err, buf.String())
	}

	if entry["component"] != "ingestion_worker" {
		t.Errorf("expected component 'ingestion_worker', got %v", entry["component"])
	}
	if entry["request_id"] != "req-group-test" {
		t.Errorf("expected request_id 'req-group-test', got %v", entry["request_id"])
	}

	metricsGroup, ok := entry["metrics"].(map[string]any)
	if !ok {
		t.Fatalf("expected metrics group object, got %v", entry["metrics"])
	}
	if metricsGroup["count"] != float64(100) {
		t.Errorf("expected metrics.count 100, got %v", metricsGroup["count"])
	}
}

func TestContextHelpers_Unset(t *testing.T) {
	t.Parallel()

	ctxBg := context.Background()
	if id := logging.RequestIDFromContext(ctxBg); id != "" {
		t.Errorf("expected empty string for unset request_id, got %s", id)
	}
	if id := logging.CorrelationIDFromContext(ctxBg); id != "" {
		t.Errorf("expected empty string for unset correlation_id, got %s", id)
	}

	ctxTodo := context.TODO()
	if id := logging.RequestIDFromContext(ctxTodo); id != "" {
		t.Errorf("expected empty string for unset request_id with context.TODO, got %s", id)
	}
	if id := logging.CorrelationIDFromContext(ctxTodo); id != "" {
		t.Errorf("expected empty string for unset correlation_id with context.TODO, got %s", id)
	}
}
