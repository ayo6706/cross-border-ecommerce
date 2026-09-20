package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/config"
)

type contextKey string

const (
	requestIDKey     contextKey = "request_id"
	correlationIDKey contextKey = "correlation_id"
)

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(requestIDKey).(string); ok {
		return val
	}
	return ""
}

func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey, correlationID)
}

func CorrelationIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(correlationIDKey).(string); ok {
		return val
	}
	return ""
}

type ContextHandler struct {
	inner slog.Handler
}

func NewContextHandler(inner slog.Handler) *ContextHandler {
	return &ContextHandler{inner: inner}
}

func (h *ContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *ContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if ctx != nil {
		if reqID := RequestIDFromContext(ctx); reqID != "" {
			record.AddAttrs(slog.String("request_id", reqID))
		}
		if corrID := CorrelationIDFromContext(ctx); corrID != "" {
			record.AddAttrs(slog.String("correlation_id", corrID))
		}
	}
	return h.inner.Handle(ctx, record)
}

func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{inner: h.inner.WithGroup(name)}
}

func ParseLevel(levelStr string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(levelStr)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func NewLogger(cfg config.LogConfig, out ...io.Writer) *slog.Logger {
	var targetOut io.Writer = os.Stdout
	if len(out) > 0 && out[0] != nil {
		targetOut = out[0]
	}

	level := ParseLevel(cfg.Level)
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: cfg.AddSource,
	}

	var baseHandler slog.Handler
	if strings.ToLower(strings.TrimSpace(cfg.Format)) == "text" {
		baseHandler = slog.NewTextHandler(targetOut, opts)
	} else {
		baseHandler = slog.NewJSONHandler(targetOut, opts)
	}

	return slog.New(NewContextHandler(baseHandler))
}
