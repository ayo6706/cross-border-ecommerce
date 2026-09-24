package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"
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
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Options configures NewLogger. Level is one of debug, info, warn, error;
// Format is "json" (default) or "text".
type Options struct {
	Level     string
	Format    string
	AddSource bool
}

// NewLogger builds a structured logger writing to w that also records
// request and correlation IDs carried in the context.
func NewLogger(w io.Writer, opts Options) *slog.Logger {
	handlerOpts := &slog.HandlerOptions{
		Level:     ParseLevel(opts.Level),
		AddSource: opts.AddSource,
	}

	var baseHandler slog.Handler
	if strings.EqualFold(strings.TrimSpace(opts.Format), "text") {
		baseHandler = slog.NewTextHandler(w, handlerOpts)
	} else {
		baseHandler = slog.NewJSONHandler(w, handlerOpts)
	}

	return slog.New(NewContextHandler(baseHandler))
}
