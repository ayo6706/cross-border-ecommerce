package logging

import (
	"context"
	"errors"
	"fmt"
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

var (
	ErrInvalidLevel  = errors.New("log level must be one of debug, info, warn, error")
	ErrInvalidFormat = errors.New("log format must be json or text")
)

// ParseLevel accepts the slog level names (debug, info, warn, error; any case). Anything else
// is an error: an unknown level never falls back to info.
func ParseLevel(s string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.TrimSpace(s))); err != nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidLevel, s)
	}
	return level, nil
}

type Format string

const (
	FormatJSON Format = "json"
	FormatText Format = "text"
)

// ParseFormat accepts json or text, in any case.
func ParseFormat(s string) (Format, error) {
	switch f := Format(strings.ToLower(strings.TrimSpace(s))); f {
	case FormatJSON, FormatText:
		return f, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrInvalidFormat, s)
	}
}

// Options configures NewLogger; platform/config parses them from LOG_* variables.
type Options struct {
	Level     slog.Level
	Format    Format
	AddSource bool
}

// NewLogger builds a structured logger writing to w that also records
// request and correlation IDs carried in the context.
func NewLogger(w io.Writer, opts Options) *slog.Logger {
	handlerOpts := &slog.HandlerOptions{
		Level:     opts.Level,
		AddSource: opts.AddSource,
	}

	var baseHandler slog.Handler
	if opts.Format == FormatText {
		baseHandler = slog.NewTextHandler(w, handlerOpts)
	} else {
		baseHandler = slog.NewJSONHandler(w, handlerOpts)
	}

	return slog.New(NewContextHandler(baseHandler))
}
