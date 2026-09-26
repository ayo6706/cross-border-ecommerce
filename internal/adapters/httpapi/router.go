package httpapi

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/logging"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type RouterConfig struct {
	Logger      *slog.Logger
	DB          Pinger
	DLQReplayer dlq.Replayer
}

func NewRouter(cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health/live", HandleLiveness())
	mux.HandleFunc("GET /health/ready", HandleReadiness(cfg.DB))
	mux.HandleFunc("POST /v1/dlq/{id}/replay", HandleDLQReplay(cfg.DLQReplayer, cfg.Logger))

	var handler http.Handler = mux
	// Order: RequestID -> Logging -> Recovery -> Mux
	handler = recoveryMiddleware(handler, cfg.Logger)
	handler = loggingMiddleware(handler, cfg.Logger)
	handler = requestIDMiddleware(handler)

	return handler
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *statusRecorder) Write(b []byte) (int, error) {
	if rec.statusCode == 0 {
		rec.statusCode = http.StatusOK
	}
	n, err := rec.ResponseWriter.Write(b)
	rec.bytesWritten += int64(n)
	return n, err
}

func generateRequestID() string {
	id, err := uuid.NewString()
	if err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return id
}

func sanitizeHeaderID(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || len(trimmed) > 128 {
		return ""
	}
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' || r == ':' {
			continue
		}
		return ""
	}
	return trimmed
}

func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := sanitizeHeaderID(r.Header.Get("X-Request-ID"))
		if reqID == "" {
			reqID = generateRequestID()
		}

		ctx := logging.WithRequestID(r.Context(), reqID)
		w.Header().Set("X-Request-ID", reqID)

		if corrID := sanitizeHeaderID(r.Header.Get("X-Correlation-ID")); corrID != "" {
			ctx = logging.WithCorrelationID(ctx, corrID)
			w.Header().Set("X-Correlation-ID", corrID)
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func loggingMiddleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(rec, r)

		if logger != nil {
			logger.InfoContext(r.Context(), "http request served",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.statusCode),
				slog.Int64("bytes", rec.bytesWritten),
				slog.Duration("duration", time.Since(start)),
				slog.String("remote_addr", r.RemoteAddr),
			)
		}
	})
}

func recoveryMiddleware(next http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if logger != nil {
					logger.ErrorContext(r.Context(), "panic recovered in http handler",
						slog.Any("panic", rec),
						slog.String("stack", string(debug.Stack())),
					)
				}
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
