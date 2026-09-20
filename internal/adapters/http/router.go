package http

import (
	"log/slog"
	"net/http"
	"time"
)

type RouterConfig struct {
	Logger *slog.Logger
	DB     Pinger
}

func NewRouter(cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health/live", HandleLiveness())
	mux.HandleFunc("GET /health/ready", HandleReadiness(cfg.DB))

	handler := recoveryMiddleware(mux, cfg.Logger)
	handler = loggingMiddleware(handler, cfg.Logger)

	return handler
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
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
				slog.Duration("duration", time.Since(start)),
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
					)
				}
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
