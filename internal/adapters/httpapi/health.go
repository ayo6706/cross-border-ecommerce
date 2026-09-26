package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type HealthStatus struct {
	Status  string            `json:"status"`
	Details map[string]string `json:"details,omitempty"`
}

func HandleLiveness() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(HealthStatus{Status: "UP"})
	}
}

func writeNotReady(w http.ResponseWriter) {
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(HealthStatus{
		Status: "NOT_READY",
		Details: map[string]string{
			"database": "unavailable",
		},
	})
}

func HandleReadiness(db Pinger, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if db == nil {
			writeNotReady(w)
			return
		}

		pingCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := db.Ping(pingCtx); err != nil {
			if logger != nil {
				logger.ErrorContext(r.Context(), "readiness database ping failed", slog.Any("error", err))
			}
			writeNotReady(w)
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(HealthStatus{
			Status: "READY",
			Details: map[string]string{
				"database": "OK",
			},
		})
	}
}
