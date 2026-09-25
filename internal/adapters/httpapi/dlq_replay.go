package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
)

type dlqReplayResponse struct {
	Status         string `json:"status"`
	ReplayOutboxID string `json:"replay_outbox_id"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func HandleDLQReplay(replayer dlq.Replayer, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			writeJSONError(w, "dlq message id is required", http.StatusBadRequest)
			return
		}

		if replayer == nil {
			if logger != nil {
				logger.ErrorContext(r.Context(), "dlq replayer is not configured")
			}
			writeJSONError(w, "dlq service unavailable", http.StatusServiceUnavailable)
			return
		}

		outboxID, err := replayer.Replay(r.Context(), id)
		if err != nil {
			switch {
			case errors.Is(err, dlq.ErrInvalidDLQID):
				writeJSONError(w, "invalid dlq id format", http.StatusBadRequest)
			case errors.Is(err, dlq.ErrDLQNotFound):
				writeJSONError(w, "dlq message not found", http.StatusNotFound)
			case errors.Is(err, dlq.ErrAlreadyReplayed):
				writeJSONError(w, "dlq message already replayed", http.StatusConflict)
			case errors.Is(err, dlq.ErrNotReplayable):
				writeJSONError(w, "dlq message cannot be replayed", http.StatusConflict)
			default:
				if logger != nil {
					logger.ErrorContext(r.Context(), "failed to replay dlq message",
						slog.String("id", id),
						slog.Any("error", err),
					)
				}
				writeJSONError(w, "internal server error", http.StatusInternalServerError)
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(dlqReplayResponse{
			Status:         "REPLAYED",
			ReplayOutboxID: outboxID,
		})
	}
}

func writeJSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: msg})
}
