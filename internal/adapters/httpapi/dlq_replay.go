package httpapi

import (
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

func HandleDLQReplay(replayer dlq.Replayer, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		if id == "" {
			writeJSONError(w, "dlq message id is required", http.StatusBadRequest)
			return
		}

		if replayer == nil {
			writeUnavailable(w, r, logger, "dlq service")
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
				writeInternalError(w, r, logger, "failed to replay dlq message", err, slog.String("id", id))
			}
			return
		}

		writeJSON(w, http.StatusAccepted, dlqReplayResponse{
			Status:         "REPLAYED",
			ReplayOutboxID: outboxID,
		})
	}
}
