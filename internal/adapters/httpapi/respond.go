package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, msg string, code int) {
	writeJSON(w, code, errorResponse{Error: msg})
}

// writeInternalError logs the cause and answers with a generic 500: details stay in the logs.
func writeInternalError(
	w http.ResponseWriter, r *http.Request, logger *slog.Logger,
	msg string, err error, attrs ...slog.Attr,
) {
	if logger != nil {
		attrs = append([]slog.Attr{slog.Any("error", err)}, attrs...)
		logger.LogAttrs(r.Context(), slog.LevelError, msg, attrs...)
	}
	writeJSONError(w, "internal server error", http.StatusInternalServerError)
}

// writeUnavailable answers 503 for a dependency the router was built without.
func writeUnavailable(w http.ResponseWriter, r *http.Request, logger *slog.Logger, dependency string) {
	if logger != nil {
		logger.ErrorContext(r.Context(), dependency+" is not configured")
	}
	writeJSONError(w, dependency+" unavailable", http.StatusServiceUnavailable)
}
