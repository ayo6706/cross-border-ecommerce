package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/httpapi"
	"github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDLQReplayer struct {
	replayFn func(ctx context.Context, id string) (string, error)
}

func (m *mockDLQReplayer) Replay(ctx context.Context, id string) (string, error) {
	if m.replayFn != nil {
		return m.replayFn(ctx, id)
	}
	return "outbox-123", nil
}

func TestDLQReplay_Handler(t *testing.T) {
	tests := []struct {
		name           string
		id             string
		replayFn       func(ctx context.Context, id string) (string, error)
		expectedStatus int
		expectedBody   map[string]any
	}{
		{
			name: "successful replay returns 202 Accepted with outbox ID",
			id:   "dlq-uuid-1",
			replayFn: func(ctx context.Context, id string) (string, error) {
				return "outbox-abc-123", nil
			},
			expectedStatus: http.StatusAccepted,
			expectedBody: map[string]any{
				"status":           "REPLAYED",
				"replay_outbox_id": "outbox-abc-123",
			},
		},
		{
			name: "not found returns 404",
			id:   "dlq-uuid-2",
			replayFn: func(ctx context.Context, id string) (string, error) {
				return "", dlq.ErrDLQNotFound
			},
			expectedStatus: http.StatusNotFound,
			expectedBody: map[string]any{
				"error": "dlq message not found",
			},
		},
		{
			name: "already replayed returns 409 Conflict",
			id:   "dlq-uuid-3",
			replayFn: func(ctx context.Context, id string) (string, error) {
				return "", dlq.ErrAlreadyReplayed
			},
			expectedStatus: http.StatusConflict,
			expectedBody: map[string]any{
				"error": "dlq message already replayed",
			},
		},
		{
			name: "corrupt not replayable returns 409 Conflict",
			id:   "dlq-uuid-4",
			replayFn: func(ctx context.Context, id string) (string, error) {
				return "", dlq.ErrNotReplayable
			},
			expectedStatus: http.StatusConflict,
			expectedBody: map[string]any{
				"error": "dlq message cannot be replayed",
			},
		},
		{
			name: "invalid dlq id format returns 400 Bad Request",
			id:   "not-a-valid-uuid",
			replayFn: func(ctx context.Context, id string) (string, error) {
				return "", dlq.ErrInvalidDLQID
			},
			expectedStatus: http.StatusBadRequest,
			expectedBody: map[string]any{
				"error": "invalid dlq id format",
			},
		},
		{
			name: "internal server error returns 500",
			id:   "dlq-uuid-5",
			replayFn: func(ctx context.Context, id string) (string, error) {
				return "", errors.New("db connection failure")
			},
			expectedStatus: http.StatusInternalServerError,
			expectedBody: map[string]any{
				"error": "internal server error",
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			store := &mockDLQReplayer{replayFn: tc.replayFn}
			handler := httpapi.NewRouter(httpapi.RouterConfig{
				DLQReplayer: store,
			})

			req := httptest.NewRequest(http.MethodPost, "/v1/dlq/"+tc.id+"/replay", nil)
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			assert.Equal(t, tc.expectedStatus, rec.Code)

			var body map[string]any
			err := json.NewDecoder(rec.Body).Decode(&body)
			require.NoError(t, err)

			for k, v := range tc.expectedBody {
				assert.Equal(t, v, body[k], "field %s mismatch", k)
			}
		})
	}
}
