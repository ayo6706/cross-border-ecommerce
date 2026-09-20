package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	httpAdapter "github.com/ayo6706/cross-border-ecommerce/internal/adapters/http"
)

func TestHealthEndpoints(t *testing.T) {
	t.Parallel()

	router := httpAdapter.NewRouter(httpAdapter.RouterConfig{})

	tests := []struct {
		name           string
		path           string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "liveness returns 200 UP",
			path:           "/health/live",
			expectedStatus: http.StatusOK,
			expectedBody:   "UP",
		},
		{
			name:           "readiness returns 200 READY",
			path:           "/health/ready",
			expectedStatus: http.StatusOK,
			expectedBody:   "READY",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rr := httptest.NewRecorder()

			router.ServeHTTP(rr, req)

			if rr.Code != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d", tc.expectedStatus, rr.Code)
			}

			var resp httpAdapter.HealthStatus
			if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode response json: %v", err)
			}

			if resp.Status != tc.expectedBody {
				t.Errorf("expected status %q, got %q", tc.expectedBody, resp.Status)
			}
		})
	}

	t.Run("readiness returns 503 when database ping fails", func(t *testing.T) {
		failingRouter := httpAdapter.NewRouter(httpAdapter.RouterConfig{
			DB: &mockPinger{shouldFail: true},
		})

		req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
		rr := httptest.NewRecorder()

		failingRouter.ServeHTTP(rr, req)

		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503, got %d", rr.Code)
		}

		var resp httpAdapter.HealthStatus
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode json: %v", err)
		}
		if resp.Status != "NOT_READY" {
			t.Errorf("expected NOT_READY, got %q", resp.Status)
		}
	})
}

type mockPinger struct {
	shouldFail bool
}

func (m *mockPinger) Ping(ctx context.Context) error {
	if m.shouldFail {
		return errors.New("connection refused")
	}
	return nil
}
