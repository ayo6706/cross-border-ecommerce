package auth_test

import (
	"net/http"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/auth"
)

func TestAuthStrategies(t *testing.T) {
	t.Run("BearerAuth", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://api.example.com", nil)
		b := auth.NewBearerAuth("secret_token")
		_ = b.ApplyAuth(req)
		if req.Header.Get("Authorization") != "Bearer secret_token" {
			t.Errorf("expected Bearer secret_token, got %q", req.Header.Get("Authorization"))
		}
	})

	t.Run("APIKeyHeaderAuth", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://api.example.com", nil)
		k := auth.NewAPIKeyHeaderAuth("key_123", "X-Custom-Key")
		_ = k.ApplyAuth(req)
		if req.Header.Get("X-Custom-Key") != "key_123" {
			t.Errorf("expected key_123, got %q", req.Header.Get("X-Custom-Key"))
		}
	})

	t.Run("BasicAuth", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://api.example.com", nil)
		b := auth.NewBasicAuth("user", "pass")
		_ = b.ApplyAuth(req)
		u, p, ok := req.BasicAuth()
		if !ok || u != "user" || p != "pass" {
			t.Errorf("basic auth mismatch: %s, %s, %v", u, p, ok)
		}
	})
}
