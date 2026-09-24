package secrets_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/secrets"
)

func TestEnvSecretResolver(t *testing.T) {
	t.Setenv("TEST_SUPPLIER_KEY", "secret-token-123")

	resolver := secrets.NewEnvSecretResolver()
	ctx := context.Background()

	t.Run("ResolvesEnvReference", func(t *testing.T) {
		val, err := resolver.ResolveSecret(ctx, "env:TEST_SUPPLIER_KEY")
		if err != nil || val != "secret-token-123" {
			t.Fatalf("expected resolved env value, got %q, err: %v", val, err)
		}
	})

	t.Run("RejectsLiteralValue", func(t *testing.T) {
		_, err := resolver.ResolveSecret(ctx, "my-literal-api-key")
		if !errors.Is(err, source.ErrInvalidSecretRef) {
			t.Fatalf("expected ErrInvalidSecretRef for literal secret, got %v", err)
		}
	})

	t.Run("RejectsEmptyReference", func(t *testing.T) {
		if _, err := resolver.ResolveSecret(ctx, ""); !errors.Is(err, source.ErrInvalidSecretRef) {
			t.Fatalf("expected ErrInvalidSecretRef for empty reference, got %v", err)
		}
	})

	t.Run("RejectsUnsupportedScheme", func(t *testing.T) {
		if _, err := resolver.ResolveSecret(ctx, "vault:secret/supplier"); err == nil {
			t.Fatal("expected error for unsupported scheme, got nil")
		}
	})

	t.Run("MissingVariableReturnsError", func(t *testing.T) {
		if _, err := resolver.ResolveSecret(ctx, "env:NON_EXISTENT_SECRET_VARIABLE_KEY"); err == nil {
			t.Fatal("expected error for missing env variable, got nil")
		}
	})
}
