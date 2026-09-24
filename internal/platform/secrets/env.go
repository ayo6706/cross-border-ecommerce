package secrets

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

var _ source.SecretResolver = (*EnvSecretResolver)(nil)

const envScheme = "env"

// EnvSecretResolver resolves "env:<NAME>" references from environment variables.
type EnvSecretResolver struct{}

func NewEnvSecretResolver() *EnvSecretResolver {
	return &EnvSecretResolver{}
}

func (r *EnvSecretResolver) ResolveSecret(_ context.Context, ref string) (string, error) {
	if err := source.ValidateSecretRef(ref); err != nil {
		return "", err
	}

	scheme, key, _ := strings.Cut(strings.TrimSpace(ref), ":")
	if scheme != envScheme {
		return "", fmt.Errorf("unsupported secret reference scheme %q (supported: %s)", scheme, envScheme)
	}

	val, ok := os.LookupEnv(strings.TrimSpace(key))
	if !ok || val == "" {
		return "", fmt.Errorf("secret reference %q is not set in the environment", ref)
	}
	return val, nil
}
