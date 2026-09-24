package source

import (
	"context"
	"errors"
	"strings"
)

// ErrInvalidSecretRef is returned for credentials that are not "<scheme>:<key>" references.
var ErrInvalidSecretRef = errors.New("secret must be a reference such as env:SUPPLIER_TOKEN, not a literal value")

// SecretResolver resolves secret references (e.g. "env:SUPPLIER_TOKEN") to secret values.
// Source configuration stores only references, never the secrets themselves.
type SecretResolver interface {
	ResolveSecret(ctx context.Context, ref string) (string, error)
}

// ValidateSecretRef checks that ref has the form "<scheme>:<key>", where scheme
// is lowercase letters. It does not check that the referenced secret exists.
func ValidateSecretRef(ref string) error {
	scheme, key, found := strings.Cut(strings.TrimSpace(ref), ":")
	if !found || scheme == "" || strings.TrimSpace(key) == "" {
		return ErrInvalidSecretRef
	}
	for _, r := range scheme {
		if r < 'a' || r > 'z' {
			return ErrInvalidSecretRef
		}
	}
	return nil
}
