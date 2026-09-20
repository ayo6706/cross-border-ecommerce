package internal_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestDomainLayerPurity enforces the hexagonal architecture rule that internal/domain
// packages MUST remain pure Go without importing infrastructure, adapters, or external drivers.
func TestDomainLayerPurity(t *testing.T) {
	domainRoot := filepath.Join(".", "domain")

	fset := token.NewFileSet()
	err := filepath.WalkDir(domainRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		node, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("failed to parse file %s: %v", path, err)
		}

		for _, imp := range node.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)

			// Forbidden imports in pure domain:
			forbiddenPrefixes := []string{
				"database/sql",
				"net/http",
				"github.com/jackc/pgx",
				"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure",
				"github.com/ayo6706/cross-border-ecommerce/internal/adapters",
				"github.com/ayo6706/cross-border-ecommerce/internal/application",
			}

			for _, forbidden := range forbiddenPrefixes {
				if strings.HasPrefix(importPath, forbidden) {
					t.Errorf("Architecture violation in %s: domain must not import '%s'", path, importPath)
				}
			}
		}

		return nil
	})

	if err != nil {
		t.Fatalf("error walking domain directory: %v", err)
	}
}
