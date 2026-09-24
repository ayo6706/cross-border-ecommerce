package internal_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// LayerRule defines layer boundary assertions.
type LayerRule struct {
	Name              string
	Directory         string
	ForbiddenPrefixes []string
}

func TestHexagonalArchitectureLayers(t *testing.T) {
	rules := []LayerRule{
		{
			Name:      "Domain Layer Purity",
			Directory: filepath.Join(".", "domain"),
			ForbiddenPrefixes: []string{
				"database/sql",
				"net/http",
				"github.com/jackc/pgx",
				"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure",
				"github.com/ayo6706/cross-border-ecommerce/internal/adapters",
				"github.com/ayo6706/cross-border-ecommerce/internal/application",
			},
		},
		{
			Name:      "Application Layer Purity",
			Directory: filepath.Join(".", "application"),
			ForbiddenPrefixes: []string{
				"database/sql",
				"github.com/jackc/pgx",
				"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure",
				"github.com/ayo6706/cross-border-ecommerce/internal/adapters",
			},
		},
		{
			Name:      "Adapters Layer Purity",
			Directory: filepath.Join(".", "adapters"),
			ForbiddenPrefixes: []string{
				"github.com/jackc/pgx",
				"github.com/ayo6706/cross-border-ecommerce/internal/infrastructure",
			},
		},
	}

	fset := token.NewFileSet()

	for _, rule := range rules {
		t.Run(rule.Name, func(t *testing.T) {
			err := filepath.WalkDir(rule.Directory, func(path string, d fs.DirEntry, err error) error {
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
					for _, forbidden := range rule.ForbiddenPrefixes {
						if strings.HasPrefix(importPath, forbidden) {
							t.Errorf("Architecture boundary violation in %s: %s must not import '%s'", path, rule.Name, importPath)
						}
					}
				}

				return nil
			})

			if err != nil {
				t.Fatalf("error walking %s directory: %v", rule.Directory, err)
			}
		})
	}
}
