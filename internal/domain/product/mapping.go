package product

import (
	"fmt"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/jsonpath"
	"golang.org/x/text/unicode/norm"
)

// FieldMapping specifies the JSON path mappings from a source's raw record payload
// to the product's canonical fields.
type FieldMapping struct {
	NamePath          string            `json:"name_path"`
	DescriptionPath   string            `json:"description_path,omitempty"`
	BrandPath         string            `json:"brand_path,omitempty"`
	OriginCountryPath string            `json:"origin_country_path,omitempty"`
	AttributePaths    map[string]string `json:"attribute_paths,omitempty"`
}

// Validate checks that the mapping satisfies domain invariants:
// - NamePath is required and non-empty.
// - All configured paths have valid JSON dot/bracket syntax.
// - Attribute key names do not collide after case-folding (e.g. "Color" and "color").
func (m *FieldMapping) Validate() error {
	if m == nil {
		return ErrInvalidFieldMapping
	}
	if strings.TrimSpace(m.NamePath) == "" {
		return fmt.Errorf("%w: missing required 'name_path'", ErrInvalidFieldMapping)
	}

	if err := jsonpath.ValidatePath(m.NamePath); err != nil {
		return fmt.Errorf("%w: 'name_path': %w", ErrInvalidFieldMapping, err)
	}

	if m.DescriptionPath != "" {
		if err := jsonpath.ValidatePath(m.DescriptionPath); err != nil {
			return fmt.Errorf("%w: 'description_path': %w", ErrInvalidFieldMapping, err)
		}
	}

	if m.BrandPath != "" {
		if err := jsonpath.ValidatePath(m.BrandPath); err != nil {
			return fmt.Errorf("%w: 'brand_path': %w", ErrInvalidFieldMapping, err)
		}
	}

	if m.OriginCountryPath != "" {
		if err := jsonpath.ValidatePath(m.OriginCountryPath); err != nil {
			return fmt.Errorf("%w: 'origin_country_path': %w", ErrInvalidFieldMapping, err)
		}
	}

	seen := make(map[string]string, len(m.AttributePaths))
	for k, path := range m.AttributePaths {
		trimmed := strings.TrimSpace(k)
		if trimmed == "" {
			return fmt.Errorf("%w: empty attribute key", ErrInvalidFieldMapping)
		}
		if err := jsonpath.ValidatePath(path); err != nil {
			return fmt.Errorf("%w: attribute %q path %q: %w", ErrInvalidFieldMapping, k, path, err)
		}
		folded := strings.ToLower(norm.NFC.String(trimmed))
		if original, exists := seen[folded]; exists {
			return fmt.Errorf("%w: '%s' and '%s' collide after case-folding", ErrDuplicateAttributeKey, original, k)
		}
		seen[folded] = k
	}

	return nil
}

// NormalizedProduct represents the extracted and normalized canonical representation of a product.
// Casing for display fields (CanonicalName, Description, Brand) is preserved.
type NormalizedProduct struct {
	CanonicalName string
	Description   string
	Brand         string
	OriginCountry string
	Attributes    map[string]string
}
