package identity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/extraction"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

// IdentityStrategy resolves an external product identifier from a raw record JSON payload.
type IdentityStrategy interface {
	Resolve(rawRecord []byte) (string, error)
}

// PathIdentityStrategy extracts identity from a specific JSON path (e.g. "sku", "id", "item.merchant_code").
type PathIdentityStrategy struct {
	Path string
}

func NewPathIdentityStrategy(path string) *PathIdentityStrategy {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		trimmed = "id"
	}
	return &PathIdentityStrategy{Path: trimmed}
}

func (s *PathIdentityStrategy) Resolve(rawRecord []byte) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(rawRecord))
	dec.UseNumber()

	var root any
	if err := dec.Decode(&root); err != nil {
		return "", fmt.Errorf("%w: cannot parse record json: %v", ingestion.ErrMalformedRecord, err)
	}

	val, found := extraction.LookupString(root, s.Path)
	if !found || strings.TrimSpace(val) == "" {
		return "", fmt.Errorf("%w: path %q missing or empty in record", ingestion.ErrIdentityNotFound, s.Path)
	}

	return strings.TrimSpace(val), nil
}

// CompositeIdentityStrategy resolves identity by combining multiple field values with a delimiter.
// e.g. fields=["warehouse_id", "item.part_number"], separator=":" -> "UK01:LG-65-C4"
type CompositeIdentityStrategy struct {
	Fields    []string
	Separator string
}

func NewCompositeIdentityStrategy(fields []string, separator string) (*CompositeIdentityStrategy, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("composite identity requires at least one field")
	}
	sep := separator
	if sep == "" {
		sep = ":"
	}
	cleaned := make([]string, 0, len(fields))
	for _, f := range fields {
		t := strings.TrimSpace(f)
		if t != "" {
			cleaned = append(cleaned, t)
		}
	}
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("composite identity requires non-empty field names")
	}
	return &CompositeIdentityStrategy{
		Fields:    cleaned,
		Separator: sep,
	}, nil
}

func (s *CompositeIdentityStrategy) Resolve(rawRecord []byte) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(rawRecord))
	dec.UseNumber()

	var root any
	if err := dec.Decode(&root); err != nil {
		return "", fmt.Errorf("%w: cannot parse record json: %v", ingestion.ErrMalformedRecord, err)
	}

	parts := make([]string, 0, len(s.Fields))
	for _, field := range s.Fields {
		val, found := extraction.LookupString(root, field)
		if !found || strings.TrimSpace(val) == "" {
			return "", fmt.Errorf("%w: composite field %q missing or empty", ingestion.ErrIdentityNotFound, field)
		}
		parts = append(parts, strings.TrimSpace(val))
	}

	return strings.Join(parts, s.Separator), nil
}

// CustomIdentityStrategy wraps a functional identity resolver.
type CustomIdentityStrategy struct {
	resolver func(rawRecord []byte) (string, error)
}

func NewCustomIdentityStrategy(fn func(rawRecord []byte) (string, error)) *CustomIdentityStrategy {
	return &CustomIdentityStrategy{resolver: fn}
}

func (s *CustomIdentityStrategy) Resolve(rawRecord []byte) (string, error) {
	if s.resolver == nil {
		return "", fmt.Errorf("%w: nil resolver function", ingestion.ErrIdentityNotFound)
	}
	return s.resolver(rawRecord)
}
