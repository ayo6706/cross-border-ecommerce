package product

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/jsonpath"
	"golang.org/x/text/unicode/norm"
)

// Normalize converts a raw record's JSON byte payload into a NormalizedProduct
// according to the provided FieldMapping.
func Normalize(payload []byte, m FieldMapping) (NormalizedProduct, error) {
	if len(payload) == 0 {
		return NormalizedProduct{}, ErrMalformedRecord
	}

	if !jsonpath.IsValidUTF8(payload) {
		return NormalizedProduct{}, ErrMalformedRecord
	}

	if strings.TrimSpace(m.NamePath) == "" {
		return NormalizedProduct{}, ErrMissingFieldMapping
	}

	if err := m.Validate(); err != nil {
		return NormalizedProduct{}, err
	}

	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()

	var root any
	if err := dec.Decode(&root); err != nil {
		return NormalizedProduct{}, fmt.Errorf("%w: %w", ErrMalformedRecord, err)
	}

	rootMap, ok := root.(map[string]any)
	if !ok {
		return NormalizedProduct{}, fmt.Errorf("%w: payload root must be a JSON object", ErrMalformedRecord)
	}

	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return NormalizedProduct{}, fmt.Errorf("%w: unexpected trailing data", ErrMalformedRecord)
	}

	nameVal, found := jsonpath.LookupPath(rootMap, m.NamePath)
	if !found || nameVal == nil {
		return NormalizedProduct{}, ErrMissingCanonicalField
	}
	if _, isBool := nameVal.(bool); isBool {
		return NormalizedProduct{}, fmt.Errorf("%w: canonical name cannot be a boolean", ErrMalformedRecord)
	}
	canonicalName, err := extractScalarString(nameVal)
	if err != nil {
		return NormalizedProduct{}, fmt.Errorf("%w: name path: %w", ErrMalformedRecord, err)
	}
	canonicalName = collapseWhitespace(canonicalName)
	if canonicalName == "" {
		return NormalizedProduct{}, ErrMissingCanonicalField
	}

	var description string
	if m.DescriptionPath != "" {
		if val, found := jsonpath.LookupPath(rootMap, m.DescriptionPath); found && val != nil {
			str, err := extractScalarString(val)
			if err != nil {
				return NormalizedProduct{}, fmt.Errorf("%w: description path: %w", ErrMalformedRecord, err)
			}
			description = collapseWhitespace(str)
		}
	}

	var brand string
	if m.BrandPath != "" {
		if val, found := jsonpath.LookupPath(rootMap, m.BrandPath); found && val != nil {
			str, err := extractScalarString(val)
			if err != nil {
				return NormalizedProduct{}, fmt.Errorf("%w: brand path: %w", ErrMalformedRecord, err)
			}
			brand = collapseWhitespace(str)
		}
	}

	var originCountry string
	if m.OriginCountryPath != "" {
		if val, found := jsonpath.LookupPath(rootMap, m.OriginCountryPath); found && val != nil {
			str, err := extractScalarString(val)
			if err != nil {
				return NormalizedProduct{}, fmt.Errorf("%w: origin country path: %w", ErrMalformedRecord, err)
			}
			cleaned := collapseWhitespace(str)
			if cleaned != "" {
				if len(cleaned) != 2 || !isAlpha(cleaned) {
					return NormalizedProduct{}, fmt.Errorf("%w: got %q", ErrInvalidOriginCountry, cleaned)
				}
				originCountry = strings.ToUpper(cleaned)
			}
		}
	}

	var attributes map[string]string
	if len(m.AttributePaths) > 0 {
		attributes = make(map[string]string, len(m.AttributePaths))
		for attrKey, path := range m.AttributePaths {
			val, found := jsonpath.LookupPath(rootMap, path)
			if !found || val == nil {
				continue
			}
			str, err := extractScalarString(val)
			if err != nil {
				return NormalizedProduct{}, fmt.Errorf("%w: attribute %q: %w", ErrMalformedRecord, attrKey, err)
			}
			cleaned := collapseWhitespace(str)
			if cleaned != "" {
				attributes[attrKey] = cleaned
			}
		}
		if len(attributes) == 0 {
			attributes = nil
		}
	}

	return NormalizedProduct{
		CanonicalName: canonicalName,
		Description:   description,
		Brand:         brand,
		OriginCountry: originCountry,
		Attributes:    attributes,
	}, nil
}

func extractScalarString(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case json.Number:
		canon, err := jsonpath.CanonicalizeNumber(string(val))
		if err != nil {
			return "", err
		}
		return canon, nil
	case bool:
		return strconv.FormatBool(val), nil
	default:
		return "", fmt.Errorf("expected scalar value, got %T", v)
	}
}

// collapseWhitespace applies Unicode NFC normalization, strips zero-width runes
// (U+200B, U+FEFF, U+200C, U+200D), collapses internal whitespace runs to a single space,
// and trims leading/trailing spaces.
func collapseWhitespace(s string) string {
	if s == "" {
		return ""
	}

	nfc := s
	if !norm.NFC.IsNormalString(s) {
		nfc = norm.NFC.String(s)
	}

	// Check if whitespace normalization or zero-width stripping is needed
	needsCollapse := false
	if nfc[0] <= ' ' || nfc[len(nfc)-1] <= ' ' {
		needsCollapse = true
	} else {
		for i := 0; i < len(nfc); i++ {
			b := nfc[i]
			if b < 0x20 || b >= 0x80 {
				needsCollapse = true
				break
			}
			if b == ' ' && i+1 < len(nfc) && nfc[i+1] <= ' ' {
				needsCollapse = true
				break
			}
		}
	}

	if !needsCollapse {
		return nfc
	}

	var sb strings.Builder
	sb.Grow(len(nfc))
	inWhitespace := false

	for _, r := range nfc {
		// Strip zero-width runes
		if r == '\u200B' || r == '\uFEFF' || r == '\u200C' || r == '\u200D' {
			continue
		}

		if unicode.IsSpace(r) {
			if !inWhitespace {
				sb.WriteByte(' ')
				inWhitespace = true
			}
		} else {
			sb.WriteRune(r)
			inWhitespace = false
		}
	}

	return strings.TrimSpace(sb.String())
}

func isAlpha(s string) bool {
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}
