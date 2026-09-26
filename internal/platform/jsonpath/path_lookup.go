package jsonpath

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

var (
	ErrExponentTooLarge  = errors.New("number exponent exceeds maximum allowed cap of 308")
	ErrMalformedNumber   = errors.New("malformed number format")
	ErrInvalidPathSyntax = errors.New("invalid path syntax")
)

// ValidatePath validates the syntax of a dot-and-bracket notation JSON path:
// - Non-empty, not ".", no leading/trailing or consecutive dots
// - Bracket indices must be closed, non-empty ASCII digits (e.g. "[0]"), without invalid chars
// - Cannot have unclosed brackets or orphan closing brackets
//
//nolint:funlen,gocognit // legacy baseline 2026-09-26: fix in ENG-049
func ValidatePath(path string) error {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" || trimmed == "." {
		return fmt.Errorf("%w: empty path or root '.' is not allowed", ErrInvalidPathSyntax)
	}

	if strings.HasPrefix(trimmed, ".") || strings.HasSuffix(trimmed, ".") {
		return fmt.Errorf("%w: leading or trailing dot in %q", ErrInvalidPathSyntax, path)
	}

	if strings.Contains(trimmed, "..") {
		return fmt.Errorf("%w: consecutive dots in %q", ErrInvalidPathSyntax, path)
	}

	inBracket := false
	bracketDigits := 0

	for i := 0; i < len(trimmed); i++ {
		c := trimmed[i]
		switch c {
		case '[':
			if inBracket {
				return fmt.Errorf("%w: nested bracket at position %d in %q", ErrInvalidPathSyntax, i, path)
			}
			inBracket = true
			bracketDigits = 0
		case ']':
			if !inBracket {
				return fmt.Errorf("%w: unexpected closing bracket at position %d in %q", ErrInvalidPathSyntax, i, path)
			}
			if bracketDigits == 0 {
				return fmt.Errorf("%w: empty bracket index at position %d in %q", ErrInvalidPathSyntax, i, path)
			}
			inBracket = false
			// After closing bracket, next character if any must be '.' or '['
			if i+1 < len(trimmed) && trimmed[i+1] != '.' && trimmed[i+1] != '[' {
				return fmt.Errorf("%w: invalid character %q after bracket at position %d in %q", ErrInvalidPathSyntax, trimmed[i+1], i+1, path)
			}
		default:
			if inBracket {
				if c < '0' || c > '9' {
					return fmt.Errorf("%w: non-numeric array index %q at position %d in %q", ErrInvalidPathSyntax, c, i, path)
				}
				bracketDigits++
			} else if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
				return fmt.Errorf("%w: whitespace in path at position %d in %q", ErrInvalidPathSyntax, i, path)
			}
		}
	}

	if inBracket {
		return fmt.Errorf("%w: unclosed bracket in %q", ErrInvalidPathSyntax, path)
	}

	return nil
}

// LookupPath traverses a parsed JSON structure (map[string]any or []any) using dot notation.
// Supports array indexing via dot notation or bracket notation (e.g. "items.0.sku" or "items[0].sku").
// Returns nil, false if any key or index in the path is not found.
func LookupPath(val any, path string) (any, bool) {
	tokens := TokenizePath(path)
	if len(tokens) == 0 {
		return val, true
	}

	current := val
	for _, token := range tokens {
		if current == nil {
			return nil, false
		}

		switch node := current.(type) {
		case map[string]any:
			next, ok := node[token]
			if !ok {
				return nil, false
			}
			current = next

		case []any:
			idx, err := strconv.Atoi(token)
			if err != nil || idx < 0 || idx >= len(node) {
				return nil, false
			}
			current = node[idx]

		default:
			return nil, false
		}
	}

	return current, true
}

// TokenizePath splits a dot/bracket path into normalized step tokens.
// e.g. "payload.items[0].sku" -> ["payload", "items", "0", "sku"]
func TokenizePath(path string) []string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" || trimmed == "." {
		return nil
	}

	// Normalize bracket notation: "items[0]" -> "items.0"
	var sb strings.Builder
	sb.Grow(len(trimmed))
	for i := 0; i < len(trimmed); i++ {
		c := trimmed[i]
		switch c {
		case '[':
			sb.WriteByte('.')
		case ']':
			// omit closing bracket
		default:
			sb.WriteByte(c)
		}
	}

	rawTokens := strings.Split(sb.String(), ".")
	tokens := make([]string, 0, len(rawTokens))
	for _, t := range rawTokens {
		token := strings.TrimSpace(t)
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	return tokens
}

// CanonicalizeNumber takes a numeric string (from json.Number or representation) and returns
// its canonical form without using float64:
// - Exponents must satisfy |exp| <= 308 (returns ErrExponentTooLarge otherwise)
// - Exponents are expanded so the output has no 'e' or 'E'
// - Trailing fractional zeros are stripped ("65.0" -> "65", "65.50" -> "65.5")
// - "-0" and "-0.0" map to "0"
//
//nolint:funlen,gocognit // legacy baseline 2026-09-26: fix in ENG-044
func CanonicalizeNumber(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", ErrMalformedNumber
	}

	negative := false
	switch s[0] {
	case '-':
		negative = true
		s = s[1:]
	case '+':
		s = s[1:]
	}

	if s == "" {
		return "", ErrMalformedNumber
	}

	var baseStr string
	var exp int64 = 0

	eIdx := strings.IndexAny(s, "eE")
	if eIdx >= 0 {
		baseStr = s[:eIdx]
		expStr := s[eIdx+1:]
		if expStr == "" {
			return "", ErrMalformedNumber
		}
		var err error
		exp, err = strconv.ParseInt(expStr, 10, 64)
		if err != nil {
			return "", fmt.Errorf("%w: invalid exponent: %w", ErrMalformedNumber, err)
		}
		if exp > 308 || exp < -308 {
			return "", ErrExponentTooLarge
		}
	} else {
		baseStr = s
	}

	dotIdx := strings.IndexByte(baseStr, '.')
	var intPart, fracPart string
	if dotIdx >= 0 {
		intPart = baseStr[:dotIdx]
		fracPart = baseStr[dotIdx+1:]
	} else {
		intPart = baseStr
		fracPart = ""
	}

	if intPart == "" && fracPart == "" {
		return "", ErrMalformedNumber
	}

	for i := 0; i < len(intPart); i++ {
		if intPart[i] < '0' || intPart[i] > '9' {
			return "", ErrMalformedNumber
		}
	}
	for i := 0; i < len(fracPart); i++ {
		if fracPart[i] < '0' || fracPart[i] > '9' {
			return "", ErrMalformedNumber
		}
	}

	allDigits := intPart + fracPart
	allDigits = strings.TrimLeft(allDigits, "0")
	if allDigits == "" {
		return "0", nil
	}

	var decPos int
	if intPart == "" || strings.TrimLeft(intPart, "0") == "" {
		leadingZerosInFrac := len(fracPart) - len(strings.TrimLeft(fracPart, "0"))
		decPos = -leadingZerosInFrac
	} else {
		decPos = len(strings.TrimLeft(intPart, "0"))
	}

	newDecPos := int64(decPos) + exp

	var res string
	switch {
	case newDecPos <= 0:
		zeros := int(-newDecPos)
		res = "0." + strings.Repeat("0", zeros) + allDigits
	case newDecPos >= int64(len(allDigits)):
		zeros := int(newDecPos - int64(len(allDigits)))
		res = allDigits + strings.Repeat("0", zeros)
	default:
		res = allDigits[:newDecPos] + "." + allDigits[newDecPos:]
	}

	if strings.Contains(res, ".") {
		res = strings.TrimRight(res, "0")
		res = strings.TrimRight(res, ".")
	}

	if res == "0" || res == "" {
		return "0", nil
	}

	if negative {
		return "-" + res, nil
	}
	return res, nil
}

func IsValidUTF8(b []byte) bool {
	return utf8.Valid(b)
}
