package extraction

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// LookupPath traverses a parsed JSON structure (map[string]any or []any) using dot notation.
// Supports array indexing via dot notation or bracket notation (e.g. "items.0.sku" or "items[0].sku").
// Returns nil if any key or index in the path is not found.
func LookupPath(val any, path string) (any, bool) {
	tokens := tokenizePath(path)
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

// LookupString traverses the path and returns the resolved value as a string.
// Automatically formats numbers and booleans as strings.
func LookupString(val any, path string) (string, bool) {
	target, found := LookupPath(val, path)
	if !found || target == nil {
		return "", false
	}

	switch v := target.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		return trimmed, trimmed != ""
	case json.Number:
		str := strings.TrimSpace(v.String())
		return str, str != ""
	case float64:
		// Format integer float without scientific notation or trailing zeros
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10), true
		}
		return fmt.Sprintf("%v", v), true
	case int:
		return strconv.Itoa(v), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case bool:
		return strconv.FormatBool(v), true
	default:
		return "", false
	}
}

// tokenizePath splits a dot/bracket path into normalized step tokens.
// e.g. "payload.items[0].sku" -> ["payload", "items", "0", "sku"]
func tokenizePath(path string) []string {
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
