package extraction

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/jsonpath"
)

// LookupPath traverses a parsed JSON structure (map[string]any or []any) using dot notation.
// Supports array indexing via dot notation or bracket notation (e.g. "items.0.sku" or "items[0].sku").
// Returns nil if any key or index in the path is not found.
func LookupPath(val any, path string) (any, bool) {
	return jsonpath.LookupPath(val, path)
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
