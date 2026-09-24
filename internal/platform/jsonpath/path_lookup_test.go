package jsonpath_test

import (
	"strings"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/jsonpath"
)

func TestTokenizePath(t *testing.T) {
	tests := []struct {
		path string
		want []string
	}{
		{"", nil},
		{".", nil},
		{"id", []string{"id"}},
		{"variants.0.sku", []string{"variants", "0", "sku"}},
		{"variants[0].sku", []string{"variants", "0", "sku"}},
		{"items[1][2].code", []string{"items", "1", "2", "code"}},
	}

	for _, tt := range tests {
		got := jsonpath.TokenizePath(tt.path)
		if len(got) != len(tt.want) {
			t.Fatalf("TokenizePath(%q) = %v; want %v", tt.path, got, tt.want)
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("TokenizePath(%q)[%d] = %q; want %q", tt.path, i, got[i], tt.want[i])
			}
		}
	}
}

func TestValidatePath(t *testing.T) {
	tests := []struct {
		path    string
		wantErr bool
	}{
		{"title", false},
		{"details.color", false},
		{"variants[0].sku", false},
		{"items[1][2].code", false},
		{"", true},
		{".", true},
		{".title", true},
		{"title.", true},
		{"details..color", true},
		{"items[0", true},
		{"items[x]", true},
		{"items[]", true},
		{"a]b", true},
		{"items[0]abc", true},
		{"items [0]", true},
	}

	for _, tt := range tests {
		err := jsonpath.ValidatePath(tt.path)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidatePath(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
		}
	}
}

func TestCanonicalizeNumber(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"0", "0", false},
		{"-0", "0", false},
		{"-0.0", "0", false},
		{"0.00", "0", false},
		{"65", "65", false},
		{"65.0", "65", false},
		{"65.50", "65.5", false},
		{"65.5000", "65.5", false},
		{"0.5", "0.5", false},
		{"0.05", "0.05", false},
		{"-65.0", "-65", false},
		{"-65.50", "-65.5", false},
		{"1.23e2", "123", false},
		{"1.23e-2", "0.0123", false},
		{"1e3", "1000", false},
		{"1e-3", "0.001", false},
		{"-1.23e2", "-123", false},
		{"1e308", "1" + strings.Repeat("0", 308), false},
		{"1e309", "", true},  // Exponent capped at 308
		{"1e-309", "", true}, // Exponent capped at -308
		{"not-a-number", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		got, err := jsonpath.CanonicalizeNumber(tt.input)
		if (err != nil) != tt.wantErr {
			t.Fatalf("CanonicalizeNumber(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("CanonicalizeNumber(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
