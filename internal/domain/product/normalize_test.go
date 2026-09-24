package product_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

func TestNormalize_MissingRequiredField(t *testing.T) {
	mapping := product.FieldMapping{
		NamePath: "title",
	}

	tests := []struct {
		name    string
		payload string
	}{
		{"PathMissing", `{"description": "A widget"}`},
		{"PathNull", `{"title": null, "description": "A widget"}`},
		{"PathEmptyString", `{"title": "", "description": "A widget"}`},
		{"PathWhitespaceOnly", `{"title": "   \n\t  ", "description": "A widget"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := product.Normalize([]byte(tt.payload), mapping)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tt.name)
			}
			if !errors.Is(err, product.ErrMissingCanonicalField) {
				t.Fatalf("expected ErrMissingCanonicalField, got: %v", err)
			}
		})
	}

	t.Run("EmptyMappingReturnsErrMissingFieldMapping", func(t *testing.T) {
		emptyMapping := product.FieldMapping{}
		_, err := product.Normalize([]byte(`{"title": "Valid"}`), emptyMapping)
		if !errors.Is(err, product.ErrMissingFieldMapping) {
			t.Fatalf("expected ErrMissingFieldMapping, got: %v", err)
		}
	})

	t.Run("InvalidPathSyntaxReturnsErrInvalidFieldMapping", func(t *testing.T) {
		invalidMapping := product.FieldMapping{
			NamePath: "title[0",
		}
		_, err := product.Normalize([]byte(`{"title": "Valid"}`), invalidMapping)
		if !errors.Is(err, product.ErrInvalidFieldMapping) {
			t.Fatalf("expected ErrInvalidFieldMapping, got: %v", err)
		}
	})
}

func TestNormalize_Malformed(t *testing.T) {
	mapping := product.FieldMapping{
		NamePath: "title",
	}

	tests := []struct {
		name    string
		payload []byte
	}{
		{"InvalidUTF8", []byte("{\"title\": \"Widget \xff\"}")},
		{"TrailingGarbage", []byte(`{"title": "Widget"} trailing_junk`)},
		{"MultipleObjects", []byte(`{"title": "Widget"}{"title": "Other"}`)},
		{"ArrayRoot", []byte(`[{"title": "Widget"}]`)},
		{"TruncatedJSON", []byte(`{"title": "Wid`)},
		{"EmptyBytes", []byte(``)},
		{"BooleanName", []byte(`{"title": true}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := product.Normalize(tt.payload, mapping)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tt.name)
			}
			if !errors.Is(err, product.ErrMalformedRecord) {
				t.Fatalf("expected ErrMalformedRecord, got: %v", err)
			}
		})
	}
}

func TestNormalize_NumberCanonicalForm(t *testing.T) {
	mapping := product.FieldMapping{
		NamePath: "title",
		AttributePaths: map[string]string{
			"diagonal":    "specs.screen_size",
			"weight":      "specs.weight_kg",
			"sku_code":    "sku",
			"rating":      "rating",
			"coefficient": "coefficient",
		},
	}

	payload := `{
		"title": "4K Smart TV",
		"sku": "007",
		"specs": {
			"screen_size": 65.0,
			"weight_kg": 24.50
		},
		"rating": -0,
		"coefficient": 1.23e2
	}`

	p, err := product.Normalize([]byte(payload), mapping)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Actual JSON numbers have trailing fractional zeros stripped and exponents expanded
	if got := p.Attributes["diagonal"]; got != "65" {
		t.Errorf("diagonal = %q, want %q", got, "65")
	}
	if got := p.Attributes["weight"]; got != "24.5" {
		t.Errorf("weight = %q, want %q", got, "24.5")
	}
	if got := p.Attributes["rating"]; got != "0" {
		t.Errorf("rating = %q, want %q", got, "0")
	}
	if got := p.Attributes["coefficient"]; got != "123" {
		t.Errorf("coefficient = %q, want %q", got, "123")
	}

	// String attributes remain verbatim (e.g. leading zeros preserved)
	if got := p.Attributes["sku_code"]; got != "007" {
		t.Errorf("sku_code = %q, want %q", got, "007")
	}
}

func TestNormalize_Idempotence(t *testing.T) {
	mapping := product.FieldMapping{
		NamePath:          "title",
		DescriptionPath:   "description",
		BrandPath:         "brand",
		OriginCountryPath: "country",
		AttributePaths: map[string]string{
			"color": "color",
			"size":  "size",
		},
	}

	raw := `{
		"title": "  Ergonomic \u200B Chair  \n",
		"description": "Office chair with lumbar support",
		"brand": "Herman Miller",
		"country": "us",
		"color": "black",
		"size": "XL"
	}`

	p1, err := product.Normalize([]byte(raw), mapping)
	if err != nil {
		t.Fatalf("first normalization failed: %v", err)
	}

	// OriginCountry is normalized to uppercase ISO-2
	if p1.OriginCountry != "US" {
		t.Fatalf("OriginCountry = %q, want US", p1.OriginCountry)
	}

	// Re-serialize normalized product to JSON
	serialized, err := json.Marshal(map[string]any{
		"title":       p1.CanonicalName,
		"description": p1.Description,
		"brand":       p1.Brand,
		"country":     p1.OriginCountry,
		"color":       p1.Attributes["color"],
		"size":        p1.Attributes["size"],
	})
	if err != nil {
		t.Fatalf("re-serializing failed: %v", err)
	}

	p2, err := product.Normalize(serialized, mapping)
	if err != nil {
		t.Fatalf("second normalization failed: %v", err)
	}

	if p1.CanonicalName != p2.CanonicalName ||
		p1.Description != p2.Description ||
		p1.Brand != p2.Brand ||
		p1.OriginCountry != p2.OriginCountry {
		t.Fatalf("Normalize is not idempotent: %+v != %+v", p1, p2)
	}

	fp1 := product.Fingerprint(p1)
	fp2 := product.Fingerprint(p2)
	if fp1 != fp2 {
		t.Fatalf("fingerprint drift across idempotent normalization: %s != %s", fp1, fp2)
	}
}

func TestNormalize_WhitespaceAndControlCharacters(t *testing.T) {
	mapping := product.FieldMapping{
		NamePath:        "title",
		DescriptionPath: "desc",
	}

	tests := []struct {
		name     string
		payload  string
		wantName string
		wantDesc string
	}{
		{
			name:     "VerticalTabAndFormFeed",
			payload:  `{"title": "a\u000bb", "desc": "c\u000cd"}`,
			wantName: "a b",
			wantDesc: "c d",
		},
		{
			name:     "TrailingFormFeed",
			payload:  `{"title": "a\u000c", "desc": "\u000cb"}`,
			wantName: "a",
			wantDesc: "b",
		},
		{
			name:     "MultipleConsecutiveWhitespaceTypes",
			payload:  `{"title": "a \t\n\r\u000b\u000c  b", "desc": "d"}`,
			wantName: "a b",
			wantDesc: "d",
		},
		{
			name:     "ZeroWidthRunesStripped",
			payload:  `{"title": "a\u200Bb\uFEFFc\u200Cd\u200De", "desc": "clean"}`,
			wantName: "abcde",
			wantDesc: "clean",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := product.Normalize([]byte(tt.payload), mapping)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if p.CanonicalName != tt.wantName {
				t.Errorf("CanonicalName = %q, want %q", p.CanonicalName, tt.wantName)
			}
			if p.Description != tt.wantDesc {
				t.Errorf("Description = %q, want %q", p.Description, tt.wantDesc)
			}
		})
	}
}
