package product

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"golang.org/x/text/unicode/norm"
)

// canonicalPayload defines the fixed-order JSON serialization structure for fingerprinting.
// json.Marshal automatically sorts map keys in alphabetical order and properly escapes strings.
type canonicalPayload struct {
	Attributes    map[string]string `json:"attributes,omitempty"`
	Brand         string            `json:"brand,omitempty"`
	CanonicalName string            `json:"canonical_name"`
	Description   string            `json:"description,omitempty"`
	OriginCountry string            `json:"origin_country,omitempty"`
}

// Fingerprint computes a deterministic, versioned SHA-256 hash
// over the canonical representation of a NormalizedProduct.
func Fingerprint(p NormalizedProduct) string {
	var foldedAttrs map[string]string
	if len(p.Attributes) > 0 {
		foldedAttrs = make(map[string]string, len(p.Attributes))
		for k, v := range p.Attributes {
			foldedKey := strings.ToLower(norm.NFC.String(strings.TrimSpace(k)))
			foldedAttrs[foldedKey] = v
		}
	}

	foldedBrand := strings.ToLower(norm.NFC.String(strings.TrimSpace(p.Brand)))

	payload := canonicalPayload{
		CanonicalName: p.CanonicalName,
		Description:   p.Description,
		Brand:         foldedBrand,
		OriginCountry: p.OriginCountry,
		Attributes:    foldedAttrs,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		// canonicalPayload containing only string fields and map[string]string cannot fail marshaling
		panic(fmt.Sprintf("failed to marshal canonical payload: %v", err))
	}

	sum := sha256.Sum256(data)
	return fmt.Sprintf("v1:%s", hex.EncodeToString(sum[:]))
}
