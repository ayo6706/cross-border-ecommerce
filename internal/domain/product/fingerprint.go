package product

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
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

func canonicalize(p NormalizedProduct) canonicalPayload {
	var foldedAttrs map[string]string
	if len(p.Attributes) > 0 {
		foldedAttrs = make(map[string]string, len(p.Attributes))
		for k, v := range p.Attributes {
			foldedKey := strings.ToLower(norm.NFC.String(strings.TrimSpace(k)))
			foldedAttrs[foldedKey] = v
		}
	}

	foldedBrand := strings.ToLower(norm.NFC.String(strings.TrimSpace(p.Brand)))

	return canonicalPayload{
		CanonicalName: p.CanonicalName,
		Description:   p.Description,
		Brand:         foldedBrand,
		OriginCountry: p.OriginCountry,
		Attributes:    foldedAttrs,
	}
}

// FingerprintV1Prefix is the version prefix for v1 canonical fingerprints.
const FingerprintV1Prefix = "v1:"

// Fingerprint computes a deterministic, versioned SHA-256 hash
// over the canonical representation of a NormalizedProduct.
func Fingerprint(p NormalizedProduct) string {
	payload := canonicalize(p)

	data, err := json.Marshal(payload)
	if err != nil {
		// canonicalPayload containing only string fields and map[string]string cannot fail marshaling
		panic(fmt.Sprintf("failed to marshal canonical payload: %v", err))
	}

	sum := sha256.Sum256(data)
	return fmt.Sprintf("%s%s", FingerprintV1Prefix, hex.EncodeToString(sum[:]))
}

// FingerprintFromVersion computes the v1 fingerprint for an existing ProductVersion.
func FingerprintFromVersion(pv ProductVersion) string {
	np := NormalizedProduct{
		CanonicalName: pv.CanonicalName,
		Description:   pv.Description,
		Brand:         pv.Brand,
		OriginCountry: pv.OriginCountry,
		Attributes:    pv.Attributes,
	}
	return Fingerprint(np)
}

// DetectFieldChanges compares the canonical representation of the stored current version against
// the incoming normalized product and returns a sorted, deduplicated slice of modified field names.
func DetectFieldChanges(current ProductVersion, next NormalizedProduct) []string {
	currentNorm := NormalizedProduct{
		CanonicalName: current.CanonicalName,
		Description:   current.Description,
		Brand:         current.Brand,
		OriginCountry: current.OriginCountry,
		Attributes:    current.Attributes,
	}

	cPayload := canonicalize(currentNorm)
	nPayload := canonicalize(next)

	var diffs []string

	if cPayload.CanonicalName != nPayload.CanonicalName {
		diffs = append(diffs, "canonical_name")
	}
	if cPayload.Description != nPayload.Description {
		diffs = append(diffs, "description")
	}
	if cPayload.Brand != nPayload.Brand {
		diffs = append(diffs, "brand")
	}
	if cPayload.OriginCountry != nPayload.OriginCountry {
		diffs = append(diffs, "origin_country")
	}

	// Check attributes diff
	attrsChanged := false
	if len(cPayload.Attributes) != len(nPayload.Attributes) {
		attrsChanged = true
	} else {
		for k, v := range cPayload.Attributes {
			if nextVal, ok := nPayload.Attributes[k]; !ok || nextVal != v {
				attrsChanged = true
				break
			}
		}
	}

	if attrsChanged {
		diffs = append(diffs, "attributes")
	}

	sort.Strings(diffs)
	return diffs
}
