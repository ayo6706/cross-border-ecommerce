package product_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

func TestFingerprint_SemanticEquivalence(t *testing.T) {
	// Payloads differing only in key order, whitespace, config attribute-key case, Unicode composition (NFC vs NFD)
	// must produce identical fingerprints.

	// NFD: e + combining acute accent (\u0301)
	caféNFD := "Cafe\u0301 Deluxe"
	// NFC: composed é (\u00e9)
	caféNFC := "Caf\u00e9 Deluxe"

	mapping1 := product.FieldMapping{
		NamePath:          "title",
		DescriptionPath:   "desc",
		BrandPath:         "brand",
		OriginCountryPath: "country",
		AttributePaths: map[string]string{
			"color": "details.color",
			"size":  "details.size",
		},
	}

	mappingKeyCase := product.FieldMapping{
		NamePath:          "title",
		DescriptionPath:   "desc",
		BrandPath:         "brand",
		OriginCountryPath: "country",
		AttributePaths: map[string]string{
			"Color": "details.color",
			"SIZE":  "details.size",
		},
	}

	payload1 := `{
		"title": "` + caféNFC + `",
		"desc": "Espresso machine with 15 bar pump",
		"brand": "De'Longhi",
		"country": "IT",
		"details": {
			"color": "Silver",
			"size": "Medium"
		}
	}`

	payload2 := `{
		"details": {
			"size": "Medium",
			"color": "Silver"
		},
		"country": "it",
		"brand": "de'longhi",
		"desc": "Espresso  machine \u200B with 15 \n bar pump",
		"title": "  ` + caféNFD + `  "
	}`

	payload3 := `{
		"brand": "DE'LONGHI",
		"country": "IT",
		"title": "\u000c\u000b  ` + caféNFC + `  \u000b\u000c",
		"desc": "Espresso\u000bmachine\u000cwith\t15\r\nbar\u000bpump",
		"details": {
			"color": "\u000bSilver\u000c",
			"size": "Medium"
		}
	}`

	p1, err := product.Normalize([]byte(payload1), mapping1)
	if err != nil {
		t.Fatalf("normalize payload1: %v", err)
	}

	p2, err := product.Normalize([]byte(payload2), mappingKeyCase)
	if err != nil {
		t.Fatalf("normalize payload2: %v", err)
	}

	p3, err := product.Normalize([]byte(payload3), mapping1)
	if err != nil {
		t.Fatalf("normalize payload3: %v", err)
	}

	fp1 := product.Fingerprint(p1)
	fp2 := product.Fingerprint(p2)
	fp3 := product.Fingerprint(p3)

	if !strings.HasPrefix(fp1, "v1:") {
		t.Fatalf("fingerprint %q does not have required prefix 'v1:'", fp1)
	}
	if fp1 != fp2 {
		t.Fatalf("expected identical fingerprints for payload1 and payload2, got:\nfp1: %s\nfp2: %s", fp1, fp2)
	}
	if fp1 != fp3 {
		t.Fatalf("expected identical fingerprints for payload1 and payload3 (\\v/\\f handling), got:\nfp1: %s\nfp3: %s", fp1, fp3)
	}
}

func TestFingerprint_EmptyNullAbsentAttributesEquivalence(t *testing.T) {
	mapping := product.FieldMapping{
		NamePath: "title",
		AttributePaths: map[string]string{
			"color": "color",
		},
	}

	payloadAbsent := `{"title": "Shirt"}`
	payloadNull := `{"title": "Shirt", "color": null}`
	payloadEmpty := `{"title": "Shirt", "color": ""}`
	payloadWhitespace := `{"title": "Shirt", "color": "   \t\n  "}`

	pAbsent, err := product.Normalize([]byte(payloadAbsent), mapping)
	if err != nil {
		t.Fatalf("normalize absent: %v", err)
	}
	pNull, err := product.Normalize([]byte(payloadNull), mapping)
	if err != nil {
		t.Fatalf("normalize null: %v", err)
	}
	pEmpty, err := product.Normalize([]byte(payloadEmpty), mapping)
	if err != nil {
		t.Fatalf("normalize empty: %v", err)
	}
	pWhitespace, err := product.Normalize([]byte(payloadWhitespace), mapping)
	if err != nil {
		t.Fatalf("normalize whitespace: %v", err)
	}

	fpAbsent := product.Fingerprint(pAbsent)
	fpNull := product.Fingerprint(pNull)
	fpEmpty := product.Fingerprint(pEmpty)
	fpWhitespace := product.Fingerprint(pWhitespace)

	if fpAbsent != fpNull || fpAbsent != fpEmpty || fpAbsent != fpWhitespace {
		t.Fatalf("empty/null/absent attributes produced different fingerprints:\nabsent:     %s\nnull:       %s\nempty:      %s\nwhitespace: %s",
			fpAbsent, fpNull, fpEmpty, fpWhitespace)
	}
}

func TestFingerprint_IgnoresVolatileFields(t *testing.T) {
	mapping := product.FieldMapping{
		NamePath:        "title",
		DescriptionPath: "desc",
		BrandPath:       "brand",
	}

	payloadBase := `{
		"title": "Wireless Noise Cancelling Headphones",
		"desc": "Over-ear bluetooth headphones",
		"brand": "Sony",
		"price": 299.99,
		"currency": "USD",
		"stock": 42,
		"updated_at": "2026-09-01T12:00:00Z",
		"etag": "W/\"12345\""
	}`

	payloadMutatedVolatiles := `{
		"title": "Wireless Noise Cancelling Headphones",
		"desc": "Over-ear bluetooth headphones",
		"brand": "Sony",
		"price": 199.50,
		"currency": "EUR",
		"stock": 0,
		"updated_at": "2026-09-24T18:00:00Z",
		"etag": "W/\"99999\""
	}`

	p1, err := product.Normalize([]byte(payloadBase), mapping)
	if err != nil {
		t.Fatalf("normalize base: %v", err)
	}

	p2, err := product.Normalize([]byte(payloadMutatedVolatiles), mapping)
	if err != nil {
		t.Fatalf("normalize mutated volatiles: %v", err)
	}

	fp1 := product.Fingerprint(p1)
	fp2 := product.Fingerprint(p2)

	if fp1 != fp2 {
		t.Fatalf("volatile commercial fields affected fingerprint: %s != %s", fp1, fp2)
	}
}

func TestFingerprint_DetectsEachField(t *testing.T) {
	mapping := product.FieldMapping{
		NamePath:          "title",
		DescriptionPath:   "desc",
		BrandPath:         "brand",
		OriginCountryPath: "country",
		AttributePaths: map[string]string{
			"color": "color",
		},
	}

	basePayload := `{
		"title": "Running Shoes",
		"desc": "Lightweight trail shoes",
		"brand": "Nike",
		"country": "VN",
		"color": "Red"
	}`

	pBase, err := product.Normalize([]byte(basePayload), mapping)
	if err != nil {
		t.Fatalf("normalize base: %v", err)
	}
	baseFP := product.Fingerprint(pBase)

	mutations := []struct {
		field   string
		payload string
	}{
		{"Name", `{"title": "Running Shoes Pro", "desc": "Lightweight trail shoes", "brand": "Nike", "country": "VN", "color": "Red"}`},
		{"Description", `{"title": "Running Shoes", "desc": "Lightweight road shoes", "brand": "Nike", "country": "VN", "color": "Red"}`},
		{"Brand", `{"title": "Running Shoes", "desc": "Lightweight trail shoes", "brand": "Adidas", "country": "VN", "color": "Red"}`},
		{"OriginCountry", `{"title": "Running Shoes", "desc": "Lightweight trail shoes", "brand": "Nike", "country": "US", "color": "Red"}`},
		{"AttributeValue", `{"title": "Running Shoes", "desc": "Lightweight trail shoes", "brand": "Nike", "country": "VN", "color": "Blue"}`},
	}

	for _, m := range mutations {
		t.Run(m.field, func(t *testing.T) {
			pMut, err := product.Normalize([]byte(m.payload), mapping)
			if err != nil {
				t.Fatalf("normalize mutation %s: %v", m.field, err)
			}
			mutFP := product.Fingerprint(pMut)
			if mutFP == baseFP {
				t.Errorf("mutation to field %s did not change fingerprint (%s)", m.field, mutFP)
			}
		})
	}
}

func TestFingerprint_GoldenVectors(t *testing.T) {
	mapping := product.FieldMapping{
		NamePath:          "title",
		DescriptionPath:   "description",
		BrandPath:         "brand",
		OriginCountryPath: "country",
		AttributePaths: map[string]string{
			"color": "attributes.color",
			"power": "attributes.power_w",
		},
	}

	v1Payload := `{
		"title": "Ultra Blender 3000",
		"description": "High-speed professional blender",
		"brand": "Vitamix",
		"country": "US",
		"attributes": {
			"color": "Black",
			"power_w": 1400
		}
	}`

	p1, err := product.Normalize([]byte(v1Payload), mapping)
	if err != nil {
		t.Fatalf("normalize v1: %v", err)
	}

	fp1 := product.Fingerprint(p1)
	// Pinned literal golden hash prevents accidental algorithmic drift.
	wantFP1 := "v1:a7be21d00a2f19364dfc5733fc2114d030d920c477e4e63a194e7e65b1e0404c"
	if fp1 != wantFP1 {
		t.Errorf("Golden vector 1 hash mismatch:\ngot:  %s\nwant: %s", fp1, wantFP1)
	}

	v2Payload := `{"title": "Simple Notebook"}`
	p2, err := product.Normalize([]byte(v2Payload), mapping)
	if err != nil {
		t.Fatalf("normalize v2: %v", err)
	}
	fp2 := product.Fingerprint(p2)
	wantFP2 := "v1:3e7ae6e7cbe7b614b8c65fdbf5fc982e9903fe96da7988324e69d43e4abb14a4"
	if fp2 != wantFP2 {
		t.Errorf("Golden vector 2 hash mismatch:\ngot:  %s\nwant: %s", fp2, wantFP2)
	}
}

func FuzzFingerprint_Permutation(f *testing.F) {
	// Seed corpus with valid variations
	f.Add("Wireless Mouse", "Logitech", "black", "1000")
	f.Add("Gaming Keyboard", "Razer", "green", "500")

	mapping := product.FieldMapping{
		NamePath:  "title",
		BrandPath: "brand",
		AttributePaths: map[string]string{
			"color": "color",
			"dpi":   "dpi",
		},
	}

	f.Fuzz(func(t *testing.T, title, brand, color, dpi string) {
		trimmedTitle := strings.TrimSpace(title)
		if trimmedTitle == "" {
			return
		}

		// Use json.Marshal to ensure well-formed JSON across arbitrary fuzz string inputs
		dataA, err := json.Marshal(map[string]any{
			"title": title, "brand": brand, "color": color, "dpi": dpi,
		})
		if err != nil {
			return
		}

		dataB, err := json.Marshal(map[string]any{
			"brand": brand, "dpi": dpi, "title": title, "color": color,
		})
		if err != nil {
			return
		}

		pA, errA := product.Normalize(dataA, mapping)
		if errA != nil {
			return
		}
		pB, errB := product.Normalize(dataB, mapping)
		if errB != nil {
			return
		}

		fpA := product.Fingerprint(pA)
		fpB := product.Fingerprint(pB)

		if fpA != fpB {
			t.Fatalf("Permutation produced differing fingerprints:\nfpA: %s\nfpB: %s", fpA, fpB)
		}
	})
}

func BenchmarkFingerprint(b *testing.B) {
	mapping := product.FieldMapping{
		NamePath:          "title",
		DescriptionPath:   "description",
		BrandPath:         "brand",
		OriginCountryPath: "country",
		AttributePaths: map[string]string{
			"screen": "specs.screen",
			"ram":    "specs.ram_gb",
			"cpu":    "specs.cpu",
		},
	}

	// Genuine 2 KB sample payload representing realistic supplier catalog records
	payload := []byte(`{
		"title": "ThinkPad X1 Carbon Gen 11 14-inch Ultrabook Core i7 32GB 1TB",
		"description": "Engineered for elite mobile performance, the Lenovo ThinkPad X1 Carbon Gen 11 features a 13th Gen Intel Core i7-1365U vPro processor (10 cores, 12 threads, up to 5.20 GHz Turbo Boost), 32 GB soldered LPDDR5-6400MHz high-speed memory, and a blazing-fast 1 TB PCIe NVMe TLC Opal 2.0 Solid State Drive. Visual excellence is delivered through a stunning 14.0-inch WUXGA (1920 x 1200) IPS Anti-Glare Touchscreen display with 400 nits brightness, 100% sRGB color gamut, and Eyesafe Certified low blue light technology. Security and connectivity are enterprise-grade, incorporating an FHD RGB+IR hybrid camera with mechanical webcam privacy shutter, discrete Trusted Platform Module (dTPM 2.0) chip, match-on-chip power button fingerprint reader, Kensington Nano Security Slot, Wi-Fi 6E AX211 802.11AX (2x2), and Bluetooth 5.1 wireless. Port selection includes two Thunderbolt 4 / USB4 40Gbps (supporting data transfer, Power Delivery 3.0, and DisplayPort 1.4a), two USB 3.2 Gen 1 Type-A (one Always On), HDMI 2.0b output, and a 3.5mm combo headphone/microphone jack. Built with aerospace-grade carbon fiber top cover and magnesium-aluminum alloy bottom chassis in iconic Deep Black finish.",
		"brand": "Lenovo",
		"country": "CN",
		"price": 1849.00,
		"currency": "USD",
		"stock": 15,
		"specs": {
			"screen": "14.0-inch WUXGA (1920 x 1200) IPS Touchscreen 400 nits",
			"ram_gb": 32,
			"cpu": "Intel Core i7-1365U vPro (10 cores, up to 5.20 GHz)",
			"storage": "1 TB PCIe NVMe TLC SSD",
			"battery_wh": 57,
			"weight_kg": 1.12
		},
		"metadata": {
			"supplier_id": "SUP-990214-APAC",
			"warehouse": "HKG-GLOBAL-DIST-01",
			"upc": "0196803251482",
			"ean": "0196803251482",
			"sku": "21HM000DUS",
			"categories": ["Laptops", "Ultrabooks", "Enterprise Computing", "Lenovo ThinkPad"],
			"compliance_tags": ["ENERGY_STAR_8.0", "EPEAT_GOLD", "RoHS_COMPLIANT", "TCO_9.0"],
			"notes": "Top tier commercial SKU intake record used for canonical normalizer latency and memory allocation profiling under high volume ingestion."
		}
	}`)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p, err := product.Normalize(payload, mapping)
		if err != nil {
			b.Fatalf("failed to normalize: %v", err)
		}
		fp := product.Fingerprint(p)
		if fp == "" {
			b.Fatal("empty fingerprint")
		}
	}
}
