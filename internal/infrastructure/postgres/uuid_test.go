package postgres

import (
	"testing"
)

func TestUUIDHelpers(t *testing.T) {
	t.Parallel()

	// 1. Generation
	u1, err := newUUID()
	if err != nil {
		t.Fatalf("unexpected error generating uuid: %v", err)
	}
	if !u1.Valid {
		t.Fatal("expected generated uuid to be valid")
	}

	// Check RFC 4122 version 4 and variant
	if u1.Bytes[6]>>4 != 4 {
		t.Errorf("expected version 4, got %d", u1.Bytes[6]>>4)
	}
	if u1.Bytes[8]>>6 != 2 {
		t.Errorf("expected variant RFC 4122 (10xx), got %d", u1.Bytes[8]>>6)
	}

	// 2. String conversion
	str1 := uuidToString(u1)
	if len(str1) != 36 {
		t.Fatalf("expected 36 chars in uuid string, got %d (%s)", len(str1), str1)
	}

	// 3. Parse back
	u2, err := parseUUID(str1)
	if err != nil {
		t.Fatalf("failed to parse back uuid string: %v", err)
	}
	if u1.Bytes != u2.Bytes {
		t.Fatalf("expected identical bytes after round-trip: %v != %v", u1.Bytes, u2.Bytes)
	}

	// 4. Invalid parse cases
	if _, err := parseUUID(""); err == nil {
		t.Error("expected error parsing empty string")
	}
	if _, err := parseUUID("not-a-valid-uuid"); err == nil {
		t.Error("expected error parsing invalid uuid string")
	}
}
