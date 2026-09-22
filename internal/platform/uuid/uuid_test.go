package uuid_test

import (
	"regexp"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

var uuidRegex = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewV4(t *testing.T) {
	t.Parallel()

	b, err := uuid.NewV4()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// RFC 4122 version 4
	if b[6]>>4 != 4 {
		t.Errorf("expected version 4, got %d", b[6]>>4)
	}
	// Variant RFC 4122 (10xx)
	if b[8]>>6 != 2 {
		t.Errorf("expected variant 2 (10xx), got %d", b[8]>>6)
	}
}

func TestNewString(t *testing.T) {
	t.Parallel()

	s, err := uuid.NewString()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(s) != 36 {
		t.Errorf("expected length 36, got %d (%s)", len(s), s)
	}

	if !uuidRegex.MatchString(s) {
		t.Errorf("string %q did not match RFC 4122 v4 pattern", s)
	}
}

func TestFormat(t *testing.T) {
	t.Parallel()

	var b [16]byte
	for i := range b {
		b[i] = byte(i)
	}

	formatted := uuid.Format(b)
	expected := "00010203-0405-0607-0809-0a0b0c0d0e0f"
	if formatted != expected {
		t.Errorf("expected %s, got %s", expected, formatted)
	}
}
