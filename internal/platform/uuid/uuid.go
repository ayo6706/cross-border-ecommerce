package uuid

import (
	"crypto/rand"
	"errors"
	"fmt"
)

// NewV4 generates an RFC 4122 version 4 UUID as a 16-byte array.
func NewV4() ([16]byte, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return b, err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant RFC 4122 (10xx)
	return b, nil
}

// NewString returns a canonical RFC 4122 v4 formatted string (36 characters).
func NewString() (string, error) {
	b, err := NewV4()
	if err != nil {
		return "", err
	}
	return Format(b), nil
}

// Format converts a 16-byte UUID array into standard hyphenated string representation.
func Format(b [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// Validate checks whether s is a valid 36-character hyphenated UUID string.
func Validate(s string) error {
	if len(s) != 36 {
		return errors.New("uuid string must be exactly 36 characters")
	}
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return errors.New("invalid uuid format: missing hyphen")
	}
	for i := 0; i < 36; i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return fmt.Errorf("invalid uuid character %c at position %d", c, i)
		}
	}
	return nil
}
