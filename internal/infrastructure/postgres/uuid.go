package postgres

import (
	"crypto/rand"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

func newUUID() (pgtype.UUID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return pgtype.UUID{}, fmt.Errorf("read random bytes: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // RFC 4122 version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return pgtype.UUID{Bytes: b, Valid: true}, nil
}

func parseUUID(s string) (pgtype.UUID, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return pgtype.UUID{}, fmt.Errorf("uuid cannot be empty")
	}

	var u pgtype.UUID
	if err := u.Scan(trimmed); err != nil {
		return pgtype.UUID{}, fmt.Errorf("parse uuid: %w", err)
	}
	return u, nil
}

func uuidToString(u pgtype.UUID) string {
	if !u.Valid {
		return ""
	}
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		u.Bytes[0], u.Bytes[1], u.Bytes[2], u.Bytes[3],
		u.Bytes[4], u.Bytes[5],
		u.Bytes[6], u.Bytes[7],
		u.Bytes[8], u.Bytes[9],
		u.Bytes[10], u.Bytes[11], u.Bytes[12], u.Bytes[13], u.Bytes[14], u.Bytes[15],
	)
}
