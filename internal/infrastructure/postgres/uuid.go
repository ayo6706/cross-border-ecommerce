package postgres

import (
	"fmt"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func newUUID() (pgtype.UUID, error) {
	b, err := uuid.NewV4()
	if err != nil {
		return pgtype.UUID{}, fmt.Errorf("read random bytes: %w", err)
	}
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
	return uuid.Format(u.Bytes)
}
