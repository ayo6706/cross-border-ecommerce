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

func uuidPtr(u pgtype.UUID) *string {
	if !u.Valid {
		return nil
	}
	s := uuid.Format(u.Bytes)
	return &s
}

// parseOptionalUUID maps nil or blank to NULL; anything else must parse.
func parseOptionalUUID(s *string) (pgtype.UUID, error) {
	if s == nil || strings.TrimSpace(*s) == "" {
		return pgtype.UUID{}, nil
	}
	return parseUUID(*s)
}

// parseUUIDs converts ids for a uuid[] parameter and names the first id that does not parse.
func parseUUIDs(ids []string) ([]pgtype.UUID, error) {
	out := make([]pgtype.UUID, len(ids))
	for i, id := range ids {
		u, err := parseUUID(id)
		if err != nil {
			return nil, fmt.Errorf("id %q: %w", id, err)
		}
		out[i] = u
	}
	return out, nil
}
