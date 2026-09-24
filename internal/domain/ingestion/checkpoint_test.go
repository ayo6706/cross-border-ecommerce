package ingestion_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

func TestFormatAndParseTimestampCheckpoint(t *testing.T) {
	t.Run("ValidTimestamp_RFC3339Nano", func(t *testing.T) {
		origTime := time.Date(2026, 9, 22, 14, 30, 15, 123456789, time.UTC)
		formatted := ingestion.FormatTimestampCheckpoint(origTime)
		if formatted == "" {
			t.Fatal("expected non-empty formatted timestamp")
		}

		parsed, err := ingestion.ParseTimestampCheckpoint(formatted)
		if err != nil {
			t.Fatalf("unexpected error parsing timestamp: %v", err)
		}

		if !parsed.Equal(origTime) {
			t.Errorf("parsed time mismatch: got %v, want %v", parsed, origTime)
		}
	})

	t.Run("ValidTimestamp_NonUTC_NormalizedToUTC", func(t *testing.T) {
		loc := time.FixedZone("EST", -5*3600)
		origTime := time.Date(2026, 9, 22, 9, 30, 0, 0, loc)
		formatted := ingestion.FormatTimestampCheckpoint(origTime)

		parsed, err := ingestion.ParseTimestampCheckpoint(formatted)
		if err != nil {
			t.Fatalf("unexpected error parsing timestamp: %v", err)
		}

		if parsed.Location() != time.UTC {
			t.Errorf("expected UTC location, got %v", parsed.Location())
		}
		if !parsed.Equal(origTime) {
			t.Errorf("expected equal instant: got %v, want %v", parsed, origTime)
		}
	})

	t.Run("EmptyTimestamp", func(t *testing.T) {
		zeroTime := time.Time{}
		formatted := ingestion.FormatTimestampCheckpoint(zeroTime)
		if formatted != "" {
			t.Errorf("expected empty string for zero time, got %q", formatted)
		}

		parsed, err := ingestion.ParseTimestampCheckpoint("")
		if err != nil {
			t.Fatalf("unexpected error on empty string: %v", err)
		}
		if !parsed.IsZero() {
			t.Errorf("expected zero time for empty string, got %v", parsed)
		}
	})

	t.Run("InvalidTimestampFormat", func(t *testing.T) {
		_, err := ingestion.ParseTimestampCheckpoint("invalid-date-string")
		if err == nil {
			t.Fatal("expected error parsing invalid timestamp, got nil")
		}
		if !errors.Is(err, ingestion.ErrInvalidCheckpoint) {
			t.Errorf("expected ErrInvalidCheckpoint, got %v", err)
		}
	})
}

func TestFormatAndParseByteOffsetCheckpoint(t *testing.T) {
	t.Run("ValidPositiveOffset", func(t *testing.T) {
		offset := int64(1048576)
		formatted := ingestion.FormatByteOffsetCheckpoint(offset)
		if formatted != "offset:1048576" {
			t.Errorf("expected offset:1048576, got %q", formatted)
		}

		parsed, err := ingestion.ParseByteOffsetCheckpoint(formatted)
		if err != nil {
			t.Fatalf("unexpected error parsing byte offset: %v", err)
		}
		if parsed != offset {
			t.Errorf("offset mismatch: got %d, want %d", parsed, offset)
		}
	})

	t.Run("ValidZeroOffset", func(t *testing.T) {
		formatted := ingestion.FormatByteOffsetCheckpoint(0)
		if formatted != "offset:0" {
			t.Errorf("expected offset:0, got %q", formatted)
		}

		parsed, err := ingestion.ParseByteOffsetCheckpoint(formatted)
		if err != nil {
			t.Fatalf("unexpected error parsing zero offset: %v", err)
		}
		if parsed != 0 {
			t.Errorf("expected 0, got %d", parsed)
		}
	})

	t.Run("BareNumericOffsetWithoutPrefix", func(t *testing.T) {
		parsed, err := ingestion.ParseByteOffsetCheckpoint("2048")
		if err != nil {
			t.Fatalf("unexpected error parsing bare numeric offset: %v", err)
		}
		if parsed != 2048 {
			t.Errorf("expected 2048, got %d", parsed)
		}
	})

	t.Run("EmptyByteOffset", func(t *testing.T) {
		parsed, err := ingestion.ParseByteOffsetCheckpoint("")
		if err != nil {
			t.Fatalf("unexpected error on empty offset: %v", err)
		}
		if parsed != 0 {
			t.Errorf("expected 0, got %d", parsed)
		}
	})

	t.Run("NegativeOffsetFormat", func(t *testing.T) {
		formatted := ingestion.FormatByteOffsetCheckpoint(-1)
		if formatted != "" {
			t.Errorf("expected empty string for negative offset, got %q", formatted)
		}

		_, err := ingestion.ParseByteOffsetCheckpoint("offset:-50")
		if err == nil {
			t.Fatal("expected error on negative offset, got nil")
		}
		if !errors.Is(err, ingestion.ErrInvalidCheckpoint) {
			t.Errorf("expected ErrInvalidCheckpoint, got %v", err)
		}
	})

	t.Run("InvalidOffsetFormat", func(t *testing.T) {
		_, err := ingestion.ParseByteOffsetCheckpoint("offset:not-a-number")
		if err == nil {
			t.Fatal("expected error on non-numeric offset, got nil")
		}
		if !errors.Is(err, ingestion.ErrInvalidCheckpoint) {
			t.Errorf("expected ErrInvalidCheckpoint, got %v", err)
		}
	})
}

func TestFormatAndParseCursorCheckpoint(t *testing.T) {
	t.Run("ValidCursor", func(t *testing.T) {
		cursor := "eyJzb3VyY2UiOiJzdXBwbGllcl9hIiwicGFnZSI6Mn0="
		formatted := ingestion.FormatCursorCheckpoint(cursor)
		if formatted != "cursor:"+cursor {
			t.Errorf("expected prefixed cursor, got %q", formatted)
		}

		parsed, err := ingestion.ParseCursorCheckpoint(formatted)
		if err != nil {
			t.Fatalf("unexpected error parsing cursor: %v", err)
		}
		if parsed != cursor {
			t.Errorf("cursor mismatch: got %q, want %q", parsed, cursor)
		}
	})

	t.Run("BareCursorWithoutPrefix", func(t *testing.T) {
		raw := "opaque_cursor_token_123"
		parsed, err := ingestion.ParseCursorCheckpoint(raw)
		if err != nil {
			t.Fatalf("unexpected error parsing bare cursor: %v", err)
		}
		if parsed != raw {
			t.Errorf("expected %q, got %q", raw, parsed)
		}
	})

	t.Run("EmptyCursor", func(t *testing.T) {
		formatted := ingestion.FormatCursorCheckpoint("")
		if formatted != "" {
			t.Errorf("expected empty string, got %q", formatted)
		}

		parsed, err := ingestion.ParseCursorCheckpoint("")
		if err != nil {
			t.Fatalf("unexpected error on empty cursor: %v", err)
		}
		if parsed != "" {
			t.Errorf("expected empty string, got %q", parsed)
		}
	})

	t.Run("EmptyCursorWithPrefixOnly", func(t *testing.T) {
		_, err := ingestion.ParseCursorCheckpoint("cursor:")
		if err == nil {
			t.Fatal("expected error on empty cursor prefix, got nil")
		}
		if !errors.Is(err, ingestion.ErrInvalidCheckpoint) {
			t.Errorf("expected ErrInvalidCheckpoint, got %v", err)
		}
	})

	t.Run("FormatCursorIdempotent", func(t *testing.T) {
		formatted := ingestion.FormatCursorCheckpoint("cursor:already_prefixed")
		if formatted != "cursor:already_prefixed" {
			t.Errorf("expected idempotent formatting, got %q", formatted)
		}
	})

	t.Run("ByteOffsetRejectedAsCursor", func(t *testing.T) {
		_, err := ingestion.ParseCursorCheckpoint("offset:1024")
		if err == nil {
			t.Fatal("expected error parsing offset as cursor, got nil")
		}
		if !errors.Is(err, ingestion.ErrInvalidCheckpoint) {
			t.Errorf("expected ErrInvalidCheckpoint, got %v", err)
		}
	})
}
