package ingestion

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	byteOffsetPrefix = "offset:"
	cursorPrefix     = "cursor:"
)

// FormatTimestampCheckpoint serializes a UTC time into RFC3339Nano format.
func FormatTimestampCheckpoint(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// ParseTimestampCheckpoint parses an RFC3339 or RFC3339Nano timestamp string into a UTC time.Time.
func ParseTimestampCheckpoint(cp string) (time.Time, error) {
	trimmed := strings.TrimSpace(cp)
	if trimmed == "" {
		return time.Time{}, nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, trimmed)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, trimmed)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: invalid timestamp format: %s", ErrInvalidCheckpoint, trimmed)
		}
	}

	return parsed.UTC(), nil
}

// FormatByteOffsetCheckpoint formats a non-negative byte offset as "offset:<bytes>".
func FormatByteOffsetCheckpoint(offset int64) string {
	if offset < 0 {
		return ""
	}
	return fmt.Sprintf("%s%d", byteOffsetPrefix, offset)
}

// ParseByteOffsetCheckpoint parses an "offset:<bytes>" string into an int64.
func ParseByteOffsetCheckpoint(cp string) (int64, error) {
	trimmed := strings.TrimSpace(cp)
	if trimmed == "" {
		return 0, nil
	}

	valStr := trimmed
	if strings.HasPrefix(strings.ToLower(trimmed), byteOffsetPrefix) {
		valStr = trimmed[len(byteOffsetPrefix):]
	}

	offset, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid byte offset format: %s", ErrInvalidCheckpoint, trimmed)
	}

	if offset < 0 {
		return 0, fmt.Errorf("%w: byte offset cannot be negative: %d", ErrInvalidCheckpoint, offset)
	}

	return offset, nil
}

// FormatCursorCheckpoint prefixes an opaque token with "cursor:".
func FormatCursorCheckpoint(cursor string) string {
	trimmed := strings.TrimSpace(cursor)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, cursorPrefix) {
		return trimmed
	}
	return cursorPrefix + trimmed
}

// ParseCursorCheckpoint extracts the opaque cursor token from a checkpoint string.
func ParseCursorCheckpoint(cp string) (string, error) {
	trimmed := strings.TrimSpace(cp)
	if trimmed == "" {
		return "", nil
	}

	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, byteOffsetPrefix) {
		return "", fmt.Errorf("%w: byte offset cannot be parsed as cursor: %s", ErrInvalidCheckpoint, trimmed)
	}

	if strings.HasPrefix(lower, cursorPrefix) {
		cursorVal := strings.TrimSpace(trimmed[len(cursorPrefix):])
		if cursorVal == "" {
			return "", fmt.Errorf("%w: empty cursor value in checkpoint: %s", ErrInvalidCheckpoint, trimmed)
		}
		return cursorVal, nil
	}

	return trimmed, nil
}
