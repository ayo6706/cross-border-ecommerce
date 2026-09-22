package ingestion

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type CheckpointType string

const (
	CheckpointTypeEmpty      CheckpointType = "EMPTY"
	CheckpointTypeTimestamp  CheckpointType = "TIMESTAMP"
	CheckpointTypeByteOffset CheckpointType = "BYTE_OFFSET"
	CheckpointTypeCursor     CheckpointType = "CURSOR"
)

const (
	byteOffsetPrefix = "offset:"
	cursorPrefix     = "cursor:"
)

func FormatTimestampCheckpoint(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

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

func FormatByteOffsetCheckpoint(offset int64) string {
	if offset < 0 {
		return ""
	}
	return fmt.Sprintf("%s%d", byteOffsetPrefix, offset)
}

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

func FormatCursorCheckpoint(cursor string) string {
	trimmed := strings.TrimSpace(cursor)
	if trimmed == "" {
		return ""
	}
	return fmt.Sprintf("%s%s", cursorPrefix, trimmed)
}

func ParseCursorCheckpoint(cp string) (string, error) {
	trimmed := strings.TrimSpace(cp)
	if trimmed == "" {
		return "", nil
	}

	if strings.HasPrefix(strings.ToLower(trimmed), cursorPrefix) {
		cursorVal := strings.TrimSpace(trimmed[len(cursorPrefix):])
		if cursorVal == "" {
			return "", fmt.Errorf("%w: empty cursor value in checkpoint: %s", ErrInvalidCheckpoint, trimmed)
		}
		return cursorVal, nil
	}

	return trimmed, nil
}

func DetectCheckpointType(cp string) CheckpointType {
	trimmed := strings.TrimSpace(cp)
	if trimmed == "" {
		return CheckpointTypeEmpty
	}

	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, byteOffsetPrefix) {
		return CheckpointTypeByteOffset
	}
	if strings.HasPrefix(lower, cursorPrefix) {
		return CheckpointTypeCursor
	}

	if _, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return CheckpointTypeTimestamp
	}
	if _, err := time.Parse(time.RFC3339, trimmed); err == nil {
		return CheckpointTypeTimestamp
	}

	return CheckpointTypeCursor
}
