package extraction

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

// RecordExtractor extracts raw record byte slices from an incoming transport payload.
type RecordExtractor interface {
	ExtractRecords(payload []byte) ([][]byte, error)
}

// PathRecordExtractor extracts records located at a specific dot-notation path (e.g. "payload.catalog", "products").
// If RecordsPath is empty or ".", the root payload is expected to be a JSON array.
type PathRecordExtractor struct {
	RecordsPath string
}

func NewPathRecordExtractor(recordsPath string) *PathRecordExtractor {
	return &PathRecordExtractor{
		RecordsPath: strings.TrimSpace(recordsPath),
	}
}

func (e *PathRecordExtractor) ExtractRecords(payload []byte) ([][]byte, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("%w: empty payload", ingestion.ErrEmptyPayload)
	}

	// 1. Root-array mode (e.g. RecordsPath is empty or ".")
	if e.RecordsPath == "" || e.RecordsPath == "." {
		var rawList []json.RawMessage
		if err := json.Unmarshal(trimmed, &rawList); err != nil {
			if strings.HasPrefix(string(trimmed), "{") {
				return nil, fmt.Errorf("%w: expected root array, found json object", ingestion.ErrPathWrongType)
			}
			return nil, fmt.Errorf("%w: payload is not a valid json array: %v", ingestion.ErrMalformedRecord, err)
		}

		result := make([][]byte, 0, len(rawList))
		for i, item := range rawList {
			itemTrimmed := bytes.TrimSpace(item)
			if !bytes.HasPrefix(itemTrimmed, []byte("{")) {
				return nil, fmt.Errorf("%w: record at index %d is not a json object: %s", ingestion.ErrRecordNotObject, i, string(itemTrimmed))
			}
			result = append(result, itemTrimmed)
		}
		return result, nil
	}

	// 2. Nested path extraction
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	dec.UseNumber()

	var root any
	if err := dec.Decode(&root); err != nil {
		return nil, fmt.Errorf("%w: invalid json payload: %v", ingestion.ErrMalformedRecord, err)
	}

	target, found := LookupPath(root, e.RecordsPath)
	if !found || target == nil {
		return nil, fmt.Errorf("%w: path %q does not exist in response payload", ingestion.ErrPathNotFound, e.RecordsPath)
	}

	sliceNode, ok := target.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: path %q resolved to %T, expected json array", ingestion.ErrPathWrongType, e.RecordsPath, target)
	}

	result := make([][]byte, 0, len(sliceNode))
	for i, item := range sliceNode {
		itemMap, isMap := item.(map[string]any)
		if !isMap || itemMap == nil {
			return nil, fmt.Errorf("%w: record at index %d is not a json object", ingestion.ErrRecordNotObject, i)
		}
		marshaled, err := json.Marshal(itemMap)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to marshal record at index %d: %v", ingestion.ErrMalformedRecord, i, err)
		}
		result = append(result, marshaled)
	}

	return result, nil
}
