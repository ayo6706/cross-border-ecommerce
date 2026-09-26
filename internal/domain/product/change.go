package product

import (
	"errors"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type ChangeType string

const (
	ChangeTypeNew     ChangeType = "NEW"
	ChangeTypeChanged ChangeType = "CHANGED"
)

type ProductChange struct {
	ID             string
	ProductID      ID
	FromVersionID  *string
	ToVersionID    string
	ChangeType     ChangeType
	ChangedFields  []string
	IngestionRunID *string
	RawRecordID    *string
	DetectedAt     time.Time
}

//nolint:funlen // legacy baseline 2026-09-26: fix in ENG-016
func NewProductChange(
	id string,
	productID ID,
	fromVersionID *string,
	toVersionID string,
	changeType ChangeType,
	changedFields []string,
	ingestionRunID *string,
	rawRecordID *string,
) (*ProductChange, error) {
	trimmedID := strings.TrimSpace(id)
	if trimmedID == "" {
		generatedID, err := uuid.NewString()
		if err != nil {
			return nil, err
		}
		trimmedID = generatedID
	}

	if strings.TrimSpace(string(productID)) == "" {
		return nil, errors.New("product id cannot be empty")
	}
	if strings.TrimSpace(toVersionID) == "" {
		return nil, errors.New("to_version_id cannot be empty")
	}
	if changeType != ChangeTypeNew && changeType != ChangeTypeChanged {
		return nil, errors.New("invalid change type: must be NEW or CHANGED")
	}
	if changeType == ChangeTypeNew && fromVersionID != nil && strings.TrimSpace(*fromVersionID) != "" {
		return nil, errors.New("from_version_id must be nil for change type NEW")
	}
	if changeType == ChangeTypeChanged && (fromVersionID == nil || strings.TrimSpace(*fromVersionID) == "") {
		return nil, errors.New("from_version_id is required for change type CHANGED")
	}

	var fromID *string
	if fromVersionID != nil && strings.TrimSpace(*fromVersionID) != "" {
		v := strings.TrimSpace(*fromVersionID)
		fromID = &v
	}

	var runID *string
	if ingestionRunID != nil && strings.TrimSpace(*ingestionRunID) != "" {
		v := strings.TrimSpace(*ingestionRunID)
		runID = &v
	}

	var rawID *string
	if rawRecordID != nil && strings.TrimSpace(*rawRecordID) != "" {
		v := strings.TrimSpace(*rawRecordID)
		rawID = &v
	}

	fieldsCopy := make([]string, len(changedFields))
	copy(fieldsCopy, changedFields)

	return &ProductChange{
		ID:             trimmedID,
		ProductID:      productID,
		FromVersionID:  fromID,
		ToVersionID:    strings.TrimSpace(toVersionID),
		ChangeType:     changeType,
		ChangedFields:  fieldsCopy,
		IngestionRunID: runID,
		RawRecordID:    rawID,
		DetectedAt:     time.Now().UTC(),
	}, nil
}

func (pc *ProductChange) Validate() error {
	if pc == nil {
		return ErrInvalidProductState
	}
	if strings.TrimSpace(pc.ID) == "" {
		return errors.New("product change id cannot be empty")
	}
	if strings.TrimSpace(string(pc.ProductID)) == "" {
		return errors.New("product change product id cannot be empty")
	}
	if strings.TrimSpace(pc.ToVersionID) == "" {
		return errors.New("product change to_version_id cannot be empty")
	}
	if pc.ChangeType != ChangeTypeNew && pc.ChangeType != ChangeTypeChanged {
		return errors.New("invalid product change type")
	}
	return nil
}
