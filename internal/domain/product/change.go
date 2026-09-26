package product

import (
	"time"
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
