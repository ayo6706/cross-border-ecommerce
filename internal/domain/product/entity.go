package product

import "time"

type ID string

type Status string

// StatusDraft is the only status the ingestion path writes; products.status defaults to it (migration 000006).
const StatusDraft Status = "DRAFT"

type Product struct {
	ID                 ID
	CanonicalName      string
	Description        string
	Brand              string
	OriginCountry      string
	Status             Status
	CurrentVersionID   *string
	CurrentFingerprint string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}
