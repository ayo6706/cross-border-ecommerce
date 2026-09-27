package product

import (
	"time"
)

type ProductVersion struct {
	ID             string
	ProductID      ID
	VersionNumber  int
	Fingerprint    string
	CanonicalName  string
	Description    string
	Brand          string
	OriginCountry  string
	Attributes     map[string]string
	IngestionRunID *string
	CreatedAt      time.Time
}
