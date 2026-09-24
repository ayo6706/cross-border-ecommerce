package product

import (
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type ID string

type Status string

const (
	StatusUnknown  Status = ""
	StatusDraft    Status = "DRAFT"
	StatusActive   Status = "ACTIVE"
	StatusArchived Status = "ARCHIVED"
)

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

func NewProduct(id ID, canonicalName, description, brand, originCountry string) (*Product, error) {
	trimmedID := strings.TrimSpace(string(id))
	if trimmedID == "" {
		generatedID, err := uuid.NewString()
		if err != nil {
			return nil, err
		}
		trimmedID = generatedID
	}

	now := time.Now().UTC()
	p := &Product{
		ID:            ID(trimmedID),
		CanonicalName: strings.TrimSpace(canonicalName),
		Description:   strings.TrimSpace(description),
		Brand:         strings.TrimSpace(brand),
		OriginCountry: strings.ToUpper(strings.TrimSpace(originCountry)),
		Status:        StatusDraft,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Product) Validate() error {
	if p == nil {
		return ErrInvalidProductState
	}
	if strings.TrimSpace(p.CanonicalName) == "" {
		return ErrEmptyCanonicalName
	}
	return nil
}
