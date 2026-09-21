package product

import (
	"strings"
	"time"

	"github.com/shopspring/decimal"
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
	now := time.Now().UTC()
	p := &Product{
		ID:            id,
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

type ProductPrice struct {
	ProductID ID
	SourceID  string
	Currency  string
	Amount    decimal.Decimal
	UpdatedAt time.Time
}

type ProductInventory struct {
	ProductID ID
	SourceID  string
	Quantity  int64
	UpdatedAt time.Time
}
