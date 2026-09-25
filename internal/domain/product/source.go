package product

import (
	"errors"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type ProductSource struct {
	ID                  string
	ProductID           ID
	SourceID            string
	ExternalProductID   string
	FirstSeenAt         time.Time
	LastChangedAt       time.Time
	LastSourceUpdatedAt *time.Time
	LastReceivedAt      time.Time
}

func NewProductSource(
	id string,
	productID ID,
	sourceID string,
	externalProductID string,
	sourceUpdatedAt *time.Time,
	receivedAt time.Time,
) (*ProductSource, error) {
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
	if strings.TrimSpace(string(sourceID)) == "" {
		return nil, errors.New("source id cannot be empty")
	}
	if strings.TrimSpace(externalProductID) == "" {
		return nil, errors.New("external product id cannot be empty")
	}
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}

	now := time.Now().UTC()
	return &ProductSource{
		ID:                  trimmedID,
		ProductID:           productID,
		SourceID:            sourceID,
		ExternalProductID:   strings.TrimSpace(externalProductID),
		FirstSeenAt:         now,
		LastChangedAt:       now,
		LastSourceUpdatedAt: sourceUpdatedAt,
		LastReceivedAt:      receivedAt.UTC(),
	}, nil
}

func (ps *ProductSource) Validate() error {
	if ps == nil {
		return ErrInvalidProductState
	}
	if strings.TrimSpace(ps.ID) == "" {
		return errors.New("product source id cannot be empty")
	}
	if strings.TrimSpace(string(ps.ProductID)) == "" {
		return errors.New("product source product id cannot be empty")
	}
	if strings.TrimSpace(string(ps.SourceID)) == "" {
		return errors.New("product source source id cannot be empty")
	}
	if strings.TrimSpace(ps.ExternalProductID) == "" {
		return errors.New("product source external product id cannot be empty")
	}
	return nil
}
