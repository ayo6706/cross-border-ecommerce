package product

import (
	"errors"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
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

//nolint:funlen // legacy baseline 2026-09-26: fix in ENG-016
func NewProductVersion(
	id string,
	productID ID,
	versionNumber int,
	fingerprint string,
	canonicalName, description, brand, originCountry string,
	attributes map[string]string,
	ingestionRunID *string,
) (*ProductVersion, error) {
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
	if versionNumber < 1 {
		return nil, errors.New("version number must be >= 1")
	}
	if strings.TrimSpace(fingerprint) == "" {
		return nil, errors.New("fingerprint cannot be empty")
	}
	if strings.TrimSpace(canonicalName) == "" {
		return nil, ErrEmptyCanonicalName
	}

	attrsCopy := make(map[string]string, len(attributes))
	for k, v := range attributes {
		attrsCopy[k] = v
	}

	var trimmedRunID *string
	if ingestionRunID != nil && strings.TrimSpace(*ingestionRunID) != "" {
		v := strings.TrimSpace(*ingestionRunID)
		trimmedRunID = &v
	}

	return &ProductVersion{
		ID:             trimmedID,
		ProductID:      productID,
		VersionNumber:  versionNumber,
		Fingerprint:    strings.TrimSpace(fingerprint),
		CanonicalName:  strings.TrimSpace(canonicalName),
		Description:    strings.TrimSpace(description),
		Brand:          strings.TrimSpace(brand),
		OriginCountry:  strings.ToUpper(strings.TrimSpace(originCountry)),
		Attributes:     attrsCopy,
		IngestionRunID: trimmedRunID,
		CreatedAt:      time.Now().UTC(),
	}, nil
}

func (pv *ProductVersion) Validate() error {
	if pv == nil {
		return ErrInvalidProductState
	}
	if strings.TrimSpace(pv.ID) == "" {
		return errors.New("product version id cannot be empty")
	}
	if strings.TrimSpace(string(pv.ProductID)) == "" {
		return errors.New("product version product id cannot be empty")
	}
	if pv.VersionNumber < 1 {
		return errors.New("product version number must be >= 1")
	}
	if strings.TrimSpace(pv.Fingerprint) == "" {
		return errors.New("product version fingerprint cannot be empty")
	}
	if strings.TrimSpace(pv.CanonicalName) == "" {
		return ErrEmptyCanonicalName
	}
	return nil
}
