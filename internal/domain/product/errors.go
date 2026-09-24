package product

import (
	"errors"
)

var (
	ErrProductNotFound       = errors.New("product not found")
	ErrInvalidProductState   = errors.New("invalid product state")
	ErrMissingCanonicalField = errors.New("product canonical field cannot be empty")
	ErrEmptyCanonicalName    = ErrMissingCanonicalField
	ErrMalformedRecord       = errors.New("malformed raw record payload")
	ErrInvalidFieldMapping   = errors.New("invalid product field mapping")
	ErrMissingFieldMapping   = errors.New("missing product field mapping")
	ErrInvalidOriginCountry  = errors.New("invalid origin country: must be 2-letter ISO code")
	ErrDuplicateAttributeKey = errors.New("duplicate attribute key in field mapping after case folding")
)
