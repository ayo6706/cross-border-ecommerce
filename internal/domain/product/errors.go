package product

import (
	"errors"
)

var (
	ErrProductNotFound       = errors.New("product not found")
	ErrInvalidProductState   = errors.New("invalid product state")
	ErrMissingCanonicalField = errors.New("product canonical field cannot be empty")
	ErrMalformedRecord       = errors.New("malformed source record")
	ErrInvalidFieldMapping   = errors.New("invalid product field mapping")
	ErrMissingFieldMapping   = errors.New("missing product field mapping")
	ErrInvalidOriginCountry  = errors.New("invalid origin country: must be 2-letter ISO code")
	ErrDuplicateAttributeKey = errors.New("duplicate attribute key in field mapping after case folding")
	ErrIdentityConflict      = errors.New("concurrent product identity conflict")
	ErrVersionConflict       = errors.New("concurrent product version conflict")
	ErrDeadlockConflict      = errors.New("concurrent transaction conflict (deadlock or serialization)")
	ErrInvalidTransition     = errors.New("invalid product transition state")
	ErrNegativeMetric        = errors.New("metrics cannot be negative")
	ErrInvalidListParams     = errors.New("invalid product list parameters")
)
