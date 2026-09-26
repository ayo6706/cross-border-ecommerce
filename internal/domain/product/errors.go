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
	ErrIdentityConflict      = errors.New("concurrent product identity conflict")
	ErrVersionConflict       = errors.New("concurrent product version conflict")
	ErrDeadlockConflict      = errors.New("concurrent transaction conflict (deadlock or serialization)")
	ErrVersionNotFound       = errors.New("product version not found")
	ErrInvalidTransition     = errors.New("invalid product transition state")
)
