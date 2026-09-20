package product

import (
	"errors"
)

var (
	ErrProductNotFound     = errors.New("product not found")
	ErrInvalidProductState = errors.New("invalid product state")
	ErrEmptyCanonicalName  = errors.New("product canonical name cannot be empty")
)
