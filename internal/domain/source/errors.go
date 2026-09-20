package source

import (
	"errors"
)

var (
	ErrSourceNotFound      = errors.New("source not found")
	ErrInvalidSourceConfig = errors.New("invalid source configuration")
	ErrInvalidSourceState  = errors.New("invalid source state")
)
