package source

import (
	"errors"
)

var (
	ErrSourceNotFound      = errors.New("source not found")
	ErrInvalidSourceConfig = errors.New("invalid source configuration")
	ErrInvalidSourceState  = errors.New("invalid source state")
	ErrInvalidSourceID     = errors.New("source id cannot be empty")
	ErrInvalidSourceName   = errors.New("source name cannot be empty")
	ErrInvalidSourceType   = errors.New("unrecognized source type")
	ErrInvalidRateLimit    = errors.New("rate limit must be greater than zero")
	ErrSourceInUse         = errors.New("source cannot be deleted because it is referenced by existing runs or records")
)
