package source

import (
	"time"
)

type ID string

type Type string

const (
	TypeUnknown Type = ""
	TypeAPI     Type = "API"
	TypeFile    Type = "FILE"
	TypeFeed    Type = "FEED"
	TypeScraper Type = "SCRAPER"
)

type Source struct {
	ID        ID
	Name      string
	Type      Type
	Config    map[string]any
	RateLimit int
	CreatedAt time.Time
	UpdatedAt time.Time
	Enabled   bool
}

func NewSource(id ID, name string, srcType Type, config map[string]any, rateLimit int) (*Source, error) {
	now := time.Now().UTC()
	if config == nil {
		config = make(map[string]any)
	}
	s := &Source{
		ID:        id,
		Name:      name,
		Type:      srcType,
		Config:    config,
		RateLimit: rateLimit,
		Enabled:   true,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Source) Validate() error {
	if s == nil {
		return ErrInvalidSourceState
	}
	if string(s.ID) == "" {
		return ErrInvalidSourceID
	}
	if s.Name == "" {
		return ErrInvalidSourceName
	}
	switch s.Type {
	case TypeAPI, TypeFile, TypeFeed, TypeScraper:
	default:
		return ErrInvalidSourceType
	}
	if s.RateLimit <= 0 {
		return ErrInvalidRateLimit
	}
	return nil
}

func (s *Source) SetRateLimit(limit int) error {
	if limit <= 0 {
		return ErrInvalidRateLimit
	}
	s.RateLimit = limit
	s.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Source) Enable() {
	s.Enabled = true
	s.UpdatedAt = time.Now().UTC()
}

func (s *Source) Disable() {
	s.Enabled = false
	s.UpdatedAt = time.Now().UTC()
}
