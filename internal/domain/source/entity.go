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
