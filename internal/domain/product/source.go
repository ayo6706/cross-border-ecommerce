package product

import (
	"time"
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
