package compliance

import (
	"context"
	"time"
)

type Repository interface {
	FindTariffRate(ctx context.Context, hsCode string, countryCode string, at time.Time) (*TariffRate, error)
	IsSanctioned(ctx context.Context, countryCode string, entityName string) (bool, error)
	RecordDecision(ctx context.Context, decision *ComplianceDecision) error
}
