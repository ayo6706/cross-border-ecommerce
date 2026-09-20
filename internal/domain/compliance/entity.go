package compliance

import (
	"time"

	"github.com/shopspring/decimal"
)

type DecisionStatus string

const (
	DecisionAllow DecisionStatus = "ALLOW"
	DecisionBlock DecisionStatus = "BLOCK"
	DecisionHold  DecisionStatus = "HOLD"
)

type HSCode struct {
	Code        string
	Description string
	CountryCode string
}

type TariffRate struct {
	HSCode        string
	CountryCode   string
	DutyRate      decimal.Decimal
	VatRate       decimal.Decimal
	EffectiveFrom time.Time
	EffectiveTo   time.Time
}

type ComplianceDecision struct {
	ID                  string
	ProductID           string
	Status              DecisionStatus
	ReasonCode          string
	RuleVersion         string
	EvaluatedAt         time.Time
	RequiresHumanReview bool
}
