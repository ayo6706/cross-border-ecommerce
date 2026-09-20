package compliance

import (
	"errors"
)

var (
	ErrHSCodeNotFound        = errors.New("hs code not found")
	ErrNoEffectiveTariff     = errors.New("no effective tariff rate for date")
	ErrSanctionRuleViolation = errors.New("sanctions rule violation")
)
