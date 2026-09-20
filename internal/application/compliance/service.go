package compliance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/compliance"
)

type Service struct {
	repo compliance.Repository
}

func NewService(repo compliance.Repository) (*Service, error) {
	if repo == nil {
		return nil, errors.New("compliance repository is required")
	}
	return &Service{repo: repo}, nil
}

type EvaluateParams struct {
	ProductID       string
	HSCode          string
	CountryCode     string
	Manufacturer    string
	TransactionDate time.Time
}

func (s *Service) Evaluate(ctx context.Context, params EvaluateParams) (*compliance.ComplianceDecision, error) {
	if strings.TrimSpace(params.ProductID) == "" {
		return nil, errors.New("product id is required")
	}

	sanctioned, err := s.repo.IsSanctioned(ctx, params.CountryCode, params.Manufacturer)
	if err != nil {
		return nil, fmt.Errorf("compliance sanction check failed: %w", err)
	}

	evalDate := params.TransactionDate
	if evalDate.IsZero() {
		evalDate = time.Now().UTC()
	}

	if sanctioned {
		decision := &compliance.ComplianceDecision{
			ID:                  fmt.Sprintf("dec_%s_%d", params.ProductID, evalDate.Unix()),
			ProductID:           params.ProductID,
			Status:              compliance.DecisionBlock,
			ReasonCode:          "SANCTIONED_ORIGIN_OR_ENTITY",
			RuleVersion:         "2026.1",
			EvaluatedAt:         evalDate,
			RequiresHumanReview: false,
		}
		if err := s.repo.RecordDecision(ctx, decision); err != nil {
			return nil, fmt.Errorf("compliance record sanction decision: %w", err)
		}
		return decision, nil
	}

	decision := &compliance.ComplianceDecision{
		ID:                  fmt.Sprintf("dec_%s_%d", params.ProductID, evalDate.Unix()),
		ProductID:           params.ProductID,
		Status:              compliance.DecisionAllow,
		ReasonCode:          "STANDARD_CLEARANCE",
		RuleVersion:         "2026.1",
		EvaluatedAt:         evalDate,
		RequiresHumanReview: false,
	}

	if err := s.repo.RecordDecision(ctx, decision); err != nil {
		return nil, fmt.Errorf("compliance record clearance decision: %w", err)
	}

	return decision, nil
}
