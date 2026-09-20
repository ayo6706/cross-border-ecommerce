package compliance_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	complianceApp "github.com/ayo6706/cross-border-ecommerce/internal/application/compliance"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/compliance"
)

type mockComplianceRepo struct {
	mu         sync.Mutex
	sanctions  map[string]bool
	decisions  []*compliance.ComplianceDecision
}

func newMockComplianceRepo() *mockComplianceRepo {
	return &mockComplianceRepo{
		sanctions: map[string]bool{
			"KP": true, // North Korea
			"IR": true, // Iran
		},
	}
}

func (m *mockComplianceRepo) FindTariffRate(ctx context.Context, hsCode string, countryCode string, at time.Time) (*compliance.TariffRate, error) {
	return nil, nil
}

func (m *mockComplianceRepo) IsSanctioned(ctx context.Context, countryCode string, entityName string) (bool, error) {
	if m.sanctions[strings.ToUpper(countryCode)] {
		return true, nil
	}
	return false, nil
}

func (m *mockComplianceRepo) RecordDecision(ctx context.Context, decision *compliance.ComplianceDecision) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.decisions = append(m.decisions, decision)
	return nil
}

func TestComplianceService_Evaluate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newMockComplianceRepo()
	svc, err := complianceApp.NewService(repo)
	if err != nil {
		t.Fatalf("unexpected error creating service: %v", err)
	}

	t.Run("sanctioned country blocks shipment", func(t *testing.T) {
		res, err := svc.Evaluate(ctx, complianceApp.EvaluateParams{
			ProductID:       "prod-kp-001",
			CountryCode:     "KP",
			Manufacturer:    "State Minerals",
			TransactionDate: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Status != compliance.DecisionBlock {
			t.Errorf("expected status %v, got %v", compliance.DecisionBlock, res.Status)
		}
	})

	t.Run("non-sanctioned country allows standard clearance", func(t *testing.T) {
		res, err := svc.Evaluate(ctx, complianceApp.EvaluateParams{
			ProductID:       "prod-us-001",
			CountryCode:     "US",
			Manufacturer:    "TechCorp Inc",
			TransactionDate: time.Now().UTC(),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Status != compliance.DecisionAllow {
			t.Errorf("expected status %v, got %v", compliance.DecisionAllow, res.Status)
		}
	})
}
