package identity_test

import (
	"errors"
	"testing"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/identity"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

func TestPathIdentityStrategy(t *testing.T) {
	strategy := identity.NewPathIdentityStrategy("item.merchant_code")

	t.Run("ResolvesNestedField", func(t *testing.T) {
		record := []byte(`{"item": {"merchant_code": "LG-65-001", "name": "TV"}}`)
		id, err := strategy.Resolve(record)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != "LG-65-001" {
			t.Errorf("expected LG-65-001, got %q", id)
		}
	})

	t.Run("LosslessNumericID", func(t *testing.T) {
		numStrat := identity.NewPathIdentityStrategy("id")
		record := []byte(`{"id": 987654321012345678, "title": "Large ID"}`)
		id, err := numStrat.Resolve(record)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != "987654321012345678" {
			t.Errorf("expected 987654321012345678, got %q", id)
		}
	})

	t.Run("MissingFieldReturnsErrIdentityNotFound", func(t *testing.T) {
		record := []byte(`{"item": {"name": "TV"}}`)
		_, err := strategy.Resolve(record)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, ingestion.ErrIdentityNotFound) {
			t.Errorf("expected ErrIdentityNotFound, got %v", err)
		}
	})
}

func TestCompositeIdentityStrategy(t *testing.T) {
	strategy, err := identity.NewCompositeIdentityStrategy([]string{"warehouse_id", "item.part_number"}, ":")
	if err != nil {
		t.Fatalf("unexpected init error: %v", err)
	}

	t.Run("ResolvesCompositeFields", func(t *testing.T) {
		record := []byte(`{"warehouse_id": "UK01", "item": {"part_number": "LG-65-C4"}}`)
		id, err := strategy.Resolve(record)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != "UK01:LG-65-C4" {
			t.Errorf("expected UK01:LG-65-C4, got %q", id)
		}
	})

	t.Run("MissingOneFieldReturnsErrIdentityNotFound", func(t *testing.T) {
		record := []byte(`{"warehouse_id": "UK01", "item": {}}`)
		_, err := strategy.Resolve(record)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, ingestion.ErrIdentityNotFound) {
			t.Errorf("expected ErrIdentityNotFound, got %v", err)
		}
	})
}
