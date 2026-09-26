package product_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	productApp "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

type mockProductRepository struct {
	mu       sync.RWMutex
	products map[product.ID]*product.Product
}

func newMockProductRepository() *mockProductRepository {
	return &mockProductRepository{
		products: make(map[product.ID]*product.Product),
	}
}

func (m *mockProductRepository) FindByID(ctx context.Context, id product.ID) (*product.Product, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.products[id]
	if !ok {
		return nil, product.ErrProductNotFound
	}
	cp := *p
	return &cp, nil
}

func (m *mockProductRepository) Save(ctx context.Context, p *product.Product) error {
	if p == nil {
		return product.ErrInvalidProductState
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *p
	m.products[p.ID] = &cp
	return nil
}

func (m *mockProductRepository) List(ctx context.Context, params product.ListParams) ([]*product.Product, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]*product.Product, 0, len(m.products))
	for _, p := range m.products {
		cp := *p
		res = append(res, &cp)
	}
	return res, nil
}

func (m *mockProductRepository) FindSnapshotsByIdentities(ctx context.Context, identities []product.IdentityRef) (map[string]*product.Snapshot, error) {
	return make(map[string]*product.Snapshot), nil
}

func (m *mockProductRepository) ApplyBatch(ctx context.Context, plan *product.BatchPlan) error {
	if plan == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range plan.ProductsToInsert {
		cp := *p
		m.products[p.ID] = &cp
	}
	return nil
}

func TestProductService_Constructor(t *testing.T) {
	t.Parallel()

	svc, err := productApp.NewService(nil)
	if err == nil {
		t.Fatal("expected error when repo is nil, got nil")
	}
	if svc != nil {
		t.Fatalf("expected nil service, got %v", svc)
	}

	repo := newMockProductRepository()
	svc, err = productApp.NewService(repo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil service")
	}
}

func TestProductService_CreateAndGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newMockProductRepository()
	svc, err := productApp.NewService(repo)
	if err != nil {
		t.Fatalf("failed to initialize service: %v", err)
	}

	tests := []struct {
		name      string
		params    productApp.CreateProductParams
		expectErr bool
		targetErr error
	}{
		{
			name: "valid product registration",
			params: productApp.CreateProductParams{
				ID:            product.ID("prod-101"),
				CanonicalName: "Sony WH-1000XM5 Headphones",
				Description:   "Noise Cancelling Headphones",
				Brand:         "Sony",
				OriginCountry: "jp",
			},
			expectErr: false,
		},
		{
			name: "empty canonical name returns validation error",
			params: productApp.CreateProductParams{
				ID:            product.ID("prod-102"),
				CanonicalName: "   ",
				Brand:         "Sony",
			},
			expectErr: true,
			targetErr: product.ErrEmptyCanonicalName,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			created, err := svc.CreateProduct(ctx, tc.params)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.targetErr != nil && !errors.Is(err, tc.targetErr) {
					t.Fatalf("expected error %v, got %v", tc.targetErr, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error creating product: %v", err)
			}
			if created.CanonicalName != tc.params.CanonicalName {
				t.Errorf("expected canonical name %q, got %q", tc.params.CanonicalName, created.CanonicalName)
			}
			if created.OriginCountry != "JP" {
				t.Errorf("expected uppercase country 'JP', got %q", created.OriginCountry)
			}
			if created.Status != product.StatusDraft {
				t.Errorf("expected draft status, got %q", created.Status)
			}

			// Verify retrieval
			fetched, err := svc.GetProductByID(ctx, created.ID)
			if err != nil {
				t.Fatalf("failed to get product by id: %v", err)
			}
			if fetched.ID != created.ID {
				t.Errorf("expected id %v, got %v", created.ID, fetched.ID)
			}
		})
	}
}

func TestProductService_GetProductByID_NotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := newMockProductRepository()
	svc, _ := productApp.NewService(repo)

	_, err := svc.GetProductByID(ctx, product.ID("non-existent"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, product.ErrProductNotFound) {
		t.Fatalf("expected ErrProductNotFound wrapped, got %v", err)
	}
}
