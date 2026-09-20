package postgres

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

var _ product.Repository = (*ProductRepository)(nil)

type ProductRepository struct {
	mu       sync.RWMutex
	products map[product.ID]*product.Product
	byFp     map[string]product.ID
}

func NewProductRepository() *ProductRepository {
	return &ProductRepository{
		products: make(map[product.ID]*product.Product),
		byFp:     make(map[string]product.ID),
	}
}

func (r *ProductRepository) FindByID(ctx context.Context, id product.ID) (*product.Product, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	p, ok := r.products[id]
	if !ok {
		return nil, product.ErrProductNotFound
	}

	copied := *p
	return &copied, nil
}

func (r *ProductRepository) FindByFingerprint(ctx context.Context, fingerprint string) (*product.Product, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	id, ok := r.byFp[fingerprint]
	if !ok {
		return nil, product.ErrProductNotFound
	}

	p := r.products[id]
	copied := *p
	return &copied, nil
}

func (r *ProductRepository) Save(ctx context.Context, p *product.Product) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if p == nil {
		return product.ErrInvalidProductState
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	copied := *p
	r.products[p.ID] = &copied
	if p.CurrentFingerprint != "" {
		r.byFp[p.CurrentFingerprint] = p.ID
	}

	return nil
}

func (r *ProductRepository) List(ctx context.Context, params product.ListParams) ([]*product.Product, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	limit := params.Limit
	if limit <= 0 {
		limit = 50
	}

	result := make([]*product.Product, 0, len(r.products))
	for _, p := range r.products {
		copied := *p
		result = append(result, &copied)
	}

	slices.SortFunc(result, func(a, b *product.Product) int {
		return cmp.Compare(a.ID, b.ID)
	})

	if len(result) > limit {
		result = result[:limit]
	}

	return result, nil
}
