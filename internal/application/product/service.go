package product

import (
	"context"
	"errors"
	"fmt"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

// Service is the catalogue read side behind GET /v1/products and GET /v1/products/{id}.
type Service struct {
	repo product.Repository
}

func NewService(repo product.Repository) (*Service, error) {
	if repo == nil {
		return nil, errors.New("product repository is required")
	}
	return &Service{repo: repo}, nil
}

func (s *Service) GetProductByID(ctx context.Context, id product.ID) (*product.Product, error) {
	p, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get product %q: %w", id, err)
	}
	return p, nil
}

func (s *Service) ListProducts(ctx context.Context, params product.ListParams) (product.Page, error) {
	page, err := s.repo.List(ctx, params)
	if err != nil {
		return product.Page{}, fmt.Errorf("list products: %w", err)
	}
	return page, nil
}
