package product

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

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
	if strings.TrimSpace(string(id)) == "" {
		return nil, errors.New("product id cannot be empty")
	}
	p, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("service find product by id: %w", err)
	}
	return p, nil
}

type CreateProductParams struct {
	ID            product.ID
	CanonicalName string
	Description   string
	Brand         string
	OriginCountry string
}

func (s *Service) CreateProduct(ctx context.Context, params CreateProductParams) (*product.Product, error) {
	p, err := product.NewProduct(params.ID, params.CanonicalName, params.Description, params.Brand, params.OriginCountry)
	if err != nil {
		return nil, fmt.Errorf("create product: %w", err)
	}

	if err := s.repo.Save(ctx, p); err != nil {
		return nil, fmt.Errorf("service save product: %w", err)
	}

	return p, nil
}
