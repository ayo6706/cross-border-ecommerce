package product

import (
	"context"
	"time"
)

type ListParams struct {
	LastCreatedAt *time.Time
	LastID        *ID
	Limit         int
}

type Repository interface {
	FindByID(ctx context.Context, id ID) (*Product, error)
	Save(ctx context.Context, p *Product) error
	List(ctx context.Context, params ListParams) ([]*Product, error)
}
