package product

import (
	"context"
)

type ListParams struct {
	LastCreatedAt *string
	LastID        *ID
	Limit         int
}

type Repository interface {
	FindByID(ctx context.Context, id ID) (*Product, error)
	FindByFingerprint(ctx context.Context, fingerprint string) (*Product, error)
	Save(ctx context.Context, p *Product) error
	List(ctx context.Context, params ListParams) ([]*Product, error)
}
