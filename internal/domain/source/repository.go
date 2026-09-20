package source

import (
	"context"
)

type Repository interface {
	FindByID(ctx context.Context, id ID) (*Source, error)
	FindActive(ctx context.Context) ([]*Source, error)
	List(ctx context.Context) ([]*Source, error)
	Save(ctx context.Context, s *Source) error
	Delete(ctx context.Context, id ID) error
}
