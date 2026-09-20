package source

import (
	"context"
)

type Repository interface {
	FindByID(ctx context.Context, id ID) (*Source, error)
	FindActive(ctx context.Context) ([]*Source, error)
	Save(ctx context.Context, s *Source) error
}
