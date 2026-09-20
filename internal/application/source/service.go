package source

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type Service struct {
	repo source.Repository
}

func NewService(repo source.Repository) (*Service, error) {
	if repo == nil {
		return nil, errors.New("source repository is required")
	}
	return &Service{repo: repo}, nil
}

type CreateSourceParams struct {
	ID        source.ID
	Name      string
	Type      source.Type
	Config    map[string]any
	RateLimit int
}

func (s *Service) CreateSource(ctx context.Context, params CreateSourceParams) (*source.Source, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	src, err := source.NewSource(
		params.ID,
		strings.TrimSpace(params.Name),
		params.Type,
		params.Config,
		params.RateLimit,
	)
	if err != nil {
		return nil, fmt.Errorf("create source entity: %w", err)
	}

	if err := s.repo.Save(ctx, src); err != nil {
		return nil, fmt.Errorf("save source: %w", err)
	}

	return src, nil
}

func (s *Service) GetSource(ctx context.Context, id source.ID) (*source.Source, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if strings.TrimSpace(string(id)) == "" {
		return nil, source.ErrInvalidSourceID
	}

	src, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get source: %w", err)
	}

	return src, nil
}

func (s *Service) ListSources(ctx context.Context) ([]*source.Source, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	sources, err := s.repo.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list sources: %w", err)
	}

	return sources, nil
}

func (s *Service) ListActiveSources(ctx context.Context) ([]*source.Source, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	sources, err := s.repo.FindActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("list active sources: %w", err)
	}

	return sources, nil
}

func (s *Service) UpdateRateLimit(ctx context.Context, id source.ID, rateLimit int) (*source.Source, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	src, err := s.GetSource(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := src.SetRateLimit(rateLimit); err != nil {
		return nil, err
	}

	if err := s.repo.Save(ctx, src); err != nil {
		return nil, fmt.Errorf("save updated rate limit: %w", err)
	}

	return src, nil
}

func (s *Service) SetEnabled(ctx context.Context, id source.ID, enabled bool) (*source.Source, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	src, err := s.GetSource(ctx, id)
	if err != nil {
		return nil, err
	}

	if enabled {
		src.Enable()
	} else {
		src.Disable()
	}

	if err := s.repo.Save(ctx, src); err != nil {
		return nil, fmt.Errorf("save source state: %w", err)
	}

	return src, nil
}

func (s *Service) DeleteSource(ctx context.Context, id source.ID) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if strings.TrimSpace(string(id)) == "" {
		return source.ErrInvalidSourceID
	}

	if err := s.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete source: %w", err)
	}

	return nil
}
