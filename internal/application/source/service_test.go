package source_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	appsource "github.com/ayo6706/cross-border-ecommerce/internal/application/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type memorySourceRepo struct {
	mu      sync.RWMutex
	sources map[source.ID]*source.Source
}

func newMemorySourceRepo() *memorySourceRepo {
	return &memorySourceRepo{
		sources: make(map[source.ID]*source.Source),
	}
}

func (m *memorySourceRepo) FindByID(_ context.Context, id source.ID) (*source.Source, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, ok := m.sources[id]
	if !ok {
		return nil, source.ErrSourceNotFound
	}
	cp := *s
	return &cp, nil
}

func (m *memorySourceRepo) FindActive(_ context.Context) ([]*source.Source, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]*source.Source, 0, len(m.sources))
	for _, s := range m.sources {
		if s.Enabled {
			cp := *s
			res = append(res, &cp)
		}
	}
	slices.SortFunc(res, func(a, b *source.Source) int {
		return strings.Compare(string(a.ID), string(b.ID))
	})
	return res, nil
}

func (m *memorySourceRepo) List(_ context.Context) ([]*source.Source, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	res := make([]*source.Source, 0, len(m.sources))
	for _, s := range m.sources {
		cp := *s
		res = append(res, &cp)
	}
	slices.SortFunc(res, func(a, b *source.Source) int {
		return strings.Compare(string(a.ID), string(b.ID))
	})
	return res, nil
}

func (m *memorySourceRepo) Save(_ context.Context, s *source.Source) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := *s
	m.sources[s.ID] = &cp
	return nil
}

func (m *memorySourceRepo) Delete(_ context.Context, id source.ID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.sources, id)
	return nil
}

func TestSourceService_CRUD(t *testing.T) {
	t.Parallel()

	repo := newMemorySourceRepo()
	svc, err := appsource.NewService(repo)
	if err != nil {
		t.Fatalf("failed to create source service: %v", err)
	}

	ctx := context.Background()

	// 1. Create Source
	src, err := svc.CreateSource(ctx, appsource.CreateSourceParams{
		ID:        "src-api-01",
		Name:      "Supplier API 1",
		Type:      source.TypeAPI,
		Config:    map[string]any{"url": "https://api.supplier.com"},
		RateLimit: 50,
	})
	if err != nil {
		t.Fatalf("failed to create source: %v", err)
	}
	if src.ID != "src-api-01" || !src.Enabled || src.RateLimit != 50 {
		t.Fatalf("unexpected source created: %+v", src)
	}

	// 2. Get Source
	fetched, err := svc.GetSource(ctx, "src-api-01")
	if err != nil {
		t.Fatalf("failed to get source: %v", err)
	}
	if fetched.Name != "Supplier API 1" {
		t.Fatalf("expected name Supplier API 1, got %s", fetched.Name)
	}

	// 3. Update Rate Limit
	updated, err := svc.UpdateRateLimit(ctx, "src-api-01", 200)
	if err != nil {
		t.Fatalf("failed to update rate limit: %v", err)
	}
	if updated.RateLimit != 200 {
		t.Fatalf("expected rate limit 200, got %d", updated.RateLimit)
	}

	// Invalid rate limit rejected
	_, err = svc.UpdateRateLimit(ctx, "src-api-01", 0)
	if !errors.Is(err, source.ErrInvalidRateLimit) {
		t.Fatalf("expected ErrInvalidRateLimit, got %v", err)
	}

	// 4. Disable and Enable
	disabled, err := svc.SetEnabled(ctx, "src-api-01", false)
	if err != nil {
		t.Fatalf("failed to disable source: %v", err)
	}
	if disabled.Enabled {
		t.Fatalf("expected source to be disabled")
	}

	activeList, err := svc.ListActiveSources(ctx)
	if err != nil {
		t.Fatalf("failed to list active sources: %v", err)
	}
	if len(activeList) != 0 {
		t.Fatalf("expected 0 active sources, got %d", len(activeList))
	}

	allList, err := svc.ListSources(ctx)
	if err != nil {
		t.Fatalf("failed to list all sources: %v", err)
	}
	if len(allList) != 1 {
		t.Fatalf("expected 1 total source, got %d", len(allList))
	}

	// 5. Delete Source
	if err := svc.DeleteSource(ctx, "src-api-01"); err != nil {
		t.Fatalf("failed to delete source: %v", err)
	}

	_, err = svc.GetSource(ctx, "src-api-01")
	if !errors.Is(err, source.ErrSourceNotFound) {
		t.Fatalf("expected ErrSourceNotFound, got %v", err)
	}
}

func TestSourceService_ConstructorValidation(t *testing.T) {
	t.Parallel()

	_, err := appsource.NewService(nil)
	if err == nil {
		t.Fatalf("expected error on nil repository")
	}
}
