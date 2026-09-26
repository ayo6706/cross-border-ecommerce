package sources

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/auth"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/extraction"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/identity"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/pagination"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/policy"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/secrets"
)

type cachedAdapter struct {
	adapter   ingestion.Adapter
	updatedAt time.Time
}

type sourceLimiter struct {
	rate    int
	limiter RateLimiter
}

type Registry struct {
	mu             sync.RWMutex
	adapters       map[source.ID]cachedAdapter
	limiters       map[source.ID]sourceLimiter
	secretResolver source.SecretResolver
}

func NewRegistry(opts ...RegistryOption) *Registry {
	r := &Registry{
		adapters:       make(map[source.ID]cachedAdapter),
		limiters:       make(map[source.ID]sourceLimiter),
		secretResolver: secrets.NewEnvSecretResolver(),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

type RegistryOption func(*Registry)

func WithSecretResolver(resolver source.SecretResolver) RegistryOption {
	return func(r *Registry) {
		if resolver != nil {
			r.secretResolver = resolver
		}
	}
}

func (r *Registry) Register(sourceID source.ID, adapter ingestion.Adapter) error {
	if strings.TrimSpace(string(sourceID)) == "" {
		return ingestion.ErrInvalidSourceID
	}
	if isNilAdapter(adapter) {
		return errors.New("adapter cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[sourceID] = cachedAdapter{
		adapter:   adapter,
		updatedAt: time.Time{},
	}
	return nil
}

func (r *Registry) Resolve(ctx context.Context, src *source.Source) (ingestion.Adapter, error) {
	if src == nil {
		return nil, source.ErrInvalidSourceState
	}

	r.mu.RLock()
	entry, exists := r.adapters[src.ID]
	r.mu.RUnlock()

	if exists {
		// If explicitly registered (zero UpdatedAt) or matches source UpdatedAt timestamp
		if entry.updatedAt.IsZero() || entry.updatedAt.Equal(src.UpdatedAt) {
			if isNilAdapter(entry.adapter) {
				return nil, errors.New("registered adapter is nil")
			}
			return entry.adapter, nil
		}
	}

	adapter, err := r.buildDefaultAdapter(ctx, src)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.adapters[src.ID] = cachedAdapter{
		adapter:   adapter,
		updatedAt: src.UpdatedAt,
	}
	r.mu.Unlock()

	return adapter, nil
}

func isNilAdapter(a ingestion.Adapter) bool {
	if a == nil {
		return true
	}
	val := reflect.ValueOf(a)
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return val.IsNil()
	default:
		return false
	}
}

func (r *Registry) buildDefaultAdapter(ctx context.Context, src *source.Source) (ingestion.Adapter, error) {
	switch src.Type {
	case source.TypeAPI:
		return r.buildAPIAdapter(ctx, src)
	case source.TypeFeed, source.TypeFile:
		return r.buildFeedAdapter(src)
	default:
		return nil, fmt.Errorf("%w: %s", source.ErrInvalidSourceType, src.Type)
	}
}

//nolint:funlen // legacy baseline 2026-09-26: fix in ENG-049
func (r *Registry) buildAPIAdapter(ctx context.Context, src *source.Source) (ingestion.Adapter, error) {
	apiCfg, err := src.ParseAPIConfig()
	if err != nil {
		return nil, fmt.Errorf("parse api source config: %w", err)
	}

	limiter, err := r.limiterFor(src)
	if err != nil {
		return nil, err
	}

	authStrat, err := r.buildAuth(ctx, apiCfg)
	if err != nil {
		return nil, err
	}

	extractor := extraction.NewPathRecordExtractor(apiCfg.RecordsPath)

	var idStrat identity.IdentityStrategy
	if len(apiCfg.CompositeIDs) > 0 {
		sep := apiCfg.CompositeSep
		if sep == "" {
			sep = ":"
		}
		comp, err := identity.NewCompositeIdentityStrategy(apiCfg.CompositeIDs, sep)
		if err != nil {
			return nil, fmt.Errorf("create composite identity: %w", err)
		}
		idStrat = comp
	} else {
		idField := apiCfg.IDField
		if idField == "" {
			idField = "id"
		}
		idStrat = identity.NewPathIdentityStrategy(idField)
	}

	var pagStrat pagination.PaginationStrategy
	switch apiCfg.Pagination.Kind {
	case "page":
		pageSize := apiCfg.Pagination.PageSize
		if pageSize <= 0 {
			pageSize = 100
		}
		pagStrat = pagination.NewPagePagination(apiCfg.Pagination.PageParam, apiCfg.Pagination.PageSizeParam, pageSize)
	case "cursor":
		cursorParam := apiCfg.Pagination.CursorParam
		if cursorParam == "" {
			cursorParam = "cursor"
		}
		pagStrat = pagination.NewCursorPagination(cursorParam, apiCfg.Pagination.CursorPath)
	}

	return NewRESTAdapter(RESTAdapterConfig{
		SourceID:    src.ID,
		BaseURL:     apiCfg.BaseURL,
		Auth:        authStrat,
		Pagination:  pagStrat,
		Extractor:   extractor,
		Identity:    idStrat,
		RateLimiter: limiter,
	})
}

func (r *Registry) buildFeedAdapter(src *source.Source) (ingestion.Adapter, error) {
	feedCfg, err := src.ParseFeedConfig()
	if err != nil {
		return nil, fmt.Errorf("parse feed source config: %w", err)
	}

	var format FeedFormat
	switch strings.ToUpper(feedCfg.Format) {
	case "CSV":
		format = FeedFormatCSV
	default:
		format = FeedFormatNDJSON
	}

	return NewFeedFileAdapter(FeedFileConfig{
		SourceID:     src.ID,
		Format:       format,
		FilePath:     feedCfg.FilePath,
		BatchSize:    feedCfg.BatchSize,
		IDField:      feedCfg.IDField,
		CompositeIDs: feedCfg.CompositeIDs,
		CompositeSep: feedCfg.CompositeSep,
		ErrorPolicy:  feedErrorPolicy(feedCfg.SkipMalformed),
	})
}

func feedErrorPolicy(skipMalformed bool) policy.ErrorPolicy {
	if skipMalformed {
		return policy.ErrorPolicy{Policy: policy.PolicySkipMalformed}
	}
	return policy.ErrorPolicy{Policy: policy.PolicyFailFast}
}

// buildAuth resolves the source's credential references into an auth strategy.
// The auth kind was validated by source.ParseAPIConfig.
func (r *Registry) buildAuth(ctx context.Context, cfg *source.APIConfig) (auth.AuthStrategy, error) {
	switch cfg.AuthKind {
	case source.AuthBearer:
		token, err := r.secretResolver.ResolveSecret(ctx, cfg.AuthRef)
		if err != nil {
			return nil, fmt.Errorf("resolve bearer token: %w", err)
		}
		return auth.NewBearerAuth(token), nil
	case source.AuthAPIKeyHeader:
		key, err := r.secretResolver.ResolveSecret(ctx, cfg.AuthRef)
		if err != nil {
			return nil, fmt.Errorf("resolve api key: %w", err)
		}
		return auth.NewAPIKeyHeaderAuth(key, cfg.AuthHeader), nil
	case source.AuthBasic:
		password, err := r.secretResolver.ResolveSecret(ctx, cfg.AuthPasswordRef)
		if err != nil {
			return nil, fmt.Errorf("resolve basic auth password: %w", err)
		}
		return auth.NewBasicAuth(cfg.AuthUser, password), nil
	default:
		return &auth.NoAuth{}, nil
	}
}

// limiterFor returns the source's shared rate limiter, replacing it when the
// configured rate changes so limiter state is never shared across rates.
func (r *Registry) limiterFor(src *source.Source) (RateLimiter, error) {
	if src.RateLimitPerSecond <= 0 {
		return nil, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if cached, ok := r.limiters[src.ID]; ok && cached.rate == src.RateLimitPerSecond {
		return cached.limiter, nil
	}

	limiter, err := NewTokenBucketLimiter(src.RateLimitPerSecond, src.RateLimitPerSecond)
	if err != nil {
		return nil, fmt.Errorf("create rate limiter: %w", err)
	}
	r.limiters[src.ID] = sourceLimiter{rate: src.RateLimitPerSecond, limiter: limiter}
	return limiter, nil
}
