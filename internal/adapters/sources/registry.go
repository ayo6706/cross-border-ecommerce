package sources

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/auth"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/extraction"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/identity"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/pagination"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

type Registry struct {
	mu       sync.RWMutex
	adapters map[source.ID]ingestion.Adapter
}

func NewRegistry() *Registry {
	return &Registry{
		adapters: make(map[source.ID]ingestion.Adapter),
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
	r.adapters[sourceID] = adapter
	return nil
}

func (r *Registry) Resolve(src *source.Source) (ingestion.Adapter, error) {
	if src == nil {
		return nil, source.ErrInvalidSourceState
	}

	r.mu.RLock()
	adapter, exists := r.adapters[src.ID]
	r.mu.RUnlock()
	if exists {
		if isNilAdapter(adapter) {
			return nil, errors.New("registered adapter is nil")
		}
		return adapter, nil
	}

	return r.buildDefaultAdapter(src)
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

func (r *Registry) buildDefaultAdapter(src *source.Source) (ingestion.Adapter, error) {
	switch src.Type {
	case source.TypeAPI:
		return r.buildAPIAdapter(src)
	case source.TypeFeed, source.TypeFile:
		return r.buildFeedAdapter(src)
	default:
		return nil, fmt.Errorf("%w: %s", source.ErrInvalidSourceType, src.Type)
	}
}

func (r *Registry) buildAPIAdapter(src *source.Source) (ingestion.Adapter, error) {
	baseURL, _ := src.Config["base_url"].(string)
	if strings.TrimSpace(baseURL) == "" {
		return nil, errors.New("missing 'base_url' in api source config")
	}

	var limiter RateLimiter
	if src.RateLimit > 0 {
		var err error
		limiter, err = NewTokenBucketLimiter(src.RateLimit, src.RateLimit)
		if err != nil {
			return nil, fmt.Errorf("create rate limiter: %w", err)
		}
	}

	var authStrat auth.AuthStrategy = &auth.NoAuth{}
	if token, ok := src.Config["bearer_token"].(string); ok && strings.TrimSpace(token) != "" {
		authStrat = auth.NewBearerAuth(strings.TrimSpace(token))
	} else if keyVal, ok := src.Config["api_key_value"].(string); ok && strings.TrimSpace(keyVal) != "" {
		keyName, _ := src.Config["api_key_name"].(string)
		authStrat = auth.NewAPIKeyHeaderAuth(keyVal, keyName)
	} else if user, ok := src.Config["username"].(string); ok && strings.TrimSpace(user) != "" {
		pass, _ := src.Config["password"].(string)
		authStrat = auth.NewBasicAuth(user, pass)
	}

	recordsPath, _ := src.Config["records_path"].(string)
	extractor := extraction.NewPathRecordExtractor(recordsPath)

	var idStrat identity.IdentityStrategy
	compositeIDs := parseConfigStringSlice(src.Config["composite_ids"])
	if len(compositeIDs) > 0 {
		compositeSep, _ := src.Config["composite_sep"].(string)
		if compositeSep == "" {
			compositeSep = ":"
		}
		comp, err := identity.NewCompositeIdentityStrategy(compositeIDs, compositeSep)
		if err != nil {
			return nil, fmt.Errorf("create composite identity: %w", err)
		}
		idStrat = comp
	} else {
		idField, _ := src.Config["id_field"].(string)
		if idField == "" {
			idField, _ = src.Config["id_path"].(string)
		}
		if idField == "" {
			idField = "id"
		}
		idStrat = identity.NewPathIdentityStrategy(idField)
	}

	var pagStrat pagination.PaginationStrategy
	cursorPath, _ := src.Config["cursor_path"].(string)
	cursorParam, _ := src.Config["cursor_param"].(string)
	pageParam, _ := src.Config["page_param"].(string)
	if pageParam != "" {
		limitParam, _ := src.Config["limit_param"].(string)
		pageSize := 100
		if ps, ok := src.Config["page_size"]; ok {
			pageSize = parseConfigInt(ps, 100)
		}
		pagStrat = pagination.NewPagePagination(pageParam, limitParam, pageSize)
	} else if cursorParam != "" || cursorPath != "" {
		if cursorParam == "" {
			cursorParam = "cursor"
		}
		pagStrat = pagination.NewCursorPagination(cursorParam, cursorPath)
	}

	return NewRESTAdapter(RESTAdapterConfig{
		SourceID:    src.ID,
		BaseURL:     baseURL,
		Auth:        authStrat,
		Pagination:  pagStrat,
		Extractor:   extractor,
		Identity:    idStrat,
		RateLimiter: limiter,
	})
}

func (r *Registry) buildFeedAdapter(src *source.Source) (ingestion.Adapter, error) {
	filePath, _ := src.Config["file_path"].(string)
	formatStr, _ := src.Config["format"].(string)
	rawFormat := strings.TrimSpace(formatStr)
	var format FeedFormat
	switch {
	case rawFormat == "" || strings.EqualFold(rawFormat, string(FeedFormatNDJSON)):
		format = FeedFormatNDJSON
	case strings.EqualFold(rawFormat, string(FeedFormatCSV)):
		format = FeedFormatCSV
	default:
		return nil, fmt.Errorf("unsupported feed format: %s", rawFormat)
	}

	batchSize := 500
	if bs, ok := src.Config["batch_size"]; ok {
		batchSize = parseConfigInt(bs, 500)
	}

	idField, _ := src.Config["id_field"].(string)
	compositeIDs := parseConfigStringSlice(src.Config["composite_ids"])
	compositeSep, _ := src.Config["composite_sep"].(string)
	skipMalformed := true
	if sm, ok := src.Config["skip_malformed"].(bool); ok {
		skipMalformed = sm
	}

	return NewFeedFileAdapter(FeedFileConfig{
		SourceID:      src.ID,
		Format:        format,
		FilePath:      filePath,
		BatchSize:     batchSize,
		IDField:       idField,
		CompositeIDs:  compositeIDs,
		CompositeSep:  compositeSep,
		SkipMalformed: skipMalformed,
	})
}

func parseConfigStringSlice(val any) []string {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []string:
		return v
	case []any:
		res := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				res = append(res, strings.TrimSpace(s))
			}
		}
		return res
	default:
		return nil
	}
}

func parseConfigInt(val any, defaultVal int) int {
	switch v := val.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return defaultVal
}
