package sources

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/auth"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/extraction"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/identity"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/pagination"
	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/policy"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/source"
)

const (
	defaultMaxResponseBodyBytes = 16 * 1024 * 1024 // 16 MiB
	drainLimitBytes             = 64 * 1024        // 64 KiB
)

// HTTPClient allows injecting custom *http.Client or mock transports.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// RESTAdapterConfig specifies the components required to assemble a RESTAdapter.
type RESTAdapterConfig struct {
	SourceID     source.ID
	BaseURL      string
	Client       HTTPClient
	Auth         auth.AuthStrategy
	Pagination   pagination.PaginationStrategy
	Extractor    extraction.RecordExtractor
	Identity     identity.IdentityStrategy
	RateLimiter  RateLimiter
	ErrorTracker *policy.ErrorTracker
	MaxBodyBytes int64
}

// RESTAdapter is a composed HTTP source adapter. It coordinates transport, authentication, 
// rate limiting, pagination, record extraction, and identity resolution without 
// embedding supplier-specific product schemas.
type RESTAdapter struct {
	sourceID     source.ID
	baseURL      string
	client       HTTPClient
	auth         auth.AuthStrategy
	pagination   pagination.PaginationStrategy
	extractor    extraction.RecordExtractor
	identity     identity.IdentityStrategy
	limiter      RateLimiter
	errorTracker *policy.ErrorTracker
	maxBodyBytes int64
}

var _ ingestion.ProbingAdapter = (*RESTAdapter)(nil)

// NewRESTAdapter constructs and validates a RESTAdapter.
func NewRESTAdapter(cfg RESTAdapterConfig) (*RESTAdapter, error) {
	if strings.TrimSpace(string(cfg.SourceID)) == "" {
		return nil, fmt.Errorf("%w: source id cannot be empty", ingestion.ErrInvalidSourceID)
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, fmt.Errorf("%w: base url cannot be empty", ingestion.ErrSourceContractViolation)
	}

	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	authStrat := cfg.Auth
	if authStrat == nil {
		authStrat = &auth.NoAuth{}
	}

	extractor := cfg.Extractor
	if extractor == nil {
		extractor = extraction.NewPathRecordExtractor("")
	}

	idStrat := cfg.Identity
	if idStrat == nil {
		idStrat = identity.NewPathIdentityStrategy("id")
	}

	tracker := cfg.ErrorTracker
	if tracker == nil {
		tracker = policy.NewErrorTracker(policy.PolicyFailFast, 0.05, nil)
	}

	maxBytes := cfg.MaxBodyBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxResponseBodyBytes
	}

	return &RESTAdapter{
		sourceID:     cfg.SourceID,
		baseURL:      strings.TrimSpace(cfg.BaseURL),
		client:       client,
		auth:         authStrat,
		pagination:   cfg.Pagination,
		extractor:    extractor,
		identity:     idStrat,
		limiter:      cfg.RateLimiter,
		errorTracker: tracker,
		maxBodyBytes: maxBytes,
	}, nil
}

// FetchRecords executes a single paginated HTTP fetch against the source endpoint.
func (a *RESTAdapter) FetchRecords(ctx context.Context, checkpoint string) ([]*ingestion.RawRecord, string, error) {
	if a.limiter != nil {
		if err := a.limiter.Wait(ctx); err != nil {
			return nil, "", err
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.baseURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("create http request: %w", err)
	}

	if a.pagination != nil {
		if err := a.pagination.ApplyPagination(req, checkpoint); err != nil {
			return nil, "", err
		}
	}

	if a.auth != nil {
		if err := a.auth.ApplyAuth(req); err != nil {
			return nil, "", fmt.Errorf("apply auth: %w", err)
		}
	}

	req.Header.Set("Accept", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("%w: http request failed: %v", ingestion.ErrAdapterUnavailable, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimitBytes))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, "", fmt.Errorf("%w: status %d from %s", ingestion.ErrAuthenticationFailed, resp.StatusCode, a.baseURL)
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, "", fmt.Errorf("%w: rate limited by upstream server %s", ingestion.ErrRateLimitExceeded, a.baseURL)
	case resp.StatusCode >= 500:
		return nil, "", fmt.Errorf("%w: upstream server error %d from %s", ingestion.ErrAdapterUnavailable, resp.StatusCode, a.baseURL)
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return nil, "", fmt.Errorf("%w: unexpected http status %d from %s", ingestion.ErrSourceContractViolation, resp.StatusCode, a.baseURL)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, a.maxBodyBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("%w: read response body: %v", ingestion.ErrAdapterUnavailable, err)
	}
	if int64(len(body)) > a.maxBodyBytes {
		return nil, "", fmt.Errorf("%w: response body exceeds %d bytes", ingestion.ErrAdapterUnavailable, a.maxBodyBytes)
	}

	rawItems, err := a.extractor.ExtractRecords(body)
	if err != nil {
		// Contract violations (path not found, wrong type, etc.) fail the run
		return nil, "", fmt.Errorf("%w: extract records: %w", ingestion.ErrSourceContractViolation, err)
	}

	etag := strings.Trim(resp.Header.Get("ETag"), "\"")
	records := make([]*ingestion.RawRecord, 0, len(rawItems))

	for i, rawItem := range rawItems {
		extID, idErr := a.identity.Resolve(rawItem)
		if idErr != nil {
			recErr := a.errorTracker.RecordError(i+1, rawItem, idErr)
			if recErr != nil {
				return nil, "", recErr
			}
			continue
		}

		rec, recInitErr := ingestion.NewRawRecord(
			"",
			a.sourceID,
			extID,
			rawItem,
			"",
			etag,
			nil,
			"",
			time.Now().UTC(),
		)
		if recInitErr != nil {
			recErr := a.errorTracker.RecordError(i+1, rawItem, recInitErr)
			if recErr != nil {
				return nil, "", recErr
			}
			continue
		}

		records = append(records, rec)
		a.errorTracker.RecordSuccess()
	}

	var nextCheckpoint string
	if a.pagination != nil {
		nextCP, pagErr := a.pagination.ExtractNextCheckpoint(resp, body, checkpoint, len(records))
		if pagErr != nil {
			return nil, "", fmt.Errorf("%w: extract next checkpoint: %w", ingestion.ErrSourceContractViolation, pagErr)
		}
		nextCheckpoint = nextCP
	}

	return records, nextCheckpoint, nil
}

// Fetch provides the expressive, typed FetchRequest/FetchResult contract.
func (a *RESTAdapter) Fetch(ctx context.Context, req ingestion.FetchRequest) (ingestion.FetchResult, error) {
	return fetchWithRecords(ctx, req, a.FetchRecords)
}

// Probe executes a pre-flight dry-run diagnostic query to verify reachability, auth, and schema extraction.
func (a *RESTAdapter) Probe(ctx context.Context) (*ingestion.SourceProbeResult, error) {
	return probeWithFetch(ctx, a)
}
