package source

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

type ID string

type Type string

const (
	TypeUnknown Type = ""
	TypeAPI     Type = "API"
	TypeFile    Type = "FILE"
	TypeFeed    Type = "FEED"
	TypeScraper Type = "SCRAPER"
)

// PaginationConfig defines typed pagination options for API sources.
type PaginationConfig struct {
	Kind          string `json:"kind,omitempty"` // "cursor", "page", "link", "none"
	CursorParam   string `json:"cursor_param,omitempty"`
	CursorPath    string `json:"cursor_path,omitempty"`
	PageParam     string `json:"page_param,omitempty"`
	PageSizeParam string `json:"page_size_param,omitempty"`
	PageSize      int    `json:"page_size,omitempty"`
}

// APIConfig defines typed configuration for API sources.
type APIConfig struct {
	BaseURL         string                `json:"base_url"`
	RecordsPath     string                `json:"records_path,omitempty"`
	IDField         string                `json:"id_field,omitempty"`
	CompositeIDs    []string              `json:"composite_ids,omitempty"`
	CompositeSep    string                `json:"composite_sep,omitempty"`
	Pagination      PaginationConfig      `json:"pagination,omitempty"`
	AuthKind        string                `json:"auth_kind,omitempty"` // "none", "bearer", "api_key_header", "basic"
	AuthRef         string                `json:"auth_ref,omitempty"`  // e.g. "env:SUPPLIER_API_KEY"
	AuthHeader      string                `json:"auth_header,omitempty"`
	AuthUser        string                `json:"auth_user,omitempty"`
	AuthPasswordRef string                `json:"auth_password_ref,omitempty"`
	FieldMapping    *product.FieldMapping `json:"field_mapping,omitempty"`
}

// FeedConfig defines typed configuration for Feed/File sources.
type FeedConfig struct {
	FilePath      string                `json:"file_path"`
	Format        string                `json:"format"` // "CSV", "NDJSON"
	IDField       string                `json:"id_field,omitempty"`
	CompositeIDs  []string              `json:"composite_ids,omitempty"`
	CompositeSep  string                `json:"composite_sep,omitempty"`
	BatchSize     int                   `json:"batch_size,omitempty"`
	SkipMalformed bool                  `json:"skip_malformed,omitempty"`
	FieldMapping  *product.FieldMapping `json:"field_mapping,omitempty"`
}

type Source struct {
	ID                 ID
	Name               string
	Type               Type
	Config             map[string]any
	RateLimitPerSecond int
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Enabled            bool
}

// NewSource constructs and validates a new Source domain entity.
func NewSource(id ID, name string, srcType Type, config map[string]any, rateLimitPerSecond int) (*Source, error) {
	now := time.Now().UTC()
	if config == nil {
		config = make(map[string]any)
	}
	s := &Source{
		ID:                 id,
		Name:               strings.TrimSpace(name),
		Type:               srcType,
		Config:             config,
		RateLimitPerSecond: rateLimitPerSecond,
		Enabled:            true,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// Validate ensures domain invariants and configuration schema integrity.
func (s *Source) Validate() error {
	if s == nil {
		return ErrInvalidSourceState
	}
	if strings.TrimSpace(string(s.ID)) == "" {
		return ErrInvalidSourceID
	}
	if strings.TrimSpace(s.Name) == "" {
		return ErrInvalidSourceName
	}
	if s.RateLimitPerSecond <= 0 {
		return ErrInvalidRateLimit
	}

	switch s.Type {
	case TypeAPI:
		if _, err := s.ParseAPIConfig(); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidSourceConfig, err)
		}
	case TypeFile, TypeFeed:
		if _, err := s.ParseFeedConfig(); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidSourceConfig, err)
		}
	case TypeScraper:
		//TODO: Scraper configuration validation if needed
	default:
		return ErrInvalidSourceType
	}

	return nil
}

// Supported APIConfig.AuthKind values.
const (
	AuthNone         = "none"
	AuthBearer       = "bearer"
	AuthAPIKeyHeader = "api_key_header"
	AuthBasic        = "basic"
)

func (s *Source) ParseAPIConfig() (*APIConfig, error) {
	if s == nil {
		return nil, ErrInvalidSourceState
	}

	cfg := &APIConfig{
		BaseURL:      configString(s.Config, "base_url"),
		RecordsPath:  configString(s.Config, "records_path"),
		IDField:      configString(s.Config, "id_field"),
		CompositeIDs: parseStringSlice(s.Config["composite_ids"]),
		CompositeSep: configString(s.Config, "composite_sep"),
		Pagination: PaginationConfig{
			PageParam:     configString(s.Config, "page_param"),
			PageSizeParam: configString(s.Config, "page_size_param"),
			PageSize:      parseInt(s.Config["page_size"], 100),
			CursorParam:   configString(s.Config, "cursor_param"),
			CursorPath:    configString(s.Config, "cursor_path"),
		},
		AuthKind:        strings.ToLower(configString(s.Config, "auth_kind")),
		AuthRef:         configString(s.Config, "auth_ref"),
		AuthHeader:      configString(s.Config, "auth_header"),
		AuthUser:        configString(s.Config, "auth_user"),
		AuthPasswordRef: configString(s.Config, "auth_password_ref"),
	}

	switch {
	case cfg.Pagination.PageParam != "":
		cfg.Pagination.Kind = "page"
	case cfg.Pagination.CursorParam != "" || cfg.Pagination.CursorPath != "":
		cfg.Pagination.Kind = "cursor"
	default:
		cfg.Pagination.Kind = "none"
	}

	if err := validateBaseURL(cfg.BaseURL); err != nil {
		return nil, err
	}
	if err := cfg.validateAuth(); err != nil {
		return nil, err
	}

	fm, err := parseFieldMapping(s.Config["field_mapping"])
	if err != nil {
		return nil, err
	}
	cfg.FieldMapping = fm

	return cfg, nil
}

func validateBaseURL(raw string) error {
	if raw == "" {
		return fmt.Errorf("missing required 'base_url'")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("'base_url' must be an absolute http(s) URL, got %q", raw)
	}
	return nil
}

func (c *APIConfig) validateAuth() error {
	switch c.AuthKind {
	case "", AuthNone:
		if c.AuthRef != "" || c.AuthPasswordRef != "" {
			return fmt.Errorf("auth references are set but 'auth_kind' is %q", c.AuthKind)
		}
		c.AuthKind = AuthNone
	case AuthBearer, AuthAPIKeyHeader:
		if err := ValidateSecretRef(c.AuthRef); err != nil {
			return fmt.Errorf("'auth_ref': %w", err)
		}
	case AuthBasic:
		if c.AuthUser == "" {
			return fmt.Errorf("'auth_user' is required for basic auth")
		}
		if err := ValidateSecretRef(c.AuthPasswordRef); err != nil {
			return fmt.Errorf("'auth_password_ref': %w", err)
		}
	default:
		return fmt.Errorf("unsupported 'auth_kind' %q", c.AuthKind)
	}
	return nil
}

// ParseFeedConfig parses and validates the config map of a feed/file source.
func (s *Source) ParseFeedConfig() (*FeedConfig, error) {
	if s == nil {
		return nil, ErrInvalidSourceState
	}

	cfg := &FeedConfig{
		FilePath:      configString(s.Config, "file_path"),
		Format:        strings.ToUpper(configString(s.Config, "format")),
		IDField:       configString(s.Config, "id_field"),
		CompositeIDs:  parseStringSlice(s.Config["composite_ids"]),
		CompositeSep:  configString(s.Config, "composite_sep"),
		BatchSize:     parseInt(s.Config["batch_size"], 500),
		SkipMalformed: true,
	}
	if sm, ok := s.Config["skip_malformed"].(bool); ok {
		cfg.SkipMalformed = sm
	}

	if cfg.FilePath == "" {
		return nil, fmt.Errorf("missing required 'file_path'")
	}
	if cfg.Format == "" {
		cfg.Format = "NDJSON"
	}
	if cfg.Format != "CSV" && cfg.Format != "NDJSON" {
		return nil, fmt.Errorf("unsupported feed format %q (must be CSV or NDJSON)", cfg.Format)
	}
	if cfg.BatchSize <= 0 {
		return nil, fmt.Errorf("'batch_size' must be positive, got %d", cfg.BatchSize)
	}

	fm, err := parseFieldMapping(s.Config["field_mapping"])
	if err != nil {
		return nil, err
	}
	cfg.FieldMapping = fm

	return cfg, nil
}

// GetFieldMapping extracts and validates the FieldMapping configured on the source.
func (s *Source) GetFieldMapping() (*product.FieldMapping, error) {
	if s == nil {
		return nil, ErrInvalidSourceState
	}
	return parseFieldMapping(s.Config["field_mapping"])
}

func parseFieldMapping(raw any) (*product.FieldMapping, error) {
	if raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("field_mapping must be a JSON object")
	}
	attrs, err := parseStringMap(m["attribute_paths"])
	if err != nil {
		return nil, err
	}

	fm := &product.FieldMapping{
		NamePath:          configString(m, "name_path"),
		DescriptionPath:   configString(m, "description_path"),
		BrandPath:         configString(m, "brand_path"),
		OriginCountryPath: configString(m, "origin_country_path"),
		AttributePaths:    attrs,
	}
	if err := fm.Validate(); err != nil {
		return nil, err
	}
	return fm, nil
}

func parseStringMap(raw any) (map[string]string, error) {
	if raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("attribute_paths must be a JSON object, got %T", raw)
	}
	res := make(map[string]string, len(m))
	for k, v := range m {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("attribute_paths[%q] must be a string path, got %T", k, v)
		}
		res[k] = strings.TrimSpace(str)
	}
	return res, nil
}

func configString(cfg map[string]any, key string) string {
	v, _ := cfg[key].(string)
	return strings.TrimSpace(v)
}

func (s *Source) SetRateLimit(limit int) error {
	if limit <= 0 {
		return ErrInvalidRateLimit
	}
	s.RateLimitPerSecond = limit
	s.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Source) Enable() {
	s.Enabled = true
	s.UpdatedAt = time.Now().UTC()
}

func (s *Source) Disable() {
	s.Enabled = false
	s.UpdatedAt = time.Now().UTC()
}

func parseStringSlice(val any) []string {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case []string:
		return v
	case []any:
		res := make([]string, 0, len(v))
		for _, item := range v {
			if str, ok := item.(string); ok && strings.TrimSpace(str) != "" {
				res = append(res, strings.TrimSpace(str))
			}
		}
		return res
	default:
		return nil
	}
}

func parseInt(val any, defaultVal int) int {
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
