package auth

import (
	"net/http"
	"strings"
)

// AuthStrategy encapsulates injecting authentication credentials into an outbound HTTP request.
type AuthStrategy interface {
	ApplyAuth(req *http.Request) error
}

type NoAuth struct{}

func (a *NoAuth) ApplyAuth(req *http.Request) error {
	return nil
}

type BearerAuth struct {
	Token string
}

func NewBearerAuth(token string) *BearerAuth {
	return &BearerAuth{Token: strings.TrimSpace(token)}
}

func (a *BearerAuth) ApplyAuth(req *http.Request) error {
	if a.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.Token)
	}
	return nil
}

type APIKeyAuth struct {
	Key        string
	HeaderName string
	QueryParam string
}

func NewAPIKeyHeaderAuth(key, headerName string) *APIKeyAuth {
	h := strings.TrimSpace(headerName)
	if h == "" {
		h = "X-API-Key"
	}
	return &APIKeyAuth{
		Key:        strings.TrimSpace(key),
		HeaderName: h,
	}
}

func NewAPIKeyQueryAuth(key, queryParam string) *APIKeyAuth {
	q := strings.TrimSpace(queryParam)
	if q == "" {
		q = "api_key"
	}
	return &APIKeyAuth{
		Key:        strings.TrimSpace(key),
		QueryParam: q,
	}
}

func (a *APIKeyAuth) ApplyAuth(req *http.Request) error {
	if a.Key == "" {
		return nil
	}
	if a.HeaderName != "" {
		req.Header.Set(a.HeaderName, a.Key)
	}
	if a.QueryParam != "" {
		q := req.URL.Query()
		q.Set(a.QueryParam, a.Key)
		req.URL.RawQuery = q.Encode()
	}
	return nil
}

type BasicAuth struct {
	Username string
	Password string
}

func NewBasicAuth(username, password string) *BasicAuth {
	return &BasicAuth{
		Username: username,
		Password: password,
	}
}

func (a *BasicAuth) ApplyAuth(req *http.Request) error {
	if a.Username != "" || a.Password != "" {
		req.SetBasicAuth(a.Username, a.Password)
	}
	return nil
}
