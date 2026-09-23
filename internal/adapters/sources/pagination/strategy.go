package pagination

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/adapters/sources/extraction"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/ingestion"
)

// PaginationStrategy abstracts how an HTTP request is configured for the current page
// and how the subsequent page checkpoint is extracted from the response.
type PaginationStrategy interface {
	ApplyPagination(req *http.Request, checkpoint string) error
	ExtractNextCheckpoint(resp *http.Response, body []byte, sentCheckpoint string, recordCount int) (string, error)
}

// CursorPagination implements token/cursor-based pagination.
type CursorPagination struct {
	RequestParam string
	CursorPath   string
}

func NewCursorPagination(requestParam, cursorPath string) *CursorPagination {
	param := strings.TrimSpace(requestParam)
	if param == "" {
		param = "cursor"
	}
	path := strings.TrimSpace(cursorPath)
	if path == "" {
		path = "next_cursor"
	}
	return &CursorPagination{
		RequestParam: param,
		CursorPath:   path,
	}
}

func (p *CursorPagination) ApplyPagination(req *http.Request, checkpoint string) error {
	cursor, err := ingestion.ParseCursorCheckpoint(checkpoint)
	if err != nil {
		return err
	}
	if cursor != "" {
		q := req.URL.Query()
		q.Set(p.RequestParam, cursor)
		req.URL.RawQuery = q.Encode()
	}
	return nil
}

func (p *CursorPagination) ExtractNextCheckpoint(resp *http.Response, body []byte, sentCheckpoint string, recordCount int) (string, error) {
	if len(body) == 0 {
		return "", nil
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var root any
	if err := dec.Decode(&root); err != nil {
		return "", nil // non-json or malformed handled upstream
	}

	rawToken, found := extraction.LookupString(root, p.CursorPath)
	if !found || strings.TrimSpace(rawToken) == "" {
		return "", nil
	}

	token := strings.TrimSpace(rawToken)
	sentCursor, _ := ingestion.ParseCursorCheckpoint(sentCheckpoint)
	if sentCursor != "" && token == sentCursor {
		return "", nil // echoed cursor protection
	}

	return ingestion.FormatCursorCheckpoint(token), nil
}

// PagePagination implements numeric page-based pagination (?page=1&limit=50).
type PagePagination struct {
	PageParam     string
	PageSizeParam string
	PageSize      int
}

func NewPagePagination(pageParam, pageSizeParam string, pageSize int) *PagePagination {
	pParam := strings.TrimSpace(pageParam)
	if pParam == "" {
		pParam = "page"
	}
	sParam := strings.TrimSpace(pageSizeParam)
	if sParam == "" {
		sParam = "limit"
	}
	size := pageSize
	if size <= 0 {
		size = 50
	}
	return &PagePagination{
		PageParam:     pParam,
		PageSizeParam: sParam,
		PageSize:      size,
	}
}

func (p *PagePagination) ApplyPagination(req *http.Request, checkpoint string) error {
	page := 1
	trimmed := strings.TrimSpace(checkpoint)
	if trimmed != "" {
		val, err := strconv.Atoi(trimmed)
		if err != nil || val < 1 {
			return fmt.Errorf("%w: invalid page checkpoint %q", ingestion.ErrInvalidCheckpoint, trimmed)
		}
		page = val
	}

	q := req.URL.Query()
	q.Set(p.PageParam, strconv.Itoa(page))
	q.Set(p.PageSizeParam, strconv.Itoa(p.PageSize))
	req.URL.RawQuery = q.Encode()
	return nil
}

func (p *PagePagination) ExtractNextCheckpoint(resp *http.Response, body []byte, sentCheckpoint string, recordCount int) (string, error) {
	// If returned fewer records than the requested page size, we have reached the end.
	if recordCount < p.PageSize {
		return "", nil
	}

	currentPage := 1
	trimmed := strings.TrimSpace(sentCheckpoint)
	if trimmed != "" {
		if val, err := strconv.Atoi(trimmed); err == nil && val >= 1 {
			currentPage = val
		}
	}

	return strconv.Itoa(currentPage + 1), nil
}

// LinkHeaderPagination implements RFC 5988 web linking.
type LinkHeaderPagination struct {
	linkRegex *regexp.Regexp
}

func NewLinkHeaderPagination() *LinkHeaderPagination {
	return &LinkHeaderPagination{
		linkRegex: regexp.MustCompile(`<([^>]+)>;\s*rel="([^"]+)"`),
	}
}

func (p *LinkHeaderPagination) ApplyPagination(req *http.Request, checkpoint string) error {
	trimmed := strings.TrimSpace(checkpoint)
	if trimmed == "" {
		return nil
	}

	parsed, err := url.Parse(trimmed)
	if err == nil && parsed.RawQuery != "" {
		// Merge query params from next URL into req
		nextQ := parsed.Query()
		q := req.URL.Query()
		for k, v := range nextQ {
			q[k] = v
		}
		req.URL.RawQuery = q.Encode()
		return nil
	}

	return nil
}

func (p *LinkHeaderPagination) ExtractNextCheckpoint(resp *http.Response, body []byte, sentCheckpoint string, recordCount int) (string, error) {
	if resp == nil {
		return "", nil
	}

	linkHeaders := resp.Header.Values("Link")
	for _, header := range linkHeaders {
		matches := p.linkRegex.FindAllStringSubmatch(header, -1)
		for _, m := range matches {
			if len(m) == 3 && m[2] == "next" {
				return m[1], nil
			}
		}
	}

	return "", nil
}
