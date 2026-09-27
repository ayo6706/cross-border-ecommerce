package httpapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	productApp "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	"github.com/ayo6706/cross-border-ecommerce/internal/domain/product"
)

var invalidListParamsMessage = fmt.Sprintf(
	"invalid list parameters: limit must be 1..%d and cursor a next_cursor value", product.MaxListLimit)

type productResponse struct {
	ID               string    `json:"id"`
	CanonicalName    string    `json:"canonical_name"`
	Description      string    `json:"description"`
	Brand            string    `json:"brand"`
	OriginCountry    string    `json:"origin_country"`
	Status           string    `json:"status"`
	CurrentVersionID *string   `json:"current_version_id"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type productPageResponse struct {
	Items      []productResponse `json:"items"`
	NextCursor *string           `json:"next_cursor"`
}

// cursorToken is the JSON inside the opaque base64url next_cursor.
type cursorToken struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func HandleListProducts(svc *productApp.Service, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if svc == nil {
			writeUnavailable(w, r, logger, "product catalogue")
			return
		}
		params, err := listParamsFromQuery(r.URL.Query())
		if err != nil {
			writeJSONError(w, invalidListParamsMessage, http.StatusBadRequest)
			return
		}

		page, err := svc.ListProducts(r.Context(), params)
		if errors.Is(err, product.ErrInvalidListParams) {
			writeJSONError(w, invalidListParamsMessage, http.StatusBadRequest)
			return
		}
		if err != nil {
			writeInternalError(w, r, logger, "failed to list products", err)
			return
		}
		resp, err := toPageResponse(page)
		if err != nil {
			writeInternalError(w, r, logger, "failed to encode next cursor", err)
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func HandleGetProduct(svc *productApp.Service, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if svc == nil {
			writeUnavailable(w, r, logger, "product catalogue")
			return
		}
		p, err := svc.GetProductByID(r.Context(), product.ID(r.PathValue("id")))
		switch {
		case errors.Is(err, product.ErrProductNotFound):
			writeJSONError(w, "product not found", http.StatusNotFound)
		case err != nil:
			writeInternalError(w, r, logger, "failed to get product", err)
		default:
			writeJSON(w, http.StatusOK, toProductResponse(p))
		}
	}
}

// listParamsFromQuery parses limit and cursor. Values that parse but are out of range (limit 0,
// a cursor without an id) are rejected by ListParams.Validate in the repository, before any SQL.
func listParamsFromQuery(q url.Values) (product.ListParams, error) {
	params := product.ListParams{Limit: product.DefaultListLimit}
	if raw := q.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil {
			return product.ListParams{}, fmt.Errorf("%w: limit: %w", product.ErrInvalidListParams, err)
		}
		params.Limit = limit
	}
	if token := q.Get("cursor"); token != "" {
		after, err := decodeCursor(token)
		if err != nil {
			return product.ListParams{}, err
		}
		params.After = after
	}
	return params, nil
}

func encodeCursor(c *product.Cursor) (string, error) {
	raw, err := json.Marshal(cursorToken{CreatedAt: c.CreatedAt, ID: string(c.ID)})
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// decodeCursor accepts exactly what encodeCursor emits: one JSON object with known fields.
func decodeCursor(token string) (*product.Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("%w: cursor encoding: %w", product.ErrInvalidListParams, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var tok cursorToken
	if err := dec.Decode(&tok); err != nil {
		return nil, fmt.Errorf("%w: cursor body: %w", product.ErrInvalidListParams, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: data after cursor body", product.ErrInvalidListParams)
	}
	return &product.Cursor{CreatedAt: tok.CreatedAt, ID: product.ID(tok.ID)}, nil
}

func toPageResponse(page product.Page) (productPageResponse, error) {
	resp := productPageResponse{Items: make([]productResponse, 0, len(page.Items))}
	for _, p := range page.Items {
		resp.Items = append(resp.Items, toProductResponse(p))
	}
	if page.Next != nil {
		next, err := encodeCursor(page.Next)
		if err != nil {
			return productPageResponse{}, err
		}
		resp.NextCursor = &next
	}
	return resp, nil
}

func toProductResponse(p *product.Product) productResponse {
	return productResponse{
		ID:               string(p.ID),
		CanonicalName:    p.CanonicalName,
		Description:      p.Description,
		Brand:            p.Brand,
		OriginCountry:    p.OriginCountry,
		Status:           string(p.Status),
		CurrentVersionID: p.CurrentVersionID,
		CreatedAt:        p.CreatedAt,
		UpdatedAt:        p.UpdatedAt,
	}
}
