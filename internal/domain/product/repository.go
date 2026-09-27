package product

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Page size bounds for listing the catalogue.
const (
	DefaultListLimit = 50
	MaxListLimit     = 500
)

// Cursor is a keyset position: the (created_at, id) of the last product on the previous page.
type Cursor struct {
	CreatedAt time.Time
	ID        ID
}

// ListParams selects one page, newest first (created_at DESC, id DESC). After is nil for the first page.
type ListParams struct {
	After *Cursor
	Limit int
}

// Validate rejects a page size outside 1..MaxListLimit and a cursor missing either key.
func (p ListParams) Validate() error {
	if p.Limit < 1 || p.Limit > MaxListLimit {
		return fmt.Errorf("%w: limit %d outside 1..%d", ErrInvalidListParams, p.Limit, MaxListLimit)
	}
	if p.After != nil && (p.After.CreatedAt.IsZero() || strings.TrimSpace(string(p.After.ID)) == "") {
		return fmt.Errorf("%w: cursor needs both created_at and id", ErrInvalidListParams)
	}
	return nil
}

// Page is one page of the catalogue. Next is nil on the last page.
type Page struct {
	Items []*Product
	Next  *Cursor
}

type Repository interface {
	FindByID(ctx context.Context, id ID) (*Product, error)
	// List returns ErrInvalidListParams for params that fail Validate or name a malformed id.
	List(ctx context.Context, params ListParams) (Page, error)
	FindSnapshotsByIdentities(ctx context.Context, identities []IdentityRef) (map[string]*Snapshot, error)
	ApplyBatch(ctx context.Context, plan *BatchPlan) error
}
