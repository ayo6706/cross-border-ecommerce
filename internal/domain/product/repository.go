package product

import (
	"context"
	"time"
)

type ListParams struct {
	LastCreatedAt *time.Time
	LastID        *ID
	Limit         int
}

type Repository interface {
	FindByID(ctx context.Context, id ID) (*Product, error)
	Save(ctx context.Context, p *Product) error
	List(ctx context.Context, params ListParams) ([]*Product, error)
	FindSnapshotByIdentity(ctx context.Context, sourceID string, externalProductID string) (*Snapshot, error)
	CreateProductWithSource(ctx context.Context, p *Product, ps *ProductSource) error
	CreateVersion(ctx context.Context, pv *ProductVersion) error
	GuardedUpdateVersion(ctx context.Context, p *Product, expectedVersionID *string) error
	GuardedUpdateFingerprintOnly(ctx context.Context, productID ID, expectedVersionID *string, newFingerprint string, updatedAt time.Time) error
	CreateChange(ctx context.Context, pc *ProductChange) error
	UpdateSourceWatermark(ctx context.Context, psID string, sourceUpdatedAt *time.Time, receivedAt time.Time) error
	UpdateSourceOnChanged(ctx context.Context, psID string, lastChangedAt time.Time, sourceUpdatedAt *time.Time, receivedAt time.Time) error
}
