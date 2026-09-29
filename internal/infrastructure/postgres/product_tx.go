package postgres

import (
	"context"
	"fmt"

	appProduct "github.com/ayo6706/cross-border-ecommerce/internal/application/product"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ appProduct.TxRunner = (*ProductTxManager)(nil)

type ProductTxManager struct {
	pool *pgxpool.Pool
}

func NewProductTxManager(pool *pgxpool.Pool) (*ProductTxManager, error) {
	if pool == nil {
		return nil, fmt.Errorf("pgxpool cannot be nil")
	}
	return &ProductTxManager{pool: pool}, nil
}

func (m *ProductTxManager) WithinTx(ctx context.Context, fn func(repos appProduct.TxRepos) error) error {
	return withinTx(ctx, m.pool, "product", func(r txRepositories) error {
		return fn(appProduct.TxRepos{Products: r.products, RunProcessing: r.processing, Outbox: r.outbox})
	})
}
