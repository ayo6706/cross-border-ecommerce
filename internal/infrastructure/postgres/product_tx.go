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
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin product transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	prodRepo, err := NewProductRepository(tx)
	if err != nil {
		return fmt.Errorf("create tx product repository: %w", err)
	}

	processingRepo, err := NewRunProcessingRepository(tx)
	if err != nil {
		return fmt.Errorf("create tx run processing repository: %w", err)
	}

	outboxRepo, err := NewOutboxRepository(tx)
	if err != nil {
		return fmt.Errorf("create tx outbox repository: %w", err)
	}

	if err := fn(appProduct.TxRepos{
		Products:      prodRepo,
		RunProcessing: processingRepo,
		Outbox:        outboxRepo,
	}); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit product transaction: %w", err)
	}

	return nil
}
