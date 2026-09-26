package postgres //nolint:dupl // legacy baseline 2026-09-26: fix in ENG-018

import (
	"context"
	"fmt"

	appIngestion "github.com/ayo6706/cross-border-ecommerce/internal/application/ingestion"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ appIngestion.TxRunner = (*TxManager)(nil)

type TxManager struct {
	pool *pgxpool.Pool
}

func NewTxManager(pool *pgxpool.Pool) (*TxManager, error) {
	if pool == nil {
		return nil, fmt.Errorf("pgxpool cannot be nil")
	}
	return &TxManager{pool: pool}, nil
}

func (m *TxManager) WithinTx(ctx context.Context, fn func(repos appIngestion.TxRepos) error) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	runRepo, err := NewIngestionRepository(tx)
	if err != nil {
		return fmt.Errorf("create tx run repository: %w", err)
	}

	rawRepo, err := NewRawRecordRepository(tx)
	if err != nil {
		return fmt.Errorf("create tx raw record repository: %w", err)
	}

	if err := fn(appIngestion.TxRepos{
		Runs:       runRepo,
		RawRecords: rawRepo,
	}); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
