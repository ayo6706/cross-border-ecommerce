package postgres //nolint:dupl // legacy baseline 2026-09-26: fix in ENG-018

import (
	"context"
	"errors"
	"fmt"

	appDLQ "github.com/ayo6706/cross-border-ecommerce/internal/application/dlq"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ appDLQ.TxRunner = (*DLQTxManager)(nil)

type DLQTxManager struct {
	pool *pgxpool.Pool
}

func NewDLQTxManager(pool *pgxpool.Pool) (*DLQTxManager, error) {
	if pool == nil {
		return nil, errors.New("pgxpool cannot be nil")
	}
	return &DLQTxManager{pool: pool}, nil
}

func (m *DLQTxManager) WithinTx(ctx context.Context, fn func(repos appDLQ.TxRepos) error) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin dlq transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	dlqRepo, err := NewDLQRepository(tx)
	if err != nil {
		return fmt.Errorf("create tx dlq repository: %w", err)
	}
	outboxRepo, err := NewOutboxRepository(tx)
	if err != nil {
		return fmt.Errorf("create tx outbox repository: %w", err)
	}

	if err := fn(appDLQ.TxRepos{DLQ: dlqRepo, Outbox: outboxRepo}); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dlq transaction: %w", err)
	}
	return nil
}
