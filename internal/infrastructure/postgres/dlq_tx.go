package postgres

import (
	"context"
	"errors"

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
	return withinTx(ctx, m.pool, "dlq", func(r txRepositories) error {
		return fn(appDLQ.TxRepos{DLQ: r.dlq, Outbox: r.outbox})
	})
}
