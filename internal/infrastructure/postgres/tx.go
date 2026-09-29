package postgres

import (
	"context"
	"fmt"

	appIngestion "github.com/ayo6706/cross-border-ecommerce/internal/application/ingestion"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// txRepositories are the repositories bound to one transaction. Each TxManager hands its
// application port the subset it declares.
type txRepositories struct {
	runs       *IngestionRepository
	rawRecords *RawRecordRepository
	products   *ProductRepository
	processing *RunProcessingRepository
	outbox     *OutboxRepository
	dlq        *DLQRepository
}

func newTxRepositories(tx pgx.Tx) (txRepositories, error) {
	var (
		r    txRepositories
		errs [6]error
	)
	r.runs, errs[0] = NewIngestionRepository(tx)
	r.rawRecords, errs[1] = NewRawRecordRepository(tx)
	r.products, errs[2] = NewProductRepository(tx)
	r.processing, errs[3] = NewRunProcessingRepository(tx)
	r.outbox, errs[4] = NewOutboxRepository(tx)
	r.dlq, errs[5] = NewDLQRepository(tx)
	for _, err := range errs {
		if err != nil {
			return txRepositories{}, err
		}
	}
	return r, nil
}

// withinTx runs fn with repositories bound to one transaction and commits only when fn succeeds.
// fn's error is returned as is: it already names the failed step.
func withinTx(ctx context.Context, pool *pgxpool.Pool, name string, fn func(txRepositories) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s transaction: %w", name, err)
	}
	defer func() {
		_ = tx.Rollback(ctx) // no-op after Commit
	}()

	repos, err := newTxRepositories(tx)
	if err != nil {
		return fmt.Errorf("bind %s repositories: %w", name, err)
	}
	if err := fn(repos); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s transaction: %w", name, err)
	}
	return nil
}

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
	return withinTx(ctx, m.pool, "ingestion", func(r txRepositories) error {
		return fn(appIngestion.TxRepos{Runs: r.runs, RawRecords: r.rawRecords})
	})
}
