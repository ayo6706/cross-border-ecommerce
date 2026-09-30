// Package regulatory runs the dataset lifecycle (load, review, activate or reject), the coverage
// check decisions make before relying on a category of rules, and the resolution of rules in force.
package regulatory

import (
	"context"
	"errors"
	"fmt"
	"time"

	domain "github.com/ayo6706/cross-border-ecommerce/internal/domain/regulatory"
	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

type Service struct {
	tx       TxRunner
	datasets DatasetRepository
	tariffs  TariffRepository
	slas     domain.SLAs
	now      func() time.Time
}

func NewService(
	tx TxRunner, datasets DatasetRepository, tariffs TariffRepository, slas domain.SLAs,
) (*Service, error) {
	if tx == nil || datasets == nil || tariffs == nil {
		return nil, errors.New("regulatory service needs a transaction runner, a dataset and a tariff repository")
	}
	if slas == nil {
		return nil, fmt.Errorf("%w: no SLAs configured", domain.ErrInvalidSLA)
	}
	return &Service{tx: tx, datasets: datasets, tariffs: tariffs, slas: slas, now: now}, nil
}

// now is truncated to the microsecond precision of timestamptz, so the times the service returns
// and publishes equal the ones it stores.
func now() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// CuratedLoad is a hand-researched file: its rules and the provenance of the dataset they form.
type CuratedLoad struct {
	Dataset domain.NewDatasetParams
	Rules   domain.CuratedRules
}

// LoadCurated stores a curated file as a new LOADED dataset with its rules, in one transaction.
// Curated data always needs a recorded review before activation.
func (s *Service) LoadCurated(ctx context.Context, load CuratedLoad) (*domain.Dataset, error) {
	load.Dataset.RequiresReview = true
	d, err := domain.NewDataset(load.Dataset)
	if err != nil {
		return nil, err
	}
	if err := load.Rules.Validate(d.Category); err != nil {
		return nil, err
	}
	err = s.tx.WithinTx(ctx, func(r TxRepos) error {
		if err := r.Datasets.Create(ctx, d); err != nil {
			return err
		}
		return r.Rules.InsertCurated(ctx, d.ID, load.Rules)
	})
	if err != nil {
		return nil, fmt.Errorf("load curated dataset: %w", err)
	}
	return d, nil
}

func (s *Service) Review(ctx context.Context, id, reviewer, note string) (*domain.Dataset, error) {
	return s.transition(ctx, "review", id, func(d *domain.Dataset) error {
		return d.RecordReview(reviewer, note, s.now())
	})
}

func (s *Service) Reject(ctx context.Context, id, reason string) (*domain.Dataset, error) {
	return s.transition(ctx, "reject", id, func(d *domain.Dataset) error { return d.Reject(reason) })
}

// transition loads a dataset locked, applies one entity transition and writes it back guarded by
// the status it was read with.
func (s *Service) transition(
	ctx context.Context, op, id string, apply func(*domain.Dataset) error,
) (*domain.Dataset, error) {
	var out *domain.Dataset
	err := s.tx.WithinTx(ctx, func(r TxRepos) error {
		d, err := r.Datasets.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		from := d.Status
		if err := apply(d); err != nil {
			return err
		}
		out = d
		return r.Datasets.SaveLifecycle(ctx, d, from)
	})
	if err != nil {
		return nil, fmt.Errorf("%s dataset %s: %w", op, id, err)
	}
	return out, nil
}

// Activate makes a LOADED dataset the ACTIVE version of its key. The previous ACTIVE version is
// superseded at the same instant, and the event is written in the same transaction.
func (s *Service) Activate(ctx context.Context, id string) (*domain.Dataset, error) {
	eventID, err := uuid.NewString()
	if err != nil {
		return nil, fmt.Errorf("generate event id: %w", err)
	}
	var out *domain.Dataset
	err = s.tx.WithinTx(ctx, func(r TxRepos) error {
		d, err := r.Datasets.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		now := s.now()
		if err := d.Activate(now); err != nil {
			return err
		}
		event := domain.DatasetActivated{EventID: eventID}
		// Supersede before saving: the one-ACTIVE-per-key index is checked per statement.
		previous, found, err := r.Datasets.ActiveForUpdate(ctx, d.Jurisdiction, d.Category, d.Source)
		if err != nil {
			return err
		}
		if found {
			if err := supersede(ctx, r.Datasets, previous, now); err != nil {
				return err
			}
			event.PreviousDatasetID = previous.ID
		}
		if err := r.Datasets.SaveLifecycle(ctx, d, domain.StatusLoaded); err != nil {
			return err
		}
		event.Dataset = *d
		out = d
		return r.Outbox.CreateDatasetActivatedEvent(ctx, event)
	})
	if err != nil {
		return nil, fmt.Errorf("activate dataset %s: %w", id, err)
	}
	return out, nil
}

func supersede(ctx context.Context, datasets DatasetRepository, d *domain.Dataset, now time.Time) error {
	if err := d.Supersede(now); err != nil {
		return err
	}
	return datasets.SaveLifecycle(ctx, d, domain.StatusActive)
}

// Coverage returns the datasets in force for (jurisdiction, category) at `at`, and ErrNoCoverage or
// ErrStaleCoverage when a decision must not rely on them. The datasets are returned with a stale
// error so the caller can report which one is stale.
func (s *Service) Coverage(
	ctx context.Context, jurisdiction string, category domain.Category, at time.Time,
) ([]*domain.Dataset, error) {
	jurisdiction, err := domain.ParseJurisdiction(jurisdiction)
	if err != nil {
		return nil, err
	}
	if category, err = domain.ParseCategory(string(category)); err != nil {
		return nil, err
	}
	active, err := s.datasets.ActiveAt(ctx, jurisdiction, category, at)
	if err != nil {
		return nil, fmt.Errorf("list datasets active at %s: %w", at.UTC().Format(time.RFC3339), err)
	}
	return active, s.slas.Check(category, active, at)
}

// ResolveTariff returns the duty in force for the query: rules effective on its transaction date,
// from the tariff datasets in force, and within their SLA, at its evaluation time. Every error
// domain.HoldReason maps is a HOLD, never a zero rate.
func (s *Service) ResolveTariff(ctx context.Context, q domain.TariffQuery) (*domain.TariffResolution, error) {
	datasets, err := s.Coverage(ctx, q.Jurisdiction(), domain.CategoryTariff, q.EvaluatedAt())
	if err != nil {
		return nil, fmt.Errorf("resolve tariff: %w", err)
	}
	ids := make([]string, 0, len(datasets))
	for _, d := range datasets {
		ids = append(ids, d.ID)
	}
	candidates, err := s.tariffs.TariffCandidates(ctx, ids, q)
	if err != nil {
		return nil, fmt.Errorf("resolve tariff: %w", err)
	}
	resolution, err := domain.ResolveTariff(q, datasets, candidates)
	if err != nil {
		return nil, fmt.Errorf("resolve tariff: %w", err)
	}
	return resolution, nil
}
