package dlq

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ayo6706/cross-border-ecommerce/internal/platform/uuid"
)

var _ Replayer = (*ReplayService)(nil)

// ReplayService re-publishes dead-lettered messages through the transactional outbox.
type ReplayService struct {
	tx TxRunner
}

func NewReplayService(tx TxRunner) (*ReplayService, error) {
	if tx == nil {
		return nil, errors.New("dlq replay tx runner cannot be nil")
	}
	return &ReplayService{tx: tx}, nil
}

// Replay writes an outbox event that re-publishes the message to its consumer group, reusing the
// original event_id so the idempotency guard stops a second effect. The outbox write and the
// DEAD -> REPLAYED transition commit together; if another replay won the transition, nothing is written.
func (s *ReplayService) Replay(ctx context.Context, id string) (string, error) {
	if strings.TrimSpace(id) == "" {
		return "", fmt.Errorf("%w: id cannot be empty", ErrInvalidDLQID)
	}
	outboxID, err := uuid.NewString()
	if err != nil {
		return "", fmt.Errorf("generate replay outbox id: %w", err)
	}

	err = s.tx.WithinTx(ctx, func(repos TxRepos) error {
		src, err := repos.DLQ.GetReplaySource(ctx, id)
		if err != nil {
			return err
		}
		if !src.Replayable {
			return ErrNotReplayable
		}
		if err := repos.Outbox.CreateReplayEvent(ctx, ReplayEvent{
			ID:              outboxID,
			ReplayOfEventID: src.EventID,
			TargetGroup:     src.ConsumerGroup,
			EventType:       src.EventType,
			AggregateType:   src.AggregateType,
			AggregateID:     src.AggregateID,
			Payload:         src.Payload,
		}); err != nil {
			return fmt.Errorf("create replay outbox event: %w", err)
		}
		marked, err := repos.DLQ.MarkReplayed(ctx, id, outboxID)
		if err != nil {
			return fmt.Errorf("mark dlq message replayed: %w", err)
		}
		if !marked {
			return ErrAlreadyReplayed
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outboxID, nil
}
