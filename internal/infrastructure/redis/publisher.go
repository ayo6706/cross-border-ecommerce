package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	"github.com/redis/go-redis/v9"
)

var _ appOutbox.Publisher = (*Publisher)(nil)

type Publisher struct {
	client *redis.Client
}

func NewPublisher(ctx context.Context, redisURL string) (*Publisher, error) {
	if strings.TrimSpace(redisURL) == "" {
		return nil, errors.New("redis URL cannot be empty")
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}

	client := redis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &Publisher{client: client}, nil
}

func NewPublisherFromClient(client *redis.Client) *Publisher {
	return &Publisher{client: client}
}

func (p *Publisher) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}

func (p *Publisher) Publish(ctx context.Context, e appOutbox.Event) error {
	if p.client == nil {
		return fmt.Errorf("%w: redis client is nil", appOutbox.ErrBrokerUnavailable)
	}

	stream := e.EventType
	if strings.TrimSpace(stream) == "" {
		return errors.New("event type stream key cannot be empty")
	}

	values := map[string]any{
		"event_id":       e.ID,
		"aggregate_type": e.AggregateType,
		"aggregate_id":   e.AggregateID,
		"event_type":     e.EventType,
		"payload":        e.Payload,
		"created_at":     e.CreatedAt.UTC().Format(time.RFC3339Nano),
	}

	err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Err()

	if err != nil {
		return ClassifyRedisError(err, ctx.Err())
	}

	return nil
}

func ClassifyRedisError(err error, callerCtxErr error) error {
	if err == nil {
		return nil
	}

	if callerCtxErr != nil {
		return callerCtxErr
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", appOutbox.ErrBrokerUnavailable, err)
	}

	var redisErr redis.Error
	if errors.As(err, &redisErr) {
		msg := strings.ToUpper(strings.TrimSpace(redisErr.Error()))
		for _, prefix := range []string{
			"LOADING",
			"READONLY",
			"MASTERDOWN",
			"OOM",
			"BUSY",
			"TRYAGAIN",
			"CLUSTERDOWN",
		} {
			if strings.HasPrefix(msg, prefix) || strings.HasPrefix(msg, "ERR "+prefix) {
				return fmt.Errorf("%w: %w", appOutbox.ErrBrokerUnavailable, err)
			}
		}

		return err
	}

	return fmt.Errorf("%w: %w", appOutbox.ErrBrokerUnavailable, err)
}
