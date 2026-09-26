package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	appOutbox "github.com/ayo6706/cross-border-ecommerce/internal/application/outbox"
	"github.com/redis/go-redis/v9"
)

var _ appOutbox.Publisher = (*Publisher)(nil)

type PublisherOption func(*Publisher)

func WithRetention(retention time.Duration) PublisherOption {
	return func(p *Publisher) {
		p.retention = retention
	}
}

type Publisher struct {
	client    *redis.Client
	retention time.Duration
}

func NewPublisher(ctx context.Context, redisURL string, opts ...PublisherOption) (*Publisher, error) {
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

	p := &Publisher{client: client}
	for _, opt := range opts {
		opt(p)
	}

	return p, nil
}

func NewPublisherFromClient(client *redis.Client, opts ...PublisherOption) *Publisher {
	p := &Publisher{client: client}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Publisher) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}

func (p *Publisher) Retention() time.Duration {
	return p.retention
}

func (p *Publisher) Publish(ctx context.Context, e appOutbox.Event) error {
	if p.client == nil {
		return fmt.Errorf("%w: redis client is nil", appMessaging.ErrBrokerUnavailable)
	}

	stream := e.EventType
	if strings.TrimSpace(stream) == "" {
		return errors.New("event type stream key cannot be empty")
	}

	values := EncodeEvent(e)

	args := &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}

	if p.retention > 0 {
		cutoff := time.Now().Add(-p.retention)
		if ms := cutoff.UnixMilli(); ms > 0 {
			args.MinID = fmt.Sprintf("%d-0", ms)
			args.Approx = true
		}
	}

	err := p.client.XAdd(ctx, args).Err()
	if err != nil {
		return ClassifyRedisError(err, ctx.Err())
	}

	return nil
}

func ClassifyRedisError(err, callerCtxErr error) error {
	if err == nil {
		return nil
	}

	if callerCtxErr != nil {
		return callerCtxErr
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", appMessaging.ErrBrokerUnavailable, err)
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
				return fmt.Errorf("%w: %w", appMessaging.ErrBrokerUnavailable, err)
			}
		}

		return err
	}

	return fmt.Errorf("%w: %w", appMessaging.ErrBrokerUnavailable, err)
}
