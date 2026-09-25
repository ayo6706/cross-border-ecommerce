package redis

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateConsumerName(t *testing.T) {
	t.Parallel()

	name, err := GenerateConsumerName("custom")
	require.NoError(t, err)
	assert.Contains(t, name, "custom-")

	defaultName, err := GenerateConsumerName("")
	require.NoError(t, err)
	assert.NotEmpty(t, defaultName)
}

func TestConsumerConfig_Validate(t *testing.T) {
	t.Parallel()

	validCfg := ConsumerConfig{
		Stream:           "product.changed",
		Group:            "compliance-evaluators",
		ConsumerName:     "c-1",
		BatchSize:        10,
		BlockDuration:    2 * time.Second,
		ClaimMinIdle:     60 * time.Second,
		ClaimInterval:    10 * time.Second,
		ClaimBatchSize:   10,
		BaseBackoff:      100 * time.Millisecond,
		MaxBackoff:       5 * time.Second,
		Concurrency:      10,
		QueueSize:        20,
		HandlerTimeout:   5 * time.Second,
		DrainTimeout:     5 * time.Second,
		RetryMaxAttempts: 5,
		RetryBaseBackoff: 200 * time.Millisecond,
		RetryMaxBackoff:  2 * time.Second,
		DLQStore:         &memoryDLQStore{},
	}

	require.NoError(t, validCfg.Validate())

	tests := []struct {
		name        string
		modify      func(*ConsumerConfig)
		expectedErr string
	}{
		{
			name: "empty stream",
			modify: func(c *ConsumerConfig) {
				c.Stream = "   "
			},
			expectedErr: "stream name cannot be empty",
		},
		{
			name: "empty group",
			modify: func(c *ConsumerConfig) {
				c.Group = ""
			},
			expectedErr: "consumer group cannot be empty",
		},
		{
			name: "empty consumer name",
			modify: func(c *ConsumerConfig) {
				c.ConsumerName = "   "
			},
			expectedErr: "consumer name cannot be empty",
		},
		{
			name: "stream longer than dlq column",
			modify: func(c *ConsumerConfig) {
				c.Stream = strings.Repeat("s", 129)
			},
			expectedErr: "stream name must be at most 128 bytes",
		},
		{
			name: "group longer than dlq column",
			modify: func(c *ConsumerConfig) {
				c.Group = strings.Repeat("g", 129)
			},
			expectedErr: "consumer group must be at most 128 bytes",
		},
		{
			name: "consumer name longer than dlq column",
			modify: func(c *ConsumerConfig) {
				c.ConsumerName = strings.Repeat("c", 129)
			},
			expectedErr: "consumer name must be at most 128 bytes",
		},
		{
			name: "zero batch size",
			modify: func(c *ConsumerConfig) {
				c.BatchSize = 0
			},
			expectedErr: "batch size must be between 1 and 1000",
		},
		{
			name: "excessive batch size",
			modify: func(c *ConsumerConfig) {
				c.BatchSize = 1001
			},
			expectedErr: "batch size must be between 1 and 1000",
		},
		{
			name: "zero block duration",
			modify: func(c *ConsumerConfig) {
				c.BlockDuration = 0
			},
			expectedErr: "block duration must be strictly positive",
		},
		{
			name: "negative claim min idle",
			modify: func(c *ConsumerConfig) {
				c.ClaimMinIdle = -1 * time.Second
			},
			expectedErr: "claim min idle must be strictly positive",
		},
		{
			name: "zero claim interval",
			modify: func(c *ConsumerConfig) {
				c.ClaimInterval = 0
			},
			expectedErr: "claim interval must be strictly positive",
		},
		{
			name: "zero claim batch size",
			modify: func(c *ConsumerConfig) {
				c.ClaimBatchSize = 0
			},
			expectedErr: "claim batch size must be between 1 and 1000",
		},
		{
			name: "excessive claim batch size",
			modify: func(c *ConsumerConfig) {
				c.ClaimBatchSize = 1001
			},
			expectedErr: "claim batch size must be between 1 and 1000",
		},
		{
			name: "zero base backoff",
			modify: func(c *ConsumerConfig) {
				c.BaseBackoff = 0
			},
			expectedErr: "base backoff must be strictly positive",
		},
		{
			name: "max backoff less than base backoff",
			modify: func(c *ConsumerConfig) {
				c.BaseBackoff = 5 * time.Second
				c.MaxBackoff = 1 * time.Second
			},
			expectedErr: "max backoff cannot be less than base backoff",
		},
		{
			name: "zero concurrency",
			modify: func(c *ConsumerConfig) {
				c.Concurrency = 0
			},
			expectedErr: "concurrency must be strictly positive",
		},
		{
			name: "negative queue size",
			modify: func(c *ConsumerConfig) {
				c.QueueSize = -1
			},
			expectedErr: "queue size cannot be negative",
		},
		{
			name: "zero handler timeout",
			modify: func(c *ConsumerConfig) {
				c.HandlerTimeout = 0
			},
			expectedErr: "handler timeout must be strictly positive",
		},
		{
			name: "zero drain timeout",
			modify: func(c *ConsumerConfig) {
				c.DrainTimeout = 0
			},
			expectedErr: "drain timeout must be strictly positive",
		},
		{
			name: "claim min idle less than or equal to handler timeout",
			modify: func(c *ConsumerConfig) {
				c.ClaimMinIdle = 5 * time.Second
				c.HandlerTimeout = 10 * time.Second
			},
			expectedErr: "claim min idle (5s) must be strictly greater than handler timeout (10s)",
		},
		{
			name: "retry max attempts <= 0",
			modify: func(c *ConsumerConfig) {
				c.RetryMaxAttempts = 0
			},
			expectedErr: "attempts must be between 1 and 100",
		},
		{
			name: "retry base backoff <= 0",
			modify: func(c *ConsumerConfig) {
				c.RetryBaseBackoff = 0
			},
			expectedErr: "base backoff must be strictly positive",
		},
		{
			name: "retry max backoff < base backoff",
			modify: func(c *ConsumerConfig) {
				c.RetryBaseBackoff = 5 * time.Second
				c.RetryMaxBackoff = 1 * time.Second
			},
			expectedErr: "max backoff cannot be less than base backoff",
		},
		{
			name: "retry max attempts excessive greater than 100",
			modify: func(c *ConsumerConfig) {
				c.RetryMaxAttempts = 101
			},
			expectedErr: "attempts must be between 1 and 100",
		},
		{
			name: "retry max attempts int overflow math.MaxInt32",
			modify: func(c *ConsumerConfig) {
				c.RetryMaxAttempts = 1 << 31
			},
			expectedErr: "attempts must be between 1 and 100",
		},
		{
			name: "nil dlq store",
			modify: func(c *ConsumerConfig) {
				c.DLQStore = nil
			},
			expectedErr: "dlq store cannot be nil",
		},
		{
			name: "worst case retry window >= claim min idle",
			modify: func(c *ConsumerConfig) {
				c.HandlerTimeout = 12 * time.Second // 5 * 12s + 4 * 2s = 68s >= 60s
			},
			expectedErr: "worst-case retry window (1m8s) must be strictly less than claim min idle (1m0s)",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := validCfg
			tt.modify(&cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidConsumerConfig)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestNewConsumer_NilAndValidationGuards(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := ConsumerConfig{
		Stream:           "product.changed",
		Group:            "compliance-evaluators",
		ConsumerName:     "c-1",
		BatchSize:        10,
		BlockDuration:    2 * time.Second,
		ClaimMinIdle:     60 * time.Second,
		ClaimInterval:    10 * time.Second,
		ClaimBatchSize:   10,
		BaseBackoff:      100 * time.Millisecond,
		MaxBackoff:       5 * time.Second,
		Concurrency:      10,
		QueueSize:        20,
		HandlerTimeout:   5 * time.Second,
		DrainTimeout:     5 * time.Second,
		RetryMaxAttempts: 5,
		RetryBaseBackoff: 200 * time.Millisecond,
		RetryMaxBackoff:  2 * time.Second,
		DLQStore:         &memoryDLQStore{},
	}

	cNilClient, err := NewConsumer(nil, cfg, logger)
	assert.Nil(t, cNilClient)
	assert.ErrorIs(t, err, ErrNilConsumerClient)

	err = EnsureGroup(context.Background(), nil, "stream", "group")
	assert.ErrorIs(t, err, appMessaging.ErrBrokerUnavailable)

	cNilHandler := &Consumer{logger: logger}
	err = cNilHandler.Run(context.Background(), nil)
	assert.ErrorIs(t, err, ErrNilHandler)
}
