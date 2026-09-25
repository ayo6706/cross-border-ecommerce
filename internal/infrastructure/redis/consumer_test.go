package redis

import (
	"context"
	"io"
	"log/slog"
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
		Stream:         "product.changed",
		Group:          "compliance-evaluators",
		ConsumerName:   "c-1",
		BatchSize:      10,
		BlockDuration:  2 * time.Second,
		ClaimMinIdle:   30 * time.Second,
		ClaimInterval:  10 * time.Second,
		ClaimBatchSize: 10,
		BaseBackoff:    100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
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
		Stream:         "product.changed",
		Group:          "compliance-evaluators",
		ConsumerName:   "c-1",
		BatchSize:      10,
		BlockDuration:  2 * time.Second,
		ClaimMinIdle:   30 * time.Second,
		ClaimInterval:  10 * time.Second,
		ClaimBatchSize: 10,
		BaseBackoff:    100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
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
