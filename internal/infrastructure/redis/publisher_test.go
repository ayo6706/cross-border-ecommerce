package redis

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	appMessaging "github.com/ayo6706/cross-border-ecommerce/internal/application/messaging"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type customNetError struct {
	msg string
}

func (e *customNetError) Error() string   { return e.msg }
func (e *customNetError) Timeout() bool   { return true }
func (e *customNetError) Temporary() bool { return true }

var _ net.Error = (*customNetError)(nil)

type customRedisError string

func (e customRedisError) Error() string { return string(e) }
func (e customRedisError) RedisError()   {}

var _ goredis.Error = customRedisError("")

func TestClassifyRedisError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		callerCtxErr error
		expectedWrap error
		isPerEvent   bool
	}{
		{
			name:         "nil error returns nil",
			err:          nil,
			callerCtxErr: nil,
			expectedWrap: nil,
		},
		{
			name:         "caller context cancelled returns context.Canceled",
			err:          context.Canceled,
			callerCtxErr: context.Canceled,
			expectedWrap: context.Canceled,
		},
		{
			name:         "caller context deadline exceeded returns context.DeadlineExceeded",
			err:          context.DeadlineExceeded,
			callerCtxErr: context.DeadlineExceeded,
			expectedWrap: context.DeadlineExceeded,
		},
		{
			name:         "client timeout with live caller ctx returns ErrBrokerUnavailable",
			err:          context.DeadlineExceeded,
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
		{
			name:         "redis LOADING error returns ErrBrokerUnavailable",
			err:          customRedisError("LOADING Redis is loading the dataset in memory"),
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
		{
			name:         "redis READONLY error returns ErrBrokerUnavailable",
			err:          customRedisError("READONLY You can't write against a read only replica"),
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
		{
			name:         "redis MASTERDOWN error returns ErrBrokerUnavailable",
			err:          customRedisError("MASTERDOWN Link with MASTER is down"),
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
		{
			name:         "redis OOM error returns ErrBrokerUnavailable",
			err:          customRedisError("OOM command not allowed when used memory > 'maxmemory'"),
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
		{
			name:         "redis BUSY error returns ErrBrokerUnavailable",
			err:          customRedisError("BUSY Redis is busy running a script"),
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
		{
			name:         "redis TRYAGAIN error returns ErrBrokerUnavailable",
			err:          customRedisError("TRYAGAIN Multiple keys request during rehashing"),
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
		{
			name:         "redis CLUSTERDOWN error returns ErrBrokerUnavailable",
			err:          customRedisError("CLUSTERDOWN Hash slot not served"),
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
		{
			name:         "redis WRONGTYPE error is treated as per-event poison error",
			err:          customRedisError("WRONGTYPE Operation against a key holding the wrong kind of value"),
			callerCtxErr: nil,
			isPerEvent:   true,
		},
		{
			name:         "redis generic syntax error is treated as per-event poison error",
			err:          customRedisError("ERR syntax error"),
			callerCtxErr: nil,
			isPerEvent:   true,
		},
		{
			name:         "network error returns ErrBrokerUnavailable",
			err:          &customNetError{msg: "connection refused"},
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
		{
			name:         "EOF error returns ErrBrokerUnavailable",
			err:          io.EOF,
			callerCtxErr: nil,
			expectedWrap: appMessaging.ErrBrokerUnavailable,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ClassifyRedisError(tt.err, tt.callerCtxErr)
			if tt.err == nil {
				assert.NoError(t, got)
				return
			}
			require.Error(t, got)
			if tt.isPerEvent {
				assert.False(t, errors.Is(got, appMessaging.ErrBrokerUnavailable))
				assert.Equal(t, tt.err.Error(), got.Error())
			} else {
				assert.ErrorIs(t, got, tt.expectedWrap)
			}
		})
	}
}
