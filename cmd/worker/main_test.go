package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsOnlyContextCanceled(t *testing.T) {
	t.Parallel()

	assert.False(t, isOnlyContextCanceled(nil), "nil error is not context canceled")

	assert.True(t, isOnlyContextCanceled(context.Canceled), "direct context.Canceled returns true")
	assert.True(t, isOnlyContextCanceled(fmt.Errorf("wrapped: %w", context.Canceled)), "wrapped context.Canceled returns true")

	joinedCanceled := errors.Join(context.Canceled, fmt.Errorf("nested: %w", context.Canceled))
	assert.True(t, isOnlyContextCanceled(joinedCanceled), "joined pure cancellations return true")

	assert.False(t, isOnlyContextCanceled(errors.New("db error")), "unrelated error returns false")

	joinedMixed := errors.Join(context.Canceled, errors.New("db error"))
	assert.False(t, isOnlyContextCanceled(joinedMixed), "mixed cancellation and real error returns false (must not drop real error)")

	joinedNestedMixed := errors.Join(
		fmt.Errorf("wrap1: %w", context.Canceled),
		errors.Join(context.Canceled, errors.New("connection lost")),
	)
	assert.False(t, isOnlyContextCanceled(joinedNestedMixed), "deeply nested real error returns false")
}
