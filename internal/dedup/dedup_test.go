package dedup

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func testChecker(t *testing.T, window time.Duration) (*Checker, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	checker, err := NewChecker(redis.NewClient(&redis.Options{Addr: server.Addr()}), window)
	require.NoError(t, err)
	return checker, server
}

func TestCheckerWindowBounds(t *testing.T) {
	server := miniredis.RunT(t)
	_, err := NewChecker(redis.NewClient(&redis.Options{Addr: server.Addr()}), 5*time.Second)
	require.Error(t, err, "window below 10s must fail closed")
	_, err = NewChecker(redis.NewClient(&redis.Options{Addr: server.Addr()}), 31*time.Second)
	require.Error(t, err, "window above 30s must fail closed")
	_, err = NewChecker(redis.NewClient(&redis.Options{Addr: server.Addr()}), 15*time.Second)
	require.NoError(t, err)
}

func TestIsDuplicateWithinWindow(t *testing.T) {
	checker, _ := testChecker(t, 15*time.Second)
	ctx := context.Background()
	hash := PayloadHash("657210300", 1, 6418000, 3372500, 8400, 127500, time.Unix(1709315661, 0))

	duplicate, err := checker.IsDuplicate(ctx, "657210300", 1, hash)
	require.NoError(t, err)
	require.False(t, duplicate, "first sighting is not a duplicate")

	duplicate, err = checker.IsDuplicate(ctx, "657210300", 1, hash)
	require.NoError(t, err)
	require.True(t, duplicate, "identical report inside the window is a duplicate")
}

func TestIsDuplicateExpiresAfterWindow(t *testing.T) {
	checker, server := testChecker(t, 10*time.Second)
	ctx := context.Background()
	hash := PayloadHash("657210300", 1, 6418000, 3372500, 8400, 127500, time.Unix(1709315661, 0))

	duplicate, err := checker.IsDuplicate(ctx, "657210300", 1, hash)
	require.NoError(t, err)
	require.False(t, duplicate)

	server.FastForward(11 * time.Second)

	duplicate, err = checker.IsDuplicate(ctx, "657210300", 1, hash)
	require.NoError(t, err)
	require.False(t, duplicate, "report reappearing after the window is fresh")
}

func TestIsDuplicateDistinctKeys(t *testing.T) {
	checker, _ := testChecker(t, 15*time.Second)
	ctx := context.Background()
	hash := PayloadHash("657210300", 1, 6418000, 3372500, 8400, 127500, time.Unix(1709315661, 0))

	_, err := checker.IsDuplicate(ctx, "657210300", 1, hash)
	require.NoError(t, err)
	// Different msg_type and different mmsi are distinct dedup keys.
	duplicate, err := checker.IsDuplicate(ctx, "657210300", 3, hash)
	require.NoError(t, err)
	require.False(t, duplicate)
	duplicate, err = checker.IsDuplicate(ctx, "657221000", 1, hash)
	require.NoError(t, err)
	require.False(t, duplicate)
}

func TestPayloadHashStability(t *testing.T) {
	observed := time.Unix(1709315661, 0).UTC()
	first := PayloadHash("657210300", 1, 6418000, 3372500, 8400, 127500, observed)
	second := PayloadHash("657210300", 1, 6418000, 3372500, 8400, 127500, observed)
	require.Equal(t, first, second)
	require.NotEqual(t, first, PayloadHash("657210300", 1, 6418001, 3372500, 8400, 127500, observed))
}
