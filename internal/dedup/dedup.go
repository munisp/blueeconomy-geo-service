// Package dedup suppresses duplicate position reports inside a short
// tumbling window using Redis SETNX with TTL. The deduplication key is
// (mmsi, msg_type, payload_hash) per the approved architecture; the window
// is configurable between 10 and 30 seconds. Fail-closed: when Redis
// errors, the report is treated as NOT a duplicate (admitted downstream)
// and the error is surfaced — a dedup outage must never silently drop
// traffic.
package dedup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// MinWindow / MaxWindow bound the configurable dedup window (10–30 s per
	// the approved architecture).
	MinWindow = 10 * time.Second
	MaxWindow = 30 * time.Second

	keyPrefix = "geo:dedup:"
)

// Checker deduplicates reports via Redis SETNX+TTL.
type Checker struct {
	client redis.UniversalClient
	window time.Duration
}

// NewChecker builds a checker; the window is clamped fail-closed into the
// approved 10–30 second range.
func NewChecker(client redis.UniversalClient, window time.Duration) (*Checker, error) {
	if client == nil {
		return nil, errors.New("dedup redis client is required")
	}
	if window < MinWindow || window > MaxWindow {
		return nil, fmt.Errorf("dedup window %s outside approved %s..%s range", window, MinWindow, MaxWindow)
	}
	return &Checker{client: client, window: window}, nil
}

// PayloadHash is the deterministic hash of the semantic payload fields (not
// the receive timestamp) so identical re-broadcasts collapse.
func PayloadHash(mmsi string, messageType int32, latitudeMicros, longitudeMicros int32, speedMilliknots, courseMillidegrees uint32, observedAt time.Time) string {
	buffer := make([]byte, 0, 64)
	buffer = append(buffer, mmsi...)
	buffer = append(buffer, 0)
	var scratch [8]byte
	binary.BigEndian.PutUint32(scratch[:4], uint32(messageType))
	buffer = append(buffer, scratch[:4]...)
	binary.BigEndian.PutUint32(scratch[:4], uint32(latitudeMicros))
	buffer = append(buffer, scratch[:4]...)
	binary.BigEndian.PutUint32(scratch[:4], uint32(longitudeMicros))
	buffer = append(buffer, scratch[:4]...)
	binary.BigEndian.PutUint32(scratch[:4], speedMilliknots)
	buffer = append(buffer, scratch[:4]...)
	binary.BigEndian.PutUint32(scratch[:4], courseMillidegrees)
	buffer = append(buffer, scratch[:4]...)
	binary.BigEndian.PutUint64(scratch[:8], uint64(observedAt.UTC().Unix()))
	buffer = append(buffer, scratch[:8]...)
	sum := sha256.Sum256(buffer)
	return hex.EncodeToString(sum[:16])
}

// IsDuplicate reports whether an identical (mmsi, msg_type, payload_hash)
// report was already seen inside the window. A Redis error is returned
// together with duplicate=false (admit downstream, never silently drop).
func (checker *Checker) IsDuplicate(ctx context.Context, mmsi string, messageType int32, payloadHash string) (bool, error) {
	key := fmt.Sprintf("%s%s:%d:%s", keyPrefix, mmsi, messageType, payloadHash)
	set, err := checker.client.SetNX(ctx, key, 1, checker.window).Result()
	if err != nil {
		return false, fmt.Errorf("dedup SETNX %q: %w", key, err)
	}
	return !set, nil
}
