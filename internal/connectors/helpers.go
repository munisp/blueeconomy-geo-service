package connectors

import (
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/dedup"
)

// dedupKey builds the (mmsi, msg_type, payload_hash) window key payload for
// AIS frames.
func dedupKey(mmsi string, messageType int32, latitudeMicros, longitudeMicros int32, speedMilliknots, courseMillidegrees uint32, observedAt time.Time) string {
	return dedup.PayloadHash(mmsi, messageType, latitudeMicros, longitudeMicros, speedMilliknots, courseMillidegrees, observedAt)
}
