// Package realtime is the in-process SSE fan-out hub (G1): the ingest
// pipeline publishes validated vessel positions and fence transitions into
// the hub, and authenticated subscribers receive them over
// GET /v1/geo/stream (Server-Sent Events). The hub is deliberately
// best-effort OFF the hot path: Publish never blocks the pipeline, slow
// subscribers are dropped with a metric (geo_sse_dropped_total), and the
// stream is a live-notification channel only — the REST read model remains
// the source of truth (a missed SSE frame is recovered by the next poll).
//
// Clearance doctrine: every event carries its classification; a subscriber
// receives only events their clearance covers (same ladder as the REST
// reads). No event is ever synthesized by the hub.
package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
)

// Event is one fan-out unit. Payload is the already-built JSON document
// (the signed-envelope payload or a compact projection of it); the hub
// never mutates it.
type Event struct {
	Type           string          `json:"type"` // geo.vessel-position.v1 | geo.geofence-event.v1 | ...
	Classification string          `json:"classification"`
	OccurredAt     time.Time       `json:"occurredAt"`
	Payload        json.RawMessage `json:"payload"`
}

type subscriber struct {
	events   chan Event
	cleared  map[string]struct{}
}

// hubShardCount bounds lock contention: subscribers are striped over this
// many shards, each with its own mutex, so Publish fan-out and
// Subscribe/Unsubscribe serialize only within one shard (1/16 of the
// subscriber base) instead of globally. Sends happen under the shard lock,
// so closing a channel on drop/unsubscribe can never race a send.
const hubShardCount = 16

type hubShard struct {
	mu   sync.Mutex
	subs map[uint64]subscriber
}

// Hub is a goroutine-safe broadcast hub with bounded per-subscriber queues.
type Hub struct {
	shards  [hubShardCount]hubShard
	nextID  atomic.Uint64
	queue   int
	metrics *metrics.Registry
	now     func() time.Time
}

func (hub *Hub) shard(id uint64) *hubShard {
	return &hub.shards[id%hubShardCount]
}

// NewHub wires the hub against the metrics registry.
func NewHub(registry *metrics.Registry) (*Hub, error) {
	if registry == nil {
		return nil, errors.New("realtime hub metrics registry is required")
	}
	hub := &Hub{queue: 64, metrics: registry, now: time.Now}
	for i := range hub.shards {
		hub.shards[i].subs = map[uint64]subscriber{}
	}
	return hub, nil
}

// Publish fans one event out to every cleared subscriber. Non-blocking: a
// subscriber whose queue is full is dropped (the client must re-poll the
// REST read model to re-sync — the stream contract says so).
func (hub *Hub) Publish(event Event) {
	if event.Type == "" {
		return
	}
	// Per-shard fan-out: each shard is locked only for its own non-blocking
	// sends, so Subscribe/Unsubscribe on other shards never waits on this
	// publish and a slow-subscriber drop (close under the same lock) cannot
	// race a send.
	dropped := 0
	count := 0
	for i := range hub.shards {
		shard := &hub.shards[i]
		shard.mu.Lock()
		for id, sub := range shard.subs {
			if _, ok := sub.cleared[event.Classification]; !ok {
				continue // clearance floor: the subscriber never sees what REST hides
			}
			select {
			case sub.events <- event:
			default:
				close(sub.events)
				delete(shard.subs, id)
				dropped++
			}
		}
		count += len(shard.subs)
		shard.mu.Unlock()
	}
	hub.metrics.Inc("geo_sse_events_total", map[string]string{"event_type": event.Type})
	if dropped > 0 {
		hub.metrics.Add("geo_sse_dropped_total", nil, int64(dropped))
	}
	hub.metrics.Set("geo_sse_subscribers", nil, int64(count))
}

// Subscribe registers a subscriber cleared for the given classification
// labels and returns its event channel plus an unsubscribe func.
func (hub *Hub) Subscribe(clearedLabels []string) (<-chan Event, func()) {
	cleared := make(map[string]struct{}, len(clearedLabels))
	for _, label := range clearedLabels {
		cleared[label] = struct{}{}
	}
	events := make(chan Event, hub.queue)
	id := hub.nextID.Add(1)
	shard := hub.shard(id)
	shard.mu.Lock()
	shard.subs[id] = subscriber{events: events, cleared: cleared}
	shard.mu.Unlock()
	count := hub.SubscriberCount()
	hub.metrics.Set("geo_sse_subscribers", nil, int64(count))
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			shard.mu.Lock()
			if sub, ok := shard.subs[id]; ok {
				close(sub.events)
				delete(shard.subs, id)
			}
			shard.mu.Unlock()
			hub.metrics.Set("geo_sse_subscribers", nil, int64(hub.SubscriberCount()))
		})
	}
	return events, cancel
}

// SubscriberCount reports the live subscriber gauge (status endpoint).
func (hub *Hub) SubscriberCount() int {
	total := 0
	for i := range hub.shards {
		hub.shards[i].mu.Lock()
		total += len(hub.shards[i].subs)
		hub.shards[i].mu.Unlock()
	}
	return total
}

// heartbeatInterval keeps proxies and clients alive on quiet feeds.
const heartbeatInterval = 25 * time.Second

// ServeSSE streams events to one authenticated subscriber until the request
// context ends. clearedLabels is the subscriber's clearance-covered label
// set (computed by the API boundary).
func (hub *Hub) ServeSSE(writer http.ResponseWriter, request *http.Request, clearedLabels []string) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		http.Error(writer, `{"error":"streaming unsupported"}`, http.StatusInternalServerError)
		return
	}
	events, cancel := hub.Subscribe(clearedLabels)
	defer cancel()
	header := writer.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)
	// The opening comment documents the resync contract for the client.
	fmt.Fprintf(writer, ": geo SSE stream; notifications only — the REST read model is the source of truth, re-poll to resync after any gap\n\n")
	flusher.Flush()
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-request.Context().Done():
			return
		case event, open := <-events:
			if !open {
				// Dropped for slowness: say so explicitly, then end the
				// stream so the client re-syncs via REST before resuming.
				fmt.Fprintf(writer, "event: stream-error\ndata: {\"error\":\"SLOW_CONSUMER: event queue overflowed, re-sync via REST and reconnect\"}\n\n")
				flusher.Flush()
				return
			}
			raw, err := json.Marshal(event)
			if err != nil {
				continue
			}
			fmt.Fprintf(writer, "event: %s\ndata: %s\n\n", event.Type, raw)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprintf(writer, ": heartbeat %s\n\n", hub.now().UTC().Format(time.RFC3339))
			flusher.Flush()
		}
	}
}

// BroadcastPipeline adapts a hub to the connectors.Pipeline broadcast hook
// (positions + geofence transitions). Payloads are marshalled once;
// marshalling failure drops the notification with a metric (the pipeline's
// authoritative Kafka publish has already succeeded by this point).
func BroadcastPipeline(hub *Hub, metricsRegistry *metrics.Registry) func(eventType, classification string, occurredAt time.Time, payload any) {
	return func(eventType, classification string, occurredAt time.Time, payload any) {
		if _, err := sign.ParseClassification(classification); err != nil {
			metricsRegistry.Inc("geo_sse_dropped_total", nil)
			return
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			metricsRegistry.Inc("geo_sse_dropped_total", nil)
			return
		}
		hub.Publish(Event{Type: eventType, Classification: classification, OccurredAt: occurredAt.UTC(), Payload: raw})
	}
}

// Shutdown releases all subscribers (server teardown).
func (hub *Hub) Shutdown(_ context.Context) {
	for i := range hub.shards {
		shard := &hub.shards[i]
		shard.mu.Lock()
		for id, sub := range shard.subs {
			close(sub.events)
			delete(shard.subs, id)
		}
		shard.mu.Unlock()
	}
}
