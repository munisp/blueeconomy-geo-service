package realtime

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
)

func TestHubClearanceFiltering(t *testing.T) {
	hub, err := NewHub(metrics.NewRegistry())
	require.NoError(t, err)
	public, cancelPub := hub.Subscribe([]string{"PUBLIC"})
	defer cancelPub()
	secret, cancelSec := hub.Subscribe([]string{"PUBLIC", "INTERNAL", "RESTRICTED", "CONFIDENTIAL", "SECRET"})
	defer cancelSec()

	payload, _ := json.Marshal(map[string]string{"k": "v"})
	hub.Publish(Event{Type: "geo.vessel-position.v1", Classification: "PUBLIC", OccurredAt: time.Now(), Payload: payload})
	hub.Publish(Event{Type: "geo.vessel-position.v1", Classification: "SECRET", OccurredAt: time.Now(), Payload: payload})

	// PUBLIC subscriber receives only the PUBLIC event.
	select {
	case ev := <-public:
		require.Equal(t, "PUBLIC", ev.Classification)
	case <-time.After(time.Second):
		t.Fatal("public subscriber did not receive the PUBLIC event")
	}
	select {
	case ev := <-public:
		t.Fatalf("public subscriber received a %s event above its clearance", ev.Classification)
	case <-time.After(100 * time.Millisecond):
	}
	// SECRET-cleared subscriber receives both, in order.
	for _, want := range []string{"PUBLIC", "SECRET"} {
		select {
		case ev := <-secret:
			require.Equal(t, want, ev.Classification)
		case <-time.After(time.Second):
			t.Fatalf("cleared subscriber did not receive the %s event", want)
		}
	}
	require.Equal(t, 2, hub.SubscriberCount())
}

func TestHubSlowConsumerDropped(t *testing.T) {
	hub, err := NewHub(metrics.NewRegistry())
	require.NoError(t, err)
	events, cancel := hub.Subscribe([]string{"PUBLIC"})
	defer cancel()
	payload, _ := json.Marshal(map[string]string{"k": "v"})
	// Overflow the 64-frame queue; the subscriber is dropped, not blocked on.
	for i := 0; i < 200; i++ {
		hub.Publish(Event{Type: "geo.vessel-position.v1", Classification: "PUBLIC", OccurredAt: time.Now(), Payload: payload})
	}
	require.Equal(t, 0, hub.SubscriberCount())
	// The channel is closed so ServeSSE can end the stream honestly.
	count := 0
	for range events {
		count++
	}
	require.Equal(t, 64, count)
}

func TestServeSSEHeaders(t *testing.T) {
	hub, err := NewHub(metrics.NewRegistry())
	require.NoError(t, err)
	req := httptest.NewRequest("GET", "/v1/geo/stream", nil)
	ctx, cancel := context.WithCancel(req.Context())
	cancel() // client already gone: ServeSSE writes headers then returns
	rec := httptest.NewRecorder()
	hub.ServeSSE(rec, req.WithContext(ctx), []string{"PUBLIC"})
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	require.Contains(t, rec.Body.String(), "resync")
}
