package realtime

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
)

// Publish fan-out must not serialize against concurrent Subscribe/Unsubscribe
// churn on other shards (race-checked); clearance filtering still applies.
func TestHubPublishConcurrentWithSubscribeChurn(t *testing.T) {
	hub, err := NewHub(metrics.NewRegistry())
	require.NoError(t, err)
	payload, _ := json.Marshal(map[string]string{"k": "v"})
	event := Event{Type: "geo.vessel-position.v1", Classification: "PUBLIC", OccurredAt: time.Now(), Payload: payload}

	var wait sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				select {
				case <-stop:
					return
				default:
					hub.Publish(event)
				}
			}
		}()
	}
	// Churn subscribers while publishing; every subscriber must stay
	// clearance-filtered (PUBLIC-only events here) and never block Publish.
	for i := 0; i < 200; i++ {
		events, cancel := hub.Subscribe([]string{"PUBLIC"})
		hub.Publish(event)
		select {
		case ev := <-events:
			require.Equal(t, "PUBLIC", ev.Classification)
		case <-time.After(time.Second):
			t.Fatal("subscriber did not receive a published event")
		}
		cancel()
	}
	close(stop)
	wait.Wait()
	require.Equal(t, 0, hub.SubscriberCount())
}

// A subscriber dropped for slowness is pruned while healthy subscribers on
// any shard keep receiving (drop closes the slow channel under the shard
// lock, never racing a send).
func TestHubDropDoesNotAffectHealthySubscribers(t *testing.T) {
	hub, err := NewHub(metrics.NewRegistry())
	require.NoError(t, err)
	slow, cancelSlow := hub.Subscribe([]string{"PUBLIC"})
	defer cancelSlow()
	healthy, cancelHealthy := hub.Subscribe([]string{"PUBLIC"})
	defer cancelHealthy()
	// Healthy subscriber drains continuously while the slow one overflows.
	stopDrain := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case _, open := <-healthy:
				if !open {
					return
				}
			case <-stopDrain:
				return
			}
		}
	}()
	payload, _ := json.Marshal(map[string]string{"k": "v"})
	for i := 0; i < 200; i++ { // overflow the slow subscriber's 64-frame queue
		hub.Publish(Event{Type: "geo.vessel-position.v1", Classification: "PUBLIC", OccurredAt: time.Now(), Payload: payload})
		time.Sleep(time.Millisecond) // the draining subscriber keeps up; the slow one cannot
	}
	require.Equal(t, 1, hub.SubscriberCount())
	// Slow channel closed (64 buffered frames then EOF); healthy one open.
	count := 0
	for range slow {
		count++
	}
	require.Equal(t, 64, count)
	close(stopDrain)
	<-drained
	hub.Publish(Event{Type: "geo.vessel-position.v1", Classification: "PUBLIC", OccurredAt: time.Now(), Payload: payload})
	select {
	case ev := <-healthy:
		require.Equal(t, "PUBLIC", ev.Classification)
	case <-time.After(time.Second):
		t.Fatal("healthy subscriber stopped receiving after a slow drop")
	}
	cancelHealthy()
}
