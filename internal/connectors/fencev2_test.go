package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

type fakeFenceV2Store struct {
	fences    []store.FenceRow
	events    []store.FenceEventRow
	failRead  bool
	failWrite bool
}

func (f *fakeFenceV2Store) ListActiveGeofencesPlatform(_ context.Context) ([]store.FenceRow, error) {
	if f.failRead {
		return nil, errors.New("connection refused")
	}
	return f.fences, nil
}

func (f *fakeFenceV2Store) InsertGeofenceEventIngest(_ context.Context, ev store.FenceEventRow) error {
	if f.failWrite {
		return errors.New("connection refused")
	}
	f.events = append(f.events, ev)
	return nil
}

type fakeFenceV2Publisher struct {
	published []any
	fail      bool
}

func (f *fakeFenceV2Publisher) PublishSignedEnvelope(_ context.Context, _ string, _ string, payload any, _ time.Time, _ string, _ map[string]string) error {
	if f.fail {
		return errors.New("kafka down")
	}
	f.published = append(f.published, payload)
	return nil
}

const fenceV2Ring = `[[-4000000,39000000],[-4000000,40000000],[-5000000,40000000],[-5000000,39000000],[-4000000,39000000]]`

func fenceV2Row() store.FenceRow {
	return store.FenceRow{
		GeofenceID: "port.mombasa.approach", Version: 1, TenantID: "tenant-a",
		Name: "Mombasa Approach", Classification: "INTERNAL",
		VerticesMicros: json.RawMessage(fenceV2Ring), State: "ACTIVE",
	}
}

func TestFenceV2IngestEmitsSignedTransition(t *testing.T) {
	fake := &fakeFenceV2Store{fences: []store.FenceRow{fenceV2Row()}}
	evaluator, err := NewFenceV2Evaluator(fake)
	require.NoError(t, err)
	publisher := &fakeFenceV2Publisher{}
	sog := uint32(3000)
	inside := store.Position{
		MMSI: "205123000", SourceClass: sign.SourceAIS,
		LatitudeMicros: -4_500_000, LongitudeMicros: 39_500_000,
		SpeedOverGroundMilliknots: &sog, ObservedAt: time.Now().UTC(), Classification: "INTERNAL",
	}
	require.NoError(t, evaluator.ObservePosition(context.Background(), publisher, inside))
	require.Len(t, publisher.published, 1)
	payload, ok := publisher.published[0].(sign.GeofenceEventRecorded)
	require.True(t, ok)
	require.Equal(t, "ENTER", payload.Event)
	require.Equal(t, "Mombasa Approach", payload.ZoneName) // G13: name, not id
	require.Len(t, fake.events, 1)
	require.Equal(t, "tenant-a", fake.events[0].TenantID)
	require.Len(t, fake.events[0].EnvelopeDigest, 64)

	// A second identical position emits nothing (no phantom transitions).
	require.NoError(t, evaluator.ObservePosition(context.Background(), publisher, inside))
	require.Len(t, publisher.published, 1)

	// Leaving the fence emits EXIT.
	outside := inside
	outside.LongitudeMicros = 41_000_000
	outside.ObservedAt = outside.ObservedAt.Add(time.Minute)
	require.NoError(t, evaluator.ObservePosition(context.Background(), publisher, outside))
	require.Len(t, publisher.published, 2)
	require.Equal(t, "EXIT", publisher.published[1].(sign.GeofenceEventRecorded).Event)
}

func TestFenceV2FailClosed(t *testing.T) {
	sog := uint32(3000)
	inside := store.Position{
		MMSI: "205123000", SourceClass: sign.SourceAIS,
		LatitudeMicros: -4_500_000, LongitudeMicros: 39_500_000,
		SpeedOverGroundMilliknots: &sog, ObservedAt: time.Now().UTC(), Classification: "INTERNAL",
	}

	// Fence store unreadable → error, nothing published/persisted.
	fake := &fakeFenceV2Store{failRead: true}
	evaluator, _ := NewFenceV2Evaluator(fake)
	publisher := &fakeFenceV2Publisher{}
	require.ErrorContains(t, evaluator.ObservePosition(context.Background(), publisher, inside), "FENCE_STORE_UNAVAILABLE")
	require.Empty(t, publisher.published)

	// Publisher down → error, nothing persisted (announce-before-persist).
	fake = &fakeFenceV2Store{fences: []store.FenceRow{fenceV2Row()}}
	evaluator, _ = NewFenceV2Evaluator(fake)
	publisher = &fakeFenceV2Publisher{fail: true}
	require.ErrorContains(t, evaluator.ObservePosition(context.Background(), publisher, inside), "FENCE_EVENT_PUBLISH_FAILED")
	require.Empty(t, fake.events)

	// Non-MMSI positions (app reports) skip silently.
	evaluator, _ = NewFenceV2Evaluator(&fakeFenceV2Store{fences: []store.FenceRow{fenceV2Row()}})
	publisher = &fakeFenceV2Publisher{}
	noMMSI := inside
	noMMSI.MMSI = ""
	noMMSI.VesselRef = "app-reporter-1"
	require.NoError(t, evaluator.ObservePosition(context.Background(), publisher, noMMSI))
	require.Empty(t, publisher.published)
}
