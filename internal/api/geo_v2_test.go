package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// fakeGeoV2Store is an in-memory GeoV2Store for handler tests.
type fakeGeoV2Store struct {
	mu        sync.Mutex
	fences    []store.FenceRow
	events    []store.FenceEventRow
	tracks    map[string][]store.TrackPointRow
	queue     []store.QueueObservationRow
	failReads bool
}

func (f *fakeGeoV2Store) maybeFail() error {
	if f.failReads {
		return errors.New("connection refused")
	}
	return nil
}

func (f *fakeGeoV2Store) ListActiveGeofences(_ context.Context, tenantID string, _ []string) ([]store.FenceRow, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	var out []store.FenceRow
	for _, r := range f.fences {
		if r.TenantID == tenantID && r.State == "ACTIVE" {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeGeoV2Store) GetGeofenceHistory(_ context.Context, tenantID, id string) ([]store.FenceRow, error) {
	var out []store.FenceRow
	for _, r := range f.fences {
		if r.TenantID == tenantID && r.GeofenceID == id {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return nil, pgx.ErrNoRows
	}
	return out, nil
}

func (f *fakeGeoV2Store) CreateGeofenceVersion(_ context.Context, row store.FenceRow, expected int) (store.FenceRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	latest := 0
	for _, r := range f.fences {
		if r.GeofenceID == row.GeofenceID && r.TenantID == row.TenantID && r.Version > latest {
			latest = r.Version
		}
	}
	if latest != expected {
		return store.FenceRow{}, fmt.Errorf("VERSION_CONFLICT: latest version is %d, expected %d", latest, expected)
	}
	for i, r := range f.fences {
		if r.GeofenceID == row.GeofenceID && r.TenantID == row.TenantID && r.State == "ACTIVE" {
			f.fences[i].State = "RETIRED"
		}
	}
	row.Version = latest + 1
	row.State = "ACTIVE"
	row.CreatedAt = time.Now()
	f.fences = append(f.fences, row)
	return row, nil
}

func (f *fakeGeoV2Store) RetireGeofence(_ context.Context, tenantID, id string) error {
	for i, r := range f.fences {
		if r.GeofenceID == id && r.TenantID == tenantID && r.State == "ACTIVE" {
			f.fences[i].State = "RETIRED"
			return nil
		}
	}
	return pgx.ErrNoRows
}

func (f *fakeGeoV2Store) InsertGeofenceEvent(_ context.Context, ev store.FenceEventRow) error {
	f.events = append(f.events, ev)
	return nil
}

func (f *fakeGeoV2Store) ListGeofenceEvents(_ context.Context, tenantID, id string, _ []string, _ int) ([]store.FenceEventRow, error) {
	var out []store.FenceEventRow
	for _, e := range f.events {
		if e.TenantID == tenantID && e.GeofenceID == id && e.EnvelopeDigest != "" {
			out = append(out, e)
		}
	}
	return out, nil
}

func (f *fakeGeoV2Store) QueryTrack(_ context.Context, mmsi string, from, to time.Time, _ []string, _ int) ([]store.TrackPointRow, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	var out []store.TrackPointRow
	for _, p := range f.tracks[mmsi] {
		if !p.ObservedAt.Before(from) && !p.ObservedAt.After(to) {
			out = append(out, p)
		}
	}
	return out, nil
}

func (f *fakeGeoV2Store) NearestVessels(_ context.Context, _, _ int32, _ float64, _ []string, _ int) ([]store.NearestVesselRow, error) {
	return []store.NearestVesselRow{{
		MMSI: "205123000", ShipName: "TEST VESSEL", LatitudeMicros: -4_100_000, LongitudeMicros: 39_700_000,
		SogMilliknots: 10_000, DistanceMeters: 6000, ObservedAt: time.Now().Add(-time.Minute),
	}}, nil
}

func (f *fakeGeoV2Store) LatestPosition(_ context.Context, mmsi string, _ []string) (store.TrackPointRow, error) {
	if pts := f.tracks[mmsi]; len(pts) > 0 {
		return pts[len(pts)-1], nil
	}
	return store.TrackPointRow{}, pgx.ErrNoRows
}

func (f *fakeGeoV2Store) QueueObservations(_ context.Context, code string, _ time.Time, _ int) ([]store.QueueObservationRow, error) {
	var out []store.QueueObservationRow
	for _, r := range f.queue {
		if r.PortCode == code {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeGeoV2Store) InsertQueueObservation(_ context.Context, row store.QueueObservationRow) error {
	f.queue = append(f.queue, row)
	return nil
}

// recordingPublisher records signed-envelope publications.
type recordingPublisher struct {
	mu        sync.Mutex
	published []string
	fail      bool
}

func (r *recordingPublisher) PublishSignedEnvelope(_ context.Context, eventType, _ string, _ any, _ time.Time, _ string, _ map[string]string) error {
	if r.fail {
		return errors.New("kafka down")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.published = append(r.published, eventType)
	return nil
}

func testPrincipalCtx() context.Context {
	return auth.WithPrincipal(context.Background(), auth.Principal{
		Subject: "tester", Roles: map[string]struct{}{"geo-admin": {}}, Clearance: "SECRET", TenantID: "tenant-a",
	})
}

func newTestGeoV2(fake *fakeGeoV2Store) *GeoV2 {
	g, err := NewGeoV2(fake)
	if err != nil {
		panic(err)
	}
	return g
}

const testRing = `[[-4000000,39000000],[-4000000,40000000],[-5000000,40000000],[-5000000,39000000],[-4000000,39000000]]`

func TestFenceCRUDVersioning(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)

	body := `{"geofenceId":"port.mombasa.approach","name":"Mombasa Approach","classification":"INTERNAL","verticesMicros":` + testRing + `,"dwellThresholdSeconds":600,"dwellSpeedGateMilliknots":1000}`
	req := httptest.NewRequest("POST", "/v1/geo/fences", strings.NewReader(body)).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	g.createFence(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"version":1`)

	// Second create without version path conflicts (fence exists).
	rec = httptest.NewRecorder()
	g.createFence(rec, httptest.NewRequest("POST", "/v1/geo/fences", strings.NewReader(body)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusConflict, rec.Code)

	// New version via the versions endpoint with correct expectedVersion.
	req = httptest.NewRequest("POST", "/v1/geo/fences/port.mombasa.approach/versions",
		strings.NewReader(strings.Replace(body, `"dwellThresholdSeconds":600`, `"dwellThresholdSeconds":900,"expectedVersion":1`, 1))).WithContext(testPrincipalCtx())
	req.SetPathValue("id", "port.mombasa.approach")
	rec = httptest.NewRecorder()
	g.createFenceVersion(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"version":2`)

	// History shows both versions; only v2 active.
	req = httptest.NewRequest("GET", "/v1/geo/fences/port.mombasa.approach", nil).WithContext(testPrincipalCtx())
	req.SetPathValue("id", "port.mombasa.approach")
	rec = httptest.NewRecorder()
	g.getFenceHistory(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"version":2`)
	active, err := fake.ListActiveGeofences(context.Background(), "tenant-a", nil)
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, 2, active[0].Version)
	require.Equal(t, 900, active[0].DwellThresholdSeconds)

	// Stale expectedVersion is rejected.
	req = httptest.NewRequest("POST", "/v1/geo/fences/port.mombasa.approach/versions",
		strings.NewReader(strings.Replace(body, `"dwellThresholdSeconds":600`, `"expectedVersion":1`, 1))).WithContext(testPrincipalCtx())
	req.SetPathValue("id", "port.mombasa.approach")
	rec = httptest.NewRecorder()
	g.createFenceVersion(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code)

	// Retire.
	req = httptest.NewRequest("POST", "/v1/geo/fences/port.mombasa.approach/retire", nil).WithContext(testPrincipalCtx())
	req.SetPathValue("id", "port.mombasa.approach")
	rec = httptest.NewRecorder()
	g.retireFence(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestFenceValidationRejectsBadGeometry(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)
	openRing := `{"geofenceId":"bad","name":"Bad","classification":"INTERNAL","verticesMicros":[[-4000000,39000000],[-4000000,40000000],[-5000000,40000000],[-5000000,39000000]]}`
	rec := httptest.NewRecorder()
	g.createFence(rec, httptest.NewRequest("POST", "/v1/geo/fences", strings.NewReader(openRing)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "closed")
}

func TestEvaluatePublishesAndPersistsFenceEvents(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)
	pub := &recordingPublisher{}
	g.FenceEvents = pub

	create := `{"geofenceId":"anchorage.kilindini","name":"Kilindini","classification":"INTERNAL","verticesMicros":` + testRing + `}`
	rec := httptest.NewRecorder()
	g.createFence(rec, httptest.NewRequest("POST", "/v1/geo/fences", strings.NewReader(create)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusCreated, rec.Code)

	// Report outside → inside: one ENTER.
	eval := `{"reports":[
		{"mmsi":"205123000","latMicros":-6000000,"lonMicros":39500000,"sogMilliknots":5000,"observedAtUnix":1000},
		{"mmsi":"205123000","latMicros":-4500000,"lonMicros":39500000,"sogMilliknots":5000,"observedAtUnix":1010}]}`
	rec = httptest.NewRecorder()
	g.evaluatePositions(rec, httptest.NewRequest("POST", "/v1/geo/fences/evaluate", strings.NewReader(eval)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"ENTER"`)
	require.Equal(t, []string{"geo.geofence-event.v1"}, pub.published)
	require.Len(t, fake.events, 1)
	require.Equal(t, "ENTER", fake.events[0].EventType)
	require.Len(t, fake.events[0].EnvelopeDigest, 64, "events must carry a provenance digest")
}

func TestEvaluateFailsClosedWithoutPublisher(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake) // FenceEvents unwired
	eval := `{"reports":[{"mmsi":"205123000","latMicros":-4500000,"lonMicros":39500000,"sogMilliknots":5000,"observedAtUnix":1000}]}`
	rec := httptest.NewRecorder()
	g.evaluatePositions(rec, httptest.NewRequest("POST", "/v1/geo/fences/evaluate", strings.NewReader(eval)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), "FENCE_EVENTS_UNWIRED")
	require.Empty(t, fake.events, "nothing persists when the publisher is unwired")
}

func TestEvaluateRejectsBadReportsIndividually(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)
	g.FenceEvents = &recordingPublisher{}
	eval := `{"reports":[{"mmsi":"bad","latMicros":0,"lonMicros":0,"sogMilliknots":0,"observedAtUnix":1000}]}`
	rec := httptest.NewRecorder()
	g.evaluatePositions(rec, httptest.NewRequest("POST", "/v1/geo/fences/evaluate", strings.NewReader(eval)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "mmsi must be 9 digits")
}

func TestQueryTrackReportsGaps(t *testing.T) {
	fake := &fakeGeoV2Store{tracks: map[string][]store.TrackPointRow{
		"205123000": {
			{LatitudeMicros: -4_000_000, LongitudeMicros: 39_000_000, SogMilliknots: 8000, ObservedAt: time.Unix(1000, 0)},
			{LatitudeMicros: -4_010_000, LongitudeMicros: 39_010_000, SogMilliknots: 8000, ObservedAt: time.Unix(1060, 0)},
			{LatitudeMicros: -4_100_000, LongitudeMicros: 39_100_000, SogMilliknots: 8000, ObservedAt: time.Unix(5000, 0)},
		},
	}}
	g := newTestGeoV2(fake)
	req := httptest.NewRequest("GET", "/v1/geo/tracks/205123000?from=900&to=6000&maxGapSeconds=300", nil).WithContext(testPrincipalCtx())
	req.SetPathValue("mmsi", "205123000")
	rec := httptest.NewRecorder()
	g.queryTrack(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"pointCount":3`)
	require.Contains(t, rec.Body.String(), `"gapCount":1`)
	require.Contains(t, rec.Body.String(), `"provenance"`)

	// Empty window: honest no-data message.
	req = httptest.NewRequest("GET", "/v1/geo/tracks/205123000?from=9000&to=9999", nil).WithContext(testPrincipalCtx())
	req.SetPathValue("mmsi", "205123000")
	rec = httptest.NewRecorder()
	g.queryTrack(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "NO_TRACK_DATA")

	// Store down: 503 fail-closed.
	fake.failReads = true
	req = httptest.NewRequest("GET", "/v1/geo/tracks/205123000?from=900&to=6000", nil).WithContext(testPrincipalCtx())
	req.SetPathValue("mmsi", "205123000")
	rec = httptest.NewRecorder()
	g.queryTrack(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestPortApproachETAConfidenceLabelled(t *testing.T) {
	fake := &fakeGeoV2Store{tracks: map[string][]store.TrackPointRow{
		"205123000": {{LatitudeMicros: -4_062_000 + 166_667, LongitudeMicros: 39_672_000, SogMilliknots: 10_000, ObservedAt: time.Now().Add(-time.Minute)}},
	}}
	g := newTestGeoV2(fake)
	req := httptest.NewRequest("GET", "/v1/geo/ports/kemba/approaches?mmsi=205123000", nil).WithContext(testPrincipalCtx())
	req.SetPathValue("code", "kemba")
	rec := httptest.NewRecorder()
	g.portApproaches(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"confidence":"HIGH"`)
	require.Contains(t, rec.Body.String(), `"etaAt"`)

	// Unknown port.
	req = httptest.NewRequest("GET", "/v1/geo/ports/XXXXX/approaches", nil).WithContext(testPrincipalCtx())
	req.SetPathValue("code", "XXXXX")
	rec = httptest.NewRecorder()
	g.portApproaches(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCongestionForecastInsufficientHistoryHonest(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)
	req := httptest.NewRequest("GET", "/v1/geo/ports/KEMBA/congestion/forecast", nil).WithContext(testPrincipalCtx())
	req.SetPathValue("code", "KEMBA")
	rec := httptest.NewRecorder()
	g.congestionForecast(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code)
	require.Contains(t, rec.Body.String(), "INSUFFICIENT_HISTORY")
}

func TestCongestionForecastServesBaseline(t *testing.T) {
	fake := &fakeGeoV2Store{}
	now := time.Now().Truncate(time.Hour)
	for i := 0; i < 24*10; i++ {
		fake.queue = append(fake.queue, store.QueueObservationRow{
			PortCode: "KEMBA", QueueLength: 8 + i%5, Source: "test", ObservedAt: now.Add(time.Duration(i-24*10) * time.Hour),
		})
	}
	g := newTestGeoV2(fake)
	req := httptest.NewRequest("GET", "/v1/geo/ports/KEMBA/congestion/forecast?horizonHours=12", nil).WithContext(testPrincipalCtx())
	req.SetPathValue("code", "KEMBA")
	rec := httptest.NewRecorder()
	g.congestionForecast(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "baseline")
	require.Contains(t, rec.Body.String(), `"backtestMAE"`)

	var decoded struct {
		Forecast struct {
			Model  string `json:"model"`
			Points []struct {
				QueueLength float64 `json:"queueLength"`
				Lower95     float64 `json:"lower95"`
				Upper95     float64 `json:"upper95"`
			} `json:"points"`
		} `json:"forecast"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &decoded))
	require.Len(t, decoded.Forecast.Points, 12)
	require.Contains(t, decoded.Forecast.Model, "baseline")
}

func TestNearestVesselsProvenanceStamped(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)
	req := httptest.NewRequest("GET", "/v1/geo/vessels/nearest?lat=-4062000&lon=39672000", nil).WithContext(testPrincipalCtx())
	rec := httptest.NewRecorder()
	g.nearestVessels(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"source":"ais_positions"`)
}
