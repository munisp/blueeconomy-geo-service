// Phase-7 realtime-schedule tests: transit registry CRUD + RLS scoping,
// GTFS static zip from PG, GTFS-RT protobuf round-trip, staleness
// omission, computed ETAs and alerts. Same infrastructure gates as
// pipeline_integration_test.go.
package integration

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	gtfs "github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/munisp/blueeconomy-geo-service/internal/api"
	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	geogtfs "github.com/munisp/blueeconomy-geo-service/internal/gtfs"
	"github.com/munisp/blueeconomy-geo-service/internal/gtfsrt"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

const (
	transitTenant  = "itest-tenant"
	transitTenant2 = "itest-tenant-2"
	// Straight route at (1.0, 3.0) → (1.01, 3.0): 1111.95 m due north.
	stopALat, stopALon = 1_000_000, 3_000_000
	stopBLat, stopBLon = 1_010_000, 3_000_000
)

// cleanTransit removes every itest transit row (tenant-bound, honoring
// RLS) plus the shared-plane position rows of the test MMSIs.
func cleanTransit(t *testing.T, h *harness) {
	t.Helper()
	ctx := context.Background()
	for _, tenant := range []string{transitTenant, transitTenant2} {
		require.NoError(t, h.store.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			// Tenant-bound transactions: every statement is implicitly
			// scoped to this tenant by RLS, so a full wipe is safe and
			// covers rows whose IDs do not carry the itest- prefix (e.g.
			// uuid alert IDs from the HTTP test).
			for _, statement := range []string{
				`DELETE FROM transit_alerts`,
				`DELETE FROM transit_route_vessels`,
				`DELETE FROM transit_stop_times`,
				`DELETE FROM transit_trips`,
				`DELETE FROM transit_calendars`,
				`DELETE FROM transit_stops`,
				`DELETE FROM transit_routes`,
				`DELETE FROM transit_agencies`,
			} {
				if _, err := tx.Exec(ctx, statement); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	for _, statement := range []string{
		`DELETE FROM latest_positions WHERE mmsi LIKE '657990%'`,
		`DELETE FROM ais_positions WHERE mmsi LIKE '657990%'`,
	} {
		_, err := h.store.Pool().Exec(ctx, statement)
		require.NoError(t, err)
	}
}

// seedTransit loads the two-stop straight route with an all-day trip.
func seedTransit(t *testing.T, h *harness, tenant string, assignments ...string) {
	t.Helper()
	ctx := context.Background()
	weekdays := [7]bool{true, true, true, true, true, true, true}
	require.NoError(t, h.store.UpsertTransitAgency(ctx, tenant, store.TransitAgency{
		AgencyID: "itest-agency", Name: "ITest Waterways", URL: "https://example.test",
		Timezone: "UTC", Lang: "en",
	}))
	require.NoError(t, h.store.UpsertTransitRoute(ctx, tenant, store.TransitRoute{
		RouteID: "itest-route-1", AgencyID: "itest-agency", ShortName: "T1",
		LongName: "ITest Straight Route", RouteType: 4, DefaultSpeedMilliknots: 8000, Active: true,
	}))
	require.NoError(t, h.store.UpsertTransitStop(ctx, tenant, store.TransitStop{
		StopID: "itest-stop-a", Name: "Alpha Jetty", LatitudeMicros: stopALat, LongitudeMicros: stopALon,
	}))
	require.NoError(t, h.store.UpsertTransitStop(ctx, tenant, store.TransitStop{
		StopID: "itest-stop-b", Name: "Bravo Jetty", LatitudeMicros: stopBLat, LongitudeMicros: stopBLon,
	}))
	require.NoError(t, h.store.UpsertTransitCalendar(ctx, tenant, store.TransitCalendar{
		ServiceID: "itest-daily", Weekdays: weekdays,
		StartDate: time.Now().UTC().Add(-24 * time.Hour), EndDate: time.Now().UTC().Add(24 * time.Hour),
	}))
	require.NoError(t, h.store.UpsertTransitTrip(ctx, tenant, store.TransitTrip{
		TripID: "itest-trip-1", RouteID: "itest-route-1", ServiceID: "itest-daily", Headsign: "Bravo",
	}))
	require.NoError(t, h.store.ReplaceTransitStopTimes(ctx, tenant, "itest-trip-1", []store.TransitStopTime{
		{TripID: "itest-trip-1", StopSequence: 1, StopID: "itest-stop-a", ArrivalSeconds: 60, DepartureSeconds: 60},
		{TripID: "itest-trip-1", StopSequence: 2, StopID: "itest-stop-b", ArrivalSeconds: 86340, DepartureSeconds: 86340},
	}))
	for _, mmsi := range assignments {
		require.NoError(t, h.store.AssignRouteVessel(ctx, tenant, store.RouteVessel{
			RouteID: "itest-route-1", MMSI: mmsi,
		}))
	}
}

// reportPosition inserts one AIS position row (speed sample) and refreshes
// the hot latest_positions row.
func reportPosition(t *testing.T, h *harness, mmsi string, latMicros, lonMicros int32,
	speedMilliknots *uint32, observedAt time.Time) {
	t.Helper()
	position := store.Position{
		PositionReportID:          "itest-transit-" + mmsi + "-" + observedAt.Format("150405.000000"),
		MMSI:                      mmsi,
		SourceClass:               "AIS",
		LatitudeMicros:            latMicros,
		LongitudeMicros:           lonMicros,
		SpeedOverGroundMilliknots: speedMilliknots,
		Classification:            "PUBLIC",
		ObservedAt:                observedAt.UTC().Truncate(time.Microsecond),
		ReceiverID:                "itest-rx",
	}
	require.NoError(t, h.store.InsertPositions(ctx, []store.Position{position}))
	require.NoError(t, h.store.UpsertLatestPosition(ctx, position))
}

var ctx = context.Background()

func unmarshalFeed(t *testing.T, payload []byte) *gtfs.FeedMessage {
	t.Helper()
	feed := &gtfs.FeedMessage{}
	require.NoError(t, proto.Unmarshal(payload, feed))
	require.NotNil(t, feed.Header)
	require.Equal(t, "2.0", feed.Header.GetGtfsRealtimeVersion())
	require.Equal(t, gtfs.FeedHeader_FULL_DATASET, feed.Header.GetIncrementality())
	require.Greater(t, feed.Header.GetTimestamp(), uint64(0))
	return feed
}

func newFeedBuilder(t *testing.T, h *harness) (*gtfsrt.Builder, *metrics.Registry) {
	t.Helper()
	registry := metrics.NewRegistry()
	builder, err := gtfsrt.NewBuilder(h.store, registry, gtfsrt.Config{})
	require.NoError(t, err)
	return builder, registry
}

// TestTransitRegistryCRUDAndRLS proves the registry is tenant-scoped
// (default-deny), updatable, deletable and seedable.
func TestTransitRegistryCRUDAndRLS(t *testing.T) {
	h := newHarness(t)
	cleanTransit(t, h)

	// Create via the seed loader (also covers YAML/JSON parsing path at
	// the store level).
	seedTransit(t, h, transitTenant, "657990001")
	registry, err := h.store.LoadTransitRegistry(ctx, transitTenant)
	require.NoError(t, err)
	require.Len(t, registry.Agencies, 1)
	require.Len(t, registry.Routes, 1)
	require.Len(t, registry.Stops, 2)
	require.Len(t, registry.Trips, 1)
	require.Len(t, registry.StopTimes, 2)
	require.Len(t, registry.Assignments, 1)

	// RLS: the second tenant sees nothing; an unbound session is denied.
	other, err := h.store.LoadTransitRegistry(ctx, transitTenant2)
	require.NoError(t, err)
	require.Empty(t, other.Routes)
	require.Empty(t, other.Stops)
	var visible int
	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM transit_routes WHERE route_id LIKE 'itest-%'`).Scan(&visible))
	require.Zero(t, visible, "unbound session must read zero registry rows (default deny)")
	_, err = h.store.Pool().Exec(ctx, `INSERT INTO transit_routes
		(route_id, tenant_id, agency_id, long_name) VALUES ('itest-unbound', 'itest-tenant', 'itest-agency', 'x')`)
	require.Error(t, err, "unbound session must not write registry rows")

	// Update (upsert): rename + retire the route.
	require.NoError(t, h.store.UpsertTransitRoute(ctx, transitTenant, store.TransitRoute{
		RouteID: "itest-route-1", AgencyID: "itest-agency", ShortName: "T1",
		LongName: "ITest Renamed Route", RouteType: 4, DefaultSpeedMilliknots: 8000, Active: false,
	}))
	registry, err = h.store.LoadTransitRegistry(ctx, transitTenant)
	require.NoError(t, err)
	require.Equal(t, "ITest Renamed Route", registry.Routes[0].LongName)
	require.False(t, registry.Routes[0].Active)

	// Monotonic stop_times are enforced at the storage boundary.
	err = h.store.ReplaceTransitStopTimes(ctx, transitTenant, "itest-trip-1", []store.TransitStopTime{
		{TripID: "itest-trip-1", StopSequence: 1, StopID: "itest-stop-a", ArrivalSeconds: 500, DepartureSeconds: 500},
		{TripID: "itest-trip-1", StopSequence: 2, StopID: "itest-stop-b", ArrivalSeconds: 400, DepartureSeconds: 400},
	})
	require.Error(t, err, "non-monotonic stop_times must be rejected")

	// Delete cascades trips/stop_times/assignments.
	require.NoError(t, h.store.DeleteTransitRoute(ctx, transitTenant, "itest-route-1"))
	registry, err = h.store.LoadTransitRegistry(ctx, transitTenant)
	require.NoError(t, err)
	require.Empty(t, registry.Routes)
	require.Empty(t, registry.Trips)
	require.Empty(t, registry.StopTimes)
	require.Empty(t, registry.Assignments)
}

// TestGTFSStaticFeedIntegration builds the zip from the PG-backed
// registry and asserts structural validity.
func TestGTFSStaticFeedIntegration(t *testing.T) {
	h := newHarness(t)
	cleanTransit(t, h)
	seedTransit(t, h, transitTenant, "657990001")

	registry, err := h.store.LoadTransitRegistry(ctx, transitTenant)
	require.NoError(t, err)
	payload, etag, err := geogtfs.BuildStaticZip(registry)
	require.NoError(t, err)
	require.NotEmpty(t, etag)

	reader, err := zip.NewReader(bytes.NewReader(payload), int64(len(payload)))
	require.NoError(t, err)
	files := map[string]bool{}
	var stopTimesCSV [][]string
	for _, file := range reader.File {
		files[file.Name] = true
		if file.Name == "stop_times.txt" {
			handle, err := file.Open()
			require.NoError(t, err)
			stopTimesCSV, err = csv.NewReader(handle).ReadAll()
			require.NoError(t, err)
			require.NoError(t, handle.Close())
		}
	}
	for _, required := range []string{"agency.txt", "routes.txt", "stops.txt", "trips.txt", "stop_times.txt", "calendar.txt"} {
		require.True(t, files[required], "required GTFS file %s", required)
	}
	require.Len(t, stopTimesCSV, 3, "header + 2 visits")
	require.Equal(t, "00:01:00", stopTimesCSV[1][1])
	require.Equal(t, "23:59:00", stopTimesCSV[2][1])
}

// TestGTFSRTVehiclePositionsStaleness proves the STALENESS RULE: fresh
// positions are emitted (MMSI = vehicle.id), stale or missing positions
// are OMITTED with a counted reason — never interpolated.
func TestGTFSRTVehiclePositionsStaleness(t *testing.T) {
	h := newHarness(t)
	cleanTransit(t, h)
	seedTransit(t, h, transitTenant, "657990001", "657990002", "657990003")
	builder, feedMetrics := newFeedBuilder(t, h)
	cleared := []string{"PUBLIC"}
	now := time.Now().UTC()

	// Fresh position, on-route midpoint, moving.
	speed := uint32(10000)
	reportPosition(t, h, "657990001", 1_005_000, 3_000_000, &speed, now.Add(-5*time.Second))
	// Stale position (300 s > 120 s threshold).
	reportPosition(t, h, "657990002", 1_005_000, 3_000_000, &speed, now.Add(-300*time.Second))
	// 657990003 has no position at all.

	before := feedMetrics.Snapshot()
	payload, err := builder.BuildVehiclePositions(ctx, transitTenant, cleared)
	require.NoError(t, err)
	feed := unmarshalFeed(t, payload)
	require.Len(t, feed.Entity, 1, "only the fresh vessel may appear — honest absence otherwise")
	entity := feed.Entity[0]
	require.Equal(t, "657990001", entity.GetVehicle().GetVehicle().GetId())
	require.Equal(t, "itest-trip-1", entity.GetVehicle().GetTrip().GetTripId())
	require.InDelta(t, 1.005, entity.GetVehicle().GetPosition().GetLatitude(), 1e-6)
	require.InDelta(t, 3.0, entity.GetVehicle().GetPosition().GetLongitude(), 1e-6)
	require.InDelta(t, 5.144, entity.GetVehicle().GetPosition().GetSpeed(), 0.01)
	require.Equal(t, uint32(2), entity.GetVehicle().GetCurrentStopSequence(),
		"mid-route vessel must be IN_TRANSIT_TO the remaining stop")
	require.Equal(t, gtfs.VehiclePosition_IN_TRANSIT_TO, entity.GetVehicle().GetCurrentStatus())

	after := feedMetrics.Snapshot()
	require.Equal(t, int64(1), after[`geo_gtfsrt_entities_omitted_total{feed="vehiclepositions",reason="stale"}`]-
		before[`geo_gtfsrt_entities_omitted_total{feed="vehiclepositions",reason="stale"}`])
	require.Equal(t, int64(1), after[`geo_gtfsrt_entities_omitted_total{feed="vehiclepositions",reason="no_position"}`]-
		before[`geo_gtfsrt_entities_omitted_total{feed="vehiclepositions",reason="no_position"}`])
	require.Equal(t, int64(1), after[`geo_gtfsrt_entities_emitted_total{feed="vehiclepositions"}`]-
		before[`geo_gtfsrt_entities_emitted_total{feed="vehiclepositions"}`])
}

// TestGTFSRTTripUpdatesETA proves computed ETAs on a synthetic straight
// track, the stationary-vessel omission, and the low-confidence fallback.
func TestGTFSRTTripUpdatesETA(t *testing.T) {
	h := newHarness(t)
	cleanTransit(t, h)
	seedTransit(t, h, transitTenant, "657990011", "657990012", "657990013")
	builder, feedMetrics := newFeedBuilder(t, h)
	cleared := []string{"PUBLIC"}
	now := time.Now().UTC().Truncate(time.Second)

	// Vessel 1: midpoint, five consistent 10 kn samples → live ETA.
	moving := uint32(10000)
	for i := 0; i < 5; i++ {
		reportPosition(t, h, "657990011", 1_005_000, 3_000_000, &moving, now.Add(time.Duration(-i)*time.Second))
	}
	// Vessel 2: stationary (0 kn samples).
	stationary := uint32(0)
	for i := 0; i < 5; i++ {
		reportPosition(t, h, "657990012", 1_005_000, 3_000_000, &stationary, now.Add(time.Duration(-i)*time.Second))
	}
	// Vessel 3: fresh position, NO speed observations → fallback at the
	// route default speed (8 kn), marked low-confidence.
	reportPosition(t, h, "657990013", 1_005_000, 3_000_000, nil, now.Add(-2*time.Second))

	before := feedMetrics.Snapshot()
	payload, err := builder.BuildTripUpdates(ctx, transitTenant, cleared)
	require.NoError(t, err)
	feed := unmarshalFeed(t, payload)

	updatesByMMSI := map[string]*gtfs.TripUpdate{}
	for _, entity := range feed.Entity {
		updatesByMMSI[entity.GetTripUpdate().GetVehicle().GetId()] = entity.GetTripUpdate()
	}
	require.NotContains(t, updatesByMMSI, "657990012",
		"a stationary vessel's ETA is unknowable and must be omitted (NO_SHOW-style)")

	// Live ETA: ~556 m remaining at 10 kn (5.1444 m/s) ≈ 108 s.
	live := updatesByMMSI["657990011"]
	require.NotNil(t, live)
	require.Len(t, live.GetStopTimeUpdate(), 1, "only the remaining stop is predicted")
	update := live.GetStopTimeUpdate()[0]
	require.Equal(t, uint32(2), update.GetStopSequence())
	require.Equal(t, "itest-stop-b", update.GetStopId())
	require.Equal(t, int32(60), update.GetArrival().GetUncertainty())
	etaIn := update.GetArrival().GetTime() - now.Unix()
	require.InDelta(t, 108, etaIn, 20, "ETA = remaining distance / median speed")

	// Fallback ETA: ~556 m at the route default 8 kn (4.1156 m/s) ≈ 135 s,
	// marked LOW CONFIDENCE (300 s uncertainty), trip stays SCHEDULED.
	fallback := updatesByMMSI["657990013"]
	require.NotNil(t, fallback)
	require.Equal(t, gtfs.TripDescriptor_SCHEDULED, fallback.GetTrip().GetScheduleRelationship())
	require.Len(t, fallback.GetStopTimeUpdate(), 1)
	fallbackUpdate := fallback.GetStopTimeUpdate()[0]
	require.Equal(t, int32(300), fallbackUpdate.GetArrival().GetUncertainty(),
		"default-speed predictions must be marked low-confidence, never as live")
	fallbackEtaIn := fallbackUpdate.GetArrival().GetTime() - now.Unix()
	require.InDelta(t, 135, fallbackEtaIn, 25)

	after := feedMetrics.Snapshot()
	require.Equal(t, int64(1), after[`geo_gtfsrt_eta_total{mode="live"}`]-before[`geo_gtfsrt_eta_total{mode="live"}`])
	require.Equal(t, int64(1), after[`geo_gtfsrt_eta_total{mode="scheduled_fallback"}`]-
		before[`geo_gtfsrt_eta_total{mode="scheduled_fallback"}`])
	require.Equal(t, int64(1), after[`geo_gtfsrt_eta_omitted_total{reason="not_moving"}`]-
		before[`geo_gtfsrt_eta_omitted_total{reason="not_moving"}`])
}

// TestGTFSRTAlertsFeed proves active-window scoping of the alerts feed.
func TestGTFSRTAlertsFeed(t *testing.T) {
	h := newHarness(t)
	cleanTransit(t, h)
	seedTransit(t, h, transitTenant)
	builder, _ := newFeedBuilder(t, h)
	now := time.Now().UTC()
	starts := now.Add(-time.Hour)
	ends := now.Add(time.Hour)
	expiredEnd := now.Add(-time.Minute)

	require.NoError(t, h.store.CreateTransitAlert(ctx, transitTenant, store.TransitAlert{
		AlertID: "itest-alert-weather", Cause: "WEATHER", Effect: "NO_SERVICE",
		RouteID: "itest-route-1", StartsAt: &starts, EndsAt: &ends,
		HeaderText: "Harmattan haze: service suspended", CreatedBy: "itest-admin",
	}))
	require.NoError(t, h.store.CreateTransitAlert(ctx, transitTenant, store.TransitAlert{
		AlertID: "itest-alert-expired", Cause: "MAINTENANCE", Effect: "REDUCED_SERVICE",
		RouteID: "itest-route-1", StartsAt: &starts, EndsAt: &expiredEnd,
		HeaderText: "Expired works notice", CreatedBy: "itest-admin",
	}))
	require.Error(t, h.store.CreateTransitAlert(ctx, transitTenant, store.TransitAlert{
		AlertID: "itest-alert-unscoped", Cause: "WEATHER", Effect: "NO_SERVICE",
		HeaderText: "no scope", CreatedBy: "itest-admin",
	}), "alerts without route/stop scope must be rejected (informed_entity is required)")

	payload, err := builder.BuildAlerts(ctx, transitTenant)
	require.NoError(t, err)
	feed := unmarshalFeed(t, payload)
	require.Len(t, feed.Entity, 1, "only the in-window alert may be published")
	alert := feed.Entity[0].GetAlert()
	require.Equal(t, "alert:itest-alert-weather", feed.Entity[0].GetId())
	require.Equal(t, gtfs.Alert_WEATHER, alert.GetCause())
	require.Equal(t, gtfs.Alert_NO_SERVICE, alert.GetEffect())
	require.Equal(t, "itest-route-1", alert.GetInformedEntity()[0].GetRouteId())
	require.Equal(t, "Harmattan haze: service suspended", alert.GetHeaderText().GetTranslation()[0].GetText())
	require.Equal(t, uint64(starts.Unix()), alert.GetActivePeriod()[0].GetStart())
	require.Equal(t, uint64(ends.Unix()), alert.GetActivePeriod()[0].GetEnd())

	// The operator kill-switch removes the alert from the feed.
	deactivated, err := h.store.DeactivateTransitAlert(ctx, transitTenant, "itest-alert-weather")
	require.NoError(t, err)
	require.True(t, deactivated)
	payload, err = builder.BuildAlerts(ctx, transitTenant)
	require.NoError(t, err)
	require.Empty(t, unmarshalFeed(t, payload).Entity)
}

// TestTransitSeedLoader covers the documented YAML seed path end to end:
// parse the example document, load it idempotently, reload (upserts), and
// read the registry back.
func TestTransitSeedLoader(t *testing.T) {
	h := newHarness(t)
	cleanTransit(t, h)
	document, err := os.ReadFile("../fixtures/transit_seed_itest.yaml")
	require.NoError(t, err)
	seed, err := store.ParseTransitSeed(document)
	require.NoError(t, err)
	require.NoError(t, h.store.SeedTransitRegistry(ctx, transitTenant, seed))
	// Idempotent reload (upserts) must not fail or duplicate.
	require.NoError(t, h.store.SeedTransitRegistry(ctx, transitTenant, seed))
	registry, err := h.store.LoadTransitRegistry(ctx, transitTenant)
	require.NoError(t, err)
	require.Len(t, registry.Routes, 1)
	require.Len(t, registry.Stops, 3)
	require.Len(t, registry.StopTimes, 3)
	require.Len(t, registry.Assignments, 1)
	require.Equal(t, int32(6_451_830), registry.Stops[0].LatitudeMicros)
}

// stubAuthenticator resolves principals from test headers, mirroring the
// trusted-proxy contract.
type stubAuthenticator struct{}

func (stubAuthenticator) Authenticate(request *http.Request) (auth.Principal, error) {
	if request.Header.Get("X-Test-Subject") == "" {
		return auth.Principal{}, errors.New("unauthenticated")
	}
	roles := map[string]struct{}{}
	for _, role := range strings.Split(request.Header.Get("X-Test-Roles"), ",") {
		if role != "" {
			roles[role] = struct{}{}
		}
	}
	return auth.Principal{
		Subject:   request.Header.Get("X-Test-Subject"),
		Roles:     roles,
		Clearance: "PUBLIC",
		TenantID:  request.Header.Get("X-Test-Tenant"),
	}, nil
}

// TestFeedEndpointsHTTP exercises the full authenticated feed surface:
// GTFS zip + protobuf content types, ETag/304, and the alert admin
// endpoint's auth/role enforcement.
func TestFeedEndpointsHTTP(t *testing.T) {
	h := newHarness(t)
	cleanTransit(t, h)
	seedTransit(t, h, transitTenant, "657990021")
	speed := uint32(10000)
	reportPosition(t, h, "657990021", 1_005_000, 3_000_000, &speed, time.Now().UTC())

	server, err := api.NewServer(h.store, metrics.NewRegistry())
	require.NoError(t, err)
	builder, err := gtfsrt.NewBuilder(h.store, metrics.NewRegistry(), gtfsrt.Config{})
	require.NoError(t, err)
	require.NoError(t, server.AttachFeeds(builder))
	handler := server.Handler(stubAuthenticator{}, nil)

	reader := map[string]string{
		"X-Test-Subject": "itest-reader", "X-Test-Roles": "geo-reader", "X-Test-Tenant": transitTenant,
	}
	get := func(path string, headers map[string]string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	// Unauthenticated → 401 (fail closed).
	require.Equal(t, http.StatusUnauthorized, get("/feeds/gtfs.zip", nil).Code)

	// GTFS static zip.
	response := get("/feeds/gtfs.zip", reader)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "application/zip", response.Header().Get("Content-Type"))
	etag := response.Header().Get("ETag")
	require.NotEmpty(t, etag)
	zipReader, err := zip.NewReader(bytes.NewReader(response.Body.Bytes()), int64(response.Body.Len()))
	require.NoError(t, err)
	require.NotEmpty(t, zipReader.File)

	// ETag revalidation → 304.
	request := httptest.NewRequest(http.MethodGet, "/feeds/gtfs.zip", nil)
	for key, value := range reader {
		request.Header.Set(key, value)
	}
	request.Header.Set("If-None-Match", etag)
	revalidated := httptest.NewRecorder()
	handler.ServeHTTP(revalidated, request)
	require.Equal(t, http.StatusNotModified, revalidated.Code)

	// GTFS-RT protobuf feeds.
	for _, path := range []string{
		"/feeds/gtfs-rt/vehiclepositions.pb", "/feeds/gtfs-rt/tripupdates.pb", "/feeds/gtfs-rt/alerts.pb",
	} {
		response := get(path, reader)
		require.Equal(t, http.StatusOK, response.Code, path)
		require.Equal(t, "application/x-protobuf", response.Header().Get("Content-Type"))
		feed := unmarshalFeed(t, response.Body.Bytes())
		require.NotNil(t, feed.Header)
	}

	// Alert admin: reader role is rejected, admin role creates.
	create := func(headers map[string]string, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/v1/geo/transit/alerts", strings.NewReader(body))
		for key, value := range headers {
			request.Header.Set(key, value)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	payload := `{"cause":"WEATHER","effect":"NO_SERVICE","route_id":"itest-route-1","header_text":"Storm suspension"}`
	require.Equal(t, http.StatusForbidden, create(reader, payload).Code,
		"alert creation requires the admin role")
	admin := map[string]string{
		"X-Test-Subject": "itest-admin", "X-Test-Roles": "geo-transit-admin", "X-Test-Tenant": transitTenant,
	}
	created := create(admin, payload)
	require.Equal(t, http.StatusCreated, created.Code)
	require.Equal(t, http.StatusBadRequest, create(admin, `{"cause":"WEATHER","effect":"NO_SERVICE","header_text":"unscoped"}`).Code,
		"scope-less alerts are rejected")

	// The new alert is immediately in the alerts feed.
	response = get("/feeds/gtfs-rt/alerts.pb", reader)
	require.Equal(t, http.StatusOK, response.Code)
	feed := unmarshalFeed(t, response.Body.Bytes())
	require.Len(t, feed.Entity, 1)
	require.Equal(t, gtfs.Alert_WEATHER, feed.Entity[0].GetAlert().GetCause())
}
