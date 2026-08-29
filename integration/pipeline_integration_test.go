// Integration tests for the full hot path against real Postgres+PostGIS and
// Redis (Kafka optional). Gated by environment; skipped unless:
//
//	GEO_TEST_PG_DSN        postgres://geo:...@host:5432/geo_test  (PostGIS 3+)
//	GEO_TEST_REDIS_ADDR    host:6379
//	GEO_TEST_KAFKA_BROKERS host:9092  (optional — when set, the real bus
//	                         producer is exercised instead of the recorder)
//
// The tests run the migrations, push the synthetic replay fixture through
// the full pipeline and verify partitioning, geofence enter/exit, app-report
// idempotency and tenant RLS.
package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"crypto/ed25519"
	"crypto/rand"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/db"
	"github.com/munisp/blueeconomy-geo-service/internal/bus"
	"github.com/munisp/blueeconomy-geo-service/internal/connectors"
	"github.com/munisp/blueeconomy-geo-service/internal/dedup"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
	"github.com/munisp/blueeconomy-geo-service/internal/validate"
)

// recorder is the Publisher stub capturing every published message.
type recorder struct {
	mu       sync.Mutex
	messages []recordedMessage
}

type recordedMessage struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

func (rec *recorder) Publish(_ context.Context, topic, key string, value []byte, headers map[string]string) error {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.messages = append(rec.messages, recordedMessage{topic, key, value, headers})
	return nil
}

func (rec *recorder) byTopic(topic string) []recordedMessage {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	out := make([]recordedMessage, 0)
	for _, message := range rec.messages {
		if message.Topic == topic {
			out = append(out, message)
		}
	}
	return out
}

// harness wires the pipeline against the env-provided infrastructure.
type harness struct {
	store     *store.Store
	redis     *redis.Client
	pipeline  *connectors.Pipeline
	recorder  *recorder
	publisher connectors.Publisher
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dsn := os.Getenv("GEO_TEST_PG_DSN")
	redisAddr := os.Getenv("GEO_TEST_REDIS_ADDR")
	if dsn == "" || redisAddr == "" {
		t.Skip("GEO_TEST_PG_DSN and GEO_TEST_REDIS_ADDR are required for integration tests")
	}
	ctx := context.Background()
	storage, err := store.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(storage.Close)
	require.NoError(t, store.Migrate(ctx, storage, db.MigrationsFS))
	require.NoError(t, storage.EnsurePositionPartitions(ctx, time.Now(), time.Now().Add(24*time.Hour)))

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	require.NoError(t, redisClient.Ping(ctx).Err())
	t.Cleanup(func() { _ = redisClient.Close() })

	dedupChecker, err := dedup.NewChecker(redisClient, 15*time.Second)
	require.NoError(t, err)
	zoneState, err := store.NewZoneStateStore(redisClient)
	require.NoError(t, err)

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := sign.NewSigner(privateKey, "0")
	require.NoError(t, err)

	var publisher connectors.Publisher
	rec := &recorder{}
	if brokers := os.Getenv("GEO_TEST_KAFKA_BROKERS"); brokers != "" {
		producer, err := bus.NewProducer(bus.Config{Brokers: strings.Split(brokers, ",")})
		require.NoError(t, err)
		t.Cleanup(func() { _ = producer.Close() })
		publisher = producer
	} else {
		publisher = rec
	}

	pipeline, err := connectors.NewPipeline(connectors.Pipeline{
		Store:     storage,
		Dedup:     dedupChecker,
		ZoneState: zoneState,
		Producer:  publisher,
		Signer:    signer,
		Tracker:   validate.NewTracker(),
		Metrics:   metrics.NewRegistry(),
		Principal: sign.Provenance{PrincipalID: "integration-test", PrincipalRole: "geo-producer"},
	})
	require.NoError(t, err)
	return &harness{store: storage, redis: redisClient, pipeline: pipeline, recorder: rec, publisher: publisher}
}

// clean removes test rows (test MMSIs are in the 000001xxx synthetic range).
func (h *harness) clean(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, statement := range []string{
		`DELETE FROM geofence_events WHERE zone_id LIKE 'itest-%'`,
		`DELETE FROM geofence_zone_approvals WHERE zone_id LIKE 'itest-%'`,
		`DELETE FROM geofence_zones WHERE zone_id LIKE 'itest-%'`,
		`DELETE FROM latest_positions WHERE mmsi LIKE '000001%' OR vessel_ref LIKE 'itest-%'`,
		`DELETE FROM vessels_static WHERE mmsi LIKE '000001%'`,
		`DELETE FROM ais_positions WHERE mmsi LIKE '000001%' OR vessel_ref LIKE 'itest-%'`,
		`DELETE FROM app_position_reports WHERE reporter_id LIKE 'itest-%'`,
		`DELETE FROM sos_alerts WHERE reporter_id LIKE 'itest-%'`,
	} {
		_, err := h.store.Pool().Exec(ctx, statement)
		require.NoError(t, err)
	}
	keys, err := h.redis.Keys(ctx, "geo:zonestate:*").Result()
	require.NoError(t, err)
	if len(keys) > 0 {
		require.NoError(t, h.redis.Del(ctx, keys...).Err())
	}
}

const (
	testMMSI    = "000001001"
	testTenant  = "itest-tenant"
	testZoneID  = "itest-zone-apapa"
	testZoneID2 = "itest-tenant2-zone"
	testTenant2 = "itest-tenant-2"
)

// zonePolygon is a small square around (6.418, 3.3725).
const zonePolygon = `{"type":"Polygon","coordinates":[[[3.36,6.41],[3.39,6.41],[3.39,6.43],[3.36,6.43],[3.36,6.41]]]}`

func TestPartitioningAndIngest(t *testing.T) {
	h := newHarness(t)
	h.clean(t)
	ctx := context.Background()

	position := store.Position{
		PositionReportID:          "itest-pos-1",
		MMSI:                      testMMSI,
		SourceClass:               "AIS",
		LatitudeMicros:            6418000,
		LongitudeMicros:           3372500,
		SpeedOverGroundMilliknots: u32(8400),
		Classification:            "PUBLIC",
		ObservedAt:                time.Now().UTC().Truncate(time.Second),
		ReceiverID:                "itest-rx",
	}
	require.NoError(t, h.store.InsertPositions(ctx, []store.Position{position}))

	// The row must land in today's daily partition.
	var partition string
	today := time.Now().UTC().Format("20060102")
	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT tableoid::regclass::text FROM ais_positions WHERE position_report_id = 'itest-pos-1'`).Scan(&partition))
	require.Equal(t, "ais_positions_"+today, partition)

	// Upsert moves latest forward, never backward.
	require.NoError(t, h.store.UpsertLatestPosition(ctx, position))
	older := position
	older.PositionReportID = "itest-pos-0"
	older.LatitudeMicros = 6400000
	older.ObservedAt = position.ObservedAt.Add(-time.Hour)
	require.NoError(t, h.store.UpsertLatestPosition(ctx, older))
	var lat int32
	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT latitude_micros FROM latest_positions WHERE mmsi = $1`, testMMSI).Scan(&lat))
	require.Equal(t, int32(6418000), lat, "latest must not regress to an older observation")
}

func TestGeofenceEnterExit(t *testing.T) {
	h := newHarness(t)
	h.clean(t)
	ctx := context.Background()

	require.NoError(t, h.store.CreateZone(ctx, testTenant, store.ZoneRow{
		ZoneID: testZoneID, Name: "ITEST Apapa Box",
		ClassificationFloor: "INTERNAL", MakerPrincipalID: "itest-maker",
	}, zonePolygon))
	require.NoError(t, h.store.ApproveZone(ctx, testTenant, testZoneID, "itest-checker"))

	// Outside the box: no events.
	outside := itestPosition("itest-pos-out", 6400000, 3400000)
	require.NoError(t, h.pipeline.HandlePosition(ctx, outside))
	// Inside the box: ENTER.
	inside := itestPosition("itest-pos-in", 6418000, 3372500)
	inside.Position.ObservedAt = inside.Position.ObservedAt.Add(time.Minute)
	require.NoError(t, h.pipeline.HandlePosition(ctx, inside))
	// Still inside: no new event.
	inside2 := itestPosition("itest-pos-in2", 6419000, 3372600)
	inside2.Position.ObservedAt = inside.Position.ObservedAt.Add(2 * time.Minute)
	require.NoError(t, h.pipeline.HandlePosition(ctx, inside2))
	// Back outside: EXIT.
	outside2 := itestPosition("itest-pos-out2", 6400000, 3400000)
	outside2.Position.ObservedAt = inside.Position.ObservedAt.Add(3 * time.Minute)
	require.NoError(t, h.pipeline.HandlePosition(ctx, outside2))

	rows, err := h.store.Pool().Query(ctx,
		`SELECT event FROM geofence_events WHERE zone_id = $1 ORDER BY occurred_at`, testZoneID)
	require.NoError(t, err)
	defer rows.Close()
	events := make([]string, 0)
	for rows.Next() {
		var event string
		require.NoError(t, rows.Scan(&event))
		events = append(events, event)
	}
	require.Equal(t, []string{"ENTER", "EXIT"}, events)

	// Signed geofence envelopes were published with the zone classification
	// floor when the recorder is active.
	if h.recorder != nil {
		published := h.recorder.byTopic(bus.TopicVesselEvents)
		geofenceEvents := make([]map[string]any, 0)
		for _, message := range published {
			var envelope map[string]any
			require.NoError(t, json.Unmarshal(message.Value, &envelope))
			if envelope["eventType"] == sign.EventGeofenceEvent {
				geofenceEvents = append(geofenceEvents, envelope)
				require.Equal(t, "INTERNAL", envelope["classification"])
				require.NotEmpty(t, envelope["provenance"].(map[string]any)["signature"])
			}
		}
		require.Len(t, geofenceEvents, 2, "ENTER and EXIT envelopes must be published")
	}
}

func TestAppReportIdempotency(t *testing.T) {
	h := newHarness(t)
	h.clean(t)
	ctx := context.Background()

	report := store.AppReport{
		PositionReportID: "itest-apr-1",
		ReporterID:       "itest-reporter-1",
		VesselReference:  "itest-vessel-1",
		LatitudeMicros:   6418000,
		LongitudeMicros:  3372500,
		RecordedAt:       time.Now().UTC().Truncate(time.Second),
		OutboxID:         "obx-1",
	}
	inserted, err := h.store.InsertAppReport(ctx, report)
	require.NoError(t, err)
	require.True(t, inserted)

	// Exact replay: idempotent absorb.
	inserted, err = h.store.InsertAppReport(ctx, report)
	require.NoError(t, err)
	require.False(t, inserted)

	// Conflicting reuse: 409 semantics.
	conflict := report
	conflict.LatitudeMicros = 6500000
	conflict.PositionReportID = "itest-apr-2"
	_, err = h.store.InsertAppReport(ctx, conflict)
	require.ErrorIs(t, err, store.ErrDuplicateOutbox)
}

func TestSOSClassificationFloor(t *testing.T) {
	h := newHarness(t)
	h.clean(t)
	ctx := context.Background()
	alert := store.SOSAlert{
		SosAlertID:      "itest-sos-1",
		ReporterID:      "itest-reporter-1",
		VesselReference: "itest-vessel-1",
		LatitudeMicros:  6418000,
		LongitudeMicros: 3372500,
		RecordedAt:      time.Now().UTC(),
		OutboxID:        "obx-sos-1",
		Classification:  "PUBLIC",
	}
	_, err := h.store.InsertSOSAlert(ctx, alert)
	require.Error(t, err, "SOS below RESTRICTED must fail closed")
	alert.Classification = "RESTRICTED"
	inserted, err := h.store.InsertSOSAlert(ctx, alert)
	require.NoError(t, err)
	require.True(t, inserted)
}

func TestTenantRLS(t *testing.T) {
	h := newHarness(t)
	h.clean(t)
	ctx := context.Background()

	require.NoError(t, h.store.CreateZone(ctx, testTenant, store.ZoneRow{
		ZoneID: testZoneID, Name: "ITEST Tenant One Zone",
		ClassificationFloor: "PUBLIC", MakerPrincipalID: "itest-maker",
	}, zonePolygon))
	require.NoError(t, h.store.CreateZone(ctx, testTenant2, store.ZoneRow{
		ZoneID: testZoneID2, Name: "ITEST Tenant Two Zone",
		ClassificationFloor: "PUBLIC", MakerPrincipalID: "itest-maker",
	}, zonePolygon))

	// Tenant one sees exactly its own zone under RLS.
	zones, err := h.store.ListZones(ctx, testTenant, []string{"PUBLIC", "INTERNAL", "RESTRICTED", "CONFIDENTIAL", "SECRET"})
	require.NoError(t, err)
	ids := make([]string, 0)
	for _, zone := range zones {
		ids = append(ids, zone.ZoneID)
	}
	require.Contains(t, ids, testZoneID)
	require.NotContains(t, ids, testZoneID2)
}

func TestReplayFixtureThroughPipeline(t *testing.T) {
	h := newHarness(t)
	h.clean(t)
	ctx := context.Background()

	// Run the committed synthetic replay file through the replay connector.
	fixture := filepath.Join("..", "fixtures", "nmea_replay.txt")
	replay := &connectors.ReplayConnector{
		File: fixture, AppEnv: "dev", Pipeline: h.pipeline,
	}
	require.NoError(t, replay.Run(ctx))

	// Synthetic fixture vessels landed on the hot path.
	var count int
	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM ais_positions WHERE mmsi IN ('657210300', '657221000', '235081000')`).Scan(&count))
	require.Equal(t, 7, count, "5 class-A + 1 class-B + 1 extended-B fixture positions")

	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM vessels_static WHERE mmsi = '657210300' AND valid_to IS NULL`).Scan(&count))
	require.Equal(t, 1, count, "type 5 static must open one SCD-2 row")

	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT count(*) FROM latest_positions WHERE mmsi = '657210300'`).Scan(&count))
	require.Equal(t, 1, count)
	h.cleanFixture(t)
}

func (h *harness) cleanFixture(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, statement := range []string{
		`DELETE FROM latest_positions WHERE mmsi IN ('657210300', '657221000', '235081000')`,
		`DELETE FROM vessels_static WHERE mmsi IN ('657210300', '657221000', '235081000')`,
		`DELETE FROM ais_positions WHERE mmsi IN ('657210300', '657221000', '235081000')`,
	} {
		_, err := h.store.Pool().Exec(ctx, statement)
		require.NoError(t, err)
	}
}

func itestPosition(id string, latMicros, lonMicros int32) connectors.IngestPosition {
	return connectors.IngestPosition{
		Position: store.Position{
			PositionReportID:          id,
			MMSI:                      testMMSI,
			SourceClass:               "AIS",
			LatitudeMicros:            latMicros,
			LongitudeMicros:           lonMicros,
			SpeedOverGroundMilliknots: u32(8400),
			Classification:            "PUBLIC",
			ObservedAt:                time.Now().UTC().Truncate(time.Second),
			ReceiverID:                "itest-rx",
		},
	}
}

func u32(value uint32) *uint32 { return &value }
