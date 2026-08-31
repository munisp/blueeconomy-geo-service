// Phase-8 MRV integration tests against a real PostgreSQL+PostGIS. Gated
// by environment; skipped unless GEO_TEST_PG_DSN is set (the harness
// derives an admin connection from it to provision a FRESH database per
// run, owned by the same non-superuser role, with UTC timezone — migration
// partition bounds and RLS posture depend on both). Kafka is exercised when
// GEO_TEST_KAFKA_BROKERS is set.
package integration

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/db"
	"github.com/munisp/blueeconomy-geo-service/internal/bus"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/mrv"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// mrvHarness wires the MRV service against a fresh database.
type mrvHarness struct {
	pool      *pgxpool.Pool
	service   *mrv.Service
	signer    *sign.Signer
	publicKey ed25519.PublicKey
	recorder  *mrvRecorder
}

// mrvRecorder captures published envelopes (brokerless runs).
type mrvRecorder struct {
	messages []mrvMessage
}

type mrvMessage struct {
	topic, key string
	value      []byte
}

func (rec *mrvRecorder) Publish(_ context.Context, topic, key string, value []byte, _ map[string]string) error {
	rec.messages = append(rec.messages, mrvMessage{topic, key, value})
	return nil
}

func newMrvHarness(t *testing.T) *mrvHarness {
	t.Helper()
	dsn := os.Getenv("GEO_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("GEO_TEST_PG_DSN is required for MRV integration tests")
	}
	ctx := context.Background()

	// Provision a FRESH database per run (the mrv migration carries seed
	// rows; replays must re-apply cleanly). The admin connection uses the
	// local trust superuser on the same server; the test role stays the
	// non-superuser owner of the database (RLS is FORCEd).
	appURL, err := url.Parse(dsn)
	require.NoError(t, err)
	adminURL := *appURL
	adminURL.User = url.UserPassword("postgres", "")
	adminURL.Path = "/postgres"
	admin, err := pgxpool.New(ctx, adminURL.String())
	require.NoError(t, err)
	defer admin.Close()
	const testDB = "mrv_itest"
	_, err = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", testDB))
	require.NoError(t, err)
	_, err = admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s OWNER %s", testDB, appURL.User.Username()))
	require.NoError(t, err)
	_, err = admin.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s SET timezone TO 'UTC'", testDB))
	require.NoError(t, err)
	require.NoError(t, err)
	adminDBURL := adminURL
	adminDBURL.Path = "/" + testDB
	adminDB, err := pgxpool.New(ctx, adminDBURL.String())
	require.NoError(t, err)
	_, err = adminDB.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS postgis")
	require.NoError(t, err)
	adminDB.Close()

	// Migrate as the non-superuser owner, session TZ UTC (PRA-128 lesson).
	appURL.Path = "/" + testDB
	query := appURL.Query()
	query.Set("timezone", "UTC")
	appURL.RawQuery = query.Encode()
	pool, err := pgxpool.New(ctx, appURL.String())
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, store.MigratePool(ctx, pool, db.MigrationsFS))

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := sign.NewSignerForProducer(privateKey, "0", mrv.ProducerName)
	require.NoError(t, err)
	service, err := mrv.NewService(pool, signer,
		sign.Provenance{PrincipalID: "mrv-itest-service", PrincipalRole: "mrv-producer"},
		metrics.NewRegistry(), nil,
		mrv.Deadlines{ReportSubmission: "03-31", SoCIssuance: "05-31", GISISForward: "06-30"},
		mrv.DefaultActivityParams(), 100, 5000)
	require.NoError(t, err)
	return &mrvHarness{
		pool: pool, service: service, signer: signer,
		publicKey: privateKey.Public().(ed25519.PublicKey), recorder: &mrvRecorder{},
	}
}

// withActor runs fn in a transaction with app.mrv_actor bound.
func (h *mrvHarness) withActor(t *testing.T, actor string, fn func(tx pgx.Tx) error) {
	t.Helper()
	ctx := context.Background()
	tx, err := h.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SELECT set_config('app.mrv_actor', $1, true)`, actor)
	require.NoError(t, err)
	require.NoError(t, fn(tx))
	require.NoError(t, tx.Commit(ctx))
}

const (
	mrvImo       = "9074729"
	mrvMMSI      = "000001777"
	mrvReporter  = "itest-op-1"
	mrvVerifier  = "itest-ver-1"
	mrvFlagAdmin = "itest-flag-admin"
)

// TestMRVRLSDefaultDeny proves the 0012 posture: unbound sessions read zero
// rows and cannot write; bound sessions work.
func TestMRVRLSDefaultDeny(t *testing.T) {
	h := newMrvHarness(t)
	ctx := context.Background()

	var count int
	require.NoError(t, h.pool.QueryRow(ctx, `SELECT count(*) FROM mrv_ships`).Scan(&count))
	require.Zero(t, count, "unbound session must read zero mrv_ships rows")
	require.NoError(t, h.pool.QueryRow(ctx, `SELECT count(*) FROM mrv_outbox`).Scan(&count))
	require.Zero(t, count)
	_, err := h.pool.Exec(ctx, `INSERT INTO mrv_ships
		(imo_number, ship_name, gt, ship_type, international_voyages, dcs_scope, registered_by)
		VALUES ('9074729','ITEST',60000,'BULK_CARRIER',true,true,'itest')`)
	require.Error(t, err, "unbound session must not insert mrv_ships rows")
	// The factor table is public reference data: readable unbound.
	require.NoError(t, h.pool.QueryRow(ctx, `SELECT count(*) FROM mrv_emission_factors`).Scan(&count))
	require.Equal(t, 8, count, "the seeded MEPC.308(73)/EU-MRV CO2 factor set must be present")

	// Bound session works.
	h.withActor(t, mrvFlagAdmin, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO mrv_ships
			(imo_number, ship_name, gt, ship_type, international_voyages, dcs_scope, registered_by)
			VALUES ('9074729','ITEST',60000,'BULK_CARRIER',true,true,$1)`, mrvFlagAdmin)
		return err
	})
}

// TestMRVWorkflow drives the full lifecycle: register, plan (four-eyes),
// fuel intake (idempotency + fail-closed factor), voyage, compile, submit,
// verification (maker != checker, AIS cross-check), SoC + signed artifact,
// and the outbox event families.
func TestMRVWorkflow(t *testing.T) {
	h := newMrvHarness(t)
	ctx := context.Background()

	// Register the ship (flag admin; dcs_scope from the 5000 GT threshold).
	ship, err := h.service.RegisterShip(ctx, mrvFlagAdmin, mrv.Ship{
		ImoNumber: mrvImo, MMSI: mrvMMSI, ShipName: "ITEST VESSEL", GT: 60_000, DWT: u32ptr(110_000),
		ShipType: "BULK_CARRIER", FlagState: "NG", InternationalVoyages: true,
	})
	require.NoError(t, err)
	require.True(t, ship.DcsScope)

	// Monitoring plan: put by the operator, four-eyes on confirmation.
	plan, err := h.service.PutMonitoringPlan(ctx, mrvReporter, mrvImo,
		map[string]string{"MAIN_ENGINE": "B"}, []string{"HFO_RME-RMK", "VLSFO_MYSTERY"})
	require.NoError(t, err)
	_, err = h.service.ConfirmMonitoringPlan(ctx, mrvReporter, plan.PlanID)
	require.ErrorIs(t, err, mrv.ErrMakerCheckerConflict)
	plan, err = h.service.ConfirmMonitoringPlan(ctx, mrvVerifier, plan.PlanID)
	require.NoError(t, err)
	require.Equal(t, mrv.PlanStateConfirmed, plan.State)

	// Fuel intake: 1000.000 t HFO, method B, July 2026.
	report := mrv.FuelReport{
		PeriodFrom: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		PeriodTo:   time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC),
		Consumer:   mrv.ConsumerMainEngine, FuelGrade: "HFO_RME-RMK", Method: mrv.MethodB,
		FuelTonnesMilli: 1_000_000,
	}
	stored, replayed, err := h.service.SubmitFuelReport(ctx, mrvReporter, mrvImo, "itest-key-1", report)
	require.NoError(t, err)
	require.False(t, replayed)
	// Identical replay returns the stored report; divergent replay conflicts.
	again, replayed, err := h.service.SubmitFuelReport(ctx, mrvReporter, mrvImo, "itest-key-1", report)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, stored.ReportID, again.ReportID)
	divergent := report
	divergent.FuelTonnesMilli = 2_000_000
	_, _, err = h.service.SubmitFuelReport(ctx, mrvReporter, mrvImo, "itest-key-1", divergent)
	require.ErrorIs(t, err, mrv.ErrIdempotencyConflict)
	// Missing idempotency key fails closed.
	_, _, err = h.service.SubmitFuelReport(ctx, mrvReporter, mrvImo, "", report)
	require.ErrorIs(t, err, mrv.ErrIdempotencyKeyNeeded)
	// Unknown fuel grade: no source-cited factor => fail closed, no record.
	unknown := report
	unknown.FuelGrade = "VLSFO_MYSTERY"
	_, _, err = h.service.SubmitFuelReport(ctx, mrvReporter, mrvImo, "itest-key-2", unknown)
	require.ErrorIs(t, err, mrv.ErrFactorUnavailable)
	// Method A requires the BDN reference (grade capture).
	noBDN := report
	noBDN.Method = mrv.MethodA
	_, _, err = h.service.SubmitFuelReport(ctx, mrvReporter, mrvImo, "itest-key-3", noBDN)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bdnReference")

	// Voyage recording (no geofence rows yet => empty evidence list).
	voyage, err := h.service.RecordVoyage(ctx, mrvReporter, mrvImo, mrv.Voyage{
		BospAt:   timePtr(time.Date(2026, 7, 3, 8, 0, 0, 0, time.UTC)),
		BospPort: "NGLOS",
		EospAt:   timePtr(time.Date(2026, 7, 19, 16, 30, 0, 0, time.UTC)),
		EospPort: "NGAPP",
	})
	require.NoError(t, err)
	require.Equal(t, mrv.VoyageSourceOperator, voyage.Source)

	// Compile 2026: HFO 1000 t x 3.114 (MEPC.308(73) Cf) = 3114.000 t CO2.
	annual, err := h.service.CompileAnnualReport(ctx, mrvReporter, mrvImo, 2026)
	require.NoError(t, err)
	require.Equal(t, mrv.ReportStateDraft, annual.State)
	var totals struct {
		CO2TonnesMilli       mrv.U64 `json:"co2TonnesMilli"`
		TotalFuelTonnesMilli mrv.U64 `json:"totalFuelTonnesMilli"`
		FuelReportCount      int     `json:"fuelReportCount"`
	}
	require.NoError(t, json.Unmarshal(annual.Totals, &totals))
	require.Equal(t, mrv.U64(3_114_000), totals.CO2TonnesMilli)
	require.Equal(t, mrv.U64(1_000_000), totals.TotalFuelTonnesMilli)
	require.Equal(t, 1, totals.FuelReportCount)
	require.Nil(t, annual.AttainedCiiNano, "no operator-approved CII config => NOT_COMPUTABLE")
	require.True(t, strings.HasPrefix(annual.FactorSetHash, "sha256:"))

	// Submit: only the compiling reporter; then maker != checker.
	_, err = h.service.SubmitAnnualReport(ctx, mrvVerifier, annual.ReportID)
	require.Error(t, err)
	annual, err = h.service.SubmitAnnualReport(ctx, mrvReporter, annual.ReportID)
	require.NoError(t, err)
	require.Equal(t, mrv.ReportStateSubmitted, annual.State)
	_, _, err = h.service.RecordDecision(ctx, mrvReporter, annual.ReportID, mrv.DecisionVerify, "")
	require.ErrorIs(t, err, mrv.ErrMakerCheckerConflict)

	// Verifier decision with the AIS cross-check (no AIS data =>
	// insufficient coverage, honestly recorded).
	verification, crosscheck, err := h.service.RecordDecision(ctx, mrvVerifier, annual.ReportID, mrv.DecisionVerify, "")
	require.NoError(t, err)
	require.Equal(t, mrv.CrosscheckInsufficientCoverage, crosscheck)
	require.Equal(t, mrv.DecisionVerify, verification.Decision)
	require.Contains(t, string(verification.AISCrosscheck), "insufficient_coverage")

	// The decision ledger is immutable.
	h.withActorExpectError(t, mrvVerifier, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE mrv_verifications SET reason = 'tampered' WHERE verification_id = $1`,
			verification.VerificationID)
		return err
	})

	// SoC: VERIFIED => artifact + sha256 + signed mrv.soc.v1 queued.
	socID, artifactSum, err := h.service.IssueSoC(ctx, mrvFlagAdmin, annual.ReportID)
	require.NoError(t, err)
	require.Len(t, artifactSum, 64)
	artifact, sum, err := h.service.GetArtifact(ctx, mrvFlagAdmin, annual.ReportID)
	require.NoError(t, err)
	require.Equal(t, artifactSum, sum)
	require.Contains(t, string(artifact), "mrv-annual-report-artifact/v1")
	require.Contains(t, string(artifact), `"signature"`)
	_, _, err = h.service.IssueSoC(ctx, mrvFlagAdmin, annual.ReportID)
	require.ErrorIs(t, err, mrv.ErrSoCExists)

	// Illegal direct state transition is trigger-guarded.
	h.withActorExpectError(t, mrvReporter, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE mrv_annual_reports SET state = 'DRAFT' WHERE report_id = $1`, annual.ReportID)
		return err
	})

	// Every emitted family is queued in the outbox, signed, verifiable.
	publisher, err := mrv.NewOutboxPublisher(h.service, h.recorder, time.Hour, 100)
	require.NoError(t, err)
	require.NoError(t, publisher.Drain(ctx))
	require.NoError(t, publisher.Drain(ctx)) // idempotent: nothing left
	byType := map[string]int{}
	for _, message := range h.recorder.messages {
		var envelope struct {
			EventType string `json:"eventType"`
			EventID   string `json:"eventId"`
		}
		require.NoError(t, json.Unmarshal(message.value, &envelope))
		require.NoError(t, mrv.VerifySignedEnvelope(message.value, h.publicKey))
		byType[envelope.EventType]++
	}
	require.Equal(t, 1, byType[mrv.EventFuelReport])
	require.Equal(t, 1, byType[mrv.EventVoyage])
	require.Equal(t, 2, byType[mrv.EventEmissionsAnnual]) // compile DRAFT + submit
	require.Equal(t, 1, byType[mrv.EventVerification])
	require.Equal(t, 1, byType[mrv.EventSoC])
	snapshot := h.service.Metrics.Snapshot()
	require.GreaterOrEqual(t, snapshot["mrv_outbox_publish_total{result=\"ok\"}"], int64(6))

	// Intake without a covering plan fails closed (plan covers MAIN_ENGINE/B
	// on HFO only).
	_, _, err = h.service.SubmitFuelReport(ctx, mrvReporter, mrvImo, "itest-key-4", mrv.FuelReport{
		PeriodFrom: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		PeriodTo:   time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC),
		Consumer:   mrv.ConsumerAuxEngine, FuelGrade: "HFO_RME-RMK", Method: mrv.MethodB,
		FuelTonnesMilli: 100_000,
	})
	require.ErrorIs(t, err, mrv.ErrNoConfirmedPlan)

	// SoC requires VERIFIED: a merely-submitted report fails closed.
	second, err := h.service.CompileAnnualReport(ctx, mrvReporter, mrvImo, 2027)
	require.NoError(t, err)
	second, err = h.service.SubmitAnnualReport(ctx, mrvReporter, second.ReportID)
	require.NoError(t, err)
	_, _, err = h.service.IssueSoC(ctx, mrvFlagAdmin, second.ReportID)
	require.ErrorIs(t, err, mrv.ErrReportState)
	// And the database boundary agrees (trigger) even if the app regresses.
	h.withActorExpectError(t, mrvFlagAdmin, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO mrv_statements_of_compliance
			(soc_id, report_id, issued_by, artifact_sha256)
			VALUES (gen_random_uuid(), $1, $2,
			 '0000000000000000000000000000000000000000000000000000000000000000')`,
			second.ReportID, mrvFlagAdmin)
		return err
	})

	// REQUEST_CLARIFICATION returns the report to SUBMITTED.
	_, _, err = h.service.RecordDecision(ctx, mrvVerifier, second.ReportID, mrv.DecisionClarify, "missing bunker evidence")
	require.NoError(t, err)
	reloaded, err := h.service.GetAnnualReport(ctx, mrvVerifier, second.ReportID)
	require.NoError(t, err)
	require.Equal(t, mrv.ReportStateSubmitted, reloaded.State)
	_ = socID
}

// TestMRVCompileFailsClosedWithoutFactor proves compile aborts when a fuel
// report's grade has no source-cited factor valid at its period (the row
// here predates every seeded factor's validity — planted directly to reach
// the compile path, which must fail closed with no estimate).
func TestMRVCompileFailsClosedWithoutFactor(t *testing.T) {
	h := newMrvHarness(t)
	ctx := context.Background()
	_, err := h.service.RegisterShip(ctx, mrvFlagAdmin, mrv.Ship{
		ImoNumber: mrvImo, ShipName: "ITEST VESSEL", GT: 60_000, ShipType: "BULK_CARRIER",
		FlagState: "NG", InternationalVoyages: true,
	})
	require.NoError(t, err)
	// Plant a factor whose validity starts in the future and a fuel report
	// on that grade (planted directly to reach the compile path): no factor
	// row is valid at the report period, so the compile must fail closed
	// with NO estimate.
	h.withActor(t, mrvFlagAdmin, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO mrv_emission_factors
			(factor_key, gas, factor_nano, unit, source_citation, valid_from)
			VALUES ('BIO_FAME_TEST', 'CO2', 2750000000, 'tCO2/t_fuel',
			 'TEST ROW — future validity only', '2030-01-01')`)
		return err
	})
	h.withActor(t, mrvReporter, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO mrv_fuel_reports
			(report_id, imo_number, external_ref, period_from, period_to, consumer, fuel_grade, method,
			 fuel_tonnes_milli, evidence_digest_sha256, reported_by)
			VALUES (gen_random_uuid(), $1, 'itest-old', '2026-01-01', '2026-06-30', 'MAIN_ENGINE',
			 'BIO_FAME_TEST', 'B', 500000,
			 'sha256:0000000000000000000000000000000000000000000000000000000000000000', $2)`,
			mrvImo, mrvReporter)
		return err
	})
	_, err = h.service.CompileAnnualReport(ctx, mrvReporter, mrvImo, 2026)
	require.ErrorIs(t, err, mrv.ErrFactorUnavailable)
}

// TestMRVActivityEstimateFromAIS plants AIS positions and proves the
// estimator reads the position plane and emits mrv.activity-estimate.v1.
func TestMRVActivityEstimateFromAIS(t *testing.T) {
	h := newMrvHarness(t)
	ctx := context.Background()
	_, err := h.service.RegisterShip(ctx, mrvFlagAdmin, mrv.Ship{
		ImoNumber: mrvImo, MMSI: mrvMMSI, ShipName: "ITEST VESSEL", GT: 60_000, ShipType: "BULK_CARRIER",
		FlagState: "NG", InternationalVoyages: true,
	})
	require.NoError(t, err)
	day := time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC)
	_, err = h.pool.Exec(ctx, `SELECT geo_ensure_position_partition($1::date)`, day)
	require.NoError(t, err)
	// 13 hourly fixes, 0.005 deg/leg at 10 kn (see the unit golden vector).
	for i := 0; i <= 12; i++ {
		_, err = h.pool.Exec(ctx, `INSERT INTO ais_positions
			(position_report_id, mmsi, vessel_ref, source_class, geom, latitude_micros, longitude_micros,
			 speed_over_ground_milliknots, position_accuracy, receiver_id, imo, callsign, ship_name,
			 classification, observed_at)
			VALUES ($1, $2, NULL, 'AIS',
			 ST_GeogFromText($3), 0, $4, 10000, 'HIGH', 'itest-rx', '', '', '', 'PUBLIC', $5)`,
			fmt.Sprintf("itest-pos-%02d", i), mrvMMSI,
			fmt.Sprintf("POINT(%f 0.000000)", 3.0+float64(i)*0.005),
			int32(3_000_000+i*5_000), day.Add(time.Duration(i+1)*time.Hour))
		require.NoError(t, err)
	}
	estimate, err := h.service.EstimateActivityForShip(ctx, mrvVerifier, mrvImo,
		day, day.Add(24*time.Hour), []string{"PUBLIC", "INTERNAL"})
	require.NoError(t, err)
	require.False(t, estimate.InsufficientCoverage)
	require.Equal(t, mrvMMSI, estimate.Mmsi)
	require.InDelta(t, 3_600_000, uint64(estimate.DistanceNmMilli), 30_000)
	require.Equal(t, mrv.U64(720), estimate.HoursUnderwayMinutes)

	// The estimate event is queued and verifies.
	publisher, err := mrv.NewOutboxPublisher(h.service, h.recorder, time.Hour, 100)
	require.NoError(t, err)
	require.NoError(t, publisher.Drain(ctx))
	require.Len(t, h.recorder.messages, 1)
	require.Equal(t, mrv.TopicActivityEstimates, h.recorder.messages[0].topic)
	require.Equal(t, mrvMMSI, h.recorder.messages[0].key)
	require.NoError(t, mrv.VerifySignedEnvelope(h.recorder.messages[0].value, h.publicKey))
}

// TestMRVBrokerEmission drains the outbox to a REAL Kafka broker and
// verifies the consumed envelope byte-for-byte (signature round-trip).
// Gated on GEO_TEST_KAFKA_BROKERS.
func TestMRVBrokerEmission(t *testing.T) {
	brokers := os.Getenv("GEO_TEST_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("GEO_TEST_KAFKA_BROKERS is required for the broker emission test")
	}
	h := newMrvHarness(t)
	ctx := context.Background()
	// Pre-provision the mrv topics (the broker does not auto-create).
	client := &kafka.Client{Addr: kafka.TCP(strings.Split(brokers, ",")...), Timeout: 10 * time.Second}
	for _, topic := range []string{mrv.TopicFuelReports, mrv.TopicVoyages, mrv.TopicVerifications,
		mrv.TopicAnnualReports, mrv.TopicSoC, mrv.TopicActivityEstimates} {
		_, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
			Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
		})
		require.NoError(t, err)
	}
	_, err := h.service.RegisterShip(ctx, mrvFlagAdmin, mrv.Ship{
		ImoNumber: mrvImo, MMSI: mrvMMSI, ShipName: "ITEST VESSEL", GT: 60_000, ShipType: "BULK_CARRIER",
		FlagState: "NG", InternationalVoyages: true,
	})
	require.NoError(t, err)
	plan, err := h.service.PutMonitoringPlan(ctx, mrvReporter, mrvImo,
		map[string]string{"MAIN_ENGINE": "B"}, []string{"MDO_MGO_DMX-DMB"})
	require.NoError(t, err)
	_, err = h.service.ConfirmMonitoringPlan(ctx, mrvVerifier, plan.PlanID)
	require.NoError(t, err)
	_, _, err = h.service.SubmitFuelReport(ctx, mrvReporter, mrvImo, "itest-broker-1", mrv.FuelReport{
		PeriodFrom: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		PeriodTo:   time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC),
		Consumer:   mrv.ConsumerMainEngine, FuelGrade: "MDO_MGO_DMX-DMB", Method: mrv.MethodB,
		FuelTonnesMilli: 12_500,
	})
	require.NoError(t, err)

	producer, err := bus.NewProducer(bus.Config{Brokers: strings.Split(brokers, ",")})
	require.NoError(t, err)
	defer producer.Close()
	publisher, err := mrv.NewOutboxPublisher(h.service, producer, time.Hour, 100)
	require.NoError(t, err)
	require.NoError(t, publisher.Drain(ctx))

	// Consume and verify: topic, key, eventType and the EdDSA/JCS round-trip
	// for THIS run's event (the topic may carry earlier runs' messages).
	var ourEvent string
	h.withActor(t, "mrv-itest-service", func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT event_id::text FROM mrv_outbox
			WHERE event_type = 'mrv.fuel-report.v1' AND published_at IS NOT NULL LIMIT 1`).Scan(&ourEvent)
	})
	message, err := consumeOne(ctx, brokers, mrv.TopicFuelReports, ourEvent)
	require.NoError(t, err)
	require.Equal(t, mrvImo, string(message.Key))
	var envelope struct {
		EventType string `json:"eventType"`
		EventID   string `json:"eventId"`
	}
	require.NoError(t, json.Unmarshal(message.Value, &envelope))
	require.Equal(t, mrv.EventFuelReport, envelope.EventType)
	require.Equal(t, ourEvent, envelope.EventID, "outbox event id is the idempotent envelope eventId")
	require.NoError(t, mrv.VerifySignedEnvelope(message.Value, h.publicKey))
}

// consumeOne reads a topic/partition until the message carrying wantEventID
// arrives (broker-gated runs; earlier runs' messages are skipped).
func consumeOne(ctx context.Context, brokers, topic, wantEventID string) (kafka.Message, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(brokers, ","), Topic: topic, Partition: 0,
		StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 2 * time.Second,
	})
	defer reader.Close()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		message, err := reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			continue
		}
		var probe struct {
			EventID string `json:"eventId"`
		}
		if json.Unmarshal(message.Value, &probe) == nil && probe.EventID == wantEventID {
			return message, nil
		}
	}
	return kafka.Message{}, fmt.Errorf("event %s not found on topic %s within the deadline", wantEventID, topic)
}

func u32ptr(value uint32) *uint32 { return &value }

func timePtr(value time.Time) *time.Time { return &value }

// withActorExpectError runs fn bound and requires an error (trigger/RLS).
func (h *mrvHarness) withActorExpectError(t *testing.T, actor string, fn func(tx pgx.Tx) error) {
	t.Helper()
	ctx := context.Background()
	tx, err := h.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SELECT set_config('app.mrv_actor', $1, true)`, actor)
	require.NoError(t, err)
	require.Error(t, fn(tx))
}
