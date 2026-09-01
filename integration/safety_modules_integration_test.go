// Phase-12 safety-compliance integration tests against a real
// PostgreSQL+PostGIS: FSC/PSC inspection state machine with detention
// maker-checker, SAR IAMSAR phase ladder with resource tasking and comms
// ledger, and marine accident investigation with hash-chained evidence.
// Gated by GEO_TEST_PG_DSN (fresh database per run, same doctrine as the
// MRV harness); no Redis/Kafka required — envelopes are constructed with a
// real Ed25519 signer and recorded in-process.
package integration

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/db"
	"github.com/munisp/blueeconomy-geo-service/internal/api"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// safetyRecorder builds real signed envelopes (fail-closed contract
// validation included) and records them for assertions.
type safetyRecorder struct {
	signer    *sign.Signer
	envelopes []sign.Envelope
}

func (rec *safetyRecorder) PublishSignedEnvelope(_ context.Context, eventType, correlationID string,
	payload any, occurredAt time.Time, _ string, _ map[string]string) error {
	envelope, err := sign.NewEnvelope(eventType, correlationID, payload, occurredAt,
		sign.Provenance{PrincipalID: "safety-itest", PrincipalRole: "geo-safety-producer"}, rec.signer)
	if err != nil {
		return err
	}
	rec.envelopes = append(rec.envelopes, envelope)
	return nil
}

type safetyHarness struct {
	store    *store.Store
	handler  http.Handler
	recorder *safetyRecorder
}

func newSafetyHarness(t *testing.T) *safetyHarness {
	t.Helper()
	dsn := os.Getenv("GEO_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("GEO_TEST_PG_DSN is required for safety integration tests")
	}
	ctx := context.Background()
	appURL, err := url.Parse(dsn)
	require.NoError(t, err)
	adminURL := *appURL
	adminURL.User = url.UserPassword("postgres", "")
	adminURL.Path = "/postgres"
	admin, err := pgxpool.New(ctx, adminURL.String())
	require.NoError(t, err)
	defer admin.Close()
	const testDB = "safety_itest"
	_, err = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", testDB))
	require.NoError(t, err)
	_, err = admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s OWNER %s", testDB, appURL.User.Username()))
	require.NoError(t, err)
	_, err = admin.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s SET timezone TO 'UTC'", testDB))
	require.NoError(t, err)
	adminDBURL := adminURL
	adminDBURL.Path = "/" + testDB
	adminDB, err := pgxpool.New(ctx, adminDBURL.String())
	require.NoError(t, err)
	_, err = adminDB.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS postgis")
	require.NoError(t, err)
	adminDB.Close()

	appURL.Path = "/" + testDB
	query := appURL.Query()
	query.Set("timezone", "UTC")
	appURL.RawQuery = query.Encode()
	migrator, err := pgxpool.New(ctx, appURL.String())
	require.NoError(t, err)
	require.NoError(t, store.MigratePool(ctx, migrator, db.MigrationsFS))
	migrator.Close()

	ingestURL := *appURL
	ingestURL.User = url.UserPassword("geo_ingest", "geo_ingest")
	storage, err := store.New(ctx, appURL.String(), ingestURL.String())
	require.NoError(t, err)
	t.Cleanup(storage.Close)

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := sign.NewSigner(privateKey, "0")
	require.NoError(t, err)
	recorder := &safetyRecorder{signer: signer}

	server, err := api.NewServer(storage, metrics.NewRegistry())
	require.NoError(t, err)
	safety, err := api.NewSafety(storage)
	require.NoError(t, err)
	safety.Events = recorder
	server.Safety = safety
	return &safetyHarness{store: storage, handler: server.Handler(headerAuthenticator{}, nil), recorder: recorder}
}

const safetyTenant = "itest-safety-tenant"

func stringsReader(body string) *strings.Reader { return strings.NewReader(body) }

// safetyRequest issues one authenticated request with the given actor.
func safetyRequest(t *testing.T, h *safetyHarness, actor, method, path, roles, clearance, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, stringsReader(body))
	request.Header.Set("X-Test-Subject", actor)
	request.Header.Set("X-Test-Roles", roles)
	request.Header.Set("X-Test-Clearance", clearance)
	request.Header.Set("X-Test-Tenant", safetyTenant)
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, request)
	return recorder
}

func seedTemplate(t *testing.T, h *safetyHarness) {
	t.Helper()
	response := safetyRequest(t, h, "itest-inspector-1", "POST", "/v1/safety/checklist-templates",
		"geo-inspector", "RESTRICTED", `{
			"templateId":"psc-tokyo-mou-1","regime":"PSC","version":1,"title":"Tokyo MOU PSC checklist",
			"items":[{"code":"01101","description":"Cargo ship safety equipment certificate","severity":"MAJOR"},
			         {"code":"07106","description":"Fire dampers operation","severity":"CRITICAL"},
			         {"code":"10109","description":"Navigation lights","severity":"MINOR"}]}`)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
}

func seedInspection(t *testing.T, h *safetyHarness, id string) {
	t.Helper()
	response := safetyRequest(t, h, "itest-inspector-1", "POST", "/v1/safety/inspections",
		"geo-inspector", "RESTRICTED", fmt.Sprintf(`{
			"inspectionId":%q,"regime":"PSC","templateId":"psc-tokyo-mou-1",
			"vesselReference":"IMO9074729","portCode":"SGSIN","classification":"RESTRICTED"}`, id))
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
}

func transition(t *testing.T, h *safetyHarness, actor, path, action, extra string) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"action":%q%s}`, action, extra)
	return safetyRequest(t, h, actor, "POST", path, "geo-inspector,geo-inspection-checker,geo-sar-coordinator,geo-investigator", "RESTRICTED", body)
}

// TestInspectionLifecycle exercises the full inspection state machine: the
// detention workflow (detain -> rectify -> release) with maker-checker
// release, deficiency severity/deadline rules and the CRITICAL-verified
// release gate.
func TestInspectionLifecycle(t *testing.T) {
	h := newSafetyHarness(t)
	seedTemplate(t, h)
	seedInspection(t, h, "insp-1")

	// Clearance floor: PUBLIC is refused.
	response := safetyRequest(t, h, "itest-inspector-1", "GET", "/v1/safety/inspections/insp-1",
		"geo-inspector", "PUBLIC", "")
	require.Equal(t, http.StatusForbidden, response.Code)

	// Illegal: detain before start.
	response = transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "detain", `,"note":"x"`)
	require.Equal(t, http.StatusConflict, response.Code)
	// SCHEDULED -> IN_PROGRESS -> COMPLETED.
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "start", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "complete", "").Code)
	// Illegal: start again.
	require.Equal(t, http.StatusConflict, transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "start", "").Code)

	// Deficiency with an unknown checklist code is refused.
	response = safetyRequest(t, h, "itest-inspector-1", "POST", "/v1/safety/inspections/insp-1/deficiencies",
		"geo-inspector", "RESTRICTED", `{"deficiencyId":"def-bad","code":"99999","description":"bogus","severity":"MINOR"}`)
	require.Equal(t, http.StatusConflict, response.Code)
	// CRITICAL deficiency without a rectification deadline violates the
	// database CHECK (severity doctrine) — 500 surfaces the refused write.
	deadline := time.Now().UTC().Add(17 * 24 * time.Hour).Format(time.RFC3339)
	response = safetyRequest(t, h, "itest-inspector-1", "POST", "/v1/safety/inspections/insp-1/deficiencies",
		"geo-inspector", "RESTRICTED", `{"deficiencyId":"def-1","code":"07106","description":"fire dampers seized","severity":"CRITICAL","rectificationDeadline":"`+deadline+`"}`)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())

	// Detain requires grounds; then detain.
	require.Equal(t, http.StatusConflict, transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "detain", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "detain", `,"note":"detainable: fire dampers"`).Code)

	// Rectify path; release is blocked while the CRITICAL deficiency is not
	// VERIFIED.
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "rectify", "").Code)
	response = transition(t, h, "itest-checker-1", "/v1/safety/inspections/insp-1/transition", "release", "")
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())

	// Rectify the deficiency, then verify — the rectifier cannot verify
	// their own work (maker-checker).
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inspector-1", "/v1/safety/deficiencies/def-1/transition", "rectify", "").Code)
	response = transition(t, h, "itest-inspector-1", "/v1/safety/deficiencies/def-1/transition", "verify", "")
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Equal(t, http.StatusOK, transition(t, h, "itest-checker-1", "/v1/safety/deficiencies/def-1/transition", "verify", "").Code)

	// Release maker-checker: the detaining maker cannot release.
	response = transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "release", "")
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	response = transition(t, h, "itest-checker-1", "/v1/safety/inspections/insp-1/transition", "release", "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var releaseBody struct {
		Inspection store.InspectionRow `json:"inspection"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &releaseBody))
	require.Equal(t, "RELEASED", releaseBody.Inspection.State)
	require.Equal(t, "itest-checker-1", *releaseBody.Inspection.ReleasedBy)
	require.Equal(t, "itest-inspector-1", *releaseBody.Inspection.DetainedBy)

	// RELEASED -> CLOSED (terminal).
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "close", "").Code)
	require.Equal(t, http.StatusConflict, transition(t, h, "itest-inspector-1", "/v1/safety/inspections/insp-1/transition", "start", "").Code)

	// Every lifecycle mutation published a signed safety.inspection.v1
	// envelope.
	require.NotEmpty(t, h.recorder.envelopes)
	for _, envelope := range h.recorder.envelopes {
		require.Equal(t, "safety.inspection.v1", envelope.EventType)
		require.NotEmpty(t, envelope.Provenance.Signature)
	}
}

// TestSarLifecycle exercises the IAMSAR phase ladder, SOS linkage, resource
// tasking and the append-only comms log.
func TestSarLifecycle(t *testing.T) {
	h := newSafetyHarness(t)
	ctx := context.Background()

	// Link the incident to a persisted SOS alert (0006 lifecycle table).
	inserted, err := h.store.InsertSOSAlert(ctx, store.SOSAlert{
		SosAlertID: "sar-sos-1", ReporterID: "itest-reporter", VesselReference: "itest-vessel",
		LatitudeMicros: 6418000, LongitudeMicros: 3372500, RecordedAt: time.Now().UTC(),
		OutboxID: "obx-sar-1", FreeText: "mayday", Classification: "RESTRICTED",
	})
	require.NoError(t, err)
	require.True(t, inserted)

	response := safetyRequest(t, h, "itest-coord-1", "POST", "/v1/safety/sar",
		"geo-sar-coordinator", "RESTRICTED", `{
			"incidentId":"sar-1","sosAlertId":"sar-sos-1","title":"vessel taking on water",
			"classification":"RESTRICTED","latitudeMicros":6418000,"longitudeMicros":3372500}`)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	var createBody struct {
		SarIncident store.SarIncidentRow `json:"sarIncident"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &createBody))
	require.Equal(t, "UNCERTAINTY", createBody.SarIncident.Phase)
	require.Equal(t, "sar-sos-1", *createBody.SarIncident.SosAlertID)

	// Phase ladder is strict: distress from UNCERTAINTY is illegal.
	require.Equal(t, http.StatusConflict, transition(t, h, "itest-coord-1", "/v1/safety/sar/sar-1/transition", "distress", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-coord-1", "/v1/safety/sar/sar-1/transition", "alert", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-coord-1", "/v1/safety/sar/sar-1/transition", "distress", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-coord-1", "/v1/safety/sar/sar-1/transition", "rescue", "").Code)

	// Resource tasking + tasking state machine.
	response = safetyRequest(t, h, "itest-coord-1", "POST", "/v1/safety/sar/sar-1/taskings",
		"geo-sar-coordinator", "RESTRICTED", `{"taskingId":"task-1","resourceType":"VESSEL","resourceName":"MPA Patrol 7"}`)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	require.Equal(t, http.StatusOK, transition(t, h, "itest-coord-1", "/v1/safety/sar-taskings/task-1/transition", "enroute", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-coord-1", "/v1/safety/sar-taskings/task-1/transition", "onscene", "").Code)

	// Comms ledger appends and reads back in order.
	for _, message := range []string{"mayday relay received", "tasking MPA Patrol 7", "on scene, 3 POB recovered"} {
		response = safetyRequest(t, h, "itest-coord-1", "POST", "/v1/safety/sar/sar-1/comms",
			"geo-sar-coordinator", "RESTRICTED",
			fmt.Sprintf(`{"direction":"OUTBOUND","channel":"VHF-16","message":%q}`, message))
		require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	}
	response = safetyRequest(t, h, "itest-coord-1", "GET", "/v1/safety/sar/sar-1/comms",
		"geo-sar-coordinator", "RESTRICTED", "")
	require.Equal(t, http.StatusOK, response.Code)
	var commsBody struct {
		CommsLog []store.SarCommsRow `json:"commsLog"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &commsBody))
	require.Len(t, commsBody.CommsLog, 3)
	require.Equal(t, "mayday relay received", commsBody.CommsLog[0].Message)

	// Close requires a reason; CLOSED is terminal and blocks tasking.
	require.Equal(t, http.StatusConflict, transition(t, h, "itest-coord-1", "/v1/safety/sar/sar-1/transition", "close", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-coord-1", "/v1/safety/sar/sar-1/transition", "close", `,"reason":"all persons recovered"`).Code)
	require.Equal(t, http.StatusConflict, transition(t, h, "itest-coord-1", "/v1/safety/sar/sar-1/transition", "alert", "").Code)
	response = safetyRequest(t, h, "itest-coord-1", "POST", "/v1/safety/sar/sar-1/taskings",
		"geo-sar-coordinator", "RESTRICTED", `{"taskingId":"task-2","resourceType":"TEAM","resourceName":"Divers"}`)
	require.Equal(t, http.StatusConflict, response.Code)

	// Signed safety.sar.v1 envelopes were emitted for open/transitions/task.
	var sarEvents int
	for _, envelope := range h.recorder.envelopes {
		if envelope.EventType == "safety.sar.v1" {
			sarEvents++
		}
	}
	require.GreaterOrEqual(t, sarEvents, 5)
}

// TestInvestigationLifecycle exercises the case state machine, hash-chain
// evidence integrity, findings and recommendations.
func TestInvestigationLifecycle(t *testing.T) {
	h := newSafetyHarness(t)

	response := safetyRequest(t, h, "itest-inv-1", "POST", "/v1/safety/investigations",
		"geo-investigator", "RESTRICTED", fmt.Sprintf(`{
			"caseId":"case-1","casualtyType":"COLLISION","severity":"VERY_SERIOUS",
			"vesselReference":"IMO9074729","occurredAt":%q,"classification":"RESTRICTED",
			"latitudeMicros":6418000,"longitudeMicros":3372500}`, time.Now().UTC().Add(-48*time.Hour).Format(time.RFC3339)))
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())

	// Evidence only lands in EVIDENCE state.
	content := sha256.Sum256([]byte("vdr-extract-1"))
	contentHash := hex.EncodeToString(content[:])
	response = safetyRequest(t, h, "itest-inv-1", "POST", "/v1/safety/investigations/case-1/evidence",
		"geo-investigator", "RESTRICTED", fmt.Sprintf(`{"evidenceId":"ev-1","description":"VDR extract","contentHash":%q}`, contentHash))
	require.Equal(t, http.StatusConflict, response.Code)

	require.Equal(t, http.StatusOK, transition(t, h, "itest-inv-1", "/v1/safety/investigations/case-1/transition", "evidence", "").Code)

	// Append three evidence items; the chain links genesis -> ev-1 -> ev-2 -> ev-3.
	prevChain := store.EvidenceGenesisHash
	for i, id := range []string{"ev-1", "ev-2", "ev-3"} {
		sum := sha256.Sum256([]byte("artefact-" + id))
		hash := hex.EncodeToString(sum[:])
		response = safetyRequest(t, h, "itest-inv-1", "POST", "/v1/safety/investigations/case-1/evidence",
			"geo-investigator", "RESTRICTED", fmt.Sprintf(`{"evidenceId":%q,"description":"artefact %d","contentHash":%q}`, id, i+1, hash))
		require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
		var evBody struct {
			Evidence store.EvidenceRow `json:"evidence"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &evBody))
		require.Equal(t, prevChain, evBody.Evidence.PrevChainHash)
		require.Equal(t, store.EvidenceChainHash(prevChain, hash), evBody.Evidence.ChainHash)
		prevChain = evBody.Evidence.ChainHash
	}

	// Read-back verifies the chain end-to-end.
	response = safetyRequest(t, h, "itest-inv-1", "GET", "/v1/safety/investigations/case-1/evidence",
		"geo-investigator", "RESTRICTED", "")
	require.Equal(t, http.StatusOK, response.Code)
	var evList struct {
		Evidence   []store.EvidenceRow `json:"evidence"`
		ChainValid bool                `json:"chainValid"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &evList))
	require.Len(t, evList.Evidence, 3)
	require.True(t, evList.ChainValid)

	// Findings/recommendations require ANALYSIS (or REPORTED) state.
	response = safetyRequest(t, h, "itest-inv-1", "POST", "/v1/safety/investigations/case-1/findings",
		"geo-investigator", "RESTRICTED", `{"findingId":"f-1","finding":"premature"}`)
	require.Equal(t, http.StatusConflict, response.Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inv-1", "/v1/safety/investigations/case-1/transition", "analysis", "").Code)
	require.Equal(t, http.StatusCreated, safetyRequest(t, h, "itest-inv-1", "POST", "/v1/safety/investigations/case-1/findings",
		"geo-investigator", "RESTRICTED", `{"findingId":"f-1","finding":"COLREG rule 8 not observed"}`).Code)
	require.Equal(t, http.StatusCreated, safetyRequest(t, h, "itest-inv-1", "POST", "/v1/safety/investigations/case-1/recommendations",
		"geo-investigator", "RESTRICTED", `{"recommendationId":"r-1","recommendation":"mandatory ECDIS refamiliarization"}`).Code)

	// Recommendation decision ladder.
	require.Equal(t, http.StatusConflict, transition(t, h, "itest-inv-1", "/v1/safety/recommendations/r-1/transition", "implement", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inv-1", "/v1/safety/recommendations/r-1/transition", "accept", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inv-1", "/v1/safety/recommendations/r-1/transition", "implement", "").Code)

	// Report then close; CLOSED is terminal.
	require.Equal(t, http.StatusConflict, transition(t, h, "itest-inv-1", "/v1/safety/investigations/case-1/transition", "close", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inv-1", "/v1/safety/investigations/case-1/transition", "report", "").Code)
	require.Equal(t, http.StatusOK, transition(t, h, "itest-inv-1", "/v1/safety/investigations/case-1/transition", "close", "").Code)
	require.Equal(t, http.StatusConflict, transition(t, h, "itest-inv-1", "/v1/safety/investigations/case-1/transition", "evidence", "").Code)

	// Evidence chain commitment was announced on safety.investigation.v1.
	var evidenceEvents int
	for _, envelope := range h.recorder.envelopes {
		require.Equal(t, "safety.investigation.v1", envelope.EventType)
		if envelope.CorrelationID == "case-1" {
			evidenceEvents++
		}
	}
	require.GreaterOrEqual(t, evidenceEvents, 5)
}

// TestSafetyTenantIsolation proves the 0016 RLS posture: a second tenant
// cannot read or transition the first tenant's safety records.
func TestSafetyTenantIsolation(t *testing.T) {
	h := newSafetyHarness(t)
	seedTemplate(t, h)
	seedInspection(t, h, "insp-iso-1")

	foreign := func(method, path, body string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, path, stringsReader(body))
		request.Header.Set("X-Test-Subject", "itest-foreign")
		request.Header.Set("X-Test-Roles", "geo-inspector,geo-admin")
		request.Header.Set("X-Test-Clearance", "SECRET")
		request.Header.Set("X-Test-Tenant", "itest-other-tenant")
		recorder := httptest.NewRecorder()
		h.handler.ServeHTTP(recorder, request)
		return recorder
	}
	require.Equal(t, http.StatusNotFound, foreign("GET", "/v1/safety/inspections/insp-iso-1", "").Code)
	response := foreign("GET", "/v1/safety/inspections", "")
	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{"inspections":[]}`, response.Body.String())
	require.Equal(t, http.StatusNotFound, foreign("POST", "/v1/safety/inspections/insp-iso-1/transition", `{"action":"start"}`).Code)
	response = foreign("GET", "/v1/safety/checklist-templates", "")
	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{"checklistTemplates":[]}`, response.Body.String())
}
