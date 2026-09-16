package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/stretchr/testify/require"
)

// Phase 19 zone-category alerting: an ENTER into an MPA-category fence must
// announce a PROTECTED_ZONE_ENTRY alert on the signed geo.geofence-event.v1
// envelope and persist the category snapshot on the event.
func TestZoneCategoryMPAEntryAlerting(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)
	pub := &recordingPublisher{}
	g.FenceEvents = pub

	create := `{"geofenceId":"mpa.lagos.bay","name":"Lagos Bay MPA","classification":"INTERNAL","zoneCategory":"mpa","verticesMicros":` + testRing + `}`
	rec := httptest.NewRecorder()
	g.createFence(rec, httptest.NewRequest("POST", "/v1/geo/fences", strings.NewReader(create)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"zoneCategory":"mpa"`)

	// outside → inside → inside → outside: ENTER alert then EXIT alert.
	eval := `{"reports":[
		{"mmsi":"205123000","latMicros":-6000000,"lonMicros":39500000,"sogMilliknots":5000,"observedAtUnix":1000},
		{"mmsi":"205123000","latMicros":-4500000,"lonMicros":39500000,"sogMilliknots":5000,"observedAtUnix":1010},
		{"mmsi":"205123000","latMicros":-4600000,"lonMicros":39500000,"sogMilliknots":5000,"observedAtUnix":1020},
		{"mmsi":"205123000","latMicros":-6000000,"lonMicros":39500000,"sogMilliknots":5000,"observedAtUnix":1030}]}`
	rec = httptest.NewRecorder()
	g.evaluatePositions(rec, httptest.NewRequest("POST", "/v1/geo/fences/evaluate", strings.NewReader(eval)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"zoneCategory":"mpa"`)
	require.Contains(t, rec.Body.String(), `"alert":"PROTECTED_ZONE_ENTRY"`)
	require.Contains(t, rec.Body.String(), `"alert":"PROTECTED_ZONE_EXIT"`)

	require.Len(t, pub.payloads, 2)
	enter, ok := pub.payloads[0].(sign.GeofenceEventRecorded)
	require.True(t, ok, "payload must be the signed GeofenceEventRecorded contract")
	require.Equal(t, "ENTER", enter.Event)
	require.Equal(t, "mpa", enter.ZoneCategory)
	require.Equal(t, "PROTECTED_ZONE_ENTRY", enter.Alert)
	require.Equal(t, "Lagos Bay MPA", enter.ZoneName)
	require.Equal(t, "mpa", pub.headers[0]["zoneCategory"])
	exit := pub.payloads[1].(sign.GeofenceEventRecorded)
	require.Equal(t, "EXIT", exit.Event)
	require.Equal(t, "PROTECTED_ZONE_EXIT", exit.Alert)

	require.Len(t, fake.events, 2)
	require.Equal(t, "mpa", fake.events[0].ZoneCategory, "category snapshot persists on the event row")
}

// General-category zones stay informational: no alert token, no protected
// header — the transition is still recorded and signed.
func TestZoneCategoryGeneralIsInformational(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)
	pub := &recordingPublisher{}
	g.FenceEvents = pub

	create := `{"geofenceId":"anchorage.kilindini","name":"Kilindini","classification":"INTERNAL","verticesMicros":` + testRing + `}`
	rec := httptest.NewRecorder()
	g.createFence(rec, httptest.NewRequest("POST", "/v1/geo/fences", strings.NewReader(create)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Contains(t, rec.Body.String(), `"zoneCategory":"general"`, "empty category normalizes to general")

	eval := `{"reports":[
		{"mmsi":"205123000","latMicros":-6000000,"lonMicros":39500000,"sogMilliknots":5000,"observedAtUnix":1000},
		{"mmsi":"205123000","latMicros":-4500000,"lonMicros":39500000,"sogMilliknots":5000,"observedAtUnix":1010}]}`
	rec = httptest.NewRecorder()
	g.evaluatePositions(rec, httptest.NewRequest("POST", "/v1/geo/fences/evaluate", strings.NewReader(eval)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, pub.payloads, 1)
	enter := pub.payloads[0].(sign.GeofenceEventRecorded)
	require.Equal(t, "general", enter.ZoneCategory)
	require.Empty(t, enter.Alert)
	require.Equal(t, "general", fake.events[0].ZoneCategory)
}

// Unknown categories are rejected fail-closed at the write boundary.
func TestZoneCategoryRejectedWhenUnknown(t *testing.T) {
	fake := &fakeGeoV2Store{}
	g := newTestGeoV2(fake)
	create := `{"geofenceId":"bad.zone","name":"Bad","classification":"INTERNAL","zoneCategory":"sandbox","verticesMicros":` + testRing + `}`
	rec := httptest.NewRecorder()
	g.createFence(rec, httptest.NewRequest("POST", "/v1/geo/fences", strings.NewReader(create)).WithContext(testPrincipalCtx()))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "zoneCategory")
}
