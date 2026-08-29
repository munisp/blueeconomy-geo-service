// Phase-6 remediation regression tests: SOS lifecycle (GE-1), RLS
// default-deny (GE-2) and SCD-2 out-of-order static reports (GE-3).
// Same infrastructure gates as pipeline_integration_test.go.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/api"
	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// headerAuthenticator is a test Authenticator asserting verified-claim
// equivalents from request headers (subject, roles, clearance, tenant).
type headerAuthenticator struct{}

func (headerAuthenticator) Authenticate(request *http.Request) (auth.Principal, error) {
	subject := strings.TrimSpace(request.Header.Get("X-Test-Subject"))
	if subject == "" {
		return auth.Principal{}, errors.New("subject header is required")
	}
	roles := make(map[string]struct{})
	for _, role := range strings.Split(request.Header.Get("X-Test-Roles"), ",") {
		if trimmed := strings.TrimSpace(role); trimmed != "" {
			roles[trimmed] = struct{}{}
		}
	}
	return auth.Principal{
		Subject:   subject,
		Roles:     roles,
		Clearance: strings.TrimSpace(request.Header.Get("X-Test-Clearance")),
		TenantID:  strings.TrimSpace(request.Header.Get("X-Test-Tenant")),
	}, nil
}

// sosRequest issues one authenticated request against the API handler.
func sosRequest(t *testing.T, handler http.Handler, method, path, roles, clearance, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	request.Header.Set("X-Test-Subject", "itest-duty-officer")
	request.Header.Set("X-Test-Roles", roles)
	request.Header.Set("X-Test-Clearance", clearance)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func insertTestSOS(t *testing.T, h *harness, id, outbox string) {
	t.Helper()
	inserted, err := h.store.InsertSOSAlert(context.Background(), store.SOSAlert{
		SosAlertID:      id,
		ReporterID:      "itest-reporter-1",
		VesselReference: "itest-vessel-1",
		LatitudeMicros:  6418000,
		LongitudeMicros: 3372500,
		RecordedAt:      time.Now().UTC(),
		OutboxID:        outbox,
		FreeText:        "taking on water",
		Classification:  "RESTRICTED",
	})
	require.NoError(t, err)
	require.True(t, inserted)
}

// TestSOSLifecycle exercises the full RAISED -> ACKNOWLEDGED -> RESOLVED
// lifecycle over HTTP: gate enforcement (403), illegal transitions (409),
// actor/timestamp/note persistence and signed lifecycle event emission.
func TestSOSLifecycle(t *testing.T) {
	h := newHarness(t)
	h.clean(t)
	ctx := context.Background()

	server, err := api.NewServer(h.store, metrics.NewRegistry())
	require.NoError(t, err)
	server.SOSEvents = h.pipeline
	handler := server.Handler(headerAuthenticator{}, nil)

	insertTestSOS(t, h, "itest-sos-lc-1", "obx-lc-1")

	// Gate: geo-reader role alone is not admitted to the lifecycle.
	response := sosRequest(t, handler, "POST", "/v1/geo/sos/itest-sos-lc-1/acknowledge", "geo-reader", "SECRET", `{}`)
	require.Equal(t, http.StatusForbidden, response.Code)
	// Gate: the sos role with clearance below RESTRICTED is refused.
	response = sosRequest(t, handler, "POST", "/v1/geo/sos/itest-sos-lc-1/acknowledge", "geo-sos-reader", "PUBLIC", `{}`)
	require.Equal(t, http.StatusForbidden, response.Code)
	// Unknown alert: 404.
	response = sosRequest(t, handler, "POST", "/v1/geo/sos/itest-sos-missing/acknowledge", "geo-sos-reader", "RESTRICTED", `{}`)
	require.Equal(t, http.StatusNotFound, response.Code)

	// RAISED -> RESOLVED is illegal only from ACKNOWLEDGED's perspective;
	// here the full ladder: acknowledge then resolve.
	response = sosRequest(t, handler, "POST", "/v1/geo/sos/itest-sos-lc-1/acknowledge", "geo-sos-reader", "RESTRICTED", `{"note":"SAR paged"}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var ackBody struct {
		SosAlert store.SOSRow `json:"sosAlert"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &ackBody))
	require.Equal(t, "ACKNOWLEDGED", ackBody.SosAlert.State)
	require.Equal(t, "itest-duty-officer", ackBody.SosAlert.AcknowledgedBy)
	require.NotNil(t, ackBody.SosAlert.AcknowledgedAt)

	// ACKNOWLEDGED -> ACKNOWLEDGED is an illegal transition: 409.
	response = sosRequest(t, handler, "POST", "/v1/geo/sos/itest-sos-lc-1/acknowledge", "geo-sos-reader", "RESTRICTED", `{}`)
	require.Equal(t, http.StatusConflict, response.Code)

	// ACKNOWLEDGED -> RESOLVED.
	response = sosRequest(t, handler, "POST", "/v1/geo/sos/itest-sos-lc-1/resolve", "geo-admin", "SECRET", `{"note":"vessel alongside"}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var resBody struct {
		SosAlert store.SOSRow `json:"sosAlert"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &resBody))
	require.Equal(t, "RESOLVED", resBody.SosAlert.State)
	require.Equal(t, "itest-duty-officer", resBody.SosAlert.ResolvedBy)
	require.NotNil(t, resBody.SosAlert.ResolvedAt)
	// The acknowledgement ledger entry survives the resolution.
	require.Equal(t, "itest-duty-officer", resBody.SosAlert.AcknowledgedBy)

	// RESOLVED is terminal: 409 on any further transition.
	response = sosRequest(t, handler, "POST", "/v1/geo/sos/itest-sos-lc-1/resolve", "geo-sos-reader", "RESTRICTED", `{}`)
	require.Equal(t, http.StatusConflict, response.Code)

	// Direct RAISED -> RESOLVED is legal.
	insertTestSOS(t, h, "itest-sos-lc-2", "obx-lc-2")
	response = sosRequest(t, handler, "POST", "/v1/geo/sos/itest-sos-lc-2/resolve", "geo-sos-reader", "RESTRICTED", `{}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	// The read model exposes lifecycle state (dashboards stop treating old
	// alerts as live).
	response = sosRequest(t, handler, "GET", "/v1/geo/sos", "geo-sos-reader", "RESTRICTED", "")
	require.Equal(t, http.StatusOK, response.Code)
	var listBody struct {
		SosAlerts []store.SOSRow `json:"sosAlerts"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &listBody))
	states := make(map[string]string)
	for _, alert := range listBody.SosAlerts {
		states[alert.SosAlertID] = alert.State
	}
	require.Equal(t, "RESOLVED", states["itest-sos-lc-1"])
	require.Equal(t, "RESOLVED", states["itest-sos-lc-2"])

	// The database ledger persisted actor + timestamp.
	var ackBy, resBy string
	var ackAt, resAt *time.Time
	require.NoError(t, h.store.Pool().QueryRow(ctx,
		`SELECT COALESCE(acknowledged_by, ''), acknowledged_at, COALESCE(resolved_by, ''), resolved_at
		 FROM sos_alerts WHERE sos_alert_id = 'itest-sos-lc-1'`).Scan(&ackBy, &ackAt, &resBy, &resAt))
	require.Equal(t, "itest-duty-officer", ackBy)
	require.Equal(t, "itest-duty-officer", resBy)
	require.NotNil(t, ackAt)
	require.NotNil(t, resAt)

	// Signed lifecycle envelopes were emitted on vessels.events with the
	// RESTRICTED floor and SAFETY priority (recorder stub only).
	if _, usingRecorder := h.publisher.(*recorder); usingRecorder {
		seen := map[string]int{}
		for _, message := range h.recorder.byTopic("vessels.events") {
			var envelope map[string]any
			require.NoError(t, json.Unmarshal(message.Value, &envelope))
			eventType, _ := envelope["eventType"].(string)
			if eventType == sign.EventSOSAcknowledged || eventType == sign.EventSOSResolved {
				seen[eventType]++
				require.Equal(t, "RESTRICTED", envelope["classification"])
				require.NotEmpty(t, envelope["provenance"].(map[string]any)["signature"])
				require.Equal(t, "SAFETY", message.Headers["priority"])
			}
		}
		require.Equal(t, 1, seen[sign.EventSOSAcknowledged])
		require.Equal(t, 2, seen[sign.EventSOSResolved])
	}
}
