// Route-table regression tests: registering the full device-plane route
// tree on one mux must never panic (Go 1.22+ ServeMux panics on ambiguous
// pattern pairs at registration — boot-fatal in production), and the
// historically ambiguous neighbours must route to distinct handlers.
package devices

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
)

// rejectAllAuthenticator fails every principal authentication (the
// admin-route wrapper then answers 401, proving the route registered).
type rejectAllAuthenticator struct{}

func (rejectAllAuthenticator) Authenticate(*http.Request) (auth.Principal, error) {
	return auth.Principal{}, errors.New("no principal in tests")
}

func newRouteTestAPI(t *testing.T) *API {
	t.Helper()
	verifier, err := NewVerifier(newFakeAuthReader(), metrics.NewRegistry(), time.Hour)
	require.NoError(t, err)
	return &API{
		Verifier: verifier, Metrics: metrics.NewRegistry(),
		Events: &fakeEventPublisher{}, DeadLetters: &fakeDeadLetters{},
		Grace: time.Hour, now: time.Now,
	}
}

func TestRegisterRoutesNeverPanics(t *testing.T) {
	api := newRouteTestAPI(t)
	require.NotPanics(t, func() {
		mux := http.NewServeMux()
		api.RegisterRoutes(mux, rejectAllAuthenticator{})
	}, "route table must be free of ambiguous ServeMux patterns")
}

func TestHistoricallyAmbiguousNeighboursRouteDistinctly(t *testing.T) {
	api := newRouteTestAPI(t)
	mux := http.NewServeMux()
	require.NotPanics(t, func() { api.RegisterRoutes(mux, rejectAllAuthenticator{}) })

	// GET /v1/devices/{id}/firmware is a device-proof endpoint: without a
	// proof it answers its own 401 (reached the firmware handler).
	request := httptest.NewRequest("GET", "/v1/devices/some-device/firmware", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.Contains(t, response.Body.String(), "Device authorization proof is required")

	// GET /v1/device-provisioning/requests/{id} is an admin endpoint: the
	// principal middleware rejects first with the platform 401 body.
	request = httptest.NewRequest("GET", "/v1/device-provisioning/requests/some-request", nil)
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.Contains(t, response.Body.String(), "unauthenticated")

	// The pair that panicked before the route restructure must resolve:
	// /v1/devices/provisioning-requests/firmware is a firmware fetch for
	// the device literally named "provisioning-requests" (401, firmware
	// handler), not an ambiguous registration.
	request = httptest.NewRequest("GET", "/v1/devices/provisioning-requests/firmware", nil)
	response = httptest.NewRecorder()
	require.NotPanics(t, func() { mux.ServeHTTP(response, request) })
	require.Equal(t, http.StatusUnauthorized, response.Code)
	require.Contains(t, response.Body.String(), "Device authorization proof is required")
}
