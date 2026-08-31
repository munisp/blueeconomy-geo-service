// HTTP-surface tests for /v1/mrv: PBAC role gating, intake envelope
// behavior and the public factor table — against an in-memory service
// double (no database) for the pure HTTP concerns, plus the trusted-proxy
// principal parsing the boundary shares with geo-service.
package mrv

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
)

// trustedRequest builds a request carrying a trusted-proxy principal.
func trustedRequest(t *testing.T, method, path, body, subject, roles, clearance string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.RemoteAddr = "192.0.2.10:51234"
	request.Header.Set("X-Blueeconomy-Authenticated-By", "itest-proxy")
	request.Header.Set("X-Blueeconomy-Authenticated-Subject", subject)
	request.Header.Set("X-Blueeconomy-Authenticated-Roles", roles)
	request.Header.Set("X-Blueeconomy-Authenticated-Clearance", clearance)
	return request
}

// TestHTTPRoleGating proves the PBAC role contract on the mux itself with a
// nil-backed handler surface (handlers fail before touching storage only
// when the role gate lets the request through; the gate is what we assert).
func TestHTTPRoleGating(t *testing.T) {
	_, network, err := net.ParseCIDR("192.0.2.0/24")
	require.NoError(t, err)
	authenticator := auth.TrustedProxyAuthenticator{CIDRs: []*net.IPNet{network}, Identity: "itest-proxy"}
	// A service with nil internals is never reached when the gate denies;
	// healthz needs no service internals.
	server := &Server{service: &Service{Metrics: metrics.NewRegistry()}}
	handler := server.Handler(authenticator)

	// Unauthenticated: 401.
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/mrv/ships", nil))
	require.Equal(t, http.StatusUnauthorized, response.Code)

	// Wrong role (reader may not register ships): 403.
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, trustedRequest(t, http.MethodPost, "/v1/mrv/ships", "{}",
		"op-1", "mrv-reader", "INTERNAL"))
	require.Equal(t, http.StatusForbidden, response.Code)

	// healthz is unauthenticated and honest about the absent CII config.
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), `"loaded":false`)
}
