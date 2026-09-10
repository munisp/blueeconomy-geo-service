package api

import (
	"net/http"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/realtime"
)

// StreamStatus reports the realtime fan-out posture truthfully to
// authenticated callers (capability discovery, #9): whether SSE is enabled,
// the live subscriber count, and the event types the stream carries.
type StreamStatus struct {
	Hub *realtime.Hub
}

// streamEvents: GET /v1/geo/stream — Server-Sent Events fan-out of
// validated vessel positions (geo.vessel-position.v1) and fence transitions
// (geo.geofence-event.v1), clearance-filtered exactly like the REST reads.
// Notifications only: the REST read model remains the source of truth.
func (server *Server) streamEvents(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	if server.Stream == nil {
		writeError(writer, http.StatusServiceUnavailable,
			"REALTIME_UNCONFIGURED: SSE fan-out is disabled (GEO_SSE_ENABLED is not true); poll /v1/geo/vessels instead")
		return
	}
	server.Stream.Hub.ServeSSE(writer, request, clearedLabels(principal.Clearance))
}

// streamStatus: GET /v1/geo/stream/status — honest stream posture.
func (server *Server) streamStatus(writer http.ResponseWriter, request *http.Request) {
	if _, ok := principalOrFail(writer, request); !ok {
		return
	}
	enabled := server.Stream != nil && server.Stream.Hub != nil
	response := map[string]any{
		"enabled": enabled,
		"eventTypes": []string{
			"geo.vessel-position.v1",
			"geo.geofence-event.v1",
		},
		"contract": "notifications only; the REST read model is the source of truth — re-poll to resync after any gap",
	}
	if enabled {
		response["subscribers"] = server.Stream.Hub.SubscriberCount()
	}
	writeJSON(writer, http.StatusOK, response)
}

// registerStreamRoutes wires the SSE surface. The routes are ALWAYS
// registered; when the hub is unwired they answer an honest 503
// (REALTIME_UNCONFIGURED) rather than vanishing, so capability discovery is
// stable across deployments.
func (server *Server) registerStreamRoutes(mux *http.ServeMux) {
	read := func(pattern string, handler http.HandlerFunc) {
		mux.Handle(pattern, auth.RequireRoles(http.HandlerFunc(handler), "geo-reader", "geo-zone-maker", "geo-zone-checker", "geo-admin"))
	}
	read("GET /v1/geo/stream", server.streamEvents)
	read("GET /v1/geo/stream/status", server.streamStatus)
}
