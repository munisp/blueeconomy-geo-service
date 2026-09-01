// Package api is the /v1/geo REST boundary: latest positions (bbox),
// vessel-360, track replay (GeoJSON LineString), vessels-in-zone, zone
// administration (maker-checker), SOS read-back and the SOS lifecycle
// (acknowledge/resolve). Every read is clearance-floor enforced (reader
// clearance >= row classification, geo ladder PUBLIC..SECRET) and
// tenant-scoped reads bind app.tenant_id for RLS.
// Roles: geo-reader (reads), geo-zone-maker / geo-zone-checker (zone admin),
// geo-sos-reader (SOS reads and lifecycle, RESTRICTED+ clearance required).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/gtfsrt"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// SignedEnvelopePublisher is the lifecycle event boundary the SOS
// acknowledge/resolve handlers publish through (connectors.Pipeline in
// production; a recording stub in tests). Envelopes are the canonical
// envelopeVersion 1.0 signed contract on vessels.events.
type SignedEnvelopePublisher interface {
	PublishSignedEnvelope(ctx context.Context, eventType, correlationID string, payload any, occurredAt time.Time, classification string, headers map[string]string) error
}

// Server wires the REST handlers.
type Server struct {
	Store   *store.Store
	Metrics *metrics.Registry
	// SOSEvents publishes the signed sos.acknowledged / sos.resolved
	// lifecycle envelopes. The lifecycle endpoints fail closed (503) when
	// it is not wired — a lifecycle transition that cannot be announced
	// must never silently persist.
	SOSEvents SignedEnvelopePublisher
	// RealtimeFeeds builds the GTFS-RT feeds; StaticFeeds the GTFS static
	// archive. Both are wired by AttachFeeds (the feed endpoints fail
	// closed with 503 when unwired).
	RealtimeFeeds *gtfsrt.Builder
	StaticFeeds   StaticFeedBuilder
	// GeoV2 wires the WP-10 surface (versioned geofences, track APIs,
	// congestion forecast). When nil the v2 routes are not registered.
	GeoV2 *GeoV2
	// Safety wires the Phase-12 safety-compliance surface (FSC/PSC
	// inspections, SAR coordination, marine accident investigation). When
	// nil the safety routes are not registered.
	Safety *Safety
}

// NewServer validates the wiring fail-closed.
func NewServer(storage *store.Store, registry *metrics.Registry) (*Server, error) {
	if storage == nil {
		return nil, errors.New("api store is required")
	}
	if registry == nil {
		return nil, errors.New("api metrics registry is required")
	}
	return &Server{Store: storage, Metrics: registry}, nil
}

// Handler builds the authenticated route tree.
func (server *Server) Handler(authenticator auth.Authenticator, appReportRoutes func(mux *http.ServeMux)) http.Handler {
	mux := http.NewServeMux()
	read := func(pattern string, handler http.HandlerFunc) {
		mux.Handle(pattern, auth.RequireRoles(http.HandlerFunc(handler), "geo-reader", "geo-zone-maker", "geo-zone-checker", "geo-admin"))
	}
	read("GET /v1/geo/vessels", server.listVessels)
	read("GET /v1/geo/vessels/{mmsi}", server.vessel360)
	read("GET /v1/geo/vessels/{mmsi}/track", server.track)
	read("GET /v1/geo/zones", server.listZones)
	read("GET /v1/geo/zones/{id}/vessels", server.vesselsInZone)
	mux.Handle("POST /v1/geo/zones",
		auth.RequireRoles(http.HandlerFunc(server.createZone), "geo-zone-maker", "geo-admin"))
	mux.Handle("POST /v1/geo/zones/{id}/approve",
		auth.RequireRoles(http.HandlerFunc(server.approveZone), "geo-zone-checker", "geo-admin"))
	mux.Handle("GET /v1/geo/sos",
		auth.RequireRoles(http.HandlerFunc(server.listSOS), "geo-sos-reader", "geo-admin"))
	mux.Handle("POST /v1/geo/sos/{id}/acknowledge",
		auth.RequireRoles(http.HandlerFunc(server.acknowledgeSOS), "geo-sos-reader", "geo-admin"))
	mux.Handle("POST /v1/geo/sos/{id}/resolve",
		auth.RequireRoles(http.HandlerFunc(server.resolveSOS), "geo-sos-reader", "geo-admin"))
	// GTFS static + GTFS-RT feeds (advisory §5) and the transit-alerts
	// admin path. Feed reads sit on the standard read role set.
	read("GET /feeds/gtfs.zip", server.serveGTFSZip)
	read("GET /feeds/gtfs-rt/vehiclepositions.pb", server.serveRealtime("vehiclepositions"))
	read("GET /feeds/gtfs-rt/tripupdates.pb", server.serveRealtime("tripupdates"))
	read("GET /feeds/gtfs-rt/alerts.pb", server.serveRealtime("alerts"))
	mux.Handle("POST /v1/geo/transit/alerts",
		auth.RequireRoles(http.HandlerFunc(server.createTransitAlert), "geo-transit-admin", "geo-admin"))
	if appReportRoutes != nil {
		appReportRoutes(mux)
	}
	if server.GeoV2 != nil {
		server.registerGeoV2Routes(mux)
	}
	if server.Safety != nil {
		server.registerSafetyRoutes(mux)
	}
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /metrics", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
		server.Metrics.WritePrometheus(writer)
	})
	return auth.Middleware(authenticator, mux)
}

// clearedLabels renders every ladder label the principal's clearance covers
// (reader clearance >= row classification).
func clearedLabels(clearance string) []string {
	label, err := sign.ParseClassification(clearance)
	if err != nil {
		return nil
	}
	all := []string{"PUBLIC", "INTERNAL", "RESTRICTED", "CONFIDENTIAL", "SECRET"}
	out := make([]string, 0, len(all))
	for _, candidate := range all {
		if label.Covers(sign.MustClassification(sign.Classification(candidate))) {
			out = append(out, candidate)
		}
	}
	return out
}

// principalOrFail resolves the authenticated principal or writes 403.
func principalOrFail(writer http.ResponseWriter, request *http.Request) (auth.Principal, bool) {
	principal, ok := auth.PrincipalFrom(request.Context())
	if !ok {
		writeError(writer, http.StatusForbidden, "principal unavailable")
		return auth.Principal{}, false
	}
	return principal, true
}

// tenantOrFail resolves the tenant binding for RLS-scoped reads.
func tenantOrFail(writer http.ResponseWriter, request *http.Request, principal auth.Principal) (string, bool) {
	if strings.TrimSpace(principal.TenantID) == "" {
		writeError(writer, http.StatusForbidden, "principal has no tenant binding")
		return "", false
	}
	return principal.TenantID, true
}

// parseMicrosParam parses a fixed-point micro-degree query parameter.
func parseMicrosParam(request *http.Request, name string) (int32, error) {
	raw := strings.TrimSpace(request.URL.Query().Get(name))
	if raw == "" {
		return 0, fmt.Errorf("query parameter %s is required", name)
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("query parameter %s must be fixed-point micro-degrees", name)
	}
	return int32(value), nil
}

func parseLimit(request *http.Request, fallback, maximum int) int {
	raw := strings.TrimSpace(request.URL.Query().Get("limit"))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

// listVessels: GET /v1/geo/vessels?bbox=minLon,minLat,maxLon,maxLat
// (micro-degrees) with classification-floor filtering.
func (server *Server) listVessels(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var minLon, minLat, maxLon, maxLat int32
	bbox := strings.TrimSpace(request.URL.Query().Get("bbox"))
	if bbox != "" {
		parts := strings.Split(bbox, ",")
		if len(parts) != 4 {
			writeError(writer, http.StatusBadRequest, "bbox must be minLon,minLat,maxLon,maxLat in micro-degrees")
			return
		}
		values := make([]int32, 4)
		for i, part := range parts {
			value, err := strconv.ParseInt(strings.TrimSpace(part), 10, 32)
			if err != nil {
				writeError(writer, http.StatusBadRequest, "bbox values must be fixed-point micro-degrees")
				return
			}
			values[i] = int32(value)
		}
		minLon, minLat, maxLon, maxLat = values[0], values[1], values[2], values[3]
	} else {
		// Whole-world default.
		minLon, minLat, maxLon, maxLat = -180_000_000, -90_000_000, 180_000_000, 90_000_000
	}
	if minLon >= maxLon || minLat >= maxLat {
		writeError(writer, http.StatusBadRequest, "bbox min must be below max")
		return
	}
	vessels, err := server.Store.ListVessels(request.Context(), minLon, minLat, maxLon, maxLat,
		clearedLabels(principal.Clearance), parseLimit(request, 500, 5000))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "vessel query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"vessels": vessels})
}

// vessel360: GET /v1/geo/vessels/{mmsi}.
func (server *Server) vessel360(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	mmsi := request.PathValue("mmsi")
	if !mmsiPattern.MatchString(mmsi) {
		writeError(writer, http.StatusBadRequest, "mmsi must be 9 digits")
		return
	}
	view, err := server.Store.GetVessel360(request.Context(), mmsi, principal.TenantID, clearedLabels(principal.Clearance))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "vessel-360 query failed")
		return
	}
	if view == nil {
		writeError(writer, http.StatusNotFound, "vessel not found at caller clearance")
		return
	}
	writeJSON(writer, http.StatusOK, view)
}

// track: GET /v1/geo/vessels/{mmsi}/track?from&to (RFC 3339) → GeoJSON.
func (server *Server) track(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	mmsi := request.PathValue("mmsi")
	if !mmsiPattern.MatchString(mmsi) {
		writeError(writer, http.StatusBadRequest, "mmsi must be 9 digits")
		return
	}
	from, err := time.Parse(time.RFC3339, strings.TrimSpace(request.URL.Query().Get("from")))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "from must be RFC 3339")
		return
	}
	to, err := time.Parse(time.RFC3339, strings.TrimSpace(request.URL.Query().Get("to")))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "to must be RFC 3339")
		return
	}
	geoJSON, points, err := server.Store.GetTrack(request.Context(), mmsi, from, to, clearedLabels(principal.Clearance))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "track query failed")
		return
	}
	writer.Header().Set("Content-Type", "application/geo+json")
	writer.WriteHeader(http.StatusOK)
	if points == 0 {
		_, _ = writer.Write([]byte(`{"type":"LineString","coordinates":[]}`))
		return
	}
	_, _ = writer.Write([]byte(geoJSON))
}

// listZones: GET /v1/geo/zones (tenant-scoped, clearance-filtered).
func (server *Server) listZones(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenant, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	zones, err := server.Store.ListZones(request.Context(), tenant, clearedLabels(principal.Clearance))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "zone query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"zones": zones})
}

// createZoneRequest is the maker payload.
type createZoneRequest struct {
	ZoneID              string          `json:"zoneId"`
	Name                string          `json:"name"`
	ClassificationFloor string          `json:"classificationFloor"`
	Geometry            json.RawMessage `json:"geometry"` // GeoJSON Polygon
}

// createZone: POST /v1/geo/zones (maker; zone persists as draft).
func (server *Server) createZone(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenant, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	var payload createZoneRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	if len(payload.Geometry) == 0 || !json.Valid(payload.Geometry) {
		writeError(writer, http.StatusBadRequest, "geometry must be a GeoJSON Polygon")
		return
	}
	var geometry struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload.Geometry, &geometry); err != nil || geometry.Type != "Polygon" {
		writeError(writer, http.StatusBadRequest, "geometry must be a GeoJSON Polygon")
		return
	}
	zone := store.ZoneRow{
		ZoneID:              payload.ZoneID,
		Name:                payload.Name,
		ClassificationFloor: payload.ClassificationFloor,
		MakerPrincipalID:    principal.Subject,
	}
	if err := server.Store.CreateZone(request.Context(), tenant, zone, string(payload.Geometry)); err != nil {
		writeError(writer, http.StatusBadRequest, "zone creation failed: "+err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"zoneId": payload.ZoneID, "state": "draft"})
}

// approveZone: POST /v1/geo/zones/{id}/approve (checker; maker != checker).
func (server *Server) approveZone(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenant, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	zoneID := request.PathValue("id")
	if strings.TrimSpace(zoneID) == "" {
		writeError(writer, http.StatusBadRequest, "zone id is required")
		return
	}
	err := server.Store.ApproveZone(request.Context(), tenant, zoneID, principal.Subject)
	switch {
	case errors.Is(err, store.ErrMakerCheckerConflict):
		writeError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrZoneNotDraft):
		writeError(writer, http.StatusConflict, err.Error())
	case err != nil:
		writeError(writer, http.StatusInternalServerError, "zone approval failed")
	default:
		writeJSON(writer, http.StatusOK, map[string]any{"zoneId": zoneID, "state": "approved"})
	}
}

// vesselsInZone: GET /v1/geo/zones/{id}/vessels.
func (server *Server) vesselsInZone(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenant, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	vessels, err := server.Store.VesselsInZone(request.Context(), tenant, request.PathValue("id"),
		clearedLabels(principal.Clearance), parseLimit(request, 500, 5000))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "vessels-in-zone query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"vessels": vessels})
}

// listSOS: GET /v1/geo/sos — requires RESTRICTED-or-higher clearance in
// addition to the geo-sos-reader role.
func (server *Server) listSOS(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	clearance, err := sign.ParseClassification(principal.Clearance)
	if err != nil || !clearance.Covers(sign.ClassificationRestricted) {
		writeError(writer, http.StatusForbidden, "sos readback requires RESTRICTED or higher clearance")
		return
	}
	alerts, err := server.Store.ListSOS(request.Context(), clearedLabels(principal.Clearance), parseLimit(request, 100, 1000))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "sos query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"sosAlerts": alerts})
}

// sosLifecycleRequest is the optional acknowledge/resolve note.
type sosLifecycleRequest struct {
	Note string `json:"note"`
}

// sosIDPattern mirrors the sos_alert_id contract shape.
var sosIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// acknowledgeSOS: POST /v1/geo/sos/{id}/acknowledge (RAISED -> ACKNOWLEDGED).
func (server *Server) acknowledgeSOS(writer http.ResponseWriter, request *http.Request) {
	server.transitionSOS(writer, request, "acknowledge")
}

// resolveSOS: POST /v1/geo/sos/{id}/resolve (RAISED|ACKNOWLEDGED -> RESOLVED).
func (server *Server) resolveSOS(writer http.ResponseWriter, request *http.Request) {
	server.transitionSOS(writer, request, "resolve")
}

// transitionSOS applies one lifecycle step with the same gate as the SOS
// read path (geo-sos-reader/geo-admin role + RESTRICTED clearance floor),
// persists the actor/timestamp/note ledger entry and publishes the signed
// lifecycle envelope. Illegal transitions are 409; unknown alerts 404.
func (server *Server) transitionSOS(writer http.ResponseWriter, request *http.Request, action string) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	clearance, err := sign.ParseClassification(principal.Clearance)
	if err != nil || !clearance.Covers(sign.ClassificationRestricted) {
		writeError(writer, http.StatusForbidden, "sos lifecycle requires RESTRICTED or higher clearance")
		return
	}
	if server.SOSEvents == nil {
		writeError(writer, http.StatusServiceUnavailable, "sos lifecycle event publisher is not wired")
		return
	}
	sosAlertID := request.PathValue("id")
	if !sosIDPattern.MatchString(sosAlertID) {
		writeError(writer, http.StatusBadRequest, "sos alert id is invalid")
		return
	}
	var payload sosLifecycleRequest
	if request.Body != nil {
		err := json.NewDecoder(request.Body).Decode(&payload)
		if err != nil && !errors.Is(err, io.EOF) {
			writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
			return
		}
	}
	alert, err := server.Store.TransitionSOSAlert(request.Context(), sosAlertID, principal.Subject, action, payload.Note)
	switch {
	case errors.Is(err, store.ErrSOSNotFound):
		writeError(writer, http.StatusNotFound, "sos alert not found")
		return
	case errors.Is(err, store.ErrSOSInvalidTransition):
		writeError(writer, http.StatusConflict, err.Error())
		return
	case err != nil:
		writeError(writer, http.StatusBadRequest, "sos lifecycle transition failed: "+err.Error())
		return
	}
	// The row is durable; announce the transition on the canonical signed
	// envelope (SAFETY priority, RESTRICTED floor inherited from the alert).
	occurredAt := time.Now().UTC()
	if action == "acknowledge" {
		if alert.AcknowledgedAt != nil {
			occurredAt = *alert.AcknowledgedAt
		}
		err = server.SOSEvents.PublishSignedEnvelope(request.Context(), sign.EventSOSAcknowledged,
			alert.SosAlertID, sign.SosAlertAcknowledged{
				SosAlertID:      alert.SosAlertID,
				ReporterID:      alert.ReporterID,
				VesselReference: alert.VesselReference,
				AcknowledgedBy:  principal.Subject,
				AcknowledgedAt:  occurredAt,
				Note:            payload.Note,
				Classification:  alert.Classification,
			}, occurredAt, alert.Classification, map[string]string{"priority": "SAFETY"})
	} else {
		if alert.ResolvedAt != nil {
			occurredAt = *alert.ResolvedAt
		}
		err = server.SOSEvents.PublishSignedEnvelope(request.Context(), sign.EventSOSResolved,
			alert.SosAlertID, sign.SosAlertResolved{
				SosAlertID:      alert.SosAlertID,
				ReporterID:      alert.ReporterID,
				VesselReference: alert.VesselReference,
				ResolvedBy:      principal.Subject,
				ResolvedAt:      occurredAt,
				Note:            payload.Note,
				Classification:  alert.Classification,
			}, occurredAt, alert.Classification, map[string]string{"priority": "SAFETY"})
	}
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "sos lifecycle event publication failed")
		return
	}
	server.Metrics.Inc("geo_sos_lifecycle_transitions_total", map[string]string{"action": action})
	writeJSON(writer, http.StatusOK, map[string]any{"sosAlert": alert})
}

func writeJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]any{"error": message})
}

// mmsiPattern enforces the contract MMSI shape on path parameters.
var mmsiPattern = regexp.MustCompile(`^[0-9]{9}$`)
