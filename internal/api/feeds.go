// GTFS static + GTFS-Realtime feed endpoints (Citizen Services Advisory
// §5) and the transit-alerts admin path. Feeds are tenant-scoped products:
// the caller's tenant binding selects the registry, the caller's clearance
// governs which shared-plane positions may be embedded. Everything is
// built fresh per request (consumers poll on 15–30s cycles) and carries a
// strong ETag so unchanged payloads answer 304.
package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/munisp/blueeconomy-geo-service/internal/gtfs"
	"github.com/munisp/blueeconomy-geo-service/internal/gtfsrt"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// StaticFeedBuilder renders the tenant's GTFS static archive.
type StaticFeedBuilder interface {
	BuildZip(request *http.Request, tenantID string) (payload []byte, etag string, err error)
}

// staticFeedBuilder adapts the store + gtfs factory to StaticFeedBuilder.
type staticFeedBuilder struct {
	storage *store.Store
}

func (builder staticFeedBuilder) BuildZip(request *http.Request, tenantID string) ([]byte, string, error) {
	registry, err := builder.storage.LoadTransitRegistry(request.Context(), tenantID)
	if err != nil {
		return nil, "", err
	}
	return gtfs.BuildStaticZip(registry)
}

// AttachFeeds wires the feed endpoints fail-closed: the GTFS-RT builder
// is required; the static builder defaults to the store-backed factory.
func (server *Server) AttachFeeds(builder *gtfsrt.Builder) error {
	if builder == nil {
		return errors.New("gtfs-rt builder is required when feeds are attached")
	}
	server.RealtimeFeeds = builder
	server.StaticFeeds = staticFeedBuilder{storage: server.Store}
	return nil
}

// serveGTFSZip: GET /feeds/gtfs.zip — deterministic archive + ETag.
func (server *Server) serveGTFSZip(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenantID, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	if server.StaticFeeds == nil {
		writeError(writer, http.StatusServiceUnavailable, "static feed factory not wired")
		return
	}
	payload, etag, err := server.StaticFeeds.BuildZip(request, tenantID)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "gtfs static feed build failed")
		return
	}
	server.Metrics.Inc("geo_feed_build_total", map[string]string{"feed": "gtfs"})
	servePayload(writer, request, payload, etag, "application/zip", "gtfs.zip")
}

// serveRealtime: GET /feeds/gtfs-rt/{vehiclepositions,tripupdates,alerts}.pb.
// The kind selects the feed; tenant + clearance are resolved per request
// (the registry is tenant-scoped, embedded positions clearance-governed).
func (server *Server) serveRealtime(kind string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		principal, ok := principalOrFail(writer, request)
		if !ok {
			return
		}
		tenantID, ok := tenantOrFail(writer, request, principal)
		if !ok {
			return
		}
		if server.RealtimeFeeds == nil {
			writeError(writer, http.StatusServiceUnavailable, "gtfs-rt producer not wired")
			return
		}
		var payload []byte
		var err error
		switch kind {
		case "vehiclepositions":
			payload, err = server.RealtimeFeeds.BuildVehiclePositions(request.Context(), tenantID, clearedLabels(principal.Clearance))
		case "tripupdates":
			payload, err = server.RealtimeFeeds.BuildTripUpdates(request.Context(), tenantID, clearedLabels(principal.Clearance))
		case "alerts":
			payload, err = server.RealtimeFeeds.BuildAlerts(request.Context(), tenantID)
		default:
			writeError(writer, http.StatusNotFound, "unknown gtfs-rt feed")
			return
		}
		if err != nil {
			writeError(writer, http.StatusInternalServerError, "gtfs-rt feed build failed")
			return
		}
		sum := sha256.Sum256(payload)
		etag := `"` + hex.EncodeToString(sum[:]) + `"`
		servePayload(writer, request, payload, etag, "application/x-protobuf", "")
	}
}

// servePayload writes one feed payload with ETag/304 handling.
func servePayload(writer http.ResponseWriter, request *http.Request, payload []byte,
	etag, contentType, downloadName string) {
	writer.Header().Set("ETag", etag)
	writer.Header().Set("Cache-Control", "no-cache")
	if downloadName != "" {
		writer.Header().Set("Content-Disposition", `attachment; filename="`+downloadName+`"`)
	}
	if match := strings.TrimSpace(request.Header.Get("If-None-Match")); match != "" && match == etag {
		writer.WriteHeader(http.StatusNotModified)
		return
	}
	writer.Header().Set("Content-Type", contentType)
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(payload)
}

// createAlertRequest is the admin alert payload.
type createAlertRequest struct {
	AlertID         string `json:"alert_id"`
	Cause           string `json:"cause"`
	Effect          string `json:"effect"`
	RouteID         string `json:"route_id"`
	StopID          string `json:"stop_id"`
	StartsAt        string `json:"starts_at"`
	EndsAt          string `json:"ends_at"`
	HeaderText      string `json:"header_text"`
	DescriptionText string `json:"description_text"`
	URL             string `json:"url"`
}

// createTransitAlert: POST /v1/geo/transit/alerts (auth required;
// geo-transit-admin or geo-admin). Maker/checker is deliberately NOT
// required for alerts (operational urgency; see advisory §5.1).
func (server *Server) createTransitAlert(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenantID, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	var body createAlertRequest
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 64*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(writer, http.StatusBadRequest, "alert payload must be valid JSON")
		return
	}
	if body.RouteID == "" && body.StopID == "" {
		writeError(writer, http.StatusBadRequest, "alert must be scoped to a route or a stop")
		return
	}
	if strings.TrimSpace(body.HeaderText) == "" {
		writeError(writer, http.StatusBadRequest, "header_text is required")
		return
	}
	startsAt, err := parseOptionalRFC3339(body.StartsAt)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "starts_at must be RFC 3339")
		return
	}
	endsAt, err := parseOptionalRFC3339(body.EndsAt)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "ends_at must be RFC 3339")
		return
	}
	alertID := strings.TrimSpace(body.AlertID)
	if alertID == "" {
		alertID = "alert-" + uuid.NewString()
	}
	alert := store.TransitAlert{
		AlertID:         alertID,
		Cause:           strings.ToUpper(strings.TrimSpace(body.Cause)),
		Effect:          strings.ToUpper(strings.TrimSpace(body.Effect)),
		RouteID:         strings.TrimSpace(body.RouteID),
		StopID:          strings.TrimSpace(body.StopID),
		StartsAt:        startsAt,
		EndsAt:          endsAt,
		HeaderText:      strings.TrimSpace(body.HeaderText),
		DescriptionText: strings.TrimSpace(body.DescriptionText),
		URL:             strings.TrimSpace(body.URL),
		Active:          true,
		CreatedBy:       principal.Subject,
	}
	if alert.Cause == "" {
		alert.Cause = "UNKNOWN_CAUSE"
	}
	if alert.Effect == "" {
		alert.Effect = "UNKNOWN_EFFECT"
	}
	if err := server.Store.CreateTransitAlert(request.Context(), tenantID, alert); err != nil {
		if strings.Contains(err.Error(), "check") || strings.Contains(err.Error(), "violates") {
			writeError(writer, http.StatusBadRequest, "alert rejected: invalid cause/effect, window or scope")
			return
		}
		if strings.Contains(err.Error(), "duplicate key") {
			writeError(writer, http.StatusConflict, "alert_id already exists")
			return
		}
		writeError(writer, http.StatusInternalServerError, "alert create failed")
		return
	}
	server.Metrics.Inc("geo_transit_alerts_created_total", nil)
	writeJSON(writer, http.StatusCreated, map[string]any{"alertId": alertID})
}

func parseOptionalRFC3339(raw string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, err
	}
	utc := parsed.UTC()
	return &utc, nil
}
