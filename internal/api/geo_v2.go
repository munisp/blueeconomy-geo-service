package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/congestion"
	"github.com/munisp/blueeconomy-geo-service/internal/fence"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
	"github.com/munisp/blueeconomy-geo-service/internal/track"
)

// GeoV2Store is the persistence boundary for the WP-10 endpoints. It is an
// interface so the handlers are unit-testable without Postgres; *store.Store
// implements it in production.
type GeoV2Store interface {
	ListActiveGeofences(ctx context.Context, tenantID string, clearedLabels []string) ([]store.FenceRow, error)
	GetGeofenceHistory(ctx context.Context, tenantID, geofenceID string) ([]store.FenceRow, error)
	CreateGeofenceVersion(ctx context.Context, row store.FenceRow, expectedVersion int) (store.FenceRow, error)
	RetireGeofence(ctx context.Context, tenantID, geofenceID string) error
	InsertGeofenceEvent(ctx context.Context, ev store.FenceEventRow) error
	ListGeofenceEvents(ctx context.Context, tenantID, geofenceID string, clearedLabels []string, limit int) ([]store.FenceEventRow, error)
	QueryTrack(ctx context.Context, mmsi string, from, to time.Time, clearedLabels []string, limit int) ([]store.TrackPointRow, error)
	NearestVessels(ctx context.Context, latMicros, lonMicros int32, radiusMeters float64, clearedLabels []string, limit int) ([]store.NearestVesselRow, error)
	LatestPosition(ctx context.Context, mmsi string, clearedLabels []string) (store.TrackPointRow, error)
	QueueObservations(ctx context.Context, portCode string, since time.Time, limit int) ([]store.QueueObservationRow, error)
	InsertQueueObservation(ctx context.Context, row store.QueueObservationRow) error
}

// GeoV2 wires the WP-10 endpoints: versioned geofence CRUD, fence
// transition evaluation (geo.geofence-event.v1 signed envelopes),
// time-windowed track queries with gap reporting, nearest-vessels,
// port-approach ETAs and the port-congestion baseline forecast.
type GeoV2 struct {
	Store GeoV2Store
	// FenceEvents publishes geo.geofence-event.v1 envelopes. The evaluate
	// endpoint fails closed (503) when unwired — a fence transition that
	// cannot be announced must never silently persist.
	FenceEvents SignedEnvelopePublisher
	engine      *fence.Engine
	now         func() time.Time
}

// NewGeoV2 wires the WP-10 surface fail-closed.
func NewGeoV2(storage GeoV2Store) (*GeoV2, error) {
	if storage == nil {
		return nil, errors.New("geo v2 store is required")
	}
	return &GeoV2{Store: storage, engine: fence.NewEngine(), now: time.Now}, nil
}

// provenance is stamped on every WP-10 read response.
type provenance struct {
	Source    string `json:"source"`
	AsOf      string `json:"asOf"`
	Stale     bool   `json:"stale"`
	StaleNote string `json:"staleNote,omitempty"`
}

func (g *GeoV2) prov(source string, dataAsOf time.Time, staleAfter time.Duration) provenance {
	p := provenance{Source: source, AsOf: dataAsOf.UTC().Format(time.RFC3339)}
	if !dataAsOf.IsZero() && g.now().Sub(dataAsOf) > staleAfter {
		p.Stale = true
		p.StaleNote = fmt.Sprintf("newest recorded data is older than %s", staleAfter)
	}
	return p
}

// RegisterRoutes adds the WP-10 routes to the mux (called from Handler when
// the server carries a GeoV2).
func (server *Server) registerGeoV2Routes(mux *http.ServeMux) {
	g := server.GeoV2
	read := func(pattern string, handler http.HandlerFunc) {
		mux.Handle(pattern, auth.RequireRoles(http.HandlerFunc(handler), "geo-reader", "geo-zone-maker", "geo-zone-checker", "geo-admin"))
	}
	read("GET /v1/geo/fences", g.listFences)
	read("GET /v1/geo/fences/{id}", g.getFenceHistory)
	read("GET /v1/geo/fences/{id}/events", g.listFenceEvents)
	mux.Handle("POST /v1/geo/fences",
		auth.RequireRoles(http.HandlerFunc(g.createFence), "geo-zone-maker", "geo-admin"))
	mux.Handle("POST /v1/geo/fences/{id}/versions",
		auth.RequireRoles(http.HandlerFunc(g.createFenceVersion), "geo-zone-maker", "geo-admin"))
	mux.Handle("POST /v1/geo/fences/{id}/retire",
		auth.RequireRoles(http.HandlerFunc(g.retireFence), "geo-zone-checker", "geo-admin"))
	mux.Handle("POST /v1/geo/fences/evaluate",
		auth.RequireRoles(http.HandlerFunc(g.evaluatePositions), "geo-ingest", "geo-admin"))
	read("GET /v1/geo/tracks/{mmsi}", g.queryTrack)
	read("GET /v1/geo/vessels/nearest", g.nearestVessels)
	read("GET /v1/geo/ports/{code}/approaches", g.portApproaches)
	read("GET /v1/geo/ports/{code}/congestion/forecast", g.congestionForecast)
	mux.Handle("POST /v1/geo/ports/{code}/queue-observations",
		auth.RequireRoles(http.HandlerFunc(g.recordQueueObservation), "geo-ingest", "geo-admin"))
}

// ─── Geofence CRUD (versioned) ──────────────────────────────────────────────

type fenceView struct {
	GeofenceID               string          `json:"geofenceId"`
	Version                  int             `json:"version"`
	Name                     string          `json:"name"`
	Classification           string          `json:"classification"`
	VerticesMicros           json.RawMessage `json:"verticesMicros"`
	DwellThresholdSeconds    int             `json:"dwellThresholdSeconds"`
	DwellSpeedGateMilliknots int             `json:"dwellSpeedGateMilliknots"`
	State                    string          `json:"state"`
	CreatedBy                string          `json:"createdBy"`
	CreatedAt                string          `json:"createdAt"`
	RetiredAt                *string         `json:"retiredAt,omitempty"`
}

func viewOf(row store.FenceRow) fenceView {
	v := fenceView{
		GeofenceID: row.GeofenceID, Version: row.Version, Name: row.Name,
		Classification: row.Classification, VerticesMicros: row.VerticesMicros,
		DwellThresholdSeconds: row.DwellThresholdSeconds, DwellSpeedGateMilliknots: row.DwellSpeedGateMilliknots,
		State: row.State, CreatedBy: row.CreatedBy, CreatedAt: row.CreatedAt.UTC().Format(time.RFC3339),
	}
	if row.RetiredAt != nil {
		s := row.RetiredAt.UTC().Format(time.RFC3339)
		v.RetiredAt = &s
	}
	return v
}

func (g *GeoV2) listFences(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenantID, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	rows, err := g.Store.ListActiveGeofences(request.Context(), tenantID, clearedLabels(principal.Clearance))
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "FENCE_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	fences := make([]fenceView, len(rows))
	for i, r := range rows {
		fences[i] = viewOf(r)
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"fences": fences, "count": len(fences),
		"provenance": g.prov("geofences", g.now(), 0),
	})
}

func (g *GeoV2) getFenceHistory(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenantID, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	rows, err := g.Store.GetGeofenceHistory(request.Context(), tenantID, request.PathValue("id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, "geofence not found")
		return
	}
	versions := make([]fenceView, len(rows))
	for i, r := range rows {
		versions[i] = viewOf(r)
	}
	writeJSON(writer, http.StatusOK, map[string]any{"versions": versions, "provenance": g.prov("geofences", g.now(), 0)})
}

var geofenceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$`)

type fenceUpsertRequest struct {
	GeofenceID               string          `json:"geofenceId"`
	Name                     string          `json:"name"`
	Classification           string          `json:"classification"`
	VerticesMicros           json.RawMessage `json:"verticesMicros"`
	DwellThresholdSeconds    int             `json:"dwellThresholdSeconds"`
	DwellSpeedGateMilliknots int             `json:"dwellSpeedGateMilliknots"`
	ExpectedVersion          int             `json:"expectedVersion"`
}

func (g *GeoV2) decodeFenceRequest(writer http.ResponseWriter, request *http.Request) (fenceUpsertRequest, []fence.Point, bool) {
	var req fenceUpsertRequest
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20)).Decode(&req); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid fence body: "+err.Error())
		return req, nil, false
	}
	if !geofenceIDPattern.MatchString(req.GeofenceID) {
		writeError(writer, http.StatusBadRequest, "geofenceId must match ^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
		return req, nil, false
	}
	if strings.TrimSpace(req.Name) == "" {
		writeError(writer, http.StatusBadRequest, "name is required")
		return req, nil, false
	}
	if _, err := sign.ParseClassification(req.Classification); err != nil {
		writeError(writer, http.StatusBadRequest, "classification must be one of PUBLIC..SECRET")
		return req, nil, false
	}
	var raw [][2]int32
	if err := json.Unmarshal(req.VerticesMicros, &raw); err != nil {
		writeError(writer, http.StatusBadRequest, "verticesMicros must be [[latMicros,lonMicros],...]")
		return req, nil, false
	}
	vertices := make([]fence.Point, len(raw))
	for i, v := range raw {
		vertices[i] = fence.Point{LatMicros: v[0], LonMicros: v[1]}
	}
	if err := fence.ValidateGeometry(vertices); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid geometry: "+err.Error())
		return req, nil, false
	}
	return req, vertices, true
}

func (g *GeoV2) createFence(writer http.ResponseWriter, request *http.Request) {
	req, _, ok := g.decodeFenceRequest(writer, request)
	if !ok {
		return
	}
	g.upsertFence(writer, request, req, 0)
}

func (g *GeoV2) createFenceVersion(writer http.ResponseWriter, request *http.Request) {
	req, _, ok := g.decodeFenceRequest(writer, request)
	if !ok {
		return
	}
	pathID := request.PathValue("id")
	if pathID != req.GeofenceID {
		writeError(writer, http.StatusBadRequest, "path geofence id must match body geofenceId")
		return
	}
	if req.ExpectedVersion < 1 {
		writeError(writer, http.StatusBadRequest, "expectedVersion (the current latest version) is required for a new version")
		return
	}
	g.upsertFence(writer, request, req, req.ExpectedVersion)
}

func (g *GeoV2) upsertFence(writer http.ResponseWriter, request *http.Request, req fenceUpsertRequest, expectedVersion int) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenantID, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	created, err := g.Store.CreateGeofenceVersion(request.Context(), store.FenceRow{
		GeofenceID: req.GeofenceID, TenantID: tenantID, Name: req.Name, Classification: req.Classification,
		VerticesMicros: req.VerticesMicros, DwellThresholdSeconds: req.DwellThresholdSeconds,
		DwellSpeedGateMilliknots: req.DwellSpeedGateMilliknots, CreatedBy: principal.Subject,
	}, expectedVersion)
	if err != nil {
		if strings.HasPrefix(err.Error(), "VERSION_CONFLICT") {
			writeError(writer, http.StatusConflict, err.Error())
			return
		}
		writeError(writer, http.StatusServiceUnavailable, "FENCE_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"fence": viewOf(created)})
}

func (g *GeoV2) retireFence(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenantID, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	if err := g.Store.RetireGeofence(request.Context(), tenantID, request.PathValue("id")); err != nil {
		writeError(writer, http.StatusNotFound, "no ACTIVE version of this geofence")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"retired": request.PathValue("id")})
}

// ─── Fence evaluation → geo.geofence-event.v1 ───────────────────────────────

type positionReport struct {
	MMSI         string `json:"mmsi"`
	LatMicros    int32  `json:"latMicros"`
	LonMicros    int32  `json:"lonMicros"`
	SogMillikn   int    `json:"sogMilliknots"`
	ObservedAt   int64  `json:"observedAtUnix"`
}

type evaluateRequest struct {
	Reports []positionReport `json:"reports"`
}

// mmsiPattern is declared in server.go and shared across the API package.

// evaluatePositions folds a batch of validated position reports into the
// fence engine and publishes/persists the resulting transitions. Fail-closed:
// 503 when the envelope publisher is unwired; bad reports are rejected
// individually with per-report errors, never silently dropped.
func (g *GeoV2) evaluatePositions(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenantID, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	if g.FenceEvents == nil {
		writeError(writer, http.StatusServiceUnavailable,
			"FENCE_EVENTS_UNWIRED: geo.geofence-event.v1 publisher is not configured; transitions are never silently persisted")
		return
	}
	var req evaluateRequest
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 4<<20)).Decode(&req); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid evaluate body: "+err.Error())
		return
	}
	if len(req.Reports) == 0 || len(req.Reports) > 1000 {
		writeError(writer, http.StatusBadRequest, "reports must contain 1..1000 position reports")
		return
	}
	rows, err := g.Store.ListActiveGeofences(request.Context(), tenantID, clearedLabels(principal.Clearance))
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "FENCE_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	fences := make([]fence.Fence, 0, len(rows))
	versions := map[string]int{}
	for _, r := range rows {
		var raw [][2]int32
		if err := json.Unmarshal(r.VerticesMicros, &raw); err != nil {
			writeError(writer, http.StatusServiceUnavailable, "FENCE_STORE_CORRUPT: persisted ring unreadable for "+r.GeofenceID)
			return
		}
		vertices := make([]fence.Point, len(raw))
		for i, v := range raw {
			vertices[i] = fence.Point{LatMicros: v[0], LonMicros: v[1]}
		}
		fences = append(fences, fence.Fence{
			GeofenceID: r.GeofenceID, Version: r.Version, Vertices: vertices,
			DwellThresholdSeconds: r.DwellThresholdSeconds, DwellSpeedGateMilliknots: r.DwellSpeedGateMilliknots,
		})
		versions[r.GeofenceID] = r.Version
	}

	type rejected struct {
		Index  int    `json:"index"`
		Reason string `json:"reason"`
	}
	var emitted []map[string]any
	var rejects []rejected
	for i, rep := range req.Reports {
		if !mmsiPattern.MatchString(rep.MMSI) {
			rejects = append(rejects, rejected{i, "mmsi must be 9 digits"})
			continue
		}
		if rep.ObservedAt <= 0 {
			rejects = append(rejects, rejected{i, "observedAtUnix must be positive"})
			continue
		}
		events := g.engine.Observe(rep.MMSI, fence.Point{LatMicros: rep.LatMicros, LonMicros: rep.LonMicros}, rep.SogMillikn, rep.ObservedAt, fences)
		for _, ev := range events {
			eventID := uuid.NewString()
			occurredAt := time.Unix(ev.OccurredAtUnix, 0).UTC()
			payload := sign.GeofenceEventRecorded{
				GeofenceEventID: eventID,
				ZoneID:          ev.GeofenceID,
				ZoneName:        ev.GeofenceID,
				Event:           string(ev.Type),
				MMSI:            rep.MMSI,
				LatitudeMicros:  rep.LatMicros,
				LongitudeMicros: rep.LonMicros,
				OccurredAt:      occurredAt,
				Classification:  "INTERNAL",
			}
			canonical, _ := json.Marshal(payload)
			digest := sha256.Sum256(canonical)
			if err := g.FenceEvents.PublishSignedEnvelope(request.Context(), sign.EventGeofenceEvent,
				eventID, payload, occurredAt, "INTERNAL", map[string]string{"producer": "geo-fence-engine"}); err != nil {
				// Fail-closed: a transition that cannot be announced is not persisted.
				writeError(writer, http.StatusServiceUnavailable, "FENCE_EVENT_PUBLISH_FAILED: "+err.Error())
				return
			}
			if err := g.Store.InsertGeofenceEvent(request.Context(), store.FenceEventRow{
				EventID: eventID, GeofenceID: ev.GeofenceID, GeofenceVersion: ev.Version,
				TenantID: tenantID, EventType: string(ev.Type), MMSI: rep.MMSI,
				LatitudeMicros: rep.LatMicros, LongitudeMicros: rep.LonMicros,
				Classification: "INTERNAL", EnvelopeDigest: hex.EncodeToString(digest[:]), OccurredAt: occurredAt,
			}); err != nil {
				writeError(writer, http.StatusServiceUnavailable, "FENCE_EVENT_PERSIST_FAILED: "+err.Error())
				return
			}
			emitted = append(emitted, map[string]any{
				"eventId": eventID, "geofenceId": ev.GeofenceID, "geofenceVersion": ev.Version,
				"eventType": ev.Type, "mmsi": rep.MMSI, "occurredAt": occurredAt.Format(time.RFC3339),
				"envelopeDigest": hex.EncodeToString(digest[:]),
			})
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"evaluated": len(req.Reports), "emitted": emitted, "rejected": rejects,
		"activeFences": len(fences),
		"provenance":  g.prov("geo-fence-engine", g.now(), 0),
	})
}

func (g *GeoV2) listFenceEvents(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	tenantID, ok := tenantOrFail(writer, request, principal)
	if !ok {
		return
	}
	rows, err := g.Store.ListGeofenceEvents(request.Context(), tenantID, request.PathValue("id"), clearedLabels(principal.Clearance), parseLimit(request, 100, 1000))
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "FENCE_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"events": rows, "count": len(rows), "provenance": g.prov("geofence_events", g.now(), 0)})
}

// ─── Track APIs ─────────────────────────────────────────────────────────────

func parseUnixParam(request *http.Request, name string) (int64, error) {
	raw := strings.TrimSpace(request.URL.Query().Get(name))
	if raw == "" {
		return 0, fmt.Errorf("query parameter %s (unix seconds) is required", name)
	}
	return strconv.ParseInt(raw, 10, 64)
}

// queryTrack: GET /v1/geo/tracks/{mmsi}?from=<unix>&to=<unix>&maxGapSeconds=
// Returns the recorded track plus every gap above the threshold. Gaps are
// reported, never filled.
func (g *GeoV2) queryTrack(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	mmsi := request.PathValue("mmsi")
	if !mmsiPattern.MatchString(mmsi) {
		writeError(writer, http.StatusBadRequest, "mmsi must be 9 digits")
		return
	}
	fromUnix, err := parseUnixParam(request, "from")
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	toUnix, err := parseUnixParam(request, "to")
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if toUnix <= fromUnix {
		writeError(writer, http.StatusBadRequest, "to must be after from")
		return
	}
	maxGap := int64(1800)
	if raw := strings.TrimSpace(request.URL.Query().Get("maxGapSeconds")); raw != "" {
		maxGap, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || maxGap <= 0 {
			writeError(writer, http.StatusBadRequest, "maxGapSeconds must be a positive integer")
			return
		}
	}
	rows, err := g.Store.QueryTrack(request.Context(), mmsi, time.Unix(fromUnix, 0), time.Unix(toUnix, 0), clearedLabels(principal.Clearance), 10000)
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "TRACK_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	points := make([]track.Point, len(rows))
	for i, r := range rows {
		points[i] = track.Point{LatMicros: r.LatitudeMicros, LonMicros: r.LongitudeMicros, AtUnix: r.ObservedAt.Unix(), SogMillikn: r.SogMilliknots}
	}
	gaps := track.DetectGaps(points, maxGap)
	var asOf time.Time
	if len(rows) > 0 {
		asOf = rows[len(rows)-1].ObservedAt
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"mmsi": mmsi, "points": rows, "pointCount": len(rows),
		"gaps": gaps, "gapCount": len(gaps), "maxGapSeconds": maxGap,
		"message": func() string {
			if len(rows) == 0 {
				return "NO_TRACK_DATA: no recorded positions in the window (gaps are reported, never filled)"
			}
			return ""
		}(),
		"provenance": g.prov("ais_positions", asOf, 15*time.Minute),
	})
}

// nearestVessels: GET /v1/geo/vessels/nearest?lat=&lon=&radiusMeters=&limit=
func (g *GeoV2) nearestVessels(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	lat, err := parseMicrosParam(request, "lat")
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	lon, err := parseMicrosParam(request, "lon")
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	radius := 50_000.0
	if raw := strings.TrimSpace(request.URL.Query().Get("radiusMeters")); raw != "" {
		radius, err = strconv.ParseFloat(raw, 64)
		if err != nil || radius <= 0 || radius > 500_000 {
			writeError(writer, http.StatusBadRequest, "radiusMeters must be in (0, 500000]")
			return
		}
	}
	rows, err := g.Store.NearestVessels(request.Context(), lat, lon, radius, clearedLabels(principal.Clearance), parseLimit(request, 20, 200))
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "TRACK_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	var asOf time.Time
	for _, r := range rows {
		if r.ObservedAt.After(asOf) {
			asOf = r.ObservedAt
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"vessels": rows, "count": len(rows),
		"provenance": g.prov("ais_positions", asOf, 15*time.Minute),
	})
}

// portAnchors is the governed reference registry of port anchor positions
// (micro-degrees). Reference data, not vessel data — extend via config when
// more ports come online.
var portAnchors = map[string][2]int32{
	"KEMBA": {-4_062_000, 39_672_000},  // Mombasa
	"KEKIS": {-4_290_000, 39_420_000},  // Kisumu (lake)
	"TZDAR": {-6_830_000, 39_290_000},  // Dar es Salaam
}

// portApproaches: GET /v1/geo/ports/{code}/approaches?mmsi=&radiusMeters=
// With mmsi: ETA heuristic for that vessel. Without: every vessel within the
// radius with its ETA heuristic. All confidence-labelled.
func (g *GeoV2) portApproaches(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	code := strings.ToUpper(request.PathValue("code"))
	anchor, known := portAnchors[code]
	if !known {
		writeError(writer, http.StatusNotFound, "unknown port code (no governed anchor position)")
		return
	}
	nowUnix := g.now().Unix()
	labels := clearedLabels(principal.Clearance)

	type approachView struct {
		MMSI          string  `json:"mmsi"`
		DistanceMeters float64 `json:"distanceMeters"`
		EtaSeconds    int64   `json:"etaSeconds"`
		EtaAt         *string `json:"etaAt,omitempty"`
		SpeedKnots    float64 `json:"speedKnots"`
		Confidence    string  `json:"confidence"`
		Explanation   string  `json:"explanation"`
		PositionAsOf  string  `json:"positionAsOf"`
	}
	view := func(mmsi string, p track.Point) approachView {
		eta := track.EstimateApproach(p, anchor[0], anchor[1], nowUnix)
		v := approachView{
			MMSI: mmsi, DistanceMeters: eta.DistanceMeters, EtaSeconds: eta.ETASeconds,
			SpeedKnots: eta.SpeedKnots, Confidence: string(eta.Confidence), Explanation: eta.Explanation,
			PositionAsOf: time.Unix(p.AtUnix, 0).UTC().Format(time.RFC3339),
		}
		if eta.ETASeconds > 0 {
			s := time.Unix(nowUnix+eta.ETASeconds, 0).UTC().Format(time.RFC3339)
			v.EtaAt = &s
		}
		return v
	}

	if mmsi := strings.TrimSpace(request.URL.Query().Get("mmsi")); mmsi != "" {
		if !mmsiPattern.MatchString(mmsi) {
			writeError(writer, http.StatusBadRequest, "mmsi must be 9 digits")
			return
		}
		row, err := g.Store.LatestPosition(request.Context(), mmsi, labels)
		if err != nil {
			writeError(writer, http.StatusNotFound, "NO_POSITION_DATA: no recorded position for this vessel")
			return
		}
		p := track.Point{LatMicros: row.LatitudeMicros, LonMicros: row.LongitudeMicros, AtUnix: row.ObservedAt.Unix(), SogMillikn: row.SogMilliknots}
		writeJSON(writer, http.StatusOK, map[string]any{
			"port": code, "approach": view(mmsi, p),
			"provenance": g.prov("ais_positions", row.ObservedAt, 15*time.Minute),
		})
		return
	}

	radius := 100_000.0
	if raw := strings.TrimSpace(request.URL.Query().Get("radiusMeters")); raw != "" {
		var err error
		radius, err = strconv.ParseFloat(raw, 64)
		if err != nil || radius <= 0 || radius > 500_000 {
			writeError(writer, http.StatusBadRequest, "radiusMeters must be in (0, 500000]")
			return
		}
	}
	rows, err := g.Store.NearestVessels(request.Context(), anchor[0], anchor[1], radius, labels, 100)
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "TRACK_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	approaches := make([]approachView, 0, len(rows))
	var asOf time.Time
	for _, r := range rows {
		if r.ObservedAt.After(asOf) {
			asOf = r.ObservedAt
		}
		approaches = append(approaches, view(r.MMSI, track.Point{LatMicros: r.LatitudeMicros, LonMicros: r.LongitudeMicros, AtUnix: r.ObservedAt.Unix(), SogMillikn: r.SogMilliknots}))
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"port": code, "approaches": approaches, "count": len(approaches),
		"provenance": g.prov("ais_positions", asOf, 15*time.Minute),
	})
}

// ─── Congestion forecast ────────────────────────────────────────────────────

// congestionForecast: GET /v1/geo/ports/{code}/congestion/forecast?horizonHours=
// Reads ONLY the recorded port_queue_observations series. Returns 409 with
// INSUFFICIENT_HISTORY when the series is too short — never a synthetic
// forecast.
func (g *GeoV2) congestionForecast(writer http.ResponseWriter, request *http.Request) {
	code := strings.ToUpper(request.PathValue("code"))
	if len(code) != 5 {
		writeError(writer, http.StatusBadRequest, "port code must be the 5-letter UN/LOCODE")
		return
	}
	horizon := 24
	if raw := strings.TrimSpace(request.URL.Query().Get("horizonHours")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 1 || v > 168 {
			writeError(writer, http.StatusBadRequest, "horizonHours must be in 1..168")
			return
		}
		horizon = v
	}
	rows, err := g.Store.QueueObservations(request.Context(), code, g.now().Add(-90*24*time.Hour), 24*90)
	if err != nil {
		writeError(writer, http.StatusServiceUnavailable, "QUEUE_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	obs := make([]congestion.Observation, len(rows))
	for i, r := range rows {
		obs[i] = congestion.Observation{ObservedAtUnix: r.ObservedAt.Unix(), QueueLength: float64(r.QueueLength)}
	}
	fc, err := congestion.ForecastSeries(code, obs, 3600, horizon, 24)
	if errors.Is(err, congestion.ErrInsufficientHistory) {
		writeJSON(writer, http.StatusConflict, map[string]any{
			"port": code, "error": err.Error(), "recordedObservations": len(obs),
			"model": congestion.ModelLabel,
		})
		return
	}
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	var asOf time.Time
	if len(rows) > 0 {
		asOf = rows[len(rows)-1].ObservedAt
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"port": code, "forecast": fc,
		// Honest labelling: this is a statistical baseline, not an ML model.
		"modelDisclaimer": "baseline statistical model (seasonal-naive + damped Holt) with residual prediction intervals; not a machine-learned model",
		"provenance":      g.prov("port_queue_observations", asOf, 2*time.Hour),
	})
}

type queueObservationRequest struct {
	QueueLength  int    `json:"queueLength"`
	Source       string `json:"source"`
	ObservedAt   int64  `json:"observedAtUnix"`
}

func (g *GeoV2) recordQueueObservation(writer http.ResponseWriter, request *http.Request) {
	code := strings.ToUpper(request.PathValue("code"))
	if len(code) != 5 {
		writeError(writer, http.StatusBadRequest, "port code must be the 5-letter UN/LOCODE")
		return
	}
	var req queueObservationRequest
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<20)).Decode(&req); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid observation body: "+err.Error())
		return
	}
	if req.QueueLength < 0 || strings.TrimSpace(req.Source) == "" || req.ObservedAt <= 0 {
		writeError(writer, http.StatusBadRequest, "queueLength>=0, source and observedAtUnix>0 are required")
		return
	}
	if err := g.Store.InsertQueueObservation(request.Context(), store.QueueObservationRow{
		PortCode: code, QueueLength: req.QueueLength, Source: req.Source, ObservedAt: time.Unix(req.ObservedAt, 0).UTC(),
	}); err != nil {
		writeError(writer, http.StatusServiceUnavailable, "QUEUE_STORE_UNAVAILABLE: "+err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"recorded": true, "port": code})
}
