package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/mlstack"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// Phase 18 shadow-mode ML policy surface: berth-allocation and route-advice
// recommendations proxied to the ml-stack policy endpoints. The surface is
// SHADOW ONLY: a suggestion is never auto-applied to the berth-slots (or
// any other) system of record, and every served suggestion is logged to
// recommendation_log first — the future reward signal for offline RL.
//
// Fail-closed ladder: unwired scorer → 503 RECOMMENDATION_UNCONFIGURED;
// untrained policy (ml-stack 409) → 503 POLICY_UNTRAINED; scorer failure →
// 503 SCORING_UNAVAILABLE; log persistence failure → 503
// RECOMMENDATION_LOG_FAILED.

// PolicyScorer is the ml-stack client boundary (mlstack.Client in
// production; a stub in tests).
type PolicyScorer interface {
	ScoreBerthAllocation(ctx context.Context, request mlstack.BerthAllocationRequest) (*mlstack.PolicyResponse, error)
	ScoreRouteAdvice(ctx context.Context, request mlstack.RouteAdviceRequest) (*mlstack.PolicyResponse, error)
}

// RecommendationLogStore is the persistence boundary
// (*store.Store in production; a recorder in tests).
type RecommendationLogStore interface {
	InsertRecommendationLog(ctx context.Context, row store.RecommendationLogRow) error
}

// Recommendations wires the shadow-mode policy endpoints.
type Recommendations struct {
	Scorer  PolicyScorer
	Log     RecommendationLogStore
	Metrics *metrics.Registry
	now     func() time.Time
}

// NewRecommendations validates the wiring fail-closed.
func NewRecommendations(scorer PolicyScorer, logStore RecommendationLogStore, registry *metrics.Registry) (*Recommendations, error) {
	if scorer == nil {
		return nil, errors.New("recommendations: policy scorer is required")
	}
	if logStore == nil {
		return nil, errors.New("recommendations: recommendation log store is required")
	}
	if registry == nil {
		return nil, errors.New("recommendations: metrics registry is required")
	}
	return &Recommendations{Scorer: scorer, Log: logStore, Metrics: registry, now: func() time.Time { return time.Now().UTC() }}, nil
}

// portCodePattern mirrors the port_code contract shape (UN/LOCODE-style).
var portCodePattern = regexp.MustCompile(`^[A-Z0-9]{2,16}$`)

// vesselInput is the public (camelCase) vessel descriptor; converted to the
// snake_case ml-stack wire shape before scoring.
type vesselInput struct {
	MMSI        string  `json:"mmsi"`
	ETA         string  `json:"eta,omitempty"` // RFC 3339
	LOAMeters   float64 `json:"loaMeters,omitempty"`
	DraftMeters float64 `json:"draftMeters,omitempty"`
}

// berthInput is the public candidate berth slot.
type berthInput struct {
	BerthID       string  `json:"berthId"`
	LengthMeters  float64 `json:"lengthMeters,omitempty"`
	DepthMeters   float64 `json:"depthMeters,omitempty"`
	OccupiedUntil string  `json:"occupiedUntil,omitempty"` // RFC 3339
}

// berthRecommendationRequest is the public POST body.
type berthRecommendationRequest struct {
	PortCode string        `json:"portCode"`
	Vessels  []vesselInput `json:"vessels"`
	Berths   []berthInput  `json:"berths,omitempty"`
}

// routeAdviceRequest is the public POST body.
type routeAdviceRequest struct {
	Origin        string       `json:"origin"`
	Destination   string       `json:"destination"`
	Vessel        *vesselInput `json:"vessel,omitempty"`
	DepartureTime string       `json:"departureTime,omitempty"`
}

func toWireVessel(vessel vesselInput) mlstack.VesselInput {
	return mlstack.VesselInput{
		MMSI: vessel.MMSI, ETA: vessel.ETA,
		LOAMeters: vessel.LOAMeters, DraftMeters: vessel.DraftMeters,
	}
}

func validVessel(vessel vesselInput) bool {
	if !mmsiPattern.MatchString(vessel.MMSI) {
		return false
	}
	if vessel.LOAMeters < 0 || vessel.DraftMeters < 0 {
		return false
	}
	if vessel.ETA != "" {
		if _, err := time.Parse(time.RFC3339, vessel.ETA); err != nil {
			return false
		}
	}
	return true
}

// registerRecommendationRoutes wires the shadow-mode policy surface. The
// routes are ALWAYS registered (same doctrine as the SSE surface); when the
// scorer is unwired they answer an honest 503 so capability discovery stays
// stable. Reads sit on the standard read role set: the endpoints are
// advisory reads that mutate nothing.
func (server *Server) registerRecommendationRoutes(mux *http.ServeMux) {
	gate := func(handler http.HandlerFunc) http.Handler {
		return auth.RequireRoles(handler, "geo-reader", "geo-zone-maker", "geo-zone-checker", "geo-admin")
	}
	mux.Handle("POST /v1/geo/berths/recommendation", gate(http.HandlerFunc(server.berthRecommendation)))
	mux.Handle("POST /v1/geo/routes/advice", gate(http.HandlerFunc(server.routeAdvice)))
}

// berthRecommendation: POST /v1/geo/berths/recommendation.
func (server *Server) berthRecommendation(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	if server.Recommend == nil {
		server.countRecommendation("berth", "unconfigured")
		writeError(writer, http.StatusServiceUnavailable,
			"RECOMMENDATION_UNCONFIGURED: ML policy scoring is not wired (ML_STACK_HTTP_URL is not set)")
		return
	}
	var payload berthRecommendationRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	if !portCodePattern.MatchString(payload.PortCode) {
		writeError(writer, http.StatusBadRequest, "portCode must be 2-16 upper-case alphanumerics (UN/LOCODE-style)")
		return
	}
	if len(payload.Vessels) == 0 || len(payload.Vessels) > 50 {
		writeError(writer, http.StatusBadRequest, "vessels must carry 1-50 incoming vessels")
		return
	}
	if len(payload.Berths) > 200 {
		writeError(writer, http.StatusBadRequest, "berths must carry at most 200 candidate slots")
		return
	}
	wireVessels := make([]mlstack.VesselInput, 0, len(payload.Vessels))
	for _, vessel := range payload.Vessels {
		if !validVessel(vessel) {
			writeError(writer, http.StatusBadRequest, "each vessel needs a 9-digit mmsi, non-negative dimensions and an RFC 3339 eta")
			return
		}
		wireVessels = append(wireVessels, toWireVessel(vessel))
	}
	wireBerths := make([]mlstack.BerthInput, 0, len(payload.Berths))
	for _, berth := range payload.Berths {
		if berth.BerthID == "" || len(berth.BerthID) > 64 || berth.LengthMeters < 0 || berth.DepthMeters < 0 {
			writeError(writer, http.StatusBadRequest, "each berth needs a berthId (<=64 chars) and non-negative dimensions")
			return
		}
		if berth.OccupiedUntil != "" {
			if _, err := time.Parse(time.RFC3339, berth.OccupiedUntil); err != nil {
				writeError(writer, http.StatusBadRequest, "berth occupiedUntil must be RFC 3339")
				return
			}
		}
		wireBerths = append(wireBerths, mlstack.BerthInput{
			BerthID: berth.BerthID, LengthMeters: berth.LengthMeters,
			DepthMeters: berth.DepthMeters, OccupiedUntil: berth.OccupiedUntil,
		})
	}
	scoreRequest := mlstack.BerthAllocationRequest{
		RequestID: uuid.NewString(),
		PortCode:  payload.PortCode,
		Vessels:   wireVessels,
		Berths:    wireBerths,
	}
	response, err := server.Recommend.Scorer.ScoreBerthAllocation(request.Context(), scoreRequest)
	if err != nil {
		server.scoreError(writer, "berth", err)
		return
	}
	server.serveRecommendation(writer, request, "berth", "berth_allocation", payload, response, principal.Subject)
}

// routeAdvice: POST /v1/geo/routes/advice.
func (server *Server) routeAdvice(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	if server.Recommend == nil {
		server.countRecommendation("route", "unconfigured")
		writeError(writer, http.StatusServiceUnavailable,
			"RECOMMENDATION_UNCONFIGURED: ML policy scoring is not wired (ML_STACK_HTTP_URL is not set)")
		return
	}
	var payload routeAdviceRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	if !portCodePattern.MatchString(payload.Origin) || !portCodePattern.MatchString(payload.Destination) {
		writeError(writer, http.StatusBadRequest, "origin and destination must be 2-16 upper-case alphanumerics (UN/LOCODE-style)")
		return
	}
	if payload.Vessel != nil && !validVessel(*payload.Vessel) {
		writeError(writer, http.StatusBadRequest, "vessel needs a 9-digit mmsi, non-negative dimensions and an RFC 3339 eta")
		return
	}
	var wireVessel *mlstack.VesselInput
	if payload.Vessel != nil {
		converted := toWireVessel(*payload.Vessel)
		wireVessel = &converted
	}
	if payload.DepartureTime != "" {
		if _, err := time.Parse(time.RFC3339, payload.DepartureTime); err != nil {
			writeError(writer, http.StatusBadRequest, "departureTime must be RFC 3339")
			return
		}
	}
	scoreRequest := mlstack.RouteAdviceRequest{
		RequestID:     uuid.NewString(),
		Origin:        payload.Origin,
		Destination:   payload.Destination,
		Vessel:        wireVessel,
		DepartureTime: payload.DepartureTime,
	}
	response, err := server.Recommend.Scorer.ScoreRouteAdvice(request.Context(), scoreRequest)
	if err != nil {
		server.scoreError(writer, "route", err)
		return
	}
	server.serveRecommendation(writer, request, "route", "route_advice", payload, response, principal.Subject)
}

// scoreError maps the typed scorer failures onto the honest 503 ladder.
func (server *Server) scoreError(writer http.ResponseWriter, kind string, err error) {
	switch {
	case mlstack.IsUntrained(err):
		server.countRecommendation(kind, "untrained")
		writeError(writer, http.StatusServiceUnavailable, err.Error())
	default:
		server.countRecommendation(kind, "unavailable")
		writeError(writer, http.StatusServiceUnavailable, "SCORING_UNAVAILABLE: the policy scorer could not answer")
	}
}

// serveRecommendation logs the suggestion FIRST (fail closed) and only then
// answers 200. The response always declares mode:shadow and never applies
// anything to a system of record. logPayload is the validated public
// request (used for the deterministic request hash and the stored feature
// vector); it deliberately excludes the freshly-minted request_id so
// identical inputs hash identically.
func (server *Server) serveRecommendation(writer http.ResponseWriter, request *http.Request, kind, logKind string, logPayload any, response *mlstack.PolicyResponse, subject string) {
	mode := response.Mode
	if mode == "" {
		// Honest default while every policy is shadow-gated; the ml-stack
		// contract carries mode and the log records what was served.
		mode = "shadow"
	}
	// Canonical request hash: Go struct marshaling is field-order stable,
	// so this digest is deterministic for identical validated payloads.
	requestJSON, err := json.Marshal(logPayload)
	if err != nil {
		server.countRecommendation(kind, "unavailable")
		writeError(writer, http.StatusServiceUnavailable, "SCORING_UNAVAILABLE: recommendation request could not be hashed")
		return
	}
	digest := sha256.Sum256(requestJSON)
	suggestion := response.Suggestion
	if len(suggestion) == 0 {
		suggestion = json.RawMessage(`null`)
	}
	row := store.RecommendationLogRow{
		RecommendationID: uuid.NewString(),
		Kind:             logKind,
		RequestHash:      hex.EncodeToString(digest[:]),
		Request:          requestJSON,
		PolicyVersion:    response.PolicyVersion,
		Mode:             mode,
		Suggestion:       suggestion,
		RequestedBy:      subject,
		CreatedAt:        server.Recommend.now(),
	}
	if err := server.Recommend.Log.InsertRecommendationLog(request.Context(), row); err != nil {
		server.countRecommendation(kind, "log_failed")
		writeError(writer, http.StatusServiceUnavailable,
			"RECOMMENDATION_LOG_FAILED: the recommendation could not be logged, so it is not served (fail-closed reward trail)")
		return
	}
	server.countRecommendation(kind, "served")
	writeJSON(writer, http.StatusOK, map[string]any{
		"recommendationId": row.RecommendationID,
		"mode":             mode,
		"policyVersion":    response.PolicyVersion,
		"suggestion":       suggestion,
		"shadow":           true,
		"note":             "advisory only: never auto-applied to the slots system of record; logged for future reward joins on recommendationId",
	})
}

func (server *Server) countRecommendation(kind, result string) {
	if server.Metrics != nil {
		server.Metrics.Inc("geo_ml_recommendations_total", map[string]string{"kind": kind, "result": result})
	}
}
