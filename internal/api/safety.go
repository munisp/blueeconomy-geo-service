package api

// Phase-12 safety-compliance REST boundary: FSC/PSC inspections (detention
// workflow with maker-checker release), SAR coordination (IAMSAR phase
// ladder, resource tasking, append-only comms log) and marine accident
// investigation (state machine, hash-chained evidence, findings and
// recommendations). Every mutation persists under tenant-bound RLS and
// publishes the canonical signed envelope (safety.inspection.v1 /
// safety.sar.v1 / safety.investigation.v1); the mutation endpoints fail
// closed (503) when the publisher is not wired. All safety endpoints
// require the RESTRICTED clearance floor, mirroring the SOS lifecycle.

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// Safety wires the Phase-12 safety-compliance endpoints.
type Safety struct {
	Store *store.Store
	// Events publishes the signed safety.*.v1 lifecycle envelopes. Mutating
	// endpoints fail closed (503) when unwired.
	Events SignedEnvelopePublisher
	now    func() time.Time
}

// NewSafety wires the safety surface fail-closed.
func NewSafety(storage *store.Store) (*Safety, error) {
	if storage == nil {
		return nil, errors.New("safety store is required")
	}
	return &Safety{Store: storage, now: time.Now}, nil
}

func (server *Server) registerSafetyRoutes(mux *http.ServeMux) {
	s := server.Safety
	read := func(pattern string, handler http.HandlerFunc) {
		mux.Handle(pattern, auth.RequireRoles(http.HandlerFunc(handler),
			"geo-reader", "geo-inspector", "geo-inspection-checker",
			"geo-sar-coordinator", "geo-investigator", "geo-admin"))
	}
	// FSC/PSC inspection.
	read("GET /v1/safety/inspections", s.listInspections)
	read("GET /v1/safety/inspections/{id}", s.getInspection)
	read("GET /v1/safety/inspections/{id}/deficiencies", s.listDeficiencies)
	read("GET /v1/safety/checklist-templates", s.listChecklistTemplates)
	mux.Handle("POST /v1/safety/checklist-templates",
		auth.RequireRoles(http.HandlerFunc(s.createChecklistTemplate), "geo-inspector", "geo-admin"))
	mux.Handle("POST /v1/safety/inspections",
		auth.RequireRoles(http.HandlerFunc(s.createInspection), "geo-inspector", "geo-admin"))
	mux.Handle("POST /v1/safety/inspections/{id}/transition",
		auth.RequireRoles(http.HandlerFunc(s.transitionInspection), "geo-inspector", "geo-inspection-checker", "geo-admin"))
	mux.Handle("POST /v1/safety/inspections/{id}/deficiencies",
		auth.RequireRoles(http.HandlerFunc(s.recordDeficiency), "geo-inspector", "geo-admin"))
	mux.Handle("POST /v1/safety/deficiencies/{id}/transition",
		auth.RequireRoles(http.HandlerFunc(s.transitionDeficiency), "geo-inspector", "geo-inspection-checker", "geo-admin"))
	// SAR coordination.
	read("GET /v1/safety/sar", s.listSarIncidents)
	read("GET /v1/safety/sar/{id}", s.getSarIncident)
	read("GET /v1/safety/sar/{id}/comms", s.listSarComms)
	mux.Handle("POST /v1/safety/sar",
		auth.RequireRoles(http.HandlerFunc(s.createSarIncident), "geo-sar-coordinator", "geo-admin"))
	mux.Handle("POST /v1/safety/sar/{id}/transition",
		auth.RequireRoles(http.HandlerFunc(s.transitionSarIncident), "geo-sar-coordinator", "geo-admin"))
	mux.Handle("POST /v1/safety/sar/{id}/taskings",
		auth.RequireRoles(http.HandlerFunc(s.taskSarResource), "geo-sar-coordinator", "geo-admin"))
	mux.Handle("POST /v1/safety/sar-taskings/{id}/transition",
		auth.RequireRoles(http.HandlerFunc(s.transitionSarTasking), "geo-sar-coordinator", "geo-admin"))
	mux.Handle("POST /v1/safety/sar/{id}/comms",
		auth.RequireRoles(http.HandlerFunc(s.appendSarComms), "geo-sar-coordinator", "geo-admin"))
	// Marine accident investigation.
	read("GET /v1/safety/investigations", s.listInvestigationCases)
	read("GET /v1/safety/investigations/{id}", s.getInvestigationCase)
	read("GET /v1/safety/investigations/{id}/evidence", s.listEvidence)
	read("GET /v1/safety/investigations/{id}/findings", s.listFindings)
	read("GET /v1/safety/investigations/{id}/recommendations", s.listRecommendations)
	mux.Handle("POST /v1/safety/investigations",
		auth.RequireRoles(http.HandlerFunc(s.createInvestigationCase), "geo-investigator", "geo-admin"))
	mux.Handle("POST /v1/safety/investigations/{id}/transition",
		auth.RequireRoles(http.HandlerFunc(s.transitionInvestigationCase), "geo-investigator", "geo-admin"))
	mux.Handle("POST /v1/safety/investigations/{id}/evidence",
		auth.RequireRoles(http.HandlerFunc(s.appendEvidence), "geo-investigator", "geo-admin"))
	mux.Handle("POST /v1/safety/investigations/{id}/findings",
		auth.RequireRoles(http.HandlerFunc(s.addFinding), "geo-investigator", "geo-admin"))
	mux.Handle("POST /v1/safety/investigations/{id}/recommendations",
		auth.RequireRoles(http.HandlerFunc(s.addRecommendation), "geo-investigator", "geo-admin"))
	mux.Handle("POST /v1/safety/recommendations/{id}/transition",
		auth.RequireRoles(http.HandlerFunc(s.decideRecommendation), "geo-investigator", "geo-admin"))
}

var safetyIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// safetyPrincipal enforces the RESTRICTED clearance floor shared by every
// safety endpoint (SOS doctrine) and returns the verified principal.
func (s *Safety) safetyPrincipal(writer http.ResponseWriter, request *http.Request) (auth.Principal, bool) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return auth.Principal{}, false
	}
	clearance, err := sign.ParseClassification(principal.Clearance)
	if err != nil || !clearance.Covers(sign.ClassificationRestricted) {
		writeError(writer, http.StatusForbidden, "safety endpoints require RESTRICTED or higher clearance")
		return auth.Principal{}, false
	}
	return principal, true
}

func (s *Safety) publisherReady(writer http.ResponseWriter) bool {
	if s.Events == nil {
		writeError(writer, http.StatusServiceUnavailable, "safety event publisher is not wired")
		return false
	}
	return true
}

func safetyPathID(writer http.ResponseWriter, request *http.Request) (string, bool) {
	id := request.PathValue("id")
	if !safetyIDPattern.MatchString(id) {
		writeError(writer, http.StatusBadRequest, "path id is invalid")
		return "", false
	}
	return id, true
}

func decodeBody(writer http.ResponseWriter, request *http.Request, target any) bool {
	if request.Body == nil {
		return true
	}
	err := json.NewDecoder(request.Body).Decode(target)
	if err != nil && !errors.Is(err, io.EOF) {
		writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return false
	}
	return true
}

func safetyStoreError(writer http.ResponseWriter, err error, context string) {
	switch {
	case errors.Is(err, store.ErrSafetyNotFound):
		writeError(writer, http.StatusNotFound, context+" not found")
	case errors.Is(err, store.ErrSafetyMakerChecker):
		writeError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrSafetyInvalidTransition):
		writeError(writer, http.StatusConflict, err.Error())
	default:
		writeError(writer, http.StatusInternalServerError, context+" failed")
	}
}

// ─── FSC/PSC inspection ─────────────────────────────────────────────────────

type checklistTemplateRequest struct {
	TemplateID string          `json:"templateId"`
	Regime     string          `json:"regime"`
	Version    int             `json:"version"`
	Title      string          `json:"title"`
	Items      json.RawMessage `json:"items"`
}

func (s *Safety) createChecklistTemplate(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	var payload checklistTemplateRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if !safetyIDPattern.MatchString(payload.TemplateID) ||
		(payload.Regime != "FSC" && payload.Regime != "PSC") || payload.Version < 1 ||
		len(payload.Title) == 0 || len(payload.Items) == 0 {
		writeError(writer, http.StatusBadRequest, "template id, regime (FSC|PSC), version, title and items are required")
		return
	}
	var items []map[string]any
	if err := json.Unmarshal(payload.Items, &items); err != nil || len(items) == 0 {
		writeError(writer, http.StatusBadRequest, "items must be a non-empty JSON array")
		return
	}
	for _, item := range items {
		code, _ := item["code"].(string)
		if code == "" {
			writeError(writer, http.StatusBadRequest, "every checklist item requires a code")
			return
		}
	}
	err := s.Store.CreateChecklistTemplate(request.Context(), store.ChecklistTemplateRow{
		TemplateID: payload.TemplateID,
		TenantID:   principal.TenantID,
		Regime:     payload.Regime,
		Version:    payload.Version,
		Title:      payload.Title,
		Items:      string(payload.Items),
		CreatedBy:  principal.Subject,
	})
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "checklist template creation failed")
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"templateId": payload.TemplateID})
}

func (s *Safety) listChecklistTemplates(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	templates, err := s.Store.ListChecklistTemplates(request.Context(), principal.TenantID,
		request.URL.Query().Get("regime"))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "checklist template query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"checklistTemplates": templates})
}

type inspectionRequest struct {
	InspectionID    string `json:"inspectionId"`
	Regime          string `json:"regime"`
	TemplateID      string `json:"templateId"`
	VesselReference string `json:"vesselReference"`
	PortCode        string `json:"portCode"`
	Classification  string `json:"classification"`
}

func (s *Safety) createInspection(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok || !s.publisherReady(writer) {
		return
	}
	var payload inspectionRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if !safetyIDPattern.MatchString(payload.InspectionID) ||
		(payload.Regime != "FSC" && payload.Regime != "PSC") ||
		!safetyIDPattern.MatchString(payload.TemplateID) || payload.VesselReference == "" {
		writeError(writer, http.StatusBadRequest, "inspection id, regime (FSC|PSC), template id and vessel reference are required")
		return
	}
	classification, err := sign.ParseClassification(payload.Classification)
	if err != nil || !classification.Covers(sign.ClassificationRestricted) {
		writeError(writer, http.StatusBadRequest, "classification floor is RESTRICTED")
		return
	}
	row, err := s.Store.CreateInspection(request.Context(), store.InspectionRow{
		InspectionID:         payload.InspectionID,
		TenantID:             principal.TenantID,
		Regime:               payload.Regime,
		TemplateID:           payload.TemplateID,
		VesselReference:      payload.VesselReference,
		PortCode:             payload.PortCode,
		Classification:       payload.Classification,
		InspectorPrincipalID: principal.Subject,
	})
	if err != nil {
		safetyStoreError(writer, err, "inspection creation")
		return
	}
	s.publishInspection(writer, request, row, principal.Subject, "create", "")
	if writer.Header().Get("X-Safety-Publish-Failed") != "" {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"inspection": row})
}

func (s *Safety) getInspection(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	row, err := s.Store.GetInspection(request.Context(), principal.TenantID, id)
	if err != nil {
		safetyStoreError(writer, err, "inspection")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"inspection": row})
}

func (s *Safety) listInspections(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	rows, err := s.Store.ListInspections(request.Context(), principal.TenantID, parseLimit(request, 100, 1000))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "inspection query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"inspections": rows})
}

type transitionRequest struct {
	Action string `json:"action"`
	Note   string `json:"note,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// publishInspection emits the signed safety.inspection.v1 envelope; on
// failure it writes 500 and marks the response so the caller does not write
// a success body afterwards.
func (s *Safety) publishInspection(writer http.ResponseWriter, request *http.Request, row store.InspectionRow, actor, action, note string) {
	occurredAt := s.now().UTC()
	err := s.Events.PublishSignedEnvelope(request.Context(), sign.EventSafetyInspection,
		row.InspectionID, sign.SafetyInspectionLifecycle{
			InspectionID:    row.InspectionID,
			Regime:          row.Regime,
			VesselReference: row.VesselReference,
			PortCode:        row.PortCode,
			Action:          action,
			State:           row.State,
			Actor:           actor,
			OccurredAt:      occurredAt,
			Note:            note,
			Classification:  row.Classification,
		}, occurredAt, row.Classification, map[string]string{"priority": "SAFETY"})
	if err != nil {
		writer.Header().Set("X-Safety-Publish-Failed", "1")
		writeError(writer, http.StatusInternalServerError, "safety inspection event publication failed")
	}
}

func (s *Safety) transitionInspection(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok || !s.publisherReady(writer) {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload transitionRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	// Detention maker-checker role gate: the release action requires the
	// checker role (the store enforces checker <> detaining maker).
	if payload.Action == "release" && !principal.HasRole("geo-inspection-checker") && !principal.HasRole("geo-admin") {
		writeError(writer, http.StatusForbidden, "release requires the geo-inspection-checker role")
		return
	}
	row, err := s.Store.TransitionInspection(request.Context(), principal.TenantID, id,
		principal.Subject, payload.Action, payload.Note)
	if err != nil {
		safetyStoreError(writer, err, "inspection transition")
		return
	}
	s.publishInspection(writer, request, row, principal.Subject, payload.Action, payload.Note)
	if writer.Header().Get("X-Safety-Publish-Failed") != "" {
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"inspection": row})
}

type deficiencyRequest struct {
	DeficiencyID          string     `json:"deficiencyId"`
	Code                  string     `json:"code"`
	Description           string     `json:"description"`
	Severity              string     `json:"severity"`
	RectificationDeadline *time.Time `json:"rectificationDeadline,omitempty"`
}

func (s *Safety) recordDeficiency(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload deficiencyRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if !safetyIDPattern.MatchString(payload.DeficiencyID) || payload.Code == "" || payload.Description == "" ||
		(payload.Severity != "MINOR" && payload.Severity != "MAJOR" && payload.Severity != "CRITICAL") {
		writeError(writer, http.StatusBadRequest, "deficiency id, code, description and severity (MINOR|MAJOR|CRITICAL) are required")
		return
	}
	row, err := s.Store.RecordDeficiency(request.Context(), principal.TenantID, store.DeficiencyRow{
		DeficiencyID:          payload.DeficiencyID,
		InspectionID:          id,
		Code:                  payload.Code,
		Description:           payload.Description,
		Severity:              payload.Severity,
		RectificationDeadline: payload.RectificationDeadline,
		RecordedBy:            principal.Subject,
	})
	if err != nil {
		safetyStoreError(writer, err, "deficiency recording")
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"deficiency": row})
}

func (s *Safety) listDeficiencies(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	rows, err := s.Store.ListDeficiencies(request.Context(), principal.TenantID, id)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "deficiency query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"deficiencies": rows})
}

func (s *Safety) transitionDeficiency(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload transitionRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if payload.Action == "verify" && !principal.HasRole("geo-inspection-checker") && !principal.HasRole("geo-admin") {
		writeError(writer, http.StatusForbidden, "verify requires the geo-inspection-checker role")
		return
	}
	row, err := s.Store.TransitionDeficiency(request.Context(), principal.TenantID, id,
		principal.Subject, payload.Action)
	if err != nil {
		safetyStoreError(writer, err, "deficiency transition")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"deficiency": row})
}

// ─── SAR coordination ───────────────────────────────────────────────────────

type sarIncidentRequest struct {
	IncidentID      string `json:"incidentId"`
	SosAlertID      string `json:"sosAlertId,omitempty"`
	Title           string `json:"title"`
	Classification  string `json:"classification"`
	LatitudeMicros  *int32 `json:"latitudeMicros,omitempty"`
	LongitudeMicros *int32 `json:"longitudeMicros,omitempty"`
}

func (s *Safety) createSarIncident(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok || !s.publisherReady(writer) {
		return
	}
	var payload sarIncidentRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if !safetyIDPattern.MatchString(payload.IncidentID) || payload.Title == "" {
		writeError(writer, http.StatusBadRequest, "incident id and title are required")
		return
	}
	classification, err := sign.ParseClassification(payload.Classification)
	if err != nil || !classification.Covers(sign.ClassificationRestricted) {
		writeError(writer, http.StatusBadRequest, "classification floor is RESTRICTED")
		return
	}
	var sosAlertID *string
	if payload.SosAlertID != "" {
		if !safetyIDPattern.MatchString(payload.SosAlertID) {
			writeError(writer, http.StatusBadRequest, "sos alert id is invalid")
			return
		}
		sosAlertID = &payload.SosAlertID
	}
	row, err := s.Store.CreateSarIncident(request.Context(), store.SarIncidentRow{
		IncidentID:     payload.IncidentID,
		TenantID:       principal.TenantID,
		SosAlertID:     sosAlertID,
		Title:          payload.Title,
		Classification: payload.Classification,
		OpenedBy:       principal.Subject,
	}, payload.LatitudeMicros, payload.LongitudeMicros)
	if err != nil {
		safetyStoreError(writer, err, "sar incident creation")
		return
	}
	s.publishSar(writer, request, row, principal.Subject, "open", "", "")
	if writer.Header().Get("X-Safety-Publish-Failed") != "" {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"sarIncident": row})
}

func (s *Safety) publishSar(writer http.ResponseWriter, request *http.Request, row store.SarIncidentRow, actor, action, resourceName, note string) {
	occurredAt := s.now().UTC()
	sosAlertID := ""
	if row.SosAlertID != nil {
		sosAlertID = *row.SosAlertID
	}
	err := s.Events.PublishSignedEnvelope(request.Context(), sign.EventSafetySAR,
		row.IncidentID, sign.SafetySarLifecycle{
			IncidentID:     row.IncidentID,
			SosAlertID:     sosAlertID,
			Action:         action,
			Phase:          row.Phase,
			Actor:          actor,
			OccurredAt:     occurredAt,
			ResourceName:   resourceName,
			Note:           note,
			Classification: row.Classification,
		}, occurredAt, row.Classification, map[string]string{"priority": "SAFETY"})
	if err != nil {
		writer.Header().Set("X-Safety-Publish-Failed", "1")
		writeError(writer, http.StatusInternalServerError, "sar event publication failed")
	}
}

func (s *Safety) getSarIncident(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	row, err := s.Store.GetSarIncident(request.Context(), principal.TenantID, id)
	if err != nil {
		safetyStoreError(writer, err, "sar incident")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"sarIncident": row})
}

func (s *Safety) listSarIncidents(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	rows, err := s.Store.ListSarIncidents(request.Context(), principal.TenantID, parseLimit(request, 100, 1000))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "sar incident query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"sarIncidents": rows})
}

func (s *Safety) transitionSarIncident(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok || !s.publisherReady(writer) {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload transitionRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	row, err := s.Store.TransitionSarIncident(request.Context(), principal.TenantID, id,
		principal.Subject, payload.Action, payload.Reason)
	if err != nil {
		safetyStoreError(writer, err, "sar incident transition")
		return
	}
	s.publishSar(writer, request, row, principal.Subject, payload.Action, "", payload.Reason)
	if writer.Header().Get("X-Safety-Publish-Failed") != "" {
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"sarIncident": row})
}

type sarTaskingRequest struct {
	TaskingID    string `json:"taskingId"`
	ResourceType string `json:"resourceType"`
	ResourceName string `json:"resourceName"`
}

func (s *Safety) taskSarResource(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok || !s.publisherReady(writer) {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload sarTaskingRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if !safetyIDPattern.MatchString(payload.TaskingID) || payload.ResourceName == "" ||
		(payload.ResourceType != "VESSEL" && payload.ResourceType != "AIRCRAFT" &&
			payload.ResourceType != "TEAM" && payload.ResourceType != "OTHER") {
		writeError(writer, http.StatusBadRequest, "tasking id, resource type (VESSEL|AIRCRAFT|TEAM|OTHER) and resource name are required")
		return
	}
	row, err := s.Store.TaskSarResource(request.Context(), principal.TenantID, store.SarTaskingRow{
		TaskingID:    payload.TaskingID,
		IncidentID:   id,
		ResourceType: payload.ResourceType,
		ResourceName: payload.ResourceName,
		TaskedBy:     principal.Subject,
	})
	if err != nil {
		safetyStoreError(writer, err, "sar resource tasking")
		return
	}
	incident, err := s.Store.GetSarIncident(request.Context(), principal.TenantID, id)
	if err != nil {
		safetyStoreError(writer, err, "sar incident")
		return
	}
	s.publishSar(writer, request, incident, principal.Subject, "task", payload.ResourceName, "")
	if writer.Header().Get("X-Safety-Publish-Failed") != "" {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"tasking": row})
}

func (s *Safety) transitionSarTasking(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload transitionRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	row, err := s.Store.TransitionSarTasking(request.Context(), principal.TenantID, id,
		principal.Subject, payload.Action)
	if err != nil {
		safetyStoreError(writer, err, "sar tasking transition")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"tasking": row})
}

type sarCommsRequest struct {
	Direction string `json:"direction"`
	Channel   string `json:"channel"`
	Message   string `json:"message"`
}

func (s *Safety) appendSarComms(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload sarCommsRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if (payload.Direction != "INBOUND" && payload.Direction != "OUTBOUND") ||
		payload.Channel == "" || payload.Message == "" {
		writeError(writer, http.StatusBadRequest, "direction (INBOUND|OUTBOUND), channel and message are required")
		return
	}
	row, err := s.Store.AppendSarComms(request.Context(), principal.TenantID, store.SarCommsRow{
		IncidentID: id,
		Direction:  payload.Direction,
		Channel:    payload.Channel,
		Message:    payload.Message,
		LoggedBy:   principal.Subject,
	})
	if err != nil {
		safetyStoreError(writer, err, "sar comms entry")
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"commsEntry": row})
}

func (s *Safety) listSarComms(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	rows, err := s.Store.ListSarComms(request.Context(), principal.TenantID, id, parseLimit(request, 500, 5000))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "sar comms query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"commsLog": rows})
}

// ─── Marine accident investigation ──────────────────────────────────────────

type investigationCaseRequest struct {
	CaseID          string     `json:"caseId"`
	CasualtyType    string     `json:"casualtyType"`
	Severity        string     `json:"severity"`
	VesselReference string     `json:"vesselReference"`
	OccurredAt      time.Time  `json:"occurredAt"`
	Classification  string     `json:"classification"`
	LatitudeMicros  *int32     `json:"latitudeMicros,omitempty"`
	LongitudeMicros *int32     `json:"longitudeMicros,omitempty"`
}

func (s *Safety) createInvestigationCase(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok || !s.publisherReady(writer) {
		return
	}
	var payload investigationCaseRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if !safetyIDPattern.MatchString(payload.CaseID) || payload.VesselReference == "" || payload.OccurredAt.IsZero() {
		writeError(writer, http.StatusBadRequest, "case id, vessel reference and occurredAt are required")
		return
	}
	classification, err := sign.ParseClassification(payload.Classification)
	if err != nil || !classification.Covers(sign.ClassificationRestricted) {
		writeError(writer, http.StatusBadRequest, "classification floor is RESTRICTED")
		return
	}
	row, err := s.Store.CreateInvestigationCase(request.Context(), store.InvestigationCaseRow{
		CaseID:           payload.CaseID,
		TenantID:         principal.TenantID,
		CasualtyType:     payload.CasualtyType,
		Severity:         payload.Severity,
		VesselReference:  payload.VesselReference,
		OccurredAt:       payload.OccurredAt,
		Classification:   payload.Classification,
		LeadInvestigator: principal.Subject,
	}, payload.LatitudeMicros, payload.LongitudeMicros)
	if err != nil {
		safetyStoreError(writer, err, "investigation case creation")
		return
	}
	s.publishInvestigation(writer, request, row, principal.Subject, "open", "", "")
	if writer.Header().Get("X-Safety-Publish-Failed") != "" {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"investigationCase": row})
}

func (s *Safety) publishInvestigation(writer http.ResponseWriter, request *http.Request, row store.InvestigationCaseRow, actor, action, evidenceID, chainHash string) {
	occurredAt := s.now().UTC()
	err := s.Events.PublishSignedEnvelope(request.Context(), sign.EventSafetyInvestigation,
		row.CaseID, sign.SafetyInvestigationLifecycle{
			CaseID:         row.CaseID,
			CasualtyType:   row.CasualtyType,
			Action:         action,
			State:          row.State,
			Actor:          actor,
			OccurredAt:     occurredAt,
			EvidenceID:     evidenceID,
			ChainHash:      chainHash,
			Classification: row.Classification,
		}, occurredAt, row.Classification, map[string]string{"priority": "SAFETY"})
	if err != nil {
		writer.Header().Set("X-Safety-Publish-Failed", "1")
		writeError(writer, http.StatusInternalServerError, "investigation event publication failed")
	}
}

func (s *Safety) getInvestigationCase(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	row, err := s.Store.GetInvestigationCase(request.Context(), principal.TenantID, id)
	if err != nil {
		safetyStoreError(writer, err, "investigation case")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"investigationCase": row})
}

func (s *Safety) listInvestigationCases(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	rows, err := s.Store.ListInvestigationCases(request.Context(), principal.TenantID, parseLimit(request, 100, 1000))
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "investigation case query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"investigationCases": rows})
}

func (s *Safety) transitionInvestigationCase(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok || !s.publisherReady(writer) {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload transitionRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	row, err := s.Store.TransitionInvestigationCase(request.Context(), principal.TenantID, id,
		principal.Subject, payload.Action)
	if err != nil {
		safetyStoreError(writer, err, "investigation case transition")
		return
	}
	s.publishInvestigation(writer, request, row, principal.Subject, payload.Action, "", "")
	if writer.Header().Get("X-Safety-Publish-Failed") != "" {
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"investigationCase": row})
}

type evidenceRequest struct {
	EvidenceID  string `json:"evidenceId"`
	Description string `json:"description"`
	ContentHash string `json:"contentHash"`
}

var sha256HexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (s *Safety) appendEvidence(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok || !s.publisherReady(writer) {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload evidenceRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if !safetyIDPattern.MatchString(payload.EvidenceID) || payload.Description == "" ||
		!sha256HexPattern.MatchString(payload.ContentHash) {
		writeError(writer, http.StatusBadRequest, "evidence id, description and a lowercase-hex sha256 contentHash are required")
		return
	}
	row, err := s.Store.AppendEvidence(request.Context(), principal.TenantID, store.EvidenceRow{
		EvidenceID:  payload.EvidenceID,
		CaseID:      id,
		Description: payload.Description,
		ContentHash: payload.ContentHash,
		CollectedBy: principal.Subject,
	})
	if err != nil {
		safetyStoreError(writer, err, "evidence append")
		return
	}
	caseRow, err := s.Store.GetInvestigationCase(request.Context(), principal.TenantID, id)
	if err != nil {
		safetyStoreError(writer, err, "investigation case")
		return
	}
	s.publishInvestigation(writer, request, caseRow, principal.Subject, "evidence", row.EvidenceID, row.ChainHash)
	if writer.Header().Get("X-Safety-Publish-Failed") != "" {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"evidence": row})
}

func (s *Safety) listEvidence(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	rows, err := s.Store.ListEvidence(request.Context(), principal.TenantID, id)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "evidence query failed")
		return
	}
	valid, err := s.Store.VerifyEvidenceChain(request.Context(), principal.TenantID, id)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "evidence chain verification failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"evidence": rows, "chainValid": valid})
}

type findingRequest struct {
	FindingID string `json:"findingId"`
	Finding   string `json:"finding"`
}

func (s *Safety) addFinding(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload findingRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if !safetyIDPattern.MatchString(payload.FindingID) || payload.Finding == "" {
		writeError(writer, http.StatusBadRequest, "finding id and finding text are required")
		return
	}
	row, err := s.Store.AddFinding(request.Context(), principal.TenantID, store.FindingRow{
		FindingID: payload.FindingID,
		CaseID:    id,
		Finding:   payload.Finding,
		CreatedBy: principal.Subject,
	})
	if err != nil {
		safetyStoreError(writer, err, "finding")
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"finding": row})
}

func (s *Safety) listFindings(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	rows, err := s.Store.ListFindings(request.Context(), principal.TenantID, id)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "finding query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"findings": rows})
}

type recommendationRequest struct {
	RecommendationID string `json:"recommendationId"`
	Recommendation   string `json:"recommendation"`
}

func (s *Safety) addRecommendation(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload recommendationRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	if !safetyIDPattern.MatchString(payload.RecommendationID) || payload.Recommendation == "" {
		writeError(writer, http.StatusBadRequest, "recommendation id and text are required")
		return
	}
	row, err := s.Store.AddRecommendation(request.Context(), principal.TenantID, store.RecommendationRow{
		RecommendationID: payload.RecommendationID,
		CaseID:           id,
		Recommendation:   payload.Recommendation,
		CreatedBy:        principal.Subject,
	})
	if err != nil {
		safetyStoreError(writer, err, "recommendation")
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"recommendation": row})
}

func (s *Safety) listRecommendations(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	rows, err := s.Store.ListRecommendations(request.Context(), principal.TenantID, id)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "recommendation query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"recommendations": rows})
}

func (s *Safety) decideRecommendation(writer http.ResponseWriter, request *http.Request) {
	principal, ok := s.safetyPrincipal(writer, request)
	if !ok {
		return
	}
	id, ok := safetyPathID(writer, request)
	if !ok {
		return
	}
	var payload transitionRequest
	if !decodeBody(writer, request, &payload) {
		return
	}
	row, err := s.Store.DecideRecommendation(request.Context(), principal.TenantID, id,
		principal.Subject, payload.Action)
	if err != nil {
		safetyStoreError(writer, err, "recommendation transition")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"recommendation": row})
}
