// The /v1/mrv REST boundary: operator intake (fuel reports, voyages,
// monitoring plans), the verification workflow, annual-report compile and
// Statement of Compliance issuance. PBAC roles: mrv-reporter (operator),
// mrv-verifier, mrv-flag-admin, mrv-reader. Spans cover intake, estimate,
// compile and verify; telemetry-off boots and serves identically.
package mrv

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
)

// Role contract (phase-8 spec §4.1).
const (
	RoleReporter  = "mrv-reporter"
	RoleVerifier  = "mrv-verifier"
	RoleFlagAdmin = "mrv-flag-admin"
	RoleReader    = "mrv-reader"
)

// Server wires the REST handlers onto the MRV service.
type Server struct {
	service *Service
	tracer  trace.Tracer
}

// NewServer validates the wiring fail-closed.
func NewServer(service *Service) (*Server, error) {
	if service == nil {
		return nil, errors.New("mrv service is required")
	}
	return &Server{service: service, tracer: otel.Tracer("github.com/munisp/blueeconomy-geo-service/internal/mrv")}, nil
}

// Handler builds the authenticated route tree.
func (server *Server) Handler(authenticator auth.Authenticator) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/mrv/ships",
		auth.RequireRoles(http.HandlerFunc(server.registerShip), RoleFlagAdmin))
	mux.Handle("PUT /v1/mrv/ships/{imo}/monitoring-plans",
		auth.RequireRoles(http.HandlerFunc(server.putMonitoringPlan), RoleReporter))
	mux.Handle("POST /v1/mrv/monitoring-plans/{id}/confirm",
		auth.RequireRoles(http.HandlerFunc(server.confirmMonitoringPlan), RoleVerifier))
	mux.Handle("POST /v1/mrv/ships/{imo}/fuel-reports",
		auth.RequireRoles(http.HandlerFunc(server.submitFuelReport), RoleReporter))
	mux.Handle("POST /v1/mrv/ships/{imo}/voyages",
		auth.RequireRoles(http.HandlerFunc(server.recordVoyage), RoleReporter))
	mux.Handle("GET /v1/mrv/ships/{imo}/activity-estimate",
		auth.RequireRoles(http.HandlerFunc(server.activityEstimate), RoleReporter, RoleVerifier))
	mux.Handle("POST /v1/mrv/reports/annual/{imo}/{year}/compile",
		auth.RequireRoles(http.HandlerFunc(server.compileAnnual), RoleReporter))
	// NOTE: the spec's "{id}:submit" custom-verb form is not expressible in
	// net/http ServeMux wildcards; the resource path form is used.
	mux.Handle("POST /v1/mrv/reports/annual/{id}/submit",
		auth.RequireRoles(http.HandlerFunc(server.submitAnnual), RoleReporter))
	mux.Handle("POST /v1/mrv/reports/annual/{id}/decisions",
		auth.RequireRoles(http.HandlerFunc(server.recordDecision), RoleVerifier))
	mux.Handle("POST /v1/mrv/reports/annual/{id}/soc",
		auth.RequireRoles(http.HandlerFunc(server.issueSoC), RoleFlagAdmin))
	mux.Handle("GET /v1/mrv/ships/{imo}/cii/{year}",
		auth.RequireRoles(http.HandlerFunc(server.getCII), RoleReader, RoleReporter, RoleVerifier, RoleFlagAdmin))
	mux.Handle("GET /v1/mrv/reports/annual/{id}/artifact",
		auth.RequireRoles(http.HandlerFunc(server.getArtifact), RoleReader, RoleVerifier, RoleFlagAdmin))
	// The factor table with citations is public reference data: any
	// authenticated platform role may read it.
	mux.Handle("GET /v1/mrv/factors",
		auth.RequireRoles(http.HandlerFunc(server.listFactors),
			RoleReader, RoleReporter, RoleVerifier, RoleFlagAdmin))
	// Detailed operational status (CII config, deadlines) is authenticated
	// reference data; the public /healthz below carries only {"status":"ok"}.
	mux.Handle("GET /v1/status",
		auth.RequireRoles(http.HandlerFunc(server.status),
			RoleReader, RoleReporter, RoleVerifier, RoleFlagAdmin))
	outer := http.NewServeMux()
	outer.HandleFunc("GET /healthz", server.healthz)
	// /metrics exposes operational internals; it is authenticated like
	// every /v1 route, never anonymous.
	outer.Handle("GET /metrics", auth.Middleware(authenticator, http.HandlerFunc(
		func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "text/plain; version=0.0.4")
			server.service.Metrics.WritePrometheus(writer)
		})))
	outer.Handle("/", auth.Middleware(authenticator, mux))
	return securityHeaders(outer)
}

// securityHeaders sets the platform HTTP security headers on every response
// (HSTS, nosniff, frame deny, no-referrer), including error responses.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "DENY")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(writer, request)
	})
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

// clearedLabels renders every ladder label the principal's clearance covers.
func clearedLabels(clearance string) []string {
	label, err := sign.ParseClassification(clearance)
	if err != nil {
		return nil
	}
	out := make([]string, 0, 5)
	for _, candidate := range []string{"PUBLIC", "INTERNAL", "RESTRICTED", "CONFIDENTIAL", "SECRET"} {
		if label.Covers(sign.MustClassification(sign.Classification(candidate))) {
			out = append(out, candidate)
		}
	}
	return out
}

// ---------------------------------------------------------------------
// Ships

type registerShipRequest struct {
	ImoNumber            string  `json:"imoNumber"`
	MMSI                 string  `json:"mmsi"`
	ShipName             string  `json:"shipName"`
	GT                   uint32  `json:"gt"`
	DWT                  *uint32 `json:"dwt"`
	ShipType             string  `json:"shipType"`
	FlagState            string  `json:"flagState"`
	InternationalVoyages bool    `json:"internationalVoyages"`
}

func (server *Server) registerShip(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var payload registerShipRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	ship, err := server.service.RegisterShip(request.Context(), principal.Subject, Ship{
		ImoNumber: payload.ImoNumber, MMSI: payload.MMSI, ShipName: payload.ShipName,
		GT: payload.GT, DWT: payload.DWT, ShipType: strings.ToUpper(strings.TrimSpace(payload.ShipType)),
		FlagState:            strings.ToUpper(strings.TrimSpace(payload.FlagState)),
		InternationalVoyages: payload.InternationalVoyages,
	})
	if err != nil {
		writeError(writer, http.StatusBadRequest, "ship registration failed: "+err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, ship)
}

// ---------------------------------------------------------------------
// Monitoring plans

type putPlanRequest struct {
	Methods    map[string]string `json:"methods"`
	FuelGrades []string          `json:"fuelGrades"`
}

func (server *Server) putMonitoringPlan(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var payload putPlanRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	plan, err := server.service.PutMonitoringPlan(request.Context(), principal.Subject, request.PathValue("imo"),
		payload.Methods, payload.FuelGrades)
	switch {
	case errors.Is(err, ErrShipNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(writer, http.StatusBadRequest, "monitoring plan registration failed: "+err.Error())
	default:
		writeJSON(writer, http.StatusCreated, plan)
	}
}

func (server *Server) confirmMonitoringPlan(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	plan, err := server.service.ConfirmMonitoringPlan(request.Context(), principal.Subject, request.PathValue("id"))
	switch {
	case errors.Is(err, ErrPlanNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrMakerCheckerConflict), errors.Is(err, ErrPlanState):
		writeError(writer, http.StatusConflict, err.Error())
	case err != nil:
		writeError(writer, http.StatusInternalServerError, "plan confirmation failed")
	default:
		writeJSON(writer, http.StatusOK, plan)
	}
}

// ---------------------------------------------------------------------
// Fuel reports

type fuelReportRequest struct {
	PeriodFrom           time.Time       `json:"periodFrom"`
	PeriodTo             time.Time       `json:"periodTo"`
	Consumer             string          `json:"consumer"`
	FuelGrade            string          `json:"fuelGrade"`
	Method               string          `json:"method"`
	FuelTonnesMilli      U64             `json:"fuelTonnesMilli"`
	DistanceNmMilli      *U64            `json:"distanceNmMilli"`
	HoursUnderwayMinutes *U64            `json:"hoursUnderwayMinutes"`
	BdnReference         string          `json:"bdnReference"`
	Evidence             json.RawMessage `json:"evidence"`
}

func (server *Server) submitFuelReport(writer http.ResponseWriter, request *http.Request) {
	ctx, span := server.tracer.Start(request.Context(), "mrv.intake.fuel-report")
	defer span.End()
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var payload fuelReportRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	report := FuelReport{
		PeriodFrom: payload.PeriodFrom, PeriodTo: payload.PeriodTo, Consumer: payload.Consumer,
		FuelGrade: payload.FuelGrade, Method: payload.Method, FuelTonnesMilli: uint64(payload.FuelTonnesMilli),
		BdnRef: payload.BdnReference, Evidence: payload.Evidence,
	}
	if payload.DistanceNmMilli != nil {
		value := uint64(*payload.DistanceNmMilli)
		report.DistanceNmMilli = &value
	}
	if payload.HoursUnderwayMinutes != nil {
		value := uint64(*payload.HoursUnderwayMinutes)
		report.HoursUnderwayMinutes = &value
	}
	stored, replayed, err := server.service.SubmitFuelReport(ctx, principal.Subject, request.PathValue("imo"),
		request.Header.Get("Idempotency-Key"), report)
	switch {
	case errors.Is(err, ErrIdempotencyKeyNeeded):
		server.metricFuelReport("rejected")
		writeError(writer, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrIdempotencyConflict):
		server.metricFuelReport("conflict")
		writeError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, ErrShipNotFound):
		server.metricFuelReport("rejected")
		writeError(writer, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrNoConfirmedPlan), errors.Is(err, ErrFactorUnavailable):
		server.metricFuelReport("rejected")
		writeError(writer, http.StatusUnprocessableEntity, err.Error())
	case err != nil:
		server.metricFuelReport("rejected")
		writeError(writer, http.StatusBadRequest, "fuel report intake failed: "+err.Error())
	default:
		span.SetAttributes(attribute.Bool("mrv.replay", replayed))
		server.metricFuelReport("accepted")
		status := http.StatusCreated
		if replayed {
			status = http.StatusOK
		}
		writeJSON(writer, status, stored)
	}
}

func (server *Server) metricFuelReport(result string) {
	server.service.Metrics.Inc("mrv_fuel_reports_total", map[string]string{"result": result})
}

// ---------------------------------------------------------------------
// Voyages

type voyageRequest struct {
	BospAt               *time.Time `json:"bospAt"`
	BospPort             string     `json:"bospPortCode"`
	EospAt               *time.Time `json:"eospAt"`
	EospPort             string     `json:"eospPortCode"`
	CargoTonnesMilli     *U64       `json:"cargoTonnesMilli"`
	LadenDistanceNmMilli *U64       `json:"ladenDistanceNmMilli"`
}

func (server *Server) recordVoyage(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var payload voyageRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	voyage := Voyage{
		BospAt: payload.BospAt, BospPort: payload.BospPort, EospAt: payload.EospAt, EospPort: payload.EospPort,
	}
	if payload.CargoTonnesMilli != nil {
		value := uint64(*payload.CargoTonnesMilli)
		voyage.CargoTonnesMilli = &value
	}
	if payload.LadenDistanceNmMilli != nil {
		value := uint64(*payload.LadenDistanceNmMilli)
		voyage.LadenDistanceNmMilli = &value
	}
	stored, err := server.service.RecordVoyage(request.Context(), principal.Subject, request.PathValue("imo"), voyage)
	switch {
	case errors.Is(err, ErrShipNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(writer, http.StatusBadRequest, "voyage recording failed: "+err.Error())
	default:
		writeJSON(writer, http.StatusCreated, stored)
	}
}

// ---------------------------------------------------------------------
// Activity estimate

func (server *Server) activityEstimate(writer http.ResponseWriter, request *http.Request) {
	ctx, span := server.tracer.Start(request.Context(), "mrv.estimate.activity")
	defer span.End()
	principal, ok := principalOrFail(writer, request)
	if !ok {
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
	estimate, err := server.service.EstimateActivityForShip(ctx, principal.Subject, request.PathValue("imo"),
		from, to, clearedLabels(principal.Clearance))
	switch {
	case errors.Is(err, ErrShipNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case err != nil:
		writeError(writer, http.StatusBadRequest, "activity estimate failed: "+err.Error())
	default:
		span.SetAttributes(attribute.Bool("mrv.insufficient_coverage", estimate.InsufficientCoverage))
		writeJSON(writer, http.StatusOK, estimate)
	}
}

// ---------------------------------------------------------------------
// Annual reports, decisions, SoC

func (server *Server) compileAnnual(writer http.ResponseWriter, request *http.Request) {
	ctx, span := server.tracer.Start(request.Context(), "mrv.report.compile")
	defer span.End()
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	year, err := strconv.Atoi(request.PathValue("year"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "year must be a calendar year")
		return
	}
	report, err := server.service.CompileAnnualReport(ctx, principal.Subject, request.PathValue("imo"), year)
	switch {
	case errors.Is(err, ErrShipNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrReportState):
		writeError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, ErrFactorUnavailable):
		writeError(writer, http.StatusUnprocessableEntity, err.Error())
	case err != nil:
		writeError(writer, http.StatusBadRequest, "annual report compile failed: "+err.Error())
	default:
		writeJSON(writer, http.StatusCreated, report)
	}
}

func (server *Server) submitAnnual(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	report, err := server.service.SubmitAnnualReport(request.Context(), principal.Subject, request.PathValue("id"))
	switch {
	case errors.Is(err, ErrReportNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrReportState):
		writeError(writer, http.StatusConflict, err.Error())
	case err != nil:
		writeError(writer, http.StatusBadRequest, "annual report submit failed: "+err.Error())
	default:
		writeJSON(writer, http.StatusOK, report)
	}
}

type decisionRequest struct {
	Decision string `json:"decision"`
	Reason   string `json:"reason"`
}

func (server *Server) recordDecision(writer http.ResponseWriter, request *http.Request) {
	ctx, span := server.tracer.Start(request.Context(), "mrv.verify.decision")
	defer span.End()
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var payload decisionRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	verification, crosscheck, err := server.service.RecordDecision(ctx, principal.Subject,
		request.PathValue("id"), payload.Decision, payload.Reason)
	switch {
	case errors.Is(err, ErrReportNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrReportState), errors.Is(err, ErrMakerCheckerConflict):
		writeError(writer, http.StatusConflict, err.Error())
	case err != nil:
		writeError(writer, http.StatusBadRequest, "verification decision failed: "+err.Error())
	default:
		span.SetAttributes(attribute.String("mrv.decision", verification.Decision),
			attribute.String("mrv.ais_crosscheck", string(crosscheck)))
		decisionLabel := map[string]string{
			DecisionVerify: "verify", DecisionReject: "reject", DecisionClarify: "clarify",
		}[verification.Decision]
		server.service.Metrics.Inc("mrv_verification_decisions_total", map[string]string{"decision": decisionLabel})
		server.service.Metrics.Inc("mrv_ais_crosscheck_total", map[string]string{"result": string(crosscheck)})
		writeJSON(writer, http.StatusCreated, verification)
	}
}

func (server *Server) issueSoC(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	socID, artifactSha256, err := server.service.IssueSoC(request.Context(), principal.Subject, request.PathValue("id"))
	switch {
	case errors.Is(err, ErrReportNotFound):
		writeError(writer, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrReportState), errors.Is(err, ErrSoCExists):
		writeError(writer, http.StatusConflict, err.Error())
	case err != nil:
		writeError(writer, http.StatusInternalServerError, "statement of compliance issuance failed")
	default:
		server.service.Metrics.Inc("mrv_soc_issued_total", nil)
		writeJSON(writer, http.StatusCreated, map[string]any{
			"socId": socID, "artifactSha256": artifactSha256,
			"lateIssuance": PastDeadline(server.service.Deadlines.SoCIssuance, time.Now().UTC()),
		})
	}
}

func (server *Server) getCII(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	year, err := strconv.Atoi(request.PathValue("year"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "year must be a calendar year")
		return
	}
	var report AnnualReport
	var ciiRating *string
	err = server.service.withActor(request.Context(), principal.Subject, func(tx pgx.Tx) error {
		return tx.QueryRow(request.Context(), `SELECT report_id, imo_number, calendar_year, attained_cii_nano,
			required_cii_nano, cii_rating, state FROM mrv_annual_reports
			WHERE imo_number = $1 AND calendar_year = $2`, request.PathValue("imo"), year).
			Scan(&report.ReportID, &report.ImoNumber, &report.CalendarYear, &report.AttainedCiiNano,
				&report.RequiredCiiNano, &ciiRating, &report.State)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(writer, http.StatusNotFound, "no annual report for this ship and year")
		return
	}
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "cii query failed")
		return
	}
	response := map[string]any{
		"imoNumber": report.ImoNumber, "calendarYear": report.CalendarYear, "reportState": report.State,
		"ciiConfig": server.service.CII.Summary(),
	}
	if report.AttainedCiiNano != nil && report.RequiredCiiNano != nil && ciiRating != nil {
		response["attainedCiiNano"] = strconv.FormatUint(*report.AttainedCiiNano, 10)
		response["requiredCiiNano"] = strconv.FormatUint(*report.RequiredCiiNano, 10)
		response["ciiRating"] = *ciiRating
	} else {
		// Honest UNAVAILABLE: CII was not computable from approved config.
		response["cii"] = "NOT_COMPUTABLE"
	}
	writeJSON(writer, http.StatusOK, response)
}

func (server *Server) getArtifact(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	artifact, sum, err := server.service.GetArtifact(request.Context(), principal.Subject, request.PathValue("id"))
	switch {
	case errors.Is(err, ErrReportNotFound):
		writeError(writer, http.StatusNotFound, "no signed artifact for this report")
	case err != nil:
		writeError(writer, http.StatusInternalServerError, "artifact query failed")
	default:
		writeJSON(writer, http.StatusOK, map[string]any{
			"reportId": request.PathValue("id"), "artifactSha256": sum, "artifact": artifact,
		})
	}
}

func (server *Server) listFactors(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var factors []Factor
	err := server.service.withActor(request.Context(), principal.Subject, func(tx pgx.Tx) error {
		var err error
		factors, err = ListFactors(request.Context(), tx)
		return err
	})
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "factor query failed")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"factors": factors})
}

// healthz is a minimal public liveness probe: it reports only {"status":"ok"}
// and never leaks configuration internals.
func (server *Server) healthz(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{"status": "ok"})
}

// status reports mode and dependency posture truthfully to authenticated
// principals (see GET /v1/status).
func (server *Server) status(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"status":    "ok",
		"ciiConfig": server.service.CII.Summary(),
		"deadlines": server.service.Deadlines,
	})
}

func writeJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]any{"error": message})
}
