package connectors

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// AppReportHandler is the Tier-0 mobile outbox flush endpoint. Position
// reports dedupe on (reporter_id, outbox_id) — an exact replay returns 200
// idempotent, a conflicting reuse of an outbox_id returns 409.
type AppReportHandler struct {
	Pipeline *Pipeline
}

// appReportRequest is the flush payload. The device clock reading
// (recordedAt) is untrusted and reconciled against the server receipt time
// carried as the envelope occurredAt.
type appReportRequest struct {
	ReporterID      string `json:"reporterId"`
	VesselReference string `json:"vesselReference"`
	LatitudeMicros  int32  `json:"latitudeMicros"`
	LongitudeMicros int32  `json:"longitudeMicros"`
	AccuracyM       uint32 `json:"accuracyM"`
	// SpeedMillimetresPerSecond is optional device-reported speed.
	SpeedMillimetresPerSecond *uint32 `json:"speedMillimetresPerSecond,omitempty"`
	RecordedAt                string  `json:"recordedAt"`
	OutboxID                  string  `json:"outboxId"`
	// SOS, when present, makes the request a distress alert (SAFETY
	// priority, RESTRICTED minimum classification).
	SOS *struct {
		FreeText string `json:"freeText"`
	} `json:"sos,omitempty"`
}

// RegisterRoutes mounts the flush endpoint.
func (handler *AppReportHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("POST /v1/geo/app-reports", auth.RequireRoles(http.HandlerFunc(handler.serve), "geo-app-reporter"))
}

// serve handles one flush request.
func (handler *AppReportHandler) serve(writer http.ResponseWriter, request *http.Request) {
	if handler.Pipeline == nil {
		writeAppError(writer, http.StatusServiceUnavailable, "pipeline unavailable")
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, 1<<16))
	if err != nil {
		writeAppError(writer, http.StatusBadRequest, "request body unreadable")
		return
	}
	var payload appReportRequest
	if err := json.Unmarshal(body, &payload); err != nil {
		writeAppError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	if err := validateAppReport(payload); err != nil {
		writeAppError(writer, http.StatusBadRequest, err.Error())
		return
	}
	recordedAt, err := time.Parse(time.RFC3339, payload.RecordedAt)
	if err != nil {
		writeAppError(writer, http.StatusBadRequest, "recordedAt must be RFC 3339")
		return
	}
	receivedAt := time.Now().UTC()
	if payload.SOS != nil {
		handler.serveSOS(writer, request, payload, recordedAt, receivedAt)
		return
	}
	reportID := "apr-" + uuid.NewString()
	report := store.AppReport{
		PositionReportID:          reportID,
		ReporterID:                payload.ReporterID,
		VesselReference:           payload.VesselReference,
		LatitudeMicros:            payload.LatitudeMicros,
		LongitudeMicros:           payload.LongitudeMicros,
		AccuracyM:                 payload.AccuracyM,
		SpeedMillimetresPerSecond: payload.SpeedMillimetresPerSecond,
		RecordedAt:                recordedAt,
		OutboxID:                  payload.OutboxID,
		Classification:            string(sign.ClassificationPublic),
	}
	inserted, err := handler.Pipeline.Store.InsertAppReport(request.Context(), report)
	if errors.Is(err, store.ErrDuplicateOutbox) {
		writeAppError(writer, http.StatusConflict, "outbox_id already used with a different payload")
		return
	}
	if err != nil {
		writeAppError(writer, http.StatusInternalServerError, "app report persistence failed")
		return
	}
	if !inserted {
		// Exact replay: idempotent success, no duplicate event emission.
		writeAppJSON(writer, http.StatusOK, map[string]any{
			"status":           "idempotent",
			"positionReportId": reportID,
		})
		return
	}
	// Project onto the hot path: ais_positions + latest_positions by
	// vessel_ref, then the signed geo.app-position-report.v1 envelope.
	speedKnots := speedMMSToMilliknots(payload.SpeedMillimetresPerSecond)
	if err := handler.Pipeline.HandlePosition(request.Context(), IngestPosition{
		Position: store.Position{
			PositionReportID:          reportID,
			VesselRef:                 payload.VesselReference,
			SourceClass:               sign.SourceAppReport,
			LatitudeMicros:            payload.LatitudeMicros,
			LongitudeMicros:           payload.LongitudeMicros,
			SpeedOverGroundMilliknots: speedKnots,
			PositionAccuracy:          sign.AccuracyUnspecified,
			Classification:            string(sign.ClassificationPublic),
			ObservedAt:                recordedAt,
		},
	}); err != nil {
		writeAppError(writer, http.StatusInternalServerError, "app report pipeline failed")
		return
	}
	eventPayload := sign.AppPositionReported{
		PositionReportID:          reportID,
		ReporterID:                payload.ReporterID,
		VesselReference:           payload.VesselReference,
		LatitudeMicros:            payload.LatitudeMicros,
		LongitudeMicros:           payload.LongitudeMicros,
		AccuracyM:                 payload.AccuracyM,
		SpeedMillimetresPerSecond: payload.SpeedMillimetresPerSecond,
		RecordedAt:                recordedAt.UTC(),
		OutboxID:                  payload.OutboxID,
		Classification:            string(sign.ClassificationPublic),
	}
	if err := handler.Pipeline.PublishSignedEnvelope(request.Context(), sign.EventAppPositionReport,
		reportID, eventPayload, receivedAt, eventPayload.Classification, nil); err != nil {
		writeAppError(writer, http.StatusInternalServerError, "app report publication failed")
		return
	}
	writeAppJSON(writer, http.StatusOK, map[string]any{
		"status":           "accepted",
		"positionReportId": reportID,
	})
}

// serveSOS persists the alert and publishes the signed geo.sos.v1 envelope
// immediately with SAFETY priority.
func (handler *AppReportHandler) serveSOS(writer http.ResponseWriter, request *http.Request, payload appReportRequest, recordedAt, receivedAt time.Time) {
	// Manual span around SOS classification + persistence + SAFETY-priority
	// publication: distress alerts are always traced end to end.
	ctx, span := tracer().Start(request.Context(), "geo.sos.classify",
		trace.WithAttributes(
			attribute.String("geo.sos.classification", string(sign.ClassificationRestricted)),
			attribute.String("geo.sos.priority", "SAFETY"),
		))
	defer span.End()
	request = request.WithContext(ctx)
	if payload.SOS.FreeText != "" && len(payload.SOS.FreeText) > 280 {
		writeAppError(writer, http.StatusBadRequest, "sos freeText exceeds 280 characters")
		return
	}
	alertID := "sos-" + uuid.NewString()
	alert := store.SOSAlert{
		SosAlertID:      alertID,
		ReporterID:      payload.ReporterID,
		VesselReference: payload.VesselReference,
		LatitudeMicros:  payload.LatitudeMicros,
		LongitudeMicros: payload.LongitudeMicros,
		RecordedAt:      recordedAt,
		OutboxID:        payload.OutboxID,
		FreeText:        payload.SOS.FreeText,
		Classification:  string(sign.ClassificationRestricted),
	}
	inserted, err := handler.Pipeline.Store.InsertSOSAlert(request.Context(), alert)
	if errors.Is(err, store.ErrDuplicateOutbox) {
		writeAppError(writer, http.StatusConflict, "outbox_id already used with a different payload")
		return
	}
	if err != nil {
		writeAppError(writer, http.StatusInternalServerError, "sos persistence failed")
		return
	}
	if !inserted {
		writeAppJSON(writer, http.StatusOK, map[string]any{
			"status":     "idempotent",
			"sosAlertId": alertID,
		})
		return
	}
	eventPayload := sign.SosAlertRaised{
		SosAlertID:      alertID,
		ReporterID:      payload.ReporterID,
		VesselReference: payload.VesselReference,
		LatitudeMicros:  payload.LatitudeMicros,
		LongitudeMicros: payload.LongitudeMicros,
		RecordedAt:      recordedAt.UTC(),
		OutboxID:        payload.OutboxID,
		FreeText:        payload.SOS.FreeText,
		Classification:  string(sign.ClassificationRestricted),
	}
	if err := handler.Pipeline.PublishSignedEnvelope(request.Context(), sign.EventSOS,
		alertID, eventPayload, receivedAt, eventPayload.Classification,
		map[string]string{"priority": "SAFETY"}); err != nil {
		writeAppError(writer, http.StatusInternalServerError, "sos publication failed")
		return
	}
	handler.Pipeline.Metrics.Inc("geo_sos_alerts_total", nil)
	writeAppJSON(writer, http.StatusOK, map[string]any{
		"status":     "accepted",
		"sosAlertId": alertID,
	})
}

// validateAppReport enforces the contract field constraints (fail closed).
func validateAppReport(payload appReportRequest) error {
	if strings.TrimSpace(payload.ReporterID) == "" || len(payload.ReporterID) > 128 {
		return errors.New("reporterId is required (max 128 chars)")
	}
	if strings.TrimSpace(payload.VesselReference) == "" || len(payload.VesselReference) > 128 {
		return errors.New("vesselReference is required (max 128 chars)")
	}
	if strings.TrimSpace(payload.OutboxID) == "" || len(payload.OutboxID) > 128 {
		return errors.New("outboxId is required (max 128 chars)")
	}
	if payload.LatitudeMicros > 90_000_000 || payload.LatitudeMicros < -90_000_000 {
		return errors.New("latitudeMicros outside ±90 degrees")
	}
	if payload.LongitudeMicros > 180_000_000 || payload.LongitudeMicros < -180_000_000 {
		return errors.New("longitudeMicros outside ±180 degrees")
	}
	if payload.LatitudeMicros == 0 && payload.LongitudeMicros == 0 {
		return errors.New("(0,0) null-island position is rejected")
	}
	if strings.TrimSpace(payload.RecordedAt) == "" {
		return errors.New("recordedAt is required")
	}
	return nil
}

// speedMMSToMilliknots converts device mm/s to milli-knots (kn = m/s ×
// 1943.8445), or nil when the device reported no speed.
func speedMMSToMilliknots(mms *uint32) *uint32 {
	if mms == nil {
		return nil
	}
	value := uint32((uint64(*mms)*19438445 + 5000000) / 10000000)
	return &value
}

func writeAppError(writer http.ResponseWriter, status int, message string) {
	writeAppJSON(writer, status, map[string]any{"error": message})
}

func writeAppJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}
