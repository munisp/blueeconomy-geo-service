// Package connectors implements the ingest boundary: NMEA TCP/UDP listeners,
// the aisstream.io WebSocket client, the GT06/Concox tracker decoder and the
// Tier-0 app-report HTTP endpoint. All connectors feed one pipeline:
//
//	decode → validate (quarantine, never drop) → dedup → store (PostGIS)
//	  → geofence evaluate → sign envelope → publish (Kafka)
//
// Every connector is env-gated and fails closed when misconfigured. Raw
// device identifiers stay inside this boundary; events carry pseudonymous
// references only.
package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/munisp/blueeconomy-geo-service/internal/bus"
	"github.com/munisp/blueeconomy-geo-service/internal/dedup"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
	"github.com/munisp/blueeconomy-geo-service/internal/validate"
)

// Publisher is the Kafka boundary the pipeline writes to (bus.Producer in
// production; a recording stub in integration tests).
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
}

// Pipeline is the shared hot-path for every connector.
type Pipeline struct {
	Store     *store.Store
	Dedup     *dedup.Checker
	ZoneState *store.ZoneStateStore
	Producer  Publisher
	Signer    *sign.Signer
	Tracker   *validate.Tracker
	Metrics   *metrics.Registry
	Principal sign.Provenance
	// PublishRaw gates the ais.raw mirror topic (raw decoded frames).
	PublishRaw bool
}

// IngestPosition is a normalized, fixed-point position ready for validation.
type IngestPosition struct {
	Position   store.Position
	PayloadKey string // dedup payload hash
}

// IngestStatic is normalized static/voyage data.
type IngestStatic struct {
	Report store.StaticReport
}

// quarantineRecord is published to vessels.quarantine — suspect traffic is
// intelligence and is never dropped.
type quarantineRecord struct {
	QuarantineID  string         `json:"quarantineId"`
	Reason        string         `json:"reason"`
	Detail        string         `json:"detail"`
	SourceClass   string         `json:"sourceClass"`
	MMSI          string         `json:"mmsi,omitempty"`
	VesselRef     string         `json:"vesselRef,omitempty"`
	ReceiverID    string         `json:"receiverId,omitempty"`
	Report        store.Position `json:"report"`
	QuarantinedAt time.Time      `json:"quarantinedAt"`
}

// NewPipeline validates the wiring fail-closed.
func NewPipeline(pipeline Pipeline) (*Pipeline, error) {
	switch {
	case pipeline.Store == nil:
		return nil, errors.New("pipeline store is required")
	case pipeline.Dedup == nil:
		return nil, errors.New("pipeline dedup checker is required")
	case pipeline.ZoneState == nil:
		return nil, errors.New("pipeline zone state store is required")
	case pipeline.Producer == nil:
		return nil, errors.New("pipeline kafka producer is required")
	case pipeline.Signer == nil:
		return nil, errors.New("pipeline signer is required")
	case pipeline.Tracker == nil:
		return nil, errors.New("pipeline validation tracker is required")
	case pipeline.Metrics == nil:
		return nil, errors.New("pipeline metrics registry is required")
	case pipeline.Principal.PrincipalID == "" || pipeline.Principal.PrincipalRole == "":
		return nil, errors.New("pipeline provenance principal is required")
	}
	return &pipeline, nil
}

// HandlePosition runs one normalized position through the hot path. Store
// writes happen before publication so a broker outage never loses the
// record; a publication failure is returned to the caller (fail closed).
func (pipeline *Pipeline) HandlePosition(ctx context.Context, ingest IngestPosition) error {
	position := ingest.Position
	if position.PositionReportID == "" {
		position.PositionReportID = "pos-" + uuid.NewString()
	}
	pipeline.Metrics.Inc("geo_positions_received_total", map[string]string{"source_class": position.SourceClass})

	// 1. Stateless validation; rejects go to quarantine, never dropped.
	report := validate.PositionReport{
		MMSI:                         position.MMSI,
		SourceClass:                  position.SourceClass,
		LatitudeMicros:               position.LatitudeMicros,
		LongitudeMicros:              position.LongitudeMicros,
		SpeedOverGroundMilliknots:    derefUint32(position.SpeedOverGroundMilliknots),
		CourseOverGroundMillidegrees: derefUint32(position.CourseOverGroundMillidegrees),
		ObservedAt:                   position.ObservedAt,
		ReceiverID:                   position.ReceiverID,
	}
	findings := validate.StaticChecks(report)
	if validate.IsNigerianFlagged(position.MMSI) {
		pipeline.Metrics.Inc("geo_positions_nigeria_mid657_total", nil)
	}
	if len(findings) > 0 {
		for _, finding := range findings {
			pipeline.Metrics.Inc("geo_quarantine_total", map[string]string{"reason": finding.Reason})
			if err := pipeline.quarantine(ctx, position, finding); err != nil {
				return err
			}
		}
		return nil
	}

	// 2. Consecutive-report checks (impossible speed, same-MMSI bifurcation).
	if finding := pipeline.Tracker.CheckConsecutive(report); finding != nil {
		pipeline.Metrics.Inc("geo_quarantine_total", map[string]string{"reason": finding.Reason})
		return pipeline.quarantine(ctx, position, *finding)
	}

	// 3. Dedup inside the tumbling window; duplicates are absorbed, Redis
	// errors admit the report (fail closed toward delivery, never drop).
	if ingest.PayloadKey != "" {
		duplicate, err := pipeline.Dedup.IsDuplicate(ctx, position.MMSI, derefInt32(position.AISMessageType), ingest.PayloadKey)
		if err != nil {
			pipeline.Metrics.Inc("geo_dedup_errors_total", nil)
		} else if duplicate {
			pipeline.Metrics.Inc("geo_dedup_absorbed_total", map[string]string{"source_class": position.SourceClass})
			return nil
		}
	}

	// 4. Persist (partitioned hot table + latest upsert) before publishing.
	// Provision the observed day on demand: late-arriving AIS, store-forward
	// tracker flushes and replayed fixtures can carry observation times
	// outside the pre-provisioned today/tomorrow window, and inserts into an
	// unprovisioned day fail closed (no default partition).
	if err := pipeline.Store.EnsurePositionPartitions(ctx, position.ObservedAt); err != nil {
		return fmt.Errorf("provision partition for %s: %w", position.ObservedAt.UTC().Format("2006-01-02"), err)
	}
	if err := pipeline.Store.InsertPositions(ctx, []store.Position{position}); err != nil {
		return err
	}
	if err := pipeline.Store.UpsertLatestPosition(ctx, position); err != nil {
		return err
	}

	// 5. Geofence evaluation (Redis per-vessel zone state → signed events).
	events, err := pipeline.Store.EvaluateGeofences(ctx, pipeline.ZoneState, position, func() string {
		return "gfe-" + uuid.NewString()
	})
	if err != nil {
		return fmt.Errorf("geofence evaluation: %w", err)
	}
	for _, event := range events {
		if err := pipeline.publishGeofenceEvent(ctx, event); err != nil {
			return err
		}
	}

	// 6. Raw mirror + signed event publication.
	if pipeline.PublishRaw && position.SourceClass == sign.SourceAIS {
		raw, err := json.Marshal(position)
		if err != nil {
			return fmt.Errorf("encode ais.raw frame: %w", err)
		}
		if err := pipeline.Producer.Publish(ctx, bus.TopicAISRaw, position.MMSI, raw, map[string]string{
			"source-class": position.SourceClass,
			"receiver-id":  position.ReceiverID,
		}); err != nil {
			return err
		}
	}
	payload := sign.VesselPositionReported{
		PositionReportID:             position.PositionReportID,
		MMSI:                         position.MMSI,
		SourceClass:                  position.SourceClass,
		LatitudeMicros:               position.LatitudeMicros,
		LongitudeMicros:              position.LongitudeMicros,
		SpeedOverGroundMilliknots:    derefUint32(position.SpeedOverGroundMilliknots),
		CourseOverGroundMillidegrees: derefUint32(position.CourseOverGroundMillidegrees),
		HeadingMillidegrees:          position.HeadingMillidegrees,
		NavStatus:                    position.NavStatus,
		PositionAccuracy:             position.PositionAccuracy,
		ObservedAt:                   position.ObservedAt.UTC(),
		ReceiverID:                   position.ReceiverID,
		AISMessageType:               position.AISMessageType,
		Classification:               position.Classification,
		IMO:                          position.IMO,
		Callsign:                     position.Callsign,
		ShipName:                     position.ShipName,
	}
	if payload.PositionAccuracy == "" {
		payload.PositionAccuracy = sign.AccuracyUnspecified
	}
	if err := pipeline.publish(ctx, sign.EventVesselPosition, position.PositionReportID, payload, position.ObservedAt, position.Classification); err != nil {
		return err
	}
	pipeline.Metrics.Inc("geo_positions_published_total", map[string]string{"source_class": position.SourceClass})
	return nil
}

// HandleStatic applies SCD-2 static upsert then publishes geo.vessel-static.v1.
func (pipeline *Pipeline) HandleStatic(ctx context.Context, ingest IngestStatic) error {
	report := ingest.Report
	if report.StaticReportID == "" {
		report.StaticReportID = "sta-" + uuid.NewString()
	}
	if err := validate.CheckMMSI(report.MMSI); err != nil {
		pipeline.Metrics.Inc("geo_quarantine_total", map[string]string{"reason": validate.ReasonMMSIFormat})
		record := map[string]any{
			"quarantineId":  "qrn-" + uuid.NewString(),
			"reason":        validate.ReasonMMSIFormat,
			"detail":        err.Error(),
			"kind":          "static",
			"report":        report,
			"quarantinedAt": time.Now().UTC(),
		}
		raw, _ := json.Marshal(record)
		return pipeline.Producer.Publish(ctx, bus.TopicVesselQuarantine, report.MMSI, raw, nil)
	}
	if err := pipeline.Store.UpsertVesselStatic(ctx, report); err != nil {
		return err
	}
	payload := sign.VesselStaticReported{
		StaticReportID:      report.StaticReportID,
		MMSI:                report.MMSI,
		IMO:                 report.IMO,
		Callsign:            report.Callsign,
		ShipName:            report.ShipName,
		ShipTypeCode:        report.ShipTypeCode,
		DimensionBowM:       report.DimensionBowM,
		DimensionSternM:     report.DimensionSternM,
		DimensionPortM:      report.DimensionPortM,
		DimensionStarboardM: report.DimensionStarboardM,
		DraughtMillimetres:  report.DraughtMillimetres,
		Destination:         report.Destination,
		ETA:                 report.ETA,
		EpfsType:            report.EpfsType,
		ObservedAt:          report.ObservedAt.UTC(),
		SourceClass:         report.SourceClass,
		Classification:      report.Classification,
	}
	if payload.EpfsType == "" {
		payload.EpfsType = "UNSPECIFIED"
	}
	if err := pipeline.publish(ctx, sign.EventVesselStatic, report.StaticReportID, payload, report.ObservedAt, report.Classification); err != nil {
		return err
	}
	pipeline.Metrics.Inc("geo_static_published_total", nil)
	return nil
}

// publishGeofenceEvent signs and publishes one zone crossing.
func (pipeline *Pipeline) publishGeofenceEvent(ctx context.Context, event store.GeofenceEvent) error {
	payload := sign.GeofenceEventRecorded{
		GeofenceEventID: event.GeofenceEventID,
		ZoneID:          event.ZoneID,
		ZoneName:        event.ZoneName,
		Event:           event.Event,
		MMSI:            event.MMSI,
		TrackReference:  event.TrackReference,
		LatitudeMicros:  event.LatitudeMicros,
		LongitudeMicros: event.LongitudeMicros,
		OccurredAt:      event.OccurredAt.UTC(),
		Classification:  event.Classification,
	}
	if err := pipeline.publish(ctx, sign.EventGeofenceEvent, event.GeofenceEventID, payload, event.OccurredAt, event.Classification); err != nil {
		return err
	}
	pipeline.Metrics.Inc("geo_geofence_events_total", map[string]string{"event": event.Event})
	return nil
}

// publish builds the signed envelope and publishes it to vessels.events.
func (pipeline *Pipeline) publish(ctx context.Context, eventType, correlationID string, payload any, occurredAt time.Time, classification string) error {
	if _, err := sign.ParseClassification(classification); err != nil {
		return fmt.Errorf("refusing to publish %s: %w", eventType, err)
	}
	envelope, err := sign.NewEnvelope(eventType, correlationID, payload, occurredAt, pipeline.Principal, pipeline.Signer)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode %s envelope: %w", eventType, err)
	}
	key := envelopeKey(payload)
	if err := pipeline.Producer.Publish(ctx, bus.TopicVesselEvents, key, raw, map[string]string{
		"event-type":     eventType,
		"classification": string(envelope.Classification),
	}); err != nil {
		return err
	}
	return nil
}

// PublishSignedEnvelope publishes a pre-built domain payload (app reports,
// SOS) with an explicit priority header. SAFETY-priority SOS envelopes are
// published immediately by the app-report connector.
func (pipeline *Pipeline) PublishSignedEnvelope(ctx context.Context, eventType, correlationID string, payload any, occurredAt time.Time, classification string, headers map[string]string) error {
	if _, err := sign.ParseClassification(classification); err != nil {
		return fmt.Errorf("refusing to publish %s: %w", eventType, err)
	}
	envelope, err := sign.NewEnvelope(eventType, correlationID, payload, occurredAt, pipeline.Principal, pipeline.Signer)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode %s envelope: %w", eventType, err)
	}
	if headers == nil {
		headers = map[string]string{}
	}
	headers["event-type"] = eventType
	headers["classification"] = string(envelope.Classification)
	return pipeline.Producer.Publish(ctx, bus.TopicVesselEvents, envelopeKey(payload), raw, headers)
}

// envelopeKey chooses the partition key (MMSI preferred, else reporter ref).
func envelopeKey(payload any) string {
	switch typed := payload.(type) {
	case sign.VesselPositionReported:
		if typed.MMSI != "" {
			return typed.MMSI
		}
	case sign.VesselStaticReported:
		return typed.MMSI
	case sign.GeofenceEventRecorded:
		if typed.MMSI != "" {
			return typed.MMSI
		}
		return typed.TrackReference
	case sign.AppPositionReported:
		return typed.ReporterID
	case sign.SosAlertRaised:
		return typed.ReporterID
	case sign.SosAlertAcknowledged:
		return typed.ReporterID
	case sign.SosAlertResolved:
		return typed.ReporterID
	}
	return ""
}

// quarantine routes a rejected report to vessels.quarantine.
func (pipeline *Pipeline) quarantine(ctx context.Context, position store.Position, finding validate.Finding) error {
	record := quarantineRecord{
		QuarantineID:  "qrn-" + uuid.NewString(),
		Reason:        finding.Reason,
		Detail:        finding.Detail,
		SourceClass:   position.SourceClass,
		MMSI:          position.MMSI,
		VesselRef:     position.VesselRef,
		ReceiverID:    position.ReceiverID,
		Report:        position,
		QuarantinedAt: time.Now().UTC(),
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode quarantine record: %w", err)
	}
	key := position.MMSI
	if key == "" {
		key = position.VesselRef
	}
	return pipeline.Producer.Publish(ctx, bus.TopicVesselQuarantine, key, raw, map[string]string{
		"reason": finding.Reason,
	})
}

func derefUint32(value *uint32) uint32 {
	if value == nil {
		return 0
	}
	return *value
}

func derefInt32(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}
