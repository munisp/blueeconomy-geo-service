// Event payload types for the geo.*.v1 contracts. Field names and fixed-point
// integer conventions mirror proto/blueeconomy/contracts/v1/geo.proto in
// blueeconomy-contracts: coordinates are micro-degrees, speeds milli-knots
// (or millimetres-per-second for app reports), courses/headings
// milli-degrees, draughts millimetres. Floating-point coordinates, speeds and
// draughts are prohibited.
package sign

import "time"

// Source classes (PositionSourceClass wire values).
const (
	SourceAIS        = "AIS"
	SourceGSMTracker = "GSM_TRACKER"
	SourceSatTracker = "SAT_TRACKER"
	SourceAppReport  = "APP_REPORT"
)

// Position accuracies (PositionAccuracy wire values).
const (
	AccuracyUnspecified = "UNSPECIFIED"
	AccuracyLow         = "LOW"
	AccuracyHigh        = "HIGH"
)

// Geofence transitions (GeofenceTransition wire values).
const (
	TransitionEnter = "ENTER"
	TransitionExit  = "EXIT"
)

// VesselPositionReported is the geo.vessel-position.v1 payload.
type VesselPositionReported struct {
	PositionReportID             string    `json:"positionReportId"`
	MMSI                         string    `json:"mmsi"`
	SourceClass                  string    `json:"sourceClass"`
	LatitudeMicros               int32     `json:"latitudeMicros"`
	LongitudeMicros              int32     `json:"longitudeMicros"`
	SpeedOverGroundMilliknots    uint32    `json:"speedOverGroundMilliknots"`
	CourseOverGroundMillidegrees uint32    `json:"courseOverGroundMillidegrees"`
	HeadingMillidegrees          *uint32   `json:"headingMillidegrees,omitempty"`
	NavStatus                    *int32    `json:"navStatus,omitempty"`
	PositionAccuracy             string    `json:"positionAccuracy"`
	ObservedAt                   time.Time `json:"observedAt"`
	ReceiverID                   string    `json:"receiverId"`
	AISMessageType               *int32    `json:"aisMessageType,omitempty"`
	Classification               string    `json:"classification"`
	IMO                          string    `json:"imo,omitempty"`
	Callsign                     string    `json:"callsign,omitempty"`
	ShipName                     string    `json:"shipName,omitempty"`
}

// VesselStaticReported is the geo.vessel-static.v1 payload (AIS types
// 5/19/24 or an equivalent registry update).
type VesselStaticReported struct {
	StaticReportID      string     `json:"staticReportId"`
	MMSI                string     `json:"mmsi"`
	IMO                 string     `json:"imo,omitempty"`
	Callsign            string     `json:"callsign,omitempty"`
	ShipName            string     `json:"shipName,omitempty"`
	ShipTypeCode        int32      `json:"shipTypeCode"`
	DimensionBowM       uint32     `json:"dimensionBowM"`
	DimensionSternM     uint32     `json:"dimensionSternM"`
	DimensionPortM      uint32     `json:"dimensionPortM"`
	DimensionStarboardM uint32     `json:"dimensionStarboardM"`
	DraughtMillimetres  uint32     `json:"draughtMillimetres"`
	Destination         string     `json:"destination,omitempty"`
	ETA                 *time.Time `json:"eta,omitempty"`
	EpfsType            string     `json:"epfsType"`
	ObservedAt          time.Time  `json:"observedAt"`
	SourceClass         string     `json:"sourceClass"`
	Classification      string     `json:"classification"`
}

// GeofenceEventRecorded is the geo.geofence-event.v1 payload. Exactly one of
// MMSI / TrackReference must be populated.
type GeofenceEventRecorded struct {
	GeofenceEventID string    `json:"geofenceEventId"`
	ZoneID          string    `json:"zoneId"`
	ZoneName        string    `json:"zoneName"`
	Event           string    `json:"event"`
	MMSI            string    `json:"mmsi,omitempty"`
	TrackReference  string    `json:"trackReference,omitempty"`
	LatitudeMicros  int32     `json:"latitudeMicros"`
	LongitudeMicros int32     `json:"longitudeMicros"`
	OccurredAt      time.Time `json:"occurredAt"`
	Classification  string    `json:"classification"`
}

// AppPositionReported is the geo.app-position-report.v1 payload.
type AppPositionReported struct {
	PositionReportID          string    `json:"positionReportId"`
	ReporterID                string    `json:"reporterId"`
	VesselReference           string    `json:"vesselReference"`
	LatitudeMicros            int32     `json:"latitudeMicros"`
	LongitudeMicros           int32     `json:"longitudeMicros"`
	AccuracyM                 uint32    `json:"accuracyM"`
	SpeedMillimetresPerSecond *uint32   `json:"speedMillimetresPerSecond,omitempty"`
	RecordedAt                time.Time `json:"recordedAt"`
	OutboxID                  string    `json:"outboxId"`
	Classification            string    `json:"classification"`
}

// SosAlertRaised is the geo.sos.v1 payload. Classification floor: RESTRICTED.
type SosAlertRaised struct {
	SosAlertID      string    `json:"sosAlertId"`
	ReporterID      string    `json:"reporterId"`
	VesselReference string    `json:"vesselReference"`
	LatitudeMicros  int32     `json:"latitudeMicros"`
	LongitudeMicros int32     `json:"longitudeMicros"`
	RecordedAt      time.Time `json:"recordedAt"`
	OutboxID        string    `json:"outboxId"`
	FreeText        string    `json:"freeText,omitempty"`
	Classification  string    `json:"classification"`
}

// SosAlertAcknowledged is the geo.sos-acknowledged.v1 payload: the acting
// principal (from verified token claims) took ownership of the alert.
// Classification floor: RESTRICTED (same as the alert it references).
type SosAlertAcknowledged struct {
	SosAlertID      string    `json:"sosAlertId"`
	ReporterID      string    `json:"reporterId"`
	VesselReference string    `json:"vesselReference"`
	AcknowledgedBy  string    `json:"acknowledgedBy"`
	AcknowledgedAt  time.Time `json:"acknowledgedAt"`
	Note            string    `json:"note,omitempty"`
	Classification  string    `json:"classification"`
}

// SosAlertResolved is the geo.sos-resolved.v1 payload: the acting principal
// closed the alert (from RAISED or ACKNOWLEDGED). Classification floor:
// RESTRICTED (same as the alert it references).
type SosAlertResolved struct {
	SosAlertID      string    `json:"sosAlertId"`
	ReporterID      string    `json:"reporterId"`
	VesselReference string    `json:"vesselReference"`
	ResolvedBy      string    `json:"resolvedBy"`
	ResolvedAt      time.Time `json:"resolvedAt"`
	Note            string    `json:"note,omitempty"`
	Classification  string    `json:"classification"`
}

// TrackPointPayload is one recorded position inside a track window (WP-10).
type TrackPointPayload struct {
	LatitudeMicros   int32     `json:"latitudeMicros"`
	LongitudeMicros  int32     `json:"longitudeMicros"`
	SogMilliknots    int32     `json:"speedOverGroundMilliknots"`
	ObservedAt       time.Time `json:"observedAt"`
}

// TrackGapPayload is a reported (never filled) track gap (WP-10).
type TrackGapPayload struct {
	Start             time.Time `json:"start"`
	End               time.Time `json:"end"`
	DurationSeconds   int64     `json:"durationSeconds"`
	FromLatitudeMicros  int32   `json:"fromLatitudeMicros"`
	FromLongitudeMicros int32   `json:"fromLongitudeMicros"`
	ToLatitudeMicros    int32   `json:"toLatitudeMicros"`
	ToLongitudeMicros   int32   `json:"toLongitudeMicros"`
}

// VesselTrackWindow is the geo.track-window.v1 payload (WP-10): recorded
// positions only — no synthesized or interpolated points.
type VesselTrackWindow struct {
	MMSI           string             `json:"mmsi"`
	WindowStart    time.Time          `json:"windowStart"`
	WindowEnd      time.Time          `json:"windowEnd"`
	Points         []TrackPointPayload `json:"points"`
	Gaps           []TrackGapPayload   `json:"gaps"`
	MaxGapSeconds  int64              `json:"maxGapSeconds"`
	Classification string             `json:"classification"`
}

// PortApproachEtaPayload is the geo.port-approach-eta.v1 payload (WP-10):
// the honest distance/recorded-speed ETA heuristic. EtaSeconds is negative
// when no honest estimate exists.
type PortApproachEtaPayload struct {
	MMSI               string    `json:"mmsi"`
	PortCode           string    `json:"portCode"`
	DistanceMeters     float64   `json:"distanceMeters"`
	EtaSeconds         int64     `json:"etaSeconds"`
	SpeedKnots         float64   `json:"speedKnots"`
	Confidence         string    `json:"confidence"`
	Explanation        string    `json:"explanation"`
	PositionObservedAt time.Time `json:"positionObservedAt"`
	Classification     string    `json:"classification"`
}
