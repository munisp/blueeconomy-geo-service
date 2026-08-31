// Event resource payloads for the mrv.*.v1 contracts. Field names and
// fixed-point conventions mirror proto/blueeconomy/contracts/v1/mrv.proto:
// uint64 quantities render as proto-JSON decimal strings (U64), optional
// quantities are pointers, enums render as their canonical short wire
// values. Floating-point quantities are prohibited.
package mrv

import "time"

// FuelReportResource is the mrv.fuel-report.v1 payload
// (MrvFuelReportRecorded; FHIR projection: Observation, code = fuel grade +
// consumer).
type FuelReportResource struct {
	ReportID             string    `json:"reportId"`
	ImoNumber            string    `json:"imoNumber"`
	ExternalReference    string    `json:"externalReference"`
	PeriodFrom           time.Time `json:"periodFrom"`
	PeriodTo             time.Time `json:"periodTo"`
	Consumer             string    `json:"consumer"`
	FuelGrade            string    `json:"fuelGrade"`
	Method               string    `json:"method"`
	FuelTonnesMilli      U64       `json:"fuelTonnesMilli"`
	DistanceNmMilli      *U64      `json:"distanceNmMilli,omitempty"`
	HoursUnderwayMinutes *U64      `json:"hoursUnderwayMinutes,omitempty"`
	BdnReference         string    `json:"bdnReference,omitempty"`
	EvidenceDigestSha256 string    `json:"evidenceDigestSha256"`
	ReportedAt           time.Time `json:"reportedAt"`
}

// VoyageResource is the mrv.voyage.v1 payload (MrvVoyageRecorded).
type VoyageResource struct {
	VoyageID                     string     `json:"voyageId"`
	ImoNumber                    string     `json:"imoNumber"`
	Source                       string     `json:"source"`
	BospAt                       *time.Time `json:"bospAt,omitempty"`
	BospPortCode                 string     `json:"bospPortCode,omitempty"`
	EospAt                       *time.Time `json:"eospAt,omitempty"`
	EospPortCode                 string     `json:"eospPortCode,omitempty"`
	CargoTonnesMilli             *U64       `json:"cargoTonnesMilli,omitempty"`
	LadenDistanceNmMilli         *U64       `json:"ladenDistanceNmMilli,omitempty"`
	GeofenceEvidenceDigestSha256 []string   `json:"geofenceEvidenceDigestSha256"`
	RecordedAt                   time.Time  `json:"recordedAt"`
}

// VerificationResource is the mrv.verification.v1 payload
// (MrvVerificationRecorded; FHIR projection: Provenance).
type VerificationResource struct {
	VerificationID            string    `json:"verificationId"`
	AnnualReportID            string    `json:"annualReportId"`
	Decision                  string    `json:"decision"`
	ReasonCode                string    `json:"reasonCode"`
	AISCrosscheckDigestSha256 string    `json:"aisCrosscheckDigestSha256,omitempty"`
	DecidedAt                 time.Time `json:"decidedAt"`
}

// EmissionsAnnualResource is the mrv.emissions-annual.v1 payload
// (MrvEmissionsAnnualReportSubmitted; FHIR projection: DocumentReference).
type EmissionsAnnualResource struct {
	AnnualReportID        string    `json:"annualReportId"`
	ImoNumber             string    `json:"imoNumber"`
	CalendarYear          uint32    `json:"calendarYear"`
	State                 string    `json:"state"`
	CO2TonnesMilli        U64       `json:"co2TonnesMilli"`
	DistanceNmMilli       U64       `json:"distanceNmMilli"`
	HoursUnderwayMinutes  U64       `json:"hoursUnderwayMinutes"`
	FactorSetDigestSha256 string    `json:"factorSetDigestSha256"`
	AttainedCiiNano       *U64      `json:"attainedCiiNano,omitempty"`
	RequiredCiiNano       *U64      `json:"requiredCiiNano,omitempty"`
	CiiRating             string    `json:"ciiRating,omitempty"`
	SubmittedAt           time.Time `json:"submittedAt"`
}

// StatementOfComplianceResource is the mrv.soc.v1 payload
// (MrvStatementOfComplianceIssued).
type StatementOfComplianceResource struct {
	SocID                string    `json:"socId"`
	AnnualReportID       string    `json:"annualReportId"`
	ImoNumber            string    `json:"imoNumber"`
	CalendarYear         uint32    `json:"calendarYear"`
	ArtifactDigestSha256 string    `json:"artifactDigestSha256"`
	IssuedAt             time.Time `json:"issuedAt"`
}

// ActivityEstimateResource is the mrv.activity-estimate.v1 payload
// (MrvActivityEstimateComputed).
type ActivityEstimateResource struct {
	EstimateID           string    `json:"estimateId"`
	ImoNumber            string    `json:"imoNumber"`
	Mmsi                 string    `json:"mmsi"`
	PeriodFrom           time.Time `json:"periodFrom"`
	PeriodTo             time.Time `json:"periodTo"`
	DistanceNmMilli      U64       `json:"distanceNmMilli"`
	HoursUnderwayMinutes U64       `json:"hoursUnderwayMinutes"`
	InsufficientCoverage bool      `json:"insufficientCoverage"`
	InputDigestSha256    string    `json:"inputDigestSha256"`
	ComputedAt           time.Time `json:"computedAt"`
}
