// Package mrv implements the Phase-8 MRV emissions module boundary of
// geo-service: operator fuel/activity intake (IMO DCS record unit), the
// EU-MRV-compatible BOSP/EOSP voyage ledger, AIS-derived activity estimates
// as a verification cross-check, the annual-report compile/submit/verify
// workflow with maker <> checker enforcement, Statement of Compliance
// issuance, and the source-cited emission-factor registry.
//
// Non-fabrication doctrine (phase-8 spec §5):
//   - Emission factors resolve ONLY from source-cited mrv_emission_factors
//     rows; an unknown grade or gas fails closed (ErrFactorUnavailable) and
//     no estimate is produced. There is no default factor.
//   - CII parameters come ONLY from an operator-approved, versioned,
//     source-cited config document (MRV_CII_CONFIG_PATH); without one every
//     CII outcome is NOT_COMPUTABLE, never estimated.
//   - No unverified annual report may produce a Statement of Compliance.
//   - Quantities are fixed-point integers (milli-tonnes, milli-nautical
//     miles, whole minutes, nano-units); floats never cross the boundary.
package mrv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// ProducerName is the deployable name asserted on every mrv.*.v1 envelope
// and used as the JWS kid prefix ("blueeconomy-geo-service-mrv-<epoch>"),
// per the contracts fixtures (fixtures/mrv/*.json).
const ProducerName = "blueeconomy-geo-service-mrv"

// Contract event types and topics (proto/blueeconomy/contracts/v1/mrv.proto,
// docs/mrv-events.md). Fail-closed sets.
const (
	EventFuelReport       = "mrv.fuel-report.v1"
	EventVoyage           = "mrv.voyage.v1"
	EventVerification     = "mrv.verification.v1"
	EventEmissionsAnnual  = "mrv.emissions-annual.v1"
	EventSoC              = "mrv.soc.v1"
	EventActivityEstimate = "mrv.activity-estimate.v1"

	TopicFuelReports       = "mrv.fuel-reports"
	TopicVoyages           = "mrv.voyages"
	TopicVerifications     = "mrv.verifications"
	TopicAnnualReports     = "mrv.annual-reports"
	TopicSoC               = "mrv.soc"
	TopicActivityEstimates = "mrv.activity-estimates"
)

// eventContract binds each event type to its topic, its proto message (FHIR
// Any projection) and its contract classification.
type eventContract struct {
	topic          string
	resourceType   string
	classification string
}

var eventContracts = map[string]eventContract{
	EventFuelReport:       {TopicFuelReports, "MrvFuelReportRecorded", "CONFIDENTIAL"},
	EventVoyage:           {TopicVoyages, "MrvVoyageRecorded", "CONFIDENTIAL"},
	EventVerification:     {TopicVerifications, "MrvVerificationRecorded", "CONFIDENTIAL"},
	EventEmissionsAnnual:  {TopicAnnualReports, "MrvEmissionsAnnualReportSubmitted", "CONFIDENTIAL"},
	EventSoC:              {TopicSoC, "MrvStatementOfComplianceIssued", "INTERNAL"},
	EventActivityEstimate: {TopicActivityEstimates, "MrvActivityEstimateComputed", "INTERNAL"},
}

// Fail-closed domain errors.
var (
	ErrFactorUnavailable    = errors.New("no source-cited emission factor resolves for this fuel grade/gas (UNRATED: fail closed)")
	ErrShipNotFound         = errors.New("mrv ship not found")
	ErrPlanNotFound         = errors.New("mrv monitoring plan not found")
	ErrPlanState            = errors.New("mrv monitoring plan state does not admit this transition")
	ErrMakerCheckerConflict = errors.New("maker and checker must be distinct principals (four-eyes)")
	ErrIdempotencyKeyNeeded = errors.New("Idempotency-Key header is required")
	ErrIdempotencyConflict  = errors.New("idempotency key replay with a divergent payload")
	ErrReportNotFound       = errors.New("mrv annual report not found")
	ErrReportState          = errors.New("mrv annual report state does not admit this transition")
	ErrSoCExists            = errors.New("statement of compliance already issued for this report")
	ErrNoConfirmedPlan      = errors.New("ship has no CONFIRMED monitoring plan covering the reported method and fuel grade")
	ErrVoyageNotFound       = errors.New("mrv voyage not found")
)

// Fuel consumer types (MrvConsumerType wire values).
const (
	ConsumerMainEngine  = "MAIN_ENGINE"
	ConsumerAuxEngine   = "AUX_ENGINE"
	ConsumerBoiler      = "BOILER"
	ConsumerInertGas    = "INERT_GAS"
	ConsumerNotUnderWay = "NOT_UNDER_WAY"
)

// Monitoring methods (MrvMonitoringMethod wire values, EU MRV Annex I).
const (
	MethodA = "A" // bunker delivery notes
	MethodB = "B" // bunker fuel tank monitoring
	MethodC = "C" // flow meters
	MethodD = "D" // direct CO2 measurement
)

// Voyage sources (MrvVoyageSource wire values).
const (
	VoyageSourceOperator   = "OPERATOR"
	VoyageSourceAISDerived = "AIS_DERIVED"
	VoyageSourceReconciled = "RECONCILED"
)

// Annual report states (MrvReportState wire values).
const (
	ReportStateDraft          = "DRAFT"
	ReportStateSubmitted      = "SUBMITTED"
	ReportStateVerifierReview = "VERIFIER_REVIEW"
	ReportStateVerified       = "VERIFIED"
	ReportStateRejected       = "REJECTED"
)

// Verification decisions (MrvVerificationDecision wire values).
const (
	DecisionVerify  = "VERIFY"
	DecisionReject  = "REJECT"
	DecisionClarify = "REQUEST_CLARIFICATION"
)

// Monitoring plan states.
const (
	PlanStateDraft      = "DRAFT"
	PlanStateSubmitted  = "SUBMITTED"
	PlanStateConfirmed  = "CONFIRMED"
	PlanStateSuperseded = "SUPERSEDED"
)

// U64 is a fixed-point unsigned quantity (milli-tonnes, milli-nautical
// miles, whole minutes, nano-units). On the wire it renders as a decimal
// STRING, matching the proto-JSON uint64 rendering of the mrv.*.v1
// contracts; arithmetic inside the boundary is integer-only.
type U64 uint64

// MarshalJSON renders the proto-JSON string form ("12500").
func (value U64) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatUint(uint64(value), 10) + `"`), nil
}

// UnmarshalJSON accepts the proto-JSON string form (and a bare number).
func (value *U64) UnmarshalJSON(raw []byte) error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		var number json.Number
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if decErr := decoder.Decode(&number); decErr != nil {
			return fmt.Errorf("fixed-point quantity must be a decimal string: %w", err)
		}
		text = number.String()
	}
	parsed, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return fmt.Errorf("fixed-point quantity %q is not an unsigned integer", text)
	}
	*value = U64(parsed)
	return nil
}
