// Envelope construction for the geo.*.v1 event family. Every envelope is the
// platform envelopeVersion 1.0 contract: the event is the primary resource of
// a FHIR-aligned message Bundle under the canonical "fhir" key, and the
// provenance signature is a JWS-EdDSA over the JCS-canonicalized envelope
// (see blueeconomy-contracts docs/geo-events.md and docs/envelope-signature.md).
package sign

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// EnvelopeVersion is the only supported envelope contract version.
	EnvelopeVersion = "1.0"
	// Producer is the deployable name asserted on every envelope and used as
	// the JWS kid prefix ("blueeconomy-geo-service-<epoch>").
	Producer = "blueeconomy-geo-service"

	// Event types (contract-governed, fail-closed set).
	EventVesselPosition    = "geo.vessel-position.v1"
	EventVesselStatic      = "geo.vessel-static.v1"
	EventGeofenceEvent     = "geo.geofence-event.v1"
	EventAppPositionReport = "geo.app-position-report.v1"
	EventSOS               = "geo.sos.v1"
	// EventSOSAcknowledged / EventSOSResolved carry the SOS lifecycle
	// ledger transitions (RESTRICTED floor, same as the alert).
	EventSOSAcknowledged = "geo.sos-acknowledged.v1"
	EventSOSResolved     = "geo.sos-resolved.v1"

	// typeURLPrefix names the proto package for the FHIR Any projection.
	typeURLPrefix = "type.googleapis.com/blueeconomy.contracts.v1."
)

// eventResourceType maps each event type to its governing proto message. A
// mismatch between the event type and the carried resource must fail closed
// in consumers, so the producer asserts exactly one mapping.
var eventResourceType = map[string]string{
	EventVesselPosition:    "VesselPositionReported",
	EventVesselStatic:      "VesselStaticReported",
	EventGeofenceEvent:     "GeofenceEventRecorded",
	EventAppPositionReport: "AppPositionReported",
	EventSOS:               "SosAlertRaised",
	EventSOSAcknowledged:   "SosAlertAcknowledged",
	EventSOSResolved:       "SosAlertResolved",
}

// sosEventTypes marks the event family carrying the RESTRICTED floor.
var sosEventTypes = map[string]bool{
	EventSOS:             true,
	EventSOSAcknowledged: true,
	EventSOSResolved:     true,
}

// Provenance binds an event to the acting principal and the integrity chain.
type Provenance struct {
	PrincipalID      string `json:"principalId"`
	PrincipalRole    string `json:"principalRole"`
	LedgerCommitHash string `json:"ledgerCommitHash"`
	Signature        string `json:"signature"`
}

// Envelope is the platform event contract (envelopeVersion 1.0). The FHIR
// message bundle is emitted under the canonical `fhir` key. The envelope
// classification is always at least as restrictive as the classification of
// the event content it carries.
type Envelope struct {
	EnvelopeVersion string          `json:"envelopeVersion"`
	EventID         string          `json:"eventId"`
	EventType       string          `json:"eventType"`
	OccurredAt      time.Time       `json:"occurredAt"`
	Producer        string          `json:"producer"`
	CorrelationID   string          `json:"correlationId"`
	Classification  Classification  `json:"classification"`
	FHIR            json.RawMessage `json:"fhir"`
	Provenance      Provenance      `json:"provenance"`
}

// fhirBundle is the FHIR R4-aligned message Bundle projection. The primary
// resource renders the proto-JSON event fields under an "@type" key naming
// the proto message, exactly as governed by docs/geo-events.md fixtures.
type fhirEntry struct {
	FullURL  string          `json:"fullUrl"`
	Resource json.RawMessage `json:"resource"`
}

type fhirBundle struct {
	ResourceType string      `json:"resourceType"`
	Type         string      `json:"type"`
	BundleID     string      `json:"bundleId"`
	Entry        []fhirEntry `json:"entry"`
}

// NewEnvelope builds a signed envelope for a geo event. eventType must be one
// of the five contract types; payload must marshal to the proto-JSON object
// of the matching message (classification field included). The envelope
// classification is raised to at least the content classification. A nil
// signer fails closed — an unsigned event pipeline must never emit.
func NewEnvelope(eventType, correlationID string, payload any, occurredAt time.Time, principal Provenance, signer *Signer) (Envelope, error) {
	if signer == nil {
		return Envelope{}, errors.New("an envelope signer is required")
	}
	resourceType, ok := eventResourceType[eventType]
	if !ok {
		return Envelope{}, fmt.Errorf("event type %q is not a geo v1 contract type", eventType)
	}
	if strings.TrimSpace(correlationID) == "" {
		return Envelope{}, errors.New("correlation id is required")
	}
	if strings.TrimSpace(principal.PrincipalID) == "" || strings.TrimSpace(principal.PrincipalRole) == "" {
		return Envelope{}, errors.New("provenance principal id and role are required")
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode %s payload: %w", eventType, err)
	}
	var resource map[string]json.RawMessage
	decoder := json.NewDecoder(strings.NewReader(string(payloadJSON)))
	decoder.UseNumber()
	if err := decoder.Decode(&resource); err != nil || len(resource) == 0 {
		return Envelope{}, fmt.Errorf("%s payload must be a JSON object", eventType)
	}
	if _, clash := resource["@type"]; clash {
		return Envelope{}, fmt.Errorf("%s payload must not carry an @type key", eventType)
	}
	// Content classification coherence: the envelope classification must be at
	// least as restrictive as the event content.
	var contentClass Classification
	if raw, present := resource["classification"]; present {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return Envelope{}, fmt.Errorf("%s payload classification is invalid", eventType)
		}
		contentClass, err = ParseClassification(text)
		if err != nil {
			return Envelope{}, fmt.Errorf("%s payload classification: %w", eventType, err)
		}
	} else {
		return Envelope{}, fmt.Errorf("%s payload must assert a classification", eventType)
	}
	// Contract floor: SOS alerts and their lifecycle events are RESTRICTED
	// minimum.
	if sosEventTypes[eventType] && contentClass.Rank() < ClassificationRestricted.Rank() {
		return Envelope{}, fmt.Errorf("%s classification floor is RESTRICTED", eventType)
	}
	typeBytes, err := json.Marshal(typeURLPrefix + resourceType)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode resource type url: %w", err)
	}
	resource["@type"] = typeBytes
	resourceJSON, err := json.Marshal(resource)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode %s resource: %w", eventType, err)
	}
	bundle := fhirBundle{
		ResourceType: "Bundle",
		Type:         "message",
		BundleID:     "bdl-" + uuid.NewString(),
		Entry: []fhirEntry{{
			FullURL:  "urn:uuid:" + uuid.NewString(),
			Resource: resourceJSON,
		}},
	}
	bundleJSON, err := json.Marshal(bundle)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode FHIR message bundle: %w", err)
	}
	occurred := occurredAt.UTC()
	if occurred.IsZero() {
		return Envelope{}, errors.New("occurredAt is required")
	}
	envelope := Envelope{
		EnvelopeVersion: EnvelopeVersion,
		EventID:         uuid.NewString(),
		EventType:       eventType,
		OccurredAt:      occurred,
		Producer:        Producer,
		CorrelationID:   correlationID,
		Classification:  contentClass,
		FHIR:            bundleJSON,
		Provenance:      principal,
	}
	signature, err := signer.Sign(envelope)
	if err != nil {
		return Envelope{}, fmt.Errorf("sign %s envelope: %w", eventType, err)
	}
	envelope.Provenance.Signature = signature
	return envelope, nil
}
