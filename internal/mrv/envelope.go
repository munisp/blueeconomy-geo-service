// Envelope construction for the mrv.*.v1 event family: canonical envelope
// v1.0 (FHIR R4 message Bundle + JWS-EdDSA over RFC 8785 JCS), producer
// "blueeconomy-geo-service-mrv", per blueeconomy-contracts
// docs/mrv-events.md, docs/envelope-signature.md and fixtures/mrv/*.json.
// Envelopes are built and signed at intake, inside the same transaction as
// the domain row + outbox row: an unsigned event pipeline never persists.
package mrv

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/munisp/blueeconomy-geo-service/internal/sign"
)

// fhirBundle mirrors the geo-service FHIR R4 message Bundle projection.
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

// BuildSignedEnvelope builds and signs the envelope v1.0 document for one
// mrv event. eventType must be one of the six contract types; resource must
// marshal to the proto-JSON object of the matching message. The event ID is
// caller-supplied (the outbox event id, so replays publish byte-identical
// envelopes and consumers dedup on it). A nil signer fails closed.
func BuildSignedEnvelope(eventType, eventID, correlationID string, resource any, occurredAt time.Time, principal sign.Provenance, ledgerCommitHash string, signer *sign.Signer) (json.RawMessage, error) {
	if signer == nil {
		return nil, errors.New("an envelope signer is required")
	}
	contract, ok := eventContracts[eventType]
	if !ok {
		return nil, fmt.Errorf("event type %q is not an mrv v1 contract type", eventType)
	}
	if _, err := uuid.Parse(eventID); err != nil {
		return nil, fmt.Errorf("event id must be a UUID: %w", err)
	}
	if strings.TrimSpace(correlationID) == "" {
		return nil, errors.New("correlation id is required")
	}
	if strings.TrimSpace(principal.PrincipalID) == "" || strings.TrimSpace(principal.PrincipalRole) == "" {
		return nil, errors.New("provenance principal id and role are required")
	}
	if occurredAt.IsZero() {
		return nil, errors.New("occurredAt is required")
	}
	resourceJSON, err := json.Marshal(resource)
	if err != nil {
		return nil, fmt.Errorf("encode %s resource: %w", eventType, err)
	}
	var fields map[string]json.RawMessage
	decoder := json.NewDecoder(strings.NewReader(string(resourceJSON)))
	decoder.UseNumber()
	if err := decoder.Decode(&fields); err != nil || len(fields) == 0 {
		return nil, fmt.Errorf("%s resource must be a JSON object", eventType)
	}
	if _, clash := fields["@type"]; clash {
		return nil, fmt.Errorf("%s resource must not carry an @type key", eventType)
	}
	typeBytes, _ := json.Marshal("type.googleapis.com/blueeconomy.contracts.v1." + contract.resourceType)
	fields["@type"] = typeBytes
	resourceBytes, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("encode %s resource: %w", eventType, err)
	}
	bundle := fhirBundle{
		ResourceType: "Bundle",
		Type:         "message",
		BundleID:     "bdl-" + uuid.NewString(),
		Entry:        []fhirEntry{{FullURL: "urn:uuid:" + uuid.NewString(), Resource: resourceBytes}},
	}
	bundleJSON, err := json.Marshal(bundle)
	if err != nil {
		return nil, fmt.Errorf("encode FHIR message bundle: %w", err)
	}
	envelope := sign.Envelope{
		EnvelopeVersion: sign.EnvelopeVersion,
		EventID:         eventID,
		EventType:       eventType,
		OccurredAt:      occurredAt.UTC(),
		Producer:        ProducerName,
		CorrelationID:   correlationID,
		Classification:  sign.MustClassification(sign.Classification(contract.classification)),
		FHIR:            bundleJSON,
		Provenance: sign.Provenance{
			PrincipalID:      principal.PrincipalID,
			PrincipalRole:    principal.PrincipalRole,
			LedgerCommitHash: ledgerCommitHash,
		},
	}
	// recordClassification is the per-record clearance label; the contract
	// fixtures carry it for CONFIDENTIAL records and omit it for INTERNAL
	// ones (INTERNAL is outside the recordClassification label set).
	if contract.classification == "CONFIDENTIAL" {
		envelope.RecordClassification = "CONFIDENTIAL"
	}
	signature, err := signer.Sign(envelope)
	if err != nil {
		return nil, fmt.Errorf("sign %s envelope: %w", eventType, err)
	}
	envelope.Provenance.Signature = signature
	raw, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode %s envelope: %w", eventType, err)
	}
	return raw, nil
}

// VerifySignedEnvelope re-parses and verifies a produced envelope against
// the producer public key (signature round-trip; used by tests and by the
// broker-gated emission check).
func VerifySignedEnvelope(raw json.RawMessage, publicKey ed25519.PublicKey) error {
	var envelope sign.Envelope
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}
	return sign.Verify(envelope, publicKey)
}
