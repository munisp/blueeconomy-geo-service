package sign

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testSigner(t *testing.T) *Signer {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := NewSigner(privateKey, "0")
	require.NoError(t, err)
	return signer
}

func testPrincipal() Provenance {
	return Provenance{PrincipalID: "f47ac10b-58cc-4372-a567-0e02b2c3d479", PrincipalRole: "geo-producer"}
}

func positionPayload() VesselPositionReported {
	msgType := int32(1)
	return VesselPositionReported{
		PositionReportID:             "pos-000001",
		MMSI:                         "657210300",
		SourceClass:                  SourceAIS,
		LatitudeMicros:               6418000,
		LongitudeMicros:              3372500,
		SpeedOverGroundMilliknots:    8400,
		CourseOverGroundMillidegrees: 127500,
		PositionAccuracy:             AccuracyHigh,
		ObservedAt:                   time.Date(2026, 8, 29, 9, 14, 21, 0, time.UTC),
		ReceiverID:                   "ais-rx-apapa-02",
		AISMessageType:               &msgType,
		Classification:               string(ClassificationPublic),
	}
}

func TestSignVerifyRoundtrip(t *testing.T) {
	signer := testSigner(t)
	envelope, err := NewEnvelope(EventVesselPosition, "pos-000001", positionPayload(),
		time.Date(2026, 8, 29, 9, 14, 27, 0, time.UTC), testPrincipal(), signer)
	require.NoError(t, err)
	require.Equal(t, EnvelopeVersion, envelope.EnvelopeVersion)
	require.Equal(t, EventVesselPosition, envelope.EventType)
	require.Equal(t, ClassificationPublic, envelope.Classification)
	require.NoError(t, Verify(envelope, signer.PublicKey()))

	// The JWS protected header carries the producer kid.
	header, err := base64.RawURLEncoding.DecodeString(strings.Split(envelope.Provenance.Signature, ".")[0])
	require.NoError(t, err)
	var parsed struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	require.NoError(t, json.Unmarshal(header, &parsed))
	require.Equal(t, "EdDSA", parsed.Alg)
	require.Equal(t, "blueeconomy-geo-service-0", parsed.Kid)

	// The FHIR bundle carries the typed resource.
	var bundle struct {
		ResourceType string `json:"resourceType"`
		Type         string `json:"type"`
		Entry        []struct {
			Resource map[string]any `json:"resource"`
		} `json:"entry"`
	}
	require.NoError(t, json.Unmarshal(envelope.FHIR, &bundle))
	require.Equal(t, "Bundle", bundle.ResourceType)
	require.Equal(t, "message", bundle.Type)
	require.Len(t, bundle.Entry, 1)
	require.Equal(t, "type.googleapis.com/blueeconomy.contracts.v1.VesselPositionReported",
		bundle.Entry[0].Resource["@type"])
	require.Equal(t, "657210300", bundle.Entry[0].Resource["mmsi"])
	// Fixed-point wire form: coordinates are integers, never floats.
	require.Equal(t, float64(6418000), bundle.Entry[0].Resource["latitudeMicros"])
}

func TestVerifyRejectsTamperedEnvelope(t *testing.T) {
	signer := testSigner(t)
	envelope, err := NewEnvelope(EventVesselPosition, "pos-000001", positionPayload(),
		time.Now().UTC(), testPrincipal(), signer)
	require.NoError(t, err)
	envelope.Classification = ClassificationSecret // tamper after signing
	require.Error(t, Verify(envelope, signer.PublicKey()))
}

func TestVerifyRejectsWrongKey(t *testing.T) {
	signer := testSigner(t)
	envelope, err := NewEnvelope(EventVesselPosition, "pos-000001", positionPayload(),
		time.Now().UTC(), testPrincipal(), signer)
	require.NoError(t, err)
	other, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	require.Error(t, Verify(envelope, other))
}

func TestNewEnvelopeRejectsUnknownEventType(t *testing.T) {
	signer := testSigner(t)
	_, err := NewEnvelope("geo.unknown.v9", "x", positionPayload(), time.Now().UTC(), testPrincipal(), signer)
	require.Error(t, err)
}

func TestNewEnvelopeSOSClassificationFloor(t *testing.T) {
	signer := testSigner(t)
	sos := SosAlertRaised{
		SosAlertID:      "sos-000001",
		ReporterID:      "rpt-001",
		VesselReference: "vsl-001",
		LatitudeMicros:  6418000,
		LongitudeMicros: 3372500,
		RecordedAt:      time.Now().UTC(),
		OutboxID:        "obx-001",
		Classification:  string(ClassificationPublic),
	}
	_, err := NewEnvelope(EventSOS, "sos-000001", sos, time.Now().UTC(), testPrincipal(), signer)
	require.Error(t, err, "SOS below RESTRICTED must fail closed")
	sos.Classification = string(ClassificationRestricted)
	envelope, err := NewEnvelope(EventSOS, "sos-000001", sos, time.Now().UTC(), testPrincipal(), signer)
	require.NoError(t, err)
	require.Equal(t, ClassificationRestricted, envelope.Classification)
}

func TestNewEnvelopeRequiresClassification(t *testing.T) {
	signer := testSigner(t)
	payload := positionPayload()
	payload.Classification = ""
	_, err := NewEnvelope(EventVesselPosition, "pos-1", payload, time.Now().UTC(), testPrincipal(), signer)
	require.Error(t, err, "content classification is mandatory")
	payload.Classification = "LOOSE"
	_, err = NewEnvelope(EventVesselPosition, "pos-1", payload, time.Now().UTC(), testPrincipal(), signer)
	require.Error(t, err, "unknown classification labels fail closed")
}

func TestParsePrivateKeyEncodings(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	seed := privateKey.Seed()
	for _, encoded := range []string{
		base64.StdEncoding.EncodeToString(seed),
		base64.RawURLEncoding.EncodeToString(seed),
		base64.StdEncoding.EncodeToString(privateKey),
	} {
		parsed, err := ParsePrivateKey(encoded)
		require.NoError(t, err)
		require.Equal(t, privateKey, parsed)
	}
	_, err = ParsePrivateKey("not-a-key")
	require.Error(t, err)
	_, err = ParsePrivateKey("")
	require.Error(t, err)
}

func TestClassificationLadder(t *testing.T) {
	require.True(t, ClassificationRestricted.Covers(ClassificationPublic))
	require.True(t, ClassificationRestricted.Covers(ClassificationRestricted))
	require.False(t, ClassificationInternal.Covers(ClassificationSecret))
	require.Equal(t, ClassificationConfidential, MaxClassification(ClassificationInternal, ClassificationConfidential))
	_, err := ParseClassification("UNCLASSIFIED")
	require.Error(t, err, "the geo ladder has no UNCLASSIFIED label")
	require.False(t, Classification("BOGUS").Covers(ClassificationPublic))
}
