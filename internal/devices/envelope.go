// Device envelope and proof signing: JWS compact serialization
// (EdDSA/Ed25519) over the JCS-canonicalized (RFC 8785) JSON payload,
// aligned with the platform envelopeVersion 1.0 provenance scheme
// (internal/sign) but keyed per device: the protected header is
// {"alg":"EdDSA","kid":"geo-device-<deviceId>-<epoch>"} where <epoch> is
// the device key-rotation epoch. Device private keys never leave the
// device; the service verifies against the registered public key epoch.
package devices

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	jsoncanonicalizer "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

const (
	// DeviceEnvelopeVersion is the only supported device envelope version.
	DeviceEnvelopeVersion = "1.0"

	signingAlgorithm = "EdDSA"
	keyIDPrefix      = "geo-device-"
)

// DeviceEnvelope is the signed telemetry envelope a device posts to
// POST /v1/devices/{id}/telemetry. The signature is the JWS compact
// serialization over the JCS-canonical envelope with the signature field
// excluded (identical construction to the platform provenance signature).
type DeviceEnvelope struct {
	EnvelopeVersion string          `json:"envelopeVersion"`
	DeviceID        string          `json:"deviceId"`
	KeyEpoch        int             `json:"keyEpoch"`
	PayloadType     string          `json:"payloadType"`
	OccurredAt      time.Time       `json:"occurredAt"`
	Payload         json.RawMessage `json:"payload"`
	Signature       string          `json:"signature"`
}

// Proof is the signed action proof a device presents for the firmware and
// MQTT-auth endpoints (and any future broker/control-plane call): a
// compact JWS over the canonical JSON
// {"action":..., "deviceId":..., "keyEpoch":...}.
type Proof struct {
	Action   string `json:"action"`
	DeviceID string `json:"deviceId"`
	KeyEpoch int    `json:"keyEpoch"`
}

// Proof actions (fail-closed set).
const (
	ProofActionFirmware = "GET_FIRMWARE"
	ProofActionMQTTAuth = "MQTT_AUTH"
)

// KeyID renders the JWS kid for one device key epoch.
func KeyID(deviceID string, epoch int) string {
	return fmt.Sprintf("%s%s-%d", keyIDPrefix, deviceID, epoch)
}

// ParseKeyID extracts the device id and epoch from a device kid. It fails
// closed on any malformed value.
func ParseKeyID(kid string) (deviceID string, epoch int, err error) {
	if !strings.HasPrefix(kid, keyIDPrefix) {
		return "", 0, fmt.Errorf("kid %q is not a geo-device key", kid)
	}
	rest := strings.TrimPrefix(kid, keyIDPrefix)
	separator := strings.LastIndex(rest, "-")
	if separator <= 0 || separator == len(rest)-1 {
		return "", 0, fmt.Errorf("kid %q does not carry a device id and epoch", kid)
	}
	deviceID = rest[:separator]
	if _, err := fmt.Sscanf(rest[separator+1:], "%d", &epoch); err != nil || epoch < 1 {
		return "", 0, fmt.Errorf("kid %q carries an invalid epoch", kid)
	}
	return deviceID, epoch, nil
}

func decodeBase64URL(value string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(value)
}

// canonicalJSON renders value as JCS-canonical (RFC 8785) JSON. Numbers
// round-trip as literals (UseNumber) so the signed bytes are stable.
func canonicalJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode payload for signing: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var generic any
	if err := decoder.Decode(&generic); err != nil {
		return nil, fmt.Errorf("decode payload for signing: %w", err)
	}
	stripped, err := json.Marshal(generic)
	if err != nil {
		return nil, fmt.Errorf("re-encode payload for signing: %w", err)
	}
	canonical, err := jsoncanonicalizer.Transform(stripped)
	if err != nil {
		return nil, fmt.Errorf("JCS-canonicalize payload: %w", err)
	}
	return canonical, nil
}

// SignPayload produces the JWS compact serialization (EdDSA) over the
// JCS-canonical form of payload with the given kid. It is the device-side
// signing primitive (also used server-side for firmware manifests with the
// service key).
func SignPayload(privateKey ed25519.PrivateKey, kid string, payload any) (string, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return "", errors.New("signing private key must be a valid Ed25519 key")
	}
	if strings.TrimSpace(kid) == "" {
		return "", errors.New("JWS kid is required")
	}
	header, err := json.Marshal(struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}{Algorithm: signingAlgorithm, KeyID: kid})
	if err != nil {
		return "", fmt.Errorf("encode JWS protected header: %w", err)
	}
	canonical, err := canonicalJSON(payload)
	if err != nil {
		return "", err
	}
	header64 := base64.RawURLEncoding.EncodeToString(header)
	payload64 := base64.RawURLEncoding.EncodeToString(canonical)
	signingInput := header64 + "." + payload64
	signature := ed25519.Sign(privateKey, []byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// unsignedEnvelopePayload renders the envelope as a generic map with the
// signature field removed — the exact object that is JCS-canonicalized and
// signed (mirrors sign.canonicalPayload for the platform envelope).
func unsignedEnvelopePayload(envelope DeviceEnvelope) (map[string]any, error) {
	raw, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode envelope for signing: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var generic map[string]any
	if err := decoder.Decode(&generic); err != nil {
		return nil, fmt.Errorf("decode envelope for signing: %w", err)
	}
	delete(generic, "signature")
	return generic, nil
}

// SignEnvelope signs one device telemetry envelope (device side and test
// harness). The returned envelope carries the JWS in Signature.
func SignEnvelope(privateKey ed25519.PrivateKey, envelope DeviceEnvelope) (DeviceEnvelope, error) {
	if envelope.EnvelopeVersion == "" {
		envelope.EnvelopeVersion = DeviceEnvelopeVersion
	}
	unsigned, err := unsignedEnvelopePayload(envelope)
	if err != nil {
		return DeviceEnvelope{}, err
	}
	signature, err := SignPayload(privateKey, KeyID(envelope.DeviceID, envelope.KeyEpoch), unsigned)
	if err != nil {
		return DeviceEnvelope{}, err
	}
	envelope.Signature = signature
	return envelope, nil
}

// verifyJWS checks a compact JWS against the expected kid and public key:
// alg must be EdDSA, the kid must match exactly, the payload must
// re-canonicalize to the exact signed bytes, and the Ed25519 signature
// must verify. The canonical payload bytes are returned for decoding.
func verifyJWS(compact, expectedKeyID string, publicKey ed25519.PublicKey) ([]byte, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, errors.New("verification public key must be a valid Ed25519 key")
	}
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		return nil, errors.New("signature is not a JWS compact serialization")
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("decode JWS protected header: %w", err)
	}
	var parsed struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if err := json.Unmarshal(header, &parsed); err != nil {
		return nil, fmt.Errorf("parse JWS protected header: %w", err)
	}
	if parsed.Algorithm != signingAlgorithm {
		return nil, fmt.Errorf("JWS alg %q is not %q", parsed.Algorithm, signingAlgorithm)
	}
	if parsed.KeyID != expectedKeyID {
		return nil, fmt.Errorf("JWS kid %q does not match the expected device key %q", parsed.KeyID, expectedKeyID)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("decode JWS signature: %w", err)
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode JWS payload: %w", err)
	}
	// The signed payload must already be JCS-canonical: re-canonicalizing
	// must reproduce the exact bytes (no malleable whitespace/key order).
	canonical, err := jsoncanonicalizer.Transform(payloadBytes)
	if err != nil {
		return nil, fmt.Errorf("JCS-canonicalize JWS payload: %w", err)
	}
	if !bytes.Equal(canonical, payloadBytes) {
		return nil, errors.New("JWS payload is not in JCS-canonical form")
	}
	if !ed25519.Verify(publicKey, []byte(parts[0]+"."+parts[1]), signature) {
		return nil, errors.New("JWS signature verification failed")
	}
	return payloadBytes, nil
}

// VerifyEnvelopeSignature verifies the envelope JWS against the device key
// for envelope.KeyEpoch and returns the fail-closed reason on rejection.
func VerifyEnvelopeSignature(envelope DeviceEnvelope, publicKey ed25519.PublicKey) error {
	unsigned, err := unsignedEnvelopePayload(envelope)
	if err != nil {
		return err
	}
	expectedCanonical, err := canonicalJSON(unsigned)
	if err != nil {
		return err
	}
	payloadBytes, err := verifyJWS(envelope.Signature, KeyID(envelope.DeviceID, envelope.KeyEpoch), publicKey)
	if err != nil {
		return err
	}
	if !bytes.Equal(payloadBytes, expectedCanonical) {
		return errors.New("envelope does not match the signed canonical payload")
	}
	return nil
}

// VerifyProof verifies a signed action proof and decodes it.
func VerifyProof(compact string, publicKey ed25519.PublicKey) (Proof, error) {
	// The kid is bound after decoding: parse the header first to learn the
	// claimed device/epoch, then require exact kid consistency.
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		return Proof{}, errors.New("proof is not a JWS compact serialization")
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Proof{}, fmt.Errorf("decode JWS protected header: %w", err)
	}
	var parsedHeader struct {
		KeyID string `json:"kid"`
	}
	if err := json.Unmarshal(header, &parsedHeader); err != nil {
		return Proof{}, fmt.Errorf("parse JWS protected header: %w", err)
	}
	payloadBytes, err := verifyJWS(compact, parsedHeader.KeyID, publicKey)
	if err != nil {
		return Proof{}, err
	}
	var proof Proof
	if err := json.Unmarshal(payloadBytes, &proof); err != nil {
		return Proof{}, fmt.Errorf("decode signed proof: %w", err)
	}
	if KeyID(proof.DeviceID, proof.KeyEpoch) != parsedHeader.KeyID {
		return Proof{}, errors.New("proof kid does not match the signed device id and epoch")
	}
	return proof, nil
}
