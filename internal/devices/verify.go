// Verify-at-ingest: device-signed envelopes and action proofs are
// authenticated against the registry before any payload touches the event
// pipeline. Every rejection carries a fail-closed reason, an audit ledger
// entry and a Prometheus counter; suspended/revoked devices and revoked or
// grace-expired key epochs never authenticate.
package devices

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
)

// AuthReader is the storage surface the verifier needs (the geo_devices
// device-plane pool in production; a fake in unit tests).
type AuthReader interface {
	LoadDeviceForAuth(ctx context.Context, deviceID string) (Device, error)
	LoadKey(ctx context.Context, deviceID string, epoch int) (Key, error)
	InsertAudit(ctx context.Context, event AuditEvent) error
}

// AuthError is one authentication rejection with its fail-closed reason.
type AuthError struct {
	Reason   string
	DeviceID string
	Detail   string
}

func (err *AuthError) Error() string {
	if err.Detail != "" {
		return fmt.Sprintf("device authentication failed (%s): %s", err.Reason, err.Detail)
	}
	return fmt.Sprintf("device authentication failed (%s)", err.Reason)
}

// Verifier authenticates device envelopes and proofs.
type Verifier struct {
	Reader  AuthReader
	Metrics *metrics.Registry
	// Grace is the window a PREVIOUS key epoch stays valid after rotation.
	Grace time.Duration
	now   func() time.Time
}

// NewVerifier wires the verifier fail-closed.
func NewVerifier(reader AuthReader, registry *metrics.Registry, grace time.Duration) (*Verifier, error) {
	if reader == nil {
		return nil, errors.New("devices: verifier auth reader is required")
	}
	if registry == nil {
		return nil, errors.New("devices: verifier metrics registry is required")
	}
	if grace <= 0 {
		grace = DefaultKeyGrace
	}
	return &Verifier{Reader: reader, Metrics: registry, Grace: grace, now: time.Now}, nil
}

// VerifiedDevice is the authenticated device context for ingest paths.
type VerifiedDevice struct {
	Device   Device
	KeyEpoch int
}

// VerifyTelemetry authenticates one posted telemetry envelope for the
// device named in the URL path. On success the verified envelope and the
// device context are returned; on rejection an *AuthError carries the
// reason (and the audit + metric are already recorded).
func (verifier *Verifier) VerifyTelemetry(ctx context.Context, pathDeviceID string, raw []byte) (DeviceEnvelope, VerifiedDevice, error) {
	var envelope DeviceEnvelope
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		verifier.reject(ctx, "", "", ReasonEnvelopeMalformed, "envelope is not valid JSON")
		return DeviceEnvelope{}, VerifiedDevice{}, &AuthError{Reason: ReasonEnvelopeMalformed, Detail: "envelope is not valid JSON"}
	}
	if envelope.EnvelopeVersion != DeviceEnvelopeVersion {
		verifier.reject(ctx, envelope.DeviceID, "", ReasonEnvelopeMalformed, "envelopeVersion is not 1.0")
		return DeviceEnvelope{}, VerifiedDevice{}, &AuthError{Reason: ReasonEnvelopeMalformed, DeviceID: envelope.DeviceID, Detail: "envelopeVersion is not 1.0"}
	}
	if strings.TrimSpace(envelope.DeviceID) == "" || envelope.KeyEpoch < 1 ||
		strings.TrimSpace(envelope.PayloadType) == "" || len(envelope.Payload) == 0 ||
		envelope.OccurredAt.IsZero() || strings.TrimSpace(envelope.Signature) == "" {
		verifier.reject(ctx, envelope.DeviceID, "", ReasonEnvelopeMalformed, "envelope fields are incomplete")
		return DeviceEnvelope{}, VerifiedDevice{}, &AuthError{Reason: ReasonEnvelopeMalformed, DeviceID: envelope.DeviceID, Detail: "envelope fields are incomplete"}
	}
	if envelope.DeviceID != pathDeviceID {
		verifier.reject(ctx, envelope.DeviceID, "", ReasonDeviceIDMismatch, "path device id does not match the envelope")
		return DeviceEnvelope{}, VerifiedDevice{}, &AuthError{Reason: ReasonDeviceIDMismatch, DeviceID: envelope.DeviceID, Detail: "path device id does not match the envelope"}
	}
	device, key, err := verifier.authenticate(ctx, envelope.DeviceID, envelope.KeyEpoch)
	if err != nil {
		return DeviceEnvelope{}, VerifiedDevice{}, err
	}
	if err := VerifyEnvelopeSignature(envelope, ed25519.PublicKey(key.PublicKey)); err != nil {
		verifier.reject(ctx, device.ID, device.TenantID, ReasonSignatureInvalid, err.Error())
		return DeviceEnvelope{}, VerifiedDevice{}, &AuthError{Reason: ReasonSignatureInvalid, DeviceID: device.ID, Detail: "signature verification failed"}
	}
	verifier.accept(ctx, device, "telemetry")
	return envelope, VerifiedDevice{Device: device, KeyEpoch: envelope.KeyEpoch}, nil
}

// VerifyProof authenticates a signed action proof (firmware fetch, MQTT
// auth) for one device.
func (verifier *Verifier) VerifyProof(ctx context.Context, compact, expectedAction string) (VerifiedDevice, error) {
	if strings.TrimSpace(compact) == "" {
		verifier.reject(ctx, "", "", ReasonEnvelopeMalformed, "proof is required")
		return VerifiedDevice{}, &AuthError{Reason: ReasonEnvelopeMalformed, Detail: "proof is required"}
	}
	// Learn the claimed device/epoch from the (untrusted) header to load
	// the candidate key; the signature check below is what authenticates.
	deviceID, epoch, err := proofClaims(compact)
	if err != nil {
		verifier.reject(ctx, "", "", ReasonEnvelopeMalformed, err.Error())
		return VerifiedDevice{}, &AuthError{Reason: ReasonEnvelopeMalformed, Detail: err.Error()}
	}
	device, key, err := verifier.authenticate(ctx, deviceID, epoch)
	if err != nil {
		return VerifiedDevice{}, err
	}
	proof, err := VerifyProof(compact, ed25519.PublicKey(key.PublicKey))
	if err != nil {
		verifier.reject(ctx, device.ID, device.TenantID, ReasonSignatureInvalid, err.Error())
		return VerifiedDevice{}, &AuthError{Reason: ReasonSignatureInvalid, DeviceID: device.ID, Detail: "proof signature verification failed"}
	}
	if proof.Action != expectedAction {
		verifier.reject(ctx, device.ID, device.TenantID, ReasonActionMismatch, "proof action does not match the endpoint")
		return VerifiedDevice{}, &AuthError{Reason: ReasonActionMismatch, DeviceID: device.ID, Detail: "proof action does not match the endpoint"}
	}
	if proof.DeviceID != deviceID || proof.KeyEpoch != epoch {
		verifier.reject(ctx, device.ID, device.TenantID, ReasonDeviceIDMismatch, "proof claims do not match the key id")
		return VerifiedDevice{}, &AuthError{Reason: ReasonDeviceIDMismatch, DeviceID: device.ID, Detail: "proof claims do not match the key id"}
	}
	verifier.accept(ctx, device, expectedAction)
	return VerifiedDevice{Device: device, KeyEpoch: epoch}, nil
}

// proofClaims extracts the device id and epoch from the JWS header kid
// without trusting them (the signature check is what authenticates).
func proofClaims(compact string) (string, int, error) {
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		return "", 0, errors.New("proof is not a JWS compact serialization")
	}
	header, err := decodeBase64URL(parts[0])
	if err != nil {
		return "", 0, errors.New("proof header is not base64url")
	}
	var parsed struct {
		KeyID string `json:"kid"`
	}
	if err := json.Unmarshal(header, &parsed); err != nil {
		return "", 0, errors.New("proof header is not valid JSON")
	}
	return ParseKeyID(parsed.KeyID)
}

// authenticate resolves device + key epoch and enforces the registry
// gates: device must be ACTIVE; the epoch must be CURRENT, or PREVIOUS
// inside the rotation grace window; REVOKED keys and unknown epochs fail.
func (verifier *Verifier) authenticate(ctx context.Context, deviceID string, epoch int) (Device, Key, error) {
	device, err := verifier.Reader.LoadDeviceForAuth(ctx, deviceID)
	if errors.Is(err, ErrDeviceNotFound) {
		verifier.reject(ctx, deviceID, "", ReasonDeviceUnknown, "device is not registered")
		return Device{}, Key{}, &AuthError{Reason: ReasonDeviceUnknown, DeviceID: deviceID, Detail: "device is not registered"}
	}
	if err != nil {
		return Device{}, Key{}, fmt.Errorf("load device for auth: %w", err)
	}
	switch device.Status {
	case StatusActive:
	case StatusSuspended:
		verifier.reject(ctx, device.ID, device.TenantID, ReasonDeviceSuspended, "device is suspended")
		return Device{}, Key{}, &AuthError{Reason: ReasonDeviceSuspended, DeviceID: device.ID, Detail: "device is suspended"}
	case StatusRevoked:
		verifier.reject(ctx, device.ID, device.TenantID, ReasonDeviceRevoked, "device is revoked")
		return Device{}, Key{}, &AuthError{Reason: ReasonDeviceRevoked, DeviceID: device.ID, Detail: "device is revoked"}
	default:
		verifier.reject(ctx, device.ID, device.TenantID, ReasonDeviceRetired, "device is decommissioned")
		return Device{}, Key{}, &AuthError{Reason: ReasonDeviceRetired, DeviceID: device.ID, Detail: "device is decommissioned"}
	}
	key, err := verifier.Reader.LoadKey(ctx, deviceID, epoch)
	if err != nil {
		verifier.reject(ctx, device.ID, device.TenantID, ReasonEpochUnknown, "key epoch is not registered")
		return Device{}, Key{}, &AuthError{Reason: ReasonEpochUnknown, DeviceID: device.ID, Detail: "key epoch is not registered"}
	}
	switch key.Status {
	case KeyCurrent:
		if epoch != device.KeyEpoch {
			// A CURRENT row must be the device's current epoch — registry
			// inconsistency fails closed.
			verifier.reject(ctx, device.ID, device.TenantID, ReasonEpochUnknown, "key epoch is not the device current epoch")
			return Device{}, Key{}, &AuthError{Reason: ReasonEpochUnknown, DeviceID: device.ID, Detail: "key epoch is not the device current epoch"}
		}
	case KeyPrevious:
		if verifier.now().Sub(key.RotatedAt) > verifier.Grace {
			verifier.reject(ctx, device.ID, device.TenantID, ReasonGraceExpired, "previous key epoch is past the rotation grace window")
			return Device{}, Key{}, &AuthError{Reason: ReasonGraceExpired, DeviceID: device.ID, Detail: "previous key epoch is past the rotation grace window"}
		}
	case KeyRevoked:
		verifier.reject(ctx, device.ID, device.TenantID, ReasonKeyRevoked, "key epoch is revoked")
		return Device{}, Key{}, &AuthError{Reason: ReasonKeyRevoked, DeviceID: device.ID, Detail: "key epoch is revoked"}
	default:
		verifier.reject(ctx, device.ID, device.TenantID, ReasonEpochUnknown, "key epoch status is unknown")
		return Device{}, Key{}, &AuthError{Reason: ReasonEpochUnknown, DeviceID: device.ID, Detail: "key epoch status is unknown"}
	}
	return device, key, nil
}

// reject records the audit entry and metric for one rejection.
func (verifier *Verifier) reject(ctx context.Context, deviceID, tenantID, reason, detail string) {
	verifier.Metrics.Inc("geo_device_auth_total", map[string]string{"result": "fail", "reason": reason})
	_ = verifier.Reader.InsertAudit(ctx, AuditEvent{
		TenantID: tenantID, DeviceID: deviceID, Event: AuditAuthFailed,
		Actor: "device:" + deviceID, Reason: reason,
		Metadata: map[string]string{"detail": detail},
	})
}

// accept records the success metric (successes are metric-only; the audit
// volume of a hot ingest path is not ledgered per message).
func (verifier *Verifier) accept(_ context.Context, _ Device, _ string) {
	verifier.Metrics.Inc("geo_device_auth_total", map[string]string{"result": "ok"})
}
