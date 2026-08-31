// Unit tests for the device plane that do not require Postgres: the
// envelope verification matrix runs against a fake AuthReader; rollout
// bucketing, manifest signing, lifecycle transitions and the mqtt-auth /
// telemetry handlers run in-memory. DB-gated coverage (RLS, provisioning
// SQL, rotation persistence) lives in integration/devices_integration_test.go.
package devices

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
)

// fakeAuthReader is the in-memory AuthReader for the verifier matrix.
type fakeAuthReader struct {
	mu      sync.Mutex
	devices map[string]Device
	keys    map[string]Key
	audits  []AuditEvent
}

func newFakeAuthReader() *fakeAuthReader {
	return &fakeAuthReader{devices: map[string]Device{}, keys: map[string]Key{}}
}

func keyRef(deviceID string, epoch int) string { return fmt.Sprintf("%s:%d", deviceID, epoch) }

func (fake *fakeAuthReader) LoadDeviceForAuth(_ context.Context, deviceID string) (Device, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	device, ok := fake.devices[deviceID]
	if !ok {
		return Device{}, ErrDeviceNotFound
	}
	return device, nil
}

func (fake *fakeAuthReader) LoadKey(_ context.Context, deviceID string, epoch int) (Key, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	key, ok := fake.keys[keyRef(deviceID, epoch)]
	if !ok {
		return Key{}, fmt.Errorf("device key epoch %d not found", epoch)
	}
	return key, nil
}

func (fake *fakeAuthReader) InsertAudit(_ context.Context, event AuditEvent) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.audits = append(fake.audits, event)
	return nil
}

func (fake *fakeAuthReader) auditReasons() []string {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	reasons := make([]string, 0, len(fake.audits))
	for _, event := range fake.audits {
		reasons = append(reasons, event.Reason)
	}
	return reasons
}

// registerDevice adds an ACTIVE device with one CURRENT key epoch.
func registerDevice(t *testing.T, fake *fakeAuthReader, deviceID string, epoch int) ed25519.PrivateKey {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	fake.devices[deviceID] = Device{
		ID: deviceID, TenantID: "tenant-a", Kind: KindAIS, OwnerAgency: "npa",
		Label: "test device", Status: StatusActive, KeyEpoch: epoch, CreatedBy: "maker",
	}
	fake.keys[keyRef(deviceID, epoch)] = Key{
		DeviceID: deviceID, Epoch: epoch, PublicKey: public, Status: KeyCurrent, RotatedAt: time.Now(),
	}
	return private
}

func newVerifier(t *testing.T, fake *fakeAuthReader, grace time.Duration) *Verifier {
	t.Helper()
	verifier, err := NewVerifier(fake, metrics.NewRegistry(), grace)
	require.NoError(t, err)
	return verifier
}

func signTelemetry(t *testing.T, private ed25519.PrivateKey, deviceID string, epoch int, payload json.RawMessage) []byte {
	t.Helper()
	envelope, err := SignEnvelope(private, DeviceEnvelope{
		DeviceID: deviceID, KeyEpoch: epoch,
		PayloadType: sign.EventVesselPosition, OccurredAt: time.Now().UTC(), Payload: payload,
	})
	require.NoError(t, err)
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	return raw
}

var positionPayload = json.RawMessage(`{"positionReportId":"pr-1","mmsi":"657210300","sourceClass":"AIS","latitudeMicros":6418000,"longitudeMicros":3372500,"speedOverGroundMilliknots":8400,"courseOverGroundMillidegrees":90000,"positionAccuracy":"HIGH","observedAt":"2025-01-01T00:00:00Z","receiverId":"rx-1","classification":"PUBLIC"}`)

func TestVerifyTelemetryValid(t *testing.T) {
	fake := newFakeAuthReader()
	private := registerDevice(t, fake, "dev-1", 1)
	verifier := newVerifier(t, fake, time.Hour)

	envelope, verified, err := verifier.VerifyTelemetry(context.Background(), "dev-1",
		signTelemetry(t, private, "dev-1", 1, positionPayload))
	require.NoError(t, err)
	require.Equal(t, "dev-1", verified.Device.ID)
	require.Equal(t, 1, verified.KeyEpoch)
	require.Equal(t, sign.EventVesselPosition, envelope.PayloadType)
	require.Empty(t, fake.auditReasons(), "no failure audit on success")
}

func TestVerifyTelemetryBadSignature(t *testing.T) {
	fake := newFakeAuthReader()
	registerDevice(t, fake, "dev-1", 1)
	other := registerDevice(t, fake, "dev-2", 1)
	verifier := newVerifier(t, fake, time.Hour)

	// Signed by dev-2's key but claiming dev-1: the kid binds dev-1 so the
	// signature does not verify against dev-1's registered key.
	raw := signTelemetry(t, other, "dev-1", 1, positionPayload)
	_, _, err := verifier.VerifyTelemetry(context.Background(), "dev-1", raw)
	var authErr *AuthError
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonSignatureInvalid, authErr.Reason)
	require.Contains(t, fake.auditReasons(), ReasonSignatureInvalid)
}

func TestVerifyTelemetryTamperedPayload(t *testing.T) {
	fake := newFakeAuthReader()
	private := registerDevice(t, fake, "dev-1", 1)
	verifier := newVerifier(t, fake, time.Hour)

	raw := signTelemetry(t, private, "dev-1", 1, positionPayload)
	// Tamper after signing: flip the MMSI.
	tampered := strings.Replace(string(raw), "657210300", "657210301", 1)
	_, _, err := verifier.VerifyTelemetry(context.Background(), "dev-1", []byte(tampered))
	var authErr *AuthError
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonSignatureInvalid, authErr.Reason)
}

func TestVerifyTelemetrySuspendedAndRevokedDevices(t *testing.T) {
	fake := newFakeAuthReader()
	suspendedKey := registerDevice(t, fake, "dev-suspended", 1)
	revokedKey := registerDevice(t, fake, "dev-revoked", 1)
	retiredKey := registerDevice(t, fake, "dev-retired", 1)
	suspended := fake.devices["dev-suspended"]
	suspended.Status = StatusSuspended
	fake.devices["dev-suspended"] = suspended
	revoked := fake.devices["dev-revoked"]
	revoked.Status = StatusRevoked
	fake.devices["dev-revoked"] = revoked
	retired := fake.devices["dev-retired"]
	retired.Status = StatusDecommissioned
	fake.devices["dev-retired"] = retired
	verifier := newVerifier(t, fake, time.Hour)

	cases := []struct {
		deviceID string
		key      ed25519.PrivateKey
		reason   string
	}{
		{"dev-suspended", suspendedKey, ReasonDeviceSuspended},
		{"dev-revoked", revokedKey, ReasonDeviceRevoked},
		{"dev-retired", retiredKey, ReasonDeviceRetired},
	}
	for _, testCase := range cases {
		_, _, err := verifier.VerifyTelemetry(context.Background(), testCase.deviceID,
			signTelemetry(t, testCase.key, testCase.deviceID, 1, positionPayload))
		var authErr *AuthError
		require.ErrorAs(t, err, &authErr, testCase.deviceID)
		require.Equal(t, testCase.reason, authErr.Reason, testCase.deviceID)
	}
}

func TestVerifyTelemetryKeyEpochMatrix(t *testing.T) {
	fake := newFakeAuthReader()
	private := registerDevice(t, fake, "dev-1", 2)
	verifier := newVerifier(t, fake, time.Hour)

	// Revoked epoch: immediate rejection.
	revoked := fake.keys[keyRef("dev-1", 1)]
	revoked.Status = KeyRevoked
	fake.keys[keyRef("dev-1", 1)] = revoked
	_, _, err := verifier.VerifyTelemetry(context.Background(), "dev-1",
		signTelemetry(t, private, "dev-1", 1, positionPayload))
	var authErr *AuthError
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonKeyRevoked, authErr.Reason)

	// Unknown epoch.
	_, _, err = verifier.VerifyTelemetry(context.Background(), "dev-1",
		signTelemetry(t, private, "dev-1", 9, positionPayload))
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonEpochUnknown, authErr.Reason)
}

func TestVerifyTelemetryPreviousEpochGrace(t *testing.T) {
	fake := newFakeAuthReader()
	oldPublic, oldPrivate, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	registerDevice(t, fake, "dev-1", 2) // current epoch 2
	verifier := newVerifier(t, fake, time.Hour)

	// PREVIOUS within the grace window: accepted (offline device that has
	// not rotated in yet).
	fake.keys[keyRef("dev-1", 1)] = Key{DeviceID: "dev-1", Epoch: 1, PublicKey: oldPublic,
		Status: KeyPrevious, RotatedAt: time.Now().Add(-30 * time.Minute)}
	_, verified, err := verifier.VerifyTelemetry(context.Background(), "dev-1",
		signTelemetry(t, oldPrivate, "dev-1", 1, positionPayload))
	require.NoError(t, err)
	require.Equal(t, 1, verified.KeyEpoch)

	// PREVIOUS beyond grace: rejected.
	expired := fake.keys[keyRef("dev-1", 1)]
	expired.RotatedAt = time.Now().Add(-2 * time.Hour)
	fake.keys[keyRef("dev-1", 1)] = expired
	_, _, err = verifier.VerifyTelemetry(context.Background(), "dev-1",
		signTelemetry(t, oldPrivate, "dev-1", 1, positionPayload))
	var authErr *AuthError
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonGraceExpired, authErr.Reason)
}

func TestVerifyTelemetryRejectsMalformedAndMismatch(t *testing.T) {
	fake := newFakeAuthReader()
	private := registerDevice(t, fake, "dev-1", 1)
	verifier := newVerifier(t, fake, time.Hour)

	_, _, err := verifier.VerifyTelemetry(context.Background(), "dev-1", []byte("{not json"))
	var authErr *AuthError
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonEnvelopeMalformed, authErr.Reason)

	// Path/envelope device mismatch.
	_, _, err = verifier.VerifyTelemetry(context.Background(), "dev-9",
		signTelemetry(t, private, "dev-1", 1, positionPayload))
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonDeviceIDMismatch, authErr.Reason)

	// Unknown device.
	_, _, err = verifier.VerifyTelemetry(context.Background(), "dev-unknown",
		signTelemetry(t, private, "dev-unknown", 1, positionPayload))
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonDeviceUnknown, authErr.Reason)

	// Wrong envelope version.
	envelope, signErr := SignEnvelope(private, DeviceEnvelope{
		EnvelopeVersion: "0.9", DeviceID: "dev-1", KeyEpoch: 1,
		PayloadType: sign.EventVesselPosition, OccurredAt: time.Now().UTC(), Payload: positionPayload,
	})
	require.NoError(t, signErr)
	raw, _ := json.Marshal(envelope)
	_, _, err = verifier.VerifyTelemetry(context.Background(), "dev-1", raw)
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonEnvelopeMalformed, authErr.Reason)
}

func TestProofRoundTripAndActionGate(t *testing.T) {
	fake := newFakeAuthReader()
	private := registerDevice(t, fake, "dev-1", 1)
	verifier := newVerifier(t, fake, time.Hour)

	proof, err := SignPayload(private, KeyID("dev-1", 1), Proof{
		Action: ProofActionMQTTAuth, DeviceID: "dev-1", KeyEpoch: 1,
	})
	require.NoError(t, err)

	verified, err := verifier.VerifyProof(context.Background(), proof, ProofActionMQTTAuth)
	require.NoError(t, err)
	require.Equal(t, "dev-1", verified.Device.ID)

	// Wrong action fails closed.
	_, err = verifier.VerifyProof(context.Background(), proof, ProofActionFirmware)
	var authErr *AuthError
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonActionMismatch, authErr.Reason)

	// Forged proof (wrong key) fails.
	_, forger, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	forged, err := SignPayload(forger, KeyID("dev-1", 1), Proof{
		Action: ProofActionMQTTAuth, DeviceID: "dev-1", KeyEpoch: 1,
	})
	require.NoError(t, err)
	_, err = verifier.VerifyProof(context.Background(), forged, ProofActionMQTTAuth)
	require.ErrorAs(t, err, &authErr)
	require.Equal(t, ReasonSignatureInvalid, authErr.Reason)
}

func TestParseKeyID(t *testing.T) {
	deviceID, epoch, err := ParseKeyID("geo-device-2f4b6c80-1234-5678-9abc-deadbeef0001-3")
	require.NoError(t, err)
	require.Equal(t, "2f4b6c80-1234-5678-9abc-deadbeef0001", deviceID)
	require.Equal(t, 3, epoch)

	for _, bad := range []string{"", "geo-device-", "other-dev-1", "geo-device-dev-", "geo-device--3", "geo-device-dev-x"} {
		_, _, err := ParseKeyID(bad)
		require.Error(t, err, bad)
	}
}

func TestRolloutBucketDeterministic(t *testing.T) {
	first := RolloutBucket("release-1", "device-1")
	require.Equal(t, first, RolloutBucket("release-1", "device-1"))
	require.GreaterOrEqual(t, first, 0)
	require.Less(t, first, 100)
	// Distinct devices spread across buckets (not all identical).
	buckets := map[int]bool{}
	for i := 0; i < 200; i++ {
		buckets[RolloutBucket("release-1", fmt.Sprintf("device-%d", i))] = true
	}
	require.Greater(t, len(buckets), 50, "bucketing must spread devices")
}

func TestSelectReleaseGates(t *testing.T) {
	device := Device{ID: "dev-1", TenantID: "tenant-a", Kind: KindGT06, KeyEpoch: 3}
	release := Release{ID: "rel-1", TenantID: "tenant-a", Kind: KindGT06, Version: "1.2.0",
		RolloutPercent: 100, MinEpoch: 2, CreatedAt: time.Now()}
	require.NotNil(t, SelectRelease([]Release{release}, device))

	// min_epoch gate.
	gated := release
	gated.MinEpoch = 4
	require.Nil(t, SelectRelease([]Release{gated}, device))

	// 0% rollout targets nobody.
	zero := release
	zero.RolloutPercent = 0
	require.Nil(t, SelectRelease([]Release{zero}, device))

	// kind/tenant mismatch.
	wrongKind := release
	wrongKind.Kind = KindAIS
	require.Nil(t, SelectRelease([]Release{wrongKind}, device))
	wrongTenant := release
	wrongTenant.TenantID = "tenant-b"
	require.Nil(t, SelectRelease([]Release{wrongTenant}, device))

	// Newest first wins when both are eligible.
	older := release
	older.ID = "rel-0"
	older.Version = "1.1.0"
	older.CreatedAt = release.CreatedAt.Add(-time.Hour)
	require.Equal(t, "1.2.0", SelectRelease([]Release{release, older}, device).Version)
}

func TestRolloutMonotonicPerDevice(t *testing.T) {
	// A device inside a 60% rollout is also inside any larger rollout for
	// the same release.
	release := Release{ID: "rel-1", TenantID: "tenant-a", Kind: KindSensor, RolloutPercent: 60, MinEpoch: 1}
	device := Device{ID: "dev-x", TenantID: "tenant-a", Kind: KindSensor, KeyEpoch: 1}
	if SelectRelease([]Release{release}, device) != nil {
		wider := release
		wider.RolloutPercent = 90
		require.NotNil(t, SelectRelease([]Release{wider}, device))
	}
}

func TestSignManifestVerifies(t *testing.T) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	kid := "blueeconomy-geo-service-0"
	manifest, err := SignManifest(Manifest{
		DeviceID: "dev-1", Kind: KindAIS, Version: "2.0.0",
		ArtifactSHA256: strings.Repeat("a", 64), ArtifactURL: "https://artifacts.example/geo/ais-2.0.0.bin",
		RolloutPercent: 50, MinEpoch: 1, GeneratedAt: time.Now().UTC(),
	}, func(kid string, payload any) (string, error) {
		return SignPayload(private, kid, payload)
	}, kid)
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Signature)

	// The manifest verifies against the service public key; the payload is
	// the canonical manifest minus the signature field.
	unsigned := manifest
	unsigned.Signature = ""
	payloadBytes, err := verifyJWS(manifest.Signature, kid, private.Public().(ed25519.PublicKey))
	require.NoError(t, err)
	expected, err := canonicalJSON(unsigned)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(payloadBytes))
}

func TestValidateStatusTransition(t *testing.T) {
	require.NoError(t, ValidateStatusTransition(StatusActive, StatusSuspended))
	require.NoError(t, ValidateStatusTransition(StatusSuspended, StatusActive))
	require.NoError(t, ValidateStatusTransition(StatusActive, StatusRevoked))
	require.Error(t, ValidateStatusTransition(StatusRevoked, StatusActive), "REVOKED is terminal")
	require.Error(t, ValidateStatusTransition(StatusDecommissioned, StatusActive), "DECOMMISSIONED is terminal")
	require.Error(t, ValidateStatusTransition(StatusActive, StatusActive))
	require.Error(t, ValidateStatusTransition(StatusActive, "BOGUS"))
}

// ---------------------------------------------------------------------
// Handler-level tests (in-memory verifier + recording publishers)
// ---------------------------------------------------------------------

type recordedEvent struct {
	EventType      string
	CorrelationID  string
	Classification string
	Headers        map[string]string
}

type fakeEventPublisher struct {
	mu     sync.Mutex
	events []recordedEvent
	err    error
}

func (fake *fakeEventPublisher) PublishSignedEnvelope(_ context.Context, eventType, correlationID string, _ any, _ time.Time, classification string, headers map[string]string) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.err != nil {
		return fake.err
	}
	fake.events = append(fake.events, recordedEvent{eventType, correlationID, classification, headers})
	return nil
}

type deadLetterMessage struct {
	Topic string
	Key   string
	Value []byte
}

type fakeDeadLetters struct {
	mu       sync.Mutex
	messages []deadLetterMessage
}

func (fake *fakeDeadLetters) Publish(_ context.Context, topic, key string, value []byte, _ map[string]string) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.messages = append(fake.messages, deadLetterMessage{topic, key, value})
	return nil
}

func newHandlerHarness(t *testing.T) (*API, *fakeAuthReader, *fakeEventPublisher, *fakeDeadLetters, ed25519.PrivateKey) {
	t.Helper()
	fake := newFakeAuthReader()
	private := registerDevice(t, fake, "dev-1", 1)
	verifier := newVerifier(t, fake, time.Hour)
	events := &fakeEventPublisher{}
	letters := &fakeDeadLetters{}
	_, manifestKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	api, err := NewAPI(&API{
		Store: nil, Verifier: verifier, Metrics: metrics.NewRegistry(),
		Events: events, DeadLetters: letters,
		ManifestKey: manifestKey, ManifestKeyID: "blueeconomy-geo-service-0",
	})
	_ = api
	require.Error(t, err, "NewAPI must fail closed without a store")
	return &API{
		Verifier: verifier, Metrics: metrics.NewRegistry(),
		Events: events, DeadLetters: letters,
		ManifestKey: manifestKey, ManifestKeyID: "blueeconomy-geo-service-0",
		Grace: time.Hour, now: time.Now,
	}, fake, events, letters, private
}

func TestTelemetryHandlerPublishesPosition(t *testing.T) {
	api, _, events, letters, private := newHandlerHarness(t)
	request := httptest.NewRequest("POST", "/v1/devices/dev-1/telemetry",
		strings.NewReader(string(signTelemetry(t, private, "dev-1", 1, positionPayload))))
	request.SetPathValue("id", "dev-1")
	response := httptest.NewRecorder()
	api.ingestTelemetry(response, request)
	require.Equal(t, 202, response.Code)
	require.Len(t, events.events, 1)
	require.Equal(t, sign.EventVesselPosition, events.events[0].EventType)
	require.Equal(t, "dev-1", events.events[0].Headers["device-id"])
	require.Empty(t, letters.messages)
}

func TestTelemetryHandlerDeadLettersUnsupportedPayload(t *testing.T) {
	api, _, events, letters, private := newHandlerHarness(t)
	envelope, err := SignEnvelope(private, DeviceEnvelope{
		DeviceID: "dev-1", KeyEpoch: 1, PayloadType: "vendor.custom.v9",
		OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{"x":1}`),
	})
	require.NoError(t, err)
	raw, _ := json.Marshal(envelope)
	request := httptest.NewRequest("POST", "/v1/devices/dev-1/telemetry", strings.NewReader(string(raw)))
	request.SetPathValue("id", "dev-1")
	response := httptest.NewRecorder()
	api.ingestTelemetry(response, request)
	require.Equal(t, 202, response.Code)
	require.Empty(t, events.events)
	require.Len(t, letters.messages, 1)
	require.Equal(t, "vessels.quarantine", letters.messages[0].Topic)
	var record deadLetterRecord
	require.NoError(t, json.Unmarshal(letters.messages[0].Value, &record))
	require.Equal(t, ReasonPayloadUnsupported, record.Reason)
}

func TestTelemetryHandlerRejectsBadSignatureWithDeadLetter(t *testing.T) {
	api, _, _, letters, _ := newHandlerHarness(t)
	_, forger, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	request := httptest.NewRequest("POST", "/v1/devices/dev-1/telemetry",
		strings.NewReader(string(signTelemetry(t, forger, "dev-1", 1, positionPayload))))
	request.SetPathValue("id", "dev-1")
	response := httptest.NewRecorder()
	api.ingestTelemetry(response, request)
	require.Equal(t, 403, response.Code)
	require.Len(t, letters.messages, 1, "known-device verification failures dead-letter for forensics")
	var record deadLetterRecord
	require.NoError(t, json.Unmarshal(letters.messages[0].Value, &record))
	require.Equal(t, ReasonSignatureInvalid, record.Reason)
}

func TestTelemetryHandlerSuspendedDeviceForbidden(t *testing.T) {
	api, fake, _, _, private := newHandlerHarness(t)
	suspended := fake.devices["dev-1"]
	suspended.Status = StatusSuspended
	fake.devices["dev-1"] = suspended
	request := httptest.NewRequest("POST", "/v1/devices/dev-1/telemetry",
		strings.NewReader(string(signTelemetry(t, private, "dev-1", 1, positionPayload))))
	request.SetPathValue("id", "dev-1")
	response := httptest.NewRecorder()
	api.ingestTelemetry(response, request)
	require.Equal(t, 403, response.Code)
	require.Contains(t, response.Body.String(), ReasonDeviceSuspended)
}

func TestMQTTAuthAllowDeny(t *testing.T) {
	api, _, _, _, private := newHandlerHarness(t)

	allow := func(clientID, password string) map[string]any {
		body, _ := json.Marshal(map[string]string{"clientid": clientID, "username": clientID, "password": password})
		request := httptest.NewRequest("POST", "/v1/devices/mqtt-auth", strings.NewReader(string(body)))
		response := httptest.NewRecorder()
		api.serveMQTTAuth(response, request)
		require.Equal(t, 200, response.Code)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
		return decoded
	}

	proof, err := SignPayload(private, KeyID("dev-1", 1), Proof{
		Action: ProofActionMQTTAuth, DeviceID: "dev-1", KeyEpoch: 1,
	})
	require.NoError(t, err)

	decoded := allow("dev-1", proof)
	require.Equal(t, "allow", decoded["result"])
	require.Equal(t, false, decoded["is_superuser"], "superuser is always denied")

	// Wrong clientid for the proof.
	decoded = allow("dev-9", proof)
	require.Equal(t, "deny", decoded["result"])
	require.Equal(t, false, decoded["is_superuser"])

	// Garbage password.
	decoded = allow("dev-1", "not-a-jws")
	require.Equal(t, "deny", decoded["result"])

	// Empty clientid.
	decoded = allow("", proof)
	require.Equal(t, "deny", decoded["result"])
}

func TestNewAPIFailsClosed(t *testing.T) {
	_, err := NewAPI(&API{})
	require.Error(t, err)
	_, err = NewVerifier(nil, metrics.NewRegistry(), time.Hour)
	require.Error(t, err)
}

func TestErrorSentinels(t *testing.T) {
	require.True(t, errors.Is(ErrMakerChecker, ErrMakerChecker))
	require.NotEqual(t, ErrMakerChecker.Error(), ErrRequestNotPending.Error())
}
