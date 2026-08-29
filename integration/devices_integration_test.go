// Integration tests for the device-management plane against real
// Postgres+PostGIS (migrations 0001-0009), gated by GEO_TEST_PG_DSN /
// GEO_TEST_REDIS_ADDR like the pipeline suite; Kafka assertions activate
// when GEO_TEST_KAFKA_BROKERS is set (real producer + consumer for the
// telemetry DLQ), otherwise the recording stub captures publications.
//
// Covers: registry CRUD + RLS cross-tenant denial, provisioning
// maker/checker (self-approve refused in app AND SQL, consume-on-use),
// the envelope verify matrix against the real geo_devices role
// connection, telemetry ingest -> Kafka event / DLQ, rotation epoch
// transitions with grace, firmware rollout bucketing and the signed
// manifest, and the EMQX mqtt-auth webhook.
package integration

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/bus"
	"github.com/munisp/blueeconomy-geo-service/internal/devices"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
)

const (
	devTenantA = "itest-dev-tenant-a"
	devTenantB = "itest-dev-tenant-b"
)

// deviceEventRecorder is the SignedEnvelopePublisher stub used when no
// real broker is configured.
type deviceEventRecorder struct {
	mu     sync.Mutex
	events []deviceEvent
}

type deviceEvent struct {
	EventType      string
	Classification string
	Headers        map[string]string
	Payload        any
}

func (rec *deviceEventRecorder) PublishSignedEnvelope(_ context.Context, eventType, _ string, payload any, _ time.Time, classification string, headers map[string]string) error {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.events = append(rec.events, deviceEvent{eventType, classification, headers, payload})
	return nil
}

// deviceHarness wires the device plane against the env-provided
// infrastructure on top of the pipeline harness (two-DSN pattern: the app
// role for tenant-bound admin, the geo_devices role for verify-at-ingest).
type deviceHarness struct {
	pipe      *harness
	devices   *devices.Store
	api       http.Handler
	verifier  *devices.Verifier
	events    *deviceEventRecorder
	letters   *recorder
	signer    *sign.Signer
	manifest  ed25519.PublicKey
	grace     time.Duration
	proxyAuth auth.TrustedProxyAuthenticator
}

func newDeviceHarness(t *testing.T) *deviceHarness {
	t.Helper()
	h := newHarness(t) // skips unless GEO_TEST_PG_DSN + GEO_TEST_REDIS_ADDR
	ctx := context.Background()
	dsn := os.Getenv("GEO_TEST_PG_DSN")

	// Provision the geo_devices test password (idempotent) and derive the
	// device-plane DSN — mirroring the geo_ingest harness pattern.
	_, err := h.store.Pool().Exec(ctx, `ALTER ROLE geo_devices LOGIN PASSWORD 'geo_devices'`)
	require.NoError(t, err)
	deviceURL, err := url.Parse(dsn)
	require.NoError(t, err)
	deviceURL.User = url.UserPassword("geo_devices", "geo_devices")

	deviceStore, err := devices.NewStore(ctx, h.store, deviceURL.String())
	require.NoError(t, err)
	t.Cleanup(deviceStore.Close)

	registry := metrics.NewRegistry()
	grace := time.Hour
	verifier, err := devices.NewVerifier(deviceStore, registry, grace)
	require.NoError(t, err)

	events := &deviceEventRecorder{}
	letters := &recorder{}
	var deadLetters devices.DeadLetterPublisher = letters
	var eventPublisher devices.EventPublisher = events
	if brokers := os.Getenv("GEO_TEST_KAFKA_BROKERS"); brokers != "" {
		producer, err := bus.NewProducer(bus.Config{Brokers: strings.Split(brokers, ",")})
		require.NoError(t, err)
		t.Cleanup(func() { _ = producer.Close() })
		deadLetters = producer
		eventPublisher = h.pipeline // real signed-envelope pipeline
	}

	_, manifestKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	api, err := devices.NewAPI(&devices.API{
		Store: deviceStore, Verifier: verifier, Metrics: registry,
		Events: eventPublisher, DeadLetters: deadLetters,
		ManifestKey: manifestKey, ManifestKeyID: "blueeconomy-geo-service-0",
		Grace: grace,
	})
	require.NoError(t, err)
	_, network, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)
	proxyAuth := auth.TrustedProxyAuthenticator{CIDRs: []*net.IPNet{network}, Identity: "itest-proxy"}
	mux := http.NewServeMux()
	api.RegisterRoutes(mux, proxyAuth)
	return &deviceHarness{
		pipe: h, devices: deviceStore, api: mux, verifier: verifier,
		events: events, letters: letters, grace: grace,
		manifest: manifestKey.Public().(ed25519.PublicKey), proxyAuth: proxyAuth,
	}
}

// clean removes test rows for both device tenants (RLS default-deny
// requires tenant-bound cleanup; ”-tenant device-plane audit rows are
// deleted with an explicitly bound empty tenant).
func (h *deviceHarness) clean(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, tenant := range []string{devTenantA, devTenantB} {
		require.NoError(t, h.pipe.store.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			for _, statement := range []string{
				`DELETE FROM device_audit_events WHERE tenant_id = current_setting('app.tenant_id')`,
				`DELETE FROM device_keys WHERE device_id IN (SELECT id FROM devices)`,
				`DELETE FROM provisioning_requests`,
				`DELETE FROM firmware_releases`,
				`DELETE FROM devices`,
			} {
				if _, err := tx.Exec(ctx, statement); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	tx, err := h.pipe.store.Pool().Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `SELECT set_config('app.tenant_id', '', true)`)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, `DELETE FROM device_audit_events WHERE device_id IS NULL OR event = 'AUTH_FAILED'`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
}

// adminRequest issues one fleet-admin call through the trusted-proxy
// authenticator with the given subject/roles/tenant.
func (h *deviceHarness) adminRequest(t *testing.T, method, path, subject, roles, tenant string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = strings.NewReader(string(raw))
	} else {
		reader = strings.NewReader("")
	}
	request := httptest.NewRequest(method, path, reader)
	request.RemoteAddr = "127.0.0.1:43210"
	request.Header.Set("X-Blueeconomy-Authenticated-By", "itest-proxy")
	request.Header.Set("X-Blueeconomy-Authenticated-Subject", subject)
	request.Header.Set("X-Blueeconomy-Authenticated-Roles", roles)
	request.Header.Set("X-Blueeconomy-Authenticated-Clearance", "SECRET")
	request.Header.Set("X-Blueeconomy-Tenant-Id", tenant)
	response := httptest.NewRecorder()
	h.api.ServeHTTP(response, request)
	return response
}

// provisionDevice drives the full maker/checker flow and returns the
// device id, the activation secret and the device key pair.
func (h *deviceHarness) provisionDevice(t *testing.T, tenant, kind string) (string, string, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	response := h.adminRequest(t, "POST", "/v1/device-provisioning/requests", "itest-maker",
		"geo-device-maker", tenant, map[string]any{
			"kind": kind, "ownerAgency": "itest-agency", "label": "itest device",
			"publicKey": base64.RawURLEncoding.EncodeToString(public),
		})
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	var created struct {
		RequestID string `json:"requestId"`
		Status    string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &created))
	require.Equal(t, devices.RequestPending, created.Status)

	response = h.adminRequest(t, "POST", "/v1/device-provisioning/requests/"+created.RequestID+"/approve",
		"itest-checker", "geo-device-checker", tenant, nil)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var approved struct {
		Approval devices.Approval `json:"approval"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &approved))
	require.NotEmpty(t, approved.Approval.DeviceID)
	require.NotEmpty(t, approved.Approval.ActivationSecret)
	require.Equal(t, 1, approved.Approval.KeyEpoch)
	return approved.Approval.DeviceID, approved.Approval.ActivationSecret, private
}

func (h *deviceHarness) deviceEnvelope(t *testing.T, private ed25519.PrivateKey, deviceID string, epoch int, payloadType string, payload json.RawMessage) []byte {
	t.Helper()
	envelope, err := devices.SignEnvelope(private, devices.DeviceEnvelope{
		DeviceID: deviceID, KeyEpoch: epoch, PayloadType: payloadType,
		OccurredAt: time.Now().UTC(), Payload: payload,
	})
	require.NoError(t, err)
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	return raw
}

var itestPositionPayload = json.RawMessage(`{"positionReportId":"pr-itest-1","mmsi":"000001777","sourceClass":"AIS","latitudeMicros":6418000,"longitudeMicros":3372500,"speedOverGroundMilliknots":8400,"courseOverGroundMillidegrees":90000,"positionAccuracy":"HIGH","observedAt":"2025-01-01T00:00:00Z","receiverId":"itest-rx","classification":"PUBLIC"}`)

func (h *deviceHarness) postTelemetry(t *testing.T, deviceID string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest("POST", "/v1/devices/"+deviceID+"/telemetry", strings.NewReader(string(body)))
	response := httptest.NewRecorder()
	h.api.ServeHTTP(response, request)
	return response
}

func TestDeviceProvisioningFlowAndConsumeOnce(t *testing.T) {
	h := newDeviceHarness(t)
	h.clean(t)
	ctx := context.Background()

	deviceID, secret, _ := h.provisionDevice(t, devTenantA, devices.KindAIS)

	// The one-time secret activates exactly once (consume-on-use).
	activate := func(requestID, sec string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]string{"requestId": requestID, "activationSecret": sec})
		request := httptest.NewRequest("POST", "/v1/devices/activate", strings.NewReader(string(body)))
		response := httptest.NewRecorder()
		h.api.ServeHTTP(response, request)
		return response
	}
	// The request id is discoverable through the tenant-scoped read.
	var requestID string
	require.NoError(t, h.pipe.store.WithTenant(ctx, devTenantA, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT id FROM provisioning_requests WHERE device_id = $1`, deviceID).Scan(&requestID)
	}))
	response := activate(requestID, "wrong-secret")
	require.Equal(t, http.StatusConflict, response.Code)
	response = activate(requestID, secret)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	response = activate(requestID, secret)
	require.Equal(t, http.StatusConflict, response.Code, "already consumed must fail")

	// Consume flipped the request to CONSUMED; the secret stays hashed at
	// rest (the raw secret never persists).
	var status, hash string
	require.NoError(t, h.pipe.store.WithTenant(ctx, devTenantA, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, encode(activation_secret_hash, 'hex')
			FROM provisioning_requests WHERE id = $1`, requestID).Scan(&status, &hash)
	}))
	require.Equal(t, devices.RequestConsumed, status)
	require.Len(t, hash, 64)
}

func TestDeviceProvisioningMakerChecker(t *testing.T) {
	h := newDeviceHarness(t)
	h.clean(t)
	ctx := context.Background()

	public, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	response := h.adminRequest(t, "POST", "/v1/device-provisioning/requests", "itest-maker",
		"geo-device-maker", devTenantA, map[string]any{
			"kind": devices.KindGT06, "ownerAgency": "itest-agency", "label": "four-eyes device",
			"publicKey": base64.RawURLEncoding.EncodeToString(public),
		})
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	var created struct {
		RequestID string `json:"requestId"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &created))

	// Self-approval is refused by the application (four-eyes).
	response = h.adminRequest(t, "POST", "/v1/device-provisioning/requests/"+created.RequestID+"/approve",
		"itest-maker", "geo-device-checker", devTenantA, nil)
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "four-eyes")

	// And by SQL: decided_by = requested_by violates the CHECK constraint.
	err = h.pipe.store.WithTenant(ctx, devTenantA, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE provisioning_requests
			SET decided_by = requested_by, decided_at = now() WHERE id = $1`, created.RequestID)
		return err
	})
	require.Error(t, err, "SQL must refuse maker == checker")

	// A distinct checker approves; a second decision is refused
	// (consume-on-use on the decision itself).
	response = h.adminRequest(t, "POST", "/v1/device-provisioning/requests/"+created.RequestID+"/approve",
		"itest-checker", "geo-device-checker", devTenantA, nil)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	response = h.adminRequest(t, "POST", "/v1/device-provisioning/requests/"+created.RequestID+"/approve",
		"itest-checker-2", "geo-device-checker", devTenantA, nil)
	require.Equal(t, http.StatusConflict, response.Code)

	// Reject flow on a fresh request.
	response = h.adminRequest(t, "POST", "/v1/device-provisioning/requests", "itest-maker",
		"geo-device-maker", devTenantA, map[string]any{
			"kind": devices.KindSensor, "ownerAgency": "itest-agency", "label": "reject me",
			"publicKey": base64.RawURLEncoding.EncodeToString(public),
		})
	require.Equal(t, http.StatusAccepted, response.Code)
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &created))
	response = h.adminRequest(t, "POST", "/v1/device-provisioning/requests/"+created.RequestID+"/reject",
		"itest-checker", "geo-device-checker", devTenantA, nil)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	// Audit ledger recorded the decisions (tenant-scoped read).
	var approvedCount, rejectedCount int
	require.NoError(t, h.pipe.store.WithTenant(ctx, devTenantA, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM device_audit_events
			WHERE event = 'PROVISIONING_APPROVED'`).Scan(&approvedCount); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT count(*) FROM device_audit_events
			WHERE event = 'PROVISIONING_REJECTED'`).Scan(&rejectedCount)
	}))
	require.GreaterOrEqual(t, approvedCount, 1)
	require.Equal(t, 1, rejectedCount)
}

func TestDeviceRegistryRLSCrossTenant(t *testing.T) {
	h := newDeviceHarness(t)
	h.clean(t)
	ctx := context.Background()

	deviceID, _, _ := h.provisionDevice(t, devTenantA, devices.KindLoRaWAN)

	// Tenant A sees its device through the API.
	response := h.adminRequest(t, "GET", "/v1/devices/"+deviceID, "itest-reader",
		"geo-device-reader", devTenantA, nil)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	// Tenant B cannot read it (RLS cross-tenant denial), cannot see the
	// provisioning request and lists an empty registry.
	response = h.adminRequest(t, "GET", "/v1/devices/"+deviceID, "itest-reader",
		"geo-device-reader", devTenantB, nil)
	require.Equal(t, http.StatusNotFound, response.Code)
	response = h.adminRequest(t, "GET", "/v1/devices", "itest-reader",
		"geo-device-reader", devTenantB, nil)
	require.Equal(t, http.StatusOK, response.Code)
	require.NotContains(t, response.Body.String(), deviceID)

	// An unbound session is default-deny on every device table.
	var count int
	require.NoError(t, h.pipe.store.Pool().QueryRow(ctx, `SELECT count(*) FROM devices`).Scan(&count))
	require.Equal(t, 0, count, "unbound session must see no devices (default deny)")
	require.NoError(t, h.pipe.store.Pool().QueryRow(ctx, `SELECT count(*) FROM device_keys`).Scan(&count))
	require.Equal(t, 0, count)
	require.NoError(t, h.pipe.store.Pool().QueryRow(ctx, `SELECT count(*) FROM provisioning_requests`).Scan(&count))
	require.Equal(t, 0, count)

	// The geo_devices role (verify-at-ingest) reads across tenants BY
	// DESIGN but holds no write grants on the registry.
	device, err := h.devices.LoadDeviceForAuth(ctx, deviceID)
	require.NoError(t, err)
	require.Equal(t, devices.StatusActive, device.Status)
	_, err = h.devices.DevicePlanePool().Exec(ctx, `UPDATE devices SET status = 'SUSPENDED' WHERE id = $1`, deviceID)
	require.Error(t, err, "geo_devices must not write the registry")
	_, err = h.devices.DevicePlanePool().Exec(ctx, `DELETE FROM device_keys WHERE device_id = $1`, deviceID)
	require.Error(t, err, "geo_devices must not delete key epochs")
}

func TestDeviceTelemetryIngestAndDLQ(t *testing.T) {
	h := newDeviceHarness(t)
	h.clean(t)
	ctx := context.Background()

	deviceID, _, private := h.provisionDevice(t, devTenantA, devices.KindAIS)

	// Valid envelope -> accepted, forwarded to the event pipeline.
	response := h.postTelemetry(t, deviceID,
		h.deviceEnvelope(t, private, deviceID, 1, sign.EventVesselPosition, itestPositionPayload))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "published")

	if os.Getenv("GEO_TEST_KAFKA_BROKERS") == "" {
		require.Len(t, h.events.events, 1)
		require.Equal(t, sign.EventVesselPosition, h.events.events[0].EventType)
		require.Equal(t, deviceID, h.events.events[0].Headers["device-id"])
	}

	// Bad signature on a known device -> 403 + dead-letter with reason.
	_, forger, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	response = h.postTelemetry(t, deviceID,
		h.deviceEnvelope(t, forger, deviceID, 1, sign.EventVesselPosition, itestPositionPayload))
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Contains(t, response.Body.String(), devices.ReasonSignatureInvalid)

	// Unsupported payload type (authentic envelope) -> 202 dead-lettered.
	response = h.postTelemetry(t, deviceID,
		h.deviceEnvelope(t, private, deviceID, 1, "vendor.custom.v9", json.RawMessage(`{"x":1}`)))
	require.Equal(t, http.StatusAccepted, response.Code)
	require.Contains(t, response.Body.String(), devices.ReasonPayloadUnsupported)

	if os.Getenv("GEO_TEST_KAFKA_BROKERS") == "" {
		// Two dead-letters: bad signature + unsupported payload.
		letters := h.letters.byTopic(bus.TopicVesselQuarantine)
		require.Len(t, letters, 2)
		reasons := map[string]bool{}
		for _, message := range letters {
			require.Equal(t, deviceID, message.Key)
			var record map[string]any
			require.NoError(t, json.Unmarshal(message.Value, &record))
			reasons[record["reason"].(string)] = true
		}
		require.True(t, reasons[devices.ReasonSignatureInvalid])
		require.True(t, reasons[devices.ReasonPayloadUnsupported])
	} else {
		// Real broker: consume the DLQ record for this device.
		brokers := strings.Split(os.Getenv("GEO_TEST_KAFKA_BROKERS"), ",")
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, Topic: bus.TopicVesselQuarantine, Partition: 0,
			StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 2 * time.Second,
		})
		defer reader.Close()
		deadline := time.Now().Add(20 * time.Second)
		found := map[string]bool{}
		for time.Now().Before(deadline) && len(found) < 2 {
			fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			message, err := reader.FetchMessage(fetchCtx)
			cancel()
			if err != nil {
				break
			}
			if string(message.Key) != deviceID {
				continue
			}
			var record map[string]any
			require.NoError(t, json.Unmarshal(message.Value, &record))
			found[record["reason"].(string)] = true
		}
		require.True(t, found[devices.ReasonSignatureInvalid], "bad-signature DLQ record must reach Kafka")
		require.True(t, found[devices.ReasonPayloadUnsupported], "unsupported-payload DLQ record must reach Kafka")
	}

	// Auth outcomes are audited (failures); the ledger row carries the
	// device's tenant, so it is read through the tenant-bound app path.
	var failCount int
	require.NoError(t, h.pipe.store.WithTenant(ctx, devTenantA, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM device_audit_events
			WHERE device_id = $1 AND event = 'AUTH_FAILED' AND reason = 'signature_invalid'`, deviceID).Scan(&failCount)
	}))
	require.Equal(t, 1, failCount)
}

func TestDeviceRotationEpochTransitions(t *testing.T) {
	h := newDeviceHarness(t)
	h.clean(t)
	ctx := context.Background()

	deviceID, _, oldPrivate := h.provisionDevice(t, devTenantA, devices.KindGT06)
	newPublic, newPrivate, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	// Maker requests the rotation (202 PENDING); self-approval refused.
	response := h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/rotate", "itest-maker",
		"geo-device-maker", devTenantA, map[string]any{
			"publicKey": base64.RawURLEncoding.EncodeToString(newPublic),
		})
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	var created struct {
		RequestID string `json:"requestId"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &created))
	response = h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/rotate/"+created.RequestID+"/approve",
		"itest-maker", "geo-device-checker", devTenantA, nil)
	require.Equal(t, http.StatusConflict, response.Code, "rotation maker may not self-approve")

	// Checker applies: epoch 2 CURRENT, epoch 1 PREVIOUS inside grace.
	response = h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/rotate/"+created.RequestID+"/approve",
		"itest-checker", "geo-device-checker", devTenantA, nil)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"keyEpoch":2`)
	var currentEpoch int
	var oldStatus, newStatus string
	require.NoError(t, h.pipe.store.WithTenant(ctx, devTenantA, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT key_epoch FROM devices WHERE id = $1`, deviceID).Scan(&currentEpoch); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT status FROM device_keys WHERE device_id = $1 AND epoch = 1`, deviceID).Scan(&oldStatus); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT status FROM device_keys WHERE device_id = $1 AND epoch = 2`, deviceID).Scan(&newStatus)
	}))
	require.Equal(t, 2, currentEpoch)
	require.Equal(t, devices.KeyPrevious, oldStatus)
	require.Equal(t, devices.KeyCurrent, newStatus)

	// The new epoch authenticates; the PREVIOUS epoch still does (grace).
	response = h.postTelemetry(t, deviceID,
		h.deviceEnvelope(t, newPrivate, deviceID, 2, sign.EventVesselPosition, itestPositionPayload))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	response = h.postTelemetry(t, deviceID,
		h.deviceEnvelope(t, oldPrivate, deviceID, 1, sign.EventVesselPosition, itestPositionPayload))
	require.Equal(t, http.StatusAccepted, response.Code, "PREVIOUS epoch inside grace must authenticate")

	// A second rotation revokes epoch 1 when its grace has already closed.
	// Simulate the elapsed grace by backdating rotated_at beyond the
	// verifier's grace window, then rotate again.
	require.NoError(t, h.pipe.store.WithTenant(ctx, devTenantA, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE device_keys SET rotated_at = now() - interval '2 hours'
			WHERE device_id = $1 AND epoch = 1`, deviceID)
		return err
	}))
	// Post-grace PREVIOUS is rejected at verify time.
	response = h.postTelemetry(t, deviceID,
		h.deviceEnvelope(t, oldPrivate, deviceID, 1, sign.EventVesselPosition, itestPositionPayload))
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Contains(t, response.Body.String(), devices.ReasonGraceExpired)

	newPublic3, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	response = h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/rotate", "itest-maker",
		"geo-device-maker", devTenantA, map[string]any{
			"publicKey": base64.RawURLEncoding.EncodeToString(newPublic3),
		})
	require.Equal(t, http.StatusAccepted, response.Code)
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &created))
	response = h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/rotate/"+created.RequestID+"/approve",
		"itest-checker", "geo-device-checker", devTenantA, nil)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	// Epoch 1 was beyond grace at rotation time: revoked immediately.
	require.NoError(t, h.pipe.store.WithTenant(ctx, devTenantA, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM device_keys WHERE device_id = $1 AND epoch = 1`, deviceID).Scan(&oldStatus)
	}))
	require.Equal(t, devices.KeyRevoked, oldStatus)
}

func TestDeviceLifecycleGatesAuth(t *testing.T) {
	h := newDeviceHarness(t)
	h.clean(t)

	deviceID, _, private := h.provisionDevice(t, devTenantA, devices.KindPOS)

	// Suspension denies telemetry immediately (403 + audit).
	response := h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/status", "itest-admin",
		"geo-device-admin", devTenantA, map[string]any{"status": "SUSPENDED"})
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	response = h.postTelemetry(t, deviceID,
		h.deviceEnvelope(t, private, deviceID, 1, sign.EventVesselPosition, itestPositionPayload))
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Contains(t, response.Body.String(), devices.ReasonDeviceSuspended)

	// Reinstate, then revoke: revocation is immediate and terminal, and
	// revokes every key epoch.
	response = h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/status", "itest-admin",
		"geo-device-admin", devTenantA, map[string]any{"status": "ACTIVE"})
	require.Equal(t, http.StatusOK, response.Code)
	response = h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/status", "itest-admin",
		"geo-device-admin", devTenantA, map[string]any{"status": "REVOKED"})
	require.Equal(t, http.StatusOK, response.Code)
	response = h.postTelemetry(t, deviceID,
		h.deviceEnvelope(t, private, deviceID, 1, sign.EventVesselPosition, itestPositionPayload))
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Contains(t, response.Body.String(), devices.ReasonDeviceRevoked)
	response = h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/status", "itest-admin",
		"geo-device-admin", devTenantA, map[string]any{"status": "ACTIVE"})
	require.Equal(t, http.StatusConflict, response.Code, "REVOKED is terminal")
}

func TestDeviceFirmwareManifestAndRollout(t *testing.T) {
	h := newDeviceHarness(t)
	h.clean(t)

	deviceID, _, private := h.provisionDevice(t, devTenantA, devices.KindSensor)

	proof := func() string {
		proof, err := devices.SignPayload(private, devices.KeyID(deviceID, 1), devices.Proof{
			Action: devices.ProofActionFirmware, DeviceID: deviceID, KeyEpoch: 1,
		})
		require.NoError(t, err)
		return proof
	}
	getFirmware := func(authorization string) *httptest.ResponseRecorder {
		request := httptest.NewRequest("GET", "/v1/devices/"+deviceID+"/firmware", nil)
		if authorization != "" {
			request.Header.Set("Authorization", authorization)
		}
		response := httptest.NewRecorder()
		h.api.ServeHTTP(response, request)
		return response
	}

	// No proof -> 401; garbage proof -> 403.
	require.Equal(t, http.StatusUnauthorized, getFirmware("").Code)
	require.Equal(t, http.StatusForbidden, getFirmware("Device garbage").Code)

	// No releases yet -> 204.
	require.Equal(t, http.StatusNoContent, getFirmware("Device "+proof()).Code)

	// A 0% rollout release targets nobody.
	sha := strings.Repeat("b", 64)
	response := h.adminRequest(t, "POST", "/v1/devices/firmware-releases", "itest-admin",
		"geo-device-admin", devTenantA, map[string]any{
			"kind": devices.KindSensor, "version": "1.0.0", "artifactSha256": sha,
			"artifactUrl": "https://artifacts.example/geo/sensor-1.0.0.bin", "rolloutPercent": 0, "minEpoch": 1,
		})
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	require.Equal(t, http.StatusNoContent, getFirmware("Device "+proof()).Code)

	// A min_epoch above the device epoch gates the release.
	response = h.adminRequest(t, "POST", "/v1/devices/firmware-releases", "itest-admin",
		"geo-device-admin", devTenantA, map[string]any{
			"kind": devices.KindSensor, "version": "1.1.0", "artifactSha256": sha,
			"artifactUrl": "https://artifacts.example/geo/sensor-1.1.0.bin", "rolloutPercent": 100, "minEpoch": 2,
		})
	require.Equal(t, http.StatusCreated, response.Code)
	require.Equal(t, http.StatusNoContent, getFirmware("Device "+proof()).Code)

	// A 100% rollout at min_epoch 1 serves the signed manifest.
	response = h.adminRequest(t, "POST", "/v1/devices/firmware-releases", "itest-admin",
		"geo-device-admin", devTenantA, map[string]any{
			"kind": devices.KindSensor, "version": "1.2.0", "artifactSha256": sha,
			"artifactUrl": "https://artifacts.example/geo/sensor-1.2.0.bin", "rolloutPercent": 100, "minEpoch": 1,
		})
	require.Equal(t, http.StatusCreated, response.Code)
	response = getFirmware("Device " + proof())
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var manifest devices.Manifest
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &manifest))
	require.Equal(t, "1.2.0", manifest.Version)
	require.Equal(t, sha, manifest.ArtifactSHA256)
	require.NotEmpty(t, manifest.Signature)
	// The manifest JWS verifies against the service public key.
	require.NoError(t, devices.VerifyManifest(manifest, h.manifest, "blueeconomy-geo-service-0"))
}

func TestDeviceMQTTAuthWebhook(t *testing.T) {
	h := newDeviceHarness(t)
	h.clean(t)

	deviceID, _, private := h.provisionDevice(t, devTenantA, devices.KindGateway)
	proof, err := devices.SignPayload(private, devices.KeyID(deviceID, 1), devices.Proof{
		Action: devices.ProofActionMQTTAuth, DeviceID: deviceID, KeyEpoch: 1,
	})
	require.NoError(t, err)

	call := func(clientID, password string) map[string]any {
		body, _ := json.Marshal(map[string]string{"clientid": clientID, "username": clientID, "password": password})
		request := httptest.NewRequest("POST", "/v1/devices/mqtt-auth", strings.NewReader(string(body)))
		response := httptest.NewRecorder()
		h.api.ServeHTTP(response, request)
		require.Equal(t, http.StatusOK, response.Code)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
		return decoded
	}

	decoded := call(deviceID, proof)
	require.Equal(t, "allow", decoded["result"])
	require.Equal(t, false, decoded["is_superuser"], "superuser is always denied")

	decoded = call(deviceID, "Device "+proof)
	require.Equal(t, "allow", decoded["result"], "Device-prefixed password is accepted")

	decoded = call("00000000-0000-0000-0000-000000000000", proof)
	require.Equal(t, "deny", decoded["result"], "clientid must match the proof device")
	require.Equal(t, false, decoded["is_superuser"])

	decoded = call(deviceID, "not-a-proof")
	require.Equal(t, "deny", decoded["result"])

	// A suspended device is denied by the same verification path.
	response := h.adminRequest(t, "POST", "/v1/devices/"+deviceID+"/status", "itest-admin",
		"geo-device-admin", devTenantA, map[string]any{"status": "SUSPENDED"})
	require.Equal(t, http.StatusOK, response.Code)
	decoded = call(deviceID, proof)
	require.Equal(t, "deny", decoded["result"])
}
