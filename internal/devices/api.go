// HTTP boundary of the device plane. Two authentication models share one
// mux: fleet-admin endpoints ride the platform principal middleware
// (geo-device-maker / geo-device-checker / geo-device-reader /
// geo-device-admin roles, tenant-bound RLS), while device endpoints
// (telemetry ingest, firmware fetch, EMQX mqtt-auth, activation)
// authenticate the device itself by its Ed25519 signed envelope/proof or
// the one-time activation secret. Every path fails closed.
package devices

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/bus"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
)

// EventPublisher publishes service-signed geo.*.v1 envelopes (the hot-path
// connectors.Pipeline in production; a recording stub in tests).
type EventPublisher interface {
	PublishSignedEnvelope(ctx context.Context, eventType, correlationID string, payload any, occurredAt time.Time, classification string, headers map[string]string) error
}

// DeadLetterPublisher is the raw Kafka boundary for dead-letter records
// (bus.Producer in production).
type DeadLetterPublisher interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
}

// API wires the device-plane handlers.
type API struct {
	Store       *Store
	Verifier    *Verifier
	Metrics     *metrics.Registry
	Events      EventPublisher
	DeadLetters DeadLetterPublisher
	// ManifestKey/ManifestKeyID sign OTA manifests with the service
	// envelope key (JWS-EdDSA, kid blueeconomy-geo-service-<epoch>).
	ManifestKey   ed25519.PrivateKey
	ManifestKeyID string
	// Grace is the key-rotation grace window (default 24h).
	Grace time.Duration
	now   func() time.Time
}

// NewAPI validates the wiring fail-closed.
func NewAPI(api *API) (*API, error) {
	if api.Store == nil || api.Verifier == nil || api.Metrics == nil {
		return nil, errors.New("devices: store, verifier and metrics registry are required")
	}
	if api.Events == nil {
		return nil, errors.New("devices: event publisher is required")
	}
	if api.DeadLetters == nil {
		return nil, errors.New("devices: dead-letter publisher is required")
	}
	if len(api.ManifestKey) != ed25519.PrivateKeySize || strings.TrimSpace(api.ManifestKeyID) == "" {
		return nil, errors.New("devices: manifest signing key and key id are required")
	}
	if api.Grace <= 0 {
		api.Grace = DefaultKeyGrace
	}
	api.now = time.Now
	return api, nil
}

// RegisterRoutes mounts the device-plane route tree. Admin routes wrap the
// platform authenticator per-route (the device endpoints authenticate by
// signed proof instead and must stay OUTSIDE the principal middleware).
func (api *API) RegisterRoutes(mux *http.ServeMux, authenticator auth.Authenticator) {
	admin := func(pattern string, handler http.HandlerFunc, roles ...string) {
		mux.Handle(pattern, auth.Middleware(authenticator,
			auth.RequireRoles(http.HandlerFunc(handler), roles...)))
	}
	readRoles := []string{"geo-device-reader", "geo-device-maker", "geo-device-checker", "geo-device-admin"}
	admin("POST /v1/devices/provisioning-requests", api.createProvisionRequest, "geo-device-maker", "geo-device-admin")
	admin("GET /v1/devices/provisioning-requests/{id}", api.getProvisionRequest, readRoles...)
	admin("POST /v1/devices/provisioning-requests/{id}/approve", api.approveProvisionRequest, "geo-device-checker", "geo-device-admin")
	admin("POST /v1/devices/provisioning-requests/{id}/reject", api.rejectProvisionRequest, "geo-device-checker", "geo-device-admin")
	admin("GET /v1/devices", api.listDevices, readRoles...)
	admin("GET /v1/devices/{id}", api.getDevice, readRoles...)
	admin("POST /v1/devices/{id}/status", api.setDeviceStatus, "geo-device-admin")
	admin("POST /v1/devices/{id}/rotate", api.requestRotation, "geo-device-maker", "geo-device-admin")
	admin("POST /v1/devices/{id}/rotate/{requestId}/approve", api.approveRotation, "geo-device-checker", "geo-device-admin")
	admin("POST /v1/devices/firmware-releases", api.createFirmwareRelease, "geo-device-admin")

	// Device-authenticated endpoints (signed envelope/proof or one-time
	// activation secret; no platform principal).
	mux.HandleFunc("POST /v1/devices/{id}/telemetry", api.ingestTelemetry)
	mux.HandleFunc("GET /v1/devices/{id}/firmware", api.serveFirmware)
	mux.HandleFunc("POST /v1/devices/mqtt-auth", api.serveMQTTAuth)
	mux.HandleFunc("POST /v1/devices/activate", api.activate)
}

// ---------------------------------------------------------------------
// Fleet-admin endpoints (principal + tenant bound)
// ---------------------------------------------------------------------

func principalOrFail(writer http.ResponseWriter, request *http.Request) (auth.Principal, bool) {
	principal, ok := auth.PrincipalFrom(request.Context())
	if !ok {
		writeDeviceError(writer, http.StatusForbidden, "principal unavailable")
		return auth.Principal{}, false
	}
	if strings.TrimSpace(principal.TenantID) == "" {
		writeDeviceError(writer, http.StatusForbidden, "principal has no tenant binding")
		return auth.Principal{}, false
	}
	return principal, true
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, target any, limit int64) bool {
	body, err := io.ReadAll(io.LimitReader(request.Body, limit))
	if err != nil {
		writeDeviceError(writer, http.StatusBadRequest, "request body unreadable")
		return false
	}
	if err := json.Unmarshal(body, target); err != nil {
		writeDeviceError(writer, http.StatusBadRequest, "request body is not valid JSON")
		return false
	}
	return true
}

type provisionRequestBody struct {
	Kind        string            `json:"kind"`
	OwnerAgency string            `json:"ownerAgency"`
	Label       string            `json:"label"`
	PublicKey   string            `json:"publicKey"`
	Metadata    map[string]string `json:"metadata"`
}

// createProvisionRequest: POST /v1/devices/provisioning-requests (maker).
// 202 PENDING mirrors the platform credential-verification pattern: the
// resource exists only as a pending decision until a checker acts.
func (api *API) createProvisionRequest(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var body provisionRequestBody
	if !decodeJSON(writer, request, &body, 1<<16) {
		return
	}
	id, err := api.Store.CreateProvisionRequest(request.Context(), principal.TenantID, ProvisionPayload{
		Kind: body.Kind, OwnerAgency: body.OwnerAgency, Label: body.Label,
		PublicKey: body.PublicKey, Metadata: body.Metadata,
	}, principal.Subject)
	if err != nil {
		writeDeviceError(writer, http.StatusBadRequest, "provisioning request rejected: "+err.Error())
		return
	}
	api.audit(request, AuditEvent{TenantID: principal.TenantID, Event: AuditProvisionRequested,
		Actor: principal.Subject, Metadata: map[string]string{"requestId": id, "kind": body.Kind}})
	api.Metrics.Inc("geo_device_provisioning_total", map[string]string{"decision": "requested"})
	writeDeviceJSON(writer, http.StatusAccepted, map[string]any{"requestId": id, "status": RequestPending})
}

func (api *API) getProvisionRequest(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	stored, err := api.Store.GetRequest(request.Context(), principal.TenantID, request.PathValue("id"))
	switch {
	case errors.Is(err, ErrRequestNotFound):
		writeDeviceError(writer, http.StatusNotFound, "provisioning request not found")
	case err != nil:
		writeDeviceError(writer, http.StatusInternalServerError, "provisioning request query failed")
	default:
		writeDeviceJSON(writer, http.StatusOK, map[string]any{"request": stored})
	}
}

// approveProvisionRequest: POST .../approve (checker; maker != checker).
// The one-time activation secret is returned exactly once, in this
// response only.
func (api *API) approveProvisionRequest(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	approval, err := api.Store.ApproveRequest(request.Context(), principal.TenantID,
		request.PathValue("id"), principal.Subject, api.Grace)
	switch {
	case errors.Is(err, ErrMakerChecker):
		writeDeviceError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, ErrRequestNotPending):
		writeDeviceError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, ErrRequestNotFound):
		writeDeviceError(writer, http.StatusNotFound, err.Error())
	case err != nil:
		writeDeviceError(writer, http.StatusInternalServerError, "provisioning approval failed")
	default:
		api.audit(request, AuditEvent{TenantID: principal.TenantID, DeviceID: approval.DeviceID,
			Event: AuditProvisionApproved, Actor: principal.Subject,
			Metadata: map[string]string{"requestId": approval.RequestID}})
		api.Metrics.Inc("geo_device_provisioning_total", map[string]string{"decision": "approved"})
		writeDeviceJSON(writer, http.StatusOK, map[string]any{"approval": approval})
	}
}

func (api *API) rejectProvisionRequest(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	stored, err := api.Store.RejectRequest(request.Context(), principal.TenantID,
		request.PathValue("id"), principal.Subject)
	switch {
	case errors.Is(err, ErrMakerChecker):
		writeDeviceError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, ErrRequestNotPending):
		writeDeviceError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, ErrRequestNotFound):
		writeDeviceError(writer, http.StatusNotFound, err.Error())
	case err != nil:
		writeDeviceError(writer, http.StatusInternalServerError, "provisioning rejection failed")
	default:
		api.audit(request, AuditEvent{TenantID: principal.TenantID, Event: AuditProvisionRejected,
			Actor: principal.Subject, Metadata: map[string]string{"requestId": stored.ID}})
		api.Metrics.Inc("geo_device_provisioning_total", map[string]string{"decision": "rejected"})
		writeDeviceJSON(writer, http.StatusOK, map[string]any{"requestId": stored.ID, "status": stored.Status})
	}
}

func (api *API) listDevices(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	devices, err := api.Store.ListDevices(request.Context(), principal.TenantID, 0)
	if err != nil {
		writeDeviceError(writer, http.StatusInternalServerError, "device query failed")
		return
	}
	writeDeviceJSON(writer, http.StatusOK, map[string]any{"devices": devices})
}

func (api *API) getDevice(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	device, err := api.Store.GetDevice(request.Context(), principal.TenantID, request.PathValue("id"))
	switch {
	case errors.Is(err, ErrDeviceNotFound):
		writeDeviceError(writer, http.StatusNotFound, "device not found")
	case err != nil:
		writeDeviceError(writer, http.StatusInternalServerError, "device query failed")
	default:
		writeDeviceJSON(writer, http.StatusOK, map[string]any{"device": device})
	}
}

type statusRequestBody struct {
	Status string `json:"status"`
}

// setDeviceStatus: POST /v1/devices/{id}/status (fleet admin, immediate —
// suspension and revocation take effect at once; terminal states are
// enforced by the store).
func (api *API) setDeviceStatus(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var body statusRequestBody
	if !decodeJSON(writer, request, &body, 1<<12) {
		return
	}
	device, err := api.Store.SetDeviceStatus(request.Context(), principal.TenantID,
		request.PathValue("id"), strings.ToUpper(strings.TrimSpace(body.Status)), principal.Subject)
	switch {
	case errors.Is(err, ErrDeviceNotFound):
		writeDeviceError(writer, http.StatusNotFound, "device not found")
	case err != nil:
		writeDeviceError(writer, http.StatusConflict, "device status transition rejected: "+err.Error())
	default:
		api.audit(request, AuditEvent{TenantID: principal.TenantID, DeviceID: device.ID,
			Event: AuditStatusChanged, Actor: principal.Subject,
			Metadata: map[string]string{"status": device.Status}})
		writeDeviceJSON(writer, http.StatusOK, map[string]any{"device": device})
	}
}

type rotationRequestBody struct {
	PublicKey string `json:"publicKey"`
}

// requestRotation: POST /v1/devices/{id}/rotate (maker) — the rotation is
// a PENDING four-eyes request until a checker approves it.
func (api *API) requestRotation(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var body rotationRequestBody
	if !decodeJSON(writer, request, &body, 1<<12) {
		return
	}
	deviceID := request.PathValue("id")
	// The device must be visible (and ACTIVE) inside the maker's tenant.
	if _, err := api.Store.GetDevice(request.Context(), principal.TenantID, deviceID); errors.Is(err, ErrDeviceNotFound) {
		writeDeviceError(writer, http.StatusNotFound, "device not found")
		return
	} else if err != nil {
		writeDeviceError(writer, http.StatusInternalServerError, "device query failed")
		return
	}
	id, err := api.Store.CreateRotationRequest(request.Context(), principal.TenantID, deviceID, body.PublicKey, principal.Subject)
	if err != nil {
		writeDeviceError(writer, http.StatusBadRequest, "rotation request rejected: "+err.Error())
		return
	}
	api.audit(request, AuditEvent{TenantID: principal.TenantID, DeviceID: deviceID,
		Event: AuditRotationRequested, Actor: principal.Subject, Metadata: map[string]string{"requestId": id}})
	api.Metrics.Inc("geo_device_rotations_total", map[string]string{"result": "requested"})
	writeDeviceJSON(writer, http.StatusAccepted, map[string]any{"requestId": id, "status": RequestPending})
}

// approveRotation: POST /v1/devices/{id}/rotate/{requestId}/approve
// (checker; maker != checker) — applies the epoch transition.
func (api *API) approveRotation(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	deviceID := request.PathValue("id")
	stored, err := api.Store.GetRequest(request.Context(), principal.TenantID, request.PathValue("requestId"))
	if errors.Is(err, ErrRequestNotFound) {
		writeDeviceError(writer, http.StatusNotFound, "rotation request not found")
		return
	}
	if err != nil {
		writeDeviceError(writer, http.StatusInternalServerError, "rotation request query failed")
		return
	}
	var rotate RotatePayload
	if err := json.Unmarshal(stored.Payload, &rotate); err != nil || rotate.Type != PayloadRotate || rotate.DeviceID != deviceID {
		writeDeviceError(writer, http.StatusConflict, "request is not a rotation for this device")
		return
	}
	approval, err := api.Store.ApproveRequest(request.Context(), principal.TenantID, stored.ID, principal.Subject, api.Grace)
	switch {
	case errors.Is(err, ErrMakerChecker):
		writeDeviceError(writer, http.StatusConflict, err.Error())
	case errors.Is(err, ErrRequestNotPending):
		writeDeviceError(writer, http.StatusConflict, err.Error())
	case err != nil:
		writeDeviceError(writer, http.StatusInternalServerError, "rotation approval failed")
	default:
		api.audit(request, AuditEvent{TenantID: principal.TenantID, DeviceID: deviceID,
			Event: AuditRotationApplied, Actor: principal.Subject,
			Metadata: map[string]string{"requestId": stored.ID, "keyEpoch": jsonNumber(approval.KeyEpoch)}})
		api.Metrics.Inc("geo_device_rotations_total", map[string]string{"result": "applied"})
		writeDeviceJSON(writer, http.StatusOK, map[string]any{
			"deviceId": approval.DeviceID, "keyEpoch": approval.KeyEpoch, "status": "rotated"})
	}
}

type firmwareReleaseBody struct {
	Kind           string `json:"kind"`
	Version        string `json:"version"`
	ArtifactSHA256 string `json:"artifactSha256"`
	ArtifactURL    string `json:"artifactUrl"`
	RolloutPercent int    `json:"rolloutPercent"`
	MinEpoch       int    `json:"minEpoch"`
}

// createFirmwareRelease: POST /v1/devices/firmware-releases (fleet admin).
// The artifact itself is hosted by the external artifact store; this
// registers only the release descriptor.
func (api *API) createFirmwareRelease(writer http.ResponseWriter, request *http.Request) {
	principal, ok := principalOrFail(writer, request)
	if !ok {
		return
	}
	var body firmwareReleaseBody
	if !decodeJSON(writer, request, &body, 1<<16) {
		return
	}
	id, err := api.Store.CreateFirmwareRelease(request.Context(), principal.TenantID, Release{
		Kind: body.Kind, Version: body.Version, ArtifactSHA256: body.ArtifactSHA256,
		ArtifactURL: body.ArtifactURL, RolloutPercent: body.RolloutPercent, MinEpoch: body.MinEpoch,
	})
	if err != nil {
		writeDeviceError(writer, http.StatusBadRequest, "firmware release rejected: "+err.Error())
		return
	}
	writeDeviceJSON(writer, http.StatusCreated, map[string]any{"releaseId": id})
}

// ---------------------------------------------------------------------
// Device-authenticated endpoints
// ---------------------------------------------------------------------

// ingestTelemetry: POST /v1/devices/{id}/telemetry — the body is the
// device-signed envelope. Verified envelopes carrying a supported geo
// contract payload are re-published as service-signed envelopes on the
// existing vessels.events pipeline; authentic-but-unprocessable payloads
// and verification failures on a known device are dead-lettered with the
// reason, never silently dropped.
func (api *API) ingestTelemetry(writer http.ResponseWriter, request *http.Request) {
	deviceID := request.PathValue("id")
	body, err := io.ReadAll(io.LimitReader(request.Body, 1<<18))
	if err != nil {
		writeDeviceError(writer, http.StatusBadRequest, "request body unreadable")
		return
	}
	envelope, verified, err := api.Verifier.VerifyTelemetry(request.Context(), deviceID, body)
	if err != nil {
		var authErr *AuthError
		if errors.As(err, &authErr) {
			status := http.StatusForbidden
			if authErr.Reason == ReasonEnvelopeMalformed {
				status = http.StatusBadRequest
			}
			// Dead-letter only when the envelope parsed and names a
			// registered device (unauthenticated garbage is not ledgered
			// to Kafka).
			if authErr.Reason != ReasonEnvelopeMalformed &&
				authErr.Reason != ReasonDeviceUnknown &&
				authErr.Reason != ReasonDeviceIDMismatch {
				api.deadLetter(request, deviceID, authErr.Reason, body)
			}
			writeDeviceError(writer, status, "device authentication failed: "+authErr.Reason)
			return
		}
		writeDeviceError(writer, http.StatusInternalServerError, "device authentication unavailable")
		return
	}
	// The verified device identity is bound into the request context for
	// the ingest path — downstream code never trusts a caller-supplied id.
	request = request.WithContext(WithDevice(request.Context(), verified))
	switch envelope.PayloadType {
	case sign.EventVesselPosition:
		var payload sign.VesselPositionReported
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil || !mmsiLike(payload.MMSI) {
			api.deadLetter(request, deviceID, ReasonPayloadUnsupported, body)
			writeDeviceJSON(writer, http.StatusAccepted, map[string]any{"status": "dead-lettered", "reason": ReasonPayloadUnsupported})
			return
		}
		api.forwardTelemetry(writer, request, envelope, verified, sign.EventVesselPosition, payload, payload.Classification)
	case sign.EventVesselStatic:
		var payload sign.VesselStaticReported
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil || !mmsiLike(payload.MMSI) {
			api.deadLetter(request, deviceID, ReasonPayloadUnsupported, body)
			writeDeviceJSON(writer, http.StatusAccepted, map[string]any{"status": "dead-lettered", "reason": ReasonPayloadUnsupported})
			return
		}
		api.forwardTelemetry(writer, request, envelope, verified, sign.EventVesselStatic, payload, payload.Classification)
	default:
		api.deadLetter(request, deviceID, ReasonPayloadUnsupported, body)
		writeDeviceJSON(writer, http.StatusAccepted, map[string]any{"status": "dead-lettered", "reason": ReasonPayloadUnsupported})
	}
}

// forwardTelemetry re-publishes one verified device payload as a
// service-signed envelope on the existing event pipeline.
func (api *API) forwardTelemetry(writer http.ResponseWriter, request *http.Request,
	envelope DeviceEnvelope, verified VerifiedDevice, eventType string, payload any, classification string) {
	correlationID := envelope.DeviceID + ":" + eventType + ":" + envelope.OccurredAt.UTC().Format(time.RFC3339Nano)
	err := api.Events.PublishSignedEnvelope(request.Context(), eventType, correlationID, payload,
		envelope.OccurredAt.UTC(), classification, map[string]string{
			"device-id":   verified.Device.ID,
			"device-kind": verified.Device.Kind,
			"key-epoch":   jsonNumber(envelope.KeyEpoch),
		})
	if err != nil {
		writeDeviceError(writer, http.StatusBadGateway, "event pipeline publication failed")
		return
	}
	api.Metrics.Inc("geo_device_telemetry_total", map[string]string{"result": "published", "eventType": eventType})
	writeDeviceJSON(writer, http.StatusAccepted, map[string]any{"status": "published", "eventType": eventType})
}

// deadLetterRecord is the DLQ payload on vessels.quarantine.
type deadLetterRecord struct {
	DeadLetterID string          `json:"deadLetterId"`
	DeviceID     string          `json:"deviceId"`
	Reason       string          `json:"reason"`
	Envelope     json.RawMessage `json:"envelope"`
	OccurredAt   time.Time       `json:"occurredAt"`
}

func (api *API) deadLetter(request *http.Request, deviceID, reason string, body []byte) {
	record := deadLetterRecord{
		DeadLetterID: "ddl-" + deviceID + "-" + jsonNumber(int(api.now().UnixNano())),
		DeviceID:     deviceID,
		Reason:       reason,
		Envelope:     json.RawMessage(body),
		OccurredAt:   api.now().UTC(),
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return
	}
	// Fail-open by design ONLY here: a dead-letter that cannot reach the
	// broker is counted (the caller still receives the rejection).
	if err := api.DeadLetters.Publish(request.Context(), bus.TopicVesselQuarantine, deviceID, raw,
		map[string]string{"dead-letter-reason": reason}); err != nil {
		api.Metrics.Inc("geo_device_dead_letter_errors_total", nil)
		return
	}
	api.Metrics.Inc("geo_device_telemetry_total", map[string]string{"result": "dead_letter", "reason": reason})
	api.audit(request, AuditEvent{DeviceID: deviceID, Event: AuditTelemetryDeadLetter,
		Actor: "device:" + deviceID, Reason: reason})
}

// serveFirmware: GET /v1/devices/{id}/firmware with
// Authorization: Device <signed proof (action GET_FIRMWARE)>. Returns the
// signed OTA manifest for the device's current rollout target, or 204 when
// no release targets the device. Artifacts are never hosted here.
func (api *API) serveFirmware(writer http.ResponseWriter, request *http.Request) {
	deviceID := request.PathValue("id")
	proof, ok := deviceAuthorization(request)
	if !ok {
		writeDeviceError(writer, http.StatusUnauthorized, "Device authorization proof is required")
		return
	}
	verified, err := api.Verifier.VerifyProof(request.Context(), proof, ProofActionFirmware)
	if err != nil {
		writeDeviceError(writer, http.StatusForbidden, "device authentication failed")
		return
	}
	if verified.Device.ID != deviceID {
		writeDeviceError(writer, http.StatusForbidden, "proof device does not match the path device")
		return
	}
	releases, err := api.Store.ListReleasesForKind(request.Context(), verified.Device.TenantID, verified.Device.Kind)
	if err != nil {
		writeDeviceError(writer, http.StatusInternalServerError, "firmware release query failed")
		return
	}
	release := SelectRelease(releases, verified.Device)
	if release == nil {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	manifest, err := SignManifest(Manifest{
		DeviceID:       verified.Device.ID,
		Kind:           release.Kind,
		Version:        release.Version,
		ArtifactSHA256: release.ArtifactSHA256,
		ArtifactURL:    release.ArtifactURL,
		RolloutPercent: release.RolloutPercent,
		MinEpoch:       release.MinEpoch,
		GeneratedAt:    api.now().UTC(),
	}, func(kid string, payload any) (string, error) {
		return SignPayload(api.ManifestKey, kid, payload)
	}, api.ManifestKeyID)
	if err != nil {
		writeDeviceError(writer, http.StatusInternalServerError, "manifest signing failed")
		return
	}
	api.audit(request, AuditEvent{TenantID: verified.Device.TenantID, DeviceID: deviceID,
		Event: AuditFirmwareServed, Actor: "device:" + deviceID,
		Metadata: map[string]string{"version": release.Version}})
	api.Metrics.Inc("geo_device_firmware_served_total", map[string]string{"kind": release.Kind})
	writeDeviceJSON(writer, http.StatusOK, manifest)
}

// mqttAuthRequest is the EMQX HTTP authentication webhook body.
type mqttAuthRequest struct {
	ClientID string `json:"clientid"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// serveMQTTAuth: POST /v1/devices/mqtt-auth implementing the EMQX HTTP
// authn contract: clientid is the device id, password the signed proof
// (action MQTT_AUTH, optionally "Device <jws>"). The response is always
// HTTP 200 with result allow/deny; is_superuser is NEVER granted.
func (api *API) serveMQTTAuth(writer http.ResponseWriter, request *http.Request) {
	var body mqttAuthRequest
	if !decodeJSON(writer, request, &body, 1<<16) {
		return
	}
	deny := func() {
		api.Metrics.Inc("geo_device_mqtt_auth_total", map[string]string{"result": "deny"})
		writeDeviceJSON(writer, http.StatusOK, map[string]any{"result": "deny", "is_superuser": false})
	}
	if strings.TrimSpace(body.ClientID) == "" {
		deny()
		return
	}
	password := strings.TrimSpace(body.Password)
	password = strings.TrimSpace(strings.TrimPrefix(password, "Device "))
	verified, err := api.Verifier.VerifyProof(request.Context(), password, ProofActionMQTTAuth)
	if err != nil || verified.Device.ID != body.ClientID {
		deny()
		return
	}
	api.Metrics.Inc("geo_device_mqtt_auth_total", map[string]string{"result": "allow"})
	writeDeviceJSON(writer, http.StatusOK, map[string]any{"result": "allow", "is_superuser": false})
}

type activateRequestBody struct {
	RequestID        string `json:"requestId"`
	ActivationSecret string `json:"activationSecret"`
}

// activate: POST /v1/devices/activate — consumes the one-time activation
// secret exactly once (APPROVED -> CONSUMED). The secret is the only
// credential; it is compared against the at-rest SHA-256.
func (api *API) activate(writer http.ResponseWriter, request *http.Request) {
	var body activateRequestBody
	if !decodeJSON(writer, request, &body, 1<<12) {
		return
	}
	deviceID, err := api.Store.ConsumeActivation(request.Context(), body.RequestID, body.ActivationSecret)
	if errors.Is(err, ErrActivationFailed) {
		api.Metrics.Inc("geo_device_provisioning_total", map[string]string{"decision": "consume_failed"})
		writeDeviceError(writer, http.StatusConflict, "activation failed: secret mismatch or request already consumed")
		return
	}
	if err != nil {
		writeDeviceError(writer, http.StatusInternalServerError, "activation unavailable")
		return
	}
	api.audit(request, AuditEvent{DeviceID: deviceID, Event: AuditProvisionConsumed,
		Actor: "device:" + deviceID, Metadata: map[string]string{"requestId": body.RequestID}})
	api.Metrics.Inc("geo_device_provisioning_total", map[string]string{"decision": "consumed"})
	writeDeviceJSON(writer, http.StatusOK, map[string]any{"status": RequestConsumed, "deviceId": deviceID})
}

// deviceAuthorization extracts the "Device <jws>" authorization proof.
func deviceAuthorization(request *http.Request) (string, bool) {
	value := strings.TrimSpace(request.Header.Get("Authorization"))
	parts := strings.SplitN(value, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Device") || strings.TrimSpace(parts[1]) == "" {
		return "", false
	}
	return strings.TrimSpace(parts[1]), true
}

// audit appends one ledger entry through the device-plane writer;
// failures are counted, never fatal to the completed decision (the
// decision itself is already durable).
func (api *API) audit(request *http.Request, event AuditEvent) {
	if err := api.Verifier.Reader.InsertAudit(request.Context(), event); err != nil {
		api.Metrics.Inc("geo_device_audit_errors_total", nil)
	}
}

func mmsiLike(value string) bool {
	if len(value) != 9 {
		return false
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func jsonNumber(value int) string {
	return strconv.Itoa(value)
}

func writeDeviceJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}

func writeDeviceError(writer http.ResponseWriter, status int, message string) {
	writeDeviceJSON(writer, status, map[string]any{"error": message})
}
