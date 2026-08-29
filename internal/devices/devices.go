// Package devices is the device-management plane (Citizen Services
// Advisory §9): the registry of IoT devices feeding the geo hot path
// (AIS receivers, GT06 trackers, LoRaWAN gateways, POS/validator units,
// generic sensors), their provisioning under maker/checker four-eyes,
// Ed25519 key epochs with rotation grace, verify-at-ingest telemetry
// authentication, signed OTA firmware manifests and the EMQX broker auth
// webhook.
//
// Doctrine: fail closed on every ambiguity; maker may never check their
// own request (enforced in SQL and in the application); privilege
// separation between the tenant-scoped admin path and the platform-wide
// device-verification path is by CONNECTION (geo_devices role, mirroring
// the geo_ingest doctrine of 0008_rls_ingest_login.sql), never by SET
// ROLE.
package devices

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Device kinds (registry CHECK constraint mirrors this set).
const (
	KindAIS       = "AIS"
	KindGT06      = "GT06"
	KindLoRaWAN   = "LORAWAN"
	KindPOS       = "POS"
	KindValidator = "VALIDATOR"
	KindGateway   = "GATEWAY"
	KindSensor    = "SENSOR"
)

// Device lifecycle statuses.
const (
	StatusActive         = "ACTIVE"
	StatusSuspended      = "SUSPENDED"
	StatusRevoked        = "REVOKED"
	StatusDecommissioned = "DECOMMISSIONED"
)

// Key epoch statuses.
const (
	KeyCurrent  = "CURRENT"
	KeyPrevious = "PREVIOUS"
	KeyRevoked  = "REVOKED"
)

// Provisioning request statuses.
const (
	RequestPending  = "PENDING"
	RequestApproved = "APPROVED"
	RequestRejected = "REJECTED"
	RequestConsumed = "CONSUMED"
)

// Provisioning payload types carried in provisioning_requests.payload.
const (
	PayloadProvision = "PROVISION"
	PayloadRotate    = "ROTATE"
)

// Audit event names written to device_audit_events.
const (
	AuditProvisionRequested  = "PROVISIONING_REQUESTED"
	AuditProvisionApproved   = "PROVISIONING_APPROVED"
	AuditProvisionRejected   = "PROVISIONING_REJECTED"
	AuditProvisionConsumed   = "PROVISIONING_CONSUMED"
	AuditRotationRequested   = "ROTATION_REQUESTED"
	AuditRotationApplied     = "ROTATION_APPLIED"
	AuditStatusChanged       = "STATUS_CHANGED"
	AuditAuthOK              = "AUTH_OK"
	AuditAuthFailed          = "AUTH_FAILED"
	AuditFirmwareServed      = "FIRMWARE_SERVED"
	AuditTelemetryAccepted   = "TELEMETRY_ACCEPTED"
	AuditTelemetryDeadLetter = "TELEMETRY_DEAD_LETTER"
)

// Authentication failure reasons (audit + metric label values).
const (
	ReasonEnvelopeMalformed  = "envelope_malformed"
	ReasonDeviceUnknown      = "device_unknown"
	ReasonDeviceSuspended    = "device_suspended"
	ReasonDeviceRevoked      = "device_revoked"
	ReasonDeviceRetired      = "device_decommissioned"
	ReasonEpochUnknown       = "key_epoch_unknown"
	ReasonKeyRevoked         = "key_revoked"
	ReasonGraceExpired       = "key_grace_expired"
	ReasonSignatureInvalid   = "signature_invalid"
	ReasonDeviceIDMismatch   = "device_id_mismatch"
	ReasonActionMismatch     = "proof_action_mismatch"
	ReasonPayloadUnsupported = "payload_type_unsupported"
)

// DefaultKeyGrace is the default window a PREVIOUS epoch stays valid after
// rotation (offline devices keep reporting until they rotate in).
const DefaultKeyGrace = 24 * time.Hour

// Device is one registered device row.
type Device struct {
	ID          string            `json:"id"`
	TenantID    string            `json:"tenantId"`
	Kind        string            `json:"kind"`
	OwnerAgency string            `json:"ownerAgency"`
	Label       string            `json:"label"`
	Status      string            `json:"status"`
	KeyEpoch    int               `json:"keyEpoch"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedBy   string            `json:"createdBy"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
}

// Key is one device_keys epoch row.
type Key struct {
	DeviceID  string    `json:"deviceId"`
	Epoch     int       `json:"epoch"`
	PublicKey []byte    `json:"-"`
	Status    string    `json:"status"`
	RotatedAt time.Time `json:"rotatedAt"`
}

// Release is one firmware_releases row.
type Release struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenantId"`
	Kind           string    `json:"kind"`
	Version        string    `json:"version"`
	ArtifactSHA256 string    `json:"artifactSha256"`
	ArtifactURL    string    `json:"artifactUrl"`
	RolloutPercent int       `json:"rolloutPercent"`
	MinEpoch       int       `json:"minEpoch"`
	CreatedAt      time.Time `json:"createdAt"`
}

// AuditEvent is one device_audit_events row.
type AuditEvent struct {
	TenantID  string
	DeviceID  string
	Event     string
	Actor     string
	Reason    string
	Metadata  map[string]string
	CreatedAt time.Time
}

// ValidKind reports whether kind is in the fail-closed registry set.
func ValidKind(kind string) bool {
	switch kind {
	case KindAIS, KindGT06, KindLoRaWAN, KindPOS, KindValidator, KindGateway, KindSensor:
		return true
	}
	return false
}

// ValidStatus reports whether status is a known lifecycle state.
func ValidStatus(status string) bool {
	switch status {
	case StatusActive, StatusSuspended, StatusRevoked, StatusDecommissioned:
		return true
	}
	return false
}

// ValidateStatusTransition enforces the lifecycle: REVOKED and
// DECOMMISSIONED are terminal; anything else moves freely between ACTIVE
// and SUSPENDED. Fail closed on unknown states.
func ValidateStatusTransition(from, to string) error {
	if !ValidStatus(from) || !ValidStatus(to) {
		return fmt.Errorf("device status %q or %q is not a known lifecycle state", from, to)
	}
	if from == to {
		return fmt.Errorf("device is already %s", from)
	}
	switch from {
	case StatusRevoked, StatusDecommissioned:
		return fmt.Errorf("device status %s is terminal", from)
	}
	return nil
}

// Sentinel errors surfaced by the store/service layers.
var (
	// ErrMakerChecker is returned when the requester attempts to decide
	// their own provisioning/rotation request (four-eyes violation).
	ErrMakerChecker = errors.New("maker may not decide their own request (four-eyes)")
	// ErrRequestNotPending is returned when deciding a request that is not
	// PENDING (already decided or consumed — consume-on-use).
	ErrRequestNotPending = errors.New("provisioning request is not pending")
	// ErrRequestNotFound is returned for an unknown request id.
	ErrRequestNotFound = errors.New("provisioning request not found")
	// ErrDeviceNotFound is returned for an unknown device id.
	ErrDeviceNotFound = errors.New("device not found")
	// ErrActivationFailed is returned when the activation secret does not
	// match or the request was already consumed.
	ErrActivationFailed = errors.New("activation failed: secret mismatch or request already consumed")
)

// validateLabel rejects empty/oversized free-text fields.
func validateLabel(field, value string, maximum int) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if len(trimmed) > maximum {
		return "", fmt.Errorf("%s exceeds %d characters", field, maximum)
	}
	return trimmed, nil
}
