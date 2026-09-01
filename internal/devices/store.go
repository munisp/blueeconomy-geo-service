// Storage for the device plane. Two CONNECTIONS by doctrine (mirroring
// store.Store's app/ingest split): the tenant-scoped application pool
// (geo role, RLS default-deny, every admin operation binds app.tenant_id)
// and the dedicated device-verification pool (geo_devices role, least
// privilege: registry/key/firmware SELECT, activation consume UPDATE
// (status), audit INSERT) for the verify-at-ingest path whose principal
// is proven by an Ed25519 signature rather than an HTTP tenant binding.
package devices

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-geo-service/internal/store"
)

// Store couples the tenant-scoped app store with the geo_devices pool.
type Store struct {
	app *store.Store
	dev *pgxpool.Pool
}

// NewStore connects the geo_devices pool (it fails closed when the device
// plane role cannot authenticate at startup) and wraps the app store.
func NewStore(ctx context.Context, app *store.Store, devicesDSN string) (*Store, error) {
	if app == nil {
		return nil, errors.New("devices: app store is required")
	}
	if strings.TrimSpace(devicesDSN) == "" {
		return nil, errors.New("devices: device-plane postgres DSN (geo_devices role) is required")
	}
	config, err := pgxpool.ParseConfig(devicesDSN)
	if err != nil {
		return nil, fmt.Errorf("devices: parse device-plane DSN: %w", err)
	}
	// otelpgx query tracer (no-op when telemetry is disabled).
	config.ConnConfig.Tracer = otelpgx.NewTracer()
	if err := store.ApplyPoolEnv(config); err != nil {
		return nil, fmt.Errorf("devices: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("devices: connect device-plane postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("devices: ping device-plane postgres: %w", err)
	}
	return &Store{app: app, dev: pool}, nil
}

// NewStoreWithPool wires an existing geo_devices pool (test harness).
func NewStoreWithPool(app *store.Store, devicePool *pgxpool.Pool) (*Store, error) {
	if app == nil || devicePool == nil {
		return nil, errors.New("devices: app store and device-plane pool are required")
	}
	return &Store{app: app, dev: devicePool}, nil
}

// Close releases the device-plane pool (the app store owns its own pools).
func (s *Store) Close() {
	s.dev.Close()
}

// DevicePlanePool exposes the geo_devices pool for integration tests.
func (s *Store) DevicePlanePool() *pgxpool.Pool {
	return s.dev
}

// ---------------------------------------------------------------------
// Admin path (tenant-bound, app role)
// ---------------------------------------------------------------------

// ProvisionPayload is the maker payload of a PROVISION request.
type ProvisionPayload struct {
	Type        string            `json:"type"`
	Kind        string            `json:"kind"`
	OwnerAgency string            `json:"ownerAgency"`
	Label       string            `json:"label"`
	PublicKey   string            `json:"publicKey"` // base64 Ed25519 public key (32 bytes)
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// RotatePayload is the maker payload of a ROTATE request.
type RotatePayload struct {
	Type      string `json:"type"`
	DeviceID  string `json:"deviceId"`
	PublicKey string `json:"publicKey"` // base64 Ed25519 public key for the new epoch
}

// Request is one provisioning_requests row.
type Request struct {
	ID          string          `json:"id"`
	TenantID    string          `json:"tenantId"`
	Payload     json.RawMessage `json:"payload"`
	RequestedBy string          `json:"requestedBy"`
	Status      string          `json:"status"`
	DecidedBy   string          `json:"decidedBy,omitempty"`
	DecidedAt   *time.Time      `json:"decidedAt,omitempty"`
	DeviceID    string          `json:"deviceId,omitempty"`
	CreatedAt   time.Time       `json:"createdAt"`
}

// decodePublicKey validates a base64 Ed25519 public key (32 bytes).
func decodePublicKey(encoded string) ([]byte, error) {
	trimmed := strings.TrimSpace(encoded)
	if trimmed == "" {
		return nil, errors.New("public key is required")
	}
	for _, decode := range []func(string) ([]byte, error){
		base64.RawURLEncoding.DecodeString,
		base64.URLEncoding.DecodeString,
		base64.RawStdEncoding.DecodeString,
		base64.StdEncoding.DecodeString,
	} {
		raw, err := decode(trimmed)
		if err == nil && len(raw) == 32 {
			return raw, nil
		}
	}
	return nil, errors.New("public key must be base64 of a 32-byte Ed25519 key")
}

// CreateProvisionRequest is the maker half of device provisioning: the
// request persists PENDING until a checker decides.
func (s *Store) CreateProvisionRequest(ctx context.Context, tenantID string, payload ProvisionPayload, maker string) (string, error) {
	if !ValidKind(payload.Kind) {
		return "", fmt.Errorf("device kind %q is not supported", payload.Kind)
	}
	if _, err := decodePublicKey(payload.PublicKey); err != nil {
		return "", err
	}
	if _, err := validateLabel("owner agency", payload.OwnerAgency, 256); err != nil {
		return "", err
	}
	if _, err := validateLabel("label", payload.Label, 256); err != nil {
		return "", err
	}
	if strings.TrimSpace(maker) == "" {
		return "", errors.New("maker principal is required")
	}
	payload.Type = PayloadProvision
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode provision payload: %w", err)
	}
	return s.insertRequest(ctx, tenantID, raw, maker)
}

// CreateRotationRequest is the maker half of a fleet key rotation: the new
// epoch applies only after a checker approves.
func (s *Store) CreateRotationRequest(ctx context.Context, tenantID, deviceID, newPublicKey, maker string) (string, error) {
	if strings.TrimSpace(deviceID) == "" {
		return "", errors.New("device id is required")
	}
	if _, err := decodePublicKey(newPublicKey); err != nil {
		return "", err
	}
	if strings.TrimSpace(maker) == "" {
		return "", errors.New("maker principal is required")
	}
	raw, err := json.Marshal(RotatePayload{Type: PayloadRotate, DeviceID: deviceID, PublicKey: strings.TrimSpace(newPublicKey)})
	if err != nil {
		return "", fmt.Errorf("encode rotation payload: %w", err)
	}
	return s.insertRequest(ctx, tenantID, raw, maker)
}

func (s *Store) insertRequest(ctx context.Context, tenantID string, payload json.RawMessage, maker string) (string, error) {
	var id string
	err := s.app.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO provisioning_requests (tenant_id, payload, requested_by)
			VALUES ($1, $2, $3) RETURNING id`, tenantID, payload, maker).Scan(&id)
	})
	if err != nil {
		return "", fmt.Errorf("create provisioning request: %w", err)
	}
	return id, nil
}

// GetRequest loads one request inside the caller's tenant (RLS).
func (s *Store) GetRequest(ctx context.Context, tenantID, requestID string) (Request, error) {
	var request Request
	err := s.app.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var decidedBy, deviceID *string
		err := tx.QueryRow(ctx, `SELECT id, tenant_id, payload, requested_by, status, decided_by,
			decided_at, device_id, created_at FROM provisioning_requests WHERE id = $1`, requestID).
			Scan(&request.ID, &request.TenantID, &request.Payload, &request.RequestedBy,
				&request.Status, &decidedBy, &request.DecidedAt, &deviceID, &request.CreatedAt)
		if err != nil {
			return err
		}
		if decidedBy != nil {
			request.DecidedBy = *decidedBy
		}
		if deviceID != nil {
			request.DeviceID = *deviceID
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Request{}, ErrRequestNotFound
	}
	if err != nil {
		return Request{}, fmt.Errorf("get provisioning request: %w", err)
	}
	return request, nil
}

// Approval is the checker outcome: the provisioned device id and the
// one-time activation secret (returned exactly once; only its SHA-256 is
// persisted).
type Approval struct {
	RequestID        string `json:"requestId"`
	DeviceID         string `json:"deviceId"`
	KeyEpoch         int    `json:"keyEpoch"`
	ActivationSecret string `json:"activationSecret"`
}

// ApproveRequest is the checker half: a principal distinct from the maker
// approves a PENDING request. For PROVISION the device row and its first
// key epoch are created and the one-time activation secret is generated;
// for ROTATE the epoch transition is applied (old CURRENT -> PREVIOUS,
// PREVIOUS epochs past the grace window -> REVOKED, new epoch CURRENT).
func (s *Store) ApproveRequest(ctx context.Context, tenantID, requestID, checker string, grace time.Duration) (Approval, error) {
	if strings.TrimSpace(checker) == "" {
		return Approval{}, errors.New("checker principal is required")
	}
	if grace <= 0 {
		grace = DefaultKeyGrace
	}
	var approval Approval
	err := s.app.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var payload json.RawMessage
		var requestedBy, status string
		err := tx.QueryRow(ctx, `SELECT payload, requested_by, status FROM provisioning_requests
			WHERE id = $1 FOR UPDATE`, requestID).Scan(&payload, &requestedBy, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrRequestNotFound
		}
		if err != nil {
			return err
		}
		if requestedBy == checker {
			return ErrMakerChecker
		}
		if status != RequestPending {
			return ErrRequestNotPending
		}
		var typed struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(payload, &typed); err != nil {
			return fmt.Errorf("provisioning request payload is invalid")
		}
		switch typed.Type {
		case PayloadProvision:
			var provision ProvisionPayload
			if err := json.Unmarshal(payload, &provision); err != nil {
				return fmt.Errorf("provision payload is invalid")
			}
			publicKey, err := decodePublicKey(provision.PublicKey)
			if err != nil {
				return err
			}
			metadata, err := json.Marshal(provision.Metadata)
			if err != nil {
				return fmt.Errorf("encode device metadata: %w", err)
			}
			var deviceID string
			if err := tx.QueryRow(ctx, `INSERT INTO devices
				(tenant_id, kind, owner_agency, label, status, key_epoch, metadata, created_by)
				VALUES ($1, $2, $3, $4, 'ACTIVE', 1, $5, $6) RETURNING id`,
				tenantID, provision.Kind, provision.OwnerAgency, provision.Label, metadata, requestedBy).
				Scan(&deviceID); err != nil {
				return fmt.Errorf("create device: %w", err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO device_keys
				(device_id, epoch, ed25519_public_key, status) VALUES ($1, 1, $2, 'CURRENT')`,
				deviceID, publicKey); err != nil {
				return fmt.Errorf("create device key epoch 1: %w", err)
			}
			secret, hash, err := generateActivationSecret()
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE provisioning_requests
				SET status = 'APPROVED', decided_by = $2, decided_at = now(),
					activation_secret_hash = $3, device_id = $4
				WHERE id = $1`, requestID, checker, hash, deviceID); err != nil {
				return fmt.Errorf("approve provisioning request: %w", err)
			}
			approval = Approval{RequestID: requestID, DeviceID: deviceID, KeyEpoch: 1, ActivationSecret: secret}
			return nil
		case PayloadRotate:
			var rotate RotatePayload
			if err := json.Unmarshal(payload, &rotate); err != nil {
				return fmt.Errorf("rotation payload is invalid")
			}
			publicKey, err := decodePublicKey(rotate.PublicKey)
			if err != nil {
				return err
			}
			epoch, err := s.applyRotation(ctx, tx, rotate.DeviceID, publicKey, grace)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE provisioning_requests
				SET status = 'APPROVED', decided_by = $2, decided_at = now(), device_id = $3
				WHERE id = $1`, requestID, checker, rotate.DeviceID); err != nil {
				return fmt.Errorf("approve rotation request: %w", err)
			}
			approval = Approval{RequestID: requestID, DeviceID: rotate.DeviceID, KeyEpoch: epoch}
			return nil
		default:
			return fmt.Errorf("provisioning request type %q is not supported", typed.Type)
		}
	})
	if err != nil {
		return Approval{}, err
	}
	return approval, nil
}

// applyRotation performs the epoch transition inside the checker
// transaction: current -> PREVIOUS (grace starts now), stale PREVIOUS
// beyond the grace window -> REVOKED, new epoch -> CURRENT.
func (s *Store) applyRotation(ctx context.Context, tx pgx.Tx, deviceID string, newPublicKey []byte, grace time.Duration) (int, error) {
	var currentEpoch int
	var status string
	err := tx.QueryRow(ctx, `SELECT key_epoch, status FROM devices WHERE id = $1 FOR UPDATE`, deviceID).
		Scan(&currentEpoch, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrDeviceNotFound
	}
	if err != nil {
		return 0, err
	}
	if status != StatusActive {
		return 0, fmt.Errorf("device is %s: rotation requires an ACTIVE device", status)
	}
	// Revoke PREVIOUS epochs whose grace window has closed (revocation is
	// immediate and terminal).
	if _, err := tx.Exec(ctx, `UPDATE device_keys SET status = 'REVOKED'
		WHERE device_id = $1 AND status = 'PREVIOUS' AND rotated_at < now() - $2::interval`,
		deviceID, grace.String()); err != nil {
		return 0, fmt.Errorf("revoke expired key epochs: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE device_keys SET status = 'PREVIOUS', rotated_at = now()
		WHERE device_id = $1 AND status = 'CURRENT'`, deviceID); err != nil {
		return 0, fmt.Errorf("demote current key epoch: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO device_keys (device_id, epoch, ed25519_public_key, status)
		VALUES ($1, $2, $3, 'CURRENT')`, deviceID, currentEpoch+1, newPublicKey); err != nil {
		return 0, fmt.Errorf("install key epoch %d: %w", currentEpoch+1, err)
	}
	if _, err := tx.Exec(ctx, `UPDATE devices SET key_epoch = $2, updated_at = now()
		WHERE id = $1`, deviceID, currentEpoch+1); err != nil {
		return 0, fmt.Errorf("advance device key epoch: %w", err)
	}
	return currentEpoch + 1, nil
}

// RejectRequest is the checker's negative decision.
func (s *Store) RejectRequest(ctx context.Context, tenantID, requestID, checker string) (Request, error) {
	if strings.TrimSpace(checker) == "" {
		return Request{}, errors.New("checker principal is required")
	}
	err := s.app.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var requestedBy, status string
		err := tx.QueryRow(ctx, `SELECT requested_by, status FROM provisioning_requests
			WHERE id = $1 FOR UPDATE`, requestID).Scan(&requestedBy, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrRequestNotFound
		}
		if err != nil {
			return err
		}
		if requestedBy == checker {
			return ErrMakerChecker
		}
		if status != RequestPending {
			return ErrRequestNotPending
		}
		if _, err := tx.Exec(ctx, `UPDATE provisioning_requests
			SET status = 'REJECTED', decided_by = $2, decided_at = now() WHERE id = $1`,
			requestID, checker); err != nil {
			return fmt.Errorf("reject provisioning request: %w", err)
		}
		return nil
	})
	if err != nil {
		return Request{}, err
	}
	return s.GetRequest(ctx, tenantID, requestID)
}

// ListDevices returns the caller tenant's registry (RLS-bound).
func (s *Store) ListDevices(ctx context.Context, tenantID string, limit int) ([]Device, error) {
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	devices := make([]Device, 0)
	err := s.app.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id, tenant_id, kind, owner_agency, label, status, key_epoch,
			metadata, created_by, created_at, updated_at FROM devices
			ORDER BY created_at DESC LIMIT $1`, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			device, err := scanDevice(rows)
			if err != nil {
				return err
			}
			devices = append(devices, device)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("list devices: %w", err)
	}
	return devices, nil
}

// GetDevice returns one registry row inside the caller's tenant.
func (s *Store) GetDevice(ctx context.Context, tenantID, deviceID string) (Device, error) {
	var device Device
	err := s.app.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `SELECT id, tenant_id, kind, owner_agency, label, status, key_epoch,
			metadata, created_by, created_at, updated_at FROM devices WHERE id = $1`, deviceID)
		var err error
		device, err = scanDevice(row)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Device{}, ErrDeviceNotFound
	}
	if err != nil {
		return Device{}, fmt.Errorf("get device: %w", err)
	}
	return device, nil
}

type deviceScanner interface {
	Scan(dest ...any) error
}

func scanDevice(row deviceScanner) (Device, error) {
	var device Device
	var metadata []byte
	if err := row.Scan(&device.ID, &device.TenantID, &device.Kind, &device.OwnerAgency,
		&device.Label, &device.Status, &device.KeyEpoch, &metadata, &device.CreatedBy,
		&device.CreatedAt, &device.UpdatedAt); err != nil {
		return Device{}, err
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &device.Metadata); err != nil {
			return Device{}, fmt.Errorf("decode device metadata: %w", err)
		}
	}
	return device, nil
}

// SetDeviceStatus applies a lifecycle transition (immediate — revocation
// and suspension take effect at once; REVOKED/DECOMMISSIONED are
// terminal). Revoking a device also revokes every key epoch.
func (s *Store) SetDeviceStatus(ctx context.Context, tenantID, deviceID, target, actor string) (Device, error) {
	if strings.TrimSpace(actor) == "" {
		return Device{}, errors.New("actor principal is required")
	}
	var device Device
	err := s.app.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		var current string
		err := tx.QueryRow(ctx, `SELECT status FROM devices WHERE id = $1 FOR UPDATE`, deviceID).Scan(&current)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrDeviceNotFound
		}
		if err != nil {
			return err
		}
		if err := ValidateStatusTransition(current, target); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE devices SET status = $2, updated_at = now()
			WHERE id = $1`, deviceID, target); err != nil {
			return fmt.Errorf("set device status: %w", err)
		}
		if target == StatusRevoked || target == StatusDecommissioned {
			if _, err := tx.Exec(ctx, `UPDATE device_keys SET status = 'REVOKED'
				WHERE device_id = $1 AND status <> 'REVOKED'`, deviceID); err != nil {
				return fmt.Errorf("revoke device keys: %w", err)
			}
		}
		row := tx.QueryRow(ctx, `SELECT id, tenant_id, kind, owner_agency, label, status, key_epoch,
			metadata, created_by, created_at, updated_at FROM devices WHERE id = $1`, deviceID)
		var scanErr error
		device, scanErr = scanDevice(row)
		return scanErr
	})
	if err != nil {
		return Device{}, err
	}
	return device, nil
}

// CreateFirmwareRelease registers one OTA release for a device kind.
func (s *Store) CreateFirmwareRelease(ctx context.Context, tenantID string, release Release) (string, error) {
	if !ValidKind(release.Kind) {
		return "", fmt.Errorf("device kind %q is not supported", release.Kind)
	}
	if _, err := validateLabel("version", release.Version, 128); err != nil {
		return "", err
	}
	if _, err := validateLabel("artifact url", release.ArtifactURL, 2048); err != nil {
		return "", err
	}
	if len(release.ArtifactSHA256) != 64 {
		return "", errors.New("artifact sha256 must be 64 lowercase hex characters")
	}
	for _, digit := range release.ArtifactSHA256 {
		if (digit < '0' || digit > '9') && (digit < 'a' || digit > 'f') {
			return "", errors.New("artifact sha256 must be 64 lowercase hex characters")
		}
	}
	if release.RolloutPercent < 0 || release.RolloutPercent > 100 {
		return "", errors.New("rollout percent must be between 0 and 100")
	}
	if release.MinEpoch < 1 {
		release.MinEpoch = 1
	}
	var id string
	err := s.app.WithTenant(ctx, tenantID, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO firmware_releases
			(tenant_id, kind, version, artifact_sha256, artifact_url, rollout_percent, min_epoch)
			VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
			tenantID, release.Kind, release.Version, release.ArtifactSHA256,
			release.ArtifactURL, release.RolloutPercent, release.MinEpoch).Scan(&id)
	})
	if err != nil {
		return "", fmt.Errorf("create firmware release: %w", err)
	}
	return id, nil
}

// ---------------------------------------------------------------------
// Device-verification path (geo_devices role, platform-wide read)
// ---------------------------------------------------------------------

// LoadDeviceForAuth reads one device for envelope verification. The
// device-plane role sees every tenant's rows by design — the device
// principal is proven by its signature, then constrained by status.
func (s *Store) LoadDeviceForAuth(ctx context.Context, deviceID string) (Device, error) {
	row := s.dev.QueryRow(ctx, `SELECT id, tenant_id, kind, owner_agency, label, status, key_epoch,
		metadata, created_by, created_at, updated_at FROM devices WHERE id = $1`, deviceID)
	device, err := scanDevice(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Device{}, ErrDeviceNotFound
	}
	if err != nil {
		return Device{}, fmt.Errorf("load device for auth: %w", err)
	}
	return device, nil
}

// LoadKey reads one key epoch for envelope verification.
func (s *Store) LoadKey(ctx context.Context, deviceID string, epoch int) (Key, error) {
	var key Key
	err := s.dev.QueryRow(ctx, `SELECT device_id, epoch, ed25519_public_key, status, rotated_at
		FROM device_keys WHERE device_id = $1 AND epoch = $2`, deviceID, epoch).
		Scan(&key.DeviceID, &key.Epoch, &key.PublicKey, &key.Status, &key.RotatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Key{}, fmt.Errorf("device key epoch %d not found", epoch)
	}
	if err != nil {
		return Key{}, fmt.Errorf("load device key: %w", err)
	}
	return key, nil
}

// ConsumeActivation flips an APPROVED request to CONSUMED exactly once,
// gated on the SHA-256 of the presented activation secret (consume-on-use).
func (s *Store) ConsumeActivation(ctx context.Context, requestID string, secret string) (string, error) {
	trimmed := strings.TrimSpace(secret)
	if trimmed == "" {
		return "", ErrActivationFailed
	}
	// The at-rest anchor is the SHA-256 of the raw 32-byte secret (see
	// generateActivationSecret); a malformed encoding fails closed.
	raw, err := base64.RawURLEncoding.DecodeString(trimmed)
	if err != nil || len(raw) != 32 {
		return "", ErrActivationFailed
	}
	hash := sha256.Sum256(raw)
	var deviceID *string
	err = s.dev.QueryRow(ctx, `UPDATE provisioning_requests SET status = 'CONSUMED'
		WHERE id = $1 AND status = 'APPROVED' AND activation_secret_hash = $2
		RETURNING device_id`, requestID, hash[:]).Scan(&deviceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrActivationFailed
	}
	if err != nil {
		return "", fmt.Errorf("consume activation: %w", err)
	}
	if deviceID == nil {
		return "", ErrActivationFailed
	}
	return *deviceID, nil
}

// ListReleasesForKind reads every release for one tenant+kind (rollout
// bucketing and epoch gating are applied by the caller, deterministically).
func (s *Store) ListReleasesForKind(ctx context.Context, tenantID, kind string) ([]Release, error) {
	rows, err := s.dev.Query(ctx, `SELECT id, tenant_id, kind, version, artifact_sha256,
		artifact_url, rollout_percent, min_epoch, created_at FROM firmware_releases
		WHERE tenant_id = $1 AND kind = $2 ORDER BY created_at DESC, id DESC`, tenantID, kind)
	if err != nil {
		return nil, fmt.Errorf("list firmware releases: %w", err)
	}
	defer rows.Close()
	releases := make([]Release, 0)
	for rows.Next() {
		var release Release
		if err := rows.Scan(&release.ID, &release.TenantID, &release.Kind, &release.Version,
			&release.ArtifactSHA256, &release.ArtifactURL, &release.RolloutPercent,
			&release.MinEpoch, &release.CreatedAt); err != nil {
			return nil, err
		}
		releases = append(releases, release)
	}
	return releases, rows.Err()
}

// InsertAudit appends one audit ledger entry (device-plane role; also
// usable from the admin path inside a tenant transaction via AuditInTx).
func (s *Store) InsertAudit(ctx context.Context, event AuditEvent) error {
	metadata, err := json.Marshal(event.Metadata)
	if err != nil {
		return fmt.Errorf("encode audit metadata: %w", err)
	}
	var deviceID *string
	if strings.TrimSpace(event.DeviceID) != "" {
		deviceID = &event.DeviceID
	}
	var reason *string
	if strings.TrimSpace(event.Reason) != "" {
		reason = &event.Reason
	}
	if _, err := s.dev.Exec(ctx, `INSERT INTO device_audit_events
		(tenant_id, device_id, event, actor, reason, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		event.TenantID, deviceID, event.Event, event.Actor, reason, metadata); err != nil {
		return fmt.Errorf("insert device audit event: %w", err)
	}
	return nil
}

// generateActivationSecret returns a 256-bit one-time secret (base64url)
// and its SHA-256 hash for at-rest storage.
func generateActivationSecret() (secret string, hash []byte, err error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, fmt.Errorf("generate activation secret: %w", err)
	}
	digest := sha256.Sum256(raw)
	return base64.RawURLEncoding.EncodeToString(raw), digest[:], nil
}
