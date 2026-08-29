// OTA firmware manifest: deterministic rollout bucketing by device UUID
// and the signed (JWS-EdDSA) manifest the device verifies before fetching
// the artifact. This service never hosts artifacts — artifact_url points
// at the external artifact store and artifact_sha256 is the device-side
// integrity anchor.
package devices

import (
	"crypto/sha256"
	"encoding/binary"
	"time"
)

// Manifest is the signed OTA target descriptor served to one device.
type Manifest struct {
	DeviceID       string    `json:"deviceId"`
	Kind           string    `json:"kind"`
	Version        string    `json:"version"`
	ArtifactSHA256 string    `json:"artifactSha256"`
	ArtifactURL    string    `json:"artifactUrl"`
	RolloutPercent int       `json:"rolloutPercent"`
	MinEpoch       int       `json:"minEpoch"`
	GeneratedAt    time.Time `json:"generatedAt"`
	// Signature is the JWS compact serialization (EdDSA) over the
	// JCS-canonical manifest with this field excluded; the kid is the
	// service signing key ("blueeconomy-geo-service-<epoch>").
	Signature string `json:"signature,omitempty"`
}

// RolloutBucket deterministically maps (release, device) into [0,100).
// The same device always lands in the same bucket for a given release, so
// a rollout percentage increase is monotone per device and tests are
// reproducible.
func RolloutBucket(releaseID, deviceID string) int {
	digest := sha256.Sum256([]byte(releaseID + ":" + deviceID))
	return int(binary.BigEndian.Uint64(digest[:8]) % 100)
}

// SelectRelease picks the newest release the device is eligible for: the
// device's key epoch must satisfy min_epoch and the device's rollout
// bucket must fall inside rollout_percent. Releases are expected newest
// first; nil means no release targets this device (not an error).
func SelectRelease(releases []Release, device Device) *Release {
	for i := range releases {
		release := releases[i]
		if release.Kind != device.Kind || release.TenantID != device.TenantID {
			continue
		}
		if device.KeyEpoch < release.MinEpoch {
			continue
		}
		if release.RolloutPercent <= 0 {
			continue
		}
		if RolloutBucket(release.ID, device.ID) >= release.RolloutPercent {
			continue
		}
		return &release
	}
	return nil
}

// SignManifest renders the signed manifest for one device+release with
// the service signing key (same JWS construction as device envelopes).
func SignManifest(manifest Manifest, sign func(kid string, payload any) (string, error), kid string) (Manifest, error) {
	unsigned := manifest
	unsigned.Signature = ""
	signature, err := sign(kid, unsigned)
	if err != nil {
		return Manifest{}, err
	}
	manifest.Signature = signature
	return manifest, nil
}
