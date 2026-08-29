// Envelope provenance signing implements the fleet-wide scheme, replicated
// from blueeconomy-port-interoperability: the provenance signature is a JWS
// compact serialization (EdDSA/Ed25519) over the JCS-canonicalized (RFC 8785)
// JSON of the full envelope with the signature field excluded. The protected
// header is {"alg":"EdDSA","kid":"blueeconomy-geo-service-<epoch>"}, where
// <epoch> is the key-rotation epoch identifying the signing key. The private
// key and its epoch come from the environment; construction fails closed when
// either is absent or invalid.
package sign

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	jsoncanonicalizer "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

const (
	// SigningKeyEnv carries the Ed25519 private key (base64 or hex encoded,
	// 32-byte seed or 64-byte private key).
	SigningKeyEnv = "ENVELOPE_SIGNING_PRIVATE_KEY"
	// SigningKeyEpochEnv carries the decimal key-rotation epoch used in the
	// JWS kid ("blueeconomy-geo-service-<epoch>").
	SigningKeyEpochEnv = "ENVELOPE_SIGNING_KEY_EPOCH"

	signingAlgorithm = "EdDSA"
	keyIDPrefix      = "blueeconomy-geo-service-"
)

// Signer signs envelope provenance with an Ed25519 key.
type Signer struct {
	privateKey ed25519.PrivateKey
	kid        string
}

// ParsePrivateKey decodes an Ed25519 private key from base64 (standard or
// URL, padded or raw) or hex. Both 32-byte seeds and 64-byte private keys are
// accepted; anything else fails closed.
func ParsePrivateKey(encoded string) (ed25519.PrivateKey, error) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" {
		return nil, errors.New("envelope signing private key is empty")
	}
	// Try every encoding and accept the one that yields a valid key length —
	// a hex string is also valid base64, so length is the discriminator.
	for _, decode := range []func(string) ([]byte, error){
		base64.RawURLEncoding.DecodeString,
		base64.URLEncoding.DecodeString,
		base64.RawStdEncoding.DecodeString,
		base64.StdEncoding.DecodeString,
		func(value string) ([]byte, error) { return hex.DecodeString(value) },
	} {
		raw, err := decode(encoded)
		if err != nil {
			continue
		}
		switch len(raw) {
		case ed25519.SeedSize:
			return ed25519.NewKeyFromSeed(raw), nil
		case ed25519.PrivateKeySize:
			return ed25519.PrivateKey(raw), nil
		}
	}
	return nil, fmt.Errorf("envelope signing private key must be base64 or hex of %d or %d bytes", ed25519.SeedSize, ed25519.PrivateKeySize)
}

// NewSigner builds a signer from a decoded private key and a decimal
// key-rotation epoch. It fails closed on an invalid key or epoch.
func NewSigner(privateKey ed25519.PrivateKey, keyEpoch string) (*Signer, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("envelope signing private key must be %d bytes", ed25519.PrivateKeySize)
	}
	public, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok || len(public) != ed25519.PublicKeySize {
		return nil, errors.New("envelope signing private key has no valid public half")
	}
	keyEpoch = strings.TrimSpace(keyEpoch)
	if keyEpoch == "" {
		return nil, errors.New("envelope signing key epoch is required")
	}
	for _, digit := range keyEpoch {
		if digit < '0' || digit > '9' {
			return nil, fmt.Errorf("envelope signing key epoch %q must be decimal digits", keyEpoch)
		}
	}
	return &Signer{privateKey: privateKey, kid: keyIDPrefix + keyEpoch}, nil
}

// SignerFromEnv loads the signer from SigningKeyEnv and SigningKeyEpochEnv.
// It fails closed when either variable is absent or invalid — an unsigned
// event pipeline must never start.
func SignerFromEnv() (*Signer, error) {
	encoded := os.Getenv(SigningKeyEnv)
	if encoded == "" {
		return nil, fmt.Errorf("%s must be set", SigningKeyEnv)
	}
	privateKey, err := ParsePrivateKey(encoded)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SigningKeyEnv, err)
	}
	epoch := os.Getenv(SigningKeyEpochEnv)
	if epoch == "" {
		return nil, fmt.Errorf("%s must be set", SigningKeyEpochEnv)
	}
	signer, err := NewSigner(privateKey, epoch)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SigningKeyEpochEnv, err)
	}
	return signer, nil
}

// KeyID is the JWS kid carried in the protected header.
func (signer *Signer) KeyID() string {
	return signer.kid
}

// PublicKey returns the verification half of the signing key.
func (signer *Signer) PublicKey() ed25519.PublicKey {
	return signer.privateKey.Public().(ed25519.PublicKey)
}

// protectedHeader is the JWS protected header, already in canonical
// (lexicographic) key order.
func protectedHeaderJSON(kid string) ([]byte, error) {
	header, err := json.Marshal(struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}{Algorithm: signingAlgorithm, KeyID: kid})
	if err != nil {
		return nil, fmt.Errorf("encode JWS protected header: %w", err)
	}
	return header, nil
}

// canonicalPayload renders the full envelope minus the signature field as
// JCS-canonical (RFC 8785) JSON. Numbers are decoded as literals so no
// float64 round-trip can alter the signed bytes.
func canonicalPayload(envelope Envelope) ([]byte, error) {
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
	provenance, ok := generic["provenance"].(map[string]any)
	if !ok {
		return nil, errors.New("envelope provenance block is missing")
	}
	delete(provenance, "signature")
	stripped, err := json.Marshal(generic)
	if err != nil {
		return nil, fmt.Errorf("re-encode envelope for signing: %w", err)
	}
	canonical, err := jsoncanonicalizer.Transform(stripped)
	if err != nil {
		return nil, fmt.Errorf("JCS-canonicalize envelope: %w", err)
	}
	return canonical, nil
}

// Sign produces the JWS compact serialization (EdDSA) over the
// JCS-canonicalized envelope excluding the signature field.
func (signer *Signer) Sign(envelope Envelope) (string, error) {
	header, err := protectedHeaderJSON(signer.kid)
	if err != nil {
		return "", err
	}
	canonical, err := canonicalPayload(envelope)
	if err != nil {
		return "", err
	}
	header64 := base64.RawURLEncoding.EncodeToString(header)
	payload64 := base64.RawURLEncoding.EncodeToString(canonical)
	signingInput := header64 + "." + payload64
	signature := ed25519.Sign(signer.privateKey, []byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

// Verify checks the provenance JWS against the public key: the header must be
// alg=EdDSA with a blueeconomy-geo-service kid, the payload must
// re-canonicalize to the exact signed bytes, and the Ed25519 signature must
// verify.
func Verify(envelope Envelope, publicKey ed25519.PublicKey) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errors.New("verification public key must be a valid Ed25519 key")
	}
	parts := strings.Split(envelope.Provenance.Signature, ".")
	if len(parts) != 3 {
		return errors.New("provenance signature is not a JWS compact serialization")
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return fmt.Errorf("decode JWS protected header: %w", err)
	}
	var parsed struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	if err := json.Unmarshal(header, &parsed); err != nil {
		return fmt.Errorf("parse JWS protected header: %w", err)
	}
	if parsed.Algorithm != signingAlgorithm {
		return fmt.Errorf("JWS alg %q is not %q", parsed.Algorithm, signingAlgorithm)
	}
	if !strings.HasPrefix(parsed.KeyID, keyIDPrefix) {
		return fmt.Errorf("JWS kid %q is not a blueeconomy-geo-service key", parsed.KeyID)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("decode JWS signature: %w", err)
	}
	canonical, err := canonicalPayload(envelope)
	if err != nil {
		return err
	}
	if base64.RawURLEncoding.EncodeToString(canonical) != parts[1] {
		return errors.New("envelope does not match the signed canonical payload")
	}
	if !ed25519.Verify(publicKey, []byte(parts[0]+"."+parts[1]), signature) {
		return errors.New("JWS signature verification failed")
	}
	return nil
}
