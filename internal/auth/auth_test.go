package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// After the 5-minute JWKS TTL, a burst of concurrent requests must share a
// single Keycloak fetch (singleflight), not stampede the JWKS endpoint.
func TestJWKSRefreshSingleflight(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks := fmt.Sprintf(`{"keys":[{"kty":"RSA","kid":"kid-1","use":"sig","alg":"RS256","n":%q,"e":%q}]}`,
		base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()))
	var fetches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(jwks))
	}))
	defer server.Close()
	jwksURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewOIDCAuthenticator("https://keycloak/realms/blueeconomy", "geo-service", jwksURL, "")
	if err != nil {
		t.Fatal(err)
	}
	// Initial load to populate the cache, then force the cache stale.
	if _, err := auth.key("kid-1", true); err != nil {
		t.Fatalf("initial key load: %v", err)
	}
	auth.mu.Lock()
	auth.loadedAt = time.Now().Add(-10 * time.Minute)
	auth.mu.Unlock()

	const goroutines = 32
	var wait sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wait.Add(1)
		go func() {
			defer wait.Done()
			loaded, err := auth.key("kid-1", true)
			if err == nil && loaded == nil {
				err = fmt.Errorf("nil key without error")
			}
			errs[i] = err
		}()
	}
	wait.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent key lookup %d: %v", i, err)
		}
	}
	if got := fetches.Load(); got != 2 { // initial + exactly one coalesced refresh
		t.Fatalf("expected 2 JWKS fetches (initial + singleflight refresh), got %d", got)
	}
}

// An unknown kid shares the refresh flight too; after one reload the honest
// "not trusted" error is returned without a fetch stampede.
func TestJWKSUnknownKidSingleflight(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks := fmt.Sprintf(`{"keys":[{"kty":"RSA","kid":"kid-1","use":"sig","alg":"RS256","n":%q,"e":%q}]}`,
		base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()))
	var fetches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(jwks))
	}))
	defer server.Close()
	jwksURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewOIDCAuthenticator("https://keycloak/realms/blueeconomy", "geo-service", jwksURL, "")
	if err != nil {
		t.Fatal(err)
	}
	const goroutines = 16
	var wait sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, errs[i] = auth.key("kid-unknown", true)
		}()
	}
	wait.Wait()
	for i, err := range errs {
		if err == nil {
			t.Fatalf("lookup %d: unknown kid must fail closed", i)
		}
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("expected 1 coalesced JWKS fetch for unknown kid, got %d", got)
	}
}
