// Package config resolves the service configuration from the environment.
// Every connector is individually gated and fails closed when enabled but
// misconfigured; secrets come from the environment only.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the resolved service configuration.
type Config struct {
	AppEnv string

	PostgresDSN string
	// IngestPostgresDSN authenticates as the least-privilege geo_ingest
	// LOGIN role (SELECT geofence_zones + INSERT geofence_events only) for
	// the platform-wide geofence evaluator. Separate connection by design:
	// the app role must never hold geo_ingest membership
	// (0008_rls_ingest_login.sql).
	IngestPostgresDSN string
	RedisAddr         string
	KafkaBrokers  []string
	DedupWindow   time.Duration
	PublishAISRaw bool

	// Principal identity asserted in envelope provenance (Keycloak service
	// account subject; never a credential).
	PrincipalID   string
	PrincipalRole string

	// PositionPlane declares the scoping doctrine of the vessel position
	// plane (ais_positions / latest_positions / vessels_static and the
	// /v1/geo vessel + track reads). Only "shared" is supported: a single
	// national picture scoped by classification clearance, NOT by tenant
	// (the tables carry no tenant column). "tenant" fails closed at startup
	// because the schema has no tenant support for positions.
	PositionPlane string

	// HTTP API.
	APIAddr string
	// Auth mode: "oidc" (Keycloak RS256) or "trusted_proxy" (edge loopback).
	AuthMode          string
	OIDCIssuer        string
	OIDCAudience      string
	OIDCJWKSURL       string
	OIDCCAFile        string
	TrustedProxyCIDRs string
	TrustedProxyID    string

	// Connectors (each independently gated).
	NMEATCPAddr     string
	NMEAUDPAddr     string
	AISStreamAPIKey string
	GT06Addr        string
	ReplayFile      string
	ReplayInterval  time.Duration
}

// FromEnv loads and validates the configuration, failing closed on any
// enabled-but-incomplete subsystem.
func FromEnv() (Config, error) {
	config := Config{
		AppEnv:            strings.ToLower(strings.TrimSpace(getenv("APP_ENV", "dev"))),
		PostgresDSN:       strings.TrimSpace(os.Getenv("GEO_PG_DSN")),
		IngestPostgresDSN: strings.TrimSpace(os.Getenv("GEO_INGEST_PG_DSN")),
		RedisAddr:         strings.TrimSpace(os.Getenv("GEO_REDIS_ADDR")),
		KafkaBrokers:      splitCSV(os.Getenv("GEO_KAFKA_BROKERS")),
		PublishAISRaw:     parseBool(getenv("GEO_PUBLISH_AIS_RAW", "true")),
		PrincipalID:       strings.TrimSpace(os.Getenv("GEO_PRODUCER_PRINCIPAL_ID")),
		PrincipalRole:     strings.TrimSpace(getenv("GEO_PRODUCER_PRINCIPAL_ROLE", "")),
		PositionPlane:     strings.ToLower(strings.TrimSpace(getenv("GEO_POSITION_PLANE", "shared"))),
		APIAddr:           strings.TrimSpace(getenv("GEO_API_ADDR", "")),
		AuthMode:          strings.ToLower(strings.TrimSpace(getenv("GEO_AUTH_MODE", "oidc"))),
		OIDCIssuer:        strings.TrimSpace(os.Getenv("GEO_OIDC_ISSUER")),
		OIDCAudience:      strings.TrimSpace(os.Getenv("GEO_OIDC_AUDIENCE")),
		OIDCJWKSURL:       strings.TrimSpace(os.Getenv("GEO_OIDC_JWKS_URL")),
		OIDCCAFile:        strings.TrimSpace(os.Getenv("GEO_OIDC_CA_FILE")),
		TrustedProxyCIDRs: strings.TrimSpace(os.Getenv("GEO_TRUSTED_PROXY_CIDRS")),
		TrustedProxyID:    strings.TrimSpace(os.Getenv("GEO_TRUSTED_PROXY_ID")),
		NMEATCPAddr:       strings.TrimSpace(os.Getenv("GEO_NMEA_TCP_ADDR")),
		NMEAUDPAddr:       strings.TrimSpace(os.Getenv("GEO_NMEA_UDP_ADDR")),
		AISStreamAPIKey:   strings.TrimSpace(os.Getenv("GEO_AISSTREAM_API_KEY")),
		GT06Addr:          strings.TrimSpace(os.Getenv("GEO_GT06_ADDR")),
		ReplayFile:        strings.TrimSpace(os.Getenv("GEO_REPLAY_FILE")),
	}
	dedupWindow, err := time.ParseDuration(getenv("GEO_DEDUP_WINDOW", "15s"))
	if err != nil {
		return config, fmt.Errorf("GEO_DEDUP_WINDOW: %w", err)
	}
	config.DedupWindow = dedupWindow
	replayInterval, err := time.ParseDuration(getenv("GEO_REPLAY_INTERVAL", "0s"))
	if err != nil {
		return config, fmt.Errorf("GEO_REPLAY_INTERVAL: %w", err)
	}
	config.ReplayInterval = replayInterval

	if config.PostgresDSN == "" {
		return config, errors.New("GEO_PG_DSN must be set")
	}
	if config.IngestPostgresDSN == "" {
		return config, errors.New("GEO_INGEST_PG_DSN must be set (geo_ingest role connection for the platform-wide geofence evaluator)")
	}
	if config.RedisAddr == "" {
		return config, errors.New("GEO_REDIS_ADDR must be set")
	}
	if len(config.KafkaBrokers) == 0 {
		return config, errors.New("GEO_KAFKA_BROKERS must be set")
	}
	if config.PrincipalID == "" || config.PrincipalRole == "" {
		return config, errors.New("GEO_PRODUCER_PRINCIPAL_ID and GEO_PRODUCER_PRINCIPAL_ROLE must be set")
	}
	if config.ReplayFile != "" && (config.AppEnv == "prod" || config.AppEnv == "production") {
		return config, errors.New("GEO_REPLAY_FILE is forbidden when APP_ENV=prod")
	}
	if config.PositionPlane != "shared" {
		// Fail closed: a tenant-scoped position plane was requested but the
		// position schema has no tenant column — silently falling back to
		// the shared picture would mis-scope national tracking data.
		return config, fmt.Errorf("GEO_POSITION_PLANE %q is unsupported: the vessel position plane is a single shared national picture scoped by classification clearance; tenant scoping has no schema support", config.PositionPlane)
	}
	if config.APIAddr != "" {
		switch config.AuthMode {
		case "oidc":
			if config.OIDCIssuer == "" || config.OIDCAudience == "" || config.OIDCJWKSURL == "" {
				return config, errors.New("GEO_OIDC_ISSUER, GEO_OIDC_AUDIENCE and GEO_OIDC_JWKS_URL are required in oidc auth mode")
			}
		case "trusted_proxy", "loopback_trusted_proxy":
			if config.TrustedProxyCIDRs == "" || config.TrustedProxyID == "" {
				return config, errors.New("GEO_TRUSTED_PROXY_CIDRS and GEO_TRUSTED_PROXY_ID are required in trusted_proxy auth mode")
			}
		default:
			return config, fmt.Errorf("GEO_AUTH_MODE %q is not oidc or trusted_proxy", config.AuthMode)
		}
	}
	return config, nil
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	out := make([]string, 0)
	for _, part := range strings.Split(value, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseBool(value string) bool {
	parsed, err := strconv.ParseBool(value)
	return err == nil && parsed
}
