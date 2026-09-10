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
	KafkaBrokers      []string
	DedupWindow       time.Duration
	PublishAISRaw     bool

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

	// GTFS-RT producer knobs (advisory §5). StaleAfter is the position
	// staleness threshold: older positions → entity omitted, never
	// interpolated. The remaining knobs tune the stop snap and ETA engine.
	GTFSRTStaleAfter            time.Duration
	GTFSRTSnapMaxMeters         float64
	GTFSRTStopArriveMeters      float64
	GTFSRTStopSpeedMilliknots   uint32
	GTFSRTETAMinSpeedMilliknots uint32
	GTFSRTSpeedSamples          int

	// Connectors (each independently gated).
	NMEATCPAddr     string
	NMEAUDPAddr     string
	AISStreamAPIKey string
	GT06Addr        string
	ReplayFile      string
	ReplayInterval  time.Duration

	// SSEEnabled gates the in-process SSE fan-out hub (G1): validated
	// positions and fence transitions are broadcast to authenticated
	// /v1/geo/stream subscribers. Default off.
	SSEEnabled bool
	// FenceV2Ingest gates ingest-time WP-10 fence evaluation (G11): every
	// validated position is folded into the fence engine and transitions
	// emit signed geo.geofence-event.v1 envelopes + persist. Default off.
	FenceV2Ingest bool
	// PCSAISImportDSN gates the port-interop pcs_ais_positions consumer
	// (G2). Empty disables the importer (capabilities report
	// configured:false); set-but-unreachable aborts startup like every
	// other connector.
	PCSAISImportDSN  string
	PCSAISImportPoll time.Duration
	// RequestLog gates structured per-request JSON logging (unified
	// observability, #16). Default on.
	RequestLog bool

	// MLStack gates the Phase-18 shadow-mode policy recommendation surface
	// (/v1/geo/berths/recommendation, /v1/geo/routes/advice). Both empty =
	// disabled (endpoints answer 503 RECOMMENDATION_UNCONFIGURED);
	// half-configured fails startup. The token is env-only and never
	// logged (mirrors singlewindow's ML_STACK_HTTP_URL pattern).
	MLStackURL          string
	MLStackServiceToken string
	MLStackTimeout      time.Duration
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
		SSEEnabled:        parseBool(getenv("GEO_SSE_ENABLED", "false")),
		FenceV2Ingest:     parseBool(getenv("GEO_FENCE_V2_INGEST", "false")),
		PCSAISImportDSN:   strings.TrimSpace(os.Getenv("GEO_PCS_AIS_IMPORT_DSN")),
		RequestLog:        parseBool(getenv("GEO_REQUEST_LOG", "true")),
		MLStackURL:        strings.TrimSpace(os.Getenv("ML_STACK_HTTP_URL")),
		// Secret, env-only; intentionally not defaulted.
		MLStackServiceToken: strings.TrimSpace(os.Getenv("ML_STACK_SERVICE_TOKEN")),
	}
	pcsPoll, err := time.ParseDuration(getenv("GEO_PCS_AIS_IMPORT_POLL", "30s"))
	if err != nil || pcsPoll <= 0 {
		return config, fmt.Errorf("GEO_PCS_AIS_IMPORT_POLL: %w", err)
	}
	config.PCSAISImportPoll = pcsPoll
	mlTimeout, err := time.ParseDuration(getenv("GEO_ML_STACK_TIMEOUT", "5s"))
	if err != nil || mlTimeout <= 0 {
		return config, fmt.Errorf("GEO_ML_STACK_TIMEOUT: %w", err)
	}
	config.MLStackTimeout = mlTimeout
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
	staleAfter, err := time.ParseDuration(getenv("GEO_GTFSRT_STALE_AFTER", "120s"))
	if err != nil {
		return config, fmt.Errorf("GEO_GTFSRT_STALE_AFTER: %w", err)
	}
	if staleAfter <= 0 {
		return config, errors.New("GEO_GTFSRT_STALE_AFTER must be positive (fail-closed staleness threshold)")
	}
	config.GTFSRTStaleAfter = staleAfter
	config.GTFSRTSnapMaxMeters, err = parsePositiveFloat(getenv("GEO_GTFSRT_SNAP_MAX_METERS", "200"))
	if err != nil {
		return config, fmt.Errorf("GEO_GTFSRT_SNAP_MAX_METERS: %w", err)
	}
	config.GTFSRTStopArriveMeters, err = parsePositiveFloat(getenv("GEO_GTFSRT_STOP_ARRIVE_METERS", "20"))
	if err != nil {
		return config, fmt.Errorf("GEO_GTFSRT_STOP_ARRIVE_METERS: %w", err)
	}
	stopSpeed, err := strconv.ParseUint(getenv("GEO_GTFSRT_STOP_SPEED_MILLIKNOTS", "500"), 10, 32)
	if err != nil {
		return config, fmt.Errorf("GEO_GTFSRT_STOP_SPEED_MILLIKNOTS: %w", err)
	}
	config.GTFSRTStopSpeedMilliknots = uint32(stopSpeed)
	etaMinSpeed, err := strconv.ParseUint(getenv("GEO_GTFSRT_ETA_MIN_SPEED_MILLIKNOTS", "1000"), 10, 32)
	if err != nil {
		return config, fmt.Errorf("GEO_GTFSRT_ETA_MIN_SPEED_MILLIKNOTS: %w", err)
	}
	config.GTFSRTETAMinSpeedMilliknots = uint32(etaMinSpeed)
	speedSamples, err := strconv.Atoi(getenv("GEO_GTFSRT_SPEED_SAMPLES", "5"))
	if err != nil || speedSamples <= 0 {
		return config, fmt.Errorf("GEO_GTFSRT_SPEED_SAMPLES must be a positive integer")
	}
	config.GTFSRTSpeedSamples = speedSamples

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
	if (config.MLStackURL == "") != (config.MLStackServiceToken == "") {
		// Fail closed: half-configured policy scoring would otherwise either
		// send unauthenticated calls to an authenticated endpoint or strand
		// a token with no destination.
		return config, errors.New("ML_STACK_HTTP_URL and ML_STACK_SERVICE_TOKEN must be set together (or both unset to disable the recommendation surface)")
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

// parsePositiveFloat parses a strictly-positive float knob.
func parsePositiveFloat(value string) (float64, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}
	if parsed <= 0 {
		return 0, errors.New("must be positive")
	}
	return parsed, nil
}
