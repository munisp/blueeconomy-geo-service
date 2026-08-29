package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func baseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("GEO_PG_DSN", "postgres://geo:secret@localhost:5432/geo")
	t.Setenv("GEO_INGEST_PG_DSN", "postgres://geo_ingest:secret@localhost:5432/geo")
	t.Setenv("GEO_REDIS_ADDR", "localhost:6379")
	t.Setenv("GEO_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("GEO_PRODUCER_PRINCIPAL_ID", "svc-geo")
	t.Setenv("GEO_PRODUCER_PRINCIPAL_ROLE", "geo-producer")
}

func TestFromEnvRequiresCoreSettings(t *testing.T) {
	_, err := FromEnv()
	require.Error(t, err)
}

func TestFromEnvValid(t *testing.T) {
	baseEnv(t)
	config, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, []string{"localhost:9092"}, config.KafkaBrokers)
	require.Equal(t, "dev", config.AppEnv)
}

func TestFromEnvRejectsReplayInProd(t *testing.T) {
	baseEnv(t)
	t.Setenv("APP_ENV", "prod")
	t.Setenv("GEO_REPLAY_FILE", "fixtures/nmea_replay.txt")
	_, err := FromEnv()
	require.Error(t, err, "replay file must be refused in prod")
}

func TestFromEnvAllowsReplayInDev(t *testing.T) {
	baseEnv(t)
	t.Setenv("GEO_REPLAY_FILE", "fixtures/nmea_replay.txt")
	config, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "fixtures/nmea_replay.txt", config.ReplayFile)
}

func TestFromEnvAPIRequiresAuthConfig(t *testing.T) {
	baseEnv(t)
	t.Setenv("GEO_API_ADDR", ":8080")
	_, err := FromEnv()
	require.Error(t, err, "oidc mode requires issuer/audience/jwks")
	t.Setenv("GEO_OIDC_ISSUER", "https://keycloak.example/realms/blueeconomy")
	t.Setenv("GEO_OIDC_AUDIENCE", "geo-service")
	t.Setenv("GEO_OIDC_JWKS_URL", "https://keycloak.example/realms/blueeconomy/protocol/openid-connect/certs")
	config, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "oidc", config.AuthMode)
}

func TestFromEnvPositionPlane(t *testing.T) {
	baseEnv(t)
	// Default: the shared national picture.
	config, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "shared", config.PositionPlane)

	// Explicit shared is accepted.
	t.Setenv("GEO_POSITION_PLANE", "shared")
	config, err = FromEnv()
	require.NoError(t, err)
	require.Equal(t, "shared", config.PositionPlane)

	// Tenant scoping has no schema support: fail closed, never silently
	// fall back to the shared plane.
	t.Setenv("GEO_POSITION_PLANE", "tenant")
	_, err = FromEnv()
	require.Error(t, err, "tenant position plane must fail closed without schema support")

	// Unknown values fail closed as well.
	t.Setenv("GEO_POSITION_PLANE", "regional")
	_, err = FromEnv()
	require.Error(t, err, "unknown position plane must fail closed")
}
