// mrv-api is the Phase-8 MRV emissions boundary: the operator-facing
// intake/verification REST API (/v1/mrv/*) over the geo-service vessel
// identity and PostGIS activity plane, with the transactional outbox
// publisher draining signed envelope v1.0 events to the mrv.* Kafka topics.
//
// Everything is env-gated and fails closed: an unsigned pipeline, an empty
// broker list, an unreachable database or a malformed CII config document
// aborts startup. Telemetry is disabled (explicit no-op) unless
// OTEL_EXPORTER_OTLP_ENDPOINT is set.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-geo-service/db"
	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/bus"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/mrv"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
	"github.com/munisp/blueeconomy-geo-service/internal/telemetry"
)

func main() {
	logger := log.New(os.Stdout, "mrv-api ", log.LstdFlags|log.LUTC)
	if err := run(logger); err != nil {
		logger.Fatalf("startup failed: %v", err)
	}
}

func run(logger *log.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Telemetry first (fail-closed config, no-op when disabled).
	telemetryConfig, err := telemetry.LoadConfig(mrv.ProducerName)
	if err != nil {
		return err
	}
	registry := metrics.NewRegistry()
	telemetryPipeline, err := telemetry.Setup(ctx, telemetryConfig)
	if err != nil {
		return err
	}
	telemetryPipeline.SetDropHook(func(spans int64) {
		registry.Add("telemetry_dropped_total", nil, spans)
	})
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), telemetry.ShutdownFlushTimeout)
		defer cancel()
		if err := telemetryPipeline.Shutdown(shutdownCtx); err != nil {
			logger.Printf("telemetry shutdown: %v", err)
		}
	}()
	if telemetryPipeline.Enabled() {
		logger.Printf("telemetry: OTLP export enabled (endpoint=%s)", telemetryConfig.Endpoint)
	} else {
		logger.Printf("telemetry: tracing disabled (OTEL_EXPORTER_OTLP_ENDPOINT not set); no-op tracer active")
	}

	dsn := strings.TrimSpace(os.Getenv("MRV_PG_DSN"))
	if dsn == "" {
		return errors.New("MRV_PG_DSN must be set")
	}
	brokers := splitCSV(os.Getenv("MRV_KAFKA_BROKERS"))
	if len(brokers) == 0 {
		return errors.New("MRV_KAFKA_BROKERS must be set")
	}
	apiAddr := strings.TrimSpace(os.Getenv("MRV_API_ADDR"))
	if apiAddr == "" {
		return errors.New("MRV_API_ADDR must be set")
	}
	principalID := strings.TrimSpace(os.Getenv("MRV_PRODUCER_PRINCIPAL_ID"))
	principalRole := strings.TrimSpace(getenv("MRV_PRODUCER_PRINCIPAL_ROLE", "mrv-producer"))
	if principalID == "" {
		return errors.New("MRV_PRODUCER_PRINCIPAL_ID must be set")
	}

	signer, err := sign.SignerFromEnvForProducer(mrv.ProducerName)
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	if os.Getenv("MRV_RUN_MIGRATIONS") != "false" {
		if err := store.MigratePool(ctx, pool, db.MigrationsFS); err != nil {
			return err
		}
	}

	// Operator-approved, source-cited CII configuration: absent means every
	// CII outcome is honestly NOT_COMPUTABLE (never estimated).
	ciiConfig, err := mrv.LoadCIIConfig(os.Getenv(mrv.CIIConfigPathEnv))
	if err != nil {
		return err
	}
	deadlines, err := mrv.DeadlinesFromEnv()
	if err != nil {
		return err
	}
	activityParams, err := activityParamsFromEnv()
	if err != nil {
		return err
	}
	tolerance, err := envUint32("MRV_AIS_CROSSCHECK_TOLERANCE_PERMILLE", 100)
	if err != nil {
		return err
	}
	gtThreshold, err := envUint32("MRV_DCS_GT_THRESHOLD", 5000)
	if err != nil {
		return err
	}

	service, err := mrv.NewService(pool, signer, sign.Provenance{
		PrincipalID: principalID, PrincipalRole: principalRole,
	}, registry, ciiConfig, deadlines, activityParams, tolerance, gtThreshold)
	if err != nil {
		return err
	}

	producer, err := bus.NewProducer(bus.Config{Brokers: brokers})
	if err != nil {
		return err
	}
	defer producer.Close()

	pollInterval, err := time.ParseDuration(getenv("MRV_OUTBOX_POLL_INTERVAL", "2s"))
	if err != nil {
		return fmt.Errorf("MRV_OUTBOX_POLL_INTERVAL: %w", err)
	}
	outboxPublisher, err := mrv.NewOutboxPublisher(service, producer, pollInterval, 100)
	if err != nil {
		return err
	}
	go outboxPublisher.Run(ctx)

	authenticator, err := buildAuthenticator()
	if err != nil {
		return err
	}
	server, err := mrv.NewServer(service)
	if err != nil {
		return err
	}
	var handler http.Handler = server.Handler(authenticator)
	handler = telemetryPipeline.Middleware(handler)
	httpServer := &http.Server{
		Addr:              apiAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	go func() {
		logger.Printf("api: listening %s", apiAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("api server: %v", err)
		}
	}()

	logger.Printf("started (kid=%s, ciiConfig=%v)", signer.KeyID(), ciiConfig != nil)
	<-ctx.Done()
	return nil
}

// buildAuthenticator wires the configured auth mode (Keycloak RS256 OIDC or
// trusted edge proxy), MRV_-prefixed environment.
func buildAuthenticator() (auth.Authenticator, error) {
	switch strings.ToLower(strings.TrimSpace(getenv("MRV_AUTH_MODE", "oidc"))) {
	case "oidc":
		jwksURL, err := url.Parse(os.Getenv("MRV_OIDC_JWKS_URL"))
		if err != nil {
			return nil, errors.New("MRV_OIDC_JWKS_URL is invalid")
		}
		return auth.NewOIDCAuthenticator(os.Getenv("MRV_OIDC_ISSUER"), os.Getenv("MRV_OIDC_AUDIENCE"),
			jwksURL, os.Getenv("MRV_OIDC_CA_FILE"))
	case "trusted_proxy", "loopback_trusted_proxy":
		cidrs := make([]*net.IPNet, 0)
		for _, cidr := range strings.Split(os.Getenv("MRV_TRUSTED_PROXY_CIDRS"), ",") {
			_, network, err := net.ParseCIDR(strings.TrimSpace(cidr))
			if err != nil {
				return nil, errors.New("MRV_TRUSTED_PROXY_CIDRS contains an invalid CIDR")
			}
			cidrs = append(cidrs, network)
		}
		return auth.TrustedProxyAuthenticator{CIDRs: cidrs, Identity: os.Getenv("MRV_TRUSTED_PROXY_ID")}, nil
	default:
		return nil, errors.New("MRV_AUTH_MODE is not oidc or trusted_proxy")
	}
}

// activityParamsFromEnv resolves the AIS estimation methodology parameters.
func activityParamsFromEnv() (mrv.ActivityParams, error) {
	params := mrv.DefaultActivityParams()
	sog, err := envUint32("MRV_AIS_SOG_THRESHOLD_MILLIKNOTS", params.SogThresholdMilliknots)
	if err != nil {
		return params, err
	}
	params.SogThresholdMilliknots = sog
	if raw := strings.TrimSpace(os.Getenv("MRV_AIS_SEGMENT_GAP_MINUTES")); raw != "" {
		minutes, err := strconv.Atoi(raw)
		if err != nil {
			return params, fmt.Errorf("MRV_AIS_SEGMENT_GAP_MINUTES: %w", err)
		}
		params.SegmentGap = time.Duration(minutes) * time.Minute
	}
	coverage, err := envUint32("MRV_AIS_MIN_COVERAGE_PERMILLE", params.MinCoveragePermille)
	if err != nil {
		return params, err
	}
	params.MinCoveragePermille = coverage
	return params, params.Validate()
}

func getenv(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func splitCSV(raw string) []string {
	out := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func envUint32(name string, fallback uint32) (uint32, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s must be an unsigned integer: %w", name, err)
	}
	return uint32(value), nil
}
