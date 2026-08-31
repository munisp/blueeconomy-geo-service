// geo-service is the sovereign vessel-tracking hot path: connectors (NMEA
// TCP/UDP :10110, aisstream.io WebSocket, GT06/Concox TCP, Tier-0 app-report
// HTTP) feed decode → validate → dedup → PostGIS store → geofence evaluate
// → signed envelope → Kafka, and the /v1/geo REST API serves the read model.
//
// Everything is env-gated and fails closed: an unsigned pipeline, an empty
// broker list, an unreachable database or a misconfigured connector aborts
// startup. GEO_REPLAY_FILE (synthetic fixture replay) refuses to start when
// APP_ENV=prod.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/munisp/blueeconomy-geo-service/db"
	"github.com/munisp/blueeconomy-geo-service/internal/api"
	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/bus"
	"github.com/munisp/blueeconomy-geo-service/internal/config"
	"github.com/munisp/blueeconomy-geo-service/internal/connectors"
	"github.com/munisp/blueeconomy-geo-service/internal/dedup"
	"github.com/munisp/blueeconomy-geo-service/internal/devices"
	"github.com/munisp/blueeconomy-geo-service/internal/gtfsrt"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
	"github.com/munisp/blueeconomy-geo-service/internal/telemetry"
	"github.com/munisp/blueeconomy-geo-service/internal/validate"
)

func main() {
	logger := log.New(os.Stdout, "geo-service ", log.LstdFlags|log.LUTC)
	if err := run(logger); err != nil {
		logger.Fatalf("startup failed: %v", err)
	}
}

func run(logger *log.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	signer, err := sign.SignerFromEnv()
	if err != nil {
		return err
	}
	registry := metrics.NewRegistry()

	// Telemetry (OTEL_DESIGN §2 Go row): OTEL_EXPORTER_OTLP_ENDPOINT unset
	// means tracing is DISABLED and the service boots and serves exactly as
	// before; when set, export is async/batched and collector-down is
	// drop-with-metric (telemetry_dropped_total), never a request failure.
	telemetryConfig, err := telemetry.LoadConfig("blueeconomy-geo-service")
	if err != nil {
		return err
	}
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

	storage, err := store.New(ctx, cfg.PostgresDSN, cfg.IngestPostgresDSN)
	if err != nil {
		return err
	}
	defer storage.Close()
	if os.Getenv("GEO_RUN_MIGRATIONS") != "false" {
		if err := store.Migrate(ctx, storage, db.MigrationsFS); err != nil {
			return err
		}
	}
	// Provision today's and tomorrow's position partitions, then daily.
	if err := storage.EnsurePositionPartitions(ctx, time.Now(), time.Now().Add(24*time.Hour)); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(6 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := storage.EnsurePositionPartitions(ctx, time.Now(), time.Now().Add(24*time.Hour)); err != nil {
					logger.Printf("partition provisioning: %v", err)
					registry.Inc("geo_partition_errors_total", nil)
				}
			}
		}
	}()

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()
	// otelredis client instrumentation (go-redis maintained; no-op spans
	// when telemetry is disabled).
	if err := redisotel.InstrumentTracing(redisClient); err != nil {
		return errors.New("instrument redis client: " + err.Error())
	}
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return errors.New("redis unreachable: " + err.Error())
	}
	dedupChecker, err := dedup.NewChecker(redisClient, cfg.DedupWindow)
	if err != nil {
		return err
	}
	zoneState, err := store.NewZoneStateStore(redisClient)
	if err != nil {
		return err
	}

	producer, err := bus.NewProducer(bus.Config{Brokers: cfg.KafkaBrokers})
	if err != nil {
		return err
	}
	defer producer.Close()

	pipeline, err := connectors.NewPipeline(connectors.Pipeline{
		Store:     storage,
		Dedup:     dedupChecker,
		ZoneState: zoneState,
		Producer:  producer,
		Signer:    signer,
		Tracker:   validate.NewTracker(),
		Metrics:   registry,
		Principal: sign.Provenance{
			PrincipalID:   cfg.PrincipalID,
			PrincipalRole: cfg.PrincipalRole,
		},
		PublishRaw: cfg.PublishAISRaw,
	})
	if err != nil {
		return err
	}

	// Connectors (each env-gated).
	connectorErrors := make(chan error, 8)
	started := 0
	if cfg.NMEATCPAddr != "" || cfg.NMEAUDPAddr != "" {
		started++
		listener := &connectors.NMEAListener{
			TCPAddr: cfg.NMEATCPAddr, UDPAddr: cfg.NMEAUDPAddr,
			Pipeline: pipeline, Logger: logger,
		}
		go func() { connectorErrors <- listener.Run(ctx) }()
	}
	if cfg.AISStreamAPIKey != "" {
		started++
		client := &connectors.AISStreamClient{
			Config:   connectors.AISStreamConfig{APIKey: cfg.AISStreamAPIKey},
			Pipeline: pipeline, Logger: logger,
		}
		go func() { connectorErrors <- client.Run(ctx) }()
	}
	if cfg.GT06Addr != "" {
		started++
		server := &connectors.GT06Server{Addr: cfg.GT06Addr, Pipeline: pipeline, Logger: logger}
		go func() { connectorErrors <- server.Run(ctx) }()
	}
	if cfg.ReplayFile != "" {
		started++
		replay := &connectors.ReplayConnector{
			File: cfg.ReplayFile, AppEnv: cfg.AppEnv,
			Pipeline: pipeline, Logger: logger, Interval: cfg.ReplayInterval,
		}
		go func() {
			if err := replay.Run(ctx); err != nil {
				connectorErrors <- err
			}
		}()
	}
	if started == 0 && cfg.APIAddr == "" {
		return errors.New("no connector enabled and no API address configured")
	}

	// REST API.
	if cfg.APIAddr != "" {
		authenticator, err := buildAuthenticator(cfg)
		if err != nil {
			return err
		}
		server, err := api.NewServer(storage, registry)
		if err != nil {
			return err
		}
		// The SOS lifecycle endpoints publish signed transition envelopes
		// through the hot-path pipeline.
		server.SOSEvents = pipeline
		// WP-10 surface: versioned geofences, fence evaluation, track APIs,
		// congestion forecast. Fence transitions publish through the same
		// signed-envelope pipeline (fail-closed when unwired).
		geoV2, err := api.NewGeoV2(storage)
		if err != nil {
			return err
		}
		geoV2.FenceEvents = pipeline
		server.GeoV2 = geoV2
		// GTFS static + GTFS-RT feeds (advisory §5): the AIS→GTFS-RT
		// adapter, staleness-gated and fail-closed.
		feedBuilder, err := gtfsrt.NewBuilder(storage, registry, gtfsrt.Config{
			StaleAfter:            cfg.GTFSRTStaleAfter,
			SnapMaxMeters:         cfg.GTFSRTSnapMaxMeters,
			StopArriveMeters:      cfg.GTFSRTStopArriveMeters,
			StopSpeedMilliknots:   cfg.GTFSRTStopSpeedMilliknots,
			ETAMinSpeedMilliknots: cfg.GTFSRTETAMinSpeedMilliknots,
			SpeedSampleCount:      cfg.GTFSRTSpeedSamples,
		})
		if err != nil {
			return err
		}
		if err := server.AttachFeeds(feedBuilder); err != nil {
			return err
		}
		appReports := &connectors.AppReportHandler{Pipeline: pipeline}
		apiHandler := server.Handler(authenticator, appReports.RegisterRoutes)
		// === BEGIN phase7 device-management plane (internal/devices) ===
		// Mounted OUTSIDE the platform principal middleware: the device
		// endpoints authenticate by Ed25519 signed envelope/proof. Admin
		// routes wrap the authenticator per-route inside the device mux.
		// The plane is env-gated (GEO_DEVICES_PG_DSN); absent it, /v1/devices/*
		// does not exist (fail closed).
		if deviceHandler, derr := buildDevicePlane(cfg, storage, pipeline, producer, registry, authenticator, signer); derr != nil {
			return derr
		} else if deviceHandler != nil {
			geoHandler := apiHandler
			apiHandler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if strings.HasPrefix(request.URL.Path, "/v1/devices/") ||
					strings.HasPrefix(request.URL.Path, "/v1/device-provisioning/") {
					deviceHandler.ServeHTTP(writer, request)
					return
				}
				geoHandler.ServeHTTP(writer, request)
			})
		}
		// === END phase7 device-management plane ===
		// otelhttp server middleware on the outermost handler: W3C
		// traceparent/baggage extraction, route-pattern span names and
		// tenant.id/agency baggage → span attributes on every request
		// (geo API, device plane and MQTT auth webhook alike).
		apiHandler = telemetryPipeline.Middleware(apiHandler)
		httpServer := &http.Server{
			Addr:              cfg.APIAddr,
			Handler:           apiHandler,
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
			logger.Printf("api: listening %s", cfg.APIAddr)
			if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				connectorErrors <- err
			}
		}()
	}

	logger.Printf("started (env=%s, kid=%s, connectors=%d)", cfg.AppEnv, signer.KeyID(), started)
	select {
	case <-ctx.Done():
		return nil
	case err := <-connectorErrors:
		if err != nil {
			return err
		}
		return nil
	}
}

// === BEGIN phase7 device-management plane (internal/devices) ===
// buildDevicePlane wires the device-management plane when
// GEO_DEVICES_PG_DSN is set (geo_devices role connection for the
// verify-at-ingest path — privilege separation by connection, never by
// SET ROLE). Returns (nil, nil) when the plane is disabled. It fails
// closed on any misconfiguration of an enabled plane.
func buildDevicePlane(_ config.Config, storage *store.Store, pipeline *connectors.Pipeline,
	producer *bus.Producer, registry *metrics.Registry, authenticator auth.Authenticator,
	signer *sign.Signer) (http.Handler, error) {
	devicesDSN := strings.TrimSpace(os.Getenv("GEO_DEVICES_PG_DSN"))
	if devicesDSN == "" {
		return nil, nil
	}
	grace := devices.DefaultKeyGrace
	if raw := strings.TrimSpace(os.Getenv("GEO_DEVICE_KEY_GRACE")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			return nil, errors.New("GEO_DEVICE_KEY_GRACE must be a positive duration")
		}
		grace = parsed
	}
	deviceStore, err := devices.NewStore(context.Background(), storage, devicesDSN)
	if err != nil {
		return nil, err
	}
	verifier, err := devices.NewVerifier(deviceStore, registry, grace)
	if err != nil {
		return nil, err
	}
	// OTA manifests are signed with the same service envelope key (already
	// validated at startup) under the service kid.
	manifestKey, err := sign.ParsePrivateKey(os.Getenv(sign.SigningKeyEnv))
	if err != nil {
		return nil, err
	}
	deviceAPI, err := devices.NewAPI(&devices.API{
		Store:         deviceStore,
		Verifier:      verifier,
		Metrics:       registry,
		Events:        pipeline,
		DeadLetters:   producer,
		ManifestKey:   manifestKey,
		ManifestKeyID: signer.KeyID(),
		Grace:         grace,
	})
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	deviceAPI.RegisterRoutes(mux, authenticator)
	return mux, nil
}

// buildAuthenticator wires the configured auth mode (Keycloak RS256 OIDC or
// trusted edge proxy).
func buildAuthenticator(cfg config.Config) (auth.Authenticator, error) {
	switch cfg.AuthMode {
	case "oidc":
		jwksURL, err := url.Parse(cfg.OIDCJWKSURL)
		if err != nil {
			return nil, errors.New("GEO_OIDC_JWKS_URL is invalid")
		}
		return auth.NewOIDCAuthenticator(cfg.OIDCIssuer, cfg.OIDCAudience, jwksURL, cfg.OIDCCAFile)
	case "trusted_proxy", "loopback_trusted_proxy":
		cidrs := make([]*net.IPNet, 0)
		for _, cidr := range strings.Split(cfg.TrustedProxyCIDRs, ",") {
			_, network, err := net.ParseCIDR(strings.TrimSpace(cidr))
			if err != nil {
				return nil, errors.New("GEO_TRUSTED_PROXY_CIDRS contains an invalid CIDR")
			}
			cidrs = append(cidrs, network)
		}
		return auth.TrustedProxyAuthenticator{CIDRs: cidrs, Identity: cfg.TrustedProxyID}, nil
	default:
		return nil, errors.New("GEO_AUTH_MODE is not oidc or trusted_proxy")
	}
}
