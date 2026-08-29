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

	"github.com/redis/go-redis/v9"

	"github.com/munisp/blueeconomy-geo-service/db"
	"github.com/munisp/blueeconomy-geo-service/internal/api"
	"github.com/munisp/blueeconomy-geo-service/internal/auth"
	"github.com/munisp/blueeconomy-geo-service/internal/bus"
	"github.com/munisp/blueeconomy-geo-service/internal/config"
	"github.com/munisp/blueeconomy-geo-service/internal/connectors"
	"github.com/munisp/blueeconomy-geo-service/internal/dedup"
	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
	"github.com/munisp/blueeconomy-geo-service/internal/store"
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

	storage, err := store.New(ctx, cfg.PostgresDSN)
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
		appReports := &connectors.AppReportHandler{Pipeline: pipeline}
		httpServer := &http.Server{
			Addr:              cfg.APIAddr,
			Handler:           server.Handler(authenticator, appReports.RegisterRoutes),
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
