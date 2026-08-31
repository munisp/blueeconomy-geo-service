// Package telemetry wires OpenTelemetry tracing for geo-service. Telemetry
// is environment-configured: OTEL_EXPORTER_OTLP_ENDPOINT unset means tracing
// is DISABLED and the service boots and serves exactly as before (explicit
// no-op tracer); a malformed endpoint or contradictory OTEL_SDK_DISABLED is
// a startup error (fail-closed configuration, matching the service posture).
//
// Export is the platform's one sanctioned fail-open (OTEL_DESIGN §1): the
// OTLP exporter is async/batched and non-blocking, and an unreachable
// collector means spans are dropped with the telemetry_dropped_total counter
// (registered in the service metrics registry) incremented — never a request
// failure. Shutdown flush is bounded at 5 seconds and never blocks SIGTERM.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	otlptracegrpc "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// ShutdownFlushTimeout bounds the graceful-shutdown span flush.
const ShutdownFlushTimeout = 5 * time.Second

// Config is the validated telemetry configuration. Enabled is false when no
// OTLP endpoint is configured; every other field is then ignored.
type Config struct {
	Enabled     bool
	Endpoint    string
	Insecure    bool
	ServiceName string
}

// LoadConfig reads OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_INSECURE,
// OTEL_SERVICE_NAME and OTEL_SDK_DISABLED. An absent endpoint means tracing
// is disabled; a present but malformed endpoint, an unknown boolean value,
// or a contradictory OTEL_SDK_DISABLED=true fails closed.
func LoadConfig(serviceName string) (Config, error) {
	if strings.TrimSpace(serviceName) == "" {
		return Config{}, errors.New("telemetry service name is required")
	}
	config := Config{ServiceName: serviceName}
	if override := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); override != "" {
		if len(override) > 128 {
			return Config{}, errors.New("OTEL_SERVICE_NAME must be at most 128 characters")
		}
		config.ServiceName = override
	}
	disabled, err := parseBoolean("OTEL_SDK_DISABLED")
	if err != nil {
		return Config{}, err
	}
	insecure, err := parseBoolean("OTEL_EXPORTER_OTLP_INSECURE")
	if err != nil {
		return Config{}, err
	}
	config.Insecure = insecure
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if disabled {
		if endpoint != "" {
			return Config{}, errors.New("OTEL_SDK_DISABLED=true conflicts with OTEL_EXPORTER_OTLP_ENDPOINT; remove one (fail-closed)")
		}
		return config, nil
	}
	if endpoint == "" {
		return config, nil
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return Config{}, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must be a host:port pair without scheme, credentials or path: %q", endpoint)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return Config{}, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT has an invalid port: %q", endpoint)
	}
	config.Enabled = true
	config.Endpoint = endpoint
	return config, nil
}

// parseBoolean accepts only empty, "true" or "false"; anything else fails
// closed rather than being silently interpreted.
func parseBoolean(name string) (bool, error) {
	switch value := strings.TrimSpace(os.Getenv(name)); value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, fmt.Errorf("%s must be true or false when set", name)
	}
}

// Propagator is the platform propagation contract: W3C tracecontext plus
// baggage (tenant.id and agency ride baggage from the edge to every server
// span).
func Propagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}

// dropCountingExporter wraps the OTLP exporter so a collector outage is a
// drop-with-metric event (telemetry_dropped_total), never a request error.
type dropCountingExporter struct {
	wrapped sdktrace.SpanExporter
	onDrop  func(spans int64)
}

func (exporter dropCountingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := exporter.wrapped.ExportSpans(ctx, spans); err != nil {
		exporter.onDrop(int64(len(spans)))
		return err
	}
	return nil
}

func (exporter dropCountingExporter) Shutdown(ctx context.Context) error {
	return exporter.wrapped.Shutdown(ctx)
}

// Telemetry carries the tracer pipeline and the HTTP middleware. Construct
// only through Setup.
type Telemetry struct {
	config         Config
	tracer         trace.Tracer
	tracerProvider *sdktrace.TracerProvider
	droppedTotal   atomic.Int64
	// onDrop mirrors dropped spans into the service metrics registry
	// (telemetry_dropped_total); optional.
	onDrop func(spans int64)
}

// Setup builds the tracer pipeline. Disabled configuration yields an
// explicit no-op tracer; enabled configuration installs the global SDK
// tracer provider with async/batched export. The propagation contract is
// installed in both modes so inject/extract behaves identically.
func Setup(ctx context.Context, config Config) (*Telemetry, error) {
	if strings.TrimSpace(config.ServiceName) == "" {
		return nil, errors.New("telemetry service name is required")
	}
	telemetry := &Telemetry{config: config}
	otel.SetTextMapPropagator(Propagator())
	if !config.Enabled {
		telemetry.tracer = noop.NewTracerProvider().Tracer(config.ServiceName)
		return telemetry, nil
	}
	serviceResource := resource.NewSchemaless(attribute.String("service.name", config.ServiceName))
	exporterOptions := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(config.Endpoint)}
	if config.Insecure {
		exporterOptions = append(exporterOptions, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, exporterOptions...)
	if err != nil {
		return nil, fmt.Errorf("create OTLP gRPC trace exporter: %w", err)
	}
	telemetry.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(dropCountingExporter{wrapped: exporter, onDrop: telemetry.recordDropped}),
		sdktrace.WithResource(serviceResource),
	)
	otel.SetTracerProvider(telemetry.tracerProvider)
	telemetry.tracer = telemetry.tracerProvider.Tracer(config.ServiceName)
	return telemetry, nil
}

// SetDropHook registers the drop-with-metric sink (the service metrics
// registry increments telemetry_dropped_total).
func (telemetry *Telemetry) SetDropHook(onDrop func(spans int64)) {
	telemetry.onDrop = onDrop
}

// recordDropped counts spans dropped on export failure (collector down).
func (telemetry *Telemetry) recordDropped(spans int64) {
	telemetry.droppedTotal.Add(spans)
	if telemetry.onDrop != nil {
		telemetry.onDrop(spans)
	}
}

// DroppedTotal reports the lifetime count of spans dropped on export
// failure. It is the in-process view of telemetry_dropped_total.
func (telemetry *Telemetry) DroppedTotal() int64 {
	return telemetry.droppedTotal.Load()
}

// Enabled reports whether OTLP trace export is active.
func (telemetry *Telemetry) Enabled() bool {
	return telemetry.config.Enabled
}

// Tracer returns the service tracer: the OTLP-backed SDK tracer when
// enabled, otherwise the explicit no-op tracer.
func (telemetry *Telemetry) Tracer() trace.Tracer {
	return telemetry.tracer
}

// Shutdown flushes and stops the tracer provider; callers bound the context
// (ShutdownFlushTimeout) so SIGTERM is never blocked.
func (telemetry *Telemetry) Shutdown(ctx context.Context) error {
	if telemetry.tracerProvider != nil {
		return telemetry.tracerProvider.Shutdown(ctx)
	}
	return nil
}

// statusRecorder captures the response status code for span attributes
// without changing response semantics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

// Middleware traces every request. The outer otelhttp handler extracts the
// W3C traceparent/baggage context and starts the server span; the span is
// named by the matched route pattern (http.Request.Pattern) so span names
// never carry raw paths or identifiers. tenant.id and agency baggage members
// become span attributes on every server span.
func (telemetry *Telemetry) Middleware(next http.Handler) http.Handler {
	inner := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		ctx := request.Context()
		span := trace.SpanFromContext(ctx)
		recorder := &statusRecorder{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(recorder, request)
		// Tenant attribution: edge-injected baggage → span attributes.
		if member := baggage.FromContext(ctx).Member("tenant.id"); member.Value() != "" {
			span.SetAttributes(attribute.String("tenant.id", member.Value()))
		}
		if member := baggage.FromContext(ctx).Member("agency"); member.Value() != "" {
			span.SetAttributes(attribute.String("agency", member.Value()))
		}
		if recorder.status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(recorder.status))
		}
	})
	options := []otelhttp.Option{
		otelhttp.WithPropagators(Propagator()),
		// otelhttp applies the formatter after the handler returns, once the
		// ServeMux has recorded the matched pattern on the request.
		otelhttp.WithSpanNameFormatter(func(_ string, request *http.Request) string {
			if request.Pattern != "" {
				return request.Pattern
			}
			return request.Method
		}),
	}
	if telemetry.tracerProvider != nil {
		options = append(options, otelhttp.WithTracerProvider(telemetry.tracerProvider))
	}
	return otelhttp.NewHandler(inner, "http.server", options...)
}

// HTTPTransport wraps an outbound RoundTripper with otelhttp client tracing:
// every outbound HTTP call becomes a CLIENT span and the live traceparent +
// baggage are injected into the request headers. A nil base wraps
// http.DefaultTransport.
func HTTPTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base)
}
