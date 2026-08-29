package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestLoadConfigDefaultsToDisabled(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	config, err := LoadConfig("blueeconomy-geo-service")
	if err != nil {
		t.Fatalf("default config must load: %v", err)
	}
	if config.Enabled {
		t.Fatal("tracing must default to disabled when no endpoint is configured")
	}
}

func TestLoadConfigFailsClosedOnMalformedValues(t *testing.T) {
	cases := []struct {
		name string
		envs map[string]string
	}{
		{"endpoint with scheme", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.example.invalid:4317"}},
		{"endpoint without port", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "collector.example.invalid"}},
		{"endpoint with credentials", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "user:secret@collector:4317"}},
		{"disabled flag garbage", map[string]string{"OTEL_SDK_DISABLED": "yes"}},
		{"conflicting disabled and endpoint", map[string]string{"OTEL_SDK_DISABLED": "true", "OTEL_EXPORTER_OTLP_ENDPOINT": "collector:4317"}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
			t.Setenv("OTEL_SDK_DISABLED", "")
			t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
			for name, value := range testCase.envs {
				t.Setenv(name, value)
			}
			if _, err := LoadConfig("blueeconomy-geo-service"); err == nil {
				t.Fatalf("case %q must fail closed", testCase.name)
			}
		})
	}
}

// testTelemetry builds a Telemetry backed by an in-memory span recorder,
// mirroring Setup without any network dependency.
func testTelemetry(t *testing.T) (*Telemetry, *tracetest.SpanRecorder) {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(recorder),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "telemetry-test"))),
	)
	telemetry := &Telemetry{
		config:         Config{ServiceName: "telemetry-test", Enabled: true},
		tracer:         tracerProvider.Tracer("telemetry-test"),
		tracerProvider: tracerProvider,
	}
	t.Cleanup(func() { _ = telemetry.Shutdown(context.Background()) })
	return telemetry, recorder
}

// TestDisabledBootServesRequests is the telemetry-off contract: no endpoint
// configured → Setup succeeds, requests are served with a non-recording
// no-op span and nothing panics.
func TestDisabledBootServesRequests(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "")
	config, err := LoadConfig("blueeconomy-geo-service")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	telemetry, err := Setup(context.Background(), config)
	if err != nil {
		t.Fatalf("disabled setup must succeed: %v", err)
	}
	if telemetry.Enabled() {
		t.Fatal("telemetry must report disabled")
	}
	handler := telemetry.Middleware(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if trace.SpanFromContext(request.Context()).IsRecording() {
			t.Error("disabled mode must use a no-op, non-recording span")
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("request must be served, got %d", response.Code)
	}
	if err := telemetry.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestKafkaCarrierRoundTrip injects the live trace context into kafka-go
// record headers (produce) and extracts it (consume).
func TestKafkaCarrierRoundTrip(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("propagation-test").Start(context.Background(), "produce")
	defer span.End()
	producerContext := trace.SpanContextFromContext(ctx)

	headers := InjectKafkaHeaders(ctx, []kafka.Header{{Key: "priority", Value: []byte("SAFETY")}})
	carrier := KafkaCarrier{Headers: &headers}
	if carrier.Get("traceparent") == "" {
		t.Fatalf("traceparent must be injected into the record headers, got %v", headers)
	}
	if carrier.Get("priority") != "SAFETY" {
		t.Fatal("existing record headers must be preserved")
	}
	consumerContext := trace.SpanContextFromContext(ExtractKafkaHeaders(context.Background(), headers))
	if consumerContext.TraceID() != producerContext.TraceID() || consumerContext.SpanID() != producerContext.SpanID() {
		t.Fatalf("consumer must join the producer trace: got %s/%s, want %s/%s",
			consumerContext.TraceID(), consumerContext.SpanID(), producerContext.TraceID(), producerContext.SpanID())
	}
	if !consumerContext.IsRemote() {
		t.Fatal("extracted context must be marked remote")
	}
}

// TestHTTPHeaderRoundTrip verifies the W3C traceparent survives outbound
// inject → inbound extract.
func TestHTTPHeaderRoundTrip(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	ctx, span := provider.Tracer("propagation-test").Start(context.Background(), "client")
	defer span.End()
	producerContext := trace.SpanContextFromContext(ctx)

	request := httptest.NewRequest(http.MethodGet, "http://downstream.invalid/v1/geo/vessels", nil)
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(request.Header))
	if request.Header.Get("traceparent") == "" {
		t.Fatal("traceparent must be injected into outbound headers")
	}
	serverContext := trace.SpanContextFromContext(Propagator().Extract(context.Background(), propagation.HeaderCarrier(request.Header)))
	if serverContext.TraceID() != producerContext.TraceID() || serverContext.SpanID() != producerContext.SpanID() {
		t.Fatalf("server must continue the client trace: got %s/%s", serverContext.TraceID(), serverContext.SpanID())
	}
}

// TestMiddlewareServerSpanRouteAndStatus drives one server handler through
// the middleware and asserts the route-pattern span name.
func TestMiddlewareServerSpanRouteAndStatus(t *testing.T) {
	telemetry, recorder := testTelemetry(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/geo/vessels/{mmsi}", func(writer http.ResponseWriter, request *http.Request) {
		if !trace.SpanFromContext(request.Context()).IsRecording() {
			t.Error("server span must be recording")
		}
		writer.WriteHeader(http.StatusOK)
	})
	handler := telemetry.Middleware(mux)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/geo/vessels/123456789", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status must pass through unchanged, got %d", response.Code)
	}
	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("exactly one server span must be recorded, got %d", len(spans))
	}
	if spans[0].Name() != "GET /v1/geo/vessels/{mmsi}" {
		t.Fatalf("span must be named by route pattern, got %q", spans[0].Name())
	}
}

// TestMiddlewareTenantBaggageAttributes asserts edge-injected tenant.id and
// agency baggage become span attributes on a server handler.
func TestMiddlewareTenantBaggageAttributes(t *testing.T) {
	telemetry, recorder := testTelemetry(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/geo/zones", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	handler := telemetry.Middleware(mux)
	request := httptest.NewRequest(http.MethodGet, "/v1/geo/zones", nil)
	request.Header.Set("baggage", "tenant.id=tenant-npa,agency=NPA")
	handler.ServeHTTP(httptest.NewRecorder(), request)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("one span expected, got %d", len(spans))
	}
	assertAttribute(t, spans[0].Attributes(), "tenant.id", "tenant-npa")
	assertAttribute(t, spans[0].Attributes(), "agency", "NPA")
}

func assertAttribute(t *testing.T, attributes []attribute.KeyValue, key, expected string) {
	t.Helper()
	for _, item := range attributes {
		if string(item.Key) == key && item.Value.AsString() == expected {
			return
		}
	}
	t.Fatalf("attribute %s=%q missing from %v", key, expected, attributes)
}
