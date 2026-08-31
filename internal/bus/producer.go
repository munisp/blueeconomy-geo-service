// Package bus is the Kafka boundary of the hot path. Topics:
//
//	ais.raw             raw validated-adjacent AIS frames, keyed by MMSI
//	vessels.events      signed envelopes (geo.*.v1), keyed by MMSI/vessel ref
//	vessels.quarantine  suspect reports (validation rejects, spoof indicators)
//
// The producer fails closed on broker errors: every write is retried with
// backoff up to a bounded attempt budget and the error is returned to the
// caller — no silent drops. Callers persist to Postgres before publishing
// where durability matters, so a broker outage never loses the record.
package bus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/munisp/blueeconomy-geo-service/internal/telemetry"
)

// tracer returns the bus tracer. With telemetry disabled (no OTLP endpoint)
// the global provider is a no-op: produce spans are non-recording and the
// fail-closed publish semantics are unchanged.
func tracer() trace.Tracer {
	return otel.Tracer("github.com/munisp/blueeconomy-geo-service/internal/bus")
}

const (
	// TopicAISRaw carries decoded+validated AIS frames keyed by MMSI.
	TopicAISRaw = "ais.raw"
	// TopicVesselEvents carries signed geo.*.v1 envelopes.
	TopicVesselEvents = "vessels.events"
	// TopicVesselQuarantine carries suspect reports, never dropped.
	TopicVesselQuarantine = "vessels.quarantine"
)

// Producer publishes messages with bounded retry/backoff.
type Producer struct {
	writer *kafka.Writer
	// attempts is the total write attempt budget per message (>= 1).
	attempts int
	// backoff is the base delay doubled between attempts.
	backoff time.Duration
	now     func() time.Time
	sleep   func(context.Context, time.Duration) error
}

// Config is the producer configuration.
type Config struct {
	Brokers []string
	// RequiredAcks: -1 (all ISR) by default — fail closed on under-replication.
	RequiredAcks kafka.RequiredAcks
	Attempts     int
	Backoff      time.Duration
}

// NewProducer builds a producer. It fails closed on an empty broker list or
// degenerate retry settings — a producer that cannot acknowledge writes must
// never start.
func NewProducer(config Config) (*Producer, error) {
	brokers := make([]string, 0, len(config.Brokers))
	for _, broker := range config.Brokers {
		if trimmed := strings.TrimSpace(broker); trimmed != "" {
			brokers = append(brokers, trimmed)
		}
	}
	if len(brokers) == 0 {
		return nil, errors.New("kafka broker list is empty")
	}
	attempts := config.Attempts
	if attempts <= 0 {
		attempts = 5
	}
	backoff := config.Backoff
	if backoff <= 0 {
		backoff = 200 * time.Millisecond
	}
	requiredAcks := config.RequiredAcks
	if requiredAcks == 0 {
		requiredAcks = kafka.RequireAll
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		RequiredAcks: requiredAcks,
		// Async=false: every Publish call awaits the broker acknowledgement;
		// errors propagate to the caller (fail closed, no silent drops).
		Async:        false,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 50 * time.Millisecond,
	}
	// Batching overrides are opt-in via the environment; unset values keep
	// the defaults above (unchanged behavior).
	//
	//	GEO_KAFKA_BATCH_SIZE        messages per batch      (default: 0 = kafka-go default)
	//	GEO_KAFKA_BATCH_TIMEOUT_MS  linger before flushing  (default: 50, current value)
	if raw := os.Getenv("GEO_KAFKA_BATCH_SIZE"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("GEO_KAFKA_BATCH_SIZE must be a positive integer, got %q", raw)
		}
		writer.BatchSize = value
	}
	if raw := os.Getenv("GEO_KAFKA_BATCH_TIMEOUT_MS"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("GEO_KAFKA_BATCH_TIMEOUT_MS must be a positive integer, got %q", raw)
		}
		writer.BatchTimeout = time.Duration(value) * time.Millisecond
	}
	return &Producer{writer: writer, attempts: attempts, backoff: backoff, now: time.Now, sleep: sleepContext}, nil
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Publish writes one message to topic with retries and exponential backoff.
// The final error is returned after the attempt budget is exhausted.
func (producer *Producer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	if strings.TrimSpace(topic) == "" {
		return errors.New("kafka topic is required")
	}
	// Produce span + manual carrier: the live W3C traceparent/baggage is
	// injected into the record headers so consumers join this trace
	// (kafka-go has no auto-instrumentation).
	ctx, span := tracer().Start(ctx, "kafka.produce "+topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.String("messaging.destination.name", topic)))
	defer span.End()
	message := kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   value,
		Time:    producer.now().UTC(),
		Headers: make([]kafka.Header, 0, len(headers)+2),
	}
	for name, headerValue := range headers {
		message.Headers = append(message.Headers, kafka.Header{Key: name, Value: []byte(headerValue)})
	}
	message.Headers = telemetry.InjectKafkaHeaders(ctx, message.Headers)
	delay := producer.backoff
	var err error
	for attempt := 1; attempt <= producer.attempts; attempt++ {
		err = producer.writer.WriteMessages(ctx, message)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("publish %s: context closed: %w", topic, ctx.Err())
		}
		if attempt < producer.attempts {
			if sleepErr := producer.sleep(ctx, delay); sleepErr != nil {
				return fmt.Errorf("publish %s: backoff interrupted: %w", topic, sleepErr)
			}
			delay *= 2
		}
	}
	span.RecordError(err)
	return fmt.Errorf("publish %s failed after %d attempts: %w", topic, producer.attempts, err)
}

// Close flushes and closes the underlying writer.
func (producer *Producer) Close() error {
	return producer.writer.Close()
}
