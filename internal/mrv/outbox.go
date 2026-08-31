// Transactional outbox publisher: drains mrv_outbox to the contract Kafka
// topics at-least-once, with the outbox event id as the idempotent key
// (the envelope eventId). Payloads are the fully signed envelope v1.0
// documents produced at intake; the publisher never re-signs. Publish
// failures are counted (mrv_outbox_publish_total{result=error}) and retried
// on the next drain — no silent drops.
package mrv

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// KafkaPublisher is the bounded-retry producer boundary (bus.Producer in
// production; a recorder in tests).
type KafkaPublisher interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
}

// OutboxPublisher drains the mrv transactional outbox.
type OutboxPublisher struct {
	service   *Service
	publisher KafkaPublisher
	interval  time.Duration
	batchSize int
	tracer    trace.Tracer
}

// NewOutboxPublisher wires the publisher, failing closed on degenerate
// configuration.
func NewOutboxPublisher(service *Service, publisher KafkaPublisher, interval time.Duration, batchSize int) (*OutboxPublisher, error) {
	if service == nil || publisher == nil {
		return nil, errors.New("mrv outbox publisher requires the service and a Kafka publisher")
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if batchSize <= 0 || batchSize > 500 {
		batchSize = 100
	}
	return &OutboxPublisher{
		service: service, publisher: publisher, interval: interval, batchSize: batchSize,
		tracer: otel.Tracer("github.com/munisp/blueeconomy-geo-service/internal/mrv"),
	}, nil
}

// Run drains until ctx is cancelled.
func (publisher *OutboxPublisher) Run(ctx context.Context) {
	ticker := time.NewTicker(publisher.interval)
	defer ticker.Stop()
	for {
		_ = publisher.Drain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Drain publishes one batch of unpublished outbox rows.
func (publisher *OutboxPublisher) Drain(ctx context.Context) error {
	ctx, span := publisher.tracer.Start(ctx, "mrv.outbox.drain")
	defer span.End()
	return publisher.service.withActor(ctx, publisher.service.Principal.PrincipalID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT event_id, aggregate_id, event_type, payload
			FROM mrv_outbox WHERE published_at IS NULL
			ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`, publisher.batchSize)
		if err != nil {
			return fmt.Errorf("poll mrv outbox: %w", err)
		}
		type pending struct {
			eventID, aggregateID, eventType string
			payload                         []byte
		}
		batch := []pending{}
		for rows.Next() {
			var row pending
			if err := rows.Scan(&row.eventID, &row.aggregateID, &row.eventType, &row.payload); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, row)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		span.SetAttributes(attribute.Int("mrv.outbox.pending", len(batch)))
		for _, row := range batch {
			contract, ok := eventContracts[row.eventType]
			if !ok {
				return fmt.Errorf("outbox event %s has unknown type %q (fail closed)", row.eventID, row.eventType)
			}
			err := publisher.publisher.Publish(ctx, contract.topic, row.aggregateID, row.payload, map[string]string{
				"content-type": "application/json",
				"eventType":    row.eventType,
				"eventId":      row.eventID,
			})
			if err != nil {
				publisher.service.Metrics.Inc("mrv_outbox_publish_total", map[string]string{"result": "error"})
				return fmt.Errorf("publish outbox event %s: %w", row.eventID, err)
			}
			if _, err := tx.Exec(ctx, `UPDATE mrv_outbox SET published_at = now() WHERE event_id = $1`, row.eventID); err != nil {
				return err
			}
			publisher.service.Metrics.Inc("mrv_outbox_publish_total", map[string]string{"result": "ok"})
		}
		return nil
	})
}
