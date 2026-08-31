// The MRV service: domain operations over PostgreSQL with the transactional
// outbox. Every mutating operation persists the domain row and the fully
// signed envelope v1.0 outbox row in ONE transaction; the outbox publisher
// (publisher.go) drains to Kafka at-least-once. All access binds the acting
// principal via app.mrv_actor (RLS default-deny, migration 0012).
package mrv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-geo-service/internal/metrics"
	"github.com/munisp/blueeconomy-geo-service/internal/sign"
)

// Ship is one in-scope vessel (links to vessels_static by MMSI).
type Ship struct {
	ImoNumber            string    `json:"imoNumber"`
	MMSI                 string    `json:"mmsi,omitempty"`
	ShipName             string    `json:"shipName"`
	GT                   uint32    `json:"gt"`
	DWT                  *uint32   `json:"dwt,omitempty"`
	ShipType             string    `json:"shipType"`
	FlagState            string    `json:"flagState"`
	InternationalVoyages bool      `json:"internationalVoyages"`
	DcsScope             bool      `json:"dcsScope"`
	RegisteredBy         string    `json:"registeredBy"`
	CreatedAt            time.Time `json:"createdAt"`
}

// MonitoringPlan is one versioned SEEMP Part II analog.
type MonitoringPlan struct {
	PlanID      string          `json:"planId"`
	ImoNumber   string          `json:"imoNumber"`
	Version     int             `json:"version"`
	Methods     json.RawMessage `json:"methods"`
	FuelGrades  []string        `json:"fuelGrades"`
	State       string          `json:"state"`
	CreatedBy   string          `json:"createdBy"`
	CreatedAt   time.Time       `json:"createdAt"`
	ConfirmedBy string          `json:"confirmedBy,omitempty"`
	ConfirmedAt *time.Time      `json:"confirmedAt,omitempty"`
}

// FuelReport is one operator-reported DCS record unit.
type FuelReport struct {
	ReportID             string          `json:"reportId"`
	ImoNumber            string          `json:"imoNumber"`
	ExternalRef          string          `json:"externalReference"`
	PeriodFrom           time.Time       `json:"periodFrom"`
	PeriodTo             time.Time       `json:"periodTo"`
	Consumer             string          `json:"consumer"`
	FuelGrade            string          `json:"fuelGrade"`
	Method               string          `json:"method"`
	FuelTonnesMilli      uint64          `json:"fuelTonnesMilli"`
	DistanceNmMilli      *uint64         `json:"distanceNmMilli,omitempty"`
	HoursUnderwayMinutes *uint64         `json:"hoursUnderwayMinutes,omitempty"`
	BdnRef               string          `json:"bdnReference,omitempty"`
	Evidence             json.RawMessage `json:"-"`
	EvidenceDigestSha256 string          `json:"evidenceDigestSha256"`
	ReportedBy           string          `json:"reportedBy"`
	CreatedAt            time.Time       `json:"createdAt"`
}

// Voyage is one BOSP/EOSP voyage-ledger entry.
type Voyage struct {
	VoyageID             string          `json:"voyageId"`
	ImoNumber            string          `json:"imoNumber"`
	BospAt               *time.Time      `json:"bospAt,omitempty"`
	BospPort             string          `json:"bospPortCode,omitempty"`
	EospAt               *time.Time      `json:"eospAt,omitempty"`
	EospPort             string          `json:"eospPortCode,omitempty"`
	CargoTonnesMilli     *uint64         `json:"cargoTonnesMilli,omitempty"`
	LadenDistanceNmMilli *uint64         `json:"ladenDistanceNmMilli,omitempty"`
	Source               string          `json:"source"`
	GeofenceEvidence     json.RawMessage `json:"geofenceEvidence"`
	RecordedBy           string          `json:"recordedBy"`
	CreatedAt            time.Time       `json:"createdAt"`
}

// AnnualReport is the DCS annual report aggregate.
type AnnualReport struct {
	ReportID        string          `json:"reportId"`
	ImoNumber       string          `json:"imoNumber"`
	CalendarYear    int             `json:"calendarYear"`
	Totals          json.RawMessage `json:"totals"`
	AttainedCiiNano *uint64         `json:"attainedCiiNano,omitempty"`
	RequiredCiiNano *uint64         `json:"requiredCiiNano,omitempty"`
	CiiRating       string          `json:"ciiRating,omitempty"`
	FactorSetHash   string          `json:"factorSetDigestSha256"`
	State           string          `json:"state"`
	CompiledBy      string          `json:"compiledBy"`
	SubmittedBy     string          `json:"submittedBy,omitempty"`
	CreatedAt       time.Time       `json:"createdAt"`
	SubmittedAt     *time.Time      `json:"submittedAt,omitempty"`
}

// Verification is one immutable verifier decision.
type Verification struct {
	VerificationID string          `json:"verificationId"`
	ReportID       string          `json:"annualReportId"`
	Decision       string          `json:"decision"`
	Verifier       string          `json:"verifierPrincipal"`
	Reason         string          `json:"reason"`
	AISCrosscheck  json.RawMessage `json:"aisCrosscheck,omitempty"`
	DecidedAt      time.Time       `json:"decidedAt"`
}

// Service bundles the MRV boundary dependencies.
type Service struct {
	Pool                        *pgxpool.Pool
	Signer                      *sign.Signer
	Principal                   sign.Provenance
	Metrics                     *metrics.Registry
	CII                         *CIIConfig
	Deadlines                   Deadlines
	ActivityParams              ActivityParams
	CrosscheckTolerancePermille uint32
	DcsGTThreshold              uint32
}

// NewService validates the wiring fail-closed.
func NewService(pool *pgxpool.Pool, signer *sign.Signer, principal sign.Provenance, registry *metrics.Registry, cii *CIIConfig, deadlines Deadlines, activity ActivityParams, tolerancePermille, dcsGTThreshold uint32) (*Service, error) {
	if pool == nil {
		return nil, errors.New("mrv postgres pool is required")
	}
	if signer == nil {
		return nil, errors.New("mrv envelope signer is required")
	}
	if strings.TrimSpace(principal.PrincipalID) == "" || strings.TrimSpace(principal.PrincipalRole) == "" {
		return nil, errors.New("mrv producer principal is required")
	}
	if registry == nil {
		return nil, errors.New("mrv metrics registry is required")
	}
	if err := activity.Validate(); err != nil {
		return nil, err
	}
	if tolerancePermille == 0 || tolerancePermille > 1000 {
		return nil, errors.New("AIS cross-check tolerance must be in (0, 1000] permille")
	}
	if dcsGTThreshold == 0 {
		return nil, errors.New("DCS GT threshold is configuration and must be positive")
	}
	return &Service{
		Pool: pool, Signer: signer, Principal: principal, Metrics: registry,
		CII: cii, Deadlines: deadlines, ActivityParams: activity,
		CrosscheckTolerancePermille: tolerancePermille, DcsGTThreshold: dcsGTThreshold,
	}, nil
}

// withActor runs fn inside a transaction with app.mrv_actor bound (RLS
// default-deny): unbound sessions read and write nothing.
func (service *Service) withActor(ctx context.Context, actor string, fn func(tx pgx.Tx) error) error {
	if strings.TrimSpace(actor) == "" {
		return errors.New("mrv actor principal is required for mrv access")
	}
	tx, err := service.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin mrv transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('app.mrv_actor', $1, true)`, actor); err != nil {
		return fmt.Errorf("bind mrv actor: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// enqueueOutbox builds+signs the envelope and inserts the outbox row inside
// the open transaction. The outbox event id IS the envelope eventId, so a
// drained replay is byte-identical and consumers dedup on it.
func (service *Service) enqueueOutbox(ctx context.Context, tx pgx.Tx, eventType, aggregateID, correlationID string, resource any, occurredAt time.Time, ledgerCommitHash string) (string, error) {
	eventID := uuid.NewString()
	envelope, err := BuildSignedEnvelope(eventType, eventID, correlationID, resource, occurredAt, service.Principal, ledgerCommitHash, service.Signer)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO mrv_outbox (event_id, aggregate_id, event_type, payload)
		VALUES ($1, $2, $3, $4)`, eventID, aggregateID, eventType, envelope); err != nil {
		return "", fmt.Errorf("insert mrv outbox row: %w", err)
	}
	return eventID, nil
}

// digestEvidence renders the sha256 digest of the operator evidence bundle
// (canonical JSON; only the digest crosses the event boundary).
func digestEvidence(evidence json.RawMessage) (string, error) {
	if len(evidence) == 0 {
		evidence = json.RawMessage(`{}`)
	}
	if !json.Valid(evidence) {
		return "", errors.New("evidence must be a valid JSON object")
	}
	sum := sha256.Sum256(evidence)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
