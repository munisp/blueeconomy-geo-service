package store

import (
	"context"
	"fmt"
	"time"
)

// RecommendationLogRow is one shadow-mode policy recommendation record
// (Phase 18). The row is the future reward signal for offline RL: the
// request payload (features) and the policy suggestion are immutable; the
// outcome columns are filled later by the reward pipeline keyed on
// recommendation_id (see 0018_recommendation_log.sql for the join
// contract).
type RecommendationLogRow struct {
	RecommendationID string
	Kind             string // berth_allocation | route_advice
	RequestHash      string // sha256 hex of the canonical request JSON
	Request          []byte // validated request payload (training features)
	PolicyVersion    string
	Mode             string // shadow
	Suggestion       []byte // policy output, verbatim
	RequestedBy      string // authenticated principal subject
	CreatedAt        time.Time
}

// InsertRecommendationLog persists one recommendation. It writes through
// the application pool (recommendation_log_app policy, 0018); the API layer
// fails closed when the insert fails — a recommendation that cannot be
// logged (its reward trail) is never served.
func (store *Store) InsertRecommendationLog(ctx context.Context, row RecommendationLogRow) error {
	_, err := store.pool.Exec(ctx, `INSERT INTO recommendation_log
		(recommendation_id, kind, request_hash, request, policy_version, mode, suggestion, requested_by, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		row.RecommendationID, row.Kind, row.RequestHash, row.Request,
		row.PolicyVersion, row.Mode, row.Suggestion, row.RequestedBy, row.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert recommendation log: %w", err)
	}
	return nil
}
