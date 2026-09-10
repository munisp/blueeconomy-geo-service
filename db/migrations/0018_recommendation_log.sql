-- 0018_recommendation_log: shadow-mode ML policy recommendation log
-- (Phase 18, /v1/geo/berths/recommendation + /v1/geo/routes/advice).
--
-- This table is the future REWARD SIGNAL for offline RL / contextual
-- bandits on berth allocation and route advice. Every served recommendation
-- is logged BEFORE the response is returned (the API fails closed: a
-- recommendation that cannot be logged is never served), so the training
-- corpus is exactly the set of suggestions shown to operators.
--
-- JOIN CONTRACT (reward pipeline):
--   * request          jsonb — the validated request payload = the feature
--                      vector (port_code, vessels, berths / origin,
--                      destination, ...). Immutable.
--   * suggestion       jsonb — the policy action that was suggested
--                      (verbatim ml-stack output, mode=shadow). Immutable.
--   * policy_version        — the promoted policy that produced the action;
--                      OPE joins on (policy_version, request/suggestion).
--   * request_hash          — sha256 hex of the canonical request JSON;
--                      join key for idempotent re-ingestion / dedup.
--   * outcome/outcome_at    — filled LATER by the reward pipeline (e.g.
--                      realized turnaround time from port-interop berth
--                      occupancy for berth_allocation; realized vs
--                      predicted delay for route_advice), keyed on
--                      recommendation_id. NULL until realized; training
--                      jobs must SELECT WHERE outcome IS NOT NULL.
--
-- Shadow doctrine: nothing here is ever auto-applied to the berth-slots
-- system of record; operator acceptance/override is itself logged
-- downstream and joins on recommendation_id.
--
-- RLS: the position plane is a shared national picture (no tenant column,
-- same doctrine as ais_positions / port_queue_observations), so the table
-- carries no tenant_id; the application role `geo` holds row-agnostic
-- CRUD through the permissive policy below, and geo_ingest has no access.

CREATE TABLE recommendation_log (
    recommendation_id UUID PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('berth_allocation', 'route_advice')),
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    request JSONB NOT NULL,
    policy_version TEXT NOT NULL CHECK (length(policy_version) BETWEEN 1 AND 128),
    mode TEXT NOT NULL CHECK (mode IN ('shadow')),
    suggestion JSONB NOT NULL,
    requested_by TEXT NOT NULL CHECK (length(requested_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Reward-join columns: populated by the offline reward pipeline only.
    outcome JSONB,
    outcome_at TIMESTAMPTZ,
    CHECK ((outcome IS NULL) = (outcome_at IS NULL))
);

CREATE INDEX recommendation_log_kind_created ON recommendation_log (kind, created_at DESC);
CREATE INDEX recommendation_log_request_hash ON recommendation_log (request_hash);
-- Reward-pipeline scan: recommendations with outcomes still pending.
CREATE INDEX recommendation_log_pending_outcome ON recommendation_log (created_at)
    WHERE outcome IS NULL;

ALTER TABLE recommendation_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE recommendation_log FORCE ROW LEVEL SECURITY;

CREATE POLICY recommendation_log_app ON recommendation_log
    FOR ALL TO geo
    USING (true)
    WITH CHECK (true);

GRANT SELECT, INSERT ON recommendation_log TO geo;
-- The reward pipeline updates outcome/outcome_at through its own role
-- (least privilege); the geo application role deliberately receives no
-- UPDATE/DELETE — served recommendations are immutable.
