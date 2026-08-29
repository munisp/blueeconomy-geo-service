-- 0006_sos_lifecycle: acknowledgement/resolution ledger for SOS alerts.
-- 0003_events.sql anticipated the RAISED/ACKNOWLEDGED/RESOLVED state machine
-- (state CHECK, live-alerts partial index, GRANT UPDATE) but no writer
-- existed, so every historical alert presented as live. This migration adds
-- the actor/timestamp/note ledger columns and the state/ledger coherence
-- invariants. Transitions are enforced by the application under SELECT ...
-- FOR UPDATE (RAISED -> ACKNOWLEDGED -> RESOLVED, RAISED -> RESOLVED direct);
-- the ledger columns make every transition attributable to the acting
-- principal from the verified token claims.

ALTER TABLE sos_alerts
    ADD COLUMN acknowledged_by TEXT CHECK (acknowledged_by IS NULL OR length(acknowledged_by) BETWEEN 1 AND 512),
    ADD COLUMN acknowledged_at TIMESTAMPTZ,
    ADD COLUMN acknowledge_note TEXT CHECK (acknowledge_note IS NULL OR length(acknowledge_note) <= 500),
    ADD COLUMN resolved_by TEXT CHECK (resolved_by IS NULL OR length(resolved_by) BETWEEN 1 AND 512),
    ADD COLUMN resolved_at TIMESTAMPTZ,
    ADD COLUMN resolve_note TEXT CHECK (resolve_note IS NULL OR length(resolve_note) <= 500);

-- State/ledger coherence: an actor stamp is always paired with its
-- timestamp; ACKNOWLEDGED/RESOLVED rows carry the matching ledger entry; a
-- RESOLVED row may skip ACKNOWLEDGED (direct RAISED -> RESOLVED) but never
-- the reverse.
ALTER TABLE sos_alerts ADD CONSTRAINT sos_alerts_lifecycle_coherence CHECK (
    (acknowledged_by IS NULL) = (acknowledged_at IS NULL)
    AND (resolved_by IS NULL) = (resolved_at IS NULL)
    AND (acknowledged_by IS NULL OR state IN ('ACKNOWLEDGED', 'RESOLVED'))
    AND ((state = 'RESOLVED') = (resolved_by IS NOT NULL))
);
