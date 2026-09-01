-- 0016_safety_modules: FSC/PSC inspection, SAR coordination and marine
-- accident investigation (Phase-12 safety-compliance modules).
--
-- Tenancy/RLS doctrine matches 0014_geofence_v2_rls: tenant-scoped tables
-- carry tenant_id NOT NULL with ENABLE+FORCE ROW LEVEL SECURITY and a policy
-- comparing tenant_id to the bound app.tenant_id GUC (unbound session =
-- default deny); child tables are scoped transitively through their parent
-- with an EXISTS subquery evaluated under the parent's tenant policy.
--
-- State machines are enforced by the application under SELECT ... FOR
-- UPDATE (same pattern as 0006_sos_lifecycle); the ledger columns below make
-- every transition attributable and keep state/ledger coherence fail-closed
-- at the database layer.

-- ─── FSC/PSC inspection ─────────────────────────────────────────────────────

-- Checklist templates: the versioned FSC/PSC inspection regimes an
-- inspection is conducted against. `items` is a JSONB array of
-- {code, description, severity} entries; codes on recorded deficiencies must
-- reference the template items (application-enforced).
CREATE TABLE safety_checklist_templates (
    template_id TEXT PRIMARY KEY CHECK (template_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    regime TEXT NOT NULL CHECK (regime IN ('FSC', 'PSC')),
    version INT NOT NULL CHECK (version >= 1),
    title TEXT NOT NULL CHECK (length(title) BETWEEN 1 AND 200),
    items JSONB NOT NULL CHECK (jsonb_typeof(items) = 'array'),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, regime, version)
);

-- Inspections. State machine:
--   SCHEDULED -> IN_PROGRESS -> COMPLETED
--   IN_PROGRESS|COMPLETED -> DETAINED (detain, maker)
--   DETAINED -> RECTIFICATION (rectification started)
--   RECTIFICATION -> RELEASED (release, checker -- four-eyes: releaser must
--     differ from the detaining principal, mirroring the geofence_zones
--     maker-checker doctrine)
--   COMPLETED|RELEASED -> CLOSED (terminal)
CREATE TABLE safety_inspections (
    inspection_id TEXT PRIMARY KEY CHECK (inspection_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    regime TEXT NOT NULL CHECK (regime IN ('FSC', 'PSC')),
    template_id TEXT NOT NULL,
    vessel_reference TEXT NOT NULL CHECK (length(vessel_reference) BETWEEN 1 AND 128),
    port_code TEXT NOT NULL CHECK (port_code ~ '^[A-Z0-9]{2,16}$'),
    classification TEXT NOT NULL CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    state TEXT NOT NULL DEFAULT 'SCHEDULED' CHECK (state IN ('SCHEDULED', 'IN_PROGRESS', 'COMPLETED', 'DETAINED', 'RECTIFICATION', 'RELEASED', 'CLOSED')),
    inspector_principal_id TEXT NOT NULL CHECK (length(inspector_principal_id) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    detained_by TEXT CHECK (detained_by IS NULL OR length(detained_by) BETWEEN 1 AND 512),
    detained_at TIMESTAMPTZ,
    detention_grounds TEXT CHECK (detention_grounds IS NULL OR length(detention_grounds) BETWEEN 1 AND 1000),
    rectification_started_by TEXT CHECK (rectification_started_by IS NULL OR length(rectification_started_by) BETWEEN 1 AND 512),
    rectification_started_at TIMESTAMPTZ,
    released_by TEXT CHECK (released_by IS NULL OR length(released_by) BETWEEN 1 AND 512),
    released_at TIMESTAMPTZ,
    closed_by TEXT CHECK (closed_by IS NULL OR length(closed_by) BETWEEN 1 AND 512),
    closed_at TIMESTAMPTZ,
    -- Detention maker-checker: the releasing checker is never the detaining
    -- maker. State/ledger coherence mirrors sos_alerts_lifecycle_coherence.
    CONSTRAINT safety_inspections_release_checker CHECK (released_by IS NULL OR released_by <> detained_by),
    CONSTRAINT safety_inspections_lifecycle_coherence CHECK (
        (detained_by IS NULL) = (detained_at IS NULL)
        AND (released_by IS NULL) = (released_at IS NULL)
        AND (rectification_started_by IS NULL) = (rectification_started_at IS NULL)
        AND (closed_by IS NULL) = (closed_at IS NULL)
        AND (detained_by IS NULL OR state IN ('DETAINED', 'RECTIFICATION', 'RELEASED', 'CLOSED'))
        AND (rectification_started_by IS NULL OR state IN ('RECTIFICATION', 'RELEASED', 'CLOSED'))
        AND (released_by IS NULL OR state IN ('RELEASED', 'CLOSED'))
        AND (state <> 'RELEASED' OR released_by IS NOT NULL)
        AND ((state = 'CLOSED') = (closed_by IS NOT NULL))
    )
);
ALTER TABLE safety_inspections
    ADD CONSTRAINT safety_inspections_template_fk
    FOREIGN KEY (template_id) REFERENCES safety_checklist_templates (template_id);

-- Deficiencies recorded against an inspection. Rectification deadline is
-- mandatory for MAJOR/CRITICAL severity (PSC detainable ground doctrine).
CREATE TABLE safety_inspection_deficiencies (
    deficiency_id TEXT PRIMARY KEY CHECK (deficiency_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    inspection_id TEXT NOT NULL REFERENCES safety_inspections (inspection_id),
    code TEXT NOT NULL CHECK (code ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'),
    description TEXT NOT NULL CHECK (length(description) BETWEEN 1 AND 1000),
    severity TEXT NOT NULL CHECK (severity IN ('MINOR', 'MAJOR', 'CRITICAL')),
    state TEXT NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN', 'RECTIFIED', 'VERIFIED')),
    rectification_deadline TIMESTAMPTZ CHECK (
        rectification_deadline IS NOT NULL OR severity = 'MINOR'),
    recorded_by TEXT NOT NULL CHECK (length(recorded_by) BETWEEN 1 AND 512),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    rectified_by TEXT CHECK (rectified_by IS NULL OR length(rectified_by) BETWEEN 1 AND 512),
    rectified_at TIMESTAMPTZ,
    verified_by TEXT CHECK (verified_by IS NULL OR length(verified_by) BETWEEN 1 AND 512),
    verified_at TIMESTAMPTZ,
    CONSTRAINT safety_deficiency_lifecycle_coherence CHECK (
        (rectified_by IS NULL) = (rectified_at IS NULL)
        AND (verified_by IS NULL) = (verified_at IS NULL)
        AND (rectified_by IS NULL OR state IN ('RECTIFIED', 'VERIFIED'))
        AND ((state = 'VERIFIED') = (verified_by IS NOT NULL))
    )
);

-- ─── SAR coordination ───────────────────────────────────────────────────────

-- SAR incidents. Phase machine (IMO IAMSAR):
--   UNCERTAINTY -> ALERT -> DISTRESS -> RESCUE -> CLOSED (terminal)
-- Direct closure from any phase is legal (false alarm / stood down); the
-- application enforces the ladder and the monotonic phase ledger.
CREATE TABLE sar_incidents (
    incident_id TEXT PRIMARY KEY CHECK (incident_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    sos_alert_id TEXT REFERENCES sos_alerts (sos_alert_id),
    title TEXT NOT NULL CHECK (length(title) BETWEEN 1 AND 200),
    position geography(POINT, 4326),
    classification TEXT NOT NULL CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    phase TEXT NOT NULL DEFAULT 'UNCERTAINTY' CHECK (phase IN ('UNCERTAINTY', 'ALERT', 'DISTRESS', 'RESCUE', 'CLOSED')),
    opened_by TEXT NOT NULL CHECK (length(opened_by) BETWEEN 1 AND 512),
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    alerted_by TEXT CHECK (alerted_by IS NULL OR length(alerted_by) BETWEEN 1 AND 512),
    alerted_at TIMESTAMPTZ,
    distress_declared_by TEXT CHECK (distress_declared_by IS NULL OR length(distress_declared_by) BETWEEN 1 AND 512),
    distress_declared_at TIMESTAMPTZ,
    rescue_started_by TEXT CHECK (rescue_started_by IS NULL OR length(rescue_started_by) BETWEEN 1 AND 512),
    rescue_started_at TIMESTAMPTZ,
    closed_by TEXT CHECK (closed_by IS NULL OR length(closed_by) BETWEEN 1 AND 512),
    closed_at TIMESTAMPTZ,
    closure_reason TEXT CHECK (closure_reason IS NULL OR length(closure_reason) BETWEEN 1 AND 500),
    CONSTRAINT sar_incidents_phase_coherence CHECK (
        (alerted_by IS NULL) = (alerted_at IS NULL)
        AND (distress_declared_by IS NULL) = (distress_declared_at IS NULL)
        AND (rescue_started_by IS NULL) = (rescue_started_at IS NULL)
        AND (closed_by IS NULL) = (closed_at IS NULL)
        AND (alerted_by IS NULL OR phase IN ('ALERT', 'DISTRESS', 'RESCUE', 'CLOSED'))
        AND (distress_declared_by IS NULL OR phase IN ('DISTRESS', 'RESCUE', 'CLOSED'))
        AND (rescue_started_by IS NULL OR phase IN ('RESCUE', 'CLOSED'))
        AND ((phase = 'CLOSED') = (closed_by IS NOT NULL))
    )
);

-- Resource tasking entries: units assigned to an incident.
CREATE TABLE sar_resource_taskings (
    tasking_id TEXT PRIMARY KEY CHECK (tasking_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    incident_id TEXT NOT NULL REFERENCES sar_incidents (incident_id),
    resource_type TEXT NOT NULL CHECK (resource_type IN ('VESSEL', 'AIRCRAFT', 'TEAM', 'OTHER')),
    resource_name TEXT NOT NULL CHECK (length(resource_name) BETWEEN 1 AND 200),
    state TEXT NOT NULL DEFAULT 'TASKED' CHECK (state IN ('TASKED', 'EN_ROUTE', 'ON_SCENE', 'RELEASED')),
    tasked_by TEXT NOT NULL CHECK (length(tasked_by) BETWEEN 1 AND 512),
    tasked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_by TEXT CHECK (released_by IS NULL OR length(released_by) BETWEEN 1 AND 512),
    released_at TIMESTAMPTZ,
    CONSTRAINT sar_tasking_coherence CHECK (
        (released_by IS NULL) = (released_at IS NULL)
        AND ((state = 'RELEASED') = (released_by IS NOT NULL))
    )
);

-- Append-only communications log. No UPDATE/DELETE is granted to the app
-- role (ledger doctrine).
CREATE TABLE sar_comms_log (
    entry_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    incident_id TEXT NOT NULL REFERENCES sar_incidents (incident_id),
    direction TEXT NOT NULL CHECK (direction IN ('INBOUND', 'OUTBOUND')),
    channel TEXT NOT NULL CHECK (length(channel) BETWEEN 1 AND 64),
    message TEXT NOT NULL CHECK (length(message) BETWEEN 1 AND 2000),
    logged_by TEXT NOT NULL CHECK (length(logged_by) BETWEEN 1 AND 512),
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ─── Marine accident investigation ──────────────────────────────────────────

-- Investigation cases. State machine:
--   OPEN -> EVIDENCE -> ANALYSIS -> REPORTED -> CLOSED (terminal)
CREATE TABLE investigation_cases (
    case_id TEXT PRIMARY KEY CHECK (case_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    tenant_id TEXT NOT NULL CHECK (tenant_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    casualty_type TEXT NOT NULL CHECK (casualty_type IN ('COLLISION', 'GROUNDING', 'FIRE', 'FLOODING', 'CAPSIZE', 'POLLUTION', 'FATALITY', 'OTHER')),
    severity TEXT NOT NULL CHECK (severity IN ('MINOR', 'SERIOUS', 'VERY_SERIOUS')),
    vessel_reference TEXT NOT NULL CHECK (length(vessel_reference) BETWEEN 1 AND 128),
    occurred_at TIMESTAMPTZ NOT NULL,
    location geography(POINT, 4326),
    classification TEXT NOT NULL CHECK (classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL', 'SECRET')),
    state TEXT NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN', 'EVIDENCE', 'ANALYSIS', 'REPORTED', 'CLOSED')),
    lead_investigator TEXT NOT NULL CHECK (length(lead_investigator) BETWEEN 1 AND 512),
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reported_by TEXT CHECK (reported_by IS NULL OR length(reported_by) BETWEEN 1 AND 512),
    reported_at TIMESTAMPTZ,
    closed_by TEXT CHECK (closed_by IS NULL OR length(closed_by) BETWEEN 1 AND 512),
    closed_at TIMESTAMPTZ,
    CONSTRAINT investigation_cases_state_coherence CHECK (
        (reported_by IS NULL) = (reported_at IS NULL)
        AND (closed_by IS NULL) = (closed_at IS NULL)
        AND (reported_by IS NULL OR state IN ('REPORTED', 'CLOSED'))
        AND ((state = 'CLOSED') = (closed_by IS NOT NULL))
    )
);

-- Evidence items with hash-chain integrity: every item commits to the chain
-- head of its case (chain_hash = sha256(prev_chain_hash || content_hash)),
-- genesis prev_chain_hash is 64 zero hex digits. Tampering with any earlier
-- item invalidates every later chain head; the application appends under
-- SELECT ... FOR UPDATE on the case row so heads are serialized.
CREATE TABLE investigation_evidence (
    evidence_id TEXT PRIMARY KEY CHECK (evidence_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    case_id TEXT NOT NULL REFERENCES investigation_cases (case_id),
    description TEXT NOT NULL CHECK (length(description) BETWEEN 1 AND 1000),
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    prev_chain_hash TEXT NOT NULL CHECK (prev_chain_hash ~ '^[0-9a-f]{64}$'),
    chain_hash TEXT NOT NULL CHECK (chain_hash ~ '^[0-9a-f]{64}$'),
    collected_by TEXT NOT NULL CHECK (length(collected_by) BETWEEN 1 AND 512),
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (case_id, chain_hash)
);

CREATE TABLE investigation_findings (
    finding_id TEXT PRIMARY KEY CHECK (finding_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    case_id TEXT NOT NULL REFERENCES investigation_cases (case_id),
    finding TEXT NOT NULL CHECK (length(finding) BETWEEN 1 AND 2000),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE investigation_recommendations (
    recommendation_id TEXT PRIMARY KEY CHECK (recommendation_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    case_id TEXT NOT NULL REFERENCES investigation_cases (case_id),
    recommendation TEXT NOT NULL CHECK (length(recommendation) BETWEEN 1 AND 2000),
    status TEXT NOT NULL DEFAULT 'PROPOSED' CHECK (status IN ('PROPOSED', 'ACCEPTED', 'REJECTED', 'IMPLEMENTED')),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_by TEXT CHECK (decided_by IS NULL OR length(decided_by) BETWEEN 1 AND 512),
    decided_at TIMESTAMPTZ,
    CONSTRAINT investigation_recommendations_coherence CHECK (
        (decided_by IS NULL) = (decided_at IS NULL)
        AND (status = 'PROPOSED' OR decided_by IS NOT NULL)
    )
);

-- ─── Row-level security (0014 doctrine) ─────────────────────────────────────

ALTER TABLE safety_checklist_templates ENABLE ROW LEVEL SECURITY;
ALTER TABLE safety_checklist_templates FORCE ROW LEVEL SECURITY;
CREATE POLICY safety_checklist_templates_tenant_policy ON safety_checklist_templates
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE safety_inspections ENABLE ROW LEVEL SECURITY;
ALTER TABLE safety_inspections FORCE ROW LEVEL SECURITY;
CREATE POLICY safety_inspections_tenant_policy ON safety_inspections
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE safety_inspection_deficiencies ENABLE ROW LEVEL SECURITY;
ALTER TABLE safety_inspection_deficiencies FORCE ROW LEVEL SECURITY;
CREATE POLICY safety_deficiencies_tenant_policy ON safety_inspection_deficiencies
    USING (EXISTS (SELECT 1 FROM safety_inspections i
                   WHERE i.inspection_id = safety_inspection_deficiencies.inspection_id
                     AND i.tenant_id = current_setting('app.tenant_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM safety_inspections i
                        WHERE i.inspection_id = safety_inspection_deficiencies.inspection_id
                          AND i.tenant_id = current_setting('app.tenant_id', true)));

ALTER TABLE sar_incidents ENABLE ROW LEVEL SECURITY;
ALTER TABLE sar_incidents FORCE ROW LEVEL SECURITY;
CREATE POLICY sar_incidents_tenant_policy ON sar_incidents
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE sar_resource_taskings ENABLE ROW LEVEL SECURITY;
ALTER TABLE sar_resource_taskings FORCE ROW LEVEL SECURITY;
CREATE POLICY sar_taskings_tenant_policy ON sar_resource_taskings
    USING (EXISTS (SELECT 1 FROM sar_incidents s
                   WHERE s.incident_id = sar_resource_taskings.incident_id
                     AND s.tenant_id = current_setting('app.tenant_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM sar_incidents s
                        WHERE s.incident_id = sar_resource_taskings.incident_id
                          AND s.tenant_id = current_setting('app.tenant_id', true)));

ALTER TABLE sar_comms_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE sar_comms_log FORCE ROW LEVEL SECURITY;
CREATE POLICY sar_comms_tenant_policy ON sar_comms_log
    USING (EXISTS (SELECT 1 FROM sar_incidents s
                   WHERE s.incident_id = sar_comms_log.incident_id
                     AND s.tenant_id = current_setting('app.tenant_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM sar_incidents s
                        WHERE s.incident_id = sar_comms_log.incident_id
                          AND s.tenant_id = current_setting('app.tenant_id', true)));

ALTER TABLE investigation_cases ENABLE ROW LEVEL SECURITY;
ALTER TABLE investigation_cases FORCE ROW LEVEL SECURITY;
CREATE POLICY investigation_cases_tenant_policy ON investigation_cases
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

ALTER TABLE investigation_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE investigation_evidence FORCE ROW LEVEL SECURITY;
CREATE POLICY investigation_evidence_tenant_policy ON investigation_evidence
    USING (EXISTS (SELECT 1 FROM investigation_cases c
                   WHERE c.case_id = investigation_evidence.case_id
                     AND c.tenant_id = current_setting('app.tenant_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM investigation_cases c
                        WHERE c.case_id = investigation_evidence.case_id
                          AND c.tenant_id = current_setting('app.tenant_id', true)));

ALTER TABLE investigation_findings ENABLE ROW LEVEL SECURITY;
ALTER TABLE investigation_findings FORCE ROW LEVEL SECURITY;
CREATE POLICY investigation_findings_tenant_policy ON investigation_findings
    USING (EXISTS (SELECT 1 FROM investigation_cases c
                   WHERE c.case_id = investigation_findings.case_id
                     AND c.tenant_id = current_setting('app.tenant_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM investigation_cases c
                        WHERE c.case_id = investigation_findings.case_id
                          AND c.tenant_id = current_setting('app.tenant_id', true)));

ALTER TABLE investigation_recommendations ENABLE ROW LEVEL SECURITY;
ALTER TABLE investigation_recommendations FORCE ROW LEVEL SECURITY;
CREATE POLICY investigation_recommendations_tenant_policy ON investigation_recommendations
    USING (EXISTS (SELECT 1 FROM investigation_cases c
                   WHERE c.case_id = investigation_recommendations.case_id
                     AND c.tenant_id = current_setting('app.tenant_id', true)))
    WITH CHECK (EXISTS (SELECT 1 FROM investigation_cases c
                        WHERE c.case_id = investigation_recommendations.case_id
                          AND c.tenant_id = current_setting('app.tenant_id', true)));

-- Grants: the application role `geo` receives DML on the workflow tables but
-- no UPDATE/DELETE on the append-only SAR comms ledger.
GRANT SELECT, INSERT, UPDATE ON safety_checklist_templates TO geo;
GRANT SELECT, INSERT, UPDATE ON safety_inspections TO geo;
GRANT SELECT, INSERT, UPDATE ON safety_inspection_deficiencies TO geo;
GRANT SELECT, INSERT, UPDATE ON sar_incidents TO geo;
GRANT SELECT, INSERT, UPDATE ON sar_resource_taskings TO geo;
GRANT SELECT, INSERT ON sar_comms_log TO geo;
GRANT SELECT, INSERT, UPDATE ON investigation_cases TO geo;
GRANT SELECT, INSERT ON investigation_evidence TO geo;
GRANT SELECT, INSERT ON investigation_findings TO geo;
GRANT SELECT, INSERT, UPDATE ON investigation_recommendations TO geo;
