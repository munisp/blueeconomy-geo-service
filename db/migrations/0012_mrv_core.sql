-- 0012_mrv_core: Phase-8 MRV emissions module (IMO DCS compliance core,
-- EU-MRV-compatible voyage ledger, CII outcomes only from operator-approved
-- source-cited configuration). Tables follow spec_mrv_emissions.md §3.1.
--
-- Doctrine:
--   * Every emission factor row carries a source citation; there is no
--     default factor and no fallback (unknown grade => fail closed).
--   * Fuel is classified by ISO 8217 viscosity grade, never sulphur label.
--   * mrv_verifications and mrv_statements_of_compliance are immutable
--     ledgers; annual-report state transitions are trigger-guarded;
--     maker <> checker is enforced at the storage boundary (plan
--     confirmation and verification decisions).
--   * MRV records are flag-administration records (not tenant-scoped):
--     RLS default-deny requires every transaction to bind the acting
--     principal via app.mrv_actor (set_config), mirroring the
--     app.tenant_id discipline of 0004/0007. mrv_emission_factors is
--     public reference data (GET /v1/mrv/factors is any-role) and carries
--     no RLS; INSERT stays migrator-only (no grant to geo).
--   * Quantities are fixed-point integers (milli-tonnes x1e3,
--     milli-nautical-miles x1e3, whole minutes, nano x1e9). Floating-point
--     storage is prohibited.

-- ---------------------------------------------------------------------
-- Ships in scope (one row per ship; links to vessels_static by mmsi).
-- vessels_static is SCD-2 (no unique constraint on mmsi), so the link is
-- maintained by the application against the current row, not by FK.
CREATE TABLE mrv_ships (
    imo_number TEXT PRIMARY KEY CHECK (imo_number ~ '^[0-9]{7}$'),
    mmsi TEXT CHECK (mmsi IS NULL OR mmsi ~ '^[0-9]{9}$'),
    ship_name TEXT NOT NULL CHECK (length(ship_name) BETWEEN 1 AND 128),
    gt INTEGER NOT NULL CHECK (gt > 0),
    dwt INTEGER CHECK (dwt IS NULL OR dwt > 0),
    ship_type TEXT NOT NULL CHECK (ship_type ~ '^[A-Z][A-Z0-9_]{1,63}$'),
    flag_state TEXT NOT NULL DEFAULT 'NG' CHECK (flag_state ~ '^[A-Z]{2}$'),
    international_voyages BOOLEAN NOT NULL,
    -- dcs_scope = gt >= configured threshold AND international; computed
    -- by the application at registration (threshold is configuration).
    dcs_scope BOOLEAN NOT NULL,
    registered_by TEXT NOT NULL CHECK (length(registered_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Monitoring plans (SEEMP Part II analog; versioned, confirmed before
-- collection; maker <> checker on confirmation).
CREATE TABLE mrv_monitoring_plans (
    plan_id UUID PRIMARY KEY,
    imo_number TEXT NOT NULL REFERENCES mrv_ships(imo_number),
    version INTEGER NOT NULL CHECK (version > 0),
    methods JSONB NOT NULL,
    fuel_grades TEXT[] NOT NULL,
    state TEXT NOT NULL DEFAULT 'DRAFT' CHECK (state IN ('DRAFT','SUBMITTED','CONFIRMED','SUPERSEDED')),
    created_by TEXT NOT NULL CHECK (length(created_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    confirmed_by TEXT CHECK (confirmed_by IS NULL OR length(confirmed_by) BETWEEN 1 AND 512),
    confirmed_at TIMESTAMPTZ,
    UNIQUE (imo_number, version),
    -- Confirmation bookkeeping coherence (a SUPERSEDED plan was confirmed
    -- once, so both fields persist); maker <> checker (four-eyes).
    CHECK ((confirmed_by IS NULL) = (confirmed_at IS NULL)),
    CHECK ((confirmed_by IS NOT NULL) = (state IN ('CONFIRMED','SUPERSEDED'))),
    CHECK (confirmed_by IS NULL OR confirmed_by <> created_by)
);

-- Operator-reported fuel/activity records (the DCS record unit).
-- external_ref is the idempotency anchor (Idempotency-Key); replay with a
-- divergent payload conflicts at the application boundary.
CREATE TABLE mrv_fuel_reports (
    report_id UUID PRIMARY KEY,
    imo_number TEXT NOT NULL REFERENCES mrv_ships(imo_number),
    external_ref TEXT NOT NULL UNIQUE CHECK (length(external_ref) BETWEEN 1 AND 256),
    period_from TIMESTAMPTZ NOT NULL,
    period_to TIMESTAMPTZ NOT NULL,
    consumer TEXT NOT NULL CHECK (consumer IN ('MAIN_ENGINE','AUX_ENGINE','BOILER','INERT_GAS','NOT_UNDER_WAY')),
    fuel_grade TEXT NOT NULL CHECK (fuel_grade ~ '^[A-Z][A-Z0-9_-]{1,63}$'),
    method TEXT NOT NULL CHECK (method IN ('A','B','C','D')),
    fuel_tonnes_milli BIGINT NOT NULL CHECK (fuel_tonnes_milli >= 0),
    distance_nm_milli BIGINT CHECK (distance_nm_milli IS NULL OR distance_nm_milli >= 0),
    hours_underway_minutes BIGINT CHECK (hours_underway_minutes IS NULL OR hours_underway_minutes >= 0),
    bdn_ref TEXT,
    evidence JSONB NOT NULL DEFAULT '{}',
    evidence_digest_sha256 TEXT NOT NULL CHECK (evidence_digest_sha256 ~ '^sha256:[0-9a-f]{64}$'),
    reported_by TEXT NOT NULL CHECK (length(reported_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (period_to > period_from),
    -- BDN grade capture is mandatory for method A (EU MRV Annex I).
    CHECK (method <> 'A' OR (bdn_ref IS NOT NULL AND length(bdn_ref) BETWEEN 1 AND 256))
);

CREATE INDEX mrv_fuel_reports_ship_period_idx ON mrv_fuel_reports (imo_number, period_from, period_to);

-- BOSP/EOSP voyage ledger (EU-MRV-compatible).
CREATE TABLE mrv_voyages (
    voyage_id UUID PRIMARY KEY,
    imo_number TEXT NOT NULL REFERENCES mrv_ships(imo_number),
    bosp_at TIMESTAMPTZ,
    bosp_port TEXT CHECK (bosp_port IS NULL OR bosp_port ~ '^[A-Z0-9]{5}$'),
    eosp_at TIMESTAMPTZ,
    eosp_port TEXT CHECK (eosp_port IS NULL OR eosp_port ~ '^[A-Z0-9]{5}$'),
    cargo_tonnes_milli BIGINT CHECK (cargo_tonnes_milli IS NULL OR cargo_tonnes_milli >= 0),
    laden_distance_nm_milli BIGINT CHECK (laden_distance_nm_milli IS NULL OR laden_distance_nm_milli >= 0),
    source TEXT NOT NULL CHECK (source IN ('OPERATOR','AIS_DERIVED','RECONCILED')),
    geofence_evidence JSONB NOT NULL DEFAULT '[]',
    recorded_by TEXT NOT NULL CHECK (length(recorded_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (eosp_at IS NULL OR bosp_at IS NULL OR eosp_at > bosp_at)
);

CREATE INDEX mrv_voyages_ship_idx ON mrv_voyages (imo_number, bosp_at);

-- Emission factor registry: the ONLY permitted factor provenance. Every
-- row carries a source citation; inserts are migrator/admin-only (no
-- INSERT grant to the application role). Public reference data: no RLS.
CREATE TABLE mrv_emission_factors (
    factor_key TEXT NOT NULL CHECK (factor_key ~ '^[A-Z][A-Z0-9_-]{1,63}$'),
    gas TEXT NOT NULL CHECK (gas IN ('CO2','CH4','N2O')),
    factor_nano BIGINT NOT NULL CHECK (factor_nano >= 0),
    unit TEXT NOT NULL CHECK (length(unit) BETWEEN 1 AND 64),
    source_citation TEXT NOT NULL CHECK (length(source_citation) BETWEEN 8 AND 512),
    valid_from DATE NOT NULL,
    PRIMARY KEY (factor_key, gas, valid_from)
);

-- Seed: CO2 Cf factors transcribed from the Cf table of MEPC.245(66) as
-- amended by MEPC.308(73)/MEPC.364(79), reproduced by EU MRV Regulation
-- (EU) 2015/757 Annex I (source table and URLs: phase-8 spec §1.2). Fuel
-- classification follows the ISO 8217 viscosity grade. CH4/N2O and
-- well-to-wake factors are deliberately NOT seeded: they may only come
-- from MEPC.391(81) / IMO Fourth GHG Study rows transcribed with their
-- citations; until then those gases fail closed (no estimate).
INSERT INTO mrv_emission_factors (factor_key, gas, factor_nano, unit, source_citation, valid_from) VALUES
    ('MDO_MGO_DMX-DMB', 'CO2', 3206000000, 'tCO2/t_fuel', 'MEPC.245(66) Cf table as amended by MEPC.308(73)/MEPC.364(79); EU MRV Regulation (EU) 2015/757 Annex I', '2018-03-01'),
    ('LFO_RMA-RMD',     'CO2', 3151000000, 'tCO2/t_fuel', 'MEPC.245(66) Cf table as amended by MEPC.308(73)/MEPC.364(79); EU MRV Regulation (EU) 2015/757 Annex I', '2018-03-01'),
    ('HFO_RME-RMK',     'CO2', 3114000000, 'tCO2/t_fuel', 'MEPC.245(66) Cf table as amended by MEPC.308(73)/MEPC.364(79); EU MRV Regulation (EU) 2015/757 Annex I', '2018-03-01'),
    ('LPG_PROPANE',     'CO2', 3000000000, 'tCO2/t_fuel', 'MEPC.245(66) Cf table as amended by MEPC.308(73)/MEPC.364(79); EU MRV Regulation (EU) 2015/757 Annex I', '2018-03-01'),
    ('LPG_BUTANE',      'CO2', 3030000000, 'tCO2/t_fuel', 'MEPC.245(66) Cf table as amended by MEPC.308(73)/MEPC.364(79); EU MRV Regulation (EU) 2015/757 Annex I', '2018-03-01'),
    ('LNG',             'CO2', 2750000000, 'tCO2/t_fuel', 'MEPC.245(66) Cf table as amended by MEPC.308(73)/MEPC.364(79); EU MRV Regulation (EU) 2015/757 Annex I', '2018-03-01'),
    ('METHANOL',        'CO2', 1375000000, 'tCO2/t_fuel', 'MEPC.245(66) Cf table as amended by MEPC.308(73)/MEPC.364(79); EU MRV Regulation (EU) 2015/757 Annex I', '2018-03-01'),
    ('ETHANOL',         'CO2', 1913000000, 'tCO2/t_fuel', 'MEPC.245(66) Cf table as amended by MEPC.308(73)/MEPC.364(79); EU MRV Regulation (EU) 2015/757 Annex I', '2018-03-01');

-- Annual DCS reports. attained/required CII stay NULL (NOT_COMPUTABLE)
-- unless computed from operator-approved, source-cited CII configuration.
CREATE TABLE mrv_annual_reports (
    report_id UUID PRIMARY KEY,
    imo_number TEXT NOT NULL REFERENCES mrv_ships(imo_number),
    calendar_year INTEGER NOT NULL CHECK (calendar_year BETWEEN 2019 AND 2200),
    totals JSONB NOT NULL,
    attained_cii_nano BIGINT CHECK (attained_cii_nano IS NULL OR attained_cii_nano >= 0),
    required_cii_nano BIGINT CHECK (required_cii_nano IS NULL OR required_cii_nano >= 0),
    cii_rating TEXT CHECK (cii_rating IS NULL OR cii_rating IN ('A','B','C','D','E')),
    -- CII outcome coherence: rating requires both CII values.
    CHECK ((cii_rating IS NULL) = (attained_cii_nano IS NULL OR required_cii_nano IS NULL)),
    factor_set_hash TEXT NOT NULL CHECK (factor_set_hash ~ '^sha256:[0-9a-f]{64}$'),
    state TEXT NOT NULL DEFAULT 'DRAFT' CHECK (state IN ('DRAFT','SUBMITTED','VERIFIER_REVIEW','VERIFIED','REJECTED')),
    compiled_by TEXT NOT NULL CHECK (length(compiled_by) BETWEEN 1 AND 512),
    submitted_by TEXT CHECK (submitted_by IS NULL OR length(submitted_by) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at TIMESTAMPTZ,
    UNIQUE (imo_number, calendar_year),
    CHECK ((state = 'DRAFT') = (submitted_by IS NULL))
);

-- State machine: DRAFT -> SUBMITTED -> VERIFIER_REVIEW -> VERIFIED|REJECTED;
-- VERIFIER_REVIEW -> SUBMITTED on REQUEST_CLARIFICATION. Totals, factor set
-- and CII outcome are frozen once the report leaves DRAFT.
CREATE FUNCTION mrv_annual_report_state_guard() RETURNS trigger AS $$
BEGIN
    IF OLD.state = NEW.state AND OLD.state <> 'DRAFT' THEN
        RAISE EXCEPTION 'mrv annual report state must transition (no-op update of state)';
    END IF;
    -- DRAFT -> DRAFT is the recompile path (totals may change pre-submit).
    IF NOT ((OLD.state = 'DRAFT' AND NEW.state IN ('DRAFT','SUBMITTED'))
         OR (OLD.state = 'SUBMITTED' AND NEW.state = 'VERIFIER_REVIEW')
         OR (OLD.state = 'VERIFIER_REVIEW' AND NEW.state IN ('VERIFIED','REJECTED','SUBMITTED'))) THEN
        RAISE EXCEPTION 'mrv annual report illegal transition % -> %', OLD.state, NEW.state;
    END IF;
    IF OLD.state <> 'DRAFT' AND (NEW.totals IS DISTINCT FROM OLD.totals
        OR NEW.factor_set_hash IS DISTINCT FROM OLD.factor_set_hash
        OR NEW.attained_cii_nano IS DISTINCT FROM OLD.attained_cii_nano
        OR NEW.required_cii_nano IS DISTINCT FROM OLD.required_cii_nano
        OR NEW.cii_rating IS DISTINCT FROM OLD.cii_rating) THEN
        RAISE EXCEPTION 'mrv annual report totals are frozen once submitted';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER mrv_annual_reports_state_guard
    BEFORE UPDATE ON mrv_annual_reports
    FOR EACH ROW EXECUTE FUNCTION mrv_annual_report_state_guard();

-- Immutable verification decision ledger; maker <> checker enforced against
-- the report's submitting principal.
CREATE TABLE mrv_verifications (
    verification_id UUID PRIMARY KEY,
    report_id UUID NOT NULL REFERENCES mrv_annual_reports(report_id),
    decision TEXT NOT NULL CHECK (decision IN ('VERIFY','REJECT','REQUEST_CLARIFICATION')),
    verifier_principal TEXT NOT NULL CHECK (length(verifier_principal) BETWEEN 1 AND 512),
    reason TEXT NOT NULL CHECK (length(reason) <= 1024),
    -- Reason code is mandatory unless the decision is VERIFY.
    CHECK (decision = 'VERIFY' OR length(reason) BETWEEN 1 AND 1024),
    ais_crosscheck JSONB,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX mrv_verifications_report_idx ON mrv_verifications (report_id, decided_at);

CREATE FUNCTION mrv_verification_four_eyes() RETURNS trigger AS $$
DECLARE
    maker TEXT;
BEGIN
    SELECT submitted_by INTO maker FROM mrv_annual_reports WHERE report_id = NEW.report_id;
    IF maker IS NOT NULL AND maker = NEW.verifier_principal THEN
        RAISE EXCEPTION 'mrv verification maker may not verify own report (four-eyes)';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER mrv_verifications_four_eyes
    BEFORE INSERT ON mrv_verifications
    FOR EACH ROW EXECUTE FUNCTION mrv_verification_four_eyes();

CREATE FUNCTION mrv_immutable_row() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'mrv ledger rows are immutable (no % on %)', TG_OP, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER mrv_verifications_immutable
    BEFORE UPDATE OR DELETE ON mrv_verifications
    FOR EACH ROW EXECUTE FUNCTION mrv_immutable_row();

-- Statements of Compliance: only a VERIFIED report may produce one
-- (trigger-enforced); one SoC per report; immutable once issued.
CREATE TABLE mrv_statements_of_compliance (
    soc_id UUID PRIMARY KEY,
    report_id UUID NOT NULL UNIQUE REFERENCES mrv_annual_reports(report_id),
    issued_by TEXT NOT NULL CHECK (length(issued_by) BETWEEN 1 AND 512),
    issued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    artifact_sha256 TEXT NOT NULL CHECK (artifact_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE FUNCTION mrv_soc_requires_verified() RETURNS trigger AS $$
DECLARE
    report_state TEXT;
BEGIN
    SELECT state INTO report_state FROM mrv_annual_reports WHERE report_id = NEW.report_id;
    IF report_state IS DISTINCT FROM 'VERIFIED' THEN
        RAISE EXCEPTION 'mrv SoC requires a VERIFIED annual report (state is %)', COALESCE(report_state, 'ABSENT');
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER mrv_soc_verified_guard
    BEFORE INSERT ON mrv_statements_of_compliance
    FOR EACH ROW EXECUTE FUNCTION mrv_soc_requires_verified();

CREATE TRIGGER mrv_soc_immutable
    BEFORE UPDATE OR DELETE ON mrv_statements_of_compliance
    FOR EACH ROW EXECUTE FUNCTION mrv_immutable_row();

-- Canonical signed annual-report artifacts (JCS + JWS-EdDSA, envelope v1.0
-- scheme); artifact_sha256 anchors mrv.soc.v1 provenance.ledgerCommitHash.
CREATE TABLE mrv_report_artifacts (
    report_id UUID PRIMARY KEY REFERENCES mrv_annual_reports(report_id),
    artifact_json JSONB NOT NULL,
    artifact_sha256 TEXT NOT NULL CHECK (artifact_sha256 ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER mrv_report_artifacts_immutable
    BEFORE UPDATE OR DELETE ON mrv_report_artifacts
    FOR EACH ROW EXECUTE FUNCTION mrv_immutable_row();

-- Transactional outbox (financial_intent_outbox LIKE-pattern): row +
-- outbox in one transaction; the publisher drains to Kafka at-least-once
-- with the outbox event id as the idempotent key. payload is the fully
-- signed envelope v1.0 document (signed at intake, fail closed).
CREATE TABLE mrv_outbox (
    event_id UUID PRIMARY KEY,
    aggregate_id TEXT NOT NULL CHECK (length(aggregate_id) BETWEEN 1 AND 256),
    event_type TEXT NOT NULL CHECK (event_type IN (
        'mrv.fuel-report.v1','mrv.voyage.v1','mrv.verification.v1',
        'mrv.emissions-annual.v1','mrv.soc.v1','mrv.activity-estimate.v1')),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX mrv_outbox_unpublished_idx ON mrv_outbox (created_at) WHERE published_at IS NULL;

-- ---------------------------------------------------------------------
-- RLS: MRV records are flag-administration records. Every transaction
-- touching them must bind the acting principal (app.mrv_actor), mirroring
-- the app.tenant_id default-deny discipline of 0007. Unbound sessions read
-- and write nothing. mrv_emission_factors is public reference data and is
-- deliberately not RLS-governed.
CREATE FUNCTION mrv_actor_bound() RETURNS boolean AS $$
    SELECT NULLIF(current_setting('app.mrv_actor', true), '') IS NOT NULL
$$ LANGUAGE sql STABLE;

DO $$
DECLARE
    governed TEXT[] := ARRAY[
        'mrv_ships','mrv_monitoring_plans','mrv_fuel_reports','mrv_voyages',
        'mrv_annual_reports','mrv_verifications','mrv_statements_of_compliance',
        'mrv_report_artifacts','mrv_outbox'];
    governed_table TEXT;
BEGIN
    FOREACH governed_table IN ARRAY governed LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', governed_table);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', governed_table);
        EXECUTE format('CREATE POLICY %I ON %I USING (mrv_actor_bound()) WITH CHECK (mrv_actor_bound())',
            governed_table || '_actor_policy', governed_table);
    END LOOP;
END $$;

GRANT SELECT, INSERT, UPDATE ON mrv_ships, mrv_monitoring_plans, mrv_voyages,
    mrv_annual_reports, mrv_report_artifacts, mrv_outbox TO geo;
GRANT SELECT, INSERT ON mrv_fuel_reports TO geo;
GRANT SELECT, INSERT ON mrv_verifications, mrv_statements_of_compliance TO geo;
GRANT SELECT ON mrv_emission_factors TO geo;
