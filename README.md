# blueeconomy-geo-service

The sovereign vessel-tracking hot path for the Nigerian maritime platform
(GEO_ARCHITECTURE.md §2). Multi-tier real tracking — Tier-3 shore AIS
receivers (NMEA TCP/UDP :10110) + aisstream.io WebSocket (dev/gap-fill),
Tier-1 GSM/GPS store-forward trackers (GT06/Concox TCP), Tier-0 Flutter-app
outbox position reports + SOS — all normalized onto the same canonical
signed envelope with `source_class`.

```
connectors/  NMEA TCP+UDP listener (:10110) · aisstream.io WS (pre-decoded JSON)
             GT06/Concox TCP tracker decoder · app-report HTTP (outbox flush)
             replay connector (dev-only synthetic fixtures)
decode/      go-ais AIVDM/AIVDO decode, multi-fragment reassembly, tag-block
             preservation (github.com/BertoldVdb/go-ais v0.4.0 — see below)
validate/    lat/lon bounds · 91°/181° sentinels · (0,0) null-island ·
             impossible speed (>60 kn) · MMSI 9-digit + MID-657 flagging ·
             same-MMSI bifurcation spoof indicator → vessels.quarantine
dedup/       (mmsi, msg_type, payload_hash) 10–30 s window, Redis SETNX+TTL
sign/        RFC 8785 JCS + JWS-EdDSA envelope v1.0 (fleet pattern, kid
             "blueeconomy-geo-service-<epoch>"), FHIR Bundle wrap,
             classification coherence (envelope >= content), SOS floor
bus/         Kafka: ais.raw (keyed mmsi) · vessels.events (signed) ·
             vessels.quarantine — fail-closed with retry/backoff
store/       PostGIS writer: ais_positions (RANGE-partitioned daily, BRIN ts,
             GiST geom) · vessels_static (SCD-2) · latest_positions (upsert) ·
             geofence evaluator (ST_Intersects enter/exit, Redis zone state)
api/         REST /v1/geo: bbox latest, vessel-360, track replay (GeoJSON
             LineString), vessels-in-zone, zone admin (maker-checker), SOS;
             PBAC (Keycloak RS256) + clearance ladder + tenant RLS
```

## Database

Migrations `db/migrations/0001..0004` (embedded, applied at startup unless
`GEO_RUN_MIGRATIONS=false`):

- `geofence_zones` — maker-checker (`draft` → `approved`), four-eyes
  (`UNIQUE(zone_id, principal_id)` approval ledger, maker ≠ checker trigger),
  `geography(POLYGON,4326)`, classification floor, tenant-scoped.
- `ais_positions` — RANGE-partitioned daily on `observed_at` (BRIN on
  `observed_at`, GiST on `geom`); daily partitions provisioned by
  `geo_ensure_position_partition(date)` at startup and on a 6-hour timer.
  Fixed-point columns (`latitude_micros`, `speed_over_ground_milliknots`,
  ...) per contract; DB CHECKs mirror contract validation.
- `vessels_static` — SCD-2 (`valid_from`/`valid_to`; one current row per
  MMSI via partial unique index).
- `latest_positions` — upsert per `mmsi`/`vessel_ref`, moves only forward in
  observed time.
- `geofence_events`, `app_position_reports`, `sos_alerts` — with
  `UNIQUE(reporter_id, outbox_id)` idempotency and the SOS RESTRICTED
  classification floor enforced by CHECK. `sos_alerts` additionally carries
  the lifecycle ledger (`acknowledged_by/at/note`, `resolved_by/at/note`,
  state/ledger coherence CHECK) behind
  `POST /v1/geo/sos/{id}/acknowledge|resolve`.
- FORCE ROW LEVEL SECURITY + tenant policies on the tenant-scoped tables
  (`app.tenant_id` bound per transaction). Since `0007_rls_default_deny.sql`
  an unbound session is **default-deny**: the tenant predicate compares
  against the bound GUC only — no NULL/'' admit clause. The one legitimate
  platform-wide path, the ingest geofence evaluator, runs as the explicit
  `geo_ingest` role (NOLOGIN, NOBYPASSRLS; `SET LOCAL ROLE` per transaction)
  with its own narrow policies (SELECT zones, INSERT crossings).

Role convention: the application connects as `geo` (NOSUPERUSER NOBYPASSRLS);
migrations run as the table-owning migrator role.

## Position plane scoping (GE-4 doctrine statement)

The vessel position plane — `ais_positions`, `latest_positions`,
`vessels_static` and the `GET /v1/geo/vessels*` read models — is a **single
shared national picture scoped by classification clearance, NOT by tenant**.
These tables deliberately carry no tenant column: the national maritime
picture is one shared asset, and the classification ladder (reader clearance
>= row classification) is the access boundary, identical to the
maritime-intelligence doctrine. Tenant isolation (RLS) governs only the
tenant-owned geofence objects (`geofence_zones`, `geofence_events`); a
vessel-360 zone history is read with the caller's tenant bound and is empty
for tenant-less principals.

`GEO_POSITION_PLANE` (default `shared`) pins this contract: setting it to
`tenant` (or any other value) **fails closed at startup** because the
position schema has no tenant support — a mis-scoped deployment must never
silently fall back to the shared plane.

## Real-source setup

| Source | Env | Notes |
| --- | --- | --- |
| Shore AIS receivers (Tier-3) | `GEO_NMEA_TCP_ADDR` / `GEO_NMEA_UDP_ADDR` (e.g. `:10110`) | NMEA 0183 AIVDM/AIVDO, tag blocks preserved |
| aisstream.io (dev/gap-fill) | `GEO_AISSTREAM_API_KEY` | Pre-decoded JSON over WebSocket; Nigerian AoI subscription by default |
| GT06/Concox trackers (Tier-1) | `GEO_GT06_ADDR` (e.g. `:30002`) | Binary protocol, X.25 CRC verified, IMEI tokenized to pseudonymous vessel ref |
| Mobile outbox (Tier-0) | REST `POST /v1/geo/app-reports` | `outbox_id` idempotency: 200 / idempotent / 409 |

Core env: `GEO_PG_DSN`, `GEO_REDIS_ADDR`, `GEO_KAFKA_BROKERS`,
`ENVELOPE_SIGNING_PRIVATE_KEY` + `ENVELOPE_SIGNING_KEY_EPOCH` (fail-closed
when absent), `GEO_PRODUCER_PRINCIPAL_ID`, `GEO_PRODUCER_PRINCIPAL_ROLE`,
`GEO_API_ADDR`, auth via `GEO_AUTH_MODE=oidc` (`GEO_OIDC_ISSUER`,
`GEO_OIDC_AUDIENCE`, `GEO_OIDC_JWKS_URL`) or `trusted_proxy`
(`GEO_TRUSTED_PROXY_CIDRS`, `GEO_TRUSTED_PROXY_ID`).
`GEO_DEDUP_WINDOW` (10s–30s, default 15s), `GEO_PUBLISH_AIS_RAW` (default
true), `GEO_POSITION_PLANE` (default `shared`; anything else fails closed —
see "Position plane scoping").

## Dev fixtures (synthetic, format-valid)

`fixtures/nmea_replay.txt` contains REAL-format AIVDM sentences generated by
the go-ais encoder (`go run ./cmd/geo-fixturegen`) — valid checksums, bit
layout and multi-fragment reassembly — for three documented **synthetic**
vessels (`SYNTH TRADER ONE` MMSI 657210300, `SYNTH FERRY TWO` 657221000,
`SYNTH COASTER` 235081000). They are development data, never presented as
live traffic. Replay them with `GEO_REPLAY_FILE=fixtures/nmea_replay.txt`;
the replay connector **refuses to start when `APP_ENV=prod`**.

## Realtime schedules: GTFS static + GTFS-RT (advisory §5)

The transit registry (migration `0009_transit_registry.sql`) is the
operator-maintained source of truth behind the feeds: `transit_agencies`,
`transit_routes` (`route_type 4` ferry), `transit_stops` (jetties,
fixed-point coordinates), `transit_calendars`, `transit_trips`,
`transit_stop_times` (seconds after midnight, monotonic per trip enforced
at the storage boundary), `transit_route_vessels` (route ↔ MMSI assignment
windows) and `transit_alerts`. Every table is tenant-scoped with the 0007
default-deny RLS posture.

**Seeding.** NIWA/operators keep the registry as a reviewed YAML or JSON
document and load it idempotently (upserts):

    GEO_PG_DSN=... GEO_INGEST_PG_DSN=... \
    go run ./cmd/geo-transitseed -tenant niwa -file registry.yaml

Format: `fixtures/transit_seed.example.yaml` (micro-degree coordinates,
second-after-midnight times, stop sequences assigned in document order).

**Endpoints** (tenant-bound, standard read roles; positions embedded at the
caller's clearance):

- `GET /feeds/gtfs.zip` — deterministic spec-valid GTFS static archive
  (agency/routes/stops/trips/stop_times/calendar), strong ETag + 304.
- `GET /feeds/gtfs-rt/vehiclepositions.pb` — one entity per route-assigned
  vessel with a FRESH position; MMSI = `vehicle.id`.
- `GET /feeds/gtfs-rt/tripupdates.pb` — computed per-jetty ETAs.
- `GET /feeds/gtfs-rt/alerts.pb` — active-window alerts.
- `POST /v1/geo/transit/alerts` — admin create (`geo-transit-admin` /
  `geo-admin`; auth required, maker/checker deliberately not required).

**Fail-closed doctrine (never fabricate):**

- Positions older than `GEO_GTFSRT_STALE_AFTER` (default 120s) → the
  entity is OMITTED from the feed (`geo_gtfsrt_entities_omitted_total`
  counted per reason), never interpolated or extrapolated.
- ETAs are computed from reported positions/speeds only: remaining
  along-route distance / rolling median of the last
  `GEO_GTFSRT_SPEED_SAMPLES` (default 5) reported SOG values. Crew-entered
  AIS ETA fields are never read.
- A vessel with ZERO speed observations is predicted at the route default
  speed and marked LOW CONFIDENCE (`schedule_relationship=SCHEDULED`,
  300s per-stop uncertainty vs 60s live) — never passed off as live.
- A vessel whose smoothed speed is below
  `GEO_GTFSRT_ETA_MIN_SPEED_MILLIKNOTS` (default 1000 = 1 kn) produces NO
  trip update (NO_SHOW-style omission, metric
  `geo_gtfsrt_eta_omitted_total{reason="not_moving"}`): a docked or
  drifting vessel's ETA is unknowable.
- Vessels snapped further than `GEO_GTFSRT_SNAP_MAX_METERS` (default 200m)
  off the route polyline get no stop attribution and no ETA.

Route geometry v1 is the stop-to-stop polyline (no shapes.txt yet); trip
matching is time-indexed against the service calendar (matching slacks
15m/30m, code defaults in `internal/gtfsrt`). Feed builds are counted
(`geo_feed_build_total`, `geo_feed_build_duration_ms_total`,
`geo_gtfsrt_entities_emitted_total`, `geo_gtfsrt_eta_total{mode}`,
`geo_gtfsrt_speed_samples_total`).

## Tests

```
go vet ./...
go test ./...          # unit tests (decode/validate/dedup/sign/geofence/gt06)
```

Integration tests are env-gated and run the migrations, then drive the full
pipeline against real PostGIS + Redis (Kafka optional):

```
docker compose -f docker-compose.integration.yml up -d
export GEO_TEST_PG_DSN=postgres://geo:$GEO_TEST_PG_PASSWORD@localhost:55432/geo_test
export GEO_TEST_REDIS_ADDR=localhost:56379
export GEO_TEST_KAFKA_BROKERS=localhost:59092   # optional
go test ./integration/...
```

They verify daily-partition routing, geofence ENTER/EXIT with signed
envelopes, app-report/SOS idempotency (exact-replay absorb, conflict 409),
SCD-2 static upsert (in-order rotation and out-of-order drop), tenant RLS
(default-deny when unbound, explicit `geo_ingest` role for the evaluator)
and the SOS lifecycle (gate 403, illegal transition 409, signed
acknowledged/resolved envelopes).

## MRV API (mrv-api)

`cmd/mrv-api` is the Phase-8 MRV emissions boundary: the operator-facing
intake/verification REST API (`/v1/mrv/*`) over the vessel identity and
PostGIS activity plane, with a transactional outbox publisher draining
signed envelope v1.0 events to the `mrv.voyages`, `mrv.verifications` and
`mrv.soc` Kafka topics. It ships as the `mrv-api` target of the repo
`Dockerfile` (`docker build --target mrv-api .`).

Everything is env-gated and fails closed: missing DSN/brokers/address,
missing signing key material, a malformed CII config or an invalid
threshold aborts startup. Telemetry is a no-op unless
`OTEL_EXPORTER_OTLP_ENDPOINT` is set.

Required:

| Env | Notes |
| --- | --- |
| `MRV_PG_DSN` | PostgreSQL/PostGIS DSN |
| `MRV_KAFKA_BROKERS` | Comma-separated broker list for the mrv.* outbox publisher |
| `MRV_API_ADDR` | Listen address, e.g. `:8082` |
| `MRV_PRODUCER_PRINCIPAL_ID` | Envelope provenance principal |
| `ENVELOPE_SIGNING_PRIVATE_KEY` + `ENVELOPE_SIGNING_KEY_EPOCH` | Shared envelope signing surface (fail-closed when absent); kid `<producer>-<epoch>` |

Optional (defaults shown; all fail-closed on malformed values):

| Env | Default | Notes |
| --- | --- | --- |
| `MRV_PRODUCER_PRINCIPAL_ROLE` | `mrv-producer` | Envelope provenance role |
| `MRV_RUN_MIGRATIONS` | `true` | Set `false` to skip migrations at boot |
| `MRV_CII_CONFIG_PATH` | unset | Operator-approved, source-cited CII config document; when absent every CII outcome is honestly `NOT_COMPUTABLE` (never estimated) |
| `MRV_DEADLINE_REPORT_MM_DD` | `03-31` | Annual report submission deadline (advisory) |
| `MRV_DEADLINE_SOC_MM_DD` | `05-31` | SoC issuance deadline (advisory) |
| `MRV_DEADLINE_GISIS_MM_DD` | `06-30` | GISIS forwarding deadline (advisory) |
| `MRV_AIS_SOG_THRESHOLD_MILLIKNOTS` | methodology default | AIS activity-estimation SOG threshold |
| `MRV_AIS_SEGMENT_GAP_MINUTES` | methodology default | AIS segment gap split |
| `MRV_AIS_MIN_COVERAGE_PERMILLE` | methodology default | Minimum AIS coverage before estimation |
| `MRV_AIS_CROSSCHECK_TOLERANCE_PERMILLE` | `100` | AIS cross-check tolerance |
| `MRV_DCS_GT_THRESHOLD` | `5000` | IMO DCS gross-tonnage threshold |
| `MRV_OUTBOX_POLL_INTERVAL` | `2s` | Outbox drain interval |
| `MRV_AUTH_MODE` | `oidc` | `oidc` or `trusted_proxy` |
| `MRV_OIDC_JWKS_URL` / `MRV_OIDC_ISSUER` / `MRV_OIDC_AUDIENCE` / `MRV_OIDC_CA_FILE` | — | OIDC (Keycloak) verification |
| `MRV_TRUSTED_PROXY_CIDRS` / `MRV_TRUSTED_PROXY_ID` | — | Trusted edge-proxy identity mode |

## Docker / CI

`Dockerfile` builds both service binaries in one build stage and exposes
two distroless static non-root runtime targets (fleet convention):
`geo-service` (default/final stage) and `mrv-api`
(`docker build --target mrv-api .`).
CI pipeline definition lives at `ci/github-actions.yml.example` and must be
moved to `.github/workflows/` once a workflow-scoped token is available.

## Notes and documented deviations

- **go-ais fork**: GEO_ARCHITECTURE D4 names `github.com/nilsmagnus/go-ais`
  "(BertoldVdb fork, full ITU-R M.1371-5)". The nilsmagnus upstream's
  `aisnmea` subpackage does not compile at its final commit
  (v0.0.0-20230818094941); the implementation therefore pins the sanctioned
  fork `github.com/BertoldVdb/go-ais v0.4.0` in go.mod.
- **Envelope classification ladder**: envelope classification strings use
  the geo ladder (PUBLIC..SECRET) with envelope >= content coherence
  asserted at signing; `geo.sos.v1` enforces the RESTRICTED floor at payload,
  store (CHECK) and API layers.
- **Classification-floor enforcement on reads** is applied in the API via the
  clearance ladder (reader clearance >= row classification); RLS governs
  tenant isolation, identical to the maritime-intelligence doctrine.
