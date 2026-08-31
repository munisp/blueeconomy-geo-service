# Performance Notes (Phase 11 audit)

Scope: index coverage vs. actual query code, unbounded queries, N+1 patterns,
connection-pool sizing, Kafka producer batching. No behavior changes; RLS and
fail-closed invariants preserved.

## Indexes added (db/migrations/0014_perf_indexes.sql)

| Index | Justifying query |
|---|---|
| `firmware_releases_tenant_kind_idx` `(tenant_id, kind, created_at DESC, id DESC)` | `devices.Store.ListReleasesForKind` — `WHERE tenant_id AND kind ORDER BY created_at DESC, id DESC`; firmware_releases shipped no secondary index in 0010 |

Coverage already present (verified, not duplicated): ais_positions
`(mmsi, observed_at DESC)` partial + BRIN time + GiST geom, geofence_events
zone/mmsi/time indexes, sos_alerts partial state index, transit registry
indexes, mrv ship-period/voyage/verification indexes, mrv_outbox partial,
vessels_static SCD-2 indexes, mrv_emission_factors PK.

## Query caps / pagination

- REST list reads already cap via explicit `LIMIT` parameters
  (app.go, fences.go, queries.go, voyage.go, transit.go) or `LIMIT 1` probes.
- `ListReleasesForKind` is intentionally unbounded: the caller applies
  deterministic rollout bucketing over the complete release set; capping it
  would change rollout semantics. Bounded in practice (firmware releases per
  tenant+kind are operator-curated). No cap added.
- GTFS/transit registry loaders read whole tables into the in-memory
  registry by design (feed build requires the full graph).

## N+1

None found on hot paths: zone-state updates use Redis pipelines, position
ingest is batched (CopyFrom/batch inserts), registry loads are single scans.

## Connection pool sizing (env, opt-in)

`store.ApplyPoolEnv` is applied to the app pool, the ingest pool
(`store.connectPool`), the device-plane pool (`devices.NewStore`) and the
mrv-api pool. Unset variables keep pgx defaults:

- `GEO_DB_POOL_MAX_CONNS` (default: pgx default = max(4, NumCPU))
- `GEO_DB_POOL_MIN_CONNS` (default: 0)
- `GEO_DB_POOL_MAX_CONN_IDLE_SEC` (default: 1800)
- `GEO_DB_POOL_MAX_CONN_LIFE_SEC` (default: 3600)

Invalid values fail closed at startup.

## Kafka producer batching (env, opt-in)

`bus` producer honors `GEO_KAFKA_BATCH_SIZE` (default: kafka-go default) and
`GEO_KAFKA_BATCH_TIMEOUT_MS` (default 50ms, the current hard-coded value).
`Async=false` and `RequiredAcks=all` are untouched.

## Remaining recommendations (not implemented)

- `geofence_events` evaluation reads per-vessel history; consider a
  `(mmsi, geofence_id, occurred_at DESC)` composite if crossing analytics
  become hot.
- `vessel-360` aggregates several point lookups per request; a read-model
  cache in Redis could shave p99 on popular MMSIs.
