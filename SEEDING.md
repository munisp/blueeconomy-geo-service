# Database Seeding (demo/development only)

`cmd/seed` loads deterministic, idempotent, clearly-synthetic Nigerian maritime
demo data (NCS/FIRS/CVFF banks, Apapa/Tin Can/Onne references, NGN kobo
amounts) into every table.

## Safety gates

The seeder **refuses** to run when `ENV=production` or `PROFILE=prod`, and
requires explicit acknowledgement via `SEED_DEMO=true`.

## Usage

```sh
# apply migrations (userspace psql is fine)
make migrate DATABASE_URL=postgres://postgres@127.0.0.1:5432/blueeconomy_geo

# seed (idempotent: INSERT ... ON CONFLICT DO NOTHING, deterministic IDs)
SEED_DEMO=true make seed DATABASE_URL=postgres://postgres@127.0.0.1:5432/blueeconomy_geo
```

## Layout

- `db/seed/seed.sql` — canonical seed data (FK-topological order, single tx).
- `cmd/seed/main.go` + `cmd/seed/seed.sql` — env-gated Go runner embedding the seed.
- `db/seed/seed-coverage.json` — proof: every public table × rowcount after seeding.
- `scripts/seed_coverage.py` — regenerates the coverage dump.

## Coverage

43/43 public tables populated (97 rows). See `db/seed/seed-coverage.json`.
No production-path logic is touched by this change.
