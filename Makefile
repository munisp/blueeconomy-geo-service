DATABASE_URL ?= postgres://postgres@127.0.0.1:5432/blueeconomy_geo

.PHONY: migrate seed seed-coverage test
migrate:
	@for f in db/migrations/*.sql; do echo "applying $$f"; psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -q -f "$$f"; done

seed:
	SEED_DEMO=$${SEED_DEMO:-false} DATABASE_URL="$(DATABASE_URL)" go run ./cmd/seed

seed-coverage: ## requires seed to have been applied
	python3 scripts/seed_coverage.py "$(DATABASE_URL)" db/seed/seed-coverage.json

test:
	go test ./...
