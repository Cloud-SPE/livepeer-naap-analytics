.PHONY: up up-tooling down build test test-integration bench load-test lint dev-api setup fmt ch-smoke ch-query push push-api push-clickhouse push-dbt push-resolver push-mcp warehouse-run warehouse-test warehouse-compile test-validation test-validation-host test-validation-docker test-validation-clean measure-baseline measure-refactor-replay migrate-status migrate-validate migrate-up bootstrap-extract resolver-logs resolver-auto resolver-bootstrap resolver-tail resolver-backfill resolver-repair-window parity-verify backfill-rollups backfill-raw-mv-views

REGISTRY  ?= tztcloud
IMAGE_TAG ?= latest

# ── Infrastructure ────────────────────────────────────────────────────────────

up:
	docker compose up --build -d

up-tooling:
	docker compose --profile tooling up --build -d warehouse

down:
	docker compose --profile tooling --profile validation down -v --remove-orphans

logs:
	docker compose logs -f

# ── Build ─────────────────────────────────────────────────────────────────────

build:
	cd api && go build ./...

# Build and push production images to the registry.
# Requires: docker login tztcloud
push: push-api push-clickhouse push-dbt push-resolver

push-api:
	docker build \
	    -f infra/docker/api.Dockerfile \
	    -t $(REGISTRY)/naap-api:$(IMAGE_TAG) \
	    .
	docker push $(REGISTRY)/naap-api:$(IMAGE_TAG)

push-clickhouse:
	docker build \
	    -f infra/docker/clickhouse.Dockerfile \
	    -t $(REGISTRY)/naap-clickhouse:$(IMAGE_TAG) \
	    .
	docker push $(REGISTRY)/naap-clickhouse:$(IMAGE_TAG)

push-dbt:
	docker build \
	    -f infra/docker/dbt.Dockerfile \
	    -t $(REGISTRY)/naap-dbt:$(IMAGE_TAG) \
	    .
	docker push $(REGISTRY)/naap-dbt:$(IMAGE_TAG)

push-resolver:
	docker build \
	    -f infra/docker/resolver.Dockerfile \
	    -t $(REGISTRY)/naap-resolver:$(IMAGE_TAG) \
	    .
	docker push $(REGISTRY)/naap-resolver:$(IMAGE_TAG)

push-mcp:
	docker build \
	    -t $(REGISTRY)/naap-mcp-clickhouse:$(IMAGE_TAG) \
	    ../mcp-clickhouse
	docker push $(REGISTRY)/naap-mcp-clickhouse:$(IMAGE_TAG)

# ── Test ──────────────────────────────────────────────────────────────────────

test:
	cd api && go test ./... -race -count=1

# Integration tests: require a running ClickHouse (make up first).
test-integration:
	cd api && CLICKHOUSE_ADDR=localhost:9000 go test -tags=integration ./internal/repo/clickhouse/... -v -timeout=60s

# Validation tests: scenario-based data-quality harness.
# `test-validation` is the default regression gate and runs against an isolated
# ClickHouse + dbt stack so replayed local development data does not distort
# runtime or cause heavy legacy views to time out the gate.
test-validation:
	$(MAKE) test-validation-clean

# Host-mode validation against whatever is running on localhost:9000.
# Keep this for ad hoc debugging only; it is not the stable regression gate.
test-validation-host:
	cd api && CLICKHOUSE_ADDR=localhost:9000 \
	         CLICKHOUSE_WRITER_USER=naap_writer \
	         CLICKHOUSE_WRITER_PASSWORD=naap_writer_changeme \
	         go test -tags=validation ./internal/validation/... -v -timeout=240s

test-validation-docker:
	@set -e; \
	trap 'docker compose --profile validation rm -sf validation-clickhouse warehouse-validation validation-go >/dev/null 2>&1 || true; docker volume rm $$(docker volume ls -q | grep livepeer-naap-analytics-v3_validation_clickhouse_data) >/dev/null 2>&1 || true' EXIT; \
	docker compose --profile validation run --rm validation-go

test-validation-clean:
	@set -e; \
	docker compose --profile validation rm -sf validation-clickhouse warehouse-validation validation-go >/dev/null 2>&1 || true; \
	docker volume rm $$(docker volume ls -q | grep livepeer-naap-analytics-v3_validation_clickhouse_data) >/dev/null 2>&1 || true; \
	trap 'docker compose --profile validation rm -sf validation-clickhouse warehouse-validation validation-go >/dev/null 2>&1 || true; docker volume rm $$(docker volume ls -q | grep livepeer-naap-analytics-v3_validation_clickhouse_data) >/dev/null 2>&1 || true' EXIT; \
	docker compose --profile validation run --rm validation-go

warehouse-run:
	docker compose --profile tooling run --rm warehouse run

warehouse-test:
	docker compose --profile tooling run --rm warehouse test

warehouse-compile:
	docker compose --profile tooling run --rm warehouse compile

# Benchmarks: measures handler+JSON overhead using the noop repo.
bench:
	cd api && go test ./internal/runtime/... -bench=. -benchmem -run='^$$' -count=3

# Load test: requires k6 (https://k6.io) and a running API (make up first).
load-test:
	k6 run tests/load/script.js

# ── Lint ──────────────────────────────────────────────────────────────────────

lint:
	cd api && go vet ./...
	cd api && staticcheck ./... || true

# ── Format ───────────────────────────────────────────────────────────────────

fmt:
	cd api && gofmt -w .

# ── Dev ───────────────────────────────────────────────────────────────────────

dev-api:
	cd api && go run ./cmd/server

# ── ClickHouse ────────────────────────────────────────────────────────────────

# Smoke test: verifies events are flowing from Kafka into ClickHouse.
# Run ~60s after `make up` to allow the Kafka engine to start consuming.
ch-smoke:
	@echo "=== ClickHouse ping ==="
	curl -sf http://localhost:8123/ping
	@echo ""
	@echo "=== Event counts by type ==="
	clickhouse-client --query "SELECT count() AS n, event_type, org FROM naap.accepted_raw_events GROUP BY event_type, org ORDER BY n DESC"
	@echo "=== Orch state rows ==="
	clickhouse-client --query "SELECT count() FROM naap.agg_orch_state FINAL"
	@echo "=== Stream hourly rows ==="
	clickhouse-client --query "SELECT sum(requested_sessions), sum(startup_success_sessions), sum(no_orch_sessions) FROM naap.api_stream_hourly"
	@echo "=== Payment hourly rows ==="
	clickhouse-client --query "SELECT count(), sum(total_wei) FROM naap.agg_payment_hourly"

# Interactive ClickHouse client.
ch-query:
	clickhouse-client --user naap_admin --password changeme

migrate-status:
	docker compose exec clickhouse bash /docker-entrypoint-initdb.d/ch-migrate.sh status

migrate-validate:
	docker compose exec clickhouse bash /docker-entrypoint-initdb.d/ch-migrate.sh validate

migrate-up:
	docker compose exec clickhouse bash /docker-entrypoint-initdb.d/ch-migrate.sh up

bootstrap-extract:
	python3 scripts/extract_clickhouse_bootstrap.py

resolver-logs:
	docker compose logs -f resolver

resolver-auto:
	docker compose run --rm $(if $(CLICKHOUSE_TIMEOUT),-e CLICKHOUSE_TIMEOUT=$(CLICKHOUSE_TIMEOUT),) resolver -mode auto $(if $(DRY_RUN),-dry-run,) $(if $(ORG),-org "$(ORG)",) $(if $(EXCLUDE_ORG_PREFIXES),-exclude-org-prefixes "$(EXCLUDE_ORG_PREFIXES)",) $(if $(FROM),-from "$(FROM)",) $(if $(TO),-to "$(TO)",)

resolver-bootstrap:
	docker compose run --rm $(if $(CLICKHOUSE_TIMEOUT),-e CLICKHOUSE_TIMEOUT=$(CLICKHOUSE_TIMEOUT),) resolver -mode bootstrap $(if $(DRY_RUN),-dry-run,) $(if $(ORG),-org "$(ORG)",) $(if $(EXCLUDE_ORG_PREFIXES),-exclude-org-prefixes "$(EXCLUDE_ORG_PREFIXES)",) $(if $(FROM),-from "$(FROM)",) $(if $(TO),-to "$(TO)",)

resolver-tail:
	docker compose run --rm $(if $(CLICKHOUSE_TIMEOUT),-e CLICKHOUSE_TIMEOUT=$(CLICKHOUSE_TIMEOUT),) resolver -mode tail $(if $(DRY_RUN),-dry-run,)

resolver-backfill:
	docker compose run --rm $(if $(CLICKHOUSE_TIMEOUT),-e CLICKHOUSE_TIMEOUT=$(CLICKHOUSE_TIMEOUT),) resolver -mode backfill $(if $(DRY_RUN),-dry-run,) -from "$(FROM)" -to "$(TO)" $(if $(ORG),-org "$(ORG)",) $(if $(EXCLUDE_ORG_PREFIXES),-exclude-org-prefixes "$(EXCLUDE_ORG_PREFIXES)",)

resolver-repair-window:
	docker compose run --rm $(if $(CLICKHOUSE_TIMEOUT),-e CLICKHOUSE_TIMEOUT=$(CLICKHOUSE_TIMEOUT),) resolver -mode repair-window $(if $(DRY_RUN),-dry-run,) -from "$(FROM)" -to "$(TO)" $(if $(ORG),-org "$(ORG)",) $(if $(EXCLUDE_ORG_PREFIXES),-exclude-org-prefixes "$(EXCLUDE_ORG_PREFIXES)",)

parity-verify:
	docker compose run --rm $(if $(CLICKHOUSE_TIMEOUT),-e CLICKHOUSE_TIMEOUT=$(CLICKHOUSE_TIMEOUT),) resolver -mode verify $(if $(DRY_RUN),-dry-run,) -from "$(FROM)" -to "$(TO)" $(if $(ORG),-org "$(ORG)",) $(if $(EXCLUDE_ORG_PREFIXES),-exclude-org-prefixes "$(EXCLUDE_ORG_PREFIXES)",)

backfill-rollups:
	docker compose exec -T clickhouse clickhouse-client --user naap_admin --password changeme --multiquery < scripts/backfill_session_rollups.sql

backfill-raw-mv-views:
	docker compose exec -T clickhouse clickhouse-client --user naap_admin --password changeme --multiquery < scripts/backfill_repointed_raw_views.sql

# ── Inspector ─────────────────────────────────────────────────────────────────

inspect:
	cd tools/inspector && uv run naap-inspect --broker infra2.cloudspe.com:9092

inspect-json:
	cd tools/inspector && uv run naap-inspect --broker infra2.cloudspe.com:9092 --json

inspect-setup:
	cd tools/inspector && uv sync

# ── Setup ─────────────────────────────────────────────────────────────────────

setup:
	@bash scripts/setup.sh
	cd tools/inspector && uv sync

measure-baseline:
	python3 scripts/measure_job_baseline.py

measure-refactor-replay:
	python3 scripts/measure_refactor_replay.py
