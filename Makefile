.PHONY: up down build test test-integration bench load-test lint dev-api setup fmt ch-smoke ch-query push push-api push-clickhouse

REGISTRY  ?= tztcloud
IMAGE_TAG ?= latest

# ── Infrastructure ────────────────────────────────────────────────────────────

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f

# ── Build ─────────────────────────────────────────────────────────────────────

build:
	cd api && go build ./...

# Build and push production images to the registry.
# Requires: docker login tztcloud
push: push-api push-clickhouse

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

# ── Test ──────────────────────────────────────────────────────────────────────

test:
	cd api && go test ./... -race -count=1

# Integration tests: require a running ClickHouse (make up first).
test-integration:
	cd api && CLICKHOUSE_ADDR=localhost:9000 go test -tags=integration ./internal/repo/clickhouse/... -v -timeout=60s

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
	clickhouse-client --query "SELECT count() AS n, event_type, org FROM naap.events GROUP BY event_type, org ORDER BY n DESC"
	@echo "=== Orch state rows ==="
	clickhouse-client --query "SELECT count() FROM naap.agg_orch_state FINAL"
	@echo "=== Stream hourly rows ==="
	clickhouse-client --query "SELECT sum(started), sum(completed), sum(no_orch) FROM naap.agg_stream_hourly"
	@echo "=== Payment hourly rows ==="
	clickhouse-client --query "SELECT count(), sum(total_wei) FROM naap.agg_payment_hourly"

# Interactive ClickHouse client.
ch-query:
	clickhouse-client --user naap_admin --password changeme

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
