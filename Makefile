.PHONY: up down build test test-integration bench load-test lint dev-api dev-pipeline setup fmt ch-smoke ch-query

# ── Infrastructure ────────────────────────────────────────────────────────────

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f

# ── Build ─────────────────────────────────────────────────────────────────────

build: build-api build-pipeline

build-api:
	cd api && go build ./...

build-pipeline:
	cd pipeline && python -m py_compile $$(find src -name "*.py")

# ── Test ──────────────────────────────────────────────────────────────────────

test: test-api test-pipeline

test-api:
	cd api && go test ./... -race -count=1

test-pipeline:
	cd pipeline && python -m pytest tests/ -v

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

lint: lint-api lint-pipeline

lint-api:
	cd api && go vet ./...
	cd api && staticcheck ./... || true

lint-pipeline:
	cd pipeline && ruff check src/ tests/
	cd pipeline && mypy src/

# ── Format ───────────────────────────────────────────────────────────────────

fmt: fmt-api fmt-pipeline

fmt-api:
	cd api && gofmt -w .

fmt-pipeline:
	cd pipeline && ruff format src/ tests/

# ── Dev ───────────────────────────────────────────────────────────────────────

dev-api:
	cd api && go run ./cmd/server

dev-pipeline:
	cd pipeline && python -m src.runtime.consumer

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
	cd tools/inspector && uv run naap-inspect --broker infra1.livepeer.cloud:9092

inspect-json:
	cd tools/inspector && uv run naap-inspect --broker infra1.livepeer.cloud:9092 --json

inspect-setup:
	cd tools/inspector && uv sync

# ── Setup ─────────────────────────────────────────────────────────────────────

setup:
	@bash scripts/setup.sh
	cd tools/inspector && uv sync
