# Product Sense

## What we're building

Livepeer NAAP Analytics is a real-time analytics pipeline and public API for the
Livepeer network. It ingests AI video streaming events from two Kafka topics
(`network_events` — daydream org; `streaming_events` — cloudspe org), processes
them into ClickHouse, and exposes a REST API for external developers and partners.

## Data sources

| Kafka topic | Org | Volume | Description |
|-------------|-----|--------|-------------|
| `network_events` | `daydream` | ~4.6M events/day | AI streaming telemetry, WebRTC stats, payments |
| `streaming_events` | `cloudspe` | ~35K events/day | Stream lifecycle, discovery, payments |

Broker: `infra1.livepeer.cloud:9092` (plaintext). Kafka retention: 7 days.

## Core goals

1. **Real-time visibility** — sub-second API query responses. End-to-end event
   propagation latency ≤ 5 seconds (Kafka → ClickHouse → API).
2. **Network transparency** — public API with no auth barrier. Developers and partners
   can query network state, stream activity, payments, and orch performance without
   registration.
3. **Reliability insight** — make failure signals (no-orch-available at ~15%, restart
   rate ~10%) first-class metrics, not buried in raw logs.
4. **Economics visibility** — expose payment flows in ETH/WEI with clear documentation
   of the probabilistic payment model.
5. **Orch accountability** — public leaderboard with transparent scoring model.

## Success criteria (launch)

- All 21 API endpoints operational with P99 ≤ 1 second response time
- End-to-end event propagation ≤ 5 seconds under normal Kafka lag
- ClickHouse ingesting both topics continuously with 0 data loss
- Rate limiting operational (60 req/min per IP)
- All requirements in `docs/product-specs/` have passing acceptance tests

## Non-goals (for now)

- Authentication / API keys (open + rate-limited is sufficient at launch)
- User-facing UI (API consumers build their own)
- Alerting / push notifications on failure thresholds
- On-chain settlement data (requires Ethereum indexing)
- Multi-region deployment

## Five non-negotiable tenants

See `docs/design-docs/core-beliefs.md` for the full definition.

1. Secure by default
2. Performance is critical (P99 ≤ 1s for all API endpoints)
3. No shortcuts — tradeoffs are deliberate and documented in ADRs
4. Testing and docs baked in
5. Simplicity first

## Architecture decisions

All architectural decisions are documented as ADRs in `docs/design-docs/`:
- **ADR-001** — ClickHouse + Kafka engine, no cache layer, LZ4/ZSTD compression
- **ADR-002** — REST/JSON, open + IP rate-limited, org model, RFC 7807 errors
