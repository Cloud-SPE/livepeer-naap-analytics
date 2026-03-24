/**
 * k6 load test script for the NAAP Analytics API.
 *
 * Usage:
 *   k6 run tests/load/script.js
 *   BASE_URL=http://staging-api:8000 k6 run tests/load/script.js
 *   k6 run --vus=50 --duration=60s tests/load/script.js
 *
 * Default: 10 VUs, 30s, random endpoint selection.
 * The rate limiter (RATE_LIMIT_RPS=30) may produce 429s at high VU counts —
 * those are expected and counted separately in the summary.
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// ── Configuration ─────────────────────────────────────────────────────────────

export const options = {
  vus: 10,
  duration: '30s',
  thresholds: {
    // 95% of requests must complete within 500ms
    http_req_duration: ['p(95)<500'],
    // Error rate must stay below 1% (excludes intentional 429s)
    'checks{type:status_ok}': ['rate>0.99'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

// ── Endpoints ─────────────────────────────────────────────────────────────────

const ENDPOINTS = [
  // Network
  '/v1/net/summary',
  '/v1/net/orchestrators',
  '/v1/net/gpu',
  '/v1/net/models',
  // Streams
  '/v1/streams/active',
  '/v1/streams/summary',
  '/v1/streams/history',
  // Performance
  '/v1/perf/fps',
  '/v1/perf/fps/history',
  '/v1/perf/latency',
  '/v1/perf/webrtc',
  // Payments
  '/v1/payments/summary',
  '/v1/payments/history',
  '/v1/payments/by-pipeline',
  '/v1/payments/by-orch',
  // Reliability
  '/v1/reliability/summary',
  '/v1/reliability/history',
  '/v1/reliability/orchs',
  '/v1/failures',
  // Leaderboard
  '/v1/leaderboard',
];

// ── Custom metrics ────────────────────────────────────────────────────────────

const rateLimited = new Counter('rate_limited_responses');

// ── Default function ──────────────────────────────────────────────────────────

export default function () {
  const endpoint = ENDPOINTS[Math.floor(Math.random() * ENDPOINTS.length)];
  const res = http.get(`${BASE_URL}${endpoint}`, {
    tags: { endpoint },
  });

  if (res.status === 429) {
    rateLimited.add(1);
  } else {
    check(res, { 'status 200': (r) => r.status === 200 }, { type: 'status_ok' });
    check(res, { 'content-type json': (r) => r.headers['Content-Type'].includes('application/json') });
  }

  sleep(0.1);
}

// ── Setup / teardown ──────────────────────────────────────────────────────────

export function setup() {
  const health = http.get(`${BASE_URL}/healthz`);
  if (health.status !== 200) {
    throw new Error(`API not healthy at ${BASE_URL}/healthz — got ${health.status}`);
  }
}
