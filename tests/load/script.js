/**
 * k6 load test script for the NaaP Analytics API.
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
import { Counter } from 'k6/metrics';

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

// ── Active endpoints (in sync with server.go routes) ──────────────────────────

const ENDPOINTS = [
  // Network / supply
  '/v1/net/orchestrators',
  '/v1/net/models',
  '/v1/net/capacity',

  // Performance (streaming)
  '/v1/perf/stream/by-model',

  // SLA / demand
  '/v1/sla/compliance',
  '/v1/network/demand',

  // GPU
  '/v1/gpu/network-demand',
  '/v1/gpu/metrics',

  // Jobs — request/response aggregates (R19)
  '/v1/jobs/demand',
  '/v1/jobs/sla',
  '/v1/jobs/by-model',

  // AI Batch (R17)
  '/v1/ai-batch/summary',
  '/v1/ai-batch/jobs',
  '/v1/ai-batch/llm/summary',

  // BYOC (R18)
  '/v1/byoc/summary',
  '/v1/byoc/jobs',
  '/v1/byoc/workers',
  '/v1/byoc/auth',

  // Dashboard — streaming / supply (R16)
  '/v1/dashboard/kpi',
  '/v1/dashboard/pipelines',
  '/v1/dashboard/orchestrators',
  '/v1/dashboard/gpu-capacity',
  '/v1/dashboard/pipeline-catalog',
  '/v1/dashboard/pricing',
  '/v1/dashboard/job-feed',

  // Dashboard — request-job (R17/R18)
  '/v1/dashboard/jobs/overview',
  '/v1/dashboard/jobs/by-pipeline',
  '/v1/dashboard/jobs/by-capability',
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
