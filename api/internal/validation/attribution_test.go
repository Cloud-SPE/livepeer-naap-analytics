//go:build validation

package validation

// ── RULE-ATTRIBUTION-001 (partial) ───────────────────────────────────────────
// Canonical Orchestrator Identity Must Be Distinguished From Proxy Identity
//
// ai_stream_status carries orchestrator_info.address (local keypair, NOT the
// canonical on-chain ETH address) and orchestrator_info.url (service URL).
// network_capabilities carries the canonical ETH address keyed by orch_uri.
//
// Migration 011 resolves canonical identity via:
//   orchestrator_info.url → agg_orch_state.uri → agg_orch_state.orch_address
//
// Addresses are lowercased at write time by mv_orch_state (migration 005/010).
// These tests verify the full resolution chain produces lowercase canonical
// identity in agg_orch_reliability_hourly, agg_stream_state, and
// agg_discovery_latency_hourly.
//
// ── RULE-ATTRIBUTION-007 ─────────────────────────────────────────────────────
// Attribution Coverage Must Be Observable And Reviewable
// The blank-gateway rate must be computable. A sudden change in this rate is
// the primary observable proxy for upstream routing changes that affect
// attribution completeness.

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// ── RULE-ATTRIBUTION-001 (partial) ───────────────────────────────────────────

// TestRuleAttribution001_OrchAddressNormalizedToLowercase verifies that the
// canonical ETH address stored in agg_orch_state (via network_capabilities)
// is lowercased and propagates correctly into agg_orch_reliability_hourly via
// the URI-based JOIN introduced in migration 011.
func TestRuleAttribution001_OrchAddressNormalizedToLowercase(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")

	// Unique URI prevents cross-test pollution in agg_orch_state (keyed by orch_address only).
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	// Mixed-case ETH address — mv_orch_state applies lower() when writing to agg_orch_state.
	mixedCaseAddr := fmt.Sprintf("0xAbCdEf%sAbCd", uid("addr"))
	wantAddr := strings.ToLower(mixedCaseAddr)

	// Step 1: seed agg_orch_state via network_capabilities.
	// mv_orch_state stores lower(address) keyed by orch_address.
	// Field is "orch_uri" (not "uri") per migration 010 correction.
	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(`[{"address":%q,"local_address":"testorch","orch_uri":%q,"version":"0.7.0"}]`,
			mixedCaseAddr, orchURI),
		IngestedAt: ts,
	}})

	// Step 2: insert ai_stream_status referencing the same URI.
	// mv_orch_reliability_hourly JOINs agg_orch_state on URI → resolves lowercase ETH address.
	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(
			`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q},"inference_status":{"fps":10.0,"restart_count":0,"last_error":""}}`,
			streamID, mixedCaseAddr, orchURI),
		IngestedAt: ts,
	}})

	// agg_orch_reliability_hourly: orch_address must be the lowercase canonical ETH address.
	orchInReliability := h.queryInt(t, `
		SELECT countIf(orch_address = ?)
		FROM naap.agg_orch_reliability_hourly
		WHERE org = ?`, wantAddr, h.org)

	if orchInReliability == 0 {
		t.Errorf("RULE-ATTRIBUTION-001: orch address not found as lowercase %q in agg_orch_reliability_hourly", wantAddr)
	}

	// The mixed-case form must never appear (lower() was applied at agg_orch_state write time).
	mixedInReliability := h.queryInt(t, `
		SELECT countIf(orch_address = ?)
		FROM naap.agg_orch_reliability_hourly
		WHERE org = ?`, mixedCaseAddr, h.org)

	if mixedInReliability != 0 {
		t.Errorf("RULE-ATTRIBUTION-001: mixed-case address %q leaked into agg_orch_reliability_hourly (expected 0 rows, got %d)", mixedCaseAddr, mixedInReliability)
	}
}

// TestRuleAttribution001_StreamStateOrchAddressIsLowercase verifies that
// agg_stream_state.orch_address carries the lowercase canonical ETH address
// resolved from agg_orch_state via the URI JOIN (migration 011).
func TestRuleAttribution001_StreamStateOrchAddressIsLowercase(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")

	// Unique URI avoids matching agg_orch_state rows with uri="" from other tests.
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	mixedCaseAddr := fmt.Sprintf("0xDeAdBeEf%sDeAd", uid("addr"))
	wantAddr := strings.ToLower(mixedCaseAddr)

	// Step 1: seed agg_orch_state with URI ↔ lowercase ETH address mapping.
	// Field is "orch_uri" (not "uri") per migration 010 correction.
	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(`[{"address":%q,"local_address":"testorch","orch_uri":%q,"version":"0.7.0"}]`,
			mixedCaseAddr, orchURI),
		IngestedAt: ts,
	}})

	// Step 2: insert ai_stream_status with the matching URI.
	// mv_stream_state JOINs agg_orch_state on URI → writes lowercase orch_address.
	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(
			`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":%q,"url":%q}}`,
			streamID, mixedCaseAddr, orchURI),
		IngestedAt: ts,
	}})

	var orchAddr string
	row := h.conn.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT orch_address FROM naap.agg_stream_state FINAL WHERE stream_id = '%s'`, streamID))
	if err := row.Scan(&orchAddr); err != nil {
		t.Fatalf("RULE-ATTRIBUTION-001: scan orch_address: %v", err)
	}

	if orchAddr != wantAddr {
		t.Errorf("RULE-ATTRIBUTION-001: orch_address = %q, want lowercase %q", orchAddr, wantAddr)
	}
}

// TestRuleAttribution001_DiscoveryResultsOrchAddressIsLowercase verifies
// normalization in agg_discovery_latency_hourly (network-side attribution).
func TestRuleAttribution001_DiscoveryOrchAddressIsLowercase(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	mixedCaseAddr := "0xFF00FF00FF00FF00FF00FF00FF00FF00FF00FF00"
	wantAddr := strings.ToLower(mixedCaseAddr)

	h.insert(t, []rawEvent{{
		EventID: uid("e"), EventType: "discovery_results", EventTs: ts, Org: h.org,
		Data: fmt.Sprintf(
			`[{"address":%q,"latency_ms":"42","url":"https://orch.example.com"}]`,
			mixedCaseAddr),
		IngestedAt: ts,
	}})

	found := h.queryInt(t, `
		SELECT countIf(orch_address = ?)
		FROM naap.agg_discovery_latency_hourly
		WHERE org = ?`, wantAddr, h.org)

	if found == 0 {
		t.Errorf("RULE-ATTRIBUTION-001: discovery orch address not found as lowercase %q", wantAddr)
	}

	mixedFound := h.queryInt(t, `
		SELECT countIf(orch_address = ?)
		FROM naap.agg_discovery_latency_hourly
		WHERE org = ?`, mixedCaseAddr, h.org)

	if mixedFound != 0 {
		t.Errorf("RULE-ATTRIBUTION-001: mixed-case discovery address %q leaked into aggregate (got %d rows)", mixedCaseAddr, mixedFound)
	}
}

// ── RULE-ATTRIBUTION-007 ─────────────────────────────────────────────────────

// TestRuleAttribution007_BlankGatewayRateIsComputable verifies that the
// blank-gateway rate can be computed from naap.events and produces a
// meaningful number. The rate itself is not asserted — it reflects the live
// upstream configuration and is expected to be high (all events in the
// reference sample have gateway=""). The test asserts the metric is computable
// and logs the rate for observability.
func TestRuleAttribution007_BlankGatewayRateIsComputable(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	// Insert: 3 events with blank gateway, 1 with a real gateway.
	events := []rawEvent{
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Gateway: "", Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Gateway: "", Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_ingest_stream_closed"}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Gateway: "", Data: fmt.Sprintf(`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`, uid("s")), IngestedAt: ts},
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Gateway:    "gw-eu-west-1", // this one has a real gateway
			Data:       fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, uid("s")),
			IngestedAt: ts},
	}
	h.insert(t, events)

	total := h.queryInt(t,
		`SELECT count() FROM naap.events WHERE org = ?`, h.org)
	blankGW := h.queryInt(t,
		`SELECT countIf(gateway = '') FROM naap.events WHERE org = ?`, h.org)

	if total == 0 {
		t.Fatal("RULE-ATTRIBUTION-007: no events found — cannot compute blank gateway rate")
	}

	blankPct := float64(blankGW) / float64(total) * 100
	t.Logf("RULE-ATTRIBUTION-007: blank_gateway=%d/%d (%.1f%%)", blankGW, total, blankPct)

	// Fixture has 3/4 blank → 75%.
	if blankGW != 3 {
		t.Errorf("RULE-ATTRIBUTION-007: expected 3 blank-gateway events in fixture, got %d", blankGW)
	}
}

// TestRuleAttribution007_BlankGatewayRateByFamily checks per-family blank
// gateway rates. A family where the rate suddenly changes (e.g. discovery_results
// goes from 100% blank to 0% blank) signals a routing change that may affect
// attribution completeness for that family.
func TestRuleAttribution007_BlankGatewayRateByFamilyIsComputable(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	h.insert(t, []rawEvent{
		// stream_trace: blank gateway (expected from reference events)
		{EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Gateway: "", Data: fmt.Sprintf(`{"stream_id":%q,"type":"gateway_receive_stream_request"}`, uid("s")), IngestedAt: ts},
		// discovery_results: blank gateway
		{EventID: uid("e"), EventType: "discovery_results", EventTs: ts, Org: h.org,
			Gateway: "", Data: `[{"address":"0xabc","latency_ms":"50","url":"https://orch.example.com"}]`, IngestedAt: ts},
		// ai_stream_status: has a real gateway
		{EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts, Org: h.org,
			Gateway:    "gw-us-east-1",
			Data:       fmt.Sprintf(`{"stream_id":%q,"pipeline":"p","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":""}}`, uid("s")),
			IngestedAt: ts},
	})

	type familyRate struct {
		Family       string
		Total        uint64
		BlankGateway uint64
	}

	rows, err := h.conn.Query(context.Background(), fmt.Sprintf(`
		SELECT
			event_type,
			count()                       AS total,
			countIf(gateway = '')         AS blank_gw
		FROM naap.events
		WHERE org = '%s'
		GROUP BY event_type
		ORDER BY event_type`, h.org))
	if err != nil {
		t.Fatalf("RULE-ATTRIBUTION-007: query: %v", err)
	}
	defer rows.Close()

	found := 0
	for rows.Next() {
		var r familyRate
		if err := rows.Scan(&r.Family, &r.Total, &r.BlankGateway); err != nil {
			t.Fatalf("RULE-ATTRIBUTION-007: scan: %v", err)
		}
		pct := float64(r.BlankGateway) / float64(r.Total) * 100
		t.Logf("RULE-ATTRIBUTION-007: family=%-25s total=%d blank_gateway=%d (%.0f%%)",
			r.Family, r.Total, r.BlankGateway, pct)
		found++
	}
	if rows.Err() != nil {
		t.Fatalf("RULE-ATTRIBUTION-007: rows error: %v", rows.Err())
	}

	if found == 0 {
		t.Error("RULE-ATTRIBUTION-007: no event families found — coverage rate uncomputable")
	}
}
