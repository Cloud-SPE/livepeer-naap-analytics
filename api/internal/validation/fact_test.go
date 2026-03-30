//go:build validation

package validation

// ── RULE-FACT-001 ─────────────────────────────────────────────────────────────
// Historical state and latest state must both be recoverable from the new
// canonical snapshot path.
//
// ── RULE-FACT-002 ─────────────────────────────────────────────────────────────
// Canonical facts must converge to one row per business key.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRuleFact001_HistoricalCapabilitySnapshotsAndLatestStateAreRecoverable(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	orchAddr := strings.ToLower(fmt.Sprintf("0x%s", uid("orch")))
	orchURI := fmt.Sprintf("https://orch-%s.example.com", uid("uri"))

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch-v1","uri":%q,"version":"0.7.0"}]`, orchAddr, orchURI),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "network_capabilities", EventTs: ts.Add(time.Minute), Org: h.org,
			Data:       fmt.Sprintf(`[{"address":%q,"local_address":"orch-v2","uri":%q,"version":"0.8.0"}]`, orchAddr, orchURI),
			IngestedAt: ts.Add(time.Minute),
		},
	})

	if h.queryInt(t, `SELECT count() FROM naap.canonical_capability_snapshots WHERE org = ? AND orch_address = ?`, h.org, orchAddr) != 2 {
		t.Errorf("RULE-FACT-001: expected 2 historical capability rows for %s", orchAddr)
	}
	if got := h.queryString(t, `SELECT version FROM naap.canonical_latest_orchestrator_state WHERE orch_address = ?`, orchAddr); got != "0.8.0" {
		t.Errorf("RULE-FACT-001: latest orchestrator version = %q, want 0.8.0", got)
	}
}

func TestRuleFact001_SessionFactReflectsLatestLifecycleState(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request"}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: uid("e"), EventType: "ai_stream_status", EventTs: ts.Add(5 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":"https://orch.example.com"},"inference_status":{"fps":10.0,"restart_count":1,"last_error":"boom"}}`, streamID, requestID),
			IngestedAt: ts.Add(5 * time.Second),
		},
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts.Add(30 * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_ingest_stream_closed"}`, streamID, requestID),
			IngestedAt: ts.Add(30 * time.Second),
		},
	})

	if h.queryInt(t, `SELECT toUInt64(completed) FROM naap.canonical_session_latest WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-FACT-001: canonical session fact did not retain completed=1")
	}
	if h.queryInt(t, `SELECT toUInt64(restart_seen) FROM naap.canonical_session_latest WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-FACT-001: canonical session fact did not retain restart_seen=1")
	}
	if h.queryInt(t, `SELECT toUInt64(error_seen) FROM naap.canonical_session_latest WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-FACT-001: canonical session fact did not retain error_seen=1")
	}
}

func TestRuleFact002_CanonicalSessionFactsConvergeToOneRowPerSession(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("s")
	requestID := uid("req")
	key := canonicalSessionKey(h.org, streamID, requestID)

	for i := 0; i < 3; i++ {
		h.insert(t, []rawEvent{{
			EventID: uid("e"), EventType: "ai_stream_status",
			EventTs: ts.Add(time.Duration(i) * time.Second), Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"pipeline":"text-to-image","state":"ONLINE","orchestrator_info":{"address":"0xabc","url":"https://orch.example.com"},"inference_status":{"fps":10.0,"restart_count":0,"last_error":""}}`, streamID, requestID),
			IngestedAt: ts.Add(time.Duration(i) * time.Second),
		}})
	}

	if h.queryInt(t, `SELECT count() FROM naap.canonical_session_latest WHERE canonical_session_key = ?`, key) != 1 {
		t.Errorf("RULE-FACT-002: canonical session fact did not converge to one row for %s", key)
	}
	if h.queryInt(t, `SELECT status_sample_count FROM naap.canonical_session_latest WHERE canonical_session_key = ?`, key) != 3 {
		t.Errorf("RULE-FACT-002: canonical session fact did not retain all 3 status samples")
	}
}
