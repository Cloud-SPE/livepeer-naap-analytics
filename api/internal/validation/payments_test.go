//go:build validation

package validation

// ── RULE-PAYMENT-LINKAGE-001 ─────────────────────────────────────────────────
// Payments must link to canonical sessions safely: request_id only.
// Payments without a request_id are left unlinked (unresolved). manifest_id must
// never act as a standalone session join key.

import (
	"fmt"
	"testing"
)

func TestRulePaymentLinkage001_RequestLinksRemainCanonical(t *testing.T) {
	h := newHarness(t)
	ts := anchor()
	streamID := uid("stream")
	requestID := uid("req")
	sessionID := uid("sess")
	key := canonicalSessionKey(h.org, streamID, requestID)

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"stream_id":%q,"request_id":%q,"type":"gateway_receive_stream_request","pipeline":"text-to-image"}`, streamID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: "pay_request", EventType: "create_new_payment", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"sessionID":%q,"requestID":%q,"manifestID":"manifest-a","pipeline":"text-to-image","sender":"0xsender","recipient":"0xrecipient","orchestrator":"https://orch.example.com","faceValue":"100","price":"2","winProb":"0.5","numTickets":"1"}`, sessionID, requestID),
			IngestedAt: ts,
		},
		{
			EventID: "pay_session", EventType: "create_new_payment", EventTs: ts, Org: h.org,
			Data:       fmt.Sprintf(`{"sessionID":%q,"requestID":"","manifestID":"manifest-a","pipeline":"text-to-image","sender":"0xsender","recipient":"0xrecipient","orchestrator":"https://orch.example.com","faceValue":"150","price":"3","winProb":"0.5","numTickets":"1"}`, sessionID),
			IngestedAt: ts,
		},
		{
			EventID: "pay_unresolved", EventType: "create_new_payment", EventTs: ts, Org: h.org,
			Data:       `{"sessionID":"sess-missing","requestID":"","manifestID":"manifest-a","pipeline":"text-to-image","sender":"0xsender2","recipient":"0xrecipient2","orchestrator":"https://orch-miss.example.com","faceValue":"200","price":"4","winProb":"0.5","numTickets":"1"}`,
			IngestedAt: ts,
		},
	})

	if got := h.queryString(t, `SELECT canonical_session_key FROM naap.canonical_payment_links WHERE org = ? AND event_id = 'pay_request'`, h.org); got != key {
		t.Errorf("RULE-PAYMENT-LINKAGE-001: request-linked payment key = %q, want %q", got, key)
	}
	if got := h.queryString(t, `SELECT link_method FROM naap.canonical_payment_links WHERE org = ? AND event_id = 'pay_request'`, h.org); got != "request_id" {
		t.Errorf("RULE-PAYMENT-LINKAGE-001: request-linked payment method = %q, want request_id", got)
	}
	// pay_session has requestID="" so it is not linkable via request_id.
	// Session-id anchoring was dropped from the resolver (see canonical_payment_links_store).
	// Expect the payment to be left unlinked and unresolved.
	if got := h.queryString(t, `SELECT link_method FROM naap.canonical_payment_links WHERE org = ? AND event_id = 'pay_session'`, h.org); got != "unlinked" {
		t.Errorf("RULE-PAYMENT-LINKAGE-001: no-request_id payment method = %q, want unlinked", got)
	}
	if got := h.queryString(t, `SELECT link_status FROM naap.canonical_payment_links WHERE org = ? AND event_id = 'pay_session'`, h.org); got != "unresolved" {
		t.Errorf("RULE-PAYMENT-LINKAGE-001: no-request_id payment status = %q, want unresolved", got)
	}
	if got := h.queryString(t, `SELECT link_status FROM naap.canonical_payment_links WHERE org = ? AND event_id = 'pay_unresolved'`, h.org); got != "unresolved" {
		t.Errorf("RULE-PAYMENT-LINKAGE-001: unresolved payment status = %q, want unresolved", got)
	}
}

func TestRulePaymentLinkage001_ManifestIDAloneDoesNotInventSessionLinks(t *testing.T) {
	h := newHarness(t)
	ts := anchor()

	h.insert(t, []rawEvent{
		{
			EventID: uid("e"), EventType: "stream_trace", EventTs: ts, Org: h.org,
			Data:       `{"stream_id":"stream-a","request_id":"req-a","type":"gateway_receive_stream_request","pipeline":"text-to-image"}`,
			IngestedAt: ts,
		},
		{
			EventID: "manifest_only_payment", EventType: "create_new_payment", EventTs: ts, Org: h.org,
			Data:       `{"sessionID":"","requestID":"","manifestID":"manifest-a","pipeline":"text-to-image","sender":"0xsender","recipient":"0xrecipient","orchestrator":"https://orch.example.com","faceValue":"100","price":"2","winProb":"0.5","numTickets":"1"}`,
			IngestedAt: ts,
		},
	})

	if got := h.queryString(t, `SELECT link_status FROM naap.canonical_payment_links WHERE org = ? AND event_id = 'manifest_only_payment'`, h.org); got != "unresolved" {
		t.Errorf("RULE-PAYMENT-LINKAGE-001: manifest-only payment status = %q, want unresolved", got)
	}
}
