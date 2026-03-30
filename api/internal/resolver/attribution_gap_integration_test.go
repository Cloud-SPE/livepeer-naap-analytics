//go:build validation

package resolver

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/livepeer/naap-analytics/internal/config"
)

func TestAttributionGap_RealAliasAddressResolvesCanonicalInterval(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	repo, err := newRepo(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("new repo: %v", err)
	}
	defer func() { _ = repo.close() }()

	start := time.Date(2026, 3, 29, 22, 35, 0, 0, time.UTC)
	end := time.Date(2026, 3, 29, 22, 50, 0, 0, time.UTC)
	spec := WindowSpec{
		Org:   "daydream",
		Start: &start,
		End:   &end,
	}
	selection := SelectionEvent{
		ID:                "fadd12f31383e7b7e482eaf0a05992b9",
		Org:               "daydream",
		SessionKey:        "debug",
		SelectionTS:       time.Date(2026, 3, 29, 22, 45, 57, 764000000, time.UTC),
		ObservedAddress:   "0xfc50b57b22eb5ef83016f5cb6bca489cdebef39a",
		ObservedURL:       "https://ai.lpt-4.moudi.network:18938",
		ObservedPipeline:  "pip_SDXL-turbo",
		ObservedModelHint: "",
		InputHash:         "debug",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	snapshots, err := repo.fetchCapabilitySnapshots(ctx, spec, []string{
		normalizeAddress(selection.ObservedAddress),
		normalizeURL(selection.ObservedURL),
	})
	if err != nil {
		t.Fatalf("fetch capability snapshots: %v", err)
	}
	if len(snapshots) == 0 {
		t.Fatalf("expected capability snapshots for alias address")
	}

	versions := buildCapabilityVersions(snapshots)
	intervals := buildCapabilityIntervals(versions)
	indexed := indexIntervals(intervals)
	aliasKey := selection.Org + "\x00" + normalizeAddress(selection.ObservedAddress)
	aliasBuckets := indexed[aliasKey]
	if len(aliasBuckets) == 0 {
		t.Fatalf("expected alias buckets for %s", aliasKey)
	}

	totalAliasMatches := 0
	for _, entry := range aliasBuckets {
		matches := matchBuckets(selection, entry.Buckets, entry.IdentityType)
		totalAliasMatches += len(matches)
	}
	if totalAliasMatches == 0 {
		t.Fatalf("expected alias matches for real sample; snapshots=%d versions=%d intervals=%d", len(snapshots), len(versions), len(intervals))
	}

	decision := resolveSelectionDecision(selection, indexed)
	if decision.Status == "unresolved" && decision.Reason == "missing_candidate" {
		t.Fatalf("expected sample to resolve or at least move out of missing_candidate; got status=%s reason=%s method=%s snapshots=%d versions=%d intervals=%d alias_matches=%d",
			decision.Status, decision.Reason, decision.Method, len(snapshots), len(versions), len(intervals), totalAliasMatches)
	}
}
