package resolver

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	resolverRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "resolver_runs_total",
		Help: "Total resolver runs by mode and status.",
	}, []string{"mode", "status"})
	resolverRunDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "resolver_run_duration_seconds",
		Help:    "Resolver run duration by mode.",
		Buckets: prometheus.DefBuckets,
	}, []string{"mode"})
	resolverSelectionEventsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "resolver_selection_events_processed_total",
		Help: "Total selection events processed by the resolver.",
	})
	resolverAttributionDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "resolver_attribution_decisions_total",
		Help: "Selection attribution decisions by status.",
	}, []string{"status"})
	resolverWatermarkUnix = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "resolver_watermark_unix",
		Help: "Resolver watermark timestamp in Unix seconds.",
	})
	resolverDirtyQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resolver_dirty_queue_depth",
		Help: "Resolver dirty queue depth by queue.",
	}, []string{"queue"})
	resolverDirtyPartitionsEnqueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "resolver_dirty_partitions_enqueued_total",
		Help: "Total historical dirty partitions enqueued from accepted raw ingest.",
	})
	resolverDirtyPartitionsRepaired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "resolver_dirty_partitions_repaired_total",
		Help: "Total historical dirty partitions repaired successfully.",
	})
	resolverDirtyWindowsEnqueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "resolver_dirty_windows_enqueued_total",
		Help: "Total same-day dirty windows enqueued from accepted raw ingest.",
	})
	resolverDirtyWindowsRepaired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "resolver_dirty_windows_repaired_total",
		Help: "Total same-day dirty windows repaired successfully.",
	})
	resolverRepairRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "resolver_repair_requests_total",
		Help: "Total resolver repair requests by status transition.",
	}, []string{"status"})
	resolverClaimConflicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "resolver_claim_conflicts_total",
		Help: "Total resolver window-claim conflicts by mode.",
	}, []string{"mode"})
	resolverDirtyScanWatermarkUnix = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "resolver_dirty_scan_watermark_unix",
		Help: "Accepted raw dirty-scan watermark in Unix seconds.",
	})
	resolverSchedulerPhase = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resolver_scheduler_phase",
		Help: "Resolver scheduler phase, with exactly one active phase set to 1.",
	}, []string{"phase"})
)

func setResolverSchedulerPhase(active string) {
	for _, phase := range []string{"bootstrap_backlog", "historical_repair", "historical_repair_wait", "repair_request", "same_day_repair", "same_day_repair_wait", "tail", "idle"} {
		value := 0.0
		if phase == active {
			value = 1
		}
		resolverSchedulerPhase.WithLabelValues(phase).Set(value)
	}
}

func newMetricsServer(port string, state func() schedulerHealthState, configure func(*http.ServeMux)) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		payload := schedulerHealthState{Status: "ok"}
		if state != nil {
			payload = state()
			if payload.Status == "" {
				payload.Status = "ok"
			}
		}
		_ = json.NewEncoder(w).Encode(payload)
	})
	if configure != nil {
		configure(mux)
	}
	return &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
}
