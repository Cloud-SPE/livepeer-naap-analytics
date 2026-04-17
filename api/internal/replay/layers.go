package replay

// Layer names the four boundaries the harness checksums at.
type Layer string

const (
	LayerRaw        Layer = "raw"
	LayerNormalized Layer = "normalized"
	LayerCanonical  Layer = "canonical"
	LayerAPI        Layer = "api"
)

// AllLayers returns the layers in forward pipeline order.
func AllLayers() []Layer {
	return []Layer{LayerRaw, LayerNormalized, LayerCanonical, LayerAPI}
}

// tablesByLayer maps each layer to the set of tables the harness snapshots
// after that layer has been materialised. Additions here expand coverage;
// omissions are deliberate (views, debug surfaces, Kafka engine tables).
//
// Keep alphabetically sorted within each slice so divergence reports list
// tables in a stable order.
var tablesByLayer = map[Layer][]string{
	LayerRaw: {
		"accepted_raw_events",
	},
	LayerNormalized: {
		"normalized_ai_batch_job",
		"normalized_ai_llm_request",
		"normalized_ai_stream_events",
		"normalized_ai_stream_status",
		"normalized_byoc_auth",
		"normalized_byoc_job",
		"normalized_byoc_payment",
		"normalized_network_capabilities",
		"normalized_session_attribution_input_latest_store",
		"normalized_session_event_rollup_latest",
		"normalized_session_orchestrator_observation_rollup_latest",
		"normalized_session_status_hour_rollup",
		"normalized_session_status_rollup_latest",
		"normalized_session_trace_rollup_latest",
		"normalized_stream_trace",
		"normalized_worker_lifecycle",
	},
	// Canonical layer — resolver-written tables that carry the semantic
	// content the API layer serves. Every table here has a refreshed_at /
	// materialized_at column sourced from RunRequest.Now, so a replay with
	// the same fixture + same Now produces byte-identical rows.
	LayerCanonical: {
		"canonical_active_stream_state_latest_store",
		"canonical_ai_batch_job_store",
		"canonical_byoc_job_store",
		"canonical_orch_capability_intervals",
		"canonical_orch_capability_versions",
		"canonical_payment_links_store",
		"canonical_selection_attribution_current",
		"canonical_selection_attribution_decisions",
		"canonical_selection_events",
		"canonical_session_current_store",
		"canonical_session_demand_input_current",
		"canonical_status_hours_store",
		"canonical_status_samples_recent_store",
		"canonical_streaming_demand_hourly_store",
		"canonical_streaming_gpu_metrics_hourly_store",
		"canonical_streaming_sla_hourly_store",
		"canonical_streaming_sla_input_hourly_store",
	},
	// API layer is populated in a later PR once dbt is wired into the
	// pipeline.
	LayerAPI: {},
}

// resolverBookkeepingTables lists the resolver_* tables the harness
// truncates before running the resolver. These are per-run state: keeping
// rows from a prior run can block new claims or contaminate query IDs.
// Deliberately not in LayerCanonical because they hold wall-clock
// timestamps that are not reproducible across runs and would defeat the
// determinism invariant.
var resolverBookkeepingTables = []string{
	"resolver_backfill_runs",
	"resolver_dead_letters",
	"resolver_dirty_orchestrators",
	"resolver_dirty_partitions",
	"resolver_dirty_selection_events",
	"resolver_dirty_sessions",
	"resolver_dirty_windows",
	"resolver_query_event_ids",
	"resolver_query_identities",
	"resolver_query_selection_event_ids",
	"resolver_query_session_keys",
	"resolver_query_window_slices",
	"resolver_repair_requests",
	"resolver_runs",
	"resolver_runtime_state",
	"resolver_window_claims",
}

// TablesForLayer returns the tables the harness snapshots for a given layer.
func TablesForLayer(layer Layer) []string {
	return tablesByLayer[layer]
}
