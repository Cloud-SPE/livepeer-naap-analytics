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
	//
	// Phase 1 of serving-layer-v2 moved the SLA scored rollup out of the
	// canonical layer and into the API layer as
	// `api_hourly_streaming_sla_store` — it is checksummed under LayerAPI
	// now, as the physical backing for the api_hourly_streaming_sla view.
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
		// Phase 2: resolver pre-computes daily SLA benchmark cohort here.
		"canonical_sla_benchmark_daily_store",
		"canonical_status_hours_store",
		"canonical_status_samples_recent_store",
		"canonical_streaming_demand_hourly_store",
		"canonical_streaming_gpu_metrics_hourly_store",
		"canonical_streaming_sla_input_hourly_store",
	},
	// API layer — the serving contract. Every entry here is a dbt-managed
	// model under warehouse/models/api/. All are views over canonical_*
	// state; a deterministic canonical snapshot + deterministic view
	// definitions => deterministic api rows. The legacy api_base_* tier
	// was retired in Phase 5 — scoring runs inline in the resolver and
	// writes to api_hourly_streaming_sla_store directly.
	LayerAPI: {
		"api_current_active_stream_state",
		"api_fact_ai_batch_job",
		"api_fact_ai_batch_llm_request",
		"api_fact_byoc_job",
		"api_hourly_byoc_auth",
		"api_hourly_request_demand",
		"api_hourly_streaming_demand",
		"api_hourly_streaming_gpu_metrics",
		"api_hourly_streaming_sla",
		// Phase 1: physical backing of api_hourly_streaming_sla. The
		// view is a thin latest-slice reader; the row data lives here.
		"api_hourly_streaming_sla_store",
		"api_observed_byoc_worker",
		"api_observed_capability_hardware",
		"api_observed_capability_offer",
		"api_observed_capability_pricing",
		"api_observed_orchestrator",
		"api_orchestrator_identity",
	},
}

// resolverWrittenApiStores lists the api_*_store tables the resolver
// writes to. These are physical tables whose canonical declaration lives
// under warehouse/ddl/stores/, but they back an api_* view (via the
// latest-slice alias pattern) rather than serving as canonical_* rollups.
//
// Before each canonical phase the harness must truncate these alongside
// the canonical_*_store tables — otherwise a prior run's rows would
// accumulate and break determinism. They are NOT in LayerCanonical (where
// truncateLayers(LayerCanonical) runs) because they are checksummed as
// part of LayerAPI; keeping the two concerns separate lets the table
// categorization reflect the API contract, not the data-authorship story.
//
// Phase 1 added api_hourly_streaming_sla_store. Subsequent phases that
// promote other canonical stores into the api layer will append to this
// list in lockstep with the rename.
var resolverWrittenApiStores = []string{
	"api_hourly_streaming_sla_store",
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
