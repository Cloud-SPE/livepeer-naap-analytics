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
	// Canonical and API layers are populated in later PRs that extend the
	// pipeline to drive the resolver and dbt respectively.
	LayerCanonical: {},
	LayerAPI:       {},
}

// TablesForLayer returns the tables the harness snapshots for a given layer.
func TablesForLayer(layer Layer) []string {
	return tablesByLayer[layer]
}
