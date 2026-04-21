// Package replay implements the layer-by-layer replay harness for the
// medallion pipeline: raw -> normalized -> canonical -> api.
//
// The harness drives a pinned raw-event fixture through every layer
// boundary and records a deterministic artifact_checksum rollup per
// table. A second run over the same fixture must produce byte-identical
// rollups; divergence is localised to the first layer whose rollup
// changes.
//
// PR 1 scope: raw -> normalized only. Later PRs extend the pipeline to
// canonical (resolver) and api (dbt) layers.
package replay
