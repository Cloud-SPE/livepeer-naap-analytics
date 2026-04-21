package replay

import (
	"fmt"
	"sort"
	"strings"
)

// Divergence records the first table per layer where two reports disagree.
// Empty Divergences means the two reports are byte-identical.
type Divergence struct {
	Layer    Layer
	Table    string
	Expected TableRollup
	Actual   TableRollup
}

// Diff compares two reports at the layer+table granularity, returning the
// ordered list of divergences. The ordering follows forward pipeline order
// (raw -> normalized -> canonical -> api) so the caller can surface the
// earliest-layer divergence as the likely root cause.
func Diff(expected, actual *Report) []Divergence {
	if expected == nil || actual == nil {
		return nil
	}
	var out []Divergence
	actByLayer := map[Layer]LayerReport{}
	for _, l := range actual.Layers {
		actByLayer[l.Layer] = l
	}
	for _, want := range expected.Layers {
		got, ok := actByLayer[want.Layer]
		if !ok {
			out = append(out, Divergence{
				Layer: want.Layer,
				Table: "(layer missing)",
			})
			continue
		}
		tables := make([]string, 0, len(want.Tables))
		for t := range want.Tables {
			tables = append(tables, t)
		}
		sort.Strings(tables)
		for _, t := range tables {
			wantRollup := want.Tables[t]
			gotRollup, present := got.Tables[t]
			if !present || wantRollup != gotRollup {
				out = append(out, Divergence{
					Layer:    want.Layer,
					Table:    t,
					Expected: wantRollup,
					Actual:   gotRollup,
				})
			}
		}
	}
	return out
}

// FormatDivergences renders a compact human-readable summary with the
// earliest-layer divergence highlighted first.
func FormatDivergences(divs []Divergence) string {
	if len(divs) == 0 {
		return "replay green: all layers match"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "replay divergence: %d table(s) differ\n", len(divs))
	fmt.Fprintf(&b, "first-divergent layer: %s\n", divs[0].Layer)
	fmt.Fprintf(&b, "first-divergent table: %s.%s\n", "naap", divs[0].Table)
	for _, d := range divs {
		fmt.Fprintf(&b,
			"  [%s] %-60s rows %d -> %d | checksum %s -> %s\n",
			d.Layer, d.Table,
			d.Expected.Rows, d.Actual.Rows,
			short(d.Expected.ArtifactChecksum), short(d.Actual.ArtifactChecksum),
		)
	}
	return b.String()
}

func short(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}
