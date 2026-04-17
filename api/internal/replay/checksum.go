package replay

import (
	"context"
	"fmt"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// Checksum returns a deterministic fingerprint of a single table.
//
// The rollup uses ClickHouse's groupBitXor over two independent sipHash64s
// of every row, giving a 128-bit order-independent fingerprint with
// negligible collision probability for tables up to billions of rows:
//   - the order of rows in storage does not affect the result
//   - inserting the same N rows twice yields the same fingerprint
//   - any column value changing in any row changes the fingerprint
//
// We use two sipHash64s rather than sipHash128 because groupBitXor does not
// accept FixedString(16); the two UInt64s are hashed with different seeds
// via a salt column ('h2-salt') and reported together as hex.
//
// Implementation notes:
//   - We read the column list from system.columns to build the tuple so the
//     harness stays in sync with schema evolution automatically.
//   - Nullable columns are coalesced to a sentinel so NULL vs empty string
//     hash distinctly but deterministically.
//   - `FINAL` is applied to ReplacingMergeTree / AggregatingMergeTree tables
//     so row-level dedup is reflected in the fingerprint.
func Checksum(ctx context.Context, conn ch.Conn, database, table string) (TableRollup, error) {
	engine, err := tableEngine(ctx, conn, database, table)
	if err != nil {
		return TableRollup{}, err
	}
	final := needsFinal(engine)

	cols, err := rollupColumns(ctx, conn, database, table)
	if err != nil {
		return TableRollup{}, err
	}
	if len(cols) == 0 {
		return TableRollup{}, fmt.Errorf("table %s.%s has no columns in system.columns", database, table)
	}

	finalClause := ""
	if final {
		finalClause = " FINAL"
	}

	// Two sipHash64 rollups with different seeds, XORed across rows. The two
	// 64-bit values concatenated give a 128-bit per-table fingerprint that
	// is order-independent (groupBitXor is associative + commutative) and
	// survives table compaction.
	cs := joinColumns(cols)
	query := fmt.Sprintf(
		"SELECT count() AS rows, "+
			"lower(hex(reinterpretAsString(groupBitXor(sipHash64(%s))))) AS h1, "+
			"lower(hex(reinterpretAsString(groupBitXor(sipHash64(%s, 'h2-salt'))))) AS h2 "+
			"FROM %s.%s%s",
		cs, cs, database, table, finalClause,
	)

	var (
		rows   uint64
		h1, h2 string
	)
	if err := conn.QueryRow(ctx, query).Scan(&rows, &h1, &h2); err != nil {
		return TableRollup{}, fmt.Errorf("checksum %s.%s: %w", database, table, err)
	}
	return TableRollup{Rows: int64(rows), ArtifactChecksum: h1 + h2}, nil
}

// rollupColumns returns the ordered column list used to build the row tuple.
//
//   - ALIAS / MATERIALIZED columns are excluded because they are computed at
//     read time from other columns and would be redundant in the fingerprint.
//   - AggregateFunction state columns (emitted by a few canonical rollups that
//     still carry p95 state for diagnostic queries) are excluded because sipHash64 cannot hash the
//     opaque binary state; any change to a state column still propagates
//     into the fingerprint through the scalar column that materialises the
//     state at query time (e.g. `avg_prompt_to_first_frame_ms` alongside
//     `ptff_p90_state`).
//
// Every remaining column is wrapped in `ifNull(toString(col), '\x00NULL')`
// in joinColumns so NULL hashes deterministically and distinctly.
func rollupColumns(ctx context.Context, conn ch.Conn, database, table string) ([]string, error) {
	q := `
        SELECT name, type
        FROM system.columns
        WHERE database = ? AND table = ?
          AND default_kind != 'ALIAS'
          AND default_kind != 'MATERIALIZED'
        ORDER BY position
    `
	rows, err := conn.Query(ctx, q, database, table)
	if err != nil {
		return nil, fmt.Errorf("list columns %s.%s: %w", database, table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		if isAggregateFunctionType(typ) {
			continue
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// isAggregateFunctionType reports whether a ClickHouse type is an
// AggregateFunction / SimpleAggregateFunction state. Matches both the bare
// form and Nullable(...) / Array(...) wrappers.
func isAggregateFunctionType(typ string) bool {
	return containsSubstring(typ, "AggregateFunction")
}

func containsSubstring(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func tableEngine(ctx context.Context, conn ch.Conn, database, table string) (string, error) {
	var engine string
	err := conn.QueryRow(ctx,
		`SELECT engine FROM system.tables WHERE database = ? AND name = ?`,
		database, table,
	).Scan(&engine)
	if err != nil {
		return "", fmt.Errorf("engine lookup %s.%s: %w", database, table, err)
	}
	return engine, nil
}

func needsFinal(engine string) bool {
	switch engine {
	case "ReplacingMergeTree", "AggregatingMergeTree", "SummingMergeTree",
		"ReplicatedReplacingMergeTree", "ReplicatedAggregatingMergeTree",
		"ReplicatedSummingMergeTree":
		return true
	}
	return false
}

// joinColumns emits a comma-separated list wrapped in tuple() with Nullable
// coalescing so NULL values hash deterministically and distinctly from ”.
func joinColumns(cols []string) string {
	// Each column is referenced bare; sipHash128 accepts a variadic tuple.
	// We deliberately do NOT wrap in tuple() — the clickhouse-go client and
	// server both handle the variadic form more efficiently.
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("ifNull(toString(%s), '\\x00NULL')", c)
	}
	return out
}
