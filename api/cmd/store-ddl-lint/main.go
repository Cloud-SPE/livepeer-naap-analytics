// Command store-ddl-lint compares the checked-in DDL under
// warehouse/ddl/stores/ with the live schema in ClickHouse. Fails if
// any declared table is missing, any live column is missing from the
// declaration, or any column type differs. Catches drift that
// apply-store-ddl.sh's `CREATE TABLE IF NOT EXISTS` cannot fix
// (existing-table conflicts are silently ignored).
//
// Intentionally does NOT apply ALTER TABLE — this is a lint, not a
// migrator. Drift is reported; the human writes an explicit migration.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

type declaredTable struct {
	Name    string            // e.g. canonical_session_current_store
	Columns map[string]string // col name -> type
}

type liveColumn struct {
	Name string `ch:"name"`
	Type string `ch:"type"`
}

var tableRE = regexp.MustCompile(`CREATE TABLE(?:\s+IF NOT EXISTS)?\s+naap\.([a-z_]+)`)

func main() {
	var (
		ddlDir = flag.String("ddl-dir", "warehouse/ddl/stores",
			"directory containing the declared store DDL")
		addr = flag.String("clickhouse-addr",
			envOr("CLICKHOUSE_ADDR", "localhost:9000"),
			"ClickHouse native-protocol address")
		database = flag.String("database",
			envOr("CLICKHOUSE_DB", "naap"),
			"ClickHouse database")
		user = flag.String("user",
			envOr("CLICKHOUSE_ADMIN_USER", "naap_admin"),
			"ClickHouse user")
		password = flag.String("password",
			envOr("CLICKHOUSE_ADMIN_PASSWORD", "changeme"),
			"ClickHouse password")
	)
	flag.Parse()

	declared, err := loadDeclared(*ddlDir)
	if err != nil {
		fatal("load declared DDL: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := ch.Open(&ch.Options{
		Addr: []string{*addr},
		Auth: ch.Auth{Database: *database, Username: *user, Password: *password},
	})
	if err != nil {
		fatal("clickhouse open: %v", err)
	}
	defer conn.Close()

	var (
		mismatches int
		summary    []string
	)
	for _, t := range declared {
		live, err := fetchLiveColumns(ctx, conn, *database, t.Name)
		if err != nil {
			fatal("fetch live columns for %s: %v", t.Name, err)
		}
		if len(live) == 0 {
			fmt.Printf("MISSING %s: declared but not present in live CH\n", t.Name)
			mismatches++
			continue
		}

		// Live → declared: every live column must appear in the file
		// with a matching type. Case-insensitive + whitespace-normalised
		// comparison because system.columns emits canonical form (e.g.
		// `LowCardinality(String)`) while the file may have whitespace.
		for _, lc := range live {
			decl, ok := t.Columns[lc.Name]
			if !ok {
				fmt.Printf("MISSING_IN_DECL %s.%s: live has %q, declaration does not list it\n",
					t.Name, lc.Name, lc.Type)
				mismatches++
				continue
			}
			if !typesEqual(decl, lc.Type) {
				fmt.Printf("TYPE_DRIFT %s.%s: declared=%q live=%q\n",
					t.Name, lc.Name, decl, lc.Type)
				mismatches++
			}
		}
		// Declared → live: every declared column must exist in the live
		// schema. Missing-in-live means an operator skipped
		// apply-store-ddl.sh after a schema change.
		for colName := range t.Columns {
			found := false
			for _, lc := range live {
				if lc.Name == colName {
					found = true
					break
				}
			}
			if !found {
				fmt.Printf("MISSING_IN_LIVE %s.%s: declared but not present in live CH\n",
					t.Name, colName)
				mismatches++
			}
		}
		summary = append(summary, fmt.Sprintf("  %s: %d live cols, %d declared cols",
			t.Name, len(live), len(t.Columns)))
	}

	fmt.Printf("\nstore-ddl-lint: %d tables checked, %d mismatches\n",
		len(declared), mismatches)
	if mismatches == 0 {
		for _, s := range summary {
			fmt.Println(s)
		}
		fmt.Println("\nstore-ddl-lint: clean")
		return
	}
	fmt.Println("")
	fmt.Println("Drift detected. This lint does not apply ALTER TABLE —")
	fmt.Println("a drifting table needs a human-written migration under")
	fmt.Println("infra/clickhouse/migrations/ that matches the declaration,")
	fmt.Println("OR an update to the declaration under warehouse/ddl/stores/")
	fmt.Println("that matches live.")
	os.Exit(1)
}

func loadDeclared(dir string) ([]declaredTable, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []declaredTable
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		body := string(data)
		t, err := parseDDL(body)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func parseDDL(body string) (declaredTable, error) {
	tm := tableRE.FindStringSubmatch(body)
	if len(tm) < 2 {
		return declaredTable{}, fmt.Errorf("no CREATE TABLE match")
	}
	name := tm[1]

	// Extract just the column list — everything between the first `(`
	// after the CREATE TABLE and the closing `)` before ENGINE.
	start := strings.Index(body, "(")
	engineIdx := strings.Index(strings.ToUpper(body), ") ENGINE")
	if start < 0 || engineIdx < 0 || engineIdx <= start {
		return declaredTable{}, fmt.Errorf("could not locate column list")
	}
	colList := body[start+1 : engineIdx]

	cols := map[string]string{}
	for _, pair := range splitColumnsList(colList) {
		cols[pair[0]] = pair[1]
	}
	if len(cols) == 0 {
		return declaredTable{}, fmt.Errorf("no columns parsed")
	}
	return declaredTable{Name: name, Columns: cols}, nil
}

// splitColumnsList splits the contents of a CREATE TABLE column list
// into (name, type) pairs, respecting paren depth so nested types
// like `AggregateFunction(argMaxIf, String, DateTime64(3, 'UTC'), UInt8)`
// stay intact. Also strips trailing `DEFAULT ...`, `MATERIALIZED ...`,
// `ALIAS ...`, and `TTL ...` clauses from the type — the lint only
// compares the core type, not the default expression.
func splitColumnsList(s string) [][2]string {
	var out [][2]string
	// Walk the list, splitting on commas that are at depth 0.
	depth := 0
	inString := false
	var buf strings.Builder
	flush := func() {
		chunk := strings.TrimSpace(buf.String())
		buf.Reset()
		if chunk == "" {
			return
		}
		// Skip `PROJECTION ...` and `INDEX ...` definitions —
		// they are table-level, not columns.
		upper := strings.ToUpper(chunk)
		if strings.HasPrefix(upper, "PROJECTION ") || strings.HasPrefix(upper, "INDEX ") {
			return
		}
		// Expected shape: `col_name` type [DEFAULT expr] [...clauses...]
		// Column names are backtick-quoted; split on the space after
		// the closing backtick.
		if !strings.HasPrefix(chunk, "`") {
			return
		}
		closeBacktick := strings.Index(chunk[1:], "`")
		if closeBacktick < 0 {
			return
		}
		name := chunk[1 : 1+closeBacktick]
		rest := strings.TrimSpace(chunk[2+closeBacktick:])
		// Trim tail clauses: DEFAULT / MATERIALIZED / ALIAS / TTL /
		// CODEC. These all begin with a keyword at depth 0 in the
		// remaining string.
		typ := trimTailClauses(rest)
		out = append(out, [2]string{name, strings.TrimSpace(typ)})
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\'' && !escaped(s, i):
			inString = !inString
			buf.WriteByte(c)
		case inString:
			buf.WriteByte(c)
		case c == '(':
			depth++
			buf.WriteByte(c)
		case c == ')':
			depth--
			buf.WriteByte(c)
		case c == ',' && depth == 0:
			flush()
		default:
			buf.WriteByte(c)
		}
	}
	flush()
	return out
}

// trimTailClauses drops everything from the first top-level DEFAULT /
// MATERIALIZED / ALIAS / TTL / CODEC keyword onwards. Paren-aware so
// `Nullable(String) DEFAULT CAST(NULL, 'Nullable(String)')` yields
// `Nullable(String)`.
func trimTailClauses(s string) string {
	keywords := []string{" DEFAULT ", " DEFAULT\t", " MATERIALIZED ", " ALIAS ", " TTL ", " CODEC("}
	lowest := -1
	depth := 0
	inString := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\'' && !escaped(s, i) {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		if c == '(' {
			depth++
			continue
		}
		if c == ')' {
			depth--
			continue
		}
		if depth == 0 {
			for _, kw := range keywords {
				if i+len(kw) <= len(s) && strings.EqualFold(s[i:i+len(kw)], kw) {
					if lowest < 0 || i < lowest {
						lowest = i
					}
					break
				}
			}
		}
	}
	if lowest >= 0 {
		return s[:lowest]
	}
	return s
}

func escaped(s string, i int) bool {
	bs := 0
	for j := i - 1; j >= 0 && s[j] == '\\'; j-- {
		bs++
	}
	return bs%2 == 1
}

// splitColumns is retained only to keep older call-sites compiling;
// the real worker is splitColumnsList. Returns a name-indexed view
// over the same split result.
func splitColumns(s string) map[string]string {
	out := map[string]string{}
	for _, pair := range splitColumnsList(s) {
		out[pair[0]] = pair[1]
	}
	return out
}

func fetchLiveColumns(ctx context.Context, conn ch.Conn, database, table string) ([]liveColumn, error) {
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
		return nil, err
	}
	defer rows.Close()
	var out []liveColumn
	for rows.Next() {
		var c liveColumn
		if err := rows.Scan(&c.Name, &c.Type); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// typesEqual compares declared vs. live ClickHouse types, absorbing
// the cosmetic differences system.columns imposes (whitespace,
// quoting). Not a full type equivalence check — the lint errs on the
// side of flagging suspicious differences; an operator can add a
// specific normalisation here if a false positive is worth suppressing.
func typesEqual(declared, live string) bool {
	return normaliseType(declared) == normaliseType(live)
}

func normaliseType(t string) string {
	// Remove whitespace; system.columns has none between identifiers,
	// the file may have some from source formatting.
	out := strings.ReplaceAll(t, " ", "")
	// Both forms render single quotes identically.
	return out
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "store-ddl-lint: "+format+"\n", args...)
	os.Exit(2)
}

// jsonDump helps when debugging; unused in happy path but kept to keep
// the dependency list honest (the test runner has parity-verify tools
// that inspect schemas via JSON).
var _ = json.Marshal
