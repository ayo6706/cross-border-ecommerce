//go:build perf

package performance

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// statement is one SQL text the code under test sent, with the arguments of its first call.
type statement struct {
	sql  string
	args []any
}

// statementRecorder is a pgx.QueryTracer that keeps every distinct DML statement sent through the
// traced pool, so the suite explains exactly what production code sends instead of a copy of it.
type statementRecorder struct {
	mu         sync.Mutex
	seen       map[string]bool
	statements []statement
}

func (r *statementRecorder) TraceQueryStart(
	ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData,
) context.Context {
	if !isDML(data.SQL) {
		return ctx
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.seen == nil {
		r.seen = map[string]bool{}
	}
	if !r.seen[data.SQL] {
		r.seen[data.SQL] = true
		r.statements = append(r.statements, statement{sql: data.SQL, args: data.Args})
	}
	return ctx
}

func (*statementRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// take returns the statements recorded since the previous call and starts a new recording.
func (r *statementRecorder) take() []statement {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.statements
	r.statements, r.seen = nil, nil
	return out
}

// isDML skips transaction control and pings: only statements that can touch a table are explained.
// sqlc starts each statement with a "-- name:" comment line, so comment lines are skipped first.
func isDML(sql string) bool {
	for line := range strings.Lines(sql) {
		fields := strings.Fields(line)
		if len(fields) == 0 || strings.HasPrefix(fields[0], "--") {
			continue
		}
		switch strings.ToUpper(fields[0]) {
		case "SELECT", "INSERT", "UPDATE", "DELETE", "WITH":
			return true
		}
		return false
	}
	return false
}

// maxRowsDiscarded bounds the rows one plan node may read and throw away through its filter. An
// index lookup discards few; walking an index to filter a table discards thousands. 5,000 is 5%
// of the smallest seeded large table: measured, the outbox claim-index scan discards ~900 at the
// seeded backlog of 1,000 pending events (the planner switches to primary-key lookups for large
// backlogs), and a primary-key walk for one 500-record page of the 10,000-record run discards
// ~10,000 once idx_raw_records_run_keyset is dropped.
const maxRowsDiscarded = 5000

// planNode is the part of EXPLAIN (FORMAT JSON) the suite reads. Buffers on the root node include
// its children; row counts are per loop.
type planNode struct {
	NodeType            string     `json:"Node Type"`
	RelationName        string     `json:"Relation Name"`
	IndexName           string     `json:"Index Name"`
	ActualLoops         float64    `json:"Actual Loops"`
	RowsRemovedByFilter float64    `json:"Rows Removed by Filter"`
	SharedHit           int64      `json:"Shared Hit Blocks"`
	SharedRead          int64      `json:"Shared Read Blocks"`
	Plans               []planNode `json:"Plans"`
}

type explained struct {
	Plan          planNode `json:"Plan"`
	PlanningTime  float64  `json:"Planning Time"`
	ExecutionTime float64  `json:"Execution Time"`
}

// explain runs EXPLAIN (ANALYZE, BUFFERS) for s in a transaction that is rolled back, so writes
// the analysis executes leave no trace. That plan is the custom plan for s's first arguments.
func explain(ctx context.Context, pool *pgxpool.Pool, s statement) (explained, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return explained{}, fmt.Errorf("begin explain transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var raw []byte
	if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+s.sql, s.args...).Scan(&raw); err != nil {
		return explained{}, fmt.Errorf("explain: %w", err)
	}
	return decodePlan(raw)
}

// explainGeneric returns the generic plan (PostgreSQL 16+), which a prepared statement switches to
// after five executions when it looks no costlier; pgx prepares every statement it caches. The
// statement keeps its $n placeholders, so it goes over the simple protocol, which binds nothing.
func explainGeneric(ctx context.Context, pool *pgxpool.Pool, s statement) (explained, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return explained{}, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	results, err := conn.Conn().PgConn().Exec(ctx, "EXPLAIN (GENERIC_PLAN, FORMAT JSON) "+s.sql).ReadAll()
	if err != nil {
		return explained{}, fmt.Errorf("explain generic plan: %w", err)
	}
	if len(results) != 1 || len(results[0].Rows) != 1 {
		return explained{}, fmt.Errorf("explain generic plan: want one row, got %d results", len(results))
	}
	return decodePlan(results[0].Rows[0][0])
}

func decodePlan(raw []byte) (explained, error) {
	var out []explained
	if err := json.Unmarshal(raw, &out); err != nil {
		return explained{}, fmt.Errorf("decode plan: %w", err)
	}
	if len(out) != 1 {
		return explained{}, fmt.Errorf("decode plan: want 1 plan, got %d", len(out))
	}
	return out[0], nil
}

type access struct {
	relation  string
	node      string
	index     string
	discarded float64 // rows removed by the node's filter, over all loops (ANALYZE plans only)
}

// accesses lists every table read in the plan. A Bitmap Heap Scan takes the index of its
// Bitmap Index Scan child. ModifyTable names the table written, not a read: its rows come from
// the child nodes, which are listed.
func accesses(n planNode) []access {
	var out []access
	if n.RelationName != "" && n.NodeType != "ModifyTable" {
		index := n.IndexName
		if n.NodeType == "Bitmap Heap Scan" {
			index = bitmapIndex(n)
		}
		out = append(out, access{
			relation: n.RelationName, node: n.NodeType, index: index,
			discarded: n.RowsRemovedByFilter * max(n.ActualLoops, 1),
		})
	}
	for _, child := range n.Plans {
		out = append(out, accesses(child)...)
	}
	return out
}

func bitmapIndex(n planNode) string {
	var names []string
	for _, child := range n.Plans {
		if child.IndexName != "" {
			names = append(names, child.IndexName)
		} else {
			names = append(names, bitmapIndex(child))
		}
	}
	return strings.Join(names, "+")
}

// firstLine names a statement in the report: sqlc puts "-- name: X :kind" on the first line.
func firstLine(sql string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(sql), "\n")
	return strings.TrimPrefix(line, "-- name: ")
}

// requireIndexAccess fails when a plan reads a large table other than through an index lookup:
// with no index, or walking an index while its filter discards more than maxRowsDiscarded rows.
// Small tables (sources) are left to the planner: a sequential scan is the right plan there.
func requireIndexAccess(t *testing.T, name string, plan explained, large map[string]bool) {
	t.Helper()
	for _, a := range accesses(plan.Plan) {
		switch {
		case !large[a.relation]:
		case a.index == "":
			t.Errorf("%s: %s on %s (no index); want an index access on tables seeded at scale",
				name, a.node, a.relation)
		case a.discarded > maxRowsDiscarded:
			t.Errorf("%s: %s on %s via %s discarded %.0f rows by filter (max %d); want an index lookup",
				name, a.node, a.relation, a.index, a.discarded, maxRowsDiscarded)
		}
	}
}

func describeAccesses(plan explained) string {
	var parts []string
	for _, a := range accesses(plan.Plan) {
		index := a.index
		if index == "" {
			index = "no index"
		}
		part := fmt.Sprintf("%s %s (%s)", a.node, a.relation, index)
		if a.discarded > 0 {
			part += fmt.Sprintf(", %.0f discarded", a.discarded)
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return "no table"
	}
	return strings.Join(parts, "; ")
}
