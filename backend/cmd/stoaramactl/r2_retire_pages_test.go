package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type r2RetireIndexedQueryAudit struct {
	r2RetireQuerier
	t       *testing.T
	queries int
}

func (a *r2RetireIndexedQueryAudit) check(ctx context.Context, sql string, args ...any) {
	a.t.Helper()
	var plan []byte
	explain := "EXPLAIN (FORMAT JSON) "
	inventoryPage := strings.Contains(sql, "FROM nas_inventory_files")
	if inventoryPage {
		explain = "EXPLAIN (ANALYZE, FORMAT JSON) "
	}
	if err := a.r2RetireQuerier.QueryRow(ctx, explain+sql, args...).Scan(&plan); err != nil {
		a.t.Fatal(err)
	}
	var root any
	if err := json.Unmarshal(plan, &root); err != nil {
		a.t.Fatal(err)
	}
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if inventoryPage && x["Relation Name"] == "nas_inventory_files" {
				rows, _ := x["Actual Rows"].(float64)
				removed, _ := x["Rows Removed by Filter"].(float64)
				loops, _ := x["Actual Loops"].(float64)
				if (rows+removed)*loops > r2RetireEvidencePageSize {
					a.t.Fatalf("inventory statement examined more than one page: %s", plan)
				}
			}
			if x["Node Type"] == "Seq Scan" && (x["Relation Name"] == "recording_clips" || x["Relation Name"] == "nas_inventory_files") {
				var indexes string
				_ = a.r2RetireQuerier.QueryRow(ctx, `SELECT coalesce(string_agg(indexname||':'||indexdef,E'\n'),'') FROM pg_indexes WHERE schemaname=current_schema() AND tablename=$1`, x["Relation Name"]).Scan(&indexes)
				a.t.Fatalf("unindexed protected-table query: %s; indexes: %s; sql: %s", plan, indexes, sql)
			}
			for _, v := range x {
				walk(v)
			}
		case []any:
			for _, v := range x {
				walk(v)
			}
		}
	}
	walk(root)
	a.queries++
}
func (a *r2RetireIndexedQueryAudit) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	a.check(ctx, sql, args...)
	return a.r2RetireQuerier.Query(ctx, sql, args...)
}
func (a *r2RetireIndexedQueryAudit) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	a.check(ctx, sql, args...)
	return a.r2RetireQuerier.QueryRow(ctx, sql, args...)
}

func TestR2RetireBoundedEvidencePagesPostgres(t *testing.T) {
	pool := joinedPurgeTestPool(t, "r2_retire_pages")
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `CREATE TABLE recordings(id bigint PRIMARY KEY,account_id bigint);
 CREATE TABLE recording_clips(id bigint PRIMARY KEY,recording_id bigint,bucket text,object_key text,etag text);
 CREATE TABLE nas_inventory_files(connection_id bigint,clip_id bigint,relative_path text,verified_at timestamptz NOT NULL DEFAULT now()-interval '2 days',state text NOT NULL DEFAULT 'present',PRIMARY KEY(connection_id,clip_id));
 INSERT INTO recordings VALUES(1,48),(2,47);
 INSERT INTO recording_clips SELECT i,CASE WHEN i<=5000 THEN 1 ELSE 2 END,'stoarama','managed/'||i,'e' FROM generate_series(1,5002) i;
 INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path) SELECT CASE WHEN i<=50000 THEN 1 ELSE 2 END,i,'p/'||i FROM generate_series(1,50002) i;
 UPDATE nas_inventory_files SET verified_at=now() WHERE clip_id>50000;
 SET LOCAL statement_timeout='30s'; SET LOCAL enable_seqscan=off; SET LOCAL jit=off`)
	if err != nil {
		t.Fatal(err)
	}
	audit := &r2RetireIndexedQueryAudit{r2RetireQuerier: tx, t: t}
	etags, err := r2RetireCatalogETags(ctx, audit, 47, "stoarama")
	if err != nil || len(etags) != 2 || etags["managed/5001"] != "e" || etags["managed/5002"] != "e" {
		t.Fatal("foreign-account page hid later own objects", len(etags), err)
	}
	count := 0
	var path string
	seen := map[string]bool{}
	err = r2RetireInventoryEach(ctx, audit, "n.relative_path", "true", nil, []any{&path}, func() error {
		if seen[path] {
			t.Fatal("duplicate inventory row", path)
		}
		seen[path] = true
		count++
		return nil
	})
	if err != nil || count != 50002 {
		t.Fatal("inventory cursor lost a connection/page", count, err)
	}
	// A whole stale page must neither cause an unbounded scan nor hide the
	// later eligible connection. Excluded projections deliberately contain NULL.
	eligible := map[string]bool{}
	err = r2RetireInventoryEach(ctx, audit, `CASE WHEN n.verified_at >= $1 THEN n.relative_path END`,
		`n.state='present' AND n.verified_at >= $1`, []any{time.Now().Add(-24 * time.Hour)}, []any{&path}, func() error {
			eligible[path] = true
			return nil
		})
	if err != nil || len(eligible) != 2 || !eligible["p/50001"] || !eligible["p/50002"] {
		t.Fatal("excluded page lost later eligible rows", eligible, err)
	}

	// A large ID gap jumps directly to the next key. Saturating the last
	// bigint range must terminate rather than overflow and revisit earlier rows.
	if _, err := tx.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,relative_path) VALUES(3,9223372036854775807,'p/max')`); err != nil {
		t.Fatal(err)
	}
	maxRows := 0
	err = r2RetireInventoryEach(ctx, audit, "n.relative_path", "n.connection_id=3", nil, []any{&path}, func() error {
		if path != "p/max" {
			t.Fatal("wrong sparse page", path)
		}
		maxRows++
		return nil
	})
	if err != nil || maxRows != 1 {
		t.Fatal("sparse/maximum cursor failed", maxRows, err)
	}

	before := audit.queries
	e, err := loadR2RetireEvidence(ctx, audit, r2RetireDeliveryOnly, true)
	if err != nil || len(e.RawPaths) != 0 || audit.queries != before+1 {
		t.Fatal("live apply enumerated evidence", audit.queries-before, err)
	}
}
