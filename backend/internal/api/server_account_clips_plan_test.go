package api

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAccountClipsCursorScopedPagePreservesGlobalOrder(t *testing.T) {
	pool, window := feedFenceFixture(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,display_path,created_at,purged_at)
	 SELECT n,CASE WHEN n%17=0 THEN 5 ELSE 1+n%3 END,1000,$1,$2,'raw/'||n||'.mp4',
	 CASE WHEN n%23=0 THEN now() ELSE now()-interval '1 hour' END,
	 CASE WHEN n%19=0 THEN now() ELSE NULL END FROM generate_series(1,1500) n`, window.Start, window.Start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	// More than a page of early IDs is held. Applying the per-recording limit
	// before the hold predicate would lose later eligible clips or return empty.
	if _, err := pool.Exec(ctx, `INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative)
	 SELECT id,'nas_collated_hold',true FROM recording_clips WHERE id<=600`); err != nil {
		t.Fatal(err)
	}
	for _, fenced := range []bool{false, true} {
		if fenced {
			activateFeedFence(t, pool)
		}
		for _, cursor := range []int64{0, 51, 600, 1499} {
			for _, limit := range []int{1, 7, 500} {
				compareFeedQueries(t, pool, 47, cursor, limit)
			}
		}
	}
}

// Run explicitly under scripts/test-db with STOARAMA_FEED_PLAN_EVIDENCE=1.
// This is a synthetic local database: it never queries production or claims a
// production latency. Both queries use the existing production indexes.
func TestAccountClipsCursorSparseTailPlan(t *testing.T) {
	if os.Getenv("STOARAMA_FEED_PLAN_EVIDENCE") != "1" {
		t.Skip("opt-in local plan evidence")
	}
	pool, window := feedFenceFixture(t)
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE INDEX idx_recording_clips_active ON recording_clips(recording_id,id) WHERE purged_at IS NULL AND released_at IS NULL`,
		`CREATE INDEX idx_recording_clips_recording_started ON recording_clips(recording_id,clip_start_at DESC)`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at)
	 SELECT n,CASE WHEN n%10=0 THEN 1 ELSE 5 END,1000,
	 CASE WHEN n<500 THEN $1::timestamptz ELSE $2::timestamptz END,
	 CASE WHEN n<500 THEN $1::timestamptz ELSE $2::timestamptz END+interval '1 minute'
	 FROM generate_series(1,250000) n`, window.Start, window.End.AddDate(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"recording_clips", "recordings", "streams", "nas_delivery_retirement_windows", "clip_storage_billing_contracts"} {
		if _, err := pool.Exec(ctx, "ANALYZE "+table); err != nil {
			t.Fatal(err)
		}
	}
	joined, err := os.ReadFile("testdata/account_clips_cursor_joined.sql")
	if err != nil {
		t.Fatal(err)
	}
	type planResult struct {
		Plan struct {
			Rows  int64 `json:"Actual Rows"`
			Hits  int64 `json:"Shared Hit Blocks"`
			Reads int64 `json:"Shared Read Blocks"`
		} `json:"Plan"`
		ExecutionMS float64 `json:"Execution Time"`
	}
	for _, mode := range []string{"ordinary", "fenced"} {
		if mode == "fenced" {
			activateFeedFence(t, pool)
		}
		results := make(map[string]planResult)
		for _, item := range []struct{ name, sql string }{{"before", string(joined)}, {"after", accountClipsCursorSQL}} {
			var raw []byte
			if err := pool.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+item.sql, int64(47), int64(200000), 500).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var parsed []planResult
			if err := json.Unmarshal(raw, &parsed); err != nil || len(parsed) != 1 {
				t.Fatalf("plan JSON: %v", err)
			}
			results[item.name] = parsed[0]
			if dir := os.Getenv("STOARAMA_FEED_PLAN_DIR"); dir != "" {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "account-clips-"+mode+"-"+item.name+".json"), raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("%s %s: rows=%d blocks=%d execution_ms=%.3f", mode, item.name, parsed[0].Plan.Rows, parsed[0].Plan.Hits+parsed[0].Plan.Reads, parsed[0].ExecutionMS)
		}
		before, after := results["before"], results["after"]
		if before.Plan.Rows != after.Plan.Rows || (mode == "fenced" && after.Plan.Rows != 0) || (mode == "ordinary" && after.Plan.Rows != 500) {
			t.Fatal("eligible tail result changed")
		}
		if mode == "fenced" && after.Plan.Hits+after.Plan.Reads >= before.Plan.Hits+before.Plan.Reads {
			t.Fatal("scoped query did not avoid global tail work in the sparse-tail fixture")
		}
	}
}
