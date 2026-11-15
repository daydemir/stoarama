package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
	}
}

// Exercise production's prepared/cached execution shape with enough populated
// recordings and held contracts to make repeated global-tail work visible.
func TestAccountClipsCursorManyRecordingPreparedPlans(t *testing.T) {
	if os.Getenv("STOARAMA_FEED_PLAN_EVIDENCE") != "1" {
		t.Skip("opt-in local plan evidence")
	}
	pool, window := feedFenceFixture(t)
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE INDEX idx_recording_clips_active ON recording_clips(recording_id,id) WHERE purged_at IS NULL AND released_at IS NULL`,
		`CREATE INDEX idx_recording_clips_recording_started ON recording_clips(recording_id,clip_start_at DESC)`,
		`INSERT INTO recordings(id,account_id,name,delivery,stream_id,cron_timezone)
   SELECT n,CASE WHEN n%4=0 THEN 99 ELSE 47 END,'rec-'||n,'nas_pull',3,'Asia/Tokyo' FROM generate_series(10,137) n`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	// Interleaved IDs, 96 populated owned recordings, substantial retained
	// history on both sides of the cursor, and a genuinely empty final tail.
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at)
  SELECT n,10+n%128,1000,
   CASE WHEN n<=450000 AND n%4<>0 THEN $1::timestamptz ELSE $2::timestamptz END+make_interval(secs=>n::int),
   CASE WHEN n<=450000 AND n%4<>0 THEN $1::timestamptz ELSE $2::timestamptz END+make_interval(secs=>n::int)+interval '1 minute'
  FROM generate_series(1,500000) n`, window.Start, window.End.AddDate(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative)
  SELECT id,CASE WHEN id%5=0 THEN 'nas_collated_hold' ELSE 'managed' END,id%3<>0
  FROM recording_clips WHERE id%5=0 OR id%7=0`); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"recording_clips", "recordings", "streams", "nas_delivery_retirement_windows", "clip_storage_billing_contracts"} {
		if _, err := pool.Exec(ctx, "ANALYZE "+table); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadFile("testdata/account_clips_cursor_joined.sql")
	if err != nil {
		t.Fatal(err)
	}
	type summary struct {
		Plan struct {
			Rows  int64 `json:"Actual Rows"`
			Hits  int64 `json:"Shared Hit Blocks"`
			Reads int64 `json:"Shared Read Blocks"`
		} `json:"Plan"`
		MS float64 `json:"Execution Time"`
	}
	for _, fence := range []string{"ordinary", "fenced"} {
		if fence == "fenced" {
			activateFeedFence(t, pool)
		}
		for _, shape := range []struct {
			cursor int64
			limit  int
		}{{0, 1}, {200000, 500}, {449000, 500}, {490000, 500}} {
			compareFeedQueries(t, pool, 47, shape.cursor, shape.limit)
		}
		for _, cache := range []string{"force_custom_plan", "force_generic_plan", "auto"} {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			func() {
				defer conn.Release()
				tx, err := conn.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				if _, err = tx.Exec(ctx, "SET LOCAL plan_cache_mode="+cache); err != nil {
					t.Fatal(err)
				}
				for _, q := range []struct{ name, sql string }{{"before", string(before)}, {"after", accountClipsCursorSQL}} {

					name := "feed_evidence_" + q.name
					if _, err = tx.Exec(ctx, "PREPARE "+name+"(bigint,bigint,bigint) AS "+q.sql, pgx.QueryExecModeSimpleProtocol); err != nil {
						t.Fatal(err)
					}
					for _, shape := range []struct {
						cursor int64
						limit  int
					}{{0, 1}, {200000, 500}, {449000, 500}, {490000, 500}} {
						var raw []byte
						repeats := 7
						var durations []float64
						for run := 0; run < repeats; run++ {
							sql := fmt.Sprintf("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE %s(47,%d,%d)", name, shape.cursor, shape.limit)
							if err = tx.QueryRow(ctx, sql, pgx.QueryExecModeSimpleProtocol).Scan(&raw); err != nil {
								t.Fatal(err)
							}
							var sample []summary
							if err = json.Unmarshal(raw, &sample); err != nil || len(sample) != 1 {
								t.Fatalf("sample: %v", err)
							}
							durations = append(durations, sample[0].MS)
						}
						var parsed []summary
						if err = json.Unmarshal(raw, &parsed); err != nil || len(parsed) != 1 {
							t.Fatalf("plan: %v", err)
						}
						x := parsed[0]
						sort.Float64s(durations)
						x.MS = durations[len(durations)/2]
						t.Logf("many %s %s %s cursor=%d limit=%d rows=%d blocks=%d median_ms=%.3f", fence, cache, q.name, shape.cursor, shape.limit, x.Plan.Rows, x.Plan.Hits+x.Plan.Reads, x.MS)
						if dir := os.Getenv("STOARAMA_FEED_PLAN_DIR"); dir != "" {
							path := filepath.Join(dir, fmt.Sprintf("many-%s-%s-%s-%d-%d.json", fence, cache, q.name, shape.cursor, shape.limit))
							if err = os.WriteFile(path, raw, 0600); err != nil {
								t.Fatal(err)
							}
						}
					}
					if _, err = tx.Exec(ctx, "DEALLOCATE "+name, pgx.QueryExecModeSimpleProtocol); err != nil {
						t.Fatal(err)
					}
				}
			}()
		}
	}
}

func TestAccountClipsCursorWindowMapPreservesSubsecondClipsAndTimezone(t *testing.T) {
	pool, w := feedFenceFixture(t)
	ctx := context.Background()
	activateFeedFence(t, pool)
	start, end := w.Start, w.End

	for n, span := range [][2]time.Time{
		{start.Add(-time.Second), start.Add(time.Microsecond)},
		{start.Add(-time.Second), start},
		{end, end.Add(time.Second)},
		{end.Add(-time.Microsecond), end.Add(time.Second)},
	} {
		if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at) VALUES($1,1,1000,$2,$3)`, n+1, span[0], span[1]); err != nil {
			t.Fatal(err)
		}
	}
	reference, err := os.ReadFile("testdata/account_clips_cursor_joined.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, zone := range []string{"UTC", "America/New_York", "Asia/Kathmandu"} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `SELECT set_config('TimeZone',$1,true)`, zone); err != nil {
			tx.Rollback(ctx)
			t.Fatal(err)
		}
		var exact bool
		if err = tx.QueryRow(ctx, `SELECT $1::timestamptz=(jsonb_build_array($1::timestamptz)->>0)::timestamptz`, start.Add(123456*time.Microsecond)).Scan(&exact); err != nil || !exact {
			tx.Rollback(ctx)
			t.Fatalf("JSON timestamp precision in %s: %v", zone, err)
		}
		before := clipQueryRows(t, tx, string(reference), 47, 0, 500)
		after := clipQueryRows(t, tx, accountClipsCursorSQL, 47, 0, 500)
		if !reflect.DeepEqual(before, after) || len(after) != 2 {
			tx.Rollback(ctx)
			t.Fatalf("fractional window changed in %s: before=%v after=%v", zone, before, after)
		}
		if err = tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
}
