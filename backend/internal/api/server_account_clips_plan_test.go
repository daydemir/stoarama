package api

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// Inspect every executed index leaf, including BitmapAnd children. A scoped
// heap access can still hide repeated scans of the global primary key.
func requireFencedRecordingAccess(t *testing.T, raw []byte) {
	t.Helper()
	type access struct {
		Kind     string   `json:"Node Type"`
		Relation string   `json:"Relation Name"`
		Index    string   `json:"Index Name"`
		Cond     string   `json:"Index Cond"`
		Loops    float64  `json:"Actual Loops"`
		Rows     int64    `json:"Actual Rows"`
		Removed  int64    `json:"Rows Removed by Filter"`
		Children []access `json:"Plans"`
	}
	var plans []struct{ Plan access }
	if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
		t.Fatalf("fenced access plan: %v", err)
	}
	seen := 0
	idColumn := regexp.MustCompile(`\bid\b`)
	var visit func(access)
	visit = func(n access) {
		if n.Loops > 0 {
			if n.Relation == "recording_clips" && n.Kind == "Seq Scan" {
				t.Fatal("fenced feed scanned all recording clips")
			}
			if strings.Contains(n.Index, "recording_clips") {
				seen++
				point := n.Index == "recording_clips_pkey" && regexp.MustCompile(`\bid\s*=`).MatchString(n.Cond)
				emptyTime := n.Index == "idx_recording_clips_recording_started" && n.Rows == 0 && n.Removed == 0
				scopedCursor := n.Index == "idx_recording_clips_active" && strings.Contains(n.Cond, "recording_id") && idColumn.MatchString(n.Cond)
				if !point && !emptyTime && !scopedCursor && (n.Index != "idx_recording_clips_active_window" || !strings.Contains(n.Cond, "recording_id") || !idColumn.MatchString(n.Cond) || !strings.Contains(n.Cond, "clip_start_at") || !strings.Contains(n.Cond, "clip_end_at")) {
					t.Fatalf("fenced feed scanned an unscoped index: %s %s; plan=%s", n.Index, n.Cond, raw)
				}
			}
		}
		for _, child := range n.Children {
			visit(child)
		}
	}
	visit(plans[0].Plan)
	if seen == 0 {
		t.Fatal("fixture did not exercise a fenced clip access")
	}
}

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
// production latency. The candidate uses the separately precreated covering index.
func seedFeedRetiredHistory(t *testing.T, pool *pgxpool.Pool, n int, many bool, start time.Time) {
	t.Helper()
	recording := "CASE WHEN n%10=0 THEN 1 ELSE 5 END"
	if many {
		recording = "10+n%128"
	}
	sql := `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,display_path,released_at)
 SELECT 1000000+n,` + recording + `,1000,$2::timestamptz-interval '1 year',$2::timestamptz-interval '1 year'+interval '1 minute',repeat(md5(n::text),28),now() FROM generate_series(1,$1::int)n`
	if _, err := pool.Exec(context.Background(), sql, n, start); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), "VACUUM (ANALYZE) recording_clips"); err != nil {
		t.Fatal(err)
	}
	var facts []byte
	if err := pool.QueryRow(context.Background(), `SELECT json_build_object('rows',reltuples,'pages',relpages,'allvisible',relallvisible,'bytes',pg_relation_size(oid)) FROM pg_class WHERE oid='recording_clips'::regclass`).Scan(&facts); err != nil {
		t.Fatal(err)
	}
	t.Logf("LOCAL physical fixture: %s", facts)
}

func TestAccountClipsCursorSparseTailPlan(t *testing.T) {
	if os.Getenv("STOARAMA_FEED_PLAN_EVIDENCE") != "1" {
		t.Skip("opt-in local plan evidence")
	}
	pool, window := feedFenceFixture(t)
	ctx := context.Background()
	for _, sql := range []string{
		`CREATE INDEX idx_recording_clips_active ON recording_clips(recording_id,id) WHERE purged_at IS NULL AND released_at IS NULL`,
		`CREATE INDEX idx_recording_clips_recording_started ON recording_clips(recording_id,clip_start_at DESC)`,
		`CREATE INDEX idx_recording_clips_active_window ON recording_clips(recording_id,id,clip_start_at,clip_end_at,size_bytes,created_at) WHERE purged_at IS NULL AND released_at IS NULL`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,display_path)
	 SELECT n,CASE WHEN n%10=0 THEN 1 ELSE 5 END,1000,
	 CASE WHEN n<500 THEN $1::timestamptz ELSE $2::timestamptz END,
	 CASE WHEN n<500 THEN $1::timestamptz ELSE $2::timestamptz END+interval '1 minute',repeat(md5(n::text),28)
	 FROM generate_series(1,250000) n`, window.Start, window.End.AddDate(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	seedFeedRetiredHistory(t, pool, 750000, false, window.Start)
	for _, table := range []string{"recording_clips", "recordings", "streams", "nas_delivery_retirement_windows", "clip_storage_billing_contracts"} {
		if _, err := pool.Exec(ctx, "ANALYZE "+table); err != nil {
			t.Fatal(err)
		}
	}
	joined, err := os.ReadFile("testdata/account_clips_cursor_9ec.sql")
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
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		candidate, err := accountClipsQuery(ctx, tx, 47)
		if err != nil {
			t.Fatal(err)
		}
		results := make(map[string]planResult)
		for _, item := range []struct{ name, sql string }{{"before", string(joined)}, {"after", candidate}} {
			var raw []byte
			if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+item.sql, int64(47), int64(200000), 500).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if mode == "fenced" && item.name == "after" {
				requireFencedRecordingAccess(t, raw)
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
		`CREATE INDEX idx_recording_clips_active_window ON recording_clips(recording_id,id,clip_start_at,clip_end_at,size_bytes,created_at) WHERE purged_at IS NULL AND released_at IS NULL`,
		`INSERT INTO recordings(id,account_id,name,delivery,stream_id,cron_timezone)
   SELECT n,CASE WHEN n%4=0 THEN 99 ELSE 47 END,'rec-'||n,'nas_pull',3,'Asia/Tokyo' FROM generate_series(10,137) n`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	// Interleaved IDs, 96 populated owned recordings, substantial retained
	// history on both sides of the cursor, and a genuinely empty final tail.
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,display_path)
  SELECT n,10+n%128,1000,
   CASE WHEN n<=450000 AND n%4<>0 THEN $1::timestamptz ELSE $2::timestamptz END+make_interval(secs=>n::int),
   CASE WHEN n<=450000 AND n%4<>0 THEN $1::timestamptz ELSE $2::timestamptz END+make_interval(secs=>n::int)+interval '1 minute',repeat(md5(n::text),28)
  FROM generate_series(1,500000) n`, window.Start, window.End.AddDate(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative)
  SELECT id,CASE WHEN id%5=0 THEN 'nas_collated_hold' ELSE 'managed' END,id%3<>0
  FROM recording_clips WHERE id%5=0 OR id%7=0`); err != nil {
		t.Fatal(err)
	}
	seedFeedRetiredHistory(t, pool, 1500000, true, window.Start)
	for _, table := range []string{"recording_clips", "recordings", "streams", "nas_delivery_retirement_windows", "clip_storage_billing_contracts"} {
		if _, err := pool.Exec(ctx, "ANALYZE "+table); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadFile("testdata/account_clips_cursor_9ec.sql")
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
				candidate, err := accountClipsQuery(ctx, tx, 47)
				if err != nil {
					t.Fatal(err)
				}
				for _, q := range []struct{ name, sql string }{{"before", string(before)}, {"after", candidate}} {

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
							if fence == "fenced" && q.name == "after" {
								requireFencedRecordingAccess(t, raw)
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
							if err = os.MkdirAll(dir, 0700); err != nil {
								t.Fatal(err)
							}
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
	reference, err := os.ReadFile("testdata/account_clips_cursor_9ec.sql")
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

// A cold/non-all-visible heap must not be consulted for an outside-window tail.
// Growing that tail fourfold must grow index work, not payload heap reads.
func TestAccountClipsCursorOutsideWindowNonvisibleHeap(t *testing.T) {
	previousBlocks := int64(0)
	for _, n := range []int{25000, 100000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			pool, window := feedFenceFixture(t)
			ctx := context.Background()
			for _, sql := range []string{
				`ALTER TABLE recording_clips SET (autovacuum_enabled=false)`,
				`DELETE FROM recording_clips`,
				`CREATE INDEX idx_recording_clips_active ON recording_clips(recording_id,id) WHERE purged_at IS NULL AND released_at IS NULL`,
				`CREATE INDEX idx_recording_clips_recording_started ON recording_clips(recording_id,clip_start_at DESC)`,
				`CREATE INDEX idx_recording_clips_active_window ON recording_clips(recording_id,id,clip_start_at,clip_end_at,size_bytes,created_at) WHERE purged_at IS NULL AND released_at IS NULL`,
			} {
				if _, err := pool.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,display_path)
   SELECT n,CASE WHEN n%10=0 THEN 1 ELSE 5 END,1000,$2::timestamptz,$2::timestamptz+interval '1 minute',repeat(md5(n::text),28)
   FROM generate_series(1,$1::int)n`, n, window.End.AddDate(0, 0, 1)); err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"recording_clips", "recordings", "streams", "nas_delivery_retirement_windows", "clip_storage_billing_contracts"} {
				if _, err := pool.Exec(ctx, "ANALYZE "+table); err != nil {
					t.Fatal(err)
				}
			}
			activateFeedFence(t, pool)
			var visible int64
			if err := pool.QueryRow(ctx, `SELECT relallvisible FROM pg_class WHERE oid='recording_clips'::regclass`).Scan(&visible); err != nil || visible != 0 {
				t.Fatalf("nonvisible fixture: %d %v", visible, err)
			}
			before, err := os.ReadFile("testdata/account_clips_cursor_9ec.sql")
			if err != nil {
				t.Fatal(err)
			}
			tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			candidate, err := accountClipsQuery(ctx, tx, 47)
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range []struct{ name, sql string }{{"before", string(before)}, {"after", candidate}} {
				var raw []byte
				if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) "+item.sql, int64(47), int64(n/2), 500).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				var plan []struct {
					Plan struct {
						Rows  int64 `json:"Actual Rows"`
						Hits  int64 `json:"Shared Hit Blocks"`
						Reads int64 `json:"Shared Read Blocks"`
					}
					MS float64 `json:"Execution Time"`
				}
				if err := json.Unmarshal(raw, &plan); err != nil || len(plan) != 1 {
					t.Fatal(err)
				}
				if plan[0].Plan.Rows != 0 {
					t.Fatal("outside-window rows became eligible")
				}
				blocks := plan[0].Plan.Hits + plan[0].Plan.Reads
				if item.name == "after" {
					requireFencedRecordingAccess(t, raw)
					type node struct {
						Kind     string  `json:"Node Type"`
						Relation string  `json:"Relation Name"`
						Loops    float64 `json:"Actual Loops"`
						Heap     int64   `json:"Heap Fetches"`
						Rows     int64   `json:"Actual Rows"`
						Removed  int64   `json:"Rows Removed by Filter"`
						Children []node  `json:"Plans"`
					}
					var tree []struct{ Plan node }
					if err := json.Unmarshal(raw, &tree); err != nil {
						t.Fatal(err)
					}
					indexOnly := 0
					var visit func(node)
					visit = func(x node) {
						if x.Relation == "recording_clips" && x.Loops > 0 {
							if (x.Kind != "Index Only Scan" && !(x.Rows == 0 && x.Removed == 0)) || x.Heap != 0 {
								t.Fatalf("outside-window heap access: %+v", x)
							}
							indexOnly++
						}
						for _, c := range x.Children {
							visit(c)
						}
					}
					visit(tree[0].Plan)
					if indexOnly == 0 {
						t.Fatal("fixture did not execute covering access")
					}
					if previousBlocks > 0 && blocks > previousBlocks*2+64 {
						t.Fatalf("outside-window index work grew disproportionately: %d -> %d", previousBlocks, blocks)
					}
					previousBlocks = blocks
				}
				if dir := os.Getenv("STOARAMA_FEED_PLAN_DIR"); dir != "" {
					if err := os.MkdirAll(dir, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("nonvisible-%d-%s.json", n, item.name)), raw, 0600); err != nil {
						t.Fatal(err)
					}
				}
				t.Logf("nonvisible n=%d %s rows=%d blocks=%d execution_ms=%.3f", n, item.name, plan[0].Plan.Rows, blocks, plan[0].MS)
			}
		})
	}
}

// A large eligible backlog with no visibility-map credit must still read only a
// bounded prefix. This catches sorting every kept ID before returning limit=1.
func TestAccountClipsCursorEligibleNonvisibleHeap(t *testing.T) {
	sizes := []int{25000, 100000}
	if os.Getenv("STOARAMA_FEED_PLAN_EVIDENCE") == "1" {
		sizes = append(sizes, 2000000)
	}
	for _, n := range sizes {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			pool, w := feedFenceFixture(t)
			ctx := context.Background()
			for _, sql := range []string{
				`ALTER TABLE recording_clips SET (autovacuum_enabled=false)`,
				`CREATE INDEX idx_recording_clips_active ON recording_clips(recording_id,id) WHERE purged_at IS NULL AND released_at IS NULL`,
				`CREATE INDEX idx_recording_clips_recording_started ON recording_clips(recording_id,clip_start_at DESC)`,
				`CREATE INDEX idx_recording_clips_active_window ON recording_clips(recording_id,id,clip_start_at,clip_end_at,size_bytes,created_at) WHERE purged_at IS NULL AND released_at IS NULL`,
				`INSERT INTO recordings(id,account_id,name,delivery,stream_id,cron_timezone) SELECT n,CASE WHEN n%4=0 THEN 99 ELSE 47 END,'rec-'||n,'nas_pull',3,'Asia/Tokyo' FROM generate_series(10,137)n`,
			} {
				if _, err := pool.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,clip_start_at,clip_end_at,display_path) SELECT n,10+n%128,1000,CASE WHEN n<=$1::int*3/4 THEN $2::timestamptz+make_interval(secs=>n%86400) ELSE $3::timestamptz END,CASE WHEN n<=$1::int*3/4 THEN $2::timestamptz+make_interval(secs=>n%86400) ELSE $3::timestamptz END+interval '1 minute',repeat(md5(n::text),28) FROM generate_series(1,$1::int)n`, n, w.Start, w.End.AddDate(0, 0, 1)); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO clip_storage_billing_contracts(clip_id,mode,authoritative) SELECT id,'nas_collated_hold',true FROM recording_clips WHERE id%5=0`); err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{"recording_clips", "recordings", "streams", "clip_storage_billing_contracts", "nas_delivery_retirement_windows"} {
				if _, err := pool.Exec(ctx, "ANALYZE "+table); err != nil {
					t.Fatal(err)
				}
			}
			activateFeedFence(t, pool)
			var visible int64
			if err := pool.QueryRow(ctx, `SELECT relallvisible FROM pg_class WHERE oid='recording_clips'::regclass`).Scan(&visible); err != nil || visible != 0 {
				t.Fatalf("nonvisible fixture: %d %v", visible, err)
			}
			before, err := os.ReadFile("testdata/account_clips_cursor_9ec.sql")
			if err != nil {
				t.Fatal(err)
			}
			for _, cache := range []string{"force_custom_plan", "force_generic_plan"} {
				tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				if _, err = tx.Exec(ctx, "SET LOCAL plan_cache_mode="+cache); err != nil {
					t.Fatal(err)
				}
				candidate, err := accountClipsQuery(ctx, tx, 47)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = tx.Exec(ctx, "PREPARE positive_page(bigint,bigint,bigint) AS "+candidate, pgx.QueryExecModeSimpleProtocol); err != nil {
					t.Fatal(err)
				}
				for _, shape := range []struct {
					cursor int64
					limit  int
				}{{0, 1}, {0, 500}, {int64(n / 2), 1}, {int64(n / 2), 500}, {int64(n * 7 / 8), 500}} {
					old := clipQueryRows(t, tx, string(before), 47, shape.cursor, shape.limit)
					current := clipQueryRows(t, tx, candidate, 47, shape.cursor, shape.limit)
					expected := shape.limit
					if shape.cursor > int64(n*3/4) {
						expected = 0
					}
					if !reflect.DeepEqual(old, current) || len(current) != expected {
						t.Fatal("eligible page changed")
					}
					var raw []byte
					if err = tx.QueryRow(ctx, fmt.Sprintf("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) EXECUTE positive_page(47,%d,%d)", shape.cursor, shape.limit), pgx.QueryExecModeSimpleProtocol).Scan(&raw); err != nil {
						t.Fatal(err)
					}
					type node struct {
						Kind     string  `json:"Node Type"`
						Relation string  `json:"Relation Name"`
						Index    string  `json:"Index Name"`
						Rows     float64 `json:"Actual Rows"`
						Loops    float64 `json:"Actual Loops"`
						Removed  float64 `json:"Rows Removed by Filter"`
						Heap     float64 `json:"Heap Fetches"`
						Hits     int64   `json:"Shared Hit Blocks"`
						Reads    int64   `json:"Shared Read Blocks"`
						Temp     int64   `json:"Temp Written Blocks"`
						Children []node  `json:"Plans"`
					}
					var plan []struct {
						Plan node
						MS   float64 `json:"Execution Time"`
					}
					if err = json.Unmarshal(raw, &plan); err != nil {
						t.Fatal(err)
					}
					tuples, heaps := float64(0), float64(0)
					var visit func(node)
					visit = func(x node) {
						if x.Relation == "recording_clips" && x.Index != "recording_clips_pkey" {
							tuples += (x.Rows + x.Removed) * x.Loops
							if x.Kind == "Index Scan" {
								heaps += (x.Rows + x.Removed) * x.Loops
							} else {
								heaps += x.Heap
							}
						}
						for _, c := range x.Children {
							visit(c)
						}
					}
					visit(plan[0].Plan)
					budget := float64(4*shape.limit + 4*128)
					if tuples > budget || heaps > budget || plan[0].Plan.Temp != 0 || plan[0].Plan.Hits+plan[0].Plan.Reads > int64(4096+24*shape.limit) {
						t.Fatalf("unbounded positive page n=%d cache=%s cursor=%d limit=%d tuples=%v heap=%v blocks=%d temp=%d", n, cache, shape.cursor, shape.limit, tuples, heaps, plan[0].Plan.Hits+plan[0].Plan.Reads, plan[0].Plan.Temp)
					}
					if dir := os.Getenv("STOARAMA_FEED_PLAN_DIR"); dir != "" {
						if err = os.MkdirAll(dir, 0700); err != nil {
							t.Fatal(err)
						}
						if err = os.WriteFile(filepath.Join(dir, fmt.Sprintf("positive-nonvisible-%d-%s-%d-%d.json", n, cache, shape.cursor, shape.limit)), raw, 0600); err != nil {
							t.Fatal(err)
						}
					}
					t.Logf("positive nonvisible n=%d %s cursor=%d limit=%d candidate_tuples=%.0f candidate_heap_visits=%.0f blocks=%d ms=%.3f", n, cache, shape.cursor, shape.limit, tuples, heaps, plan[0].Plan.Hits+plan[0].Plan.Reads, plan[0].MS)
				}
				if _, err = tx.Exec(ctx, "DEALLOCATE positive_page", pgx.QueryExecModeSimpleProtocol); err != nil {
					t.Fatal(err)
				}
				if err = tx.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
