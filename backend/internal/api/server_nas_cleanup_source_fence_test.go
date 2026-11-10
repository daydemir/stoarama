package api

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pause at real SQL boundaries without production hooks. end=true reproduces
// the reviewers' gap after the cache verdict/full replay, before permit write.
type cleanupFenceTraceKey struct{}
type cleanupFenceTracer struct {
	query   string
	end     bool
	once    sync.Once
	reached chan uint32
	resume  chan struct{}
}

func (tr *cleanupFenceTracer) pause(ctx context.Context, conn *pgx.Conn) {
	tr.once.Do(func() {
		tr.reached <- conn.PgConn().PID()
		select {
		case <-tr.resume:
		case <-ctx.Done():
		}
	})
}
func (tr *cleanupFenceTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	match := d.SQL == tr.query
	if match && !tr.end {
		tr.pause(ctx, conn)
	}
	return context.WithValue(ctx, cleanupFenceTraceKey{}, match)
}
func (tr *cleanupFenceTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, d pgx.TraceQueryEndData) {
	if tr.end && ctx.Value(cleanupFenceTraceKey{}) == true && d.Err == nil {
		tr.pause(ctx, conn)
	}
}
func cleanupFenceTrace(t *testing.T, f *cleanupFixture, query string, end bool) *cleanupFenceTracer {
	t.Helper()
	tr := &cleanupFenceTracer{query: query, end: end, reached: make(chan uint32, 1), resume: make(chan struct{})}
	cfg := f.pool.Config()
	cfg.ConnConfig.Tracer = tr
	pool, err := pgxpool.NewWithConfig(f.ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.s.pool = pool
	t.Cleanup(pool.Close)
	// Release paused handlers before closing their pool, even on test failure.
	t.Cleanup(func() { close(tr.resume) })
	return tr
}
func cleanupFenceWaitBlocked(t *testing.T, pool *pgxpool.Pool, blocked, holder uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		var waiting bool
		if err := pool.QueryRow(ctx, `SELECT $2::integer=ANY(pg_blocking_pids($1::integer))`, blocked, holder).Scan(&waiting); err != nil {
			t.Fatalf("backend %d did not block on %d: %v", blocked, holder, err)
		}
		if waiting {
			return
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("backend %d did not block on %d", blocked, holder)
		}
	}
}
func cleanupFenceReached(t *testing.T, tr *cleanupFenceTracer) uint32 {
	t.Helper()
	select {
	case pid := <-tr.reached:
		return pid
	case <-time.After(5 * time.Second):
		t.Fatal("authorization did not reach SQL boundary")
		return 0
	}
}
func cleanupFenceObserve(f *cleanupFixture, tasks []cleanupTask) <-chan []batchResult {
	done := make(chan []batchResult, 1)
	go func() {
		defer close(done)
		done <- f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})
	}()
	return done
}
func cleanupFenceResult(t *testing.T, done <-chan []batchResult) batchResult {
	t.Helper()
	select {
	case results := <-done:
		if len(results) != 1 {
			t.Fatalf("observation: %+v", results)
		}
		return results[0]
	case <-time.After(10 * time.Second):
		t.Fatal("authorization did not finish")
		return batchResult{}
	}
}
func cleanupFenceFixture(t *testing.T, miss bool) (*cleanupFixture, int64, []cleanupTask) {
	t.Helper()
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("source-fence", syntheticFiles(100, 1, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	tasks := f.leaseV2(64, 64, 1<<40, "")
	if len(tasks) != 1 {
		t.Fatalf("lease: %+v", tasks)
	}
	if miss {
		if _, err := f.pool.Exec(f.ctx, `DELETE FROM nas_cleanup_rankings_cache WHERE plan_id=$1`, id); err != nil {
			t.Fatal(err)
		}
	}
	return f, id, tasks
}

const cleanupFenceMoveBestSQL = `DELETE FROM recording_clips WHERE id>=5000000`

func cleanupFenceAssertMoved(t *testing.T, f *cleanupFixture, item nasCleanupItem) {
	t.Helper()
	var best string
	if err := f.pool.QueryRow(f.ctx, cleanupCurrentRankingsSQL, 3).Scan(&best); err != nil || best != "2026-07-19" {
		t.Fatalf("source mutation did not reproduce changed best-start: %q %v", best, err)
	}
	loc, err := time.LoadLocation(item.Timezone)
	if err != nil {
		t.Fatal(err)
	}
	start, err := time.ParseInLocation(time.DateOnly, best, loc)
	if err != nil || !item.StartAt.Before(start.AddDate(0, 0, 21)) || !item.EndAt.After(start.AddDate(0, 0, -7)) {
		t.Fatalf("candidate must overlap the new keep window: %+v %v", item, err)
	}
}
func TestNASCleanupSourceFenceSerializesVerdictThroughPermitCommit(t *testing.T) {
	for _, path := range []string{"cache-hit", "replay-miss", "legacy-replay"} {
		t.Run(path, func(t *testing.T) {
			f, id, tasks := cleanupFenceFixture(t, path == "replay-miss")
			query := cleanupRankingsCacheLoadSQL
			if path == "replay-miss" {
				query = cleanupRankingsVersionedReplaySQL
			} else if path == "legacy-replay" {
				if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET execution_rankings=NULL WHERE id=$1`, id); err != nil {
					t.Fatal(err)
				}
				query = cleanupCurrentRankingsSQL
			}
			tr := cleanupFenceTrace(t, f, query, true)
			observed := cleanupFenceObserve(f, tasks)
			authPID := cleanupFenceReached(t, tr)
			ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
			defer cancel()
			writer, err := f.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Rollback(f.ctx)
			writerPID := writer.Conn().PgConn().PID()
			written := make(chan error, 1)
			go func() { _, err := writer.Exec(ctx, cleanupFenceMoveBestSQL); written <- err }()
			// PostgreSQL proves the trigger cannot finish in the verdict/permit gap.
			cleanupFenceWaitBlocked(t, f.pool, writerPID, authPID)
			tr.resume <- struct{}{}
			result := cleanupFenceResult(t, observed)
			if !result.Delete || result.Replayed {
				t.Fatalf("serialized fresh permit: %+v", result)
			}
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			var state string
			if err := writer.QueryRow(ctx, `SELECT state FROM nas_cleanup_items WHERE id=$1`, tasks[0].ItemID).Scan(&state); err != nil || state != "delete_issued" {
				t.Fatalf("writer ran before authorization commit: %s %v", state, err)
			}
			if err := writer.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			cleanupFenceAssertMoved(t, f, tasks[0].Item)
		})
	}
}
func TestNASCleanupSourceFenceRechecksCommittedWriterBeforePermit(t *testing.T) {
	for _, miss := range []bool{false, true} {
		t.Run(fmt.Sprint("miss=", miss), func(t *testing.T) {
			f, _, tasks := cleanupFenceFixture(t, miss)
			tr := cleanupFenceTrace(t, f, cleanupRankingsVersionLockSQL, false)
			observed := cleanupFenceObserve(f, tasks)
			cleanupFenceReached(t, tr)
			if _, err := f.pool.Exec(f.ctx, cleanupFenceMoveBestSQL); err != nil {
				t.Fatal(err)
			}
			tr.resume <- struct{}{}
			result := cleanupFenceResult(t, observed)
			if result.Delete || result.Action != "keep" || !result.Paused {
				t.Fatalf("committed source mutation received authority: %+v", result)
			}
			var cache string
			var issued bool
			if err := f.pool.QueryRow(f.ctx, `SELECT stage_timings->>'rankings_cache',delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, tasks[0].ItemID).Scan(&cache, &issued); err != nil || cache != "replay" || issued {
				t.Fatalf("stale version did not force replay/KEEP: cache=%s issued=%v err=%v", cache, issued, err)
			}
			cleanupFenceAssertMoved(t, f, tasks[0].Item)
		})
	}
}
func TestNASCleanupSourceFenceFollowsPlanAndCatalogLocks(t *testing.T) {
	for _, lock := range []string{"plan", "catalog"} {
		t.Run(lock, func(t *testing.T) {
			f, id, tasks := cleanupFenceFixture(t, false)
			holder, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Rollback(f.ctx)
			query := `SELECT pg_advisory_xact_lock($1)`
			if lock == "plan" {
				_, err = holder.Exec(f.ctx, query, nasCleanupPlanLockBase+id)
			} else {
				_, err = holder.Exec(f.ctx, `SELECT id FROM recording_clips WHERE id=$1 FOR UPDATE`, tasks[0].Item.SourceID)
				query = `SELECT c.recording_id,r.stream_id,c.display_path,c.size_bytes,lower(c.sha256),c.object_key,c.bucket,c.clip_start_at,c.clip_end_at,coalesce(nullif(s.local_timezone,''),r.cron_timezone),ARRAY[c.id] FROM recording_clips c JOIN recordings r ON r.id=c.recording_id JOIN streams s ON s.id=r.stream_id WHERE r.account_id=47 AND c.id=$1 AND c.bucket=$2 FOR SHARE OF c`
			}
			if err != nil {
				t.Fatal(err)
			}
			tr := cleanupFenceTrace(t, f, query, false)
			observed := cleanupFenceObserve(f, tasks)
			authPID := cleanupFenceReached(t, tr)
			tr.resume <- struct{}{}
			cleanupFenceWaitBlocked(t, f.pool, authPID, holder.Conn().PgConn().PID())
			// An observer waiting for the earlier lock must not hold the source fence.
			// This writer can finish its trigger without waiting on the observer.
			ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
			defer cancel()
			if _, err := holder.Exec(ctx, cleanupFenceMoveBestSQL); err != nil {
				t.Fatalf("lock-order cycle: %v", err)
			}
			if err := holder.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if result := cleanupFenceResult(t, observed); result.Delete || result.Action != "keep" {
				t.Fatalf("source change under %s lock received authority: %+v", lock, result)
			}
		})
	}
}
func TestNASCleanupSourceFenceMissingRowFailsClosed(t *testing.T) {
	f, _, tasks := cleanupFenceFixture(t, false)
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM nas_cleanup_rankings_versions WHERE stream_id=3`); err != nil {
		t.Fatal(err)
	}
	result := f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})[0]
	if result.Delete || result.Status != 503 || result.Error != "cleanup source fence unavailable" {
		t.Fatalf("missing fence granted authority: %+v", result)
	}
}

func TestNASCleanupSourceFenceBackfillsUntouchedStreams(t *testing.T) {
	pool := cleanupTestPool(t)
	var version int64
	if err := pool.QueryRow(context.Background(), cleanupRankingsVersionLockSQL, 3).Scan(&version); err != nil || version != 0 {
		t.Fatalf("pre-trigger stream has no initial fence: version=%d err=%v", version, err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO streams VALUES(4,'UTC')`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), cleanupRankingsVersionLockSQL, 4).Scan(&version); err != nil || version != 1 {
		t.Fatalf("new stream has no triggered fence: version=%d err=%v", version, err)
	}
}
