package api

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
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
	query    string
	end      bool
	once     sync.Once
	reached  chan uint32
	resume   chan struct{}
	attempts chan error
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
	if ctx.Value(cleanupFenceTraceKey{}) == true && tr.attempts != nil {
		select {
		case tr.attempts <- d.Err:
		default:
		}
	}
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
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_collation_hours VALUES(78,2,0,'{"clips":[]}')`); err != nil {
		t.Fatal(err)
	}
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

func requireCleanupFrozen(t *testing.T, err error) {
	t.Helper()
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55000" || !strings.Contains(pe.Message, "frozen") {
		t.Fatalf("source writer must fail immediately with freeze error: %v", err)
	}
}

func TestNASCleanupSourceFreezeRejectsVerdictGapAndReassignmentRaces(t *testing.T) {
	for _, path := range []string{"cache-hit", "replay-miss", "legacy-replay"} {
		t.Run(path, func(t *testing.T) {
			f, id, tasks := cleanupFenceFixture(t, path == "replay-miss")
			// Create independent unfrozen sources before tracing authorization.
			if _, err := f.pool.Exec(f.ctx, `INSERT INTO streams VALUES(4,'UTC'); INSERT INTO recordings VALUES(5,47,4,'UTC'); INSERT INTO recording_clips(id,recording_id) VALUES(9,5)`); err != nil {
				t.Fatal(err)
			}
			query := cleanupRankingsCacheLoadSQL
			if path == "replay-miss" {
				query = cleanupRankingsReplaySQL
			}
			if path == "legacy-replay" {
				if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET execution_rankings=NULL WHERE id=$1`, id); err != nil {
					t.Fatal(err)
				}
				query = cleanupCurrentRankingsSQL
			}
			tr := cleanupFenceTrace(t, f, query, true)
			observed := cleanupFenceObserve(f, tasks)
			cleanupFenceReached(t, tr)
			// Both recording reassignment and the concurrent clip mutation must fail;
			// neither path can invalidate only the old stream as the version trigger did.
			for n := 0; n < 20; n++ {
				done := make(chan error, 2)
				for _, sql := range []string{`UPDATE recordings SET stream_id=3 WHERE id=5`, `UPDATE recording_clips SET recording_id=2 WHERE id=9`} {
					go func(sql string) {
						c, cancel := context.WithTimeout(f.ctx, 2*time.Second)
						defer cancel()
						_, err := f.pool.Exec(c, sql)
						done <- err
					}(sql)
				}
				requireCleanupFrozen(t, <-done)
				requireCleanupFrozen(t, <-done)
			}
			for _, sql := range []string{cleanupFenceMoveBestSQL, `UPDATE recordings SET stream_id=4 WHERE id=2`, `UPDATE recording_clips SET recording_id=5 WHERE id=5000001`, `INSERT INTO recording_clips(id,recording_id) VALUES(8,2)`} {
				_, err := f.pool.Exec(f.ctx, sql)
				requireCleanupFrozen(t, err)
			}
			tr.resume <- struct{}{}
			if result := cleanupFenceResult(t, observed); !result.Delete {
				t.Fatalf("unchanged frozen verdict refused: %+v", result)
			}
		})
	}
}

func TestNASCleanupSourceFreezeCoversDependenciesAndUnrelatedStreams(t *testing.T) {
	f, _, _ := cleanupFenceFixture(t, false)
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO streams VALUES(4,'UTC'); INSERT INTO recordings VALUES(5,47,4,'UTC'); INSERT INTO recording_clips(id,recording_id) VALUES(9,5); INSERT INTO recording_collation_hours VALUES(77,5,1,'{"clips":[]}')`); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`UPDATE recording_clips SET clip_end_at=clip_end_at+interval '1 microsecond' WHERE id=5000000`,
		`UPDATE recording_clips SET recording_id=2 WHERE id=9`,
		`DELETE FROM recording_clips WHERE id=5000000`,
		`INSERT INTO recording_clips(id,recording_id) VALUES(8,2)`,
		`INSERT INTO recording_collation_hours VALUES(78,2,1,'{"clips":[]}')`,
		`UPDATE recording_collation_hours SET recording_id=2 WHERE id=77`,
		`UPDATE recording_collation_hours SET excluded_clip_count=1,manifest='{"clips":[{"clip":{"clip_id":5000001},"disposition":"quarantined"}]}' WHERE id=78`,
		`UPDATE recording_collation_hours SET recording_id=5 WHERE id=78`,
		`DELETE FROM recording_collation_hours WHERE id=78`,
		`UPDATE recordings SET cron_timezone='UTC' WHERE id=2`,
		`UPDATE recordings SET stream_id=3 WHERE id=5`,
		`DELETE FROM recordings WHERE id=2`,
		`INSERT INTO recordings VALUES(6,47,3,'UTC')`,
		`UPDATE streams SET local_timezone='Asia/Seoul' WHERE id=3`,
		`DELETE FROM streams WHERE id=3`,
		`TRUNCATE recording_clips CASCADE`, `TRUNCATE recording_collation_hours`, `TRUNCATE recordings`, `TRUNCATE streams`,
	} {
		_, err := f.pool.Exec(f.ctx, sql)
		requireCleanupFrozen(t, err)
	}
	// Independent sources remain writable, and purged_at does not affect replay.
	var before, after string
	if err := f.pool.QueryRow(f.ctx, cleanupRankingsFingerprintSQL, 3).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=5000000; UPDATE recording_clips SET size_bytes=2 WHERE id=9; DELETE FROM recording_collation_hours WHERE id=77; UPDATE recordings SET cron_timezone='Europe/London' WHERE id=5; UPDATE streams SET local_timezone='Europe/London' WHERE id=4`); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(f.ctx, cleanupRankingsFingerprintSQL, 3).Scan(&after); err != nil || before != after {
		t.Fatalf("purged_at changed replay: %s %s %v", before, after, err)
	}
}

func TestNASCleanupSourceFreezeDrainsWriterBeforeEnableReplay(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, changedWinner := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%v/changedWinner=%v", batch, changedWinner), func(t *testing.T) {
				f := newCleanupFixture(t, 24)
				if _, err := f.pool.Exec(f.ctx, `INSERT INTO streams VALUES(4,'UTC'); INSERT INTO recordings VALUES(5,47,4,'UTC')`); err != nil {
					t.Fatal(err)
				}
				id, sha, _ := f.admit("enable-race", syntheticFiles(100, 1, 10))
				f.dryRun()
				f.s.cfg.NASCleanupDeletionEnabled = true
				writer, err := f.pool.Begin(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer writer.Rollback(f.ctx)
				sql := `UPDATE recording_clips SET size_bytes=size_bytes+1 WHERE id=5000000`
				if changedWinner {
					sql = cleanupFenceMoveBestSQL
				}
				if _, err = writer.Exec(f.ctx, sql); err != nil {
					t.Fatal(err)
				}
				tr := cleanupFenceTrace(t, f, cleanupFreezeDrainSQL, false)
				tr.attempts = make(chan error, 64)
				done := make(chan int, 1)
				go func() {
					if batch {
						code, _ := f.enableBatch([]int64{id}, []string{sha}, 2)
						done <- code
					} else {
						code, _ := safeguardControl(t, f.s, id, "enabled", map[string]any{"plan_sha256": sha})
						done <- code
					}
				}()
				cleanupFenceReached(t, tr)
				var frozen bool
				if err = f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams WHERE plan_id=$1 AND NOT validated)`, id).Scan(&frozen); err != nil || !frozen {
					t.Fatalf("freeze was not committed before drain: %v", err)
				}
				_, err = f.pool.Exec(f.ctx, `UPDATE recordings SET cron_timezone='UTC' WHERE id=2`)
				requireCleanupFrozen(t, err)
				tr.resume <- struct{}{}
				// Prove an actual drain attempt excludes the still-open writer, without
				// waiting on a subset of source table locks or entering a lock-order cycle.
				select {
				case e := <-tr.attempts:
					var pe *pgconn.PgError
					if !errors.As(e, &pe) || pe.Code != "55P03" {
						t.Fatalf("drain did not exclude old writer: %v", e)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("drain did not run")
				}
				if mode, _ := f.planMode(id); mode == "enabled" {
					t.Fatal("enabled before writer drained")
				}
				// A clip-first, then recording writer cannot deadlock against the
				// drain's stream/recording-first lock order: partial locks were released.
				writeCtx, writeCancel := context.WithTimeout(f.ctx, 2*time.Second)
				_, err = writer.Exec(writeCtx, `UPDATE recordings SET cron_timezone='Europe/London' WHERE id=5`)
				writeCancel()
				if err != nil {
					t.Fatalf("drain retained partial table locks: %v", err)
				}
				if err = writer.Commit(f.ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case code := <-done:
					if changedWinner && code == 200 {
						t.Fatal("changed winner enabled")
					}
					if !changedWinner && code != 200 {
						t.Fatalf("unchanged winner failed enable: %d", code)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("enable deadlocked")
				}
				mode, _ := f.planMode(id)
				if (mode == "enabled") == changedWinner {
					t.Fatalf("activation replay ignored old writer: %s", mode)
				}
				if err = f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams WHERE plan_id=$1)`, id).Scan(&frozen); err != nil || frozen == changedWinner {
					t.Fatalf("activation freeze lifecycle: frozen=%v err=%v", frozen, err)
				}
			})
		}
	}
}

func TestNASCleanupSourceFreezeMissingRowFailsClosed(t *testing.T) {
	f, id, tasks := cleanupFenceFixture(t, false)
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM nas_cleanup_frozen_streams WHERE plan_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	result := f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})[0]
	if result.Delete || result.Status != 503 || result.Error != "cleanup source freeze unavailable" {
		t.Fatalf("missing freeze granted authority: %+v", result)
	}
}

func TestNASCleanupSourceFreezeRejectsOldTransactionSnapshots(t *testing.T) {
	f := newCleanupFixture(t, 24)
	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = tx.Exec(f.ctx, `SELECT count(*) FROM recording_clips`); err != nil {
		t.Fatal(err)
	}
	id, sha, _ := f.admit("old-snapshot", syntheticFiles(100, 1, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	_, err = tx.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id) VALUES(9,2)`)
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "55000" {
		t.Fatalf("old snapshot bypassed freeze: %v", err)
	}
}

func TestNASCleanupSourceFreezeReleasesOnPauseCloseAndComplete(t *testing.T) {
	for _, action := range []string{"pause", "close", "complete"} {
		t.Run(action, func(t *testing.T) {
			f, id, tasks := cleanupFenceFixture(t, false)
			switch action {
			case "pause":
				if code, body := safeguardControl(t, f.s, id, "paused", map[string]any{"plan_sha256": tasks[0].PlanSHA}); code != 200 {
					t.Fatalf("pause: %d %s", code, body)
				}
			case "close":
				if code, body := safeguardControl(t, f.s, id, "paused", map[string]any{"plan_sha256": tasks[0].PlanSHA}); code != 200 {
					t.Fatalf("pause: %d %s", code, body)
				}
				if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET lease_expires_at=now()-interval '1 second' WHERE plan_id=$1`, id); err != nil {
					t.Fatal(err)
				}
				if w := closeCall(t, f.s, id, tasks[0].PlanSHA); w.Code != 200 {
					t.Fatalf("close: %d %s", w.Code, w.Body.String())
				}
			case "complete":
				results := f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})
				f.receipts(tasks, results, "deleted")
			}
			var count int
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_frozen_streams WHERE plan_id=$1`, id).Scan(&count); err != nil || count != 0 {
				t.Fatalf("%s retained freeze: %d %v", action, count, err)
			}
			if _, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET clip_end_at=clip_end_at+interval '1 microsecond' WHERE id=5000000`); err != nil {
				t.Fatalf("unfrozen source refused: %v", err)
			}
		})
	}
}

func TestNASCleanupSourceFreezeSafePurgeDoesNotLockStream(t *testing.T) {
	f, _, tasks := cleanupFenceFixture(t, false)
	purge, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer purge.Rollback(f.ctx)
	if _, err = purge.Exec(f.ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=5000000`); err != nil {
		t.Fatal(err)
	}
	// Simulates the purge's open R2 operation. Other same-stream catalog rows
	// and authorization must not wait on a stream-wide version/freeze lock.
	c, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()
	_, err = f.pool.Exec(c, `UPDATE recording_clips SET size_bytes=size_bytes+1 WHERE id=5000001`)
	requireCleanupFrozen(t, err)
	if result := f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})[0]; !result.Delete {
		t.Fatalf("purge blocked independent permit: %+v", result)
	}
	if err = purge.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
}
