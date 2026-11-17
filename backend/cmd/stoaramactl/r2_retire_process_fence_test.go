package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/db"
)

func TestR2WriterFenceProcessRunsWithRealSchema(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "process_writer_fence")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(991,'test','991','process-test','process-writer-fence','https://secret.invalid/do-not-report','off','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO recording_process_runs(stream_id,execution_class,server_id,process_id,worker_id,status,started_at,stopped_at)
 VALUES(991,'video_live','test-server','starting-process','test-worker','starting',now()-interval '1 year',NULL),
       (991,'video_live','test-server','inconsistent-running-process','test-worker','running',now()-interval '1 year',now()-interval '1 year')`); err != nil {
		t.Fatal(err)
	}
	// No stream/runtime/lease blocker: runnable children alone must block,
	// including an inconsistent row excluded by the active-stream unique index.
	h, items, err := r2WriterAudit(ctx, pool, "stoarama")
	if err != nil {
		t.Fatal(err)
	}
	if h.WritersDrained || h.Counts[r2WriterProcessRun] != 2 || len(items) != 2 || h.ProvenDead != 0 || h.ApplyCommand != nil {
		t.Fatalf("process children escaped drain: header=%+v items=%d", h, len(items))
	}
	for _, item := range items {
		if item.Category != r2WriterProcessRun || item.ProvenDead || strings.Contains(string(item.Observation), "secret.invalid") {
			t.Fatalf("invalid process diagnostic: %+v", item)
		}
	}
	if _, err = pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	injectR2WriterState(t, pool, `UPDATE r2_retirement_writer_fences SET drain_not_before=clock_timestamp()-interval '1 second'`)
	if err := r2RetireCheckWriterFence(ctx, pool, "stoarama", strings.Repeat("a", 64)); err != nil {
		t.Fatal("real-schema historical processes could not be fenced without rewriting them", err)
	}
	// Terminal history is legal; it does not authorize a runnable transition.
	if _, err = pool.Exec(ctx, `INSERT INTO recording_process_runs(stream_id,execution_class,server_id,process_id,worker_id,status,started_at,stopped_at)
 VALUES(991,'video_live','test-server','terminal-process','test-worker','stopped',now()-interval '1 year',now()-interval '1 year')`); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`INSERT INTO recording_process_runs(stream_id,execution_class,server_id,process_id,worker_id,status,started_at) VALUES(991,'video_live','test-server','new-starting','test-worker','starting',now())`,
		`INSERT INTO recording_process_runs(stream_id,execution_class,server_id,process_id,worker_id,status,started_at) VALUES(991,'video_live','test-server','new-running','test-worker','running',now())`,
		`UPDATE recording_process_runs SET last_heartbeat_at=now() WHERE process_id='starting-process'`,
		`UPDATE recording_process_runs SET last_heartbeat_at=now() WHERE process_id='inconsistent-running-process'`,
		`UPDATE recording_process_runs SET status='running' WHERE process_id='terminal-process'`,
	} {
		if _, err = pool.Exec(ctx, query); err == nil || !strings.Contains(err.Error(), "R2 retirement writer admission is fenced") {
			t.Fatalf("process admission/heartbeat not fence-rejected: %v", err)
		}
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_process_runs SET status='stopped',stopped_at=coalesce(stopped_at,now()),stop_reason='test ordinary terminal transition' WHERE status IN ('starting','running');
 UPDATE recording_process_runs SET stop_reason='test terminal history remains mutable' WHERE process_id='terminal-process'`); err != nil {
		t.Fatal(err)
	}
	h, items, err = r2WriterAudit(ctx, pool, "stoarama")
	if err != nil || !h.WritersDrained || len(items) != 0 || h.Counts[r2WriterProcessRun] != 0 || h.ProvenDead != 0 || h.ApplyCommand != nil {
		t.Fatalf("terminal history/drain: header=%+v items=%d err=%v", h, len(items), err)
	}

	// Hold the same exclusive admission lock a retirement transaction uses.
	// The new process must really wait, then fail AFTER that transaction commits.
	guard, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Release()
	tx, err := guard.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var guardPID int32
	if err = tx.QueryRow(ctx, `SELECT pg_backend_pid() FROM (SELECT pg_advisory_xact_lock(7343226011187)) locked`).Scan(&guardPID); err != nil {
		t.Fatal(err)
	}
	waiter, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer waiter.Release()
	var waiterPID int32
	if err = waiter.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&waiterPID); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	finished := make(chan struct{})
	var insertErr error
	go func() {
		defer close(finished)
		_, insertErr = waiter.Exec(waitCtx, `INSERT INTO recording_process_runs(stream_id,execution_class,server_id,process_id,worker_id,status,started_at)
 VALUES(991,'video_live','test-server','waited-process','test-worker','starting',now())`)
	}()
	defer func() {
		cancel()
		tx.Rollback(ctx)
		<-finished
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	waiting := false
	for !waiting {
		select {
		case <-finished:
			t.Fatalf("process insert did not wait for admission lock: %v", insertErr)
		case <-waitCtx.Done():
			t.Fatal("process insert never demonstrated an admission-lock wait")
		case <-ticker.C:
			if err = pool.QueryRow(waitCtx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND mode='ShareLock' AND NOT granted AND $2=ANY(pg_blocking_pids(pid)))`, waiterPID, guardPID).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	<-finished
	if insertErr == nil || !strings.Contains(insertErr.Error(), "R2 retirement writer admission is fenced") {
		t.Fatalf("post-commit process admission escaped fence: %v", insertErr)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_process_runs WHERE process_id='waited-process'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("post-commit process inserted: count=%d err=%v", count, err)
	}
}
