package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Test database only: production keeps the immutable seven-day deadline.
func matureR2WriterFence(t *testing.T, pool *pgxpool.Pool, bucket string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES($1,$2)`, bucket, r2tSHA); err != nil {
		t.Fatal(err)
	}
	injectR2WriterState(t, pool, `UPDATE r2_retirement_writer_fences SET drain_not_before=clock_timestamp()-interval '1 second'`)
}

// Seed in-flight observations that necessarily predate the admission fence.
func injectR2WriterState(t *testing.T, pool *pgxpool.Pool, query string) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, query); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestR2BlockedWritesPreservesFencedJobHistory(t *testing.T) {
	ctx := context.Background()
	pool := r2WriterFenceFixture(t)
	const seed = `INSERT INTO recordings VALUES('active'); INSERT INTO streams VALUES('on');
 INSERT INTO recording_jobs VALUES('pending'),('leased'); INSERT INTO stream_capture_runtime VALUES('running');
 INSERT INTO clip_transfer_jobs VALUES('pending'); INSERT INTO inference_box_jobs VALUES('pending');
 INSERT INTO nas_restore_requests(state) VALUES('pending'); INSERT INTO nas_stitch_tasks VALUES('leased');
 INSERT INTO recording_native_stitch_tasks VALUES('pending');
 INSERT INTO recording_presentation_v2_probe_tasks VALUES('awaiting_retention');
 INSERT INTO recording_joined_batches VALUES('frozen'); INSERT INTO recording_joined_hours VALUES('pending');
 INSERT INTO recording_joined_stream_days VALUES('pending'); INSERT INTO recording_joined_artifacts VALUES('publishing');
 INSERT INTO recording_process_runs VALUES('starting'),('running')`
	if _, err := pool.Exec(ctx, seed); err != nil {
		t.Fatal(err)
	}
	matureR2WriterFence(t, pool, "bucket")
	if err := r2RetireCheckStoppedWriters(ctx, pool, "bucket"); err == nil {
		t.Fatal("historical jobs disappeared from the old diagnostic")
	}
	for range 2 {
		if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err != nil {
			t.Fatal("fenced jobs without outstanding writes blocked retirement", err)
		}
	}
	for _, table := range []string{"recordings", "streams", "recording_jobs", "stream_capture_runtime", "clip_transfer_jobs", "inference_box_jobs", "nas_restore_requests", "nas_stitch_tasks", "recording_native_stitch_tasks", "recording_presentation_v2_probe_tasks", "recording_joined_batches", "recording_joined_hours", "recording_joined_stream_days", "recording_joined_artifacts", "recording_process_runs"} {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil || count == 0 {
			t.Fatalf("history changed: %s count=%d error=%v", table, count, err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_stitch_tasks SET state='leased'`); err == nil {
		t.Fatal("historical leased job could heartbeat through the admission fence")
	}
}

func TestR2BlockedWritesEveryOutstandingSurfaceVetoes(t *testing.T) {
	ctx := context.Background()
	pool := r2WriterFenceFixture(t)
	matureR2WriterFence(t, pool, "bucket")
	for _, tc := range []struct{ name, insert, table string }{
		{"ambiguous wire intent", `INSERT INTO r2_retirement_writer_operations(bucket) VALUES('other')`, "r2_retirement_writer_operations"},
		{"unexpired PUT capability", `INSERT INTO r2_retirement_writer_capabilities(bucket,expires_at) VALUES('other',now()+interval '1 hour')`, "r2_retirement_writer_capabilities"},
		{"recording PUT with unknown bucket", `INSERT INTO recording_upload_intents(bucket,expires_at) VALUES(NULL,now()+interval '1 hour')`, "recording_upload_intents"},
		{"generic PUT with blank bucket", `INSERT INTO upload_intents(bucket,expires_at) VALUES('',now()+interval '1 hour')`, "upload_intents"},
		{"live capture lease", `INSERT INTO capture_session_leases VALUES(now()+interval '1 hour')`, "capture_session_leases"},
		{"terminal restore still has PUT capability", `INSERT INTO nas_restore_requests VALUES('verified',now()+interval '1 hour')`, "nas_restore_requests"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			injectR2WriterState(t, pool, tc.insert)
			if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err == nil {
				t.Fatal("outstanding write accepted")
			}
			if _, err := pool.Exec(ctx, `DELETE FROM `+tc.table); err != nil {
				t.Fatal(err)
			}
			if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err != nil {
				t.Fatal("drained observation still blocked", err)
			}
		})
	}
}

func TestR2BlockedWritesRequiresExactMatureFenceAndSchema(t *testing.T) {
	ctx := context.Background()
	pool := r2WriterFenceFixture(t)
	if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err == nil {
		t.Fatal("missing fence accepted")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('bucket',$1)`, r2tSHA); err != nil {
		t.Fatal(err)
	}
	if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err == nil {
		t.Fatal("young fence accepted")
	}
	injectR2WriterState(t, pool, `UPDATE r2_retirement_writer_fences SET drain_not_before=clock_timestamp()-interval '1 second'`)
	for _, scope := range [][2]string{{"other", r2tSHA}, {"bucket", strings.Repeat("b", 64)}, {"", ""}} {
		if err := r2RetireCheckWriterFence(ctx, pool, scope[0], scope[1]); err == nil {
			t.Fatal("wrong fence scope accepted", scope)
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := r2RetireLockBlockedWrites(ctx, tx, "other", r2tSHA); err == nil {
		t.Fatal("locking a nonexistent fence succeeded")
	}
	if _, err := pool.Exec(ctx, `DROP FUNCTION r2_retirement_writes_blocked(text,text)`); err != nil {
		t.Fatal(err)
	}
	if err := r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA); err == nil {
		t.Fatal("missing migration fell back to an older policy")
	}
}
