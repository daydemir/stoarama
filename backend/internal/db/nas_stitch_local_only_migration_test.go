package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Stitching is a NAS-local writer: it produces NAS files and DB rows and never
// PUTs to R2. 0187 fenced it anyway, which stranded the single stitch lane as
// soon as the R2 writer fence was active. 0214 removes exactly those two
// triggers; every other fenced table must still refuse writes.
func TestNASStitchLocalOnlyMigrationDropsOnlyStitchFenceTriggers(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("nas_stitch_local_only_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Every table 0187 fences or inspects, with only the columns it touches.
	if _, err = pool.Exec(ctx, `
CREATE TABLE recordings(id bigint PRIMARY KEY,status text NOT NULL DEFAULT 'active');
CREATE TABLE streams(id bigint PRIMARY KEY,recording_state text NOT NULL DEFAULT 'off');
CREATE TABLE stream_capture_runtime(id bigint PRIMARY KEY,status text NOT NULL DEFAULT 'idle');
CREATE TABLE recording_jobs(id bigint PRIMARY KEY,status text NOT NULL DEFAULT 'done');
CREATE TABLE clip_transfer_jobs(id bigint PRIMARY KEY,status text NOT NULL DEFAULT 'done');
CREATE TABLE inference_box_jobs(id bigint PRIMARY KEY,status text NOT NULL DEFAULT 'done');
CREATE TABLE nas_restore_requests(id bigint PRIMARY KEY,state text NOT NULL DEFAULT 'done',put_expires_at timestamptz);
CREATE TABLE nas_stitch_tasks(id bigint PRIMARY KEY,state text NOT NULL DEFAULT 'pending');
CREATE TABLE nas_cleanup_plans(id bigint PRIMARY KEY,mode text NOT NULL,manifest jsonb NOT NULL);
CREATE TABLE nas_cleanup_items(plan_id bigint NOT NULL,permit_expires_at timestamptz);
CREATE TABLE recording_native_stitch_tasks(id bigint PRIMARY KEY,state text NOT NULL DEFAULT 'pending');
CREATE TABLE recording_presentation_v2_probe_tasks(id bigint PRIMARY KEY,state text NOT NULL DEFAULT 'done');
CREATE TABLE recording_joined_batches(id bigint PRIMARY KEY,state text NOT NULL DEFAULT 'done');
CREATE TABLE recording_joined_hours(id bigint PRIMARY KEY,state text NOT NULL DEFAULT 'done');
CREATE TABLE recording_joined_stream_days(id bigint PRIMARY KEY,state text NOT NULL DEFAULT 'done');
CREATE TABLE recording_joined_artifacts(id bigint PRIMARY KEY,publication_state text NOT NULL DEFAULT 'done');
CREATE TABLE recording_clips(id bigint PRIMARY KEY);
CREATE TABLE recording_collation_hours(id bigint PRIMARY KEY);
CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY);
CREATE TABLE nas_stitch_outputs(task_id bigint NOT NULL,relative_path text NOT NULL,sha256 text NOT NULL,size_bytes bigint NOT NULL,proof jsonb NOT NULL);
CREATE TABLE recording_native_stitch_certifications(id bigint PRIMARY KEY);
CREATE TABLE media_objects(id bigint PRIMARY KEY);
CREATE TABLE frames(id bigint PRIMARY KEY);
CREATE TABLE capture_session_leases(id bigint PRIMARY KEY,lease_expires_at timestamptz);
CREATE TABLE recording_upload_intents(id bigint PRIMARY KEY,bucket text,expires_at timestamptz);
CREATE TABLE upload_intents(id bigint PRIMARY KEY,bucket text,expires_at timestamptz);`); err != nil {
		t.Fatal(err)
	}
	fence, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0187_r2_writer_admission_fence.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(fence)); err != nil {
		t.Fatalf("apply 0187: %v", err)
	}
	localOnly, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0214_nas_stitch_local_only.sql"))
	if err != nil {
		t.Fatal(err)
	}

	// Active writer fence: the transition trigger stamps drain_not_before seven
	// days out, so the fence blocks writes for the whole test.
	if _, err = pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',repeat('a',64))`); err != nil {
		t.Fatal(err)
	}
	var drain time.Time
	if err = pool.QueryRow(ctx, `SELECT drain_not_before FROM r2_retirement_writer_fences`).Scan(&drain); err != nil || !drain.After(time.Now().Add(6*24*time.Hour)) {
		t.Fatalf("fence drain deadline not in the future: %v %v", drain, err)
	}
	// The fence is real before 0214: both stitch writes are rejected.
	for _, stmt := range []string{
		`INSERT INTO nas_stitch_tasks(id,state) VALUES(1,'pending')`,
		`INSERT INTO nas_stitch_outputs(task_id,relative_path,sha256,size_bytes,proof) VALUES(1,'out.mp4',repeat('b',64),1,'{}')`,
	} {
		if _, err = pool.Exec(ctx, stmt); err == nil {
			t.Fatalf("pre-0214 write escaped the writer fence: %s", stmt)
		}
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO nas_cleanup_plans VALUES (1,'enabled','[{"permit_reason":"stitched_included"}]'), (2,'enabled','[{"kind":"raw"}]');
INSERT INTO nas_cleanup_items VALUES (1,clock_timestamp()+interval '2 minutes');`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(localOnly)); err == nil || !strings.Contains(err.Error(), "nas_cleanup_no_stitch_retirement") {
		t.Fatalf("0214 must refuse an enabled legacy stitch cleanup plan: %v", err)
	}
	if _, err = pool.Exec(ctx, "UPDATE nas_cleanup_plans SET mode='paused' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(localOnly)); err == nil || !strings.Contains(err.Error(), "wait for existing NAS stitch delete permits") {
		t.Fatalf("0214 must refuse unexpired legacy stitch permits: %v", err)
	}
	if _, err = pool.Exec(ctx, "UPDATE nas_cleanup_items SET permit_expires_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(localOnly)); err != nil {
		t.Fatalf("apply 0214: %v", err)
	}
	if _, err = pool.Exec(ctx, "UPDATE nas_cleanup_plans SET mode='enabled' WHERE id=1"); err == nil {
		t.Fatal("previous API could re-enable a stitch retirement plan after 0214")
	}
	if _, err = pool.Exec(ctx, "UPDATE nas_cleanup_plans SET mode='enabled' WHERE id=2"); err != nil {
		t.Fatalf("ordinary cleanup plan could not be enabled: %v", err)
	}
	blockedWrites, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0198_r2_blocked_writes.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(blockedWrites)); err != nil {
		t.Fatalf("apply 0198: %v", err)
	}

	// Stitch writes now succeed while the fence is still active.
	if _, err = pool.Exec(ctx, `INSERT INTO nas_stitch_tasks(id,state) VALUES(1,'pending')`); err != nil {
		t.Fatalf("pending stitch task refused under the fence: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_stitch_tasks SET state='leased' WHERE id=1`); err != nil {
		t.Fatalf("leased stitch task refused under the fence: %v", err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO nas_stitch_outputs(task_id,relative_path,sha256,size_bytes,proof) VALUES(1,'out.mp4',repeat('b',64),1,'{}')`); err != nil {
		t.Fatalf("stitch output refused under the fence: %v", err)
	}

	// Another fenced writer still refuses, so only the stitch lane was opened.
	if _, err = pool.Exec(ctx, `INSERT INTO recording_native_stitch_tasks(id,state) VALUES(1,'pending')`); err == nil {
		t.Fatal("native stitch writer escaped the retired fence")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_clips(id) VALUES(1)`); err == nil {
		t.Fatal("catalog writer escaped the retired fence")
	}
	// Exactly the two stitch triggers are gone; every other 0187 trigger stays.
	var remaining int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM pg_trigger g JOIN pg_class c ON c.oid=g.tgrelid
 WHERE NOT g.tgisinternal AND ((g.tgname='r2_writer_stitch') OR (g.tgname='r2_writer_insert' AND c.relname='nas_stitch_outputs'))`).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("stitch trigger survived 0214: %d %v", remaining, err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM pg_trigger g JOIN pg_class c ON c.oid=g.tgrelid
 WHERE NOT g.tgisinternal AND g.tgname='r2_writer_insert' AND c.relname<>'nas_stitch_outputs'`).Scan(&remaining); err != nil || remaining != 8 {
		t.Fatalf("0187 insert triggers on other fenced tables changed: %d %v", remaining, err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgname LIKE 'r2_writer_%'`).Scan(&remaining); err != nil || remaining < 20 {
		t.Fatalf("0187 removed more triggers than the two stitch ones: %d %v", remaining, err)
	}
	// The diagnostic drain function is unchanged and still counts the stitch
	// backlog it can no longer gate. 0198's blocked-writes predicate does not
	// read these tables; here it is false only because the fence deadline is
	// still in the future.
	var drained, blocked bool
	if err = pool.QueryRow(ctx, `SELECT r2_retirement_writers_drained('stoarama')`).Scan(&drained); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT r2_retirement_writes_blocked('stoarama',repeat('a',64))`).Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	if drained {
		t.Fatal("diagnostic drain no longer reports the stranded stitch task")
	}
	if blocked {
		t.Fatal("blocked-writes predicate changed meaning")
	}
	// Removing the stitch backlog is what clears the diagnostic: nothing else in
	// this fixture blocks drain. This proves the report was about stitches only.
	if _, err = pool.Exec(ctx, `DELETE FROM nas_stitch_tasks;DELETE FROM nas_stitch_outputs`); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT r2_retirement_writers_drained('stoarama')`).Scan(&drained); err != nil || !drained {
		t.Fatalf("drain diagnostic did not clear with the stitch backlog: %t %v", drained, err)
	}
}
