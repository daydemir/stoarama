package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5/pgxpool"
)

func r2WriterFenceFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := joinedPurgeTestPool(t, "r2_writer_fence")
	ctx := context.Background()
	exec := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	// The exact additive migration runs against the admission/state surfaces;
	// TestRecordingPresentationV2MigrationRunsThroughMigrateUp covers full DDL.
	exec(`CREATE TABLE recordings(status text); CREATE TABLE streams(recording_state text); CREATE TABLE stream_capture_runtime(status text);
 CREATE TABLE recording_jobs(status text); CREATE TABLE clip_transfer_jobs(status text);
 CREATE TABLE inference_box_jobs(status text); CREATE TABLE nas_restore_requests(state text,put_expires_at timestamptz);
 CREATE TABLE nas_stitch_tasks(state text); CREATE TABLE recording_native_stitch_tasks(state text);
 CREATE TABLE recording_presentation_v2_probe_tasks(state text); CREATE TABLE recording_joined_batches(state text);
 CREATE TABLE recording_joined_hours(state text); CREATE TABLE recording_joined_stream_days(state text);
 CREATE TABLE recording_joined_artifacts(publication_state text);
 CREATE TABLE capture_session_leases(lease_expires_at timestamptz);
 CREATE TABLE recording_upload_intents(bucket text,expires_at timestamptz);
 CREATE TABLE upload_intents(bucket text,expires_at timestamptz)`)
	for _, table := range []string{"recording_clips", "recording_collation_hours", "recording_collation_outputs", "nas_stitch_outputs", "recording_native_stitch_certifications", "media_objects", "frames"} {
		exec("CREATE TABLE " + table + "(id bigint)")
	}
	body, err := os.ReadFile("../../../infra/sql/migrations/0187_r2_writer_admission_fence.sql")
	if err != nil {
		t.Fatal(err)
	}
	exec(string(body))
	return pool
}

func TestR2WriterFenceAllAdmissionsAndDrain(t *testing.T) {
	ctx := context.Background()
	pool := r2WriterFenceFixture(t)
	exec := func(sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	queued := []struct{ table, column, value string }{
		{"recordings", "status", "active"}, {"streams", "recording_state", "on"}, {"stream_capture_runtime", "status", "running"},
		{"recording_jobs", "status", "pending"}, {"recording_jobs", "status", "leased"},
		{"clip_transfer_jobs", "status", "pending"}, {"clip_transfer_jobs", "status", "leased"},
		{"inference_box_jobs", "status", "pending"}, {"nas_restore_requests", "state", "pending"},
		{"nas_stitch_tasks", "state", "leased"}, {"recording_native_stitch_tasks", "state", "pending"},
		{"recording_presentation_v2_probe_tasks", "state", "awaiting_retention"},
		{"recording_joined_batches", "state", "frozen"}, {"recording_joined_hours", "state", "pending"},
		{"recording_joined_stream_days", "state", "pending"}, {"recording_joined_artifacts", "publication_state", "publishing"},
	}
	if err := r2RetireCheckStoppedWriters(ctx, pool, "bucket"); err != nil {
		t.Fatal(err)
	}
	for _, job := range queued {
		exec(fmt.Sprintf("INSERT INTO %s(%s) VALUES('%s')", job.table, job.column, job.value))
		if r2RetireCheckStoppedWriters(ctx, pool, "bucket") == nil {
			t.Fatal("queue escaped drain", job.table, job.value)
		}
		exec("DELETE FROM " + job.table)
	}
	// Even a completed restore's live URL revokes drain.
	exec(`INSERT INTO nas_restore_requests(state,put_expires_at) VALUES('verified',now()+interval '1 hour')`)
	if r2RetireCheckStoppedWriters(ctx, pool, "bucket") == nil {
		t.Fatal("completed restore URL ignored")
	}
	exec("DELETE FROM nas_restore_requests")
	insert := `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('bucket','` + r2tSHA + `')`
	exec(insert)
	if r2RetireCheckWriterFence(ctx, pool, "bucket", r2tSHA) == nil {
		t.Fatal("legacy drain was waived")
	}
	// Activation survives reconnection/rollback; no caller-provided timestamp can
	// shorten the maximum capability lifetime.
	var future bool
	if err := pool.QueryRow(ctx, `SELECT drain_not_before>=activated_at+interval '7 days 2 minutes' FROM r2_retirement_writer_fences`).Scan(&future); err != nil || !future {
		t.Fatal(future, err)
	}
	for _, job := range queued {
		if _, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s(%s) VALUES('%s')", job.table, job.column, job.value)); err == nil || !strings.Contains(err.Error(), "fenced") {
			t.Fatal("admission escaped fence", job, err)
		}
	}
	for _, table := range []string{"capture_session_leases", "recording_upload_intents", "upload_intents", "recording_clips", "recording_collation_hours", "recording_collation_outputs", "nas_stitch_outputs", "recording_native_stitch_certifications", "media_objects", "frames"} {
		if _, err := pool.Exec(ctx, "INSERT INTO "+table+" DEFAULT VALUES"); err == nil {
			t.Fatal("registration escaped fence", table)
		}
	}
	// An active apply prevents explicit deactivation on another connection.
	lock, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	execLock := func(sql string) {
		t.Helper()
		if _, err := lock.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	execLock(`SELECT pg_advisory_lock(7343226011173)`)
	if _, err := pool.Exec(ctx, `DELETE FROM r2_retirement_writer_fences`); err == nil {
		t.Fatal("active apply lost durable fence")
	}
	execLock(`SELECT pg_advisory_unlock(7343226011173)`)
	exec(`DELETE FROM r2_retirement_writer_fences`)
	exec(`INSERT INTO clip_transfer_jobs(status) VALUES('pending')`)
}

func TestR2WriterFenceWireAndCapabilityDrain(t *testing.T) {
	ctx := context.Background()
	pool := r2WriterFenceFixture(t)
	started, finish := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case req.Method == "POST" && req.URL.Query().Has("uploads"):
			fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>`)
		case req.Method == "PUT" && req.URL.Query().Get("uploadId") == "test-upload":
			w.Header().Set("ETag", `"part"`)
		case req.Method == "POST" && req.URL.Query().Get("uploadId") == "test-upload":
			close(started)
			<-finish
			fmt.Fprint(w, `<CompleteMultipartUploadResult><ETag>"e"</ETag></CompleteMultipartUploadResult>`)
		default:
			t.Error("unexpected provider write")
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	client, err := r2.New(ctx, r2.Config{WriterDB: pool, Bucket: "bucket", Endpoint: server.URL, AccessKey: "fake", SecretKey: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	insert := `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('bucket','` + r2tSHA + `')`
	done := make(chan error, 1)
	go func() {
		_, err := client.PutMultipart(ctx, "managed/old.mp4", "video/mp4", strings.NewReader(strings.Repeat("x", 17*1024*1024)))
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("wire PUT did not start")
	}
	_, activateErr := pool.Exec(ctx, insert)
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if activateErr == nil {
		t.Fatal("activation ignored in-flight PUT")
	}
	if _, err := client.PresignPut(ctx, "managed/old.mp4", "video/mp4", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := r2RetireCheckStoppedWriters(ctx, pool, "bucket"); err == nil {
		t.Fatal("live presign did not revoke drain")
	}
	if _, err := pool.Exec(ctx, insert); err != nil {
		t.Fatal(err)
	}
	for _, write := range []func() error{
		func() error { _, e := client.PutBytes(ctx, "managed/x", "", []byte("x")); return e },
		func() error { _, e := client.PutReader(ctx, "managed/x", "", strings.NewReader("x")); return e },
		func() error { _, e := client.PutMultipart(ctx, "managed/x", "", strings.NewReader("x")); return e },
		func() error {
			_, _, e := client.PutReaderIfAbsentVerified(ctx, "managed/x", "", strings.NewReader("x"), 1, r2tSHA)
			return e
		},
		func() error { _, e := client.PresignPut(ctx, "managed/x", "", time.Hour); return e },
		func() error { _, e := client.PresignPutSizedRequest(ctx, "managed/x", "", 1, time.Hour); return e },
		func() error {
			_, e := client.PresignPutCreateOnlyRequest(ctx, "managed/x", "", 1, r2tSHA, time.Hour)
			return e
		},
	} {
		if err := write(); err == nil || !strings.Contains(err.Error(), "admission") {
			t.Fatal("fenced wire/capability admission", err)
		}
	}
}
