package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// patchRequest builds a PATCH request carrying the {id} path param and (optionally)
// an account principal in context, mirroring how chi + the account middleware invoke
// the handlers. A zero accountID omits the principal so the auth branch is exercised.
func patchRequest(id int64, accountID int64, body any) *http.Request {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(http.MethodPatch, fmt.Sprintf("/api/v1/account/recordings/%d/schedule", id), &buf)
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", fmt.Sprintf("%d", id))
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, rc)
	if accountID > 0 {
		ctx = context.WithValue(ctx, accountPrincipalContextKey, accountPrincipal{AccountID: accountID})
	}
	return req.WithContext(ctx)
}

// TestRecordingScheduleValidationAndAuth pins the pure validate/auth branches of the
// schedule PATCH: missing principal 401s, and each malformed field 400s BEFORE any DB
// access (so no pool is needed).
func TestRecordingScheduleValidationAndAuth(t *testing.T) {
	s := &Server{}

	// Auth: no principal -> 401.
	rec := httptest.NewRecorder()
	s.handleAccountRecordingSchedule(rec, patchRequest(1, 0, map[string]any{"mode": "sampled"}))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no principal: status=%d, want 401", rec.Code)
	}

	cases := []struct {
		name string
		body map[string]any
	}{
		{"bad mode", map[string]any{"mode": "hourly", "cron_expr": "*/5 * * * *"}},
		{"clip too short", map[string]any{"mode": "sampled", "cron_expr": "*/5 * * * *", "clip_duration_sec": 2}},
		{"clip too long", map[string]any{"mode": "sampled", "cron_expr": "*/5 * * * *", "clip_duration_sec": 901}},
		{"bad target_fps", map[string]any{"mode": "sampled", "cron_expr": "*/5 * * * *", "target_fps": 120}},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		s.handleAccountRecordingSchedule(rec, patchRequest(1, 42, c.body))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status=%d body=%s, want 400", c.name, rec.Code, rec.Body.String())
		}
	}
}

// TestRecordingDeliveryValidationAndAuth pins the pure validate/auth branches of the
// delivery PATCH: missing principal 401s, and an unknown delivery value 400s before
// any DB access.
func TestRecordingDeliveryValidationAndAuth(t *testing.T) {
	s := &Server{}

	rec := httptest.NewRecorder()
	s.handleAccountRecordingDelivery(rec, patchRequest(1, 0, map[string]any{"delivery": "managed"}))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no principal: status=%d, want 401", rec.Code)
	}

	rec = httptest.NewRecorder()
	s.handleAccountRecordingDelivery(rec, patchRequest(1, 42, map[string]any{"delivery": "sftp"}))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad delivery: status=%d body=%s, want 400", rec.Code, rec.Body.String())
	}
}

// TestRecordingDeliveryGate exercises the delivery PATCH against Postgres: switching a
// recording to nas_pull is rejected 400 when the account has NO nas_pull connection,
// and succeeds 200 once one exists, flipping recordings.delivery. Switching back to
// managed always succeeds. Only the owning account may edit (a foreign account 404s).
func TestRecordingDeliveryGate(t *testing.T) {
	pool, cleanup := testRecordingPatchPool(t)
	defer cleanup()

	ctx := context.Background()
	const (
		ownerAccountID   = int64(42)
		foreignAccountID = int64(99)
	)
	destID := insertPatchDestination(t, pool, ownerAccountID)
	recID := insertPatchRecording(t, pool, ownerAccountID, destID, "managed")

	s := &Server{pool: pool}

	// nas_pull with no connection -> 400, delivery unchanged.
	rec := httptest.NewRecorder()
	req := deliveryPatchReq(recID, ownerAccountID, "nas_pull")
	s.handleAccountRecordingDelivery(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("nas_pull without connection: status=%d body=%s, want 400", rec.Code, rec.Body.String())
	}
	if got := deliveryOf(t, pool, recID); got != "managed" {
		t.Fatalf("delivery after rejected switch = %q, want managed", got)
	}

	// A foreign account cannot edit the owner's recording -> 404.
	rec = httptest.NewRecorder()
	s.handleAccountRecordingDelivery(rec, deliveryPatchReq(recID, foreignAccountID, "managed"))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("foreign account edit: status=%d, want 404", rec.Code)
	}

	// Add a nas_pull connection, then the switch succeeds 200 and updates delivery.
	if _, err := pool.Exec(ctx, `
		INSERT INTO connections (account_id, kind) VALUES ($1, 'nas_pull')
	`, ownerAccountID); err != nil {
		t.Fatalf("insert connection: %v", err)
	}
	rec = httptest.NewRecorder()
	s.handleAccountRecordingDelivery(rec, deliveryPatchReq(recID, ownerAccountID, "nas_pull"))
	if rec.Code != http.StatusOK {
		t.Fatalf("nas_pull with connection: status=%d body=%s, want 200", rec.Code, rec.Body.String())
	}
	if got := deliveryOf(t, pool, recID); got != "nas_pull" {
		t.Fatalf("delivery after accepted switch = %q, want nas_pull", got)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["delivery"] != "nas_pull" || int64(body["id"].(float64)) != recID {
		t.Fatalf("response shape = %v, want updated recording JSON with delivery=nas_pull id=%d", body, recID)
	}
}

// TestRecordingScheduleUpdate exercises the schedule PATCH against Postgres: a valid
// sampled edit returns 200 with the updated recording JSON, persists the new
// cron/clip_duration, and recomputes next_fire_at. A foreign account 404s.
func TestRecordingScheduleUpdate(t *testing.T) {
	pool, cleanup := testRecordingPatchPool(t)
	defer cleanup()

	const (
		ownerAccountID   = int64(42)
		foreignAccountID = int64(99)
	)
	destID := insertPatchDestination(t, pool, ownerAccountID)
	recID := insertPatchRecording(t, pool, ownerAccountID, destID, "managed")
	var streamID int64
	if err := pool.QueryRow(context.Background(), `INSERT INTO streams (name, local_timezone) VALUES ('Rome stream', 'Europe/Rome') RETURNING id`).Scan(&streamID); err != nil {
		t.Fatalf("insert catalog stream: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE recordings SET stream_id=$2 WHERE id=$1`, recID, streamID); err != nil {
		t.Fatalf("link catalog stream: %v", err)
	}
	var jobID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO recording_jobs (recording_id, fire_at, scheduled_for, clip_duration_sec, status, lease_owner, lease_expires_at, idempotency_key)
		VALUES ($1, now(), now(), 60, 'leased', 'worker', now()+interval '5 minutes', 'old-schedule') RETURNING id
	`, recID).Scan(&jobID); err != nil {
		t.Fatalf("insert old schedule job: %v", err)
	}

	s := &Server{pool: pool}

	body := map[string]any{
		"mode":              "sampled",
		"cron_expr":         "*/10 * * * *",
		"cron_timezone":     "America/New_York",
		"clip_duration_sec": 300,
	}
	rec := httptest.NewRecorder()
	s.handleAccountRecordingSchedule(rec, schedulePatchReq(recID, ownerAccountID, body))
	if rec.Code != http.StatusOK {
		t.Fatalf("valid schedule edit: status=%d body=%s, want 200", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["cron_expr"] != "*/10 * * * *" || int(resp["clip_duration_sec"].(float64)) != 300 {
		t.Fatalf("response did not reflect the edit: %v", resp)
	}
	if int(resp["active_weekdays"].(float64)) != 127 {
		t.Fatalf("active_weekdays = %v, want default all-days mask 127", resp["active_weekdays"])
	}
	if resp["next_fire_at"] == nil {
		t.Fatalf("next_fire_at not recomputed: %v", resp["next_fire_at"])
	}
	if resp["cron_timezone"] != "Europe/Rome" {
		t.Fatalf("cron_timezone = %v, want catalog timezone Europe/Rome", resp["cron_timezone"])
	}
	var jobStatus string
	var leaseOwner *string
	if err := pool.QueryRow(context.Background(), `SELECT status, lease_owner FROM recording_jobs WHERE id=$1`, jobID).Scan(&jobStatus, &leaseOwner); err != nil {
		t.Fatalf("read old schedule job: %v", err)
	}
	if jobStatus != "canceled" || leaseOwner != nil {
		t.Fatalf("old schedule job status=%q lease_owner=%v, want canceled and unleased", jobStatus, leaseOwner)
	}
	var cursor *time.Time
	if err := pool.QueryRow(context.Background(), `SELECT last_enqueued_fire_at FROM recordings WHERE id=$1`, recID).Scan(&cursor); err != nil {
		t.Fatalf("read schedule cursor: %v", err)
	}
	if cursor == nil {
		t.Fatal("schedule edit left last_enqueued_fire_at null; old fires could replay")
	}

	missingRecID := insertPatchRecording(t, pool, ownerAccountID, destID, "managed")
	var missingStreamID int64
	if err := pool.QueryRow(context.Background(), `INSERT INTO streams (name) VALUES ('Missing timezone') RETURNING id`).Scan(&missingStreamID); err != nil {
		t.Fatalf("insert stream missing timezone: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE recordings SET stream_id=$2, name='missing-zone-rec' WHERE id=$1`, missingRecID, missingStreamID); err != nil {
		t.Fatalf("link stream missing timezone: %v", err)
	}
	missingBody := map[string]any{"mode": "sampled", "cron_expr": "*/10 * * * *", "clip_duration_sec": 300}
	rec = httptest.NewRecorder()
	s.handleAccountRecordingSchedule(rec, schedulePatchReq(missingRecID, ownerAccountID, missingBody))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing catalog timezone: status=%d body=%s, want 400", rec.Code, rec.Body.String())
	}
	missingBody["cron_timezone"] = "Europe/London"
	rec = httptest.NewRecorder()
	s.handleAccountRecordingSchedule(rec, schedulePatchReq(missingRecID, ownerAccountID, missingBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("explicit catalog timezone: status=%d body=%s, want 200", rec.Code, rec.Body.String())
	}
	var persistedTimezone string
	if err := pool.QueryRow(context.Background(), `SELECT local_timezone FROM streams WHERE id=$1`, missingStreamID).Scan(&persistedTimezone); err != nil {
		t.Fatalf("read persisted stream timezone: %v", err)
	}
	if persistedTimezone != "Europe/London" {
		t.Fatalf("persisted stream timezone=%q, want Europe/London", persistedTimezone)
	}

	unlinkedRecID := insertPatchRecording(t, pool, ownerAccountID, destID, "managed")
	unlinkedBody := map[string]any{"mode": "sampled", "cron_expr": "*/10 * * * *", "clip_duration_sec": 300}
	rec = httptest.NewRecorder()
	s.handleAccountRecordingSchedule(rec, schedulePatchReq(unlinkedRecID, ownerAccountID, unlinkedBody))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unlinked omitted timezone: status=%d body=%s, want 400", rec.Code, rec.Body.String())
	}
	unlinkedBody["cron_timezone"] = "UTC"
	rec = httptest.NewRecorder()
	s.handleAccountRecordingSchedule(rec, schedulePatchReq(unlinkedRecID, ownerAccountID, unlinkedBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlinked explicit UTC: status=%d body=%s, want 200", rec.Code, rec.Body.String())
	}

	// Foreign account cannot edit -> 404.
	rec = httptest.NewRecorder()
	s.handleAccountRecordingSchedule(rec, schedulePatchReq(recID, foreignAccountID, body))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("foreign schedule edit: status=%d, want 404", rec.Code)
	}
}

func TestRecordingJobCompleteRejectsZeroClipContinuous(t *testing.T) {
	pool, cleanup := testRecordingPatchPool(t)
	defer cleanup()

	ctx := context.Background()
	const accountID = int64(42)
	destID := insertPatchDestination(t, pool, accountID)
	recID := insertPatchRecording(t, pool, accountID, destID, "managed")
	var jobID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recording_jobs
			(recording_id, fire_at, scheduled_for, clip_duration_sec, status, lease_owner, attempt_count, idempotency_key, kind, window_end_at)
		VALUES ($1, now(), now(), 60, 'leased', 'node:7', 1, 'reccont-test', 'continuous_window', now()+interval '1 hour')
		RETURNING id
	`, recID).Scan(&jobID); err != nil {
		t.Fatalf("insert job: %v", err)
	}

	s := &Server{pool: pool}
	rec := httptest.NewRecorder()
	s.handleRecordingJobComplete(rec, recordingJobReq(jobID, nodePrincipal{NodeID: 7, AccountID: accountID, NodeType: nodeTypeRelay}))
	if rec.Code != http.StatusConflict {
		t.Fatalf("complete zero-clip continuous: status=%d body=%s, want 409", rec.Code, rec.Body.String())
	}

	var status, errText, lastErr string
	var failures int
	if err := pool.QueryRow(ctx, `
		SELECT j.status, j.error_text, r.last_error_text, r.consecutive_failures
		FROM recording_jobs j JOIN recordings r ON r.id=j.recording_id
		WHERE j.id=$1
	`, jobID).Scan(&status, &errText, &lastErr, &failures); err != nil {
		t.Fatalf("read state: %v", err)
	}
	if status != "error" || errText == "" || lastErr == "" || failures != 1 {
		t.Fatalf("state = job %q/%q rec %q failures %d, want error with health bump", status, errText, lastErr, failures)
	}
}

func TestRecordingPauseCancelsActiveJobs(t *testing.T) {
	pool, cleanup := testRecordingPatchPool(t)
	defer cleanup()

	ctx := context.Background()
	const accountID = int64(42)
	destID := insertPatchDestination(t, pool, accountID)
	recID := insertPatchRecording(t, pool, accountID, destID, "managed")
	var jobID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recording_jobs
			(recording_id, fire_at, scheduled_for, clip_duration_sec, status, lease_owner, attempt_count, idempotency_key, kind, window_end_at)
		VALUES ($1, now(), now(), 60, 'leased', 'node:7', 1, 'recpause-test', 'continuous_window', now()+interval '1 hour')
		RETURNING id
	`, recID).Scan(&jobID); err != nil {
		t.Fatalf("insert job: %v", err)
	}

	s := &Server{pool: pool}
	rec := httptest.NewRecorder()
	s.handleAccountRecordingPause(rec, patchRequest(recID, accountID, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("pause: status=%d body=%s, want 200", rec.Code, rec.Body.String())
	}

	var status string
	var leaseOwner *string
	if err := pool.QueryRow(ctx, `SELECT status, lease_owner FROM recording_jobs WHERE id=$1`, jobID).Scan(&status, &leaseOwner); err != nil {
		t.Fatalf("read job: %v", err)
	}
	if status != "canceled" || leaseOwner != nil {
		t.Fatalf("job state = %q owner %v, want canceled with nil owner", status, leaseOwner)
	}
}

func recordingJobReq(id int64, principal nodePrincipal) *http.Request {
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/recording/jobs/%d/complete", id), nil)
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", fmt.Sprintf("%d", id))
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, rc)
	ctx = context.WithValue(ctx, nodePrincipalContextKey, principal)
	return req.WithContext(ctx)
}

func deliveryPatchReq(id, accountID int64, delivery string) *http.Request {
	req := patchRequest(id, accountID, map[string]any{"delivery": delivery})
	return req
}

func schedulePatchReq(id, accountID int64, body map[string]any) *http.Request {
	return patchRequest(id, accountID, body)
}

func deliveryOf(t *testing.T, pool *pgxpool.Pool, recID int64) string {
	t.Helper()
	var d string
	if err := pool.QueryRow(context.Background(), `SELECT delivery FROM recordings WHERE id=$1`, recID).Scan(&d); err != nil {
		t.Fatalf("read delivery: %v", err)
	}
	return d
}

func insertPatchDestination(t *testing.T, pool *pgxpool.Pool, accountID int64) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO storage_destinations (account_id, name, managed) VALUES ($1, 'managed', true) RETURNING id
	`, accountID).Scan(&id); err != nil {
		t.Fatalf("insert destination: %v", err)
	}
	return id
}

func insertPatchRecording(t *testing.T, pool *pgxpool.Pool, accountID, destID int64, delivery string) int64 {
	t.Helper()
	start := time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC)
	var id int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO recordings (account_id, storage_destination_id, name, stream_url, source_kind, mode, cron_expr, cron_timezone, clip_duration_sec, status, start_at, delivery)
		VALUES ($1, $2, 'rec', 'https://example.com/live.m3u8', 'hls_live', 'sampled', '*/5 * * * *', 'UTC', 60, 'active', $3, $4)
		RETURNING id
	`, accountID, destID, start, delivery).Scan(&id); err != nil {
		t.Fatalf("insert recording: %v", err)
	}
	return id
}

// testRecordingPatchPool spins up a throwaway schema with the minimal table set the
// PATCH handlers read: recordings + its list-SELECT joins (storage_destinations,
// streams, account_billing, recording_clips) and connections. The
// account_auth_events insert is error-ignored in the handlers, so it is omitted here.
func testRecordingPatchPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()

	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed recording PATCH regression")
	}

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schema := fmt.Sprintf("api_recording_patch_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema)); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("parse db url: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
		t.Fatalf("open test pool: %v", err)
	}

	for _, stmt := range []string{
		`CREATE TABLE storage_destinations (
			id BIGSERIAL PRIMARY KEY,
			account_id BIGINT NOT NULL,
			name TEXT NOT NULL,
			managed BOOLEAN NOT NULL DEFAULT false
		)`,
		`CREATE TABLE streams (
			id BIGSERIAL PRIMARY KEY,
			name TEXT,
			location_text TEXT,
			local_timezone TEXT NOT NULL DEFAULT '',
			deleted_at TIMESTAMPTZ
		)`,
		`CREATE TABLE account_billing (
			account_id BIGINT PRIMARY KEY,
			has_payment_method BOOLEAN NOT NULL DEFAULT false
		)`,
		`CREATE TABLE connections (
			id BIGSERIAL PRIMARY KEY,
			account_id BIGINT NOT NULL,
			kind TEXT NOT NULL
		)`,
		`CREATE TABLE recordings (
			id BIGSERIAL PRIMARY KEY,
			account_id BIGINT NOT NULL,
			storage_destination_id BIGINT NOT NULL REFERENCES storage_destinations(id),
			delivery_storage_destination_id BIGINT,
			name TEXT NOT NULL,
			stream_url TEXT NOT NULL DEFAULT '',
			stream_id BIGINT,
			source_kind TEXT NOT NULL DEFAULT 'hls_live',
			mode TEXT NOT NULL DEFAULT 'sampled',
			cron_expr TEXT,
			cron_timezone TEXT NOT NULL DEFAULT 'UTC',
			clip_duration_sec INT NOT NULL DEFAULT 60,
			daily_window_start TIME,
			daily_window_end TIME,
			active_weekdays SMALLINT NOT NULL DEFAULT 127,
			target_fps INT,
			status TEXT NOT NULL DEFAULT 'active',
			next_fire_at TIMESTAMPTZ,
			last_enqueued_fire_at TIMESTAMPTZ,
			last_clip_at TIMESTAMPTZ,
			last_error_text TEXT NOT NULL DEFAULT '',
			last_error_at TIMESTAMPTZ,
			consecutive_failures INT NOT NULL DEFAULT 0,
			start_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			end_at TIMESTAMPTZ,
			storage_retention_tier TEXT NOT NULL DEFAULT 'monthly',
			delivery TEXT NOT NULL DEFAULT 'managed',
			capture_via TEXT NOT NULL DEFAULT 'cloud',
			naming_profile TEXT NOT NULL DEFAULT 'stoarama_v1',
			folder_name TEXT NOT NULL DEFAULT 'recordings',
			naming_metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE recording_clips (
			id BIGSERIAL PRIMARY KEY,
			recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
			recording_job_id BIGINT,
			storage_destination_id BIGINT,
			clip_start_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE recording_jobs (
			id BIGSERIAL PRIMARY KEY,
			recording_id BIGINT NOT NULL REFERENCES recordings(id) ON DELETE CASCADE,
			fire_at TIMESTAMPTZ NOT NULL,
			scheduled_for TIMESTAMPTZ NOT NULL,
			clip_duration_sec INT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			lease_owner TEXT,
			lease_expires_at TIMESTAMPTZ,
			attempt_count INT NOT NULL DEFAULT 0,
			error_text TEXT NOT NULL DEFAULT '',
			idempotency_key TEXT NOT NULL UNIQUE,
			kind TEXT NOT NULL DEFAULT 'clip',
			window_end_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			pool.Close()
			_, _ = admin.Exec(ctx, fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
			admin.Close()
			t.Fatalf("create test table: %v", err)
		}
	}

	return pool, func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema))
		admin.Close()
	}
}
