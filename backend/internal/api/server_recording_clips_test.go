package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRecordingJobsLeaseSQLLocksDropletCapacityGate(t *testing.T) {
	for _, want := range []string{"node_id = $2", "state IN ('provisioning', 'active')", "FOR UPDATE"} {
		if !strings.Contains(cloudRecorderLockSQL, want) {
			t.Fatalf("droplet lock SQL missing %q", want)
		}
	}
	for _, want := range []string{"live.lease_owner = $1", "live.lease_expires_at > now()", ") < $5"} {
		if !strings.Contains(cloudRecordingJobsLeaseSQL, want) {
			t.Fatalf("lease SQL missing %q", want)
		}
	}
}

func TestRecordingJobsLeaseRespectsDropletCapacityOne(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recorder_droplets (name, node_id, capacity, state)
		VALUES ('recorder-a', 1001, 1, 'active')
	`); err != nil {
		t.Fatalf("insert droplet: %v", err)
	}

	var recordingID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (account_id, storage_destination_id, name, stream_url, status, start_at)
		VALUES (42, 7, 'rec', 'https://example.test/live.m3u8', 'active', now() - interval '1 hour')
		RETURNING id
	`).Scan(&recordingID); err != nil {
		t.Fatalf("insert recording: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := pool.Exec(ctx, `
			INSERT INTO recording_jobs
				(recording_id, fire_at, scheduled_for, clip_duration_sec, status, idempotency_key)
			VALUES ($1, now() - interval '1 second', now() - interval '1 second', 60, 'pending', $2)
		`, recordingID, fmt.Sprintf("lease-capacity-%d", i)); err != nil {
			t.Fatalf("insert job %d: %v", i, err)
		}
	}

	principal := nodePrincipal{
		NodeID:      1001,
		AccountID:   42,
		NodeType:    nodeTypeLocalRecorder,
		DisplayName: "recorder-a",
	}
	wrongNode := principal
	wrongNode.NodeID++
	if got := leaseRecordingJobForTest(t, pool, wrongNode); got != nil {
		t.Fatalf("mismatched node leased job %d", got.JobID)
	}
	first := leaseRecordingJobForTest(t, pool, principal)
	if first == nil {
		t.Fatalf("first lease returned nil, want a job")
	}
	if second := leaseRecordingJobForTest(t, pool, principal); second != nil {
		t.Fatalf("second lease returned job %d, want nil while capacity is full", second.JobID)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE recording_jobs
		SET lease_expires_at = now() - interval '1 second'
		WHERE id=$1
	`, first.JobID); err != nil {
		t.Fatalf("expire first lease: %v", err)
	}
	third := leaseRecordingJobForTest(t, pool, principal)
	if third == nil {
		t.Fatalf("third lease returned nil, want another job after first lease expired")
	}
	if third.JobID == first.JobID {
		t.Fatalf("third lease reused first job %d, want the other pending job", third.JobID)
	}
}

func leaseRecordingJobForTest(t *testing.T, pool *pgxpool.Pool, principal nodePrincipal) *recordingLeaseResponse {
	t.Helper()

	s := &Server{pool: pool}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/jobs/lease", nil)
	req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, principal))
	rec := httptest.NewRecorder()

	s.handleRecordingJobsLease(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("lease status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Job *recordingLeaseResponse `json:"job"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode lease response: %v", err)
	}
	return payload.Job
}

func testRecordingLeasePool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()

	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run DB-backed lease regression")
	}

	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schema := fmt.Sprintf("api_recording_lease_%d", time.Now().UnixNano())
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
		`CREATE TABLE recorder_droplets (
			name TEXT NOT NULL,
			node_id BIGINT,
			capacity INTEGER NOT NULL,
			state TEXT NOT NULL
		)`,
		`CREATE TABLE account_billing (
			account_id BIGINT NOT NULL,
			has_payment_method BOOLEAN NOT NULL
		)`,
		`CREATE TABLE streams (
			id BIGSERIAL PRIMARY KEY,
			provider TEXT NOT NULL DEFAULT '',
			source_page_url TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE recordings (
			id BIGSERIAL PRIMARY KEY,
			stream_id BIGINT,
			account_id BIGINT NOT NULL,
			storage_destination_id BIGINT NOT NULL,
			name TEXT NOT NULL,
			stream_url TEXT NOT NULL,
			status TEXT NOT NULL,
			start_at TIMESTAMPTZ NOT NULL,
			end_at TIMESTAMPTZ,
			target_fps INTEGER,
			capture_via TEXT NOT NULL DEFAULT 'cloud'
		)`,
		`CREATE TABLE recording_jobs (
			id BIGSERIAL PRIMARY KEY,
			recording_id BIGINT NOT NULL,
			fire_at TIMESTAMPTZ NOT NULL,
			scheduled_for TIMESTAMPTZ NOT NULL,
			clip_duration_sec INTEGER NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			lease_owner TEXT,
			lease_expires_at TIMESTAMPTZ,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 3,
			idempotency_key TEXT NOT NULL UNIQUE,
			kind TEXT NOT NULL DEFAULT 'clip',
			window_end_at TIMESTAMPTZ,
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
