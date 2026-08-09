package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
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

func TestRecordingJobSurrenderReason(t *testing.T) {
	if !recordingJobSurrenderNoProgress.valid() {
		t.Fatal("no_progress surrender reason rejected")
	}
	if !recordingJobSurrenderDiskPressure.valid() {
		t.Fatal("disk_pressure surrender reason rejected")
	}
	if !recordingJobSurrenderSelfUpdate.valid() {
		t.Fatal("self_update surrender reason rejected")
	}
	if recordingJobSurrenderReason("capture_error").valid() {
		t.Fatal("unknown surrender reason accepted")
	}
}

func TestRelaySurrenderHandsJobToDifferentOwner(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO nodes (id, account_id, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES (1, 42, 'relay', 'active', now(), 1),
		       (2, 42, 'relay', 'active', now(), 1);
		INSERT INTO recordings
			(id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via)
		VALUES (1, 42, 7, 'continuous', 'https://example.test/live.m3u8', 'active', now()-interval '1 hour', 'relay');
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status,
			 lease_owner, lease_expires_at, attempt_count, idempotency_key, kind, window_end_at)
		VALUES (1, 1, now(), now(), 60, 'leased',
		        'node:1', now()+interval '3 minutes', 1, 'handoff', 'continuous_window', now()+interval '1 hour')
	`); err != nil {
		t.Fatal(err)
	}

	var handoffUntil time.Time
	if err := pool.QueryRow(ctx, recordingJobSurrenderSQL, 1, "node:1", string(recordingJobSurrenderNoProgress), nil).Scan(&handoffUntil); err != nil {
		t.Fatal(err)
	}
	if !handoffUntil.After(time.Now()) {
		t.Fatalf("handoff_until=%s is not in the future", handoffUntil)
	}

	s := &Server{pool: pool}
	if _, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("surrendering owner lease err=%v, want pgx.ErrNoRows", err)
	}
	job, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 2, AccountID: 42, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false)
	if err != nil {
		t.Fatal(err)
	}
	if job.JobID != 1 {
		t.Fatalf("different owner leased job=%d want 1", job.JobID)
	}
	var handoffOwner *string
	var persistedUntil *time.Time
	if err := pool.QueryRow(ctx, `SELECT handoff_owner, handoff_until FROM recording_jobs WHERE id=1`).Scan(&handoffOwner, &persistedUntil); err != nil {
		t.Fatal(err)
	}
	if handoffOwner != nil || persistedUntil != nil {
		t.Fatalf("handoff was not cleared on lease: owner=%v until=%v", handoffOwner, persistedUntil)
	}
}

func TestLeaseGenerationRejectsPreviousProcessOnSameNode(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO nodes (id, account_id, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES (1, 42, 'relay', 'active', now(), 1);
		INSERT INTO recordings
			(id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via)
		VALUES (1, 42, 7, 'continuous', 'https://example.test/live.m3u8', 'active', now()-interval '1 hour', 'relay');
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status,
			 attempt_count, idempotency_key, kind, window_end_at)
		VALUES (1, 1, now(), now(), 60, 'pending', 0, 'generation', 'continuous_window', now()+interval '1 hour')
	`); err != nil {
		t.Fatal(err)
	}

	principal := nodePrincipal{NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay}
	s := &Server{pool: pool}
	job, err := s.leaseRelayRecordingJob(ctx, principal, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, true)
	if err != nil {
		t.Fatal(err)
	}
	if job.LeaseToken == nil {
		t.Fatal("generation-aware lease returned no token")
	}
	if _, err := s.heartbeatRecordingJob(ctx, principal, job.JobID, "node:1", nil); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("tokenless previous process renewed replacement lease: err=%v", err)
	}
	if _, err := s.heartbeatRecordingJob(ctx, principal, job.JobID, "node:1", job.LeaseToken); err != nil {
		t.Fatalf("current lease generation could not renew: %v", err)
	}
}

func TestGenericFailRetainsMaxAttemptSemantics(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings
			(id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via)
		VALUES (1, 42, 7, 'continuous', 'https://example.test/live.m3u8', 'active', now()-interval '1 hour', 'relay');
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status,
			 lease_owner, lease_expires_at, attempt_count, max_attempts, idempotency_key, kind, window_end_at)
		VALUES (1, 1, now(), now(), 60, 'leased',
		        'node:1', now()+interval '3 minutes', 3, 3, 'generic-fail', 'continuous_window', now()+interval '1 hour')
	`); err != nil {
		t.Fatal(err)
	}
	var recordingID int64
	if err := pool.QueryRow(ctx, recordingJobFailSQL, 1, "node:1", "capture failed", nil).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	var status string
	var completedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT status, completed_at FROM recording_jobs WHERE id=1`).Scan(&status, &completedAt); err != nil {
		t.Fatal(err)
	}
	if status != "error" || completedAt == nil {
		t.Fatalf("status=%q completed_at=%v, want error with completion", status, completedAt)
	}
}

func TestRecordingUploadIntentReplaysPendingAndConsumedSegment(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	secrets, err := secretbox.NewFromBase64Key("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := secrets.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO storage_destinations
			(id, account_id, endpoint, region, bucket, key_prefix, access_key_id, secret_access_key_enc)
		VALUES (7, 42, 'https://example.r2.cloudflarestorage.com', 'auto', 'bucket', 'prefix', 'access', $1)
	`, sealed); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recordings
			(id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via,
			 cron_timezone, naming_profile, folder_name, naming_metadata_jsonb)
		VALUES (1, 42, 7, 'continuous', 'https://example.test/live.m3u8', 'active', now()-interval '1 hour', 'relay',
		        'UTC', 'stoarama_v1', 'recordings', '{}'::jsonb)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status,
			 lease_owner, lease_expires_at, attempt_count, idempotency_key, kind, window_end_at)
		VALUES (1, 1, now(), now(), 60, 'leased',
		        'node:1', now()+interval '3 minutes', 1, 'intent-replay', 'continuous_window', now()+interval '1 hour')
	`); err != nil {
		t.Fatal(err)
	}
	server := &Server{
		cfg:     config.Config{R2SignPutTTL: 15 * time.Minute},
		pool:    pool,
		secrets: secrets,
	}
	reserve := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(
			http.MethodPost,
			"/api/v1/recording/upload-intents",
			strings.NewReader(`{"job_id":1,"mime_type":"video/mp4","segment_start_ms":1785240000000}`),
		)
		req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, nodePrincipal{
			NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay,
		}))
		response := httptest.NewRecorder()
		server.handleRecordingUploadIntent(response, req)
		return response
	}

	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() {
			<-start
			responses <- reserve()
		}()
	}
	close(start)
	first, second := <-responses, <-responses
	if !((first.Code == http.StatusCreated && second.Code == http.StatusOK) ||
		(first.Code == http.StatusOK && second.Code == http.StatusCreated)) {
		t.Fatalf("concurrent reserve statuses=%d/%d bodies=%s / %s",
			first.Code, second.Code, first.Body.String(), second.Body.String())
	}
	var firstPayload, secondPayload map[string]any
	if err := json.Unmarshal(first.Body.Bytes(), &firstPayload); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondPayload); err != nil {
		t.Fatal(err)
	}
	if firstPayload["intent_id"] != secondPayload["intent_id"] {
		t.Fatalf("replay intent=%v want %v", secondPayload["intent_id"], firstPayload["intent_id"])
	}
	replayed := reserve()
	if replayed.Code != http.StatusOK {
		t.Fatalf("pending replay status=%d body=%s", replayed.Code, replayed.Body.String())
	}
	var intentCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_upload_intents`).Scan(&intentCount); err != nil {
		t.Fatal(err)
	}
	if intentCount != 1 {
		t.Fatalf("intent rows=%d want 1", intentCount)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_upload_intents SET status='consumed'`); err != nil {
		t.Fatal(err)
	}
	consumed := reserve()
	if consumed.Code != http.StatusOK {
		t.Fatalf("consumed replay status=%d body=%s", consumed.Code, consumed.Body.String())
	}
	var consumedPayload struct {
		AlreadyIngested bool `json:"already_ingested"`
	}
	if err := json.Unmarshal(consumed.Body.Bytes(), &consumedPayload); err != nil {
		t.Fatal(err)
	}
	if !consumedPayload.AlreadyIngested {
		t.Fatalf("consumed replay body=%s", consumed.Body.String())
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
	start := make(chan struct{})
	jobs := make([]*recordingLeaseResponse, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < len(jobs); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			jobs[index], errs[index] = leaseRecordingJob(pool, principal)
		}(i)
	}
	close(start)
	wg.Wait()

	var first *recordingLeaseResponse
	leased := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent lease %d: %v", i, err)
		}
		if jobs[i] != nil {
			leased++
			first = jobs[i]
		}
	}
	if leased != 1 {
		t.Fatalf("concurrent leases returned %d jobs, want exactly 1", leased)
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

// TestRecordingJobsLeaseRefusesDrainingDroplet pins the invariant that makes a
// forced roll actually roll: `stoaramactl recorder-control drain` only flips the
// droplet to draining, and everything after that depends on the droplet then
// taking no further work. If a draining droplet could still lease, the drain
// would look like it succeeded while the old capture binary kept picking up new
// windows until the drain timeout destroyed it mid-capture.
func TestRecordingJobsLeaseRefusesDrainingDroplet(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO recorder_droplets (name, node_id, capacity, state)
		VALUES ('recorder-a', 1001, 5, 'active')
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
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_jobs
			(recording_id, fire_at, scheduled_for, clip_duration_sec, status, idempotency_key)
		VALUES ($1, now() - interval '1 second', now() - interval '1 second', 60, 'pending', 'drain-gate-0')
	`, recordingID); err != nil {
		t.Fatalf("insert job: %v", err)
	}

	principal := nodePrincipal{
		NodeID:      1001,
		AccountID:   42,
		NodeType:    nodeTypeLocalRecorder,
		DisplayName: "recorder-a",
	}

	// The job is leasable while the droplet is active, so a nil lease after draining
	// is attributable to the state and not to some unrelated gate in the same query.
	active := leaseRecordingJobForTest(t, pool, principal)
	if active == nil {
		t.Fatalf("active droplet leased nothing, want the pending job")
	}
	if _, err := pool.Exec(ctx, `
		UPDATE recording_jobs SET status='pending', lease_owner=NULL, lease_expires_at=NULL WHERE id=$1
	`, active.JobID); err != nil {
		t.Fatalf("return job to pending: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE recorder_droplets SET state='draining' WHERE name='recorder-a'
	`); err != nil {
		t.Fatalf("mark draining: %v", err)
	}
	if got := leaseRecordingJobForTest(t, pool, principal); got != nil {
		t.Fatalf("draining droplet leased job %d, want no lease", got.JobID)
	}
}

func TestManagedCloudRecorderBindsAuthenticatedNodeID(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	if _, err := pool.Exec(context.Background(), `
		INSERT INTO recorder_droplets (name, node_id, capacity, state)
		VALUES ('recorder-a', 1001, 1, 'active')
	`); err != nil {
		t.Fatalf("insert droplet: %v", err)
	}
	s := &Server{pool: pool}
	principal := nodePrincipal{NodeID: 1001, NodeType: nodeTypeLocalRecorder, DisplayName: "recorder-a"}
	managed, err := s.isManagedCloudRecorder(context.Background(), principal)
	if err != nil || !managed {
		t.Fatalf("matching principal managed=%v err=%v, want true", managed, err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE recorder_droplets SET state='destroyed' WHERE name='recorder-a'`); err != nil {
		t.Fatalf("destroy droplet: %v", err)
	}
	managed, err = s.isManagedCloudRecorder(context.Background(), principal)
	if err != nil || managed {
		t.Fatalf("destroyed principal managed=%v err=%v, want false", managed, err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE recorder_droplets SET state='active' WHERE name='recorder-a'`); err != nil {
		t.Fatalf("reactivate droplet: %v", err)
	}
	principal.NodeID++
	managed, err = s.isManagedCloudRecorder(context.Background(), principal)
	if err != nil || managed {
		t.Fatalf("mismatched principal managed=%v err=%v, want false", managed, err)
	}
}

func leaseRecordingJobForTest(t *testing.T, pool *pgxpool.Pool, principal nodePrincipal) *recordingLeaseResponse {
	t.Helper()
	job, err := leaseRecordingJob(pool, principal)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func leaseRecordingJob(pool *pgxpool.Pool, principal nodePrincipal) (*recordingLeaseResponse, error) {
	s := &Server{pool: pool}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/jobs/lease", nil)
	req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, principal))
	rec := httptest.NewRecorder()

	s.handleRecordingJobsLease(rec, req)
	if rec.Code != http.StatusOK {
		return nil, fmt.Errorf("lease status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Job *recordingLeaseResponse `json:"job"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		return nil, fmt.Errorf("decode lease response: %w", err)
	}
	return payload.Job, nil
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
		`CREATE TABLE relay_groups (
			id BIGINT PRIMARY KEY,
			account_id BIGINT NOT NULL,
			max_streams INTEGER NOT NULL
		)`,
		`CREATE TABLE nodes (
			id BIGINT PRIMARY KEY,
			account_id BIGINT NOT NULL,
			node_type TEXT NOT NULL,
			status TEXT NOT NULL,
			last_heartbeat_at TIMESTAMPTZ,
			relay_max_streams INTEGER NOT NULL,
			relay_group_id BIGINT
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
			capture_via TEXT NOT NULL DEFAULT 'cloud',
			cron_timezone TEXT NOT NULL DEFAULT 'UTC',
			naming_profile TEXT NOT NULL DEFAULT 'stoarama_v1',
			folder_name TEXT NOT NULL DEFAULT 'recordings',
			naming_metadata_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb
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
			lease_token UUID,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 3,
			idempotency_key TEXT NOT NULL UNIQUE,
			kind TEXT NOT NULL DEFAULT 'clip',
			window_end_at TIMESTAMPTZ,
			handoff_owner TEXT,
			handoff_until TIMESTAMPTZ,
			error_text TEXT,
			completed_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE storage_destinations (
			id BIGINT PRIMARY KEY,
			account_id BIGINT NOT NULL,
			endpoint TEXT NOT NULL,
			region TEXT NOT NULL,
			bucket TEXT NOT NULL,
			key_prefix TEXT NOT NULL,
			access_key_id TEXT NOT NULL,
			secret_access_key_enc BYTEA NOT NULL
		)`,
		`CREATE TABLE recording_upload_intents (
			id UUID PRIMARY KEY,
			recording_id BIGINT NOT NULL,
			recording_job_id BIGINT NOT NULL,
			storage_destination_id BIGINT NOT NULL,
			endpoint TEXT NOT NULL,
			bucket TEXT NOT NULL,
			object_key TEXT NOT NULL,
			display_path TEXT NOT NULL,
			mime_type TEXT NOT NULL,
			max_size_bytes BIGINT NOT NULL,
			status TEXT NOT NULL,
			expires_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
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
