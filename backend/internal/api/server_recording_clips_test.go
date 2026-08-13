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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
)

const testRelayNodesTableDDL = `CREATE TABLE nodes (
	id BIGINT PRIMARY KEY,
	account_id BIGINT NOT NULL,
	node_type TEXT NOT NULL,
	status TEXT NOT NULL,
	last_heartbeat_at TIMESTAMPTZ,
	relay_max_streams INTEGER NOT NULL,
	relay_group_id BIGINT,
	capabilities_jsonb JSONB NOT NULL DEFAULT '{}'::jsonb
)`

const testRecordingCanaryReservationsTableDDL = `CREATE TABLE recording_canary_reservations (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	recording_id BIGINT NOT NULL,
	node_id BIGINT NOT NULL,
	expires_at TIMESTAMPTZ NOT NULL,
	window_start_at TIMESTAMPTZ,
	preopen_stage TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

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

func TestValidTimestampProvenanceVersionParity(t *testing.T) {
	attempt := "123e4567-e89b-12d3-a456-426614174000"
	contract := &capture.TimestampContract{Version: 1, Mode: "muxed_source_copy", AudioSelection: "first_optional", Tracks: []capture.TrackTimingContract{{
		StreamIndex: 0, MediaType: "video", TimeBaseNum: 1, TimeBaseDen: 1000,
		FirstTimestamp: 0, LastTimestamp: 1000, LastDuration: 40, UnitCount: 26,
		CodecSignatureSHA256: strings.Repeat("a", 64),
	}}}
	tests := []struct {
		name string
		req  recordingClipIngestRequest
		want bool
	}{
		{"complete", recordingClipIngestRequest{CaptureAttemptID: attempt, TimestampContractVersion: capture.TimestampVersionContinuousSourcePTSV1, TimestampContractStatus: capture.TimestampProbeComplete, TimestampContract: contract}, true},
		{"complete missing version", recordingClipIngestRequest{CaptureAttemptID: attempt, TimestampContractStatus: capture.TimestampProbeComplete, TimestampContract: contract}, false},
		{"unknown", recordingClipIngestRequest{CaptureAttemptID: attempt, TimestampContractStatus: capture.TimestampProbeUnknown, TimestampContractReason: "missing_terminal_duration"}, true},
		{"unknown must omit version", recordingClipIngestRequest{CaptureAttemptID: attempt, TimestampContractVersion: capture.TimestampVersionContinuousSourcePTSV1, TimestampContractStatus: capture.TimestampProbeUnknown, TimestampContractReason: "missing_terminal_duration"}, false},
		{"video contract rejects claimed audio", recordingClipIngestRequest{CaptureAttemptID: attempt, TimestampContractVersion: capture.TimestampVersionContinuousSourcePTSV1, TimestampContractStatus: capture.TimestampProbeComplete, TimestampContract: contract, AudioPresent: true}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, got := validTimestampProvenance(tc.req)
			if got != tc.want {
				t.Fatalf("valid=%v want %v", got, tc.want)
			}
		})
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

func TestSanitizeRecordingSurrenderError(t *testing.T) {
	raw := "open https://user:pass@hd-auth.skylinewebcams.com/live.m3u8?a=secret token=secret\nAuthorization: Bearer abc.def " + strings.Repeat("x", 800)
	got := sanitizeRecordingSurrenderError(raw, "no_progress")
	for _, forbidden := range []string{"user:pass", "a=secret", "token=secret", "abc.def", "\n"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("sanitized error retained %q: %q", forbidden, got)
		}
	}
	if !strings.Contains(got, "https://hd-auth.skylinewebcams.com/live.m3u8?[query]") {
		t.Fatalf("sanitized error omitted safe URL shape: %q", got)
	}
	if len([]rune(got)) > 503 {
		t.Fatalf("sanitized error length=%d", len([]rune(got)))
	}
}

func TestRelaySurrenderHandsJobToDifferentOwner(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts (id) VALUES (42);
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
	if _, err := pool.Exec(ctx, `
		UPDATE recording_jobs
		SET status='pending', scheduled_for=now(), lease_owner=NULL, lease_expires_at=NULL,
		    window_end_at=now()-interval '1 minute'
		WHERE id=1
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay}, true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, false); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expired relay window lease err=%v, want pgx.ErrNoRows", err)
	}
}

func TestRelayLeaseAtomicallyPersistsTimestampAdmission(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	raw, err := os.ReadFile("../../../infra/sql/migrations/0132_recording_clip_timestamp_contract.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id) VALUES(42);
		INSERT INTO nodes(id,account_id,node_type,status,last_heartbeat_at,relay_max_streams,capabilities_jsonb) VALUES(77,42,'relay','active',now(),4,'{"continuous_source_pts_v1":true}');
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,capture_via) VALUES(445,42,7,'canary','https://example/live.m3u8','active',now()-interval '1 hour','relay');
		INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at) VALUES(700,445,now(),now(),60,'pending','atomic-admit','continuous_window',now()+interval '1 hour')
	`); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, cfg: config.Config{ContinuousSourcePTSCanary: "77:445"}}
	principal := nodePrincipal{NodeID: 77, AccountID: 42, NodeType: nodeTypeRelay}
	resp, err := s.leaseRelayRecordingJob(ctx, principal, true, 150, true)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.TimestampContractSupported || resp.LeaseToken == nil {
		t.Fatalf("lease=%+v", resp)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_timestamp_contract_admissions WHERE recording_job_id=700 AND lease_token=$1 AND node_id=77 AND account_id=42 AND recording_id=445`, *resp.LeaseToken).Scan(&count); err != nil || count != 1 {
		t.Fatalf("admission count=%d err=%v", count, err)
	}
	// Later policy/capability changes cannot retroactively remove or forge the exact
	// immutable generation admission already committed with the lease.
	s.cfg.ContinuousSourcePTSCanary = ""
	if _, err := pool.Exec(ctx, `UPDATE nodes SET capabilities_jsonb='{}'`); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_timestamp_contract_admissions WHERE lease_token=$1`, *resp.LeaseToken).Scan(&count); err != nil || count != 1 {
		t.Fatalf("durable admission count=%d err=%v", count, err)
	}
}

func TestRelayLeaseTimestampAdmissionNegativeMatrix(t *testing.T) {
	tests := []struct {
		name, config, capability, status string
		age                              time.Duration
		target                           *int
	}{
		{"disabled", "", `{"continuous_source_pts_v1":true}`, "active", 0, nil},
		{"wrong pair", "77:446", `{"continuous_source_pts_v1":true}`, "active", 0, nil},
		{"stale", "77:445", `{"continuous_source_pts_v1":true}`, "active", 3 * time.Minute, nil},
		{"inactive", "77:445", `{"continuous_source_pts_v1":true}`, "inactive", 0, nil},
		{"nonboolean", "77:445", `{"continuous_source_pts_v1":"true"}`, "active", 0, nil},
		{"target fps", "77:445", `{"continuous_source_pts_v1":true}`, "active", 0, func() *int { v := 15; return &v }()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool, cleanup := testRecordingLeasePool(t)
			defer cleanup()
			ctx := context.Background()
			raw, err := os.ReadFile("../../../infra/sql/migrations/0132_recording_clip_timestamp_contract.sql")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, string(raw)); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `INSERT INTO accounts VALUES(42)`); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `INSERT INTO nodes(id,account_id,node_type,status,last_heartbeat_at,relay_max_streams,capabilities_jsonb) VALUES(77,42,'relay',$1,now()-$2::interval,4,$3::jsonb)`, tc.status, tc.age.String(), tc.capability); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,target_fps,capture_via) VALUES(445,42,7,'canary','https://example/live','active',now()-interval '1 hour',$1,'relay')`, tc.target); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at) VALUES(700,445,now(),now(),60,'pending','matrix','continuous_window',now()+interval '1 hour')`); err != nil {
				t.Fatal(err)
			}
			s := &Server{pool: pool, cfg: config.Config{ContinuousSourcePTSCanary: tc.config}}
			resp, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 77, AccountID: 42, NodeType: nodeTypeRelay}, true, 150, true)
			if (tc.status == "inactive" || tc.age >= 2*time.Minute) && errors.Is(err, pgx.ErrNoRows) {
				err = nil
			}
			if err != nil {
				t.Fatal(err)
			}
			if resp.TimestampContractSupported {
				t.Fatalf("unexpected admission lease=%+v", resp)
			}
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_timestamp_contract_admissions`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("admissions=%d err=%v", count, err)
			}
		})
	}
}

func TestTimestampAdmissionRequiresGenerationAwareRelayLease(t *testing.T) {
	for _, tc := range []struct {
		name, nodeType, captureVia string
		tokenSupported             bool
	}{
		{"zero lease token", "relay", "relay", false},
		{"non-relay node", "local_recorder", "relay", true},
		{"cloud recording", "relay", "cloud", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, cleanup := testRecordingLeasePool(t)
			defer cleanup()
			ctx := context.Background()
			raw, err := os.ReadFile("../../../infra/sql/migrations/0132_recording_clip_timestamp_contract.sql")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, string(raw)); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `INSERT INTO accounts VALUES(42)`); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `INSERT INTO nodes(id,account_id,node_type,status,last_heartbeat_at,relay_max_streams,capabilities_jsonb) VALUES(77,42,$1,'active',now(),4,'{"continuous_source_pts_v1":true}')`, tc.nodeType); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,capture_via) VALUES(445,42,7,'canary','https://example/live','active',now()-interval '1 hour',$1)`, tc.captureVia); err != nil {
				t.Fatal(err)
			}
			if _, err = pool.Exec(ctx, `INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at) VALUES(700,445,now(),now(),60,'pending','generation-gate','continuous_window',now()+interval '1 hour')`); err != nil {
				t.Fatal(err)
			}
			s := &Server{pool: pool, cfg: config.Config{ContinuousSourcePTSCanary: "77:445"}}
			resp, leaseErr := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 77, AccountID: 42, NodeType: tc.nodeType}, true, 150, tc.tokenSupported)
			if tc.nodeType == "relay" && tc.captureVia == "relay" && leaseErr != nil {
				t.Fatal(leaseErr)
			}
			if resp.TimestampContractSupported {
				t.Fatalf("unexpected admission lease=%+v", resp)
			}
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_timestamp_contract_admissions`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("admissions=%d err=%v", count, err)
			}
		})
	}
}

func TestTimestampAdmissionCannotBeActivatedAfterLeaseAndIsTokenIsolated(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	raw, err := os.ReadFile("../../../infra/sql/migrations/0132_recording_clip_timestamp_contract.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO accounts VALUES(42); INSERT INTO nodes(id,account_id,node_type,status,last_heartbeat_at,relay_max_streams,capabilities_jsonb) VALUES(77,42,'relay','active',now(),4,'{"continuous_source_pts_v1":true}'); INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,capture_via) VALUES(445,42,7,'canary','https://example/live','active',now()-interval '1 hour','relay'); INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at) VALUES(700,445,now(),now(),60,'pending','token-isolation','continuous_window',now()+interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	principal := nodePrincipal{NodeID: 77, AccountID: 42, NodeType: nodeTypeRelay}
	s := &Server{pool: pool}
	first, err := s.leaseRelayRecordingJob(ctx, principal, true, 150, true)
	if err != nil || first.LeaseToken == nil {
		t.Fatalf("first lease=%+v err=%v", first, err)
	}
	s.cfg.ContinuousSourcePTSCanary = "77:445"
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_timestamp_contract_admissions`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("post-activation admissions=%d err=%v", count, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,scheduled_for=now() WHERE id=700`); err != nil {
		t.Fatal(err)
	}
	second, err := s.leaseRelayRecordingJob(ctx, principal, true, 150, true)
	if err != nil || second.LeaseToken == nil || *second.LeaseToken == *first.LeaseToken || !second.TimestampContractSupported {
		t.Fatalf("second lease=%+v first=%+v err=%v", second, first, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_timestamp_contract_admissions WHERE lease_token=$1`, *first.LeaseToken).Scan(&count); err != nil || count != 0 {
		t.Fatalf("old token admissions=%d err=%v", count, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_timestamp_contract_admissions WHERE lease_token=$1`, *second.LeaseToken).Scan(&count); err != nil || count != 1 {
		t.Fatalf("new token admissions=%d err=%v", count, err)
	}
}

func TestTimestampAdmissionFailureRollsBackLease(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	raw, err := os.ReadFile("../../../infra/sql/migrations/0132_recording_clip_timestamp_contract.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `CREATE FUNCTION aaa_test_reject_timestamp_admission() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced admission failure'; END $$; CREATE TRIGGER aaa_test_reject_timestamp_admission BEFORE INSERT ON recording_timestamp_contract_admissions FOR EACH ROW EXECUTE FUNCTION aaa_test_reject_timestamp_admission(); INSERT INTO accounts VALUES(42); INSERT INTO nodes(id,account_id,node_type,status,last_heartbeat_at,relay_max_streams,capabilities_jsonb) VALUES(77,42,'relay','active',now(),4,'{"continuous_source_pts_v1":true}'); INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,capture_via) VALUES(445,42,7,'canary','https://example/live','active',now()-interval '1 hour','relay'); INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,kind,window_end_at) VALUES(700,445,now(),now(),60,'pending','rollback-admit','continuous_window',now()+interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool, cfg: config.Config{ContinuousSourcePTSCanary: "77:445"}}
	if _, err := s.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: 77, AccountID: 42, NodeType: nodeTypeRelay}, true, 150, true); err == nil {
		t.Fatal("forced admission failure unexpectedly leased")
	}
	var status string
	var owner, token *string
	if err := pool.QueryRow(ctx, `SELECT status,lease_owner,lease_token::text FROM recording_jobs WHERE id=700`).Scan(&status, &owner, &token); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || owner != nil || token != nil {
		t.Fatalf("lease was not rolled back: status=%s owner=%v token=%v", status, owner, token)
	}
}

func TestCloudSurrenderExcludesPriorOwnerAndPreservesClips(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts (id) VALUES (42);
		INSERT INTO nodes (id, account_id, node_type, status, last_heartbeat_at, relay_max_streams)
		VALUES (10, 42, 'local_recorder', 'active', now(), 1),
		       (11, 42, 'local_recorder', 'active', now(), 1);
		INSERT INTO recorder_droplets (name, node_id, state, capacity)
		VALUES ('cloud-a', 10, 'active', 1),
		       ('cloud-b', 11, 'active', 1);
		INSERT INTO recordings
			(id, account_id, storage_destination_id, name, stream_url, status, start_at, capture_via)
		VALUES (1, 42, 7, 'continuous', 'https://example.test/live.m3u8', 'active', now()-interval '1 hour', 'cloud');
		INSERT INTO recording_jobs
			(id, recording_id, fire_at, scheduled_for, clip_duration_sec, status,
			 lease_owner, lease_expires_at, lease_token, attempt_count, idempotency_key, kind, window_end_at)
		VALUES (1, 1, now(), now(), 60, 'leased',
		        'cloud-a', now()+interval '3 minutes', '00000000-0000-0000-0000-000000000001', 1, 'cloud-handoff', 'continuous_window', now()+interval '1 hour');
		INSERT INTO recording_clips (recording_job_id, capture_lease_token)
		VALUES (1, '00000000-0000-0000-0000-000000000099')
	`); err != nil {
		t.Fatal(err)
	}

	server := &Server{pool: pool}
	surrender := func(body, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/jobs/1/surrender", strings.NewReader(body))
		req = req.WithContext(recordingJobReq(1, nodePrincipal{
			NodeID: 10, AccountID: 42, NodeType: nodeTypeLocalRecorder, DisplayName: "cloud-a",
		}).Context())
		req.Header.Set(recordingLeaseTokenHeader, token)
		rec := httptest.NewRecorder()
		server.handleRecordingJobSurrender(rec, req)
		return rec
	}
	if rec := surrender(`{"reason":"disk_pressure"}`, "00000000-0000-0000-0000-000000000001"); rec.Code != http.StatusBadRequest {
		t.Fatalf("cloud non-no-progress surrender status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := surrender(`{"reason":"no_progress"}`, "00000000-0000-0000-0000-000000000002"); rec.Code != http.StatusConflict {
		t.Fatalf("cloud wrong-generation surrender status=%d body=%s", rec.Code, rec.Body.String())
	}
	var guardedStatus, guardedOwner string
	if err := pool.QueryRow(ctx, `SELECT status,lease_owner FROM recording_jobs WHERE id=1`).Scan(&guardedStatus, &guardedOwner); err != nil {
		t.Fatal(err)
	}
	if guardedStatus != "leased" || guardedOwner != "cloud-a" {
		t.Fatalf("rejected surrender mutated job: status=%q owner=%q", guardedStatus, guardedOwner)
	}
	rec := surrender(`{
		"reason":"no_progress",
		"error_text":"skyline manifest contains no playable media segments"
	}`, "00000000-0000-0000-0000-000000000001")
	if rec.Code != http.StatusOK {
		t.Fatalf("cloud surrender status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response struct {
		HandoffUntil time.Time `json:"handoff_until"`
		NextRetryAt  time.Time `json:"next_retry_at"`
		HadClips     bool      `json:"had_clips"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.HadClips {
		t.Fatal("zero-clip job reported landed clips")
	}
	delay := time.Until(response.NextRetryAt)
	if delay < 50*time.Second || delay > 70*time.Second {
		t.Fatalf("first no-progress retry delay=%s want about 1m", delay)
	}
	var blocked recordingLeaseResponse
	err := pool.QueryRow(ctx, cloudRecordingJobsLeaseSQL,
		"cloud-a", true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, recordingFreshnessGraceSec, 1, false).Scan(
		&blocked.JobID, &blocked.RecordingID, &blocked.SourceURL, &blocked.StreamID, &blocked.StreamProvider, &blocked.SourcePageURL, &blocked.ClipDurationSec,
		&blocked.StorageDestinationID, &blocked.FireAt, &blocked.AttemptCount, &blocked.LeaseExpiresAt, &blocked.TargetFPS, &blocked.Kind, &blocked.WindowEndAt, &blocked.LeaseToken,
	)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("cloud job leased before retry deadline: err=%v", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET scheduled_for=now() WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	var priorOwner recordingLeaseResponse
	err = pool.QueryRow(ctx, cloudRecordingJobsLeaseSQL,
		"cloud-a", true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, recordingFreshnessGraceSec, 1, false).Scan(
		&priorOwner.JobID, &priorOwner.RecordingID, &priorOwner.SourceURL, &priorOwner.StreamID, &priorOwner.StreamProvider, &priorOwner.SourcePageURL, &priorOwner.ClipDurationSec,
		&priorOwner.StorageDestinationID, &priorOwner.FireAt, &priorOwner.AttemptCount, &priorOwner.LeaseExpiresAt, &priorOwner.TargetFPS, &priorOwner.Kind, &priorOwner.WindowEndAt, &priorOwner.LeaseToken,
	)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("prior owner leased job during active handoff: err=%v", err)
	}
	var job recordingLeaseResponse
	err = pool.QueryRow(ctx, cloudRecordingJobsLeaseSQL,
		"cloud-b", true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, recordingFreshnessGraceSec, 1, true).Scan(
		&job.JobID, &job.RecordingID, &job.SourceURL, &job.StreamID, &job.StreamProvider, &job.SourcePageURL, &job.ClipDurationSec,
		&job.StorageDestinationID, &job.FireAt, &job.AttemptCount, &job.LeaseExpiresAt, &job.TargetFPS, &job.Kind, &job.WindowEndAt, &job.LeaseToken,
	)
	if err != nil {
		t.Fatal(err)
	}
	if job.JobID != 1 {
		t.Fatalf("different cloud egress leased job=%d want 1", job.JobID)
	}
	var handoffOwner *string
	var persistedUntil *time.Time
	if err := pool.QueryRow(ctx, `SELECT handoff_owner, handoff_until FROM recording_jobs WHERE id=1`).Scan(&handoffOwner, &persistedUntil); err != nil {
		t.Fatal(err)
	}
	if handoffOwner != nil || persistedUntil != nil {
		t.Fatalf("cloud handoff was not cleared on lease: owner=%v until=%v", handoffOwner, persistedUntil)
	}
	var errorText string
	if err := pool.QueryRow(ctx, `SELECT error_text FROM recording_jobs WHERE id=1`).Scan(&errorText); err != nil {
		t.Fatal(err)
	}
	if errorText != "skyline manifest contains no playable media segments" {
		t.Fatalf("error_text=%q", errorText)
	}

	if job.LeaseToken == nil {
		t.Fatal("generation-aware cloud lease returned no token")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips (recording_job_id, capture_lease_token) VALUES (1, $1)`, job.LeaseToken); err != nil {
		t.Fatal(err)
	}
	var landedHandoffUntil, landedNextRetryAt time.Time
	var landedHadClips bool
	if err := pool.QueryRow(ctx, recordingJobCloudSurrenderSQL, 1, "cloud-b", "capture stopped after landed media", job.LeaseToken, 11).Scan(
		&landedHandoffUntil, &landedNextRetryAt, &landedHadClips,
	); err != nil {
		t.Fatal(err)
	}
	if !landedHadClips {
		t.Fatal("landed clip was not recognized during cloud handoff")
	}
	if delay := time.Until(landedNextRetryAt); delay < -5*time.Second || delay > 5*time.Second {
		t.Fatalf("landed-media retry delay=%s want immediate", delay)
	}
	var clipCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM recording_clips WHERE recording_job_id=1`).Scan(&clipCount); err != nil {
		t.Fatal(err)
	}
	if clipCount != 2 {
		t.Fatalf("clip count=%d want old and current generation clips preserved", clipCount)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE recording_jobs
		SET scheduled_for=now(), handoff_owner=NULL, handoff_until=NULL,
		    window_end_at=now()-interval '1 minute'
		WHERE id=1
	`); err != nil {
		t.Fatal(err)
	}
	var expired recordingLeaseResponse
	err = pool.QueryRow(ctx, cloudRecordingJobsLeaseSQL,
		"cloud-c", true, recordingCaptureTimeoutMarginSec+recordingUploadMarginSec, recordingFreshnessGraceSec, 1, false).Scan(
		&expired.JobID, &expired.RecordingID, &expired.SourceURL, &expired.StreamID, &expired.StreamProvider, &expired.SourcePageURL, &expired.ClipDurationSec,
		&expired.StorageDestinationID, &expired.FireAt, &expired.AttemptCount, &expired.LeaseExpiresAt, &expired.TargetFPS, &expired.Kind, &expired.WindowEndAt, &expired.LeaseToken,
	)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expired cloud window lease err=%v, want pgx.ErrNoRows", err)
	}
}

func TestLeaseGenerationRejectsPreviousProcessOnSameNode(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()

	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts (id) VALUES (42);
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

func TestRecordingHeartbeatCapsContinuousWindowDrain(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id) VALUES(42);
		INSERT INTO nodes(id,account_id,node_type,status,last_heartbeat_at,relay_max_streams)
		VALUES(1,42,'relay','active',now(),4),(2,42,'relay','active',now(),4);
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,capture_via)
		VALUES(1,42,1,'heartbeat','https://example.test/live.m3u8','active',now()-interval '1 hour','relay');
		INSERT INTO recording_jobs
			(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,
			 attempt_count,idempotency_key,kind,window_end_at)
		VALUES
			(1,1,now(),now(),60,'leased','node:1',now()+interval '5 minutes',1,'heartbeat-far','continuous_window',now()+interval '1 hour'),
			(2,1,now(),now(),60,'leased','node:1',now()+interval '5 minutes',1,'heartbeat-near','continuous_window',now()-interval '45 minutes'),
			(3,1,now(),now(),60,'leased','node:1',now()+interval '5 minutes',1,'heartbeat-past','continuous_window',now()-interval '47 minutes'),
			(4,1,now(),now(),60,'leased','node:1',now()+interval '5 minutes',1,'heartbeat-malformed','continuous_window',NULL),
			(5,1,now(),now(),60,'leased','node:1',now()+interval '5 minutes',1,'heartbeat-clip','clip',NULL)
	`); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	principal := nodePrincipal{NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay}
	dbNow := func() time.Time {
		var now time.Time
		if err := pool.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
			t.Fatal(err)
		}
		return now
	}

	farBefore := dbNow()
	far, err := s.heartbeatRecordingJob(ctx, principal, 1, "node:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	normalLease := time.Duration(60+recordingCaptureTimeoutMarginSec+recordingUploadMarginSec) * time.Second
	if got := far.Sub(farBefore); got < normalLease-time.Second || got > normalLease+time.Second {
		t.Fatalf("far continuous lease=%s want normal rolling lease %s", got, normalLease)
	}

	var fixedCap time.Time
	if err := pool.QueryRow(ctx, `SELECT window_end_at+make_interval(secs=>$2) FROM recording_jobs WHERE id=$1`, 2, recordingContinuousPostWindowLeaseSec).Scan(&fixedCap); err != nil {
		t.Fatal(err)
	}
	near, err := s.heartbeatRecordingJob(ctx, principal, 2, "node:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !near.Equal(fixedCap) {
		t.Fatalf("near-close lease=%s want exact fixed cap %s", near, fixedCap)
	}
	nearAgain, err := s.heartbeatRecordingJob(ctx, principal, 2, "node:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !nearAgain.Equal(fixedCap) {
		t.Fatalf("repeated heartbeat moved cap: got %s want %s", nearAgain, fixedCap)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(recording_job_id) VALUES(2)`); err != nil {
		t.Fatal(err)
	}
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "2")
	completeReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/jobs/2/complete", nil)
	completeReq = completeReq.WithContext(context.WithValue(context.WithValue(completeReq.Context(), chi.RouteCtxKey, rctx), nodePrincipalContextKey, principal))
	completeResponse := httptest.NewRecorder()
	s.handleRecordingJobComplete(completeResponse, completeReq)
	if completeResponse.Code != http.StatusOK {
		t.Fatalf("completion inside drain allowance status=%d body=%s", completeResponse.Code, completeResponse.Body.String())
	}

	for _, tc := range []struct {
		name      string
		jobID     int64
		principal nodePrincipal
		owner     string
		token     *uuid.UUID
	}{
		{name: "post cap", jobID: 3, principal: principal, owner: "node:1"},
		{name: "continuous null end", jobID: 4, principal: principal, owner: "node:1"},
		{name: "wrong owner", jobID: 1, principal: nodePrincipal{NodeID: 2, AccountID: 42, NodeType: nodeTypeRelay}, owner: "node:2"},
		{name: "wrong token", jobID: 1, principal: principal, owner: "node:1", token: ptrUUID(uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := s.heartbeatRecordingJob(ctx, tc.principal, tc.jobID, tc.owner, tc.token); !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("heartbeat err=%v want pgx.ErrNoRows", err)
			}
		})
	}

	clipBefore := dbNow()
	clip, err := s.heartbeatRecordingJob(ctx, principal, 5, "node:1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := clip.Sub(clipBefore); got < normalLease-time.Second || got > normalLease+time.Second {
		t.Fatalf("finite clip lease=%s want unchanged rolling lease %s", got, normalLease)
	}
}

func ptrUUID(v uuid.UUID) *uuid.UUID { return &v }

func TestRecordingHeartbeatHandlerCancelsAfterContinuousDrainCap(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts(id) VALUES(42);
		INSERT INTO nodes(id,account_id,node_type,status,last_heartbeat_at,relay_max_streams)
		VALUES(1,42,'relay','active',now(),4);
		INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,capture_via)
		VALUES(1,42,1,'heartbeat','https://example.test/live.m3u8','active',now()-interval '1 hour','relay');
		INSERT INTO recording_jobs
			(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,
			 attempt_count,idempotency_key,kind,window_end_at)
		VALUES(1,1,now(),now(),60,'leased','node:1',now()+interval '5 minutes',1,'heartbeat-handler','continuous_window',now()-interval '47 minutes')
	`); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "1")
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/jobs/1/heartbeat", nil)
	req = req.WithContext(context.WithValue(context.WithValue(req.Context(), chi.RouteCtxKey, rctx), nodePrincipalContextKey, nodePrincipal{NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay}))
	response := httptest.NewRecorder()
	s.handleRecordingJobHeartbeat(response, req)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), `"cancel":true`) {
		t.Fatalf("heartbeat status=%d body=%s want 409 cancel", response.Code, response.Body.String())
	}
	var status, owner string
	var expires time.Time
	if err := pool.QueryRow(ctx, `SELECT status,lease_owner,lease_expires_at FROM recording_jobs WHERE id=1`).Scan(&status, &owner, &expires); err != nil {
		t.Fatal(err)
	}
	if status != "leased" || owner != "node:1" {
		t.Fatalf("heartbeat mutated job status=%q owner=%q", status, owner)
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

func TestAccountClipsFeedPreservesTimestampContractTriState(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	for _, ddl := range []string{
		`ALTER TABLE recordings ADD COLUMN delivery text NOT NULL DEFAULT 'nas_pull'`,
		`ALTER TABLE recording_clips ADD COLUMN recording_id bigint, ADD COLUMN size_bytes bigint, ADD COLUMN sha256 text, ADD COLUMN clip_start_at timestamptz, ADD COLUMN clip_end_at timestamptz, ADD COLUMN display_path text, ADD COLUMN purged_at timestamptz, ADD COLUMN released_at timestamptz, ADD COLUMN created_at timestamptz NOT NULL DEFAULT now()`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0132_recording_clip_timestamp_contract.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,capture_via) VALUES(1,42,1,'tri-state','https://example.test/live','paused',now(),'relay')`); err != nil {
		t.Fatal(err)
	}
	lease, attempt := "123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174001"
	contract := `{"version":1,"mode":"muxed_source_copy","audio_selection":"first_optional","tracks":[{"stream_index":0,"media_type":"video","time_base_num":1,"time_base_den":1000,"first_timestamp":0,"last_timestamp":1000,"last_duration":40,"unit_count":26,"codec_signature_sha256":"` + strings.Repeat("a", 64) + `"}]}`
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,recording_job_id,size_bytes,sha256,clip_start_at,clip_end_at,display_path,created_at,capture_lease_token,capture_sequence) VALUES(1,1,1,3,$1,now()-interval '3 minutes',now()-interval '2 minutes','legacy.mp4',now()-interval '2 minutes',$2,1)`, strings.Repeat("1", 64), lease); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,recording_job_id,size_bytes,sha256,clip_start_at,clip_end_at,display_path,created_at,capture_lease_token,capture_sequence,capture_attempt_id,timestamp_contract_version,timestamp_contract,timestamp_contract_status) VALUES(2,1,1,3,$1,now()-interval '3 minutes',now()-interval '2 minutes','complete.mp4',now()-interval '2 minutes',$2,2,$3,'continuous-source-pts-v1',$4,'per_clip_probe_complete')`, strings.Repeat("2", 64), lease, attempt, contract); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,recording_job_id,size_bytes,sha256,clip_start_at,clip_end_at,display_path,created_at,capture_lease_token,capture_sequence,capture_attempt_id,timestamp_contract_status,timestamp_contract_reason) VALUES(3,1,1,3,$1,now()-interval '3 minutes',now()-interval '2 minutes','unknown.mp4',now()-interval '2 minutes',$2,3,$3,'per_clip_probe_unknown','missing_terminal_duration')`, strings.Repeat("3", 64), lease, attempt); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/clips", nil)
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 42}))
	rec := httptest.NewRecorder()
	s.handleAccountClips(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Clips []map[string]any `json:"clips"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Clips) != 3 {
		t.Fatalf("clips=%v", payload.Clips)
	}
	if payload.Clips[0]["timestamp_contract_status"] != nil || payload.Clips[0]["capture_attempt_id"] != nil {
		t.Fatalf("legacy=%v", payload.Clips[0])
	}
	if payload.Clips[1]["timestamp_contract_status"] != capture.TimestampProbeComplete || payload.Clips[1]["timestamp_contract_version"] != capture.TimestampVersionContinuousSourcePTSV1 || payload.Clips[1]["timestamp_contract"] == nil {
		t.Fatalf("complete=%v", payload.Clips[1])
	}
	if payload.Clips[2]["timestamp_contract_status"] != capture.TimestampProbeUnknown || payload.Clips[2]["timestamp_contract_version"] != nil || payload.Clips[2]["timestamp_contract"] != nil || payload.Clips[2]["timestamp_contract_reason"] != "missing_terminal_duration" {
		t.Fatalf("unknown=%v", payload.Clips[2])
	}
}

func TestRecordingClipIngestRejectsUnadmittedProvenanceAndRetainsIntent(t *testing.T) {
	pool, cleanup := testRecordingLeasePool(t)
	defer cleanup()
	ctx := context.Background()
	var headCount atomic.Int64
	objectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method=%s", r.Method)
		}
		headCount.Add(1)
		w.Header().Set("Content-Length", "5")
		w.Header().Set("ETag", `"etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer objectServer.Close()
	raw, err := os.ReadFile("../../../infra/sql/migrations/0132_recording_clip_timestamp_contract.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	for _, ddl := range []string{
		`ALTER TABLE recordings ADD COLUMN delivery_storage_destination_id bigint, ADD COLUMN last_clip_at timestamptz, ADD COLUMN consecutive_failures integer NOT NULL DEFAULT 0, ADD COLUMN last_error_text text NOT NULL DEFAULT '', ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now()`,
		`ALTER TABLE recording_clips ADD COLUMN recording_id bigint, ADD COLUMN storage_destination_id bigint, ADD COLUMN endpoint text, ADD COLUMN bucket text, ADD COLUMN object_key text, ADD COLUMN display_path text, ADD COLUMN mime_type text, ADD COLUMN container text, ADD COLUMN size_bytes bigint, ADD COLUMN etag text, ADD COLUMN sha256 text, ADD COLUMN duration_ms bigint, ADD COLUMN video_codec text, ADD COLUMN audio_codec text, ADD COLUMN audio_present boolean, ADD COLUMN actual_fps double precision, ADD COLUMN video_width integer, ADD COLUMN video_height integer, ADD COLUMN resolved_url text, ADD COLUMN fire_at timestamptz, ADD COLUMN clip_start_at timestamptz, ADD COLUMN clip_end_at timestamptz`,
		`CREATE UNIQUE INDEX test_recording_clip_object ON recording_clips(bucket,object_key)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	secrets, err := secretbox.NewFromBase64Key("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := secrets.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO storage_destinations(id,account_id,endpoint,region,bucket,key_prefix,access_key_id,secret_access_key_enc) VALUES(7,42,$2,'auto','bucket','prefix','access',$1)`, sealed, objectServer.URL); err != nil {
		t.Fatal(err)
	}
	lease := "123e4567-e89b-12d3-a456-426614174000"
	intent := "123e4567-e89b-12d3-a456-426614174010"
	if _, err := pool.Exec(ctx, `INSERT INTO nodes(id,account_id,node_type,status,last_heartbeat_at,relay_max_streams) VALUES(1,42,'relay','active',now(),4)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,status,start_at,capture_via) VALUES(1,42,7,'ingest','https://example/live','active',now()-interval '1 hour','relay')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,lease_token,idempotency_key,kind,window_end_at) VALUES(1,1,now(),now(),60,'leased','node:1',now()+interval '1 hour',$1,'ingest','continuous_window',now()+interval '1 hour')`, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at) VALUES($1,1,1,7,$2,'bucket','key','clip.mp4','video/mp4',1000,'pending',now()+interval '1 hour')`, intent, objectServer.URL); err != nil {
		t.Fatal(err)
	}
	contract := `{"version":1,"mode":"muxed_source_copy","audio_selection":"first_optional","tracks":[{"stream_index":0,"media_type":"video","time_base_num":1,"time_base_den":1000,"first_timestamp":0,"last_timestamp":1000,"last_duration":40,"unit_count":26,"codec_signature_sha256":"` + strings.Repeat("a", 64) + `"}]}`
	body := fmt.Sprintf(`{"intent_id":%q,"job_id":1,"sha256":%q,"clip_start_at":%q,"clip_end_at":%q,"capture_sequence":1,"capture_attempt_id":%q,"timestamp_contract_version":"continuous-source-pts-v1","timestamp_contract_status":"per_clip_probe_complete","timestamp_contract":%s}`, intent, strings.Repeat("b", 64), time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano), "123e4567-e89b-12d3-a456-426614174001", contract)
	mismatchBody := fmt.Sprintf(`{"intent_id":%q,"job_id":1,"sha256":%q,"audio_present":true,"clip_start_at":%q,"clip_end_at":%q,"capture_sequence":1,"capture_attempt_id":%q,"timestamp_contract_version":"continuous-source-pts-v1","timestamp_contract_status":"per_clip_probe_complete","timestamp_contract":%s}`, intent, strings.Repeat("b", 64), time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano), "123e4567-e89b-12d3-a456-426614174001", contract)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/clips/ingest", strings.NewReader(mismatchBody))
	req.Header.Set(recordingLeaseTokenHeader, lease)
	req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay}))
	rec := httptest.NewRecorder()
	(&Server{pool: pool, secrets: secrets}).handleRecordingClipIngest(rec, req)
	if rec.Code != http.StatusBadRequest || headCount.Load() != 0 {
		t.Fatalf("audio mismatch status=%d heads=%d body=%s", rec.Code, headCount.Load(), rec.Body.String())
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM recording_upload_intents WHERE id=$1`, intent).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("audio mismatch intent status=%q err=%v", status, err)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/recording/clips/ingest", strings.NewReader(body))
	req.Header.Set(recordingLeaseTokenHeader, lease)
	req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay}))
	rec = httptest.NewRecorder()
	(&Server{pool: pool, secrets: secrets}).handleRecordingClipIngest(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if headCount.Load() != 0 {
		t.Fatalf("unadmitted request performed %d HEADs", headCount.Load())
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM recording_upload_intents WHERE id=$1`, intent).Scan(&status); err != nil || status != "pending" {
		t.Fatalf("intent status=%q err=%v", status, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_timestamp_contract_admissions(recording_job_id,lease_token,node_id,account_id,recording_id,policy_version) VALUES(1,$1,1,42,1,'continuous-source-pts-v1')`, lease); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/recording/clips/ingest", strings.NewReader(body))
	req.Header.Set(recordingLeaseTokenHeader, lease)
	req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay}))
	rec = httptest.NewRecorder()
	(&Server{pool: pool, secrets: secrets}).handleRecordingClipIngest(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admitted status=%d body=%s", rec.Code, rec.Body.String())
	}
	if headCount.Load() != 1 {
		t.Fatalf("admitted request HEAD count=%d", headCount.Load())
	}
	var persistedStatus string
	var persistedVersion *string
	var persistedContract []byte
	var persistedReason *string
	if err := pool.QueryRow(ctx, `SELECT timestamp_contract_status,timestamp_contract_version,timestamp_contract,timestamp_contract_reason FROM recording_clips LIMIT 1`).Scan(&persistedStatus, &persistedVersion, &persistedContract, &persistedReason); err != nil {
		t.Fatal(err)
	}
	if persistedStatus != capture.TimestampProbeComplete || persistedVersion == nil || *persistedVersion != capture.TimestampVersionContinuousSourcePTSV1 || len(persistedContract) == 0 || persistedReason != nil {
		t.Fatalf("persisted status=%q version=%v contract=%s reason=%v", persistedStatus, persistedVersion, persistedContract, persistedReason)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM recording_upload_intents WHERE id=$1`, intent).Scan(&status); err != nil || status != "consumed" {
		t.Fatalf("consumed status=%q err=%v", status, err)
	}

	unknownIntent := "123e4567-e89b-12d3-a456-426614174011"
	legacyIntent := "123e4567-e89b-12d3-a456-426614174012"
	if _, err := pool.Exec(ctx, `INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at) VALUES($1,1,1,7,$3,'bucket','unknown-key','unknown.mp4','video/mp4',1000,'pending',now()+interval '1 hour'),($2,1,1,7,$3,'bucket','legacy-key','legacy.mp4','video/mp4',1000,'pending',now()+interval '1 hour')`, unknownIntent, legacyIntent, objectServer.URL); err != nil {
		t.Fatal(err)
	}
	requestIngest := func(raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/recording/clips/ingest", strings.NewReader(raw))
		r.Header.Set(recordingLeaseTokenHeader, lease)
		r = r.WithContext(context.WithValue(r.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: 1, AccountID: 42, NodeType: nodeTypeRelay}))
		out := httptest.NewRecorder()
		(&Server{pool: pool, secrets: secrets}).handleRecordingClipIngest(out, r)
		return out
	}
	unknownBody := fmt.Sprintf(`{"intent_id":%q,"job_id":1,"sha256":%q,"clip_start_at":%q,"clip_end_at":%q,"capture_sequence":2,"capture_attempt_id":%q,"timestamp_contract_status":"per_clip_probe_unknown","timestamp_contract_reason":"probe_unavailable"}`, unknownIntent, strings.Repeat("c", 64), time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano), "123e4567-e89b-12d3-a456-426614174002")
	if out := requestIngest(unknownBody); out.Code != http.StatusOK {
		t.Fatalf("unknown ingest status=%d body=%s", out.Code, out.Body.String())
	}
	legacyBody := fmt.Sprintf(`{"intent_id":%q,"job_id":1,"sha256":%q,"clip_start_at":%q,"clip_end_at":%q,"capture_sequence":3}`, legacyIntent, strings.Repeat("d", 64), time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano))
	if out := requestIngest(legacyBody); out.Code != http.StatusOK {
		t.Fatalf("legacy ingest status=%d body=%s", out.Code, out.Body.String())
	}
	var unknownStatus, unknownReason string
	if err := pool.QueryRow(ctx, `SELECT timestamp_contract_status,timestamp_contract_reason FROM recording_clips WHERE object_key='unknown-key'`).Scan(&unknownStatus, &unknownReason); err != nil || unknownStatus != capture.TimestampProbeUnknown || unknownReason != "probe_unavailable" {
		t.Fatalf("unknown persisted status=%q reason=%q err=%v", unknownStatus, unknownReason, err)
	}
	var legacyAttempt *string
	if err := pool.QueryRow(ctx, `SELECT capture_attempt_id::text FROM recording_clips WHERE object_key='legacy-key'`).Scan(&legacyAttempt); err != nil || legacyAttempt != nil {
		t.Fatalf("legacy attempt=%v err=%v", legacyAttempt, err)
	}
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
		`CREATE TABLE accounts (
			id BIGINT PRIMARY KEY
		)`,
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
			max_streams INTEGER NOT NULL,
			bandwidth_capacity_bps BIGINT
		)`,
		`CREATE TABLE recording_bandwidth_observations (
			recording_id BIGINT PRIMARY KEY,
			observed_bandwidth_bps BIGINT NOT NULL,
			observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		testRelayNodesTableDDL,
		`CREATE TABLE streams (
			id BIGSERIAL PRIMARY KEY,
			provider TEXT NOT NULL DEFAULT '',
			source_page_url TEXT NOT NULL DEFAULT '',
			execution_class TEXT NOT NULL DEFAULT ''
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
			mode TEXT NOT NULL DEFAULT 'continuous',
			next_fire_at TIMESTAMPTZ NOT NULL DEFAULT (now()+interval '90 minutes'),
			preferred_relay_group_id BIGINT,
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
			relay_fairness_started_at TIMESTAMPTZ,
			error_text TEXT,
			completed_at TIMESTAMPTZ,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE recording_clips (
			id BIGSERIAL PRIMARY KEY,
			recording_job_id BIGINT NOT NULL,
			capture_lease_token UUID,
			capture_sequence BIGINT
		)`,
		testRecordingCanaryReservationsTableDDL,
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
