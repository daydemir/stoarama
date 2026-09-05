package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

// TestJoinedFrozenBatchClaimsInterleaveRecordings exercises the public worker
// claim API against PostgreSQL. Four simultaneous workers must get the first
// eligible hour from four recordings, not four consecutive hours from the
// highest-priority recording.
func TestJoinedFrozenBatchClaimsInterleaveRecordings(t *testing.T) {
	fixture := newJoinedHistoricalTier1FixtureWithoutCheckpoint(t, "joined-fair-scheduler@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	s, pool := fixture.s, fixture.pool

	for recordingOrdinal, recordingID := range joinedrecording.Tier1RecordingIDs[:4] {
		firstHour := 0
		if recordingOrdinal == 0 {
			firstHour = 1 // The base fixture already has this recording's 08:00 source clip.
		}
		var jobID int64
		if err := pool.QueryRow(ctx, `SELECT id FROM recording_jobs WHERE recording_id=$1
			ORDER BY scheduled_for,id LIMIT 1`, recordingID).Scan(&jobID); err != nil {
			t.Fatal(err)
		}
		for hour := firstHour; hour < 4; hour++ {
			start := fixture.clipStart.Add(time.Duration(hour) * time.Hour)
			objectKey := fmt.Sprintf("raw/fair-%d-%d.mp4", recordingID, hour)
			sha := strings.Repeat(string(rune('b'+recordingOrdinal)), 64)
			if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,
				endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,
				audio_present,fire_at,clip_start_at,clip_end_at,created_at,released_at) VALUES($1,$2,$3,$4,'clips',$5,
				$5,'video/mp4','mp4',10,$6,$7,60000,'h264',false,$8,$8,$9,$8,$9)`, recordingID, jobID,
				fixture.storageID, joinedTestSourceEndpoint, objectKey, "etag-"+objectKey, sha, start, start.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
	}

	req := fixture.req
	req.Apply, req.ExpectedRequestSHA256 = false, ""
	dry, plan := fixture.call(req)
	if dry.Code != http.StatusOK {
		t.Fatalf("refresh freeze plan status=%d body=%s", dry.Code, dry.Body.String())
	}
	progress, err := s.startJoinedTier1DryRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := 1; ordinal <= len(joinedrecording.Tier1RecordingIDs); ordinal++ {
		progress, err = s.stepJoinedTier1DryRun(ctx, joinedTier1DryRunStepRequest{RunID: progress.RunID,
			PriorityOrdinal: ordinal})
		if err != nil {
			t.Fatalf("refresh checkpoint step %d: %v", ordinal, err)
		}
	}
	if progress.RequestSHA256 == nil || *progress.RequestSHA256 != plan.RequestSHA256 {
		t.Fatalf("refresh checkpoint sha=%v want=%s", progress.RequestSHA256, plan.RequestSHA256)
	}
	req.Apply, req.ExpectedRequestSHA256 = true, plan.RequestSHA256
	if applied, _ := fixture.call(req); applied.Code != http.StatusOK {
		t.Fatalf("apply status=%d body=%s", applied.Code, applied.Body.String())
	}
	finishJoinedTier1Fixture(t, fixture, req)
	var batchRecordID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM recording_joined_batches WHERE batch_id=$1`, req.BatchID).
		Scan(&batchRecordID); err != nil {
		t.Fatal(err)
	}
	_, _, _, insertFinalChild := materializeJoinedCanonicalBatch(t, fixture, batchRecordID)
	childTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertFinalChild(childTx); err != nil {
		_ = childTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := childTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	validation, err := s.startJoinedFinalValidation(ctx, joinedFinalValidationStartRequest{ProtocolVersion: 1,
		BatchID: req.BatchID, ExpectedFrozenDenominatorSHA256: plan.FrozenDenominatorSHA256})
	if err != nil {
		t.Fatal(err)
	}
	for validation.State != "ready" {
		if validation.NextOrdinal == nil {
			t.Fatalf("validation stalled: %+v", validation)
		}
		validation, err = s.stepJoinedFinalValidation(ctx, joinedFinalValidationStepRequest{ProtocolVersion: 1,
			RunID: validation.RunID, Ordinal: *validation.NextOrdinal})
		if err != nil {
			t.Fatal(err)
		}
	}
	freezeTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET freeze_started_at=clock_timestamp() WHERE id=$1`,
		batchRecordID); err != nil {
		_ = freezeTx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=clock_timestamp() WHERE id=$1`,
		batchRecordID); err != nil {
		_ = freezeTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := freezeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Ledger publication is a prerequisite of the hour claim API. The setup
	// publishes only fixture ledgers and bypasses their lifecycle trigger; the
	// behavior under test starts at the authenticated claim endpoint below.
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts DISABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',
		finalized_token=gen_random_uuid(),etag='fixture-etag',version_id='',published_at=clock_timestamp()
		WHERE batch_record_id=$1 AND artifact_kind='allocation_ledger'`, batchRecordID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE recording_joined_artifacts ENABLE TRIGGER recording_joined_artifact_update_guard`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=FALSE,
		updated_at=clock_timestamp() WHERE batch_record_id=$1`, batchRecordID); err != nil {
		t.Fatal(err)
	}

	s.cfg.JoinedRecordingControlPlaneEnabled = true
	s.cfg.JoinedRecordingProtocolVersion = 1
	s.cfg.JoinedRecordingConnectionID = int(fixture.connectionID)
	s.cfg.JoinedRecordingBatchID = req.BatchID
	s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	s.cfg.JoinedRecordingCanaryHourIDs = ""
	s.cfg.JoinedRecordingFrozenExcludedPublicationArtifactIDs = joinedFrozenPublicationDenyForTest
	s.cfg.JoinedRecordingMaxActiveTasks = 4
	s.cfg.JoinedWorkerBootstrapToken = "joined-bootstrap-credential-32bytes"
	s.cfg.JoinedWorkerSigningKey = "joined-signing-credential-32-bytes"
	token := joinedGapBootstrapToken(t, s, req.BatchID)

	type result struct {
		status int
		claim  joinedrecording.PreflightHourClaim
	}
	start := make(chan struct{})
	results := make(chan result, 4)
	var workers sync.WaitGroup
	for worker := 1; worker <= 4; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			body, _ := json.Marshal(joinedrecording.WorkClaimRequest{ProtocolVersion: 1, BatchID: req.BatchID,
				WorkerID: fmt.Sprintf("fair-worker-%d", worker), ScratchAvailableBytes: 10 << 30, TaskBudgetBytes: 5 << 30})
			httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(body))
			httpReq.Header.Set("Authorization", "Bearer "+token)
			recorder := httptest.NewRecorder()
			s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(recorder, httpReq)
			out := result{status: recorder.Code}
			_ = json.Unmarshal(recorder.Body.Bytes(), &out.claim)
			results <- out
		}(worker)
	}
	close(start)
	workers.Wait()
	close(results)

	recordingIDs := make([]int64, 0, 4)
	for out := range results {
		if out.status != http.StatusOK {
			t.Fatalf("concurrent claim status=%d", out.status)
		}
		recordingIDs = append(recordingIDs, out.claim.RecordingID)
	}
	sort.Slice(recordingIDs, func(i, j int) bool { return recordingIDs[i] < recordingIDs[j] })
	want := append([]int64(nil), joinedrecording.Tier1RecordingIDs[:4]...)
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if fmt.Sprint(recordingIDs) != fmt.Sprint(want) {
		t.Fatalf("concurrent claim recordings=%v want one hour from each of %v", recordingIDs, want)
	}
}
