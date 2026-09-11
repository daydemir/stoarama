package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

const additiveCohortTestBatch = "goodplus-20260911-generation-1"

var additiveCohortTestIDs = []int64{339, 407, 417, 424, 427, 430, 441, 444, 445}
var additiveCohortTestDates = []string{
	"2026-08-26", "2026-08-27", "2026-08-15", "2026-08-07", "2026-08-13",
	"2026-08-08", "2026-08-07", "2026-08-08", "2026-08-12",
}

func seedJoinedAdditiveCohort(t *testing.T, fixture joinedHistoricalTier1Fixture) joinedHistoricalQualificationRequest {
	t.Helper()
	ctx := context.Background()
	request := joinedHistoricalQualificationRequest{
		ProtocolVersion: 1, ConnectionID: fixture.connectionID, BatchID: additiveCohortTestBatch, Generation: 1,
	}
	for ordinal, recordingID := range additiveCohortTestIDs {
		var streamID int64
		if err := fixture.pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,
			source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps)
			VALUES('direct',$1,$1,$1,'https://example.test/additive.m3u8','','hls',
			'video_manifest','video_live','continuous_video',30) RETURNING id`, fmt.Sprintf("additive-%d", recordingID)).Scan(&streamID); err != nil {
			t.Fatal(err)
		}
		// These are disposable test records, never mutations of the legacy cohort.
		if _, err := fixture.pool.Exec(ctx, `INSERT INTO recordings(id,account_id,storage_destination_id,name,
			stream_url,source_kind,cron_expr,cron_timezone,clip_duration_sec,status,start_at,stream_id,mode,
			daily_window_start,daily_window_end,active_weekdays,delivery,naming_profile,folder_name,naming_metadata_jsonb)
			SELECT $1,account_id,storage_destination_id,$2,stream_url,source_kind,cron_expr,cron_timezone,
			clip_duration_sec,'completed',start_at,$3,mode,daily_window_start,daily_window_end,active_weekdays,
			delivery,naming_profile,folder_name,naming_metadata_jsonb FROM recordings WHERE id=$4`,
			recordingID, fmt.Sprintf("additive-recording-%d", recordingID), streamID, joinedrecording.Tier1RecordingIDs[0]); err != nil {
			t.Fatal(err)
		}
		first, err := time.Parse("2006-01-02", additiveCohortTestDates[ordinal])
		if err != nil {
			t.Fatal(err)
		}
		entry := joinedHistoricalQualificationJobs{RecordingID: recordingID}
		for day := 0; day < 14; day++ {
			start := first.AddDate(0, 0, day).Add(8 * time.Hour)
			var jobID int64
			if err := fixture.pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,
				clip_duration_sec,status,idempotency_key,kind,window_end_at,completed_at)
				VALUES($1,$2,$2,60,'done',$3,'continuous_window',$4,$5) RETURNING id`, recordingID, start,
				fmt.Sprintf("additive:%d:%d", recordingID, day), start.Add(12*time.Hour), start.Add(12*time.Hour+time.Minute)).Scan(&jobID); err != nil {
				t.Fatal(err)
			}
			entry.JobIDs = append(entry.JobIDs, jobID)
		}
		request.RecordingJobs = append(request.RecordingJobs, entry)
	}
	return request
}

func joinedLegacyAuthorityDigest(t *testing.T, fixture joinedHistoricalTier1Fixture) string {
	t.Helper()
	var digest string
	if err := fixture.pool.QueryRow(context.Background(), `SELECT encode(sha256(convert_to(jsonb_build_object(
		'run',(SELECT to_jsonb(q) FROM recording_qualification_runs q WHERE q.id=$1),
		'batch',(SELECT to_jsonb(b) FROM recording_joined_batches b WHERE b.batch_id=$2),
		'members',(SELECT jsonb_agg(to_jsonb(m) ORDER BY ordinal) FROM recording_qualification_members m WHERE run_id=$1),
		'windows',(SELECT jsonb_agg(to_jsonb(w) ORDER BY recording_id,ordinal) FROM recording_qualification_windows w WHERE run_id=$1),
		'sources',(SELECT jsonb_agg(to_jsonb(s) ORDER BY id) FROM recording_joined_source_snapshots s
		  WHERE batch_record_id=(SELECT id FROM recording_joined_batches WHERE batch_id=$2)),
		'raw',(SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM recording_clips c WHERE recording_id=ANY($3::bigint[]))
		)::TEXT,'UTF8')),'hex')`, fixture.runID, fixture.req.BatchID, joinedrecording.Tier1RecordingIDs).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	return digest
}

func TestJoinedAdditiveCohortCoexistsAndFreezesWithoutLegacyMutation(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-additive-coexist@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	oldReq := fixture.req
	oldReq.Apply, oldReq.ExpectedRequestSHA256 = true, fixture.plan.RequestSHA256
	finishJoinedTier1Fixture(t, fixture, oldReq)
	before := joinedLegacyAuthorityDigest(t, fixture)
	request := seedJoinedAdditiveCohort(t, fixture)
	fixture.s.cfg.JoinedRecordingBatchID = request.BatchID
	fixture.s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	fixture.s.cfg.JoinedRecordingCanaryHourIDs = ""
	fixture.s.cfg.JoinedRecordingMaxActiveTasks = 2
	dry, plan, _ := fixture.callHistorical(request)
	if dry.Code != http.StatusOK || len(plan.Members) != 9 {
		t.Fatalf("additive dry status=%d body=%s", dry.Code, dry.Body.String())
	}
	request.Apply, request.ExpectedRequestSHA256 = true, plan.RequestSHA256
	applied, appliedPlan, runID := fixture.callHistorical(request)
	if applied.Code != http.StatusOK || runID == fixture.runID || appliedPlan.RequestSHA256 != plan.RequestSHA256 {
		t.Fatalf("additive apply status=%d body=%s", applied.Code, applied.Body.String())
	}
	if replay, _, replayID := fixture.callHistorical(request); replay.Code != http.StatusOK || replayID != runID {
		t.Fatalf("additive replay status=%d run=%d body=%s", replay.Code, replayID, replay.Body.String())
	}
	// The separate joining authority must not replace the account's established
	// qualification report simply because the additive run was activated later.
	reportRequest := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/qualification", nil),
		accountPrincipal{AccountID: fixture.accountID, UserID: fixture.userID, MemberRole: "owner"}, "")
	reportRecorder := httptest.NewRecorder()
	fixture.s.handleAccountRecordingQualification(reportRecorder, reportRequest)
	var report recordingQualificationResponse
	if reportRecorder.Code != http.StatusOK || json.Unmarshal(reportRecorder.Body.Bytes(), &report) != nil ||
		report.RunID != fixture.runID || report.TargetRecordings != 33 || len(report.Members) != 33 {
		t.Fatalf("additive run replaced legacy account report: status=%d body=%s", reportRecorder.Code, reportRecorder.Body.String())
	}
	var active, members, windows int
	if err := fixture.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM recording_qualification_runs WHERE account_id=$1 AND status='active'),
		(SELECT count(*) FROM recording_qualification_members WHERE run_id=$2),
		(SELECT count(*) FROM recording_qualification_windows WHERE run_id=$2)`, fixture.accountID, runID).Scan(&active, &members, &windows); err != nil || active != 2 || members != 9 || windows != 126 {
		t.Fatalf("active=%d members=%d windows=%d err=%v", active, members, windows, err)
	}
	cutoff, _ := time.Parse(time.RFC3339Nano, "2026-09-11T05:15:24.544Z")
	freeze := fixture.req
	freeze.BatchID, freeze.QualificationRunID = request.BatchID, runID
	freeze.RecordingIDs = append([]int64(nil), additiveCohortTestIDs...)
	freeze.OrderedRecordingIDSHA256 = "85b4fc9db34b50aa53666f1d364bef8ce267b581d56d17411d5342bd20648888"
	freeze.EligibilityCutoff = cutoff
	checkpoint, err := fixture.s.startJoinedTier1DryRun(ctx, freeze)
	if err != nil {
		t.Fatal(err)
	}
	for ordinal := 1; ordinal <= 9; ordinal++ {
		checkpoint, err = fixture.s.stepJoinedTier1DryRun(ctx, joinedTier1DryRunStepRequest{RunID: checkpoint.RunID, PriorityOrdinal: ordinal})
		if err != nil {
			t.Fatalf("additive checkpoint %d: %v", ordinal, err)
		}
	}
	if checkpoint.RequestSHA256 == nil {
		t.Fatal("additive checkpoint never became ready at nine recordings")
	}
	freeze.Apply, freeze.ExpectedRequestSHA256 = true, *checkpoint.RequestSHA256
	finishJoinedTier1Fixture(t, fixture, freeze)
	var recordings, days, hours int
	if err := fixture.pool.QueryRow(ctx, `SELECT expected_recordings,expected_stream_days,expected_scheduled_hours
		FROM recording_joined_batches WHERE batch_id=$1 AND state='building'`, request.BatchID).Scan(&recordings, &days, &hours); err != nil || recordings != 9 || days != 126 || hours != 1512 {
		t.Fatalf("additive freeze counts=%d/%d/%d err=%v", recordings, days, hours, err)
	}
	if after := joinedLegacyAuthorityDigest(t, fixture); after != before {
		t.Fatalf("legacy authority/raw changed: before=%s after=%s", before, after)
	}
	for _, statement := range []string{
		`UPDATE recording_qualification_runs SET definition_jsonb=definition_jsonb||'{"batch_id":"invented"}' WHERE id=$1`,
		`UPDATE recording_qualification_members SET recording_name='changed' WHERE run_id=$1`,
		`DELETE FROM recording_qualification_windows WHERE run_id=$1`,
	} {
		if _, err := fixture.pool.Exec(ctx, statement, runID); err == nil {
			t.Fatalf("activated additive authority accepted mutation: %s", statement)
		}
	}
	t.Log("JOINED_ADDITIVE_COHORT_COEXISTENCE_EXECUTED")
}

func TestJoinedAdditiveCohortPinsRejectUnapprovedCountAndBatch(t *testing.T) {
	fixture := newJoinedHistoricalTier1FixtureWithoutCheckpoint(t, "joined-additive-pins@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	for _, tc := range []struct {
		batch string
		count int
	}{
		{joinedrecording.Tier1BatchID, 9}, {additiveCohortTestBatch, 33},
		{additiveCohortTestBatch + "-other", 9}, {"unapproved", 9},
	} {
		if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_qualification_runs(account_id,definition_version,
			definition_jsonb,target_recording_count,window_sequence_start_at)
			VALUES($1,$2,jsonb_build_object('batch_id',$3::TEXT),$4,'2026-08-01')`, fixture.accountID,
			joinedrecording.Tier1HistoricalQualificationVersion, tc.batch, tc.count); err == nil {
			t.Fatalf("unapproved batch/count accepted: %+v", tc)
		}
	}
	var oldCount, newCount int
	var oldSHA, newSHA string
	if err := fixture.pool.QueryRow(ctx, `SELECT cardinality(recording_joined_cohort_ids($1)),
		cardinality(recording_joined_cohort_ids($2)),recording_joined_cohort_ids_sha256($1),
		recording_joined_cohort_ids_sha256($2)`, joinedrecording.Tier1BatchID, additiveCohortTestBatch).
		Scan(&oldCount, &newCount, &oldSHA, &newSHA); err != nil || oldCount != 33 || newCount != 9 ||
		oldSHA != joinedrecording.Tier1RecordingIDSHA || newSHA != "85b4fc9db34b50aa53666f1d364bef8ce267b581d56d17411d5342bd20648888" {
		t.Fatalf("pin identity old=%d/%s new=%d/%s err=%v", oldCount, oldSHA, newCount, newSHA, err)
	}
	request := seedJoinedAdditiveCohort(t, fixture)
	fixture.s.cfg.JoinedRecordingBatchID = additiveCohortTestBatch
	fixture.s.cfg.JoinedRecordingWorkScope = config.JoinedWorkScopeFrozenBatch
	fixture.s.cfg.JoinedRecordingCanaryHourIDs = ""
	fixture.s.cfg.JoinedRecordingMaxActiveTasks = 2
	request.RecordingJobs[0], request.RecordingJobs[1] = request.RecordingJobs[1], request.RecordingJobs[0]
	if response, _, _ := fixture.callHistorical(request); response.Code == http.StatusOK {
		t.Fatal("additive import accepted reordered cohort")
	}
	request.RecordingJobs[0], request.RecordingJobs[1] = request.RecordingJobs[1], request.RecordingJobs[0]
	request.Apply, request.ExpectedRequestSHA256 = true, strings.Repeat("0", 64)
	if response, _, _ := fixture.callHistorical(request); response.Code == http.StatusOK {
		t.Fatal("additive import accepted wrong approval hash")
	}
	var active int
	if err := fixture.pool.QueryRow(ctx, `SELECT count(*) FROM recording_qualification_runs WHERE account_id=$1 AND status='active'`, fixture.accountID).Scan(&active); err != nil || active != 1 {
		t.Fatalf("rejected additive import changed active authority count=%d err=%v", active, err)
	}
	t.Log("JOINED_ADDITIVE_COHORT_PIN_REJECTIONS_EXECUTED")
}
