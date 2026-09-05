package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"github.com/google/uuid"
)

func TestNativeStitchQualificationEligibilityFailsClosed(t *testing.T) {
	passed := "passed"
	partial := "partial"
	tests := []struct {
		name, scope, state, current string
		status                      *string
		want                        bool
	}{
		{"authoritative full current", "authoritative_occurrence", "passed", "current", &passed, true},
		{"byte audit cannot qualify", "byte_run_audit", "passed", "current", &passed, false},
		{"partial cannot qualify", "authoritative_occurrence", "partial", "current", &partial, false},
		{"missing certification cannot qualify", "authoritative_occurrence", "pending", "current", nil, false},
		{"stale NAS cannot qualify", "authoritative_occurrence", "passed", "unknown", &passed, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := nativeStitchQualificationEligible(test.scope, test.state, test.status, test.current); got != test.want {
				t.Fatalf("eligible=%v want=%v", got, test.want)
			}
		})
	}
}

func TestNativeStitchClaimSelectsEligibleConnectionBeforeLimitAndFencesLease(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	for _, ddl := range []string{
		`CREATE TABLE accounts(id bigint primary key)`,
		`INSERT INTO accounts VALUES(47)`,
		`ALTER TABLE recording_clips ADD COLUMN IF NOT EXISTS capture_attempt_id uuid, ADD COLUMN IF NOT EXISTS timestamp_contract_version text, ADD COLUMN IF NOT EXISTS timestamp_contract jsonb, ADD COLUMN IF NOT EXISTS timestamp_contract_status text, ADD COLUMN IF NOT EXISTS timestamp_contract_reason text`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0133_recording_native_stitch_certification.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatalf("apply native stitch migration: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	completed := now.Add(-time.Minute)
	const keyOne, keyTwo = int64(101), int64(102)
	var connectionOne, connectionTwo int64
	if err = pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id,inventory_generation,inventory_digest,inventory_scan_completed_at) VALUES(47,'nas_pull',$1,'g1',repeat('d',64),$2) RETURNING id`, keyOne, completed).Scan(&connectionOne); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id,inventory_generation,inventory_digest,inventory_scan_completed_at) VALUES(47,'nas_pull',$1,'g2',repeat('e',64),$2) RETURNING id`, keyTwo, completed).Scan(&connectionTwo); err != nil {
		t.Fatal(err)
	}
	start := now.Add(-14 * time.Hour)
	end := start.Add(12 * time.Hour)
	type frozenTask struct {
		taskID int64
		clip   stitchcert.ManifestClip
	}
	makeTask := func(recordingID, jobID, clipID, priority int64) frozenTask {
		if _, e := pool.Exec(ctx, `INSERT INTO recordings(id,account_id,name,status,delivery) VALUES($1,47,'recording','active','nas_pull')`, recordingID); e != nil {
			t.Fatal(e)
		}
		token := uuid.New()
		path := "recordings/" + string(rune('a'+clipID)) + ".mp4"
		if _, e := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,sha256,clip_start_at,clip_end_at,display_path,released_at,recording_job_id,capture_lease_token,capture_sequence) VALUES($1,$2,4,repeat('a',64),$3,$4,$5,now(),$6,$7,1)`, clipID, recordingID, start, end, path, jobID, token); e != nil {
			t.Fatal(e)
		}
		generation := captureGenerationFingerprint(&token)
		clip := stitchcert.ManifestClip{Ordinal: 1, ClipID: clipID, RecordingID: recordingID, RecordingJobID: jobID, RelativePath: path, SizeBytes: 4, SHA256: strings.Repeat("a", 64), ClipStartAt: start, ClipEndAt: end, CaptureGeneration: *generation, CaptureSequence: 1}
		manifest, _ := json.Marshal([]stitchcert.ManifestClip{clip})
		sha, _, _ := stitchcert.CanonicalSHA([]stitchcert.ManifestClip{clip})
		var taskID int64
		health := `{"clip_count":1,"expected_seconds":43200,"covered_seconds":43200,"coverage_pct":100,"largest_gap_seconds":0,"gap_count":0,"gap_over_30s_count":0,"gap_over_5m_count":0,"overlap_count":0,"overlap_seconds":0}`
		if e := pool.QueryRow(ctx, `INSERT INTO recording_native_stitch_tasks(account_id,recording_id,recording_job_id,window_start_at,window_end_at,health_calculated_at,health_metric_version,health_facts,job_schedule_facts,clip_manifest,clip_manifest_sha256,clip_count,source_bytes,policy_version,priority) VALUES(47,$1,$2,$3,$4,now(),2,$5,'{}',$6,$7,1,4,$8,$9) RETURNING id`, recordingID, jobID, start, end, health, manifest, sha, stitchcert.PolicyVersion, priority).Scan(&taskID); e != nil {
			t.Fatal(e)
		}
		if _, e := pool.Exec(ctx, `INSERT INTO recording_native_stitch_task_clips VALUES($1,1,$2,$3,4,repeat('a',64))`, taskID, clipID, path); e != nil {
			t.Fatal(e)
		}
		return frozenTask{taskID: taskID, clip: clip}
	}
	other := makeTask(71, 91, 11, 1)
	eligible := makeTask(72, 92, 12, 2)
	if _, err = pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,seen_generation) VALUES($1,$2,$3,$4,4,repeat('a',64),'present','g2'),($5,$6,$7,$8,4,repeat('a',64),'present','g1')`, connectionTwo, other.clip.ClipID, other.clip.RecordingID, other.clip.RelativePath, connectionOne, eligible.clip.ClipID, eligible.clip.RecordingID, eligible.clip.RelativePath); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/stitch-certifications/claim", bytes.NewReader([]byte(`{}`)))
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(keyOne)}))
	rec := httptest.NewRecorder()
	s.handleAccountNativeStitchClaim(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("claim status=%d body=%s", rec.Code, rec.Body.String())
	}
	var response struct {
		Task *nativeStitchClaimResponse `json:"task"`
	}
	if err = json.Unmarshal(rec.Body.Bytes(), &response); err != nil || response.Task == nil || response.Task.TaskID != eligible.taskID || response.Task.ClaimToken == uuid.Nil {
		t.Fatalf("claim response=%s err=%v", rec.Body.String(), err)
	}
	var state string
	var token uuid.UUID
	var attempts int
	if err = pool.QueryRow(ctx, `SELECT state,claim_token,attempt_count FROM recording_native_stitch_tasks WHERE id=$1`, eligible.taskID).Scan(&state, &token, &attempts); err != nil || state != "leased" || token != response.Task.ClaimToken || attempts != 1 {
		t.Fatalf("state=%s token=%s attempts=%d err=%v", state, token, attempts, err)
	}
	if err = pool.QueryRow(ctx, `SELECT state FROM recording_native_stitch_tasks WHERE id=$1`, other.taskID).Scan(&state); err != nil || state != "pending" {
		t.Fatalf("ineligible other-connection task mutated state=%s err=%v", state, err)
	}

	// Historical media completes once as terminal partial: bytes/decode/run are
	// durable PASS facts while unavailable source timestamps remain UNKNOWN.
	signature := map[string]any{"schema_version": float64(1), "format_name": "mov,mp4", "streams": []any{map[string]any{"codec_type": "video", "codec_name": "h264"}}}
	signatureSHA, _, err := stitchcert.CanonicalSHA(signature)
	if err != nil {
		t.Fatal(err)
	}
	clipFact := stitchcert.ClipFact{ManifestClip: eligible.clip, SidecarSHA256: strings.Repeat("b", 64), FileIdentity: stitchcert.FileIdentity{Size: 4, MTimeNS: 1, CTimeNS: 2, Inode: 3, Device: 4}, NativeSignature: signature, NativeSignatureSHA256: signatureSHA, StrictDecode: "passed"}
	report := stitchcert.Report{
		SchemaVersion: 1, PolicyVersion: stitchcert.PolicyVersion, TaskID: eligible.taskID,
		RecordingID: eligible.clip.RecordingID, RecordingJobID: eligible.clip.RecordingJobID,
		WindowStartAt: start, WindowEndAt: end, ClipManifestSHA256: response.Task.ClipManifestSHA256,
		InventoryGeneration: "g1", InventoryDigest: strings.Repeat("d", 64), InventoryCompletedAt: completed,
		Status: "partial", NASByteDecodeStatus: "passed", NativeRunConcatStatus: "passed",
		WithinRunFrameAdjacencyStatus: "unknown", WithinRunAudioContinuityStatus: "not_present", WindowContinuityStatus: "unknown",
		Timeline: stitchcert.MeasureTimeline([]stitchcert.ManifestClip{eligible.clip}, start, end), Clips: []stitchcert.ClipFact{clipFact},
		NativeRuns: []stitchcert.RunFact{{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 1, ClipCount: 1, NativeSignatureSHA256: signatureSHA, CaptureGeneration: eligible.clip.CaptureGeneration, BoundaryReason: "window_start", ValidationStatus: "single_clip_decode_only", SourceBytes: 4}},
		Seams:      []stitchcert.SeamFact{}, AudioSeams: []stitchcert.AudioSeamFact{}, ReasonCodes: []string{"continuous_source_pts_unavailable"},
		ClientVersion: "test", FFmpegVersion: "ffmpeg test", FFprobeVersion: "ffprobe test", StartedAt: now, CompletedAt: now.Add(time.Second),
	}
	reportRaw, _ := json.Marshal(report)
	completionRaw, _ := json.Marshal(nativeStitchCompleteRequest{ClaimToken: response.Task.ClaimToken, Report: reportRaw})
	completeReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/stitch-certifications/complete", bytes.NewReader(completionRaw))
	completeReq = completeReq.WithContext(context.WithValue(completeReq.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(keyOne)}))
	completeRec := httptest.NewRecorder()
	s.handleAccountNativeStitchComplete(completeRec, completeReq)
	if completeRec.Code != http.StatusOK {
		t.Fatalf("completion status=%d body=%s", completeRec.Code, completeRec.Body.String())
	}
	var certs int
	if err = pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM recording_native_stitch_certifications WHERE task_id=$1) FROM recording_native_stitch_tasks WHERE id=$1`, eligible.taskID).Scan(&state, &certs); err != nil || state != "partial" || certs != 1 {
		t.Fatalf("partial state=%s certs=%d err=%v", state, certs, err)
	}
	replayRec := httptest.NewRecorder()
	replayReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/stitch-certifications/complete", bytes.NewReader(completionRaw))
	replayReq = replayReq.WithContext(context.WithValue(replayReq.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(keyOne)}))
	s.handleAccountNativeStitchComplete(replayRec, replayReq)
	if replayRec.Code != http.StatusOK || !strings.Contains(replayRec.Body.String(), `"replayed":true`) {
		t.Fatalf("exact completion replay status=%d body=%s", replayRec.Code, replayRec.Body.String())
	}
	report.ReasonCodes = []string{"partitioned_native_runs"}
	differentRaw, _ := json.Marshal(report)
	differentBody, _ := json.Marshal(nativeStitchCompleteRequest{ClaimToken: response.Task.ClaimToken, Report: differentRaw})
	differentReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/stitch-certifications/complete", bytes.NewReader(differentBody))
	differentReq = differentReq.WithContext(context.WithValue(differentReq.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(keyOne)}))
	differentRec := httptest.NewRecorder()
	s.handleAccountNativeStitchComplete(differentRec, differentReq)
	if differentRec.Code != http.StatusConflict {
		t.Fatalf("different completion replay status=%d body=%s", differentRec.Code, differentRec.Body.String())
	}
}

func TestNativeStitchClaimIgnoresRetainedZeroByteDeliveryRows(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	for _, stmt := range []string{
		`CREATE TABLE accounts(id bigint primary key)`,
		`INSERT INTO accounts VALUES(47)`,
		`INSERT INTO connections(account_id,kind,api_key_id,inventory_generation,inventory_digest,inventory_scan_completed_at) VALUES(47,'nas_pull',101,'generation',repeat('a',64),now())`,
		`INSERT INTO recordings(id,account_id,name,status,delivery) VALUES(73,47,'retained-empty','active','nas_pull')`,
		`INSERT INTO recording_clips(id,recording_id,size_bytes,sha256,clip_start_at,clip_end_at,display_path,created_at) VALUES(13,73,0,repeat('0',64),now()-interval '2 hours',now()-interval '1 hour','recordings/empty.mp4',now()-interval '2 hours')`,
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0133_recording_native_stitch_certification.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}

	s := &Server{pool: pool}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/stitch-certifications/claim", bytes.NewReader([]byte(`{}`)))
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(101)}))
	rec := httptest.NewRecorder()
	s.handleAccountNativeStitchClaim(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"reason":"empty"`) {
		t.Fatalf("zero-byte retained row blocked stitch claim status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestNativeStitchWholeWindowContinuityRequiresExactEnvelope(t *testing.T) {
	full := stitchcert.Timeline{ExpectedSeconds: 43200, CoveredSeconds: 43200}
	if !nativeStitchWholeWindowContinuous(full) {
		t.Fatal("exact scheduled envelope was rejected")
	}
	leading := full
	leading.CoveredSeconds -= 900
	leading.LeadingGapSeconds = 900
	leading.GapCount = 1
	if nativeStitchWholeWindowContinuous(leading) {
		t.Fatal("single exact run with 15-minute leading loss claimed whole-window continuity")
	}
	overlap := full
	overlap.OverlapCount = 1
	overlap.OverlapSeconds = 1
	if nativeStitchWholeWindowContinuous(overlap) {
		t.Fatal("overlapping stored media claimed whole-window continuity")
	}
}

func TestNativeStitchCompletionKeepsV1FrameProofPartialAndRejectsForgedSeamProvenance(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	for _, ddl := range []string{
		`CREATE TABLE accounts(id bigint primary key)`,
		`INSERT INTO accounts VALUES(47)`,
		`ALTER TABLE recording_clips ADD COLUMN IF NOT EXISTS capture_attempt_id uuid, ADD COLUMN IF NOT EXISTS timestamp_contract_version text, ADD COLUMN IF NOT EXISTS timestamp_contract jsonb, ADD COLUMN IF NOT EXISTS timestamp_contract_status text, ADD COLUMN IF NOT EXISTS timestamp_contract_reason text`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0133_recording_native_stitch_certification.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	start, middle := now.Add(-14*time.Hour), now.Add(-8*time.Hour)
	end := now.Add(-2 * time.Hour)
	lease, attempt, claim := uuid.New(), uuid.New(), uuid.New()
	contract := func(first int64) stitchcert.TimestampContract {
		return stitchcert.TimestampContract{Version: 1, Mode: "muxed_source_copy", AudioSelection: "first_optional", Tracks: []stitchcert.TimestampContractTrack{{StreamIndex: 0, MediaType: "video", TimeBaseNum: 1, TimeBaseDen: 30, FirstTimestamp: first, LastTimestamp: first + 9, LastDuration: 1, UnitCount: 10, CodecSignatureSHA256: strings.Repeat("c", 64)}}}
	}
	contractOne, contractTwo := contract(0), contract(10)
	contractOneRaw, _ := json.Marshal(contractOne)
	contractTwoRaw, _ := json.Marshal(contractTwo)
	if _, err = pool.Exec(ctx, `INSERT INTO recordings(id,account_id,name,status,delivery) VALUES(77,47,'exact','active','nas_pull')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,sha256,clip_start_at,clip_end_at,display_path,released_at,recording_job_id,capture_lease_token,capture_sequence,capture_attempt_id,timestamp_contract_version,timestamp_contract_status,timestamp_contract) VALUES
		(21,77,4,repeat('a',64),$1,$2,'recordings/one.mp4',now(),99,$4,1,$5,'continuous-source-pts-v1','per_clip_probe_complete',$6),
		(23,77,0,repeat('0',64),$1,$2,'recordings/empty.mp4',NULL,99,$4,3,$5,'continuous-source-pts-v1','per_clip_probe_complete',$6),
		(22,77,4,repeat('b',64),$2,$3,'recordings/two.mp4',now(),99,$4,2,$5,'continuous-source-pts-v1','per_clip_probe_complete',$7)`, start, middle, end, lease, attempt, contractOneRaw, contractTwoRaw); err != nil {
		t.Fatal(err)
	}
	clips, manifestSHA, sourceBytes, err := loadNativeStitchManifest(ctx, pool, 47, 77, 99, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(clips) != 2 || sourceBytes != 8 || clips[0].ClipID != 21 || clips[1].ClipID != 22 {
		t.Fatalf("positive-only manifest clips=%v source_bytes=%d", clips, sourceBytes)
	}
	manifestRaw, _ := json.Marshal(clips)
	health := `{"clip_count":2,"expected_seconds":43200,"covered_seconds":43200,"coverage_pct":100,"largest_gap_seconds":0,"gap_count":0,"gap_over_30s_count":0,"gap_over_5m_count":0,"overlap_count":0,"overlap_seconds":0}`
	var taskID, connectionID int64
	if err = pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id,inventory_generation,inventory_digest,inventory_scan_completed_at) VALUES(47,'nas_pull',501,'g1',repeat('d',64),$1) RETURNING id`, now.Add(-time.Minute)).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO recording_native_stitch_tasks(account_id,recording_id,recording_job_id,window_start_at,window_end_at,health_calculated_at,health_metric_version,health_facts,job_schedule_facts,clip_manifest,clip_manifest_sha256,clip_count,source_bytes,policy_version) VALUES(47,77,99,$1,$2,now(),2,$3,'{}',$4,$5,2,$6,$7) RETURNING id`, start, end, health, manifestRaw, manifestSHA, sourceBytes, stitchcert.PolicyVersion).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	for _, clip := range clips {
		if _, err = pool.Exec(ctx, `INSERT INTO recording_native_stitch_task_clips(task_id,ordinal,clip_id,relative_path,size_bytes,sha256) VALUES($1,$2,$3,$4,$5,$6)`, taskID, clip.Ordinal, clip.ClipID, clip.RelativePath, clip.SizeBytes, clip.SHA256); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,seen_generation) VALUES($1,$2,77,$3,$4,$5,'present','g1')`, connectionID, clip.ClipID, clip.RelativePath, clip.SizeBytes, clip.SHA256); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_native_stitch_tasks SET state='leased',attempt_count=1,claim_token=$2,claimed_connection_id=$3,lease_expires_at=now()+interval '45 minutes' WHERE id=$1`, taskID, claim, connectionID); err != nil {
		t.Fatal(err)
	}
	signature := map[string]any{"schema_version": float64(1), "format_name": "mov,mp4", "streams": []any{map[string]any{"codec_type": "video", "codec_name": "h264"}}}
	signatureSHA, _, _ := stitchcert.CanonicalSHA(signature)
	clipFact := func(clip stitchcert.ManifestClip, contract stitchcert.TimestampContract) stitchcert.ClipFact {
		track := contract.Tracks[0]
		return stitchcert.ClipFact{ManifestClip: clip, SidecarSHA256: strings.Repeat("e", 64), FileIdentity: stitchcert.FileIdentity{Size: 4, MTimeNS: 1, CTimeNS: 2, Inode: clip.ClipID, Device: 4}, NativeSignature: signature, NativeSignatureSHA256: signatureSHA, StrictDecode: "passed", RecomputedTimestampContract: &contract,
			VideoTimeline: &stitchcert.StreamTimelineEvidence{FrameCount: track.UnitCount, FirstTimestamp: track.FirstTimestamp, LastTimestamp: track.LastTimestamp, LastDurationTimestamp: track.LastDuration, TimeBaseNumerator: track.TimeBaseNum, TimeBaseDenominator: track.TimeBaseDen}}
	}
	facts := []stitchcert.ClipFact{clipFact(clips[0], contractOne), clipFact(clips[1], contractTwo)}
	frame := func(ts int64) stitchcert.SeamFrameEvidence {
		return stitchcert.SeamFrameEvidence{BestEffortTimestamp: ts, DurationTimestamp: 1, TimeBaseNumerator: 1, TimeBaseDenominator: 30, PictureType: "P", DecodedSHA256: strings.Repeat("f", 64), PacketSHA256: strings.Repeat("f", 64)}
	}
	seam := stitchcert.SeamFact{Ordinal: 1, PreviousClipID: 21, NextClipID: 22, CaptureGeneration: clips[0].CaptureGeneration, PreviousSequence: 1, NextSequence: 2, NativeSignatureSHA256: signatureSHA, CaptureAttemptID: attempt.String(), TimelineBasis: "continuous_source_pts_v1", CaptureContract: "continuous-source-pts-v1", PreviousFrames: []stitchcert.SeamFrameEvidence{frame(7), frame(8), frame(9)}, NextFrames: []stitchcert.SeamFrameEvidence{frame(10), frame(11), frame(12)}, Confidence: "high", Verdict: "exact", Reason: "frame_adjacency_proven"}
	report := stitchcert.Report{SchemaVersion: 1, PolicyVersion: stitchcert.PolicyVersion, TaskID: taskID, RecordingID: 77, RecordingJobID: 99, WindowStartAt: start, WindowEndAt: end, ClipManifestSHA256: manifestSHA, InventoryGeneration: "g1", InventoryDigest: strings.Repeat("d", 64), InventoryCompletedAt: now.Add(-time.Minute), Status: "passed", NASByteDecodeStatus: "passed", NativeRunConcatStatus: "passed", WithinRunFrameAdjacencyStatus: "passed", WithinRunAudioContinuityStatus: "not_present", WindowContinuityStatus: "passed", Timeline: stitchcert.MeasureTimeline(clips, start, end), Clips: facts,
		NativeRuns: []stitchcert.RunFact{{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 2, ClipCount: 2, NativeSignatureSHA256: signatureSHA, CaptureGeneration: clips[0].CaptureGeneration, CaptureAttemptID: attempt.String(), TimestampContract: "continuous-source-pts-v1", BoundaryReason: "window_start", ValidationStatus: "lossless_concat_decode_passed", SourceBytes: sourceBytes}},
		Seams:      []stitchcert.SeamFact{seam}, AudioSeams: []stitchcert.AudioSeamFact{{Ordinal: 1, PreviousClipID: 21, NextClipID: 22, Verdict: "not_present", Reason: "audio_not_present"}}, ReasonCodes: []string{"completed"}, ClientVersion: "test", FFmpegVersion: "ffmpeg test", FFprobeVersion: "ffprobe test", StartedAt: now, CompletedAt: now.Add(time.Second)}
	complete := func(value stitchcert.Report) *httptest.ResponseRecorder {
		reportRaw, _ := json.Marshal(value)
		body, _ := json.Marshal(nativeStitchCompleteRequest{ClaimToken: claim, Report: reportRaw})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/stitch-certifications/complete", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(501)}))
		rec := httptest.NewRecorder()
		(&Server{pool: pool}).handleAccountNativeStitchComplete(rec, req)
		return rec
	}
	forged := report
	forged.Seams = append([]stitchcert.SeamFact(nil), report.Seams...)
	forged.Seams[0].PreviousFrames = append([]stitchcert.SeamFrameEvidence(nil), seam.PreviousFrames...)
	forged.Seams[0].PreviousFrames[0].BestEffortTimestamp = 4
	forged.Seams[0].PreviousFrames[1].BestEffortTimestamp = 5
	forged.Seams[0].PreviousFrames[2].BestEffortTimestamp = 6
	forged.Seams[0].NextFrames = append([]stitchcert.SeamFrameEvidence(nil), seam.NextFrames...)
	forged.Seams[0].NextFrames[0].BestEffortTimestamp = 7
	forged.Seams[0].NextFrames[1].BestEffortTimestamp = 8
	forged.Seams[0].NextFrames[2].BestEffortTimestamp = 9
	if rec := complete(forged); rec.Code != http.StatusConflict {
		t.Fatalf("forged seam status=%d body=%s", rec.Code, rec.Body.String())
	}
	forgedHashes := report
	forgedHashes.Seams = append([]stitchcert.SeamFact(nil), report.Seams...)
	forgedHashes.Seams[0].PreviousFrames = append([]stitchcert.SeamFrameEvidence(nil), seam.PreviousFrames...)
	forgedHashes.Seams[0].PreviousFrames[2].PacketSHA256 = strings.Repeat("0", 64)
	if rec := complete(forgedHashes); rec.Code != http.StatusConflict {
		t.Fatalf("true-timestamp client-authored packet hash status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := complete(report); rec.Code != http.StatusConflict {
		t.Fatalf("v1 timestamp-only full pass status=%d body=%s", rec.Code, rec.Body.String())
	}
	report.Status = "partial"
	report.WithinRunFrameAdjacencyStatus = "unknown"
	report.WindowContinuityStatus = "unknown"
	report.ReasonCodes = []string{"continuous_source_pts_unavailable"}
	if rec := complete(report); rec.Code != http.StatusConflict {
		t.Fatalf("partial v1 report persisted client-authored exact seam status=%d body=%s", rec.Code, rec.Body.String())
	}
	var rejectedState string
	if err = pool.QueryRow(ctx, `SELECT state FROM recording_native_stitch_tasks WHERE id=$1`, taskID).Scan(&rejectedState); err != nil || rejectedState != "leased" {
		t.Fatalf("rejected exact-v1 partial mutated task state=%s err=%v", rejectedState, err)
	}
	unknownWithFacts := report
	unknownWithFacts.Status = "unknown"
	unknownWithFacts.NASByteDecodeStatus = "unknown"
	unknownWithFacts.NativeRunConcatStatus = "unknown"
	unknownWithFacts.WithinRunFrameAdjacencyStatus = "unknown"
	unknownWithFacts.WithinRunAudioContinuityStatus = "unknown"
	unknownWithFacts.WindowContinuityStatus = "unknown"
	unknownWithFacts.NativeRuns = nil
	unknownWithFacts.Seams = nil
	unknownWithFacts.AudioSeams = nil
	unknownWithFacts.ReasonCodes = []string{"verification_transient"}
	if rec := complete(unknownWithFacts); rec.Code != http.StatusConflict {
		t.Fatalf("UNKNOWN with clip facts status=%d body=%s", rec.Code, rec.Body.String())
	}
	failedWithLaterFact := unknownWithFacts
	failedWithLaterFact.Status = "failed"
	failedWithLaterFact.NASByteDecodeStatus = "failed"
	failedWithLaterFact.ReasonCodes = []string{"clip_decode_failed"}
	failedWithLaterFact.Clips = append([]stitchcert.ClipFact(nil), facts...)
	failedWithLaterFact.Clips[0].StrictDecode = "failed"
	if rec := complete(failedWithLaterFact); rec.Code != http.StatusConflict {
		t.Fatalf("FAILED with a later clip fact status=%d body=%s", rec.Code, rec.Body.String())
	}
	report.Seams[0].TimelineBasis = "unavailable"
	report.Seams[0].Confidence = "none"
	report.Seams[0].Verdict = "ambiguous"
	report.Seams[0].Reason = "continuous_source_pts_unavailable"
	report.Seams[0].PreviousFrames = nil
	report.Seams[0].NextFrames = nil
	var wait sync.WaitGroup
	responses := make([]*httptest.ResponseRecorder, 2)
	for i := range responses {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			responses[index] = complete(report)
		}(i)
	}
	wait.Wait()
	for index, rec := range responses {
		if rec.Code != http.StatusOK {
			t.Fatalf("concurrent exact v1 partial[%d] status=%d body=%s", index, rec.Code, rec.Body.String())
		}
	}
	var state, status, video, audio, continuity string
	if err = pool.QueryRow(ctx, `SELECT t.state,c.status,c.within_run_frame_adjacency_status,c.within_run_audio_sample_continuity_status,c.window_continuity_status FROM recording_native_stitch_tasks t JOIN recording_native_stitch_certifications c ON c.task_id=t.id WHERE t.id=$1`, taskID).Scan(&state, &status, &video, &audio, &continuity); err != nil || state != "partial" || status != "partial" || video != "unknown" || audio != "not_present" || continuity != "unknown" {
		t.Fatalf("persisted axes state=%s status=%s video=%s audio=%s continuity=%s err=%v", state, status, video, audio, continuity, err)
	}
	var certCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_native_stitch_certifications WHERE task_id=$1`, taskID).Scan(&certCount); err != nil || certCount != 1 {
		t.Fatalf("concurrent identical completion certs=%d err=%v", certCount, err)
	}
}

func TestNativeStitchCompletionRejectsForgedHistoricalSingleClipPass(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	for _, ddl := range []string{
		`CREATE TABLE accounts(id bigint primary key)`, `INSERT INTO accounts VALUES(47)`,
		`ALTER TABLE recording_clips ADD COLUMN IF NOT EXISTS capture_attempt_id uuid, ADD COLUMN IF NOT EXISTS timestamp_contract_version text, ADD COLUMN IF NOT EXISTS timestamp_contract jsonb, ADD COLUMN IF NOT EXISTS timestamp_contract_status text, ADD COLUMN IF NOT EXISTS timestamp_contract_reason text`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0133_recording_native_stitch_certification.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	start, end, lease, claim := now.Add(-14*time.Hour), now.Add(-2*time.Hour), uuid.New(), uuid.New()
	if _, err = pool.Exec(ctx, `INSERT INTO recordings(id,account_id,name,status,delivery) VALUES(88,47,'historical','active','nas_pull')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,size_bytes,sha256,clip_start_at,clip_end_at,display_path,released_at,recording_job_id,capture_lease_token,capture_sequence) VALUES(31,88,4,repeat('a',64),$1,$2,'recordings/historical.mp4',now(),109,$3,1)`, start, end, lease); err != nil {
		t.Fatal(err)
	}
	clips, manifestSHA, sourceBytes, err := loadNativeStitchManifest(ctx, pool, 47, 88, 109, start, end)
	if err != nil {
		t.Fatal(err)
	}
	manifestRaw, _ := json.Marshal(clips)
	health := `{"clip_count":1,"expected_seconds":43200,"covered_seconds":43200,"coverage_pct":100,"largest_gap_seconds":0,"gap_count":0,"gap_over_30s_count":0,"gap_over_5m_count":0,"overlap_count":0,"overlap_seconds":0}`
	var connectionID, taskID int64
	if err = pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id,inventory_generation,inventory_digest,inventory_scan_completed_at) VALUES(47,'nas_pull',502,'g2',repeat('d',64),$1) RETURNING id`, now.Add(-time.Minute)).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `INSERT INTO recording_native_stitch_tasks(account_id,recording_id,recording_job_id,window_start_at,window_end_at,health_calculated_at,health_metric_version,health_facts,job_schedule_facts,clip_manifest,clip_manifest_sha256,clip_count,source_bytes,policy_version) VALUES(47,88,109,$1,$2,now(),2,$3,'{}',$4,$5,1,$6,$7) RETURNING id`, start, end, health, manifestRaw, manifestSHA, sourceBytes, stitchcert.PolicyVersion).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO recording_native_stitch_task_clips(task_id,ordinal,clip_id,relative_path,size_bytes,sha256) VALUES($1,1,31,'recordings/historical.mp4',4,repeat('a',64))`, []any{taskID}},
		{`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,seen_generation) VALUES($1,31,88,'recordings/historical.mp4',4,repeat('a',64),'present','g2')`, []any{connectionID}},
		{`UPDATE recording_native_stitch_tasks SET state='leased',attempt_count=1,claim_token=$2,claimed_connection_id=$3,lease_expires_at=now()+interval '45 minutes' WHERE id=$1`, []any{taskID, claim, connectionID}},
	} {
		if _, err = pool.Exec(ctx, fixture.query, fixture.args...); err != nil {
			t.Fatal(err)
		}
	}
	signature := map[string]any{"schema_version": float64(1), "format_name": "mov,mp4", "streams": []any{map[string]any{"codec_type": "video", "codec_name": "h264"}}}
	signatureSHA, _, _ := stitchcert.CanonicalSHA(signature)
	fact := stitchcert.ClipFact{ManifestClip: clips[0], SidecarSHA256: strings.Repeat("e", 64), FileIdentity: stitchcert.FileIdentity{Size: 4, MTimeNS: 1, CTimeNS: 2, Inode: 3, Device: 4}, NativeSignature: signature, NativeSignatureSHA256: signatureSHA, StrictDecode: "passed"}
	report := stitchcert.Report{SchemaVersion: 1, PolicyVersion: stitchcert.PolicyVersion, TaskID: taskID, RecordingID: 88, RecordingJobID: 109, WindowStartAt: start, WindowEndAt: end, ClipManifestSHA256: manifestSHA, InventoryGeneration: "g2", InventoryDigest: strings.Repeat("d", 64), InventoryCompletedAt: now.Add(-time.Minute), Status: "passed", NASByteDecodeStatus: "passed", NativeRunConcatStatus: "passed", WithinRunFrameAdjacencyStatus: "passed", WithinRunAudioContinuityStatus: "not_present", WindowContinuityStatus: "passed", Timeline: stitchcert.MeasureTimeline(clips, start, end), Clips: []stitchcert.ClipFact{fact}, NativeRuns: []stitchcert.RunFact{{Ordinal: 1, FirstClipOrdinal: 1, LastClipOrdinal: 1, ClipCount: 1, NativeSignatureSHA256: signatureSHA, CaptureGeneration: clips[0].CaptureGeneration, BoundaryReason: "window_start", ValidationStatus: "single_clip_decode_only", SourceBytes: 4}}, Seams: []stitchcert.SeamFact{}, AudioSeams: []stitchcert.AudioSeamFact{}, ReasonCodes: []string{"completed"}, ClientVersion: "test", FFmpegVersion: "ffmpeg test", FFprobeVersion: "ffprobe test", StartedAt: now, CompletedAt: now.Add(time.Second)}
	reportRaw, _ := json.Marshal(report)
	body, _ := json.Marshal(nativeStitchCompleteRequest{ClaimToken: claim, Report: reportRaw})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/stitch-certifications/complete", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: ptrInt64(502)}))
	rec := httptest.NewRecorder()
	(&Server{pool: pool}).handleAccountNativeStitchComplete(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("historical single-clip forged PASS status=%d body=%s", rec.Code, rec.Body.String())
	}
	var state string
	if err = pool.QueryRow(ctx, `SELECT state FROM recording_native_stitch_tasks WHERE id=$1`, taskID).Scan(&state); err != nil || state != "leased" {
		t.Fatalf("rejected forged report mutated task state=%s err=%v", state, err)
	}
}
