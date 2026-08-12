package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/stitchcert"
	"github.com/google/uuid"
)

func TestNativeStitchClaimSelectsEligibleConnectionBeforeLimitAndFencesLease(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	for _, ddl := range []string{
		`CREATE TABLE accounts(id bigint primary key)`,
		`INSERT INTO accounts VALUES(47)`,
		`ALTER TABLE recording_clips ADD COLUMN capture_attempt_id uuid, ADD COLUMN timestamp_contract_version text, ADD COLUMN timestamp_contract jsonb, ADD COLUMN timestamp_contract_status text, ADD COLUMN timestamp_contract_reason text`,
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
	if replayRec.Code != http.StatusConflict {
		t.Fatalf("stale completion replay status=%d body=%s", replayRec.Code, replayRec.Body.String())
	}
}
