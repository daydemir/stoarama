package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/recordability"
	"github.com/google/uuid"
)

func TestNormalizeCampaignAdmissionEntriesCanonicalAndExact(t *testing.T) {
	entries, err := normalizeCampaignAdmissionEntries([]campaignAdmissionApprovalEntry{
		{StreamID: 20, RecordingID: 0, SourceRevisionID: 4, SourceURL: " https://b.example/live.m3u8 ", SourcePageURL: " https://b.example/page ", Provider: " publisher ", ExternalID: " camera-b ", NormalizedLabel: " Camera B! ", SceneFrameEvidenceID: 102, SceneIdentitySHA256: strings.Repeat("B", 64)},
		{StreamID: 10, RecordingID: 7, SourceRevisionID: 0, SourceURL: "https://a.example/live.m3u8", SourcePageURL: "https://a.example/page", Provider: "publisher", ExternalID: "camera-a", NormalizedLabel: "Camera A", SceneFrameEvidenceID: 101, SceneIdentitySHA256: strings.Repeat("a", 64)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if entries[0].StreamID != 10 || entries[1].StreamID != 20 || entries[1].SourceURL != "https://b.example/live.m3u8" || entries[1].Provider != "publisher" || entries[1].NormalizedLabel != "camerab" || entries[1].SceneIdentitySHA256 != strings.Repeat("b", 64) {
		t.Fatalf("entries not canonical: %#v", entries)
	}

	bad := entries
	bad[1].SceneIdentitySHA256 = bad[0].SceneIdentitySHA256
	if _, err := normalizeCampaignAdmissionEntries(bad); err == nil {
		t.Fatal("duplicate physical scene accepted")
	}
	bad = append([]campaignAdmissionApprovalEntry(nil), entries...)
	bad[1].RecordingID = -1
	if _, err := normalizeCampaignAdmissionEntries(bad); err == nil {
		t.Fatal("negative recording id accepted")
	}
}

func TestTargetedEvidenceReplayComparisonExact(t *testing.T) {
	fps := 29.97
	base := recordability.TargetedEvidence{Result: "ok", Detail: "exact", ValidRatio: 1, DurationMs: 120000, SegmentCount: 2, FrameSHA256: strings.Repeat("a", 64), MediaSHA256: strings.Repeat("b", 64), NativeSignatureSHA256: strings.Repeat("c", 64), ChallengeProofSHA256: strings.Repeat("d", 64), VideoCodec: "h264", AudioCodec: "aac", AudioPresent: true, VideoWidth: 1920, VideoHeight: 1080, ActualFPS: &fps}
	if !targetedEvidenceEqual(base, base) {
		t.Fatal("exact evidence replay was rejected")
	}
	mutated := base
	mutated.FrameSHA256 = strings.Repeat("e", 64)
	if targetedEvidenceEqual(base, mutated) {
		t.Fatal("different evidence replay was accepted")
	}
}

func TestCampaignAdmissionApprovalRequiresExactOperatorSession(t *testing.T) {
	s := &Server{cfg: config.Config{DropletPoolOperatorEmail: "deniz@example.test"}}
	for name, principal := range map[string]accountPrincipal{
		"api_key":        {AccountID: 47, Role: accountRoleAdmin, Email: "deniz@example.test"},
		"other_operator": {AccountID: 47, UserID: 9, Role: accountRoleAdmin, Email: "other@example.test"},
		"member":         {AccountID: 47, UserID: 9, Role: accountRoleMember, Email: "deniz@example.test"},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/campaign-admission/approvals", strings.NewReader(`{}`))
			req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
			rec := httptest.NewRecorder()
			s.handleAccountCampaignAdmissionApprovalCreate(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestCampaignAdmissionHandlersPersistReplayAndSealExactBatch(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	const buildSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s.cfg = config.Config{DropletPoolOperatorEmail: "deniz@example.test", DropletPoolBuildSHA: buildSHA, DropletPoolMax: 9, DropletPoolCapacity: 5}
	userID, accountID := seedUserOrg(t, pool, "deniz@example.test", true)
	const rawSession = "campaign-admission-session"
	insertSession(t, pool, accountID, userID, rawSession)
	var sessionID int64
	if err := pool.QueryRow(context.Background(), `SELECT id FROM account_sessions WHERE session_hash=$1`, hashSecret(rawSession)).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var destinationID, streamID, sceneEvidenceID, nodeID, tokenID int64
	if err := pool.QueryRow(ctx, `INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,access_key_id,secret_access_key_enc,status,managed) VALUES($1,'admission','s3_compatible','https://s3.example.test','auto','admission','key',decode('00','hex'),'verified',true) RETURNING id`, accountID).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,local_timezone,tags) VALUES('publisher','scene-1','Approved Scene','approved-scene','https://source.example/live.m3u8','https://source.example/page','hls','video_manifest','video_live','continuous_video',30,'Europe/Berlin',ARRAY['FD']) RETURNING id`).Scan(&streamID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,new_source_url,new_source_page_url) VALUES($1,'test','bind','https://source.example/live.m3u8','https://source.example/page')`, streamID); err != nil {
		t.Fatal(err)
	}
	var frameID, mediaID int64
	frameSHA := strings.Repeat("1", 64)
	sceneSHA := strings.Repeat("2", 64)
	if err := pool.QueryRow(ctx, `INSERT INTO media_objects(storage_provider,bucket,object_key,mime_type,size_bytes,sha256) VALUES('r2','admission','scene.jpg','image/jpeg',1,$1) RETURNING id`, frameSHA).Scan(&mediaID); err != nil {
		t.Fatal(err)
	}
	capturedAt := time.Now().UTC().Add(-time.Minute)
	if err := pool.QueryRow(ctx, `INSERT INTO frames(stream_id,captured_at,raw_media_object_id,capture_status,source_kind) VALUES($1,$2,$3,'success','live') RETURNING id`, streamID, capturedAt, mediaID).Scan(&frameID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id) VALUES($1,$2,$3,$4,$5,$6,$7,'operator_visual',$8) RETURNING id`, accountID, streamID, frameID, mediaID, capturedAt, frameSHA, sceneSHA, userID).Scan(&sceneEvidenceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO nodes(account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams) VALUES($1,'worker-test','local_recorder','active',now(),1) RETURNING id`, accountID).Scan(&nodeID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'test',$2) RETURNING id`, nodeID, hashSecret("node-token")).Scan(&tokenID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recorder_droplets(name,node_id,do_droplet_id,region,size,capacity,state,last_seen_at,build_sha) VALUES('worker-test',$1,999,'nyc1','s-1vcpu-1gb',5,'active',now(),$2)`, nodeID, buildSHA); err != nil {
		t.Fatal(err)
	}
	var revisionID int64
	if err := pool.QueryRow(ctx, `SELECT max(id) FROM stream_source_revisions WHERE stream_id=$1`, streamID).Scan(&revisionID); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	end := start.Add(24 * time.Hour)
	schedule := batchScheduleRequest{TargetAccountID: accountID, StreamIDs: []int64{streamID}, StreamTimezones: []streamTimezoneInput{{StreamID: streamID, Timezone: "Europe/Berlin"}}, NamingProfile: "stoarama_v1", Mode: "continuous", ClipDurationSec: 60, DailyWindowStart: "06:00", DailyWindowEnd: "18:00", ActiveWeekdays: []int{1, 2, 3, 4, 5, 6, 7}, StartAt: &start, EndAt: &end, StorageDestinationID: destinationID, Delivery: "managed"}
	approvalBody, _ := json.Marshal(campaignAdmissionApprovalRequest{RequestID: uuid.NewString(), DeadlineAt: end, AuthorityCode: "deniz_fd_restore_20260814", FailureDomainTag: "FD", Entries: []campaignAdmissionApprovalEntry{{StreamID: streamID, SourceRevisionID: revisionID, SourceURL: "https://source.example/live.m3u8", SourcePageURL: "https://source.example/page", Provider: "publisher", ExternalID: "scene-1", NormalizedLabel: "approvedscene", SceneFrameEvidenceID: sceneEvidenceID, SceneIdentitySHA256: sceneSHA}}, Schedule: schedule})
	_ = sessionID // The router must resolve this persisted session itself.
	router := s.router()
	postApproval := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/campaign-admission/approvals", bytes.NewReader(approvalBody))
		req.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	created := postApproval()
	if created.Code != http.StatusCreated {
		t.Fatalf("approval status=%d body=%s", created.Code, created.Body.String())
	}
	var approvalResponse struct {
		ApprovalID string `json:"approval_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &approvalResponse); err != nil {
		t.Fatal(err)
	}
	replayedApproval := postApproval()
	if replayedApproval.Code != http.StatusCreated || replayedApproval.Body.String() != created.Body.String() {
		t.Fatalf("approval replay changed: first=%s second=%s", created.Body.String(), replayedApproval.Body.String())
	}
	_ = tokenID // The router must resolve this persisted node-token identity itself.
	postNode := func(path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer node-token")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	for attempt := 0; attempt < 2; attempt++ {
		requestID := uuid.NewString()
		targetBody := map[string]any{"approval_id": approvalResponse.ApprovalID, "stream_ids": []int64{streamID}, "request_id": requestID}
		targetRec := postNode("/api/v1/recording/campaign-admission/targets", targetBody)
		if targetRec.Code != http.StatusOK {
			t.Fatalf("target attempt %d status=%d body=%s", attempt+1, targetRec.Code, targetRec.Body.String())
		}
		targetReplay := postNode("/api/v1/recording/campaign-admission/targets", targetBody)
		if targetReplay.Code != http.StatusOK || targetReplay.Body.String() != targetRec.Body.String() {
			t.Fatalf("target replay changed: first=%s second=%s", targetRec.Body.String(), targetReplay.Body.String())
		}
		var targetResponse struct {
			Targets []recordability.Target `json:"targets"`
		}
		if err := json.Unmarshal(targetRec.Body.Bytes(), &targetResponse); err != nil || len(targetResponse.Targets) != 1 {
			t.Fatalf("decode target: %v %s", err, targetRec.Body.String())
		}
		fps := 30.0
		evidence := recordability.TargetedEvidence{Result: recordability.ResultOK, Detail: "valid_ratio=1.000 segments=2 native_signature_stable=true frame=true", ValidRatio: 1, DurationMs: 120000, SegmentCount: 2, FrameSHA256: strings.Repeat(fmt.Sprintf("%x", attempt+3), 64), MediaSHA256: strings.Repeat(fmt.Sprintf("%x", attempt+5), 64), VideoCodec: "h264", AudioCodec: "aac", AudioPresent: true, VideoWidth: 1920, VideoHeight: 1080, ActualFPS: &fps}
		evidence.NativeSignatureSHA256 = recordability.TargetedNativeSignatureSHA256(evidence)
		evidence.ChallengeProofSHA256 = recordability.TargetedChallengeProofSHA256(targetResponse.Targets[0].Challenge, evidence)
		evidenceBody := map[string]any{"approval_id": approvalResponse.ApprovalID, "stream_id": streamID, "attempt_id": targetResponse.Targets[0].AttemptID, "request_id": requestID, "evidence": evidence}
		firstEvidence := postNode("/api/v1/recording/campaign-admission/evidence", evidenceBody)
		if firstEvidence.Code != http.StatusCreated {
			t.Fatalf("evidence attempt %d status=%d body=%s", attempt+1, firstEvidence.Code, firstEvidence.Body.String())
		}
		replayedEvidence := postNode("/api/v1/recording/campaign-admission/evidence", evidenceBody)
		if replayedEvidence.Code != http.StatusCreated || replayedEvidence.Body.String() != firstEvidence.Body.String() {
			t.Fatalf("evidence replay changed: first=%s second=%s", firstEvidence.Body.String(), replayedEvidence.Body.String())
		}
		mutatedEvidence := evidence
		mutatedEvidence.FrameSHA256 = strings.Repeat("f", 64)
		mutatedEvidence.NativeSignatureSHA256 = recordability.TargetedNativeSignatureSHA256(mutatedEvidence)
		mutatedEvidence.ChallengeProofSHA256 = recordability.TargetedChallengeProofSHA256(targetResponse.Targets[0].Challenge, mutatedEvidence)
		mutatedBody := map[string]any{"approval_id": approvalResponse.ApprovalID, "stream_id": streamID, "attempt_id": targetResponse.Targets[0].AttemptID, "request_id": requestID, "evidence": mutatedEvidence}
		mutatedReplay := postNode("/api/v1/recording/campaign-admission/evidence", mutatedBody)
		if mutatedReplay.Code != http.StatusConflict {
			t.Fatalf("different evidence replay status=%d body=%s", mutatedReplay.Code, mutatedReplay.Body.String())
		}
	}
	schedule.CampaignAdmissionApprovalID = approvalResponse.ApprovalID
	scheduleBody, _ := json.Marshal(schedule)
	postSchedule := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(scheduleBody))
		req.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	firstSchedule := postSchedule()
	if firstSchedule.Code != http.StatusOK {
		t.Fatalf("schedule status=%d body=%s", firstSchedule.Code, firstSchedule.Body.String())
	}
	secondSchedule := postSchedule()
	if secondSchedule.Code != http.StatusOK || secondSchedule.Body.String() != firstSchedule.Body.String() {
		t.Fatalf("schedule replay changed: first=%s second=%s", firstSchedule.Body.String(), secondSchedule.Body.String())
	}
	var recordings, tracks, results int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recordings WHERE account_id=$1 AND stream_id=$2 AND status='active'),(SELECT count(*) FROM recording_campaign_tracks WHERE account_id=$1),(SELECT count(*) FROM recording_campaign_admission_results WHERE account_id=$1)`, accountID, streamID).Scan(&recordings, &tracks, &results); err != nil {
		t.Fatal(err)
	}
	if recordings != 1 || tracks != 1 || results != 1 {
		t.Fatalf("non-idempotent persisted counts recordings=%d tracks=%d results=%d", recordings, tracks, results)
	}
}
