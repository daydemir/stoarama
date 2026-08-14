package api

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/dropletpool"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type campaignProbeStoreFixture struct {
	mu          sync.Mutex
	presignKeys []string
	objects     map[string]campaignProbeStoreObject
}

type campaignProbeStoreObject struct {
	body        []byte
	etag        string
	version     string
	contentType string
}

func (f *campaignProbeStoreFixture) Bucket() string { return "campaign-test" }
func (f *campaignProbeStoreFixture) PresignPut(_ context.Context, key, _ string, _ time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.presignKeys = append(f.presignKeys, key)
	return "https://upload.invalid/" + key, nil
}
func (f *campaignProbeStoreFixture) Head(_ context.Context, key string) (r2.ObjectHead, error) {
	return f.head(key)
}
func (f *campaignProbeStoreFixture) head(key string) (r2.ObjectHead, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[key]
	if !ok {
		return r2.ObjectHead{}, fmt.Errorf("fixture object missing")
	}
	return r2.ObjectHead{ETag: object.etag, SizeBytes: int64(len(object.body)), VersionID: object.version, ContentType: object.contentType}, nil
}
func (f *campaignProbeStoreFixture) OpenExact(_ context.Context, key, etag, version string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[key]
	if !ok || object.etag != etag || object.version != version {
		return nil, fmt.Errorf("fixture exact object mismatch")
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), object.body...))), nil
}
func (f *campaignProbeStoreFixture) PutReader(_ context.Context, key, contentType string, body io.Reader) (string, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	return f.put(key, contentType, raw), nil
}
func (f *campaignProbeStoreFixture) PutBytes(_ context.Context, key, contentType string, body []byte) (string, error) {
	return f.put(key, contentType, body), nil
}
func (f *campaignProbeStoreFixture) put(key, contentType string, body []byte) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objects == nil {
		f.objects = make(map[string]campaignProbeStoreObject)
	}
	sum := sha256.Sum256(body)
	etag := hex.EncodeToString(sum[:])
	f.objects[key] = campaignProbeStoreObject{body: append([]byte(nil), body...), etag: etag, version: "v1", contentType: contentType}
	return etag
}

func buildCampaignProbeArchive(t *testing.T, colorName string) ([]byte, []byte) {
	t.Helper()
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Fatalf("named campaign admission fixture requires ffmpeg: %v", err)
	}
	dir := t.TempDir()
	segments := make([]string, 2)
	for i := range segments {
		segments[i] = filepath.Join(dir, fmt.Sprintf("segment-%d.mp4", i+1))
		cmd := exec.Command(ffmpeg,
			"-v", "error", "-f", "lavfi", "-i", "color=c="+colorName+":s=64x64:r=1:d=60.1",
			"-f", "lavfi", "-i", "anullsrc=r=8000:cl=mono", "-t", "60.1",
			"-map", "0:v:0", "-map", "1:a:0", "-c:v", "libx264", "-preset", "ultrafast",
			"-pix_fmt", "yuv420p", "-g", "1", "-c:a", "aac", "-shortest", "-y", segments[i],
		)
		if output, runErr := cmd.CombinedOutput(); runErr != nil {
			t.Fatalf("create exact campaign probe segment: %v (%s)", runErr, output)
		}
	}
	thumb, err := capture.ExtractSegmentThumbnail(context.Background(), segments[0])
	if err != nil {
		t.Fatalf("extract exact campaign probe frame: %v", err)
	}
	frame, err := os.ReadFile(thumb.Path)
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	zw := zip.NewWriter(&archive)
	for i, segment := range segments {
		raw, readErr := os.ReadFile(segment)
		if readErr != nil {
			t.Fatal(readErr)
		}
		entry, createErr := zw.Create(fmt.Sprintf("segment-%d.mp4", i+1))
		if createErr != nil {
			t.Fatal(createErr)
		}
		if _, writeErr := entry.Write(raw); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes(), frame
}

func advanceCampaignAdmissionClock(t *testing.T, runtimePool *pgxpool.Pool, seconds int) {
	t.Helper()
	if seconds < 0 || seconds > 3600 {
		t.Fatalf("invalid campaign test clock advance: %d", seconds)
	}
	var schema string
	if err := runtimePool.QueryRow(context.Background(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	adminURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	adminConfig, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		t.Fatal(err)
	}
	if adminConfig.ConnConfig.RuntimeParams == nil {
		adminConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	adminConfig.ConnConfig.RuntimeParams["search_path"] = schema
	adminPool, err := pgxpool.NewWithConfig(context.Background(), adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer adminPool.Close()
	qualified := pgx.Identifier{schema, "recording_campaign_now"}.Sanitize()
	searchPath := pgx.Identifier{schema}.Sanitize()
	statement := fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS timestamptz LANGUAGE sql STABLE SET search_path=%s,pg_catalog,pg_temp AS $clock$ SELECT pg_catalog.transaction_timestamp()+pg_catalog.make_interval(secs => %d) $clock$`, qualified, searchPath, seconds)
	if _, err := adminPool.Exec(context.Background(), statement); err != nil {
		t.Fatalf("advance isolated campaign authority clock: %v", err)
	}
}

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

func TestCanonicalizeTargetedFrameEvidenceUsesDecodedBytes(t *testing.T) {
	var jpegBytes bytes.Buffer
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	if err := jpeg.Encode(&jpegBytes, img, nil); err != nil {
		t.Fatal(err)
	}
	evidence := recordability.TargetedEvidence{
		Result:      recordability.ResultOK,
		FrameBase64: base64.StdEncoding.EncodeToString(jpegBytes.Bytes()),
	}
	if err := canonicalizeTargetedFrameEvidence(&evidence); err != nil {
		t.Fatal(err)
	}
	if len(evidence.FrameSHA256) != 64 || evidence.FrameBase64 != "" {
		t.Fatalf("frame evidence was not server-canonicalized: %#v", evidence)
	}

	mismatch := recordability.TargetedEvidence{Result: recordability.ResultOK, FrameBase64: base64.StdEncoding.EncodeToString(jpegBytes.Bytes()), FrameSHA256: strings.Repeat("f", 64)}
	if err := canonicalizeTargetedFrameEvidence(&mismatch); err == nil {
		t.Fatal("caller-authored mismatched frame hash was accepted")
	}
	if err := canonicalizeTargetedFrameEvidence(&recordability.TargetedEvidence{Result: recordability.ResultOK}); err == nil {
		t.Fatal("successful evidence without decoded frame bytes was accepted")
	}
	failed := recordability.TargetedEvidence{Result: recordability.ResultInconclusive, FrameSHA256: strings.Repeat("a", 64)}
	if err := canonicalizeTargetedFrameEvidence(&failed); err != nil || failed.FrameSHA256 != "" {
		t.Fatalf("terminal failure did not clear unobserved frame hash: evidence=%#v err=%v", failed, err)
	}
}

func TestVerifyTargetedQuarantineUsesExactMediaBytesAndRetainsVersions(t *testing.T) {
	store := &campaignProbeStoreFixture{}
	attemptID := uuid.NewString()
	mediaKey := "quarantine/campaign-probe/" + attemptID + "/media.zip"
	frameKey := "quarantine/campaign-probe/" + attemptID + "/frame.jpg"
	media, frame := buildCampaignProbeArchive(t, "green")
	store.put(mediaKey, "application/zip", media)
	store.put(frameKey, "image/jpeg", frame)
	s := &Server{campaignProbeStore: store}
	observation, err := s.verifyTargetedQuarantine(context.Background(), attemptID, strings.Repeat("a", 64), mediaKey, frameKey, targetedProbeMediaMaxBytes, targetedProbeFrameMaxBytes)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Evidence.Result != recordability.ResultOK || observation.Evidence.SegmentCount != 2 || observation.Evidence.DurationMs < 120000 || observation.Evidence.VideoCodec != "h264" || !observation.Evidence.AudioPresent || observation.Evidence.AudioCodec != "aac" {
		t.Fatalf("server-derived native evidence is incomplete: %#v", observation.Evidence)
	}
	if !lowerSHA256(observation.Evidence.FrameSHA256) || !lowerSHA256(observation.Evidence.MediaSHA256) || !lowerSHA256(observation.Evidence.NativeSignatureSHA256) || !lowerSHA256(observation.Evidence.ChallengeProofSHA256) {
		t.Fatalf("server-derived hashes are incomplete: %#v", observation.Evidence)
	}
	if observation.MediaArchiveVersion == "" || observation.FrameArchiveVersion == "" || observation.MediaArchiveETag == "" || observation.FrameArchiveETag == "" {
		t.Fatalf("protected object generations were not sealed: %#v", observation)
	}
	if _, err := store.Head(context.Background(), observation.MediaArchiveKey); err != nil {
		t.Fatalf("retained media missing: %v", err)
	}
	if _, err := store.Head(context.Background(), observation.FrameArchiveKey); err != nil {
		t.Fatalf("retained frame missing: %v", err)
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
	s.cfg = config.Config{
		DropletPoolOperatorEmail: "deniz@example.test", DropletPoolBuildSHA: buildSHA,
		DropletPoolProjectID: "project-test", DropletPoolFirewallID: "firewall-test",
		DropletPoolMax: 9, DropletPoolCapacity: 5, R2SignPutTTL: time.Minute,
	}
	store := &campaignProbeStoreFixture{}
	s.campaignProbeStore = store
	s.campaignDOAttest = func(_ context.Context, dropletID int64, name string) (dropletpool.ProviderAttestation, error) {
		return dropletpool.ProviderAttestation{DropletID: dropletID, Name: name, Region: "nyc1", Status: "active"}, nil
	}
	userID, accountID := seedUserOrg(t, pool, "deniz@example.test", true)
	_, infrastructureAccountID := seedUserOrg(t, pool, "cloud-infrastructure@example.test", false)
	const rawSession = "campaign-admission-session"
	insertSession(t, pool, accountID, userID, rawSession)
	ctx := context.Background()
	var destinationID, sceneEvidenceID, nodeID int64
	const streamID int64 = 17235 // Immutable Deniz FD-decision member in migration 0140.
	if err := pool.QueryRow(ctx, `INSERT INTO storage_destinations(account_id,name,provider,endpoint,region,bucket,access_key_id,secret_access_key_enc,status,managed) VALUES($1,'admission','s3_compatible','https://s3.example.test','auto','admission','key',decode('00','hex'),'verified',true) RETURNING id`, accountID).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO streams(id,provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,local_timezone,tags) VALUES($1,'publisher','scene-1','Approved Scene','approved-scene','https://source.example/live.m3u8','https://source.example/page','hls','video_manifest','video_live','continuous_video',30,'Europe/Berlin',ARRAY['FD'])`, streamID); err != nil {
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
	if err := pool.QueryRow(ctx, `INSERT INTO recording_scene_frame_evidence(account_id,stream_id,frame_id,media_object_id,captured_at,frame_sha256,scene_identity_sha256,verification_method,verified_by_user_id,evidence_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,'operator_visual',$8,$9) RETURNING id`, accountID, streamID, frameID, mediaID, capturedAt, frameSHA, sceneSHA, userID, strings.Repeat("0", 64)).Scan(&sceneEvidenceID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO nodes(account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams) VALUES($1,'worker-test','local_recorder','active',now(),1) RETURNING id`, infrastructureAccountID).Scan(&nodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'campaign01',$2)`, nodeID, hashSecret("node-token")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recorder_droplets(name,node_id,do_droplet_id,region,size,capacity,state,last_seen_at,build_sha) VALUES('worker-test',$1,999,'nyc1','s-1vcpu-1gb',5,'active',now(),$2)`, nodeID, buildSHA); err != nil {
		t.Fatal(err)
	}
	var standbyNodeID int64
	if err := pool.QueryRow(ctx, `INSERT INTO nodes(account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams) VALUES($1,'worker-standby','local_recorder','active',now(),1) RETURNING id`, infrastructureAccountID).Scan(&standbyNodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'campaign02',$2)`, standbyNodeID, hashSecret("standby-node-token")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recorder_droplets(name,node_id,do_droplet_id,region,size,capacity,state,last_seen_at,build_sha) VALUES('worker-standby',$1,1000,'sfo3','s-1vcpu-1gb',5,'active',now(),$2)`, standbyNodeID, buildSHA); err != nil {
		t.Fatal(err)
	}
	var nasKeyID, nasConnectionID, baselineRecordingID int64
	if err := pool.QueryRow(ctx, `INSERT INTO account_api_keys(account_id,key_prefix,secret_hash,scopes) VALUES($1,'campaign-nas','campaign-nas-secret',ARRAY['stoarama.pull']) RETURNING id`, accountID).Scan(&nasKeyID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id,last_seen_at,nas_storage_total_bytes,nas_storage_free_bytes,nas_storage_reported_at,nas_capacity_blocked) VALUES($1,'nas_pull',$2,now(),1000000000000,900000000000,now(),false) RETURNING id`, accountID, nasKeyID).Scan(&nasConnectionID); err != nil {
		t.Fatal(err)
	}
	_ = nasConnectionID
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,source_kind,mode,cron_expr,cron_timezone,clip_duration_sec,status,start_at,capture_via) VALUES($1,$2,'capacity baseline','https://baseline.example/live.m3u8','hls_live','sampled','0 * * * *','UTC',60,'completed',now()-interval '1 day','cloud') RETURNING id`, accountID, destinationID).Scan(&baselineRecordingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,fire_at,clip_start_at,clip_end_at,created_at) VALUES($1,$2,'https://s3.example.test','admission','baseline.mp4',1000000,now()-interval '1 hour',now()-interval '1 hour',now()-interval '59 minutes',now()-interval '1 hour')`, baselineRecordingID, destinationID); err != nil {
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
	probeOrderBody, _ := json.Marshal(campaignAdmissionProbeOrderRequest{ApprovalID: approvalResponse.ApprovalID, StreamID: streamID, RequestID: uuid.NewString()})
	postOperator := func(path string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		req.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	order := postOperator("/api/v1/account/recordings/campaign-admission/probe-orders", probeOrderBody)
	if order.Code != http.StatusCreated {
		t.Fatalf("probe order status=%d body=%s", order.Code, order.Body.String())
	}
	orderReplay := postOperator("/api/v1/account/recordings/campaign-admission/probe-orders", probeOrderBody)
	if orderReplay.Code != http.StatusOK || orderReplay.Body.String() != order.Body.String() {
		t.Fatalf("probe-order replay changed: first=%s second=%s", order.Body.String(), orderReplay.Body.String())
	}
	postNode := func(path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer node-token")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	archiveColors := []string{"blue", "red"}
	var evidenceIDs []string
	for attemptIndex, colorName := range archiveColors {
		lease := postNode("/api/v1/recording/campaign-admission/lease", map[string]any{})
		if lease.Code != http.StatusOK {
			t.Fatalf("probe lease %d status=%d body=%s", attemptIndex+1, lease.Code, lease.Body.String())
		}
		var leaseResponse struct {
			Target     *recordability.Target `json:"target"`
			ApprovalID string                `json:"approval_id"`
			RequestID  string                `json:"request_id"`
		}
		if err := json.Unmarshal(lease.Body.Bytes(), &leaseResponse); err != nil || leaseResponse.Target == nil {
			t.Fatalf("decode real R10 probe lease %d: err=%v body=%s", attemptIndex+1, err, lease.Body.String())
		}
		if leaseResponse.Target.ID != streamID || leaseResponse.Target.AttemptID == "" || leaseResponse.Target.Challenge == "" || !strings.Contains(leaseResponse.Target.MediaUploadURL, "/quarantine/campaign-probe/") || !strings.Contains(leaseResponse.Target.FrameUploadURL, "/quarantine/campaign-probe/") {
			t.Fatalf("lease did not return exact server-owned quarantine intents: %#v", leaseResponse.Target)
		}
		mediaArchive, frame := buildCampaignProbeArchive(t, colorName)
		mediaKey := fmt.Sprintf("quarantine/campaign-probe/%s/media.zip", leaseResponse.Target.AttemptID)
		frameKey := fmt.Sprintf("quarantine/campaign-probe/%s/frame.jpg", leaseResponse.Target.AttemptID)
		store.put(mediaKey, "application/zip", mediaArchive)
		store.put(frameKey, "image/jpeg", frame)
		evidenceBody := map[string]any{
			"approval_id": leaseResponse.ApprovalID, "stream_id": streamID,
			"attempt_id": leaseResponse.Target.AttemptID, "request_id": leaseResponse.RequestID,
			"evidence": recordability.TargetedEvidence{Result: recordability.ResultOK, Detail: "caller fields are non-authoritative", FrameSHA256: strings.Repeat("f", 64)},
		}
		evidence := postNode("/api/v1/recording/campaign-admission/evidence", evidenceBody)
		if evidence.Code != http.StatusCreated {
			t.Fatalf("evidence %d status=%d body=%s", attemptIndex+1, evidence.Code, evidence.Body.String())
		}
		var evidenceResponse struct {
			EvidenceID string `json:"evidence_id"`
		}
		if err := json.Unmarshal(evidence.Body.Bytes(), &evidenceResponse); err != nil || evidenceResponse.EvidenceID == "" {
			t.Fatalf("decode evidence %d: err=%v body=%s", attemptIndex+1, err, evidence.Body.String())
		}
		evidenceIDs = append(evidenceIDs, evidenceResponse.EvidenceID)
		replayed := postNode("/api/v1/recording/campaign-admission/evidence", evidenceBody)
		if replayed.Code != http.StatusCreated || replayed.Body.String() != evidence.Body.String() {
			t.Fatalf("evidence replay %d changed: first=%s second=%s", attemptIndex+1, evidence.Body.String(), replayed.Body.String())
		}
		presentationRequestID := uuid.NewString()
		presentationReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/campaign-admission/scene-presentations/"+evidenceResponse.EvidenceID+"?request_id="+presentationRequestID, nil)
		presentationReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
		presentationRec := httptest.NewRecorder()
		router.ServeHTTP(presentationRec, presentationReq)
		if presentationRec.Code != http.StatusOK {
			t.Fatalf("scene presentation %d status=%d body=%s", attemptIndex+1, presentationRec.Code, presentationRec.Body.String())
		}
		var presentationResponse struct {
			PresentationID string `json:"presentation_id"`
			FrameBase64    string `json:"frame_base64"`
		}
		if err := json.Unmarshal(presentationRec.Body.Bytes(), &presentationResponse); err != nil || presentationResponse.PresentationID == "" || presentationResponse.FrameBase64 == "" {
			t.Fatalf("decode protected scene presentation %d: err=%v body=%s", attemptIndex+1, err, presentationRec.Body.String())
		}
		reviewBody, _ := json.Marshal(campaignAdmissionSceneReviewRequest{ApprovalID: approvalResponse.ApprovalID, ProbeEvidenceID: evidenceResponse.EvidenceID, PresentationID: presentationResponse.PresentationID, RequestID: uuid.NewString()})
		review := postOperator("/api/v1/account/recordings/campaign-admission/scene-reviews", reviewBody)
		if review.Code != http.StatusCreated {
			t.Fatalf("scene review %d status=%d body=%s", attemptIndex+1, review.Code, review.Body.String())
		}
		if attemptIndex == 0 {
			advanceCampaignAdmissionClock(t, pool, 61)
		}
	}
	advanceCampaignAdmissionClock(t, pool, 0)
	store.mu.Lock()
	presigned := append([]string(nil), store.presignKeys...)
	store.mu.Unlock()
	if len(presigned) != 4 {
		t.Fatalf("unexpected exact quarantine reservation count: %#v", presigned)
	}
	for _, evidenceID := range evidenceIDs {
		var retained int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_targeted_probe_evidence WHERE id=$1 AND media_archive_object_key LIKE 'protected/campaign-probe/%/media.zip' AND frame_archive_object_key LIKE 'protected/campaign-probe/%/frame.jpg' AND retain_until>$2`, evidenceID, end).Scan(&retained); err != nil || retained != 1 {
			t.Fatalf("server-owned immutable evidence retention missing for %s: count=%d err=%v", evidenceID, retained, err)
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
		t.Fatalf("complete server-observed evidence did not activate schedule: status=%d body=%s", firstSchedule.Code, firstSchedule.Body.String())
	}
	secondSchedule := postSchedule()
	if secondSchedule.Code != http.StatusOK || secondSchedule.Body.String() != firstSchedule.Body.String() {
		t.Fatalf("sealed admission replay changed: first=%s second=%s", firstSchedule.Body.String(), secondSchedule.Body.String())
	}
	var recordings, attempts, evidenceRows, results, commits int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recordings WHERE account_id=$1 AND stream_id=$2),(SELECT count(*) FROM recording_targeted_probe_attempts WHERE account_id=$1 AND stream_id=$2),(SELECT count(*) FROM recording_targeted_probe_evidence WHERE account_id=$1 AND stream_id=$2),(SELECT count(*) FROM recording_campaign_admission_results WHERE account_id=$1)`, accountID, streamID).Scan(&recordings, &attempts, &evidenceRows, &results); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_campaign_admission_commits WHERE account_id=$1`, accountID).Scan(&commits); err != nil {
		t.Fatal(err)
	}
	if recordings != 1 || attempts != 2 || evidenceRows != 2 || results != 1 || commits != 1 {
		t.Fatalf("invalid sealed state recordings=%d attempts=%d evidence=%d results=%d commits=%d", recordings, attempts, evidenceRows, results, commits)
	}
}
