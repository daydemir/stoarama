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
	// Legacy frame evidence predates object-version persistence. Its immutable
	// media row still seals the exact ETag, while targeted probe evidence seals
	// both ETag and version. Keep the fixture faithful to both production paths.
	if !ok || object.etag != etag || (version != "" && object.version != version) {
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
			"-v", "error", "-f", "lavfi", "-i", "color=c="+colorName+":s=64x64:r=1:d=59.98",
			"-f", "lavfi", "-i", "anullsrc=r=8000:cl=mono", "-t", "59.98",
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

func TestCampaignProviderObservationDigestSealsCompleteDomain(t *testing.T) {
	started := time.Unix(1_700_000_000, 123_000).UTC()
	observed := started.Add(3 * time.Second)
	fields := []string{strings.Repeat("1", 64), strings.Repeat("a", 40), "s-2vcpu-4gb", strings.Repeat("2", 64), strings.Repeat("3", 64), strings.Repeat("4", 64)}
	want := campaignProviderObservationSHA256(started, observed, fields[0], fields[1], fields[2], fields[3], fields[4], fields[5])
	for index := range fields {
		mutated := append([]string(nil), fields...)
		mutated[index] += "x"
		got := campaignProviderObservationSHA256(started, observed, mutated[0], mutated[1], mutated[2], mutated[3], mutated[4], mutated[5])
		if got == want {
			t.Fatalf("provider observation digest ignored domain field %d", index)
		}
	}
	if got := campaignProviderObservationSHA256(started.Add(time.Microsecond), observed, fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]); got == want {
		t.Fatal("provider observation digest ignored bounded start time")
	}
	if got := campaignProviderObservationSHA256(started, observed.Add(time.Microsecond), fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]); got == want {
		t.Fatal("provider observation digest ignored bounded completion time")
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
	if observation.Evidence.Result != recordability.ResultOK || observation.Evidence.SegmentCount != 2 || observation.Evidence.DurationMs < 110000 || observation.Evidence.DurationMs > 130000 || observation.Evidence.VideoCodec != "h264" || !observation.Evidence.AudioPresent || observation.Evidence.AudioCodec != "aac" {
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
		DropletPoolMax: 9, DropletPoolCapacity: 5, DropletPoolSize: "s-2vcpu-4gb", R2SignPutTTL: time.Minute,
	}
	store := &campaignProbeStoreFixture{}
	s.campaignProbeStore = store
	s.campaignDOAttest = func(_ context.Context, dropletID int64, name string) (dropletpool.ProviderAttestation, error) {
		region := "nyc1"
		if dropletID == 1000 {
			region = "sfo3"
		}
		return dropletpool.ProviderAttestation{DropletID: dropletID, Name: name, Region: region, SizeSlug: "s-2vcpu-4gb", Status: "active"}, nil
	}
	userID, accountID := seedUserOrg(t, pool, "deniz@example.test", true)
	s.cfg.SharedRecordingsAccountID = accountID
	s.cfg.ServiceToken = "campaign-admission-service-token"
	_, infrastructureAccountID := seedUserOrg(t, pool, "cloud-infrastructure@example.test", false)
	const rawSession = "campaign-admission-session"
	insertSession(t, pool, accountID, userID, rawSession)
	ctx := context.Background()
	var fixtureSearchPath string
	if err := pool.QueryRow(ctx, `SHOW search_path`).Scan(&fixtureSearchPath); err != nil {
		t.Fatal(err)
	}
	fixtureAdminConfig, err := pgxpool.ParseConfig(strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL")))
	if err != nil {
		t.Fatal(err)
	}
	if fixtureAdminConfig.ConnConfig.RuntimeParams == nil {
		fixtureAdminConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	fixtureAdminConfig.ConnConfig.RuntimeParams["search_path"] = fixtureSearchPath
	fixtureAdmin, err := pgxpool.NewWithConfig(ctx, fixtureAdminConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer fixtureAdmin.Close()
	var destinationID, sceneEvidenceID, nodeID, sessionID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM account_sessions WHERE account_id=$1 AND user_id=$2 AND session_hash=$3`, accountID, userID, hashSecret(rawSession)).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
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
	baselineFrame := []byte("exact reviewed baseline frame bytes")
	frameETag := store.put("scene.jpg", "image/jpeg", baselineFrame)
	frameSHA := sha256Hex(baselineFrame)
	sceneSHA := sha256Hex([]byte("stoarama-scene-identity-v1\napproved scene"))
	if err := pool.QueryRow(ctx, `INSERT INTO media_objects(storage_provider,bucket,object_key,mime_type,size_bytes,etag,sha256) VALUES('r2','admission','scene.jpg','image/jpeg',$1,$2,$3) RETURNING id`, len(baselineFrame), frameETag, frameSHA).Scan(&mediaID); err != nil {
		t.Fatal(err)
	}
	capturedAt := time.Now().UTC().Add(-time.Minute)
	if err := pool.QueryRow(ctx, `INSERT INTO frames(stream_id,captured_at,raw_media_object_id,capture_status,source_kind) VALUES($1,$2,$3,'success','authoritative_frame_refresh') RETURNING id`, streamID, capturedAt, mediaID).Scan(&frameID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO nodes(account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams) VALUES($1,'worker-test','local_recorder','active',now(),1) RETURNING id`, infrastructureAccountID).Scan(&nodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'campaign01',$2)`, nodeID, hashSecret("node-token")); err != nil {
		t.Fatal(err)
	}
	var recorderDropletID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recorder_droplets(name,node_id,do_droplet_id,region,size,capacity,state,last_seen_at,build_sha) VALUES('worker-test',$1,999,'nyc1','s-2vcpu-4gb',5,'active',now(),$2) RETURNING id`, nodeID, buildSHA).Scan(&recorderDropletID); err != nil {
		t.Fatal(err)
	}
	var standbyNodeID int64
	if err := pool.QueryRow(ctx, `INSERT INTO nodes(account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams) VALUES($1,'worker-standby','local_recorder','active',now(),1) RETURNING id`, infrastructureAccountID).Scan(&standbyNodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'campaign02',$2)`, standbyNodeID, hashSecret("standby-node-token")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recorder_droplets(name,node_id,do_droplet_id,region,size,capacity,state,last_seen_at,build_sha) VALUES('worker-standby',$1,1000,'sfo3','s-2vcpu-4gb',5,'active',now(),$2)`, standbyNodeID, buildSHA); err != nil {
		t.Fatal(err)
	}
	var nasKeyID, nasConnectionID, baselineRecordingID, baselineStreamID int64
	if err := pool.QueryRow(ctx, `INSERT INTO account_api_keys(account_id,key_prefix,secret_hash,scopes) VALUES($1,'campaign-nas','campaign-nas-secret',ARRAY['stoarama.pull']) RETURNING id`, accountID).Scan(&nasKeyID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id,last_seen_at,nas_storage_total_bytes,nas_storage_free_bytes,nas_storage_reported_at,nas_capacity_blocked) VALUES($1,'nas_pull',$2,now(),1000000000000,900000000000,now(),false) RETURNING id`, accountID, nasKeyID).Scan(&nasConnectionID); err != nil {
		t.Fatal(err)
	}
	_ = nasConnectionID
	if err := pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,local_timezone) VALUES('baseline','baseline-capacity','Capacity Baseline','capacity-baseline','https://baseline.example/live.m3u8','https://baseline.example/page','hls','video_manifest','video_live','continuous_video',30,'UTC') RETURNING id`).Scan(&baselineStreamID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,mode,cron_expr,cron_timezone,clip_duration_sec,status,start_at,capture_via,next_fire_at) VALUES($1,$2,'capacity baseline','https://baseline.example/live.m3u8',$3,'hls_live','sampled','0 * * * *','UTC',60,'active',now()-interval '1 day','cloud',now()+interval '1 hour') RETURNING id`, accountID, destinationID, baselineStreamID).Scan(&baselineRecordingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,fire_at,clip_start_at,clip_end_at,created_at) VALUES($1,$2,'https://s3.example.test','admission','baseline.mp4',1000000,now()-interval '1 hour',now()-interval '1 hour',now()-interval '59 minutes',now()-interval '1 hour')`, baselineRecordingID, destinationID); err != nil {
		t.Fatal(err)
	}
	// The byte read and the presentation receipt are one source-bound
	// operation. A source mutation between them must invalidate the old read
	// receipt, even if the same frame object still exists.
	staleReadRequest := uuid.New()
	var staleReceipt uuid.UUID
	var staleFrameSHA, staleKey, staleETag string
	var staleSize int64
	if err := s.admissionPool.QueryRow(ctx, `SELECT read_receipt_id,frame_sha256,media_object_key,media_etag,media_size_bytes FROM recording_campaign_read_baseline_scene($1,$2,$3,$4,$5,$6,$7,$8)`, staleReadRequest, accountID, userID, sessionID, hashSecret(rawSession), "deniz_fd_restore_20260814", streamID, frameID).Scan(&staleReceipt, &staleFrameSHA, &staleKey, &staleETag, &staleSize); err != nil {
		t.Fatalf("create source-bound baseline read receipt: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE streams SET source_page_url='https://source.example/changed',updated_at=clock_timestamp() WHERE id=$1`, streamID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.admissionPool.Exec(ctx, `SELECT * FROM recording_campaign_present_baseline_scene($1,$2,$3,$4,$5,$6,$7,$8,$9)`, staleReadRequest, staleReceipt, accountID, userID, sessionID, hashSecret(rawSession), "deniz_fd_restore_20260814", streamID, frameID); err == nil {
		t.Fatal("baseline presentation accepted a read receipt from a prior source head")
	}
	if _, err := pool.Exec(ctx, `UPDATE streams SET source_page_url='https://source.example/page',updated_at=clock_timestamp() WHERE id=$1`, streamID); err != nil {
		t.Fatal(err)
	}
	var revisionID int64
	if err := pool.QueryRow(ctx, `SELECT max(id) FROM stream_source_revisions WHERE stream_id=$1`, streamID).Scan(&revisionID); err != nil {
		t.Fatal(err)
	}
	targetBody, _ := json.Marshal(authoritativeFrameTargetRequest{AccountID: accountID, StreamID: streamID, AuthorityCode: "deniz_fd_restore_20260814"})
	targetReq := httptest.NewRequest(http.MethodPost, "/api/v1/capture/authoritative-frame-target", bytes.NewReader(targetBody))
	targetReq.Header.Set("Authorization", "Bearer "+s.cfg.ServiceToken)
	targetRec := httptest.NewRecorder()
	s.handleAuthoritativeFrameTarget(targetRec, targetReq)
	if targetRec.Code != http.StatusOK {
		t.Fatalf("prepare authoritative frame target status=%d body=%s", targetRec.Code, targetRec.Body.String())
	}
	var target struct {
		StreamID             int64  `json:"stream_id"`
		SourceRevisionID     int64  `json:"source_revision_id"`
		SourceSnapshotSHA256 string `json:"source_snapshot_sha256"`
	}
	if err := json.Unmarshal(targetRec.Body.Bytes(), &target); err != nil || target.StreamID != streamID || target.SourceRevisionID != revisionID || len(target.SourceSnapshotSHA256) != 64 {
		t.Fatalf("invalid authoritative frame target: %+v err=%v", target, err)
	}
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	img.Set(0, 0, color.White)
	var baselineJPEG bytes.Buffer
	if err := jpeg.Encode(&baselineJPEG, img, nil); err != nil {
		t.Fatal(err)
	}
	baselineFrame = baselineJPEG.Bytes()
	frameSHA = sha256Hex(baselineFrame)
	capturedAt = time.Now().UTC().Add(-time.Second)
	ingestBody, _ := json.Marshal(map[string]any{
		"account_id": accountID, "stream_id": streamID, "status": "success", "captured_at": capturedAt,
		"mime_type": "image/jpeg", "frame_base64": base64.StdEncoding.EncodeToString(baselineFrame), "frame_sha256": frameSHA,
		"authoritative_frame_only": true, "authority_code": "deniz_fd_restore_20260814",
		"source_revision_id": target.SourceRevisionID, "source_snapshot_sha256": target.SourceSnapshotSHA256,
	})
	ingestReq := httptest.NewRequest(http.MethodPost, "/api/v1/capture/ingest", bytes.NewReader(ingestBody))
	ingestReq.Header.Set("Authorization", "Bearer "+s.cfg.ServiceToken)
	ingestRec := httptest.NewRecorder()
	s.handleCaptureIngest(ingestRec, ingestReq)
	var stored struct {
		FrameID     int64  `json:"frame_id"`
		FrameSHA256 string `json:"frame_sha256"`
	}
	if err := json.Unmarshal(ingestRec.Body.Bytes(), &stored); err != nil || ingestRec.Code != http.StatusOK || stored.FrameID <= 0 || stored.FrameSHA256 != frameSHA {
		t.Fatalf("decision frame ingest status=%d stored=%+v body=%s err=%v", ingestRec.Code, stored, ingestRec.Body.String(), err)
	}
	frameID = stored.FrameID
	var witnessedMediaID int64
	var witnessedFrameSHA string
	if err := fixtureAdmin.QueryRow(ctx, `SELECT witness.media_object_id,witness.frame_sha256,frame.raw_media_object_id FROM recording_campaign_authoritative_frame_witnesses witness JOIN frames frame ON frame.id=witness.frame_id WHERE witness.frame_id=$1 AND witness.account_id=$2 AND witness.stream_id=$3 AND witness.authority_code='deniz_fd_restore_20260814'`, frameID, accountID, streamID).Scan(&witnessedMediaID, &witnessedFrameSHA, &mediaID); err != nil || witnessedMediaID != mediaID || witnessedFrameSHA != frameSHA {
		t.Fatalf("immutable decision frame witness media=%d frame_media=%d sha=%q err=%v", witnessedMediaID, mediaID, witnessedFrameSHA, err)
	}
	var preparedStreamID, preparedRevisionID int64
	var preparedSource, preparedPage, preparedSnapshot, frameWitnessSHA string
	start := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	end := start.Add(24 * time.Hour)
	router := s.router()
	presentationRequestID := uuid.NewString()
	presentationReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/account/recordings/qualification/scene-presentations/%d?stream_id=%d&authority_code=deniz_fd_restore_20260814&request_id=%s", frameID, streamID, presentationRequestID), nil)
	presentationReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
	presentationRec := httptest.NewRecorder()
	router.ServeHTTP(presentationRec, presentationReq)
	if presentationRec.Code != http.StatusOK || presentationRec.Header().Get("X-Content-SHA256") != frameSHA {
		t.Fatalf("baseline presentation status=%d headers=%v body=%s", presentationRec.Code, presentationRec.Header(), presentationRec.Body.String())
	}
	presentationID := presentationRec.Header().Get("X-Stoarama-Presentation-ID")
	advanceCampaignAdmissionClock(t, pool, 301)
	presentationReplayReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/account/recordings/qualification/scene-presentations/%d?stream_id=%d&authority_code=deniz_fd_restore_20260814&request_id=%s", frameID, streamID, presentationRequestID), nil)
	presentationReplayReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
	presentationReplayRec := httptest.NewRecorder()
	router.ServeHTTP(presentationReplayRec, presentationReplayReq)
	if presentationReplayRec.Code != http.StatusOK || presentationReplayRec.Header().Get("X-Stoarama-Presentation-ID") != presentationID || !bytes.Equal(presentationReplayRec.Body.Bytes(), presentationRec.Body.Bytes()) {
		t.Fatalf("sealed baseline presentation was not replayable after read-receipt expiry: first=%d/%s replay=%d/%s", presentationRec.Code, presentationID, presentationReplayRec.Code, presentationReplayRec.Header().Get("X-Stoarama-Presentation-ID"))
	}
	advanceCampaignAdmissionClock(t, pool, 0)
	forgedAttestBody, _ := json.Marshal(map[string]any{"stream_id": streamID + 1, "authority_code": "deniz_scene_approval_20260814", "frame_id": frameID, "presentation_id": presentationID, "scene_identity": "Approved Scene"})
	forgedAttestReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/qualification/scene-attest", bytes.NewReader(forgedAttestBody))
	forgedAttestReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
	forgedAttestRec := httptest.NewRecorder()
	router.ServeHTTP(forgedAttestRec, forgedAttestReq)
	var forgedEvidence int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_scene_frame_evidence WHERE account_id=$1 AND frame_id=$2`, accountID, frameID).Scan(&forgedEvidence); forgedAttestRec.Code != http.StatusBadRequest || err != nil || forgedEvidence != 0 {
		t.Fatalf("candidate caller labels changed evidence: status=%d evidence=%d err=%v", forgedAttestRec.Code, forgedEvidence, err)
	}
	attestBody, _ := json.Marshal(sceneAttestRequest{FrameID: frameID, PresentationID: presentationID, SceneIdentity: "Approved Scene"})
	attestReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/qualification/scene-attest", bytes.NewReader(attestBody))
	attestReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
	attestRec := httptest.NewRecorder()
	router.ServeHTTP(attestRec, attestReq)
	if attestRec.Code != http.StatusOK {
		t.Fatalf("baseline attest status=%d body=%s", attestRec.Code, attestRec.Body.String())
	}
	var baselineEvidence struct {
		EvidenceID    int64  `json:"evidence_id"`
		StreamID      int64  `json:"stream_id"`
		AuthorityCode string `json:"authority_code"`
	}
	if err := json.Unmarshal(attestRec.Body.Bytes(), &baselineEvidence); err != nil || baselineEvidence.EvidenceID <= 0 || baselineEvidence.StreamID != streamID || baselineEvidence.AuthorityCode != "deniz_fd_restore_20260814" {
		t.Fatalf("decode baseline evidence: err=%v body=%s", err, attestRec.Body.String())
	}
	sceneEvidenceID = baselineEvidence.EvidenceID
	schedule := batchScheduleRequest{TargetAccountID: accountID, StreamIDs: []int64{streamID}, StreamTimezones: []streamTimezoneInput{{StreamID: streamID, Timezone: "Europe/Berlin"}}, NamingProfile: "stoarama_v1", Mode: "continuous", ClipDurationSec: 60, DailyWindowStart: "06:00", DailyWindowEnd: "18:00", ActiveWeekdays: []int{1, 2, 3, 4, 5, 6, 7}, StartAt: &start, EndAt: &end, StorageDestinationID: destinationID, Delivery: "nas_pull"}
	approvalBody, _ := json.Marshal(campaignAdmissionApprovalRequest{RequestID: uuid.NewString(), DeadlineAt: end, AuthorityCode: "deniz_fd_restore_20260814", FailureDomainTag: "FD", Entries: []campaignAdmissionApprovalEntry{{StreamID: streamID, SourceRevisionID: revisionID, SourceURL: "https://source.example/live.m3u8", SourcePageURL: "https://source.example/page", Provider: "publisher", ExternalID: "scene-1", NormalizedLabel: "approvedscene", SceneFrameEvidenceID: sceneEvidenceID, SceneIdentitySHA256: sceneSHA}}, Schedule: schedule})
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
	var orderResponse struct {
		OrderID string `json:"order_id"`
	}
	if err := json.Unmarshal(order.Body.Bytes(), &orderResponse); err != nil {
		t.Fatal(err)
	}
	statusReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/campaign-admission/probe-orders/"+orderResponse.OrderID, nil)
	statusReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
	statusRec := httptest.NewRecorder()
	router.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK || strings.Contains(statusRec.Body.String(), "source_url") || strings.Contains(statusRec.Body.String(), "object_key") || strings.Contains(statusRec.Body.String(), "challenge") {
		t.Fatalf("probe order status leaked or failed: status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}
	orderReplay := postOperator("/api/v1/account/recordings/campaign-admission/probe-orders", probeOrderBody)
	if orderReplay.Code != http.StatusCreated || orderReplay.Body.String() != order.Body.String() {
		t.Fatalf("probe-order replay changed: first=%s second=%s", order.Body.String(), orderReplay.Body.String())
	}
	nodeBearer := "node-token"
	postNode := func(path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+nodeBearer)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	// Lifecycle-first commit order: the state writer owns the exact claim ->
	// cloud-capacity fences while the lease request starts on another pool
	// connection. The request must resume only after commit and observe the
	// worker as ineligible; it can never cross the drain with a live attempt.
	lifecycleTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lifecycleTx.Exec(ctx, `UPDATE recorder_droplets SET state='draining' WHERE node_id=$1`, nodeID); err != nil {
		_ = lifecycleTx.Rollback(ctx)
		t.Fatalf("stage lifecycle-first drain: %v", err)
	}
	lifecycleLease := make(chan *httptest.ResponseRecorder, 1)
	go func() { lifecycleLease <- postNode("/api/v1/recording/campaign-admission/lease", map[string]any{}) }()
	select {
	case early := <-lifecycleLease:
		_ = lifecycleTx.Rollback(ctx)
		t.Fatalf("probe lease crossed uncommitted lifecycle fence: status=%d body=%s", early.Code, early.Body.String())
	case <-time.After(50 * time.Millisecond):
	}
	if err := lifecycleTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case afterDrain := <-lifecycleLease:
		if afterDrain.Code != http.StatusOK || !strings.Contains(afterDrain.Body.String(), `"target":null`) {
			t.Fatalf("lifecycle-first probe lease was not rejected: status=%d body=%s", afterDrain.Code, afterDrain.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("probe lease did not resume after lifecycle commit")
	}
	if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET state='active',last_seen_at=now() WHERE node_id=$1`, nodeID); err != nil {
		t.Fatal(err)
	}
	var footageFirstJobID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key) VALUES($1,now()-interval '1 second',now()-interval '1 second',60,'pending',$2) RETURNING id`, baselineRecordingID, "campaign-probe-footage-first").Scan(&footageFirstJobID); err != nil {
		t.Fatal(err)
	}
	blockedByFootage := postNode("/api/v1/recording/campaign-admission/lease", map[string]any{})
	if blockedByFootage.Code != http.StatusOK || !strings.Contains(blockedByFootage.Body.String(), `"target":null`) {
		t.Fatalf("due footage did not win before targeted probe: status=%d body=%s", blockedByFootage.Code, blockedByFootage.Body.String())
	}
	if _, err := pool.Exec(ctx, `DELETE FROM recording_jobs WHERE id=$1`, footageFirstJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET capacity=1 WHERE node_id=$1`, nodeID); err != nil {
		t.Fatal(err)
	}
	blockedByReserve := postNode("/api/v1/recording/campaign-admission/lease", map[string]any{})
	if blockedByReserve.Code != http.StatusOK || !strings.Contains(blockedByReserve.Body.String(), `"target":null`) {
		t.Fatalf("targeted probe consumed a worker's footage reserve: status=%d body=%s", blockedByReserve.Code, blockedByReserve.Body.String())
	}
	if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET capacity=5 WHERE node_id=$1`, nodeID); err != nil {
		t.Fatal(err)
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
		if attemptIndex == 0 {
			var probeFirstJobID int64
			if err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key) VALUES($1,now()-interval '1 second',now()-interval '1 second',60,'pending',$2) RETURNING id`, baselineRecordingID, "campaign-probe-probe-first").Scan(&probeFirstJobID); err != nil {
				t.Fatal(err)
			}
			jobLease := postNode("/api/v1/recording/jobs/lease", map[string]any{})
			if jobLease.Code != http.StatusOK || strings.Contains(jobLease.Body.String(), `"job":null`) {
				t.Fatalf("due footage did not claim the reserved slot after a probe started: status=%d body=%s", jobLease.Code, jobLease.Body.String())
			}
			if _, err := pool.Exec(ctx, `UPDATE recording_jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1`, probeFirstJobID); err != nil {
				t.Fatal(err)
			}
		}
		if leaseResponse.Target.ID != streamID || leaseResponse.Target.AttemptID == "" || leaseResponse.Target.Challenge == "" || !strings.Contains(leaseResponse.Target.MediaUploadURL, "/quarantine/campaign-probe/") || !strings.Contains(leaseResponse.Target.FrameUploadURL, "/quarantine/campaign-probe/") {
			t.Fatalf("lease did not return exact server-owned quarantine intents: %#v", leaseResponse.Target)
		}
		// Probe-first commit order: both the supported Store path and direct SQL
		// lifecycle/claim-head writers must observe the durable attempt occupancy.
		// Force-drain may override recording leases, never qualification evidence.
		drained, drainErr := dropletpool.NewStore(pool).MarkDrainingIfIdle(ctx, recorderDropletID)
		if drainErr != nil || drained {
			t.Fatalf("probe-first supported drain crossed occupancy: drained=%v err=%v", drained, drainErr)
		}
		if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET state='draining' WHERE node_id=$1`, nodeID); err == nil {
			t.Fatal("probe-first direct lifecycle update crossed occupancy")
		}
		if _, err := pool.Exec(ctx, `UPDATE nodes SET status='disabled' WHERE id=$1`, nodeID); err == nil {
			t.Fatal("probe-first direct node disable crossed occupancy")
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_worker_claim_heads SET state='recovery_blocked',blocked_at=now(),block_reason='durable_recovery' WHERE node_id=$1`, nodeID); err == nil {
			t.Fatal("probe-first claim-head rotation crossed occupancy")
		}
		for label, statement := range map[string]string{
			"provider id":       `UPDATE recorder_droplets SET do_droplet_id=do_droplet_id+1 WHERE node_id=$1`,
			"region":            `UPDATE recorder_droplets SET region=region||'-changed' WHERE node_id=$1`,
			"size":              `UPDATE recorder_droplets SET size=size||'-changed' WHERE node_id=$1`,
			"build":             `UPDATE recorder_droplets SET build_sha=CASE WHEN build_sha=repeat('a',40) THEN repeat('b',40) ELSE repeat('a',40) END WHERE node_id=$1`,
			"capacity":          `UPDATE recorder_droplets SET capacity=capacity+1 WHERE node_id=$1`,
			"token revoke":      `UPDATE node_tokens SET revoked_at=transaction_timestamp() WHERE id=(SELECT claim_token_id FROM recording_worker_claim_heads WHERE node_id=$1)`,
			"token purpose":     `UPDATE node_tokens SET recording_claim_purpose='existing_fence_only' WHERE id=(SELECT claim_token_id FROM recording_worker_claim_heads WHERE node_id=$1)`,
			"token generation":  `UPDATE node_tokens SET recording_claim_generation=recording_claim_generation+1 WHERE id=(SELECT claim_token_id FROM recording_worker_claim_heads WHERE node_id=$1)`,
			"token key prefix":  `UPDATE node_tokens SET key_prefix=key_prefix||'x' WHERE id=(SELECT claim_token_id FROM recording_worker_claim_heads WHERE node_id=$1)`,
			"token secret hash": `UPDATE node_tokens SET secret_hash=repeat('f',64) WHERE id=(SELECT claim_token_id FROM recording_worker_claim_heads WHERE node_id=$1)`,
		} {
			if _, err := pool.Exec(ctx, statement, nodeID); err == nil {
				t.Fatalf("probe-first %s mutation crossed occupancy", label)
			}
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
		if _, err := fixtureAdmin.Exec(ctx, `INSERT INTO recording_targeted_probe_attempt_terminal_events(attempt_id,result,event_sha256) VALUES($1,'expired_without_evidence',repeat('0',64))`, leaseResponse.Target.AttemptID); err == nil {
			t.Fatal("terminal-without-evidence committed after immutable probe evidence")
		}
		if attemptIndex == 0 {
			if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET state='draining' WHERE node_id=$1`, nodeID); err != nil {
				t.Fatal(err)
			}
			var priorTokenID, priorGeneration int64
			if err := pool.QueryRow(ctx, `SELECT claim_token_id,generation FROM recording_worker_claim_heads WHERE node_id=$1`, nodeID).Scan(&priorTokenID, &priorGeneration); err != nil {
				t.Fatal(err)
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			retirementSHA := sha256Hex([]byte(fmt.Sprintf("recording-worker-claim-retired-v1\x00%d\x00%d\x00%d", nodeID, priorGeneration, priorTokenID)))
			_, err = tx.Exec(ctx, `WITH retired AS (
				UPDATE node_tokens SET revoked_at=transaction_timestamp(),updated_at=transaction_timestamp()
				WHERE id=$3 AND revoked_at IS NULL RETURNING id
			) INSERT INTO recording_worker_claim_generation_events(node_id,generation,predecessor_generation,claim_token_id,event_type,facts_sha256)
			SELECT $1,$2,CASE WHEN $2=1 THEN NULL ELSE $2-1 END,$3,'retired',$4 FROM retired`, nodeID, priorGeneration, priorTokenID, retirementSHA)
			nodeBearer = "node-token-successor"
			if err == nil {
				_, err = tx.Exec(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'campaign03',$2)`, nodeID, hashSecret(nodeBearer))
			}
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(ctx)
			}
			if err != nil {
				t.Fatalf("rotate R10 claim token before terminal replay: %v", err)
			}
		}
		replayed := postNode("/api/v1/recording/campaign-admission/evidence", evidenceBody)
		if replayed.Code != http.StatusCreated || replayed.Body.String() != evidence.Body.String() {
			t.Fatalf("evidence replay %d changed: first=%s second=%s", attemptIndex+1, evidence.Body.String(), replayed.Body.String())
		}
		mismatchBody := map[string]any{
			"approval_id": leaseResponse.ApprovalID, "stream_id": streamID,
			"attempt_id": leaseResponse.Target.AttemptID, "request_id": leaseResponse.RequestID,
			"evidence": recordability.TargetedEvidence{Result: recordability.ResultOK, Detail: "different exact replay request"},
		}
		if mismatch := postNode("/api/v1/recording/campaign-admission/evidence", mismatchBody); mismatch.Code != http.StatusConflict {
			t.Fatalf("mismatched terminal evidence replay status=%d body=%s", mismatch.Code, mismatch.Body.String())
		}
		if attemptIndex == 0 {
			if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET state='active',last_seen_at=now() WHERE node_id=$1`, nodeID); err != nil {
				t.Fatal(err)
			}
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
	var sealedBatch batchScheduleResponse
	if err := json.Unmarshal(firstSchedule.Body.Bytes(), &sealedBatch); err != nil || sealedBatch.CapacityObservation == "" || sealedBatch.StorageObservation == "" || sealedBatch.ForecastPeakSlots <= 0 || sealedBatch.UsableAfterLoss <= 0 || sealedBatch.RequiredFreeBytes <= 0 || sealedBatch.ProjectedFreeBytes < 0 {
		t.Fatalf("DB-canonical capacity/NAS authority missing: err=%v response=%+v", err, sealedBatch)
	}
	trackReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/campaign-tracks", nil)
	trackReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
	trackRec := httptest.NewRecorder()
	router.ServeHTTP(trackRec, trackReq)
	if trackRec.Code != http.StatusOK || !strings.Contains(trackRec.Body.String(), `"grade_floor":"GOOD"`) || !strings.Contains(trackRec.Body.String(), `"required_consecutive_windows":14`) || !strings.Contains(trackRec.Body.String(), `"reporting_grade_floor":"ACCEPTABLE"`) || !strings.Contains(trackRec.Body.String(), `"reporting_required_consecutive_windows":14`) {
		t.Fatalf("campaign qualification policy was not explicitly reported: status=%d body=%s", trackRec.Code, trackRec.Body.String())
	}
	var admittedRecordingID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM recordings WHERE account_id=$1 AND stream_id=$2 AND status='active'`, accountID, streamID).Scan(&admittedRecordingID); err != nil {
		t.Fatal(err)
	}
	canonicalTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = canonicalTx.Exec(ctx, `SET LOCAL TIME ZONE 'Asia/Seoul'; SET LOCAL DateStyle='SQL, DMY'; UPDATE recordings SET next_fire_at=next_fire_at WHERE id=$1; UPDATE recording_campaign_roster_entries SET role=role WHERE recording_id=$1`, admittedRecordingID); err == nil {
		err = canonicalTx.Commit(ctx)
	} else {
		_ = canonicalTx.Rollback(ctx)
	}
	if err != nil {
		t.Fatalf("typed config/roster digest changed with TimeZone or DateStyle: %v", err)
	}
	// Make the prior committed capacity head older than its exact 120-second
	// lease, then execute a complete second approval/evidence/admission. This is
	// a fixture-only time shift under the migration owner; product roles still
	// cannot mutate immutable observations. The second commit must replace both
	// capacity and NAS seals instead of relying on the stale predecessor.
	if _, err := fixtureAdmin.Exec(ctx, `ALTER TABLE recording_campaign_capacity_observations DISABLE TRIGGER recording_campaign_capacity_observations_immutable;
		UPDATE recording_campaign_capacity_observations SET
		  observation_started_at=observation_started_at-interval '181 seconds',
		  observed_at=observed_at-interval '181 seconds',expires_at=expires_at-interval '181 seconds',
		  provider_observation_sha256=encode(sha256(convert_to(
		    floor(extract(epoch from observation_started_at-interval '181 seconds')*1000000)::bigint::text||chr(10)||
		    floor(extract(epoch from observed_at-interval '181 seconds')*1000000)::bigint::text||chr(10)||facts_sha256||chr(10)||
		    build_sha||chr(10)||size_slug||chr(10)||pool_identity_sha256||chr(10)||provider_project_sha256||chr(10)||provider_firewall_sha256,'UTF8')),'hex');
		ALTER TABLE recording_campaign_capacity_observations ENABLE TRIGGER recording_campaign_capacity_observations_immutable`); err != nil {
		t.Fatalf("age prior immutable capacity fixture: %v", err)
	}
	const secondStreamID int64 = 17237
	const secondSource = "https://source.example/second.m3u8"
	const secondPage = "https://source.example/second"
	if _, err := pool.Exec(ctx, `INSERT INTO streams(id,provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,local_timezone,tags) VALUES($1,'publisher-two','scene-2','Second Approved Scene','second-approved-scene',$2,$3,'hls','video_manifest','video_live','continuous_video',30,'Europe/Berlin',ARRAY['FD'])`, secondStreamID, secondSource, secondPage); err != nil {
		t.Fatal(err)
	}
	var secondRevisionID, secondMediaID, secondFrameID int64
	if err := pool.QueryRow(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,new_source_url,new_source_page_url) VALUES($1,'test','second-bind',$2,$3) RETURNING id`, secondStreamID, secondSource, secondPage).Scan(&secondRevisionID); err != nil {
		t.Fatal(err)
	}
	secondBaseline := []byte("second exact reviewed baseline frame bytes")
	secondBaselineETag := store.put("scene-second.jpg", "image/jpeg", secondBaseline)
	secondBaselineSHA := sha256Hex(secondBaseline)
	secondSceneSHA := sha256Hex([]byte("stoarama-scene-identity-v1\nsecond approved scene"))
	if err := pool.QueryRow(ctx, `INSERT INTO media_objects(storage_provider,bucket,object_key,mime_type,size_bytes,etag,sha256) VALUES('r2','admission','scene-second.jpg','image/jpeg',$1,$2,$3) RETURNING id`, len(secondBaseline), secondBaselineETag, secondBaselineSHA).Scan(&secondMediaID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO frames(stream_id,captured_at,raw_media_object_id,capture_status,source_kind) VALUES($1,now()-interval '1 minute',$2,'success','authoritative_frame_refresh') RETURNING id`, secondStreamID, secondMediaID).Scan(&secondFrameID); err != nil {
		t.Fatal(err)
	}
	if err := s.admissionPool.QueryRow(ctx, `SELECT stream_id,source_url,source_page_url,COALESCE(source_revision_id,0),source_snapshot_sha256 FROM recording_campaign_prepare_authoritative_frame($1,'deniz_fd_restore_20260814',$2)`, accountID, secondStreamID).Scan(&preparedStreamID, &preparedSource, &preparedPage, &preparedRevisionID, &preparedSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := s.admissionPool.QueryRow(ctx, `SELECT recording_campaign_seal_authoritative_frame($1,'deniz_fd_restore_20260814',$2,$3,$4,$5,$6)`, accountID, secondStreamID, secondFrameID, preparedRevisionID, preparedSnapshot, secondBaselineSHA).Scan(&frameWitnessSHA); err != nil || len(frameWitnessSHA) != 64 {
		t.Fatalf("seal second baseline frame witness: sha=%q err=%v", frameWitnessSHA, err)
	}
	secondBaselineRequest := uuid.NewString()
	secondPresentReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/account/recordings/qualification/scene-presentations/%d?stream_id=%d&authority_code=deniz_fd_restore_20260814&request_id=%s", secondFrameID, secondStreamID, secondBaselineRequest), nil)
	secondPresentReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
	secondPresentRec := httptest.NewRecorder()
	router.ServeHTTP(secondPresentRec, secondPresentReq)
	if secondPresentRec.Code != http.StatusOK {
		t.Fatalf("second baseline presentation status=%d body=%s", secondPresentRec.Code, secondPresentRec.Body.String())
	}
	secondAttestBody, _ := json.Marshal(sceneAttestRequest{FrameID: secondFrameID, PresentationID: secondPresentRec.Header().Get("X-Stoarama-Presentation-ID"), SceneIdentity: "Second Approved Scene"})
	secondAttestReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/qualification/scene-attest", bytes.NewReader(secondAttestBody))
	secondAttestReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
	secondAttestRec := httptest.NewRecorder()
	router.ServeHTTP(secondAttestRec, secondAttestReq)
	var secondBaselineEvidence struct {
		EvidenceID int64 `json:"evidence_id"`
	}
	if secondAttestRec.Code != http.StatusOK || json.Unmarshal(secondAttestRec.Body.Bytes(), &secondBaselineEvidence) != nil || secondBaselineEvidence.EvidenceID <= 0 {
		t.Fatalf("second baseline attest status=%d body=%s", secondAttestRec.Code, secondAttestRec.Body.String())
	}
	secondSchedule := schedule
	secondSchedule.StreamIDs = []int64{secondStreamID}
	secondSchedule.StreamTimezones = []streamTimezoneInput{{StreamID: secondStreamID, Timezone: "Europe/Berlin"}}
	secondSchedule.CampaignAdmissionApprovalID = ""
	secondApprovalBody, _ := json.Marshal(campaignAdmissionApprovalRequest{
		RequestID: uuid.NewString(), DeadlineAt: end, AuthorityCode: "deniz_fd_restore_20260814", FailureDomainTag: "FD",
		Entries:  []campaignAdmissionApprovalEntry{{StreamID: secondStreamID, SourceRevisionID: secondRevisionID, SourceURL: secondSource, SourcePageURL: secondPage, Provider: "publisher-two", ExternalID: "scene-2", NormalizedLabel: "secondapprovedscene", SceneFrameEvidenceID: secondBaselineEvidence.EvidenceID, SceneIdentitySHA256: secondSceneSHA}},
		Schedule: secondSchedule,
	})
	secondApprovalRec := postOperator("/api/v1/account/recordings/campaign-admission/approvals", secondApprovalBody)
	var secondApproval struct {
		ApprovalID string `json:"approval_id"`
	}
	if secondApprovalRec.Code != http.StatusCreated || json.Unmarshal(secondApprovalRec.Body.Bytes(), &secondApproval) != nil || secondApproval.ApprovalID == "" {
		t.Fatalf("second approval status=%d body=%s", secondApprovalRec.Code, secondApprovalRec.Body.String())
	}
	secondOrderBody, _ := json.Marshal(campaignAdmissionProbeOrderRequest{ApprovalID: secondApproval.ApprovalID, StreamID: secondStreamID, RequestID: uuid.NewString()})
	if rec := postOperator("/api/v1/account/recordings/campaign-admission/probe-orders", secondOrderBody); rec.Code != http.StatusCreated {
		t.Fatalf("second probe order status=%d body=%s", rec.Code, rec.Body.String())
	}
	for attemptIndex, colorName := range []string{"yellow", "purple"} {
		lease := postNode("/api/v1/recording/campaign-admission/lease", map[string]any{})
		var leaseResponse struct {
			Target     *recordability.Target `json:"target"`
			ApprovalID string                `json:"approval_id"`
			RequestID  string                `json:"request_id"`
		}
		if lease.Code != http.StatusOK || json.Unmarshal(lease.Body.Bytes(), &leaseResponse) != nil || leaseResponse.Target == nil || leaseResponse.Target.ID != secondStreamID {
			t.Fatalf("second probe lease %d status=%d body=%s", attemptIndex+1, lease.Code, lease.Body.String())
		}
		mediaArchive, frame := buildCampaignProbeArchive(t, colorName)
		store.put(fmt.Sprintf("quarantine/campaign-probe/%s/media.zip", leaseResponse.Target.AttemptID), "application/zip", mediaArchive)
		store.put(fmt.Sprintf("quarantine/campaign-probe/%s/frame.jpg", leaseResponse.Target.AttemptID), "image/jpeg", frame)
		evidenceBody := map[string]any{"approval_id": leaseResponse.ApprovalID, "stream_id": secondStreamID, "attempt_id": leaseResponse.Target.AttemptID, "request_id": leaseResponse.RequestID, "evidence": recordability.TargetedEvidence{Result: recordability.ResultOK, Detail: "caller fields are non-authoritative"}}
		evidenceRec := postNode("/api/v1/recording/campaign-admission/evidence", evidenceBody)
		var evidenceResponse struct {
			EvidenceID string `json:"evidence_id"`
		}
		if evidenceRec.Code != http.StatusCreated || json.Unmarshal(evidenceRec.Body.Bytes(), &evidenceResponse) != nil || evidenceResponse.EvidenceID == "" {
			t.Fatalf("second evidence %d status=%d body=%s", attemptIndex+1, evidenceRec.Code, evidenceRec.Body.String())
		}
		probePresentationReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/recordings/campaign-admission/scene-presentations/"+evidenceResponse.EvidenceID+"?request_id="+uuid.NewString(), nil)
		probePresentationReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
		probePresentationRec := httptest.NewRecorder()
		router.ServeHTTP(probePresentationRec, probePresentationReq)
		var probePresentation struct {
			PresentationID string `json:"presentation_id"`
		}
		if probePresentationRec.Code != http.StatusOK || json.Unmarshal(probePresentationRec.Body.Bytes(), &probePresentation) != nil || probePresentation.PresentationID == "" {
			t.Fatalf("second scene presentation %d status=%d body=%s", attemptIndex+1, probePresentationRec.Code, probePresentationRec.Body.String())
		}
		reviewBody, _ := json.Marshal(campaignAdmissionSceneReviewRequest{ApprovalID: secondApproval.ApprovalID, ProbeEvidenceID: evidenceResponse.EvidenceID, PresentationID: probePresentation.PresentationID, RequestID: uuid.NewString()})
		if review := postOperator("/api/v1/account/recordings/campaign-admission/scene-reviews", reviewBody); review.Code != http.StatusCreated {
			t.Fatalf("second scene review %d status=%d body=%s", attemptIndex+1, review.Code, review.Body.String())
		}
		if attemptIndex == 0 {
			advanceCampaignAdmissionClock(t, pool, 61)
		}
	}
	advanceCampaignAdmissionClock(t, pool, 0)
	secondSchedule.CampaignAdmissionApprovalID = secondApproval.ApprovalID
	secondScheduleBody, _ := json.Marshal(secondSchedule)
	postSecondSchedule := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/account/recordings/batch-schedule", bytes.NewReader(secondScheduleBody))
		request.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		return response
	}
	// Freeze admission immediately after its bounded provider reads. A provider
	// identity mutation while the global DB fence is held must invalidate that
	// observation; it cannot be freshness-laundered with DB-now after the wait.
	providerReads := make(chan struct{}, 2)
	s.campaignDOAttest = func(_ context.Context, dropletID int64, name string) (dropletpool.ProviderAttestation, error) {
		region := "nyc1"
		if dropletID == 1000 {
			region = "sfo3"
		}
		providerReads <- struct{}{}
		return dropletpool.ProviderAttestation{DropletID: dropletID, Name: name, Region: region, SizeSlug: "s-2vcpu-4gb", Status: "active"}, nil
	}
	capacityFenceTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capacityFenceTx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('campaign-admission-capacity-v1',0))`); err != nil {
		_ = capacityFenceTx.Rollback(ctx)
		t.Fatal(err)
	}
	staleProviderResult := make(chan *httptest.ResponseRecorder, 1)
	go func() { staleProviderResult <- postSecondSchedule() }()
	for i := 0; i < 2; i++ {
		select {
		case <-providerReads:
		case <-time.After(5 * time.Second):
			_ = capacityFenceTx.Rollback(ctx)
			t.Fatal("second admission did not complete its bounded provider observation")
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET do_droplet_id=do_droplet_id+10000 WHERE node_id=$1`, nodeID); err != nil {
		_ = capacityFenceTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := capacityFenceTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case stale := <-staleProviderResult:
		if stale.Code != http.StatusConflict {
			t.Fatalf("provider identity changed behind bounded observation: status=%d body=%s", stale.Code, stale.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second admission did not resume after capacity fence")
	}
	if _, err := pool.Exec(ctx, `UPDATE recorder_droplets SET do_droplet_id=do_droplet_id-10000 WHERE node_id=$1`, nodeID); err != nil {
		t.Fatal(err)
	}
	// Restore the ordinary deterministic provider callback for the successful
	// retry. The failed transaction did not consume the approval or seals.
	s.campaignDOAttest = func(_ context.Context, dropletID int64, name string) (dropletpool.ProviderAttestation, error) {
		region := "nyc1"
		if dropletID == 1000 {
			region = "sfo3"
		}
		return dropletpool.ProviderAttestation{DropletID: dropletID, Name: name, Region: region, SizeSlug: "s-2vcpu-4gb", Status: "active"}, nil
	}
	secondScheduleRec := postSecondSchedule()
	if secondScheduleRec.Code != http.StatusOK {
		t.Fatalf("complete second admission did not replace stale capacity/NAS seals: status=%d body=%s", secondScheduleRec.Code, secondScheduleRec.Body.String())
	}
	var capacitySealCount, storageSealCount int
	if err := fixtureAdmin.QueryRow(ctx, `SELECT count(DISTINCT cap.approval_id),count(DISTINCT storage.approval_id) FROM recording_campaign_capacity_reservations cap FULL JOIN recording_campaign_storage_reservations storage ON storage.approval_id=cap.approval_id WHERE cap.account_id=$1 OR storage.account_id=$1`, accountID).Scan(&capacitySealCount, &storageSealCount); err != nil || capacitySealCount != 2 || storageSealCount != 2 {
		t.Fatalf("second admission did not commit replacement capacity/NAS seals: capacity=%d storage=%d err=%v", capacitySealCount, storageSealCount, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recordings SET next_fire_at=NULL WHERE id=$1`, admittedRecordingID); err == nil {
		t.Fatal("runtime direct SQL cleared the sealed next-fire schedule")
	}
	var aliasStreamID int64
	if err := pool.QueryRow(ctx, `INSERT INTO streams(provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,local_timezone) VALUES('other','other-scene','Other Scene','other-scene','https://other.example/live.m3u8','https://other.example/page','hls','video_manifest','video_live','continuous_video',30,'UTC') RETURNING id`).Scan(&aliasStreamID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recordings SET stream_id=$2,stream_url='https://other.example/live.m3u8',name='Other Scene' WHERE id=$1`, admittedRecordingID, aliasStreamID); err == nil {
		t.Fatal("runtime direct SQL rebound an admitted active recording")
	}
	if _, err := pool.Exec(ctx, `UPDATE streams SET provider='mutated-provider' WHERE id=$1`, streamID); err == nil {
		t.Fatal("runtime direct SQL mutated the admitted source identity")
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_campaign_roster_entries SET role='backup' WHERE recording_id=$1`, admittedRecordingID); err == nil {
		t.Fatal("runtime direct SQL rewrote admitted roster provenance")
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_campaign_roster_entries SET effective_at=decision_at+interval '1 second',decision_at=decision_at+interval '1 second',source_window_end_at=now(),source_health_recording_id=$1 WHERE recording_id=$1`, admittedRecordingID); err == nil {
		t.Fatal("runtime direct SQL rewrote admitted roster time/source provenance")
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_campaign_tracks SET grade_floor='ACCEPTABLE' WHERE id=(SELECT track_id FROM recording_campaign_roster_entries WHERE recording_id=$1)`, admittedRecordingID); err == nil {
		t.Fatal("runtime direct SQL weakened the governing 14-window GOOD/GREAT qualification policy")
	}
	if _, err := pool.Exec(ctx, `UPDATE recordings SET clip_duration_sec=120 WHERE id=$1`, baselineRecordingID); err == nil {
		t.Fatal("ordinary active schedule mutation bypassed typed campaign capacity/NAS recomputation")
	}
	if _, err := pool.Exec(ctx, `UPDATE recordings SET stream_url='https://baseline.example/repaired.m3u8',source_kind='hls_live' WHERE id=$1`, baselineRecordingID); err != nil {
		t.Fatalf("occupancy-neutral supported source repair was overblocked: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recordings SET stream_id=$2,stream_url=(SELECT stream_url FROM streams WHERE id=$2) WHERE id=$1`, baselineRecordingID, streamID); err == nil {
		t.Fatal("ordinary active identity rebind collided with protected campaign occupancy")
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_free_bytes=1,nas_storage_reported_at=now(),last_seen_at=now() WHERE id=$1`, nasConnectionID); err != nil {
		t.Fatal(err)
	}
	var secondNASKeyID int64
	if err := pool.QueryRow(ctx, `INSERT INTO account_api_keys(account_id,key_prefix,secret_hash,scopes) VALUES($1,'campaign-nas-2','campaign-nas-secret-2',ARRAY['stoarama.pull']) RETURNING id`, accountID).Scan(&secondNASKeyID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO connections(account_id,kind,api_key_id,last_seen_at,nas_storage_total_bytes,nas_storage_free_bytes,nas_storage_reported_at,nas_capacity_blocked) VALUES($1,'nas_pull',$2,now(),2000000000000,1900000000000,now(),false)`, accountID, secondNASKeyID); err != nil {
		t.Fatal(err)
	}
	var unrelatedRecordingID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,stream_id,source_kind,mode,cron_expr,cron_timezone,clip_duration_sec,status,start_at,capture_via) VALUES($1,$2,'unrelated','https://other.example/live.m3u8',$3,'hls_live','sampled','0 * * * *','UTC',60,'completed',now(),'cloud') RETURNING id`, accountID, destinationID, aliasStreamID).Scan(&unrelatedRecordingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recordings SET status='active' WHERE id=$1`, unrelatedRecordingID); err == nil {
		t.Fatal("free bytes from a different NAS connection authorized the sealed reservation")
	}
	if _, err := pool.Exec(ctx, `UPDATE account_sessions SET revoked_at=now() WHERE session_hash=$1; UPDATE recorder_droplets SET state='draining' WHERE node_id IN($2,$3)`, hashSecret(rawSession), nodeID, standbyNodeID); err != nil {
		t.Fatal(err)
	}
	replayBody, _ := json.Marshal(campaignAdmissionReplayRequest{ApprovalID: approvalResponse.ApprovalID, TargetAccountID: accountID})
	replayReq := httptest.NewRequest(http.MethodPost, "/api/v1/recordings/campaign-admission/replay", bytes.NewReader(replayBody))
	replayReq.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: rawSession})
	replayRec := httptest.NewRecorder()
	router.ServeHTTP(replayRec, replayReq)
	if replayRec.Code != http.StatusOK || replayRec.Body.String() != firstSchedule.Body.String() {
		t.Fatalf("sealed admission replay after session/provider revocation changed: first=%s second=%s", firstSchedule.Body.String(), replayRec.Body.String())
	}
	var recordings int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recordings WHERE account_id=$1 AND stream_id=$2`, accountID, streamID).Scan(&recordings); err != nil || recordings != 1 {
		t.Fatalf("invalid sealed recording count=%d err=%v", recordings, err)
	}
}
