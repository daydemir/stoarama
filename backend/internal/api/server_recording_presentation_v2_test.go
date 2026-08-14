package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/daydemir/stoarama/backend/internal/secretbox"
)

type presentationV2Fixture struct {
	accountID     int64
	nodeID        int64
	recordingID   int64
	streamID      int64
	jobID         int64
	destinationID int64
	admissionID   uuid.UUID
	attemptID     uuid.UUID
	leaseToken    uuid.UUID
	taskID        uuid.UUID
}

func testPresentationV2Pool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("presentation_v2_api_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": schema}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	if err = migrateAPITestSchema(ctx, pool, filepath.Join("..", "..", "..", "infra", "sql", "migrations")); err != nil {
		pool.Close()
		admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	return pool, func() {
		pool.Close()
		admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	}
}

func seedPresentationV2Task(t *testing.T, pool *pgxpool.Pool, suffix int64, accountID int64) presentationV2Fixture {
	t.Helper()
	ctx := context.Background()
	nodeID := 8000 + suffix
	streamID := 8100 + suffix
	recordingID := 8200 + suffix
	jobID := 8300 + suffix
	destinationCandidateID := 8400 + suffix
	var destinationID int64
	clipID := 8500 + suffix
	lease := uuid.New()
	admission := uuid.New()
	attempt := uuid.New()
	intent := uuid.New()
	task := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO accounts(id,email,name,status,role) VALUES($1,$2,$2,'active','admin') ON CONFLICT(id) DO NOTHING`, accountID, fmt.Sprintf("presentation-%d@example.test", accountID)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO storage_destinations(id,account_id,name,provider,endpoint,region,bucket,key_prefix,access_key_id,secret_access_key_enc,status,verified_at,managed,shared)
		VALUES($1,$2,'v2','r2_managed','https://storage.example.test','auto','v2','','access',decode('00','hex'),'verified',now(),true,false)
		ON CONFLICT (account_id) WHERE managed DO NOTHING`, destinationCandidateID, accountID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id FROM storage_destinations WHERE account_id=$1 AND managed`, accountID).Scan(&destinationID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO streams(id,provider,external_id,name,slug,source_url,source_page_url,capture_type,source_family,execution_class,capture_family,expected_fps,recording_state) VALUES($1,'test',$2,$2,$2,$3,$4,'hls','video_manifest','video_live','continuous_video',30,'on')`, streamID, fmt.Sprintf("v2-%d", suffix), fmt.Sprintf("https://source.example.test/%d.m3u8", suffix), fmt.Sprintf("https://source.example.test/page/%d", suffix)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nodes(id,account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams) VALUES($1,$2,$3,'relay','active',now(),4)`, nodeID, accountID, fmt.Sprintf("v2-node-%d", suffix)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,stream_id,source_kind,cron_timezone,clip_duration_sec,mode,daily_window_start,daily_window_end,active_weekdays,target_fps,status,start_at,capture_via) VALUES($1,$2,$3,$4,$5,$6,'hls_live','UTC',60,'continuous','08:00','20:00',127,NULL,'active',now()-interval '1 hour','relay')`, recordingID, accountID, destinationID, fmt.Sprintf("v2-rec-%d", suffix), fmt.Sprintf("https://source.example.test/%d.m3u8", suffix), streamID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,lease_token,idempotency_key,kind,window_end_at) VALUES($1,$2,now(),now(),60,'leased',$3,now()+interval '1 hour',$4,$5,'continuous_window',now()+interval '1 hour')`, jobID, recordingID, fmt.Sprintf("node:%d", nodeID), lease, fmt.Sprintf("v2-job-%d", suffix)); err != nil {
		t.Fatal(err)
	}
	var revisionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,new_source_url,new_source_page_url,new_source_family,new_capture_type,new_execution_class) SELECT id,'test','v2 fixture',source_url,source_page_url,source_family,capture_type,execution_class FROM streams WHERE id=$1 RETURNING id`, streamID).Scan(&revisionID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_presentation_v2_admissions(
		 id,account_id,recording_id,stream_id,recording_job_id,lease_token,node_id,source_revision_id,
		 recording_stream_url_sha256,stream_source_url_sha256,stream_source_page_url_sha256,source_snapshot_sha256,
		 provider,external_id,source_family,capture_type,execution_class,capture_mode,audio_selection,policy_version,
		 parser_schema,capture_tool_identity_sha256,deadline_at)
		SELECT $1,r.account_id,r.id,r.stream_id,j.id,j.lease_token,n.id,sr.id,
		 encode(sha256(convert_to(r.stream_url,'UTF8')),'hex'),encode(sha256(convert_to(s.source_url,'UTF8')),'hex'),
		 encode(sha256(convert_to(s.source_page_url,'UTF8')),'hex'),
		 encode(sha256(convert_to(jsonb_build_array(r.account_id,r.id,r.stream_id,j.id,j.lease_token,n.id,
		 r.stream_url,s.source_url,s.source_page_url,s.provider,s.external_id,s.source_family,s.capture_type,
			s.execution_class,sr.id,'source_copy','first_optional')::text,'UTF8')),'hex'),
		 s.provider,s.external_id,s.source_family,s.capture_type,s.execution_class,'source_copy','first_optional',
		 'continuous-source-presentation-edge-v2','presentation-probe-v2',
		 recording_presentation_v2_tool_identity('8.1','8.1','62','62','60',repeat('b',64),'mov','h264','aac','presentation-probe-v2'),
		 now()+interval '14 minutes'
		FROM recordings r JOIN recording_jobs j ON j.recording_id=r.id JOIN streams s ON s.id=r.stream_id
		JOIN stream_source_revisions sr ON sr.id=$2::bigint JOIN nodes n ON n.id=$3
		WHERE r.id=$4 AND j.id=$5`, admission, revisionID, nodeID, recordingID, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_presentation_v2_attempts(id,admission_id,account_id,recording_id,stream_id,recording_job_id,lease_token,node_id,idempotency_key,ffmpeg_version,ffprobe_version,libavformat_version,libavcodec_version,libavutil_version,build_flags_sha256,demuxer_name,video_decoder_name,audio_decoder_name,parser_schema,request_sha256,response_sha256) VALUES($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,'8.1','8.1','62','62','60',repeat('b',64),'mov','h264','aac','presentation-probe-v2',repeat('c',64),encode(sha256(convert_to('attempt:'||$1::uuid::text,'UTF8')),'hex'))`, attempt, admission, accountID, recordingID, streamID, jobID, lease, nodeID, fmt.Sprintf("attempt-%d", suffix)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at) VALUES($1,$2,$3,$4,'https://storage.example.test','v2',$5,$5,'video/mp4',4096,'consumed',now()+interval '1 hour')`, intent, recordingID, jobID, destinationID, fmt.Sprintf("clip-%d.mp4", suffix)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,audio_present,fire_at,clip_start_at,clip_end_at,capture_lease_token,capture_sequence) VALUES($1,$2,$3,$4,'https://storage.example.test','v2',$5,$5,'video/mp4','mp4',1024,'etag',repeat('e',64),60000,'h264',false,now(),now(),now()+interval '1 minute',$6,1)`, clipID, recordingID, jobID, destinationID, fmt.Sprintf("clip-%d.mp4", suffix), lease); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_presentation_v2_probe_tasks(id,admission_id,attempt_id,account_id,recording_id,stream_id,recording_job_id,clip_id,upload_intent_id,lease_token,node_id,capture_sequence,clip_size_bytes,clip_sha256,local_upload_identity_sha256,staging_identity_sha256,staging_method,staging_device_id,staging_inode_id,request_sha256,response_sha256,initial_disposition,state,retention_state,absolute_deadline_at) VALUES($1::uuid,$2,$3,$4,$5,$6,$7,$8::bigint,$9,$10,$11,1,1024,repeat('e',64),repeat('f',64),repeat('1',64),'hardlink','42',$12,repeat('2',64),encode(sha256(convert_to('task:'||$1::uuid::text||':'||$8::bigint::text||':awaiting_retention','UTF8')),'hex'),'retained','awaiting_retention','awaiting',now()+interval '8 minutes')`, task, admission, attempt, accountID, recordingID, streamID, jobID, clipID, intent, lease, nodeID, fmt.Sprint(100000+suffix)); err != nil {
		t.Fatal(err)
	}
	return presentationV2Fixture{accountID: accountID, nodeID: nodeID, recordingID: recordingID, streamID: streamID, jobID: jobID, destinationID: destinationID, admissionID: admission, attemptID: attempt, leaseToken: lease, taskID: task}
}

func addPresentationV2Task(t *testing.T, pool *pgxpool.Pool, f presentationV2Fixture, sequence int64, mode string) uuid.UUID {
	t.Helper()
	if mode != "ready" && mode != "expired" && mode != "exhausted" {
		t.Fatalf("invalid presentation task fixture mode %q", mode)
	}
	ctx := context.Background()
	intent, task := uuid.New(), uuid.New()
	var clipID int64
	name := fmt.Sprintf("extra-%s-%d.mp4", f.taskID.String()[:8], sequence)
	clipSHA := fmt.Sprintf("%064x", sequence)
	if _, err := pool.Exec(ctx, `INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at) SELECT $1,$2,$3,id,endpoint,bucket,$4,$4,'video/mp4',4096,'consumed',now()+interval '1 hour' FROM storage_destinations WHERE id=$5`, intent, f.recordingID, f.jobID, name, f.destinationID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,audio_present,fire_at,clip_start_at,clip_end_at,capture_lease_token,capture_sequence) SELECT $1,$2,id,endpoint,bucket,$3,$3,'video/mp4','mp4',1024,'etag',$4,60000,'h264',false,now(),now(),now()+interval '1 minute',$5,$6 FROM storage_destinations WHERE id=$7 RETURNING id`, f.recordingID, f.jobID, name, clipSHA, f.leaseToken, sequence, f.destinationID).Scan(&clipID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_presentation_v2_probe_tasks(id,admission_id,attempt_id,account_id,recording_id,stream_id,recording_job_id,clip_id,upload_intent_id,lease_token,node_id,capture_sequence,clip_size_bytes,clip_sha256,local_upload_identity_sha256,staging_identity_sha256,staging_method,staging_device_id,staging_inode_id,request_sha256,response_sha256,initial_disposition,state,retention_state,absolute_deadline_at,created_at) VALUES($1::uuid,$2,$3,$4,$5,$6,$7,$8::bigint,$9,$10,$11,$12,1024,$13,repeat('f',64),repeat('1',64),'hardlink','42',$14,repeat('2',64),encode(sha256(convert_to('task:'||$1::uuid::text||':'||$8::bigint::text||':awaiting_retention','UTF8')),'hex'),'retained','awaiting_retention','awaiting',CASE WHEN $15 THEN now()-interval '1 minute' ELSE now()+interval '8 minutes' END,CASE WHEN $15 THEN now()-interval '2 minutes' ELSE now() END)`, task, f.admissionID, f.attemptID, f.accountID, f.recordingID, f.streamID, f.jobID, clipID, intent, f.leaseToken, f.nodeID, sequence, clipSHA, fmt.Sprint(200000+sequence), mode == "expired"); err != nil {
		t.Fatal(err)
	}
	if mode == "expired" {
		return task
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_presentation_v2_probe_tasks SET state='pending',retention_state='active',retention_method=staging_method,retention_device_id=staging_device_id,retention_inode_id=staging_inode_id,retention_identity_sha256=recording_presentation_v2_retention_identity(id,node_id,staging_method,staging_device_id,staging_inode_id,'',clip_size_bytes,clip_sha256,absolute_deadline_at),revision=revision+1 WHERE id=$1`, task); err != nil {
		t.Fatal(err)
	}
	if mode == "exhausted" {
		for i := 0; i < presentationV2MaxAttempts; i++ {
			token := uuid.New()
			if _, err := pool.Exec(ctx, `UPDATE recording_presentation_v2_probe_tasks SET state='leased',claim_token=$2,lease_expires_at=now()+interval '1 minute',attempt_count=attempt_count+1,revision=revision+1 WHERE id=$1`, task, token); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `UPDATE recording_presentation_v2_probe_tasks SET state='pending',claim_token=NULL,lease_expires_at=NULL,revision=revision+1 WHERE id=$1`, task); err != nil {
				t.Fatal(err)
			}
		}
	}
	return task
}

func TestRecordingPresentationV2IngestAmbiguousReplayAndUnavailablePostgres(t *testing.T) {
	pool, cleanup := testPresentationV2Pool(t)
	defer cleanup()
	f := seedPresentationV2Task(t, pool, 21, 47221)
	ctx := context.Background()
	var heads atomic.Int64
	objectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("object method=%s", r.Method)
		}
		heads.Add(1)
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("ETag", `"v2-etag"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer objectServer.Close()
	secrets, err := secretbox.NewFromBase64Key("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := secrets.Encrypt([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE storage_destinations SET endpoint=$2,region='auto',bucket='v2',key_prefix='',access_key_id='access',secret_access_key_enc=$3 WHERE id=$1`, f.destinationID, objectServer.URL, sealed); err != nil {
		t.Fatal(err)
	}
	intent := uuid.New()
	if _, err = pool.Exec(ctx, `INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at) VALUES($1,$2,$3,$4,$5,'v2','ambiguous.mp4','ambiguous.mp4','video/mp4',4096,'pending',now()+interval '1 hour')`, intent, f.recordingID, f.jobID, f.destinationID, objectServer.URL); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Microsecond)
	body := recordingClipIngestRequest{
		IntentID: intent.String(), JobID: f.jobID, SizeBytes: 1024, ETag: "v2-etag", SHA256: strings.Repeat("6", 64),
		DurationMs: 60_000, VideoCodec: "h264", Container: "mp4", ClipStartAt: start.Format(time.RFC3339Nano),
		ClipEndAt: start.Add(time.Minute).Format(time.RFC3339Nano), CaptureSequence: 2,
		PresentationProbe: &presentationV2IngestEnvelope{AttemptID: f.attemptID.String(), LocalUploadIdentitySHA256: strings.Repeat("7", 64), Disposition: "retained", StagingIdentitySHA256: strings.Repeat("8", 64), StagingMethod: "hardlink", StagingDeviceID: "42", StagingInodeID: "300001"},
	}
	request := func(v recordingClipIngestRequest) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(v)
		r := httptest.NewRequest(http.MethodPost, "/api/v1/recording/clips/ingest", bytes.NewReader(raw))
		r.Header.Set(recordingLeaseTokenHeader, f.leaseToken.String())
		r = r.WithContext(context.WithValue(r.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: f.nodeID, AccountID: f.accountID, NodeType: nodeTypeRelay}))
		out := httptest.NewRecorder()
		(&Server{pool: pool, secrets: secrets}).handleRecordingClipIngest(out, r)
		return out
	}
	first := request(body)
	if first.Code != http.StatusOK || heads.Load() != 1 || !strings.Contains(first.Body.String(), `"state":"awaiting_retention"`) {
		t.Fatalf("first ingest status=%d heads=%d body=%s", first.Code, heads.Load(), first.Body.String())
	}
	replay := request(body)
	if replay.Code != http.StatusOK || heads.Load() != 1 || !strings.Contains(replay.Body.String(), `"replayed":true`) {
		t.Fatalf("ingest replay status=%d heads=%d body=%s", replay.Code, heads.Load(), replay.Body.String())
	}
	body.DurationMs++
	different := request(body)
	if different.Code != http.StatusConflict || heads.Load() != 1 {
		t.Fatalf("different replay status=%d heads=%d body=%s", different.Code, heads.Load(), different.Body.String())
	}
	var taskCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_presentation_v2_probe_tasks WHERE upload_intent_id=$1`, intent).Scan(&taskCount); err != nil || taskCount != 1 {
		t.Fatalf("ambiguous ingest task count=%d err=%v", taskCount, err)
	}

	unavailableIntent := uuid.New()
	if _, err = pool.Exec(ctx, `INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at) VALUES($1,$2,$3,$4,$5,'v2','unavailable.mp4','unavailable.mp4','video/mp4',4096,'pending',now()+interval '1 hour')`, unavailableIntent, f.recordingID, f.jobID, f.destinationID, objectServer.URL); err != nil {
		t.Fatal(err)
	}
	body.IntentID, body.CaptureSequence, body.DurationMs = unavailableIntent.String(), 3, 60_000
	body.SHA256 = strings.Repeat("a", 64)
	body.PresentationProbe = &presentationV2IngestEnvelope{AttemptID: f.attemptID.String(), LocalUploadIdentitySHA256: strings.Repeat("9", 64), Disposition: "unavailable", UnavailableReason: "retention_unavailable"}
	unavailable := request(body)
	if unavailable.Code != http.StatusOK || !strings.Contains(unavailable.Body.String(), `"state":"unavailable"`) {
		t.Fatalf("unavailable ingest status=%d body=%s", unavailable.Code, unavailable.Body.String())
	}
	var state, retention string
	var releases int
	if err = pool.QueryRow(ctx, `SELECT state,retention_state,(SELECT count(*) FROM recording_presentation_v2_release_authorizations a WHERE a.task_id=t.id) FROM recording_presentation_v2_probe_tasks t WHERE upload_intent_id=$1`, unavailableIntent).Scan(&state, &retention, &releases); err != nil || state != "unavailable" || retention != "none" || releases != 0 {
		t.Fatalf("initial unavailable state=%q retention=%q releases=%d err=%v", state, retention, releases, err)
	}
}

func TestRecordingPresentationV2CampaignProtectionFencePostgres(t *testing.T) {
	pool, cleanup := testPresentationV2Pool(t)
	defer cleanup()
	f := seedPresentationV2Task(t, pool, 11, 47111)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var actor, track int64
	if err := pool.QueryRow(ctx, `INSERT INTO users(email,name,is_operator) VALUES($1,'presentation operator',true) RETURNING id`, fmt.Sprintf("presentation-operator-%d@example.test", time.Now().UnixNano())).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,required_consecutive_windows,created_by_user_id) VALUES($1,$2,'presentation fence',now()+interval '1 day',1,'GOOD',0,$3) RETURNING id`, f.accountID, fmt.Sprintf("presentation-fence-%d", time.Now().UnixNano()), actor).Scan(&track); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_campaign_roster_entries(track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id) VALUES($1,$2,$3,repeat('a',64),'primary',1,'protect',ARRAY['test'],now(),now(),now(),repeat('b',64),$4)`, track, f.recordingID, f.streamID, actor); err != nil {
		t.Fatal(err)
	}
	raceJob := f.jobID + 1_000_000
	raceLease, raceAdmission := uuid.New(), uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,lease_token,idempotency_key,kind,window_end_at) VALUES($1,$2,now(),now(),60,'leased',$3,now()+interval '1 hour',$4,$5,'continuous_window',now()+interval '1 hour')`, raceJob, f.recordingID, fmt.Sprintf("node:%d", f.nodeID), raceLease, fmt.Sprintf("race-admission-%d", time.Now().UnixNano())); err != nil {
		t.Fatal(err)
	}
	insertAdmission := func(tx pgx.Tx, target presentationV2Fixture, admission uuid.UUID, jobID int64, lease uuid.UUID) error {
		_, insertErr := tx.Exec(ctx, `INSERT INTO recording_presentation_v2_admissions(id,account_id,recording_id,stream_id,recording_job_id,lease_token,node_id,source_revision_id,recording_stream_url_sha256,stream_source_url_sha256,stream_source_page_url_sha256,source_snapshot_sha256,provider,external_id,source_family,capture_type,execution_class,capture_mode,audio_selection,policy_version,parser_schema,capture_tool_identity_sha256,deadline_at)
		SELECT $1,r.account_id,r.id,r.stream_id,j.id,j.lease_token,n.id,(SELECT max(id) FROM stream_source_revisions WHERE stream_id=s.id),encode(sha256(convert_to(r.stream_url,'UTF8')),'hex'),encode(sha256(convert_to(s.source_url,'UTF8')),'hex'),encode(sha256(convert_to(s.source_page_url,'UTF8')),'hex'),encode(sha256(convert_to(jsonb_build_array(r.account_id,r.id,r.stream_id,j.id,j.lease_token,n.id,r.stream_url,s.source_url,s.source_page_url,s.provider,s.external_id,s.source_family,s.capture_type,s.execution_class,COALESCE((SELECT max(id) FROM stream_source_revisions WHERE stream_id=s.id),0),'source_copy','first_optional')::text,'UTF8')),'hex'),s.provider,s.external_id,s.source_family,s.capture_type,s.execution_class,'source_copy','first_optional','continuous-source-presentation-edge-v2','presentation-probe-v2',recording_presentation_v2_tool_identity('8.1','8.1','62','62','60',repeat('b',64),'mov','h264','aac','presentation-probe-v2'),now()+interval '14 minutes' FROM recordings r JOIN streams s ON s.id=r.stream_id JOIN recording_jobs j ON j.recording_id=r.id JOIN nodes n ON n.id=$2 WHERE r.id=$3 AND j.id=$4`, admission, target.nodeID, target.recordingID, jobID)
		return insertErr
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = insertAdmission(tx, f, raceAdmission, raceJob, raceLease); err != nil {
		t.Fatalf("race admission insert: %v", err)
	}
	protectPID, protectResult := make(chan int32, 1), make(chan error, 1)
	go func() {
		blockedTx, beginErr := pool.Begin(ctx)
		if beginErr != nil {
			protectResult <- beginErr
			return
		}
		defer blockedTx.Rollback(ctx)
		var pid int32
		if beginErr = blockedTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
			protectResult <- beginErr
			return
		}
		protectPID <- pid
		_, protectErr := blockedTx.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['protect'],$2,now())`, track, actor)
		if protectErr == nil {
			protectErr = blockedTx.Commit(ctx)
		}
		protectResult <- protectErr
	}()
	waitForPostgresLock(t, pool, <-protectPID)
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-protectResult; err != nil {
		t.Fatalf("protection transition after actual admission commit: %v", err)
	}
	var admissionCount int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_presentation_v2_admissions WHERE id=$1`, raceAdmission).Scan(&admissionCount); err != nil || admissionCount != 1 {
		t.Fatalf("admission-first race count=%d err=%v", admissionCount, err)
	}
	if _, err = pool.Exec(ctx, `
		WITH fresh AS (SELECT gen_random_uuid() id)
		INSERT INTO recording_presentation_v2_attempts(id,admission_id,account_id,recording_id,stream_id,recording_job_id,lease_token,node_id,idempotency_key,ffmpeg_version,ffprobe_version,libavformat_version,libavcodec_version,libavutil_version,build_flags_sha256,demuxer_name,video_decoder_name,audio_decoder_name,parser_schema,request_sha256,response_sha256)
		SELECT fresh.id,admission_id,account_id,recording_id,stream_id,recording_job_id,lease_token,node_id,'protected-late','8.1','8.1','62','62','60',repeat('b',64),'mov','h264','aac','presentation-probe-v2',repeat('d',64),encode(sha256(convert_to('attempt:'||fresh.id::text,'UTF8')),'hex')
		FROM recording_presentation_v2_attempts CROSS JOIN fresh WHERE admission_id=$1 LIMIT 1`, f.admissionID); err == nil {
		t.Fatal("attempt admitted after recording became campaign protected")
	}
	if _, err = pool.Exec(ctx, `
		WITH fresh AS (SELECT gen_random_uuid() id)
		INSERT INTO recording_presentation_v2_probe_tasks(id,admission_id,attempt_id,account_id,recording_id,stream_id,recording_job_id,clip_id,upload_intent_id,lease_token,node_id,capture_sequence,clip_size_bytes,clip_sha256,local_upload_identity_sha256,staging_identity_sha256,staging_method,staging_device_id,staging_inode_id,staging_clone_identity_sha256,request_sha256,response_sha256,initial_disposition,state,retention_state,unavailable_reason,retention_identity_sha256,retention_method,retention_device_id,retention_inode_id,retention_clone_identity_sha256,revision,attempt_count,claim_token,terminal_claim_token,lease_expires_at,next_attempt_at,absolute_deadline_at,created_at,updated_at)
		SELECT fresh.id,admission_id,attempt_id,account_id,recording_id,stream_id,recording_job_id,clip_id,upload_intent_id,lease_token,node_id,capture_sequence,clip_size_bytes,clip_sha256,local_upload_identity_sha256,staging_identity_sha256,staging_method,staging_device_id,staging_inode_id,staging_clone_identity_sha256,request_sha256,encode(sha256(convert_to('task:'||fresh.id::text||':'||clip_id::text||':'||state,'UTF8')),'hex'),initial_disposition,state,retention_state,unavailable_reason,retention_identity_sha256,retention_method,retention_device_id,retention_inode_id,retention_clone_identity_sha256,revision,attempt_count,claim_token,terminal_claim_token,lease_expires_at,next_attempt_at,absolute_deadline_at,created_at,updated_at
		FROM recording_presentation_v2_probe_tasks CROSS JOIN fresh WHERE recording_presentation_v2_probe_tasks.id=$1`, f.taskID); err == nil {
		t.Fatal("ingest task admitted after recording became campaign protected")
	}

	// Opposite commit order: protection becomes authoritative first, so an
	// actual later admission insert must fail closed.
	protectedFirst := seedPresentationV2Task(t, pool, 12, 47112)
	var protectedTrack int64
	if err = pool.QueryRow(ctx, `INSERT INTO recording_campaign_tracks(account_id,campaign_key,label,deadline_at,target_count,grade_floor,required_consecutive_windows,created_by_user_id) VALUES($1,$2,'presentation fence protected first',now()+interval '1 day',1,'GOOD',0,$3) RETURNING id`, protectedFirst.accountID, fmt.Sprintf("presentation-fence-first-%d", time.Now().UnixNano()), actor).Scan(&protectedTrack); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_campaign_roster_entries(track_id,recording_id,stream_id,scene_identity_sha256,role,rank,status,reason_codes,effective_at,decision_at,evidence_observed_at,evidence_sha256,updated_by_user_id) VALUES($1,$2,$3,repeat('c',64),'primary',1,'protect',ARRAY['test'],now(),now(),now(),repeat('d',64),$4)`, protectedTrack, protectedFirst.recordingID, protectedFirst.streamID, actor); err != nil {
		t.Fatal(err)
	}
	protectedJob := protectedFirst.jobID + 1_000_000
	protectedLease := uuid.New()
	if _, err = pool.Exec(ctx, `INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,lease_owner,lease_expires_at,lease_token,idempotency_key,kind,window_end_at) VALUES($1,$2,now(),now(),60,'leased',$3,now()+interval '1 hour',$4,$5,'continuous_window',now()+interval '1 hour')`, protectedJob, protectedFirst.recordingID, fmt.Sprintf("node:%d", protectedFirst.nodeID), protectedLease, fmt.Sprintf("protected-first-%d", time.Now().UnixNano())); err != nil {
		t.Fatal(err)
	}
	protectTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer protectTx.Rollback(ctx)
	if _, err = protectTx.Exec(ctx, `SELECT transition_recording_campaign_track($1,'active',ARRAY['protect'],$2,now())`, protectedTrack, actor); err != nil {
		t.Fatal(err)
	}
	latePID, lateResult := make(chan int32, 1), make(chan error, 1)
	go func() {
		lateTx, beginErr := pool.Begin(ctx)
		if beginErr != nil {
			lateResult <- beginErr
			return
		}
		defer lateTx.Rollback(ctx)
		var pid int32
		if beginErr = lateTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); beginErr != nil {
			lateResult <- beginErr
			return
		}
		latePID <- pid
		insertErr := insertAdmission(lateTx, protectedFirst, uuid.New(), protectedJob, protectedLease)
		if insertErr == nil {
			insertErr = lateTx.Commit(ctx)
		}
		lateResult <- insertErr
	}()
	waitForPostgresLock(t, pool, <-latePID)
	if err = protectTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-lateResult; err == nil {
		t.Fatal("admission committed after campaign protection won race")
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_presentation_v2_admissions WHERE recording_job_id=$1`, protectedJob).Scan(&admissionCount); err != nil || admissionCount != 0 {
		t.Fatalf("protection-first race admission count=%d err=%v", admissionCount, err)
	}
}

func waitForPostgresLock(t *testing.T, pool *pgxpool.Pool, pid int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waitType *string
		err := pool.QueryRow(ctx, `SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&waitType)
		if err == nil && waitType != nil && *waitType == "Lock" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("backend %d never blocked on shared protection lock: %v", pid, ctx.Err())
		case <-ticker.C:
		}
	}
}

func presentationTaskRequest(task uuid.UUID, body any, principal nodePrincipal) *http.Request {
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest(http.MethodPost, "/api/v1/recording/presentation-probes/"+task.String(), bytes.NewReader(raw))
	rc := chi.NewRouteContext()
	rc.URLParams.Add("taskId", task.String())
	ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rc)
	ctx = context.WithValue(ctx, nodePrincipalContextKey, principal)
	return r.WithContext(ctx)
}

func presentationAttemptRequest(f presentationV2Fixture, body any) *http.Request {
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/recording/jobs/%d/presentation-attempts", f.jobID), bytes.NewReader(raw))
	r.Header.Set(recordingLeaseTokenHeader, f.leaseToken.String())
	rc := chi.NewRouteContext()
	rc.URLParams.Add("id", fmt.Sprint(f.jobID))
	ctx := context.WithValue(r.Context(), chi.RouteCtxKey, rc)
	ctx = context.WithValue(ctx, nodePrincipalContextKey, nodePrincipal{NodeID: f.nodeID, AccountID: f.accountID, NodeType: nodeTypeRelay})
	return r.WithContext(ctx)
}

func TestRecordingPresentationV2DisabledClaimAndFencedLifecyclePostgres(t *testing.T) {
	pool, cleanup := testPresentationV2Pool(t)
	defer cleanup()
	first := seedPresentationV2Task(t, pool, 1, 47001)
	second := seedPresentationV2Task(t, pool, 2, 47001)
	third := seedPresentationV2Task(t, pool, 3, 47001)
	s := &Server{pool: pool}
	principal := nodePrincipal{NodeID: first.nodeID, AccountID: first.accountID, NodeType: nodeTypeRelay}
	activationBody := func(f presentationV2Fixture) map[string]any {
		var deadline time.Time
		if err := pool.QueryRow(context.Background(), `SELECT absolute_deadline_at FROM recording_presentation_v2_probe_tasks WHERE id=$1`, f.taskID).Scan(&deadline); err != nil {
			t.Fatal(err)
		}
		inode := fmt.Sprint(100000 + f.nodeID - 8000)
		identity := presentationV2RetentionIdentity(presentationV2RetentionIdentityInput{TaskID: f.taskID, NodeID: f.nodeID, Method: "hardlink", DeviceID: "42", InodeID: inode, SizeBytes: 1024, FileSHA256: strings.Repeat("e", 64), Deadline: deadline})
		var databaseIdentity string
		if err := pool.QueryRow(context.Background(), `SELECT recording_presentation_v2_retention_identity($1,$2,'hardlink','42',$3,'',1024,repeat('e',64),$4)`, f.taskID, f.nodeID, inode, deadline).Scan(&databaseIdentity); err != nil || databaseIdentity != identity {
			t.Fatalf("retention identity parity application=%s database=%s err=%v", identity, databaseIdentity, err)
		}
		return map[string]any{
			"expected_revision": 1, "staging_identity_sha256": strings.Repeat("1", 64),
			"retention_identity_sha256": identity, "method": "hardlink", "device_id": "42",
			"inode_id": inode, "file_size_bytes": 1024, "file_sha256": strings.Repeat("e", 64),
		}
	}
	activate := func(f presentationV2Fixture) *httptest.ResponseRecorder {
		out := httptest.NewRecorder()
		s.handleRecordingPresentationV2Activate(out, presentationTaskRequest(f.taskID, activationBody(f), nodePrincipal{NodeID: f.nodeID, AccountID: f.accountID, NodeType: nodeTypeRelay}))
		return out
	}
	if out := activate(first); out.Code != http.StatusOK {
		t.Fatalf("activate first status=%d body=%s", out.Code, out.Body.String())
	}
	if out := activate(first); out.Code != http.StatusOK || !strings.Contains(out.Body.String(), `"replayed":true`) {
		t.Fatalf("activate replay status=%d body=%s", out.Code, out.Body.String())
	}
	crossTask := activationBody(second)
	crossTask["retention_identity_sha256"] = activationBody(first)["retention_identity_sha256"]
	out := httptest.NewRecorder()
	s.handleRecordingPresentationV2Activate(out, presentationTaskRequest(second.taskID, crossTask, nodePrincipal{NodeID: second.nodeID, AccountID: second.accountID, NodeType: nodeTypeRelay}))
	if out.Code != http.StatusConflict {
		t.Fatalf("cross-task retention identity status=%d body=%s", out.Code, out.Body.String())
	}
	forgedInode := activationBody(second)
	forgedInode["inode_id"] = "999999"
	out = httptest.NewRecorder()
	s.handleRecordingPresentationV2Activate(out, presentationTaskRequest(second.taskID, forgedInode, nodePrincipal{NodeID: second.nodeID, AccountID: second.accountID, NodeType: nodeTypeRelay}))
	if out.Code != http.StatusConflict {
		t.Fatalf("substituted retained inode status=%d body=%s", out.Code, out.Body.String())
	}
	forgedBytes := activationBody(second)
	forgedBytes["file_sha256"] = strings.Repeat("d", 64)
	out = httptest.NewRecorder()
	s.handleRecordingPresentationV2Activate(out, presentationTaskRequest(second.taskID, forgedBytes, nodePrincipal{NodeID: second.nodeID, AccountID: second.accountID, NodeType: nodeTypeRelay}))
	if out.Code != http.StatusConflict {
		t.Fatalf("substituted retained bytes status=%d body=%s", out.Code, out.Body.String())
	}
	if _, err := pool.Exec(context.Background(), `UPDATE recording_presentation_v2_probe_tasks SET state='pending',retention_state='active',retention_method='hardlink',retention_device_id=staging_device_id,retention_inode_id=staging_inode_id,retention_identity_sha256=repeat('f',64),revision=revision+1 WHERE id=$1`, second.taskID); err == nil {
		t.Fatal("direct SQL forged retention identity was accepted")
	}
	if out := activate(second); out.Code != http.StatusOK {
		t.Fatalf("activate second status=%d body=%s", out.Code, out.Body.String())
	}
	if out := activate(third); out.Code != http.StatusOK {
		t.Fatalf("activate third status=%d body=%s", out.Code, out.Body.String())
	}
	attemptBody := presentationV2AttemptRequest{
		AdmissionID: first.admissionID.String(), IdempotencyKey: "ambiguous-attempt",
		FFmpegVersion: "8.1", FFprobeVersion: "8.1", Libavformat: "62", Libavcodec: "62", Libavutil: "60",
		BuildFlagsSHA256: strings.Repeat("b", 64), Demuxer: "mov", VideoDecoder: "h264", AudioDecoder: "aac", ParserSchema: "presentation-probe-v2",
	}
	var databaseToolIdentity string
	if err := pool.QueryRow(context.Background(), `SELECT recording_presentation_v2_tool_identity($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		attemptBody.FFmpegVersion, attemptBody.FFprobeVersion, attemptBody.Libavformat, attemptBody.Libavcodec,
		attemptBody.Libavutil, attemptBody.BuildFlagsSHA256, attemptBody.Demuxer, attemptBody.VideoDecoder,
		attemptBody.AudioDecoder, attemptBody.ParserSchema).Scan(&databaseToolIdentity); err != nil {
		t.Fatal(err)
	}
	if applicationToolIdentity := presentationV2ToolIdentity(attemptBody); applicationToolIdentity != databaseToolIdentity {
		t.Fatalf("semantic tool identity differs application=%s database=%s", applicationToolIdentity, databaseToolIdentity)
	}
	unicodeTool := attemptBody
	unicodeTool.FFmpegVersion = "FFmpeg 8.1 猫\n"
	unicodeTool.FFprobeVersion = "<8.1>&"
	if err := pool.QueryRow(context.Background(), `SELECT recording_presentation_v2_tool_identity($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		unicodeTool.FFmpegVersion, unicodeTool.FFprobeVersion, unicodeTool.Libavformat, unicodeTool.Libavcodec,
		unicodeTool.Libavutil, unicodeTool.BuildFlagsSHA256, unicodeTool.Demuxer, unicodeTool.VideoDecoder,
		unicodeTool.AudioDecoder, unicodeTool.ParserSchema).Scan(&databaseToolIdentity); err != nil {
		t.Fatal(err)
	}
	if applicationToolIdentity := presentationV2ToolIdentity(unicodeTool); applicationToolIdentity != databaseToolIdentity {
		t.Fatalf("unicode semantic tool identity differs application=%s database=%s", applicationToolIdentity, databaseToolIdentity)
	}
	rec := httptest.NewRecorder()
	s.handleRecordingPresentationV2Attempt(rec, presentationAttemptRequest(first, attemptBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("attempt status=%d body=%s", rec.Code, rec.Body.String())
	}
	var firstAttempt map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &firstAttempt); err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Attempt(rec, presentationAttemptRequest(first, attemptBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("attempt replay status=%d body=%s", rec.Code, rec.Body.String())
	}
	var replayAttempt map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &replayAttempt); err != nil || replayAttempt["attempt_id"] != firstAttempt["attempt_id"] {
		t.Fatalf("attempt replay differs first=%v replay=%v err=%v", firstAttempt, replayAttempt, err)
	}
	attemptBody.FFmpegVersion = "different"
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Attempt(rec, presentationAttemptRequest(first, attemptBody))
	if rec.Code != http.StatusConflict {
		t.Fatalf("different attempt replay status=%d body=%s", rec.Code, rec.Body.String())
	}
	attemptBody.IdempotencyKey = "different-tool-new-key"
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Attempt(rec, presentationAttemptRequest(first, attemptBody))
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "semantic tool identity") {
		t.Fatalf("unadmitted semantic tool status=%d body=%s", rec.Code, rec.Body.String())
	}
	forgedAttempt := uuid.New()
	if _, err := pool.Exec(context.Background(), `INSERT INTO recording_presentation_v2_attempts(id,admission_id,account_id,recording_id,stream_id,recording_job_id,lease_token,node_id,idempotency_key,ffmpeg_version,ffprobe_version,libavformat_version,libavcodec_version,libavutil_version,build_flags_sha256,demuxer_name,video_decoder_name,audio_decoder_name,parser_schema,request_sha256,response_sha256) SELECT $1::uuid,admission_id,account_id,recording_id,stream_id,recording_job_id,lease_token,node_id,'direct-forged-tool','forged',ffprobe_version,libavformat_version,libavcodec_version,libavutil_version,build_flags_sha256,demuxer_name,video_decoder_name,audio_decoder_name,parser_schema,repeat('a',64),encode(sha256(convert_to('attempt:'||$1::uuid::text,'UTF8')),'hex') FROM recording_presentation_v2_attempts WHERE admission_id=$2 LIMIT 1`, forgedAttempt, first.admissionID); err == nil {
		t.Fatal("direct SQL forged semantic tool identity was accepted")
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/presentation-probes/claim", nil)
	req = req.WithContext(context.WithValue(req.Context(), nodePrincipalContextKey, principal))
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Claim(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"enabled":false`) || !strings.Contains(rec.Body.String(), `"task":null`) {
		t.Fatalf("disabled claim status=%d body=%s", rec.Code, rec.Body.String())
	}
	var state string
	var attemptCount int
	if err := pool.QueryRow(context.Background(), `SELECT state,attempt_count FROM recording_presentation_v2_probe_tasks WHERE id=$1`, first.taskID).Scan(&state, &attemptCount); err != nil || state != "pending" || attemptCount != 0 {
		t.Fatalf("disabled claim mutated state=%q attempts=%d err=%v", state, attemptCount, err)
	}
	claimed, err := s.claimRecordingPresentationV2(context.Background(), principal)
	if err != nil || claimed == nil || claimed.ID != first.taskID {
		t.Fatalf("internal fenced claim=%+v err=%v", claimed, err)
	}
	wrong := nodePrincipal{NodeID: second.nodeID, AccountID: second.accountID + 1, NodeType: nodeTypeRelay}
	if got, err := s.claimRecordingPresentationV2(context.Background(), wrong); err != nil || got != nil {
		t.Fatalf("foreign claim=%+v err=%v", got, err)
	}

	axes := make([]presentationV2Axis, 0, 6)
	for _, axis := range []string{"demux_video", "raw_video", "video_presentation", "demux_audio", "raw_audio", "audio_sample"} {
		axes = append(axes, presentationV2Axis{Axis: axis, Status: "unknown", Reason: "probe_unavailable"})
	}
	body := presentationV2Completion{ClaimToken: claimed.ClaimToken.String(), RequestSHA256: claimed.RequestSHA256, AuthoredStatus: "unknown", TerminalReason: "all_axes_unknown", Axes: axes}
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Complete(rec, presentationTaskRequest(first.taskID, body, principal))
	if rec.Code != http.StatusOK {
		t.Fatalf("unknown fact completion status=%d body=%s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Complete(rec, presentationTaskRequest(first.taskID, body, principal))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"replayed":true`) {
		t.Fatalf("completion replay status=%d body=%s", rec.Code, rec.Body.String())
	}
	body.TerminalReason = "some_axes_unknown"
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Complete(rec, presentationTaskRequest(first.taskID, body, principal))
	if rec.Code != http.StatusConflict {
		t.Fatalf("differing completion replay status=%d body=%s", rec.Code, rec.Body.String())
	}
	var binding string
	if err := pool.QueryRow(context.Background(), `SELECT binding_sha256 FROM recording_presentation_v2_release_authorizations WHERE task_id=$1`, first.taskID).Scan(&binding); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO recording_presentation_v2_release_acknowledgements(task_id,release_version,node_id,binding_sha256) VALUES($1,1,$2,$3)`, first.taskID, first.nodeID, binding); err == nil {
		t.Fatal("direct release acknowledgement without released state committed")
	}
	ackStart := make(chan struct{})
	ackResults := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() {
			<-ackStart
			out := httptest.NewRecorder()
			s.handleRecordingPresentationV2ReleaseAck(out, presentationTaskRequest(first.taskID, map[string]any{"release_version": 1, "binding_sha256": binding}, principal))
			ackResults <- out
		}()
	}
	close(ackStart)
	for range 2 {
		out := <-ackResults
		if out.Code != http.StatusOK {
			t.Fatalf("concurrent release ack status=%d body=%s", out.Code, out.Body.String())
		}
	}
	if _, err := pool.Exec(context.Background(), `UPDATE recording_presentation_v2_authored_facts SET authored_status='partial' WHERE task_id=$1`, first.taskID); err == nil {
		t.Fatal("append-only fact update succeeded")
	}
	secondPrincipal := nodePrincipal{NodeID: second.nodeID, AccountID: second.accountID, NodeType: nodeTypeRelay}
	secondClaim, err := s.claimRecordingPresentationV2(context.Background(), secondPrincipal)
	if err != nil || secondClaim == nil || secondClaim.ID != second.taskID {
		t.Fatalf("second internal claim=%+v err=%v", secondClaim, err)
	}
	forgedAxes := append([]presentationV2Axis(nil), axes...)
	forgedAxes[0].PacketEdges = []presentationV2PacketEdge{{Side: "leading", Rank: 1, Ordinal: 0, Duration: 1, TimeBaseNum: 1, TimeBaseDen: 1000, SideDataSHA256: strings.Repeat("5", 64), PayloadSHA256: strings.Repeat("6", 64)}}
	forged := presentationV2Completion{ClaimToken: secondClaim.ClaimToken.String(), RequestSHA256: secondClaim.RequestSHA256, AuthoredStatus: "unknown", TerminalReason: "all_axes_unknown", Axes: forgedAxes}
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Complete(rec, presentationTaskRequest(second.taskID, forged, secondPrincipal))
	if rec.Code != http.StatusConflict {
		t.Fatalf("unknown axis with authoritative child status=%d body=%s", rec.Code, rec.Body.String())
	}
	forged.Axes = axes
	forged.AuthoredStatus = "partial"
	forged.TerminalReason = "some_axes_unknown"
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Complete(rec, presentationTaskRequest(second.taskID, forged, secondPrincipal))
	if rec.Code != http.StatusConflict {
		t.Fatalf("forged aggregate status=%d body=%s", rec.Code, rec.Body.String())
	}
	var facts int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM recording_presentation_v2_authored_facts WHERE task_id=$1`, second.taskID).Scan(&facts); err != nil || facts != 0 {
		t.Fatalf("forged fact rollback count=%d err=%v", facts, err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT state FROM recording_presentation_v2_probe_tasks WHERE id=$1`, second.taskID).Scan(&state); err != nil || state != "leased" {
		t.Fatalf("forged completion task state=%q err=%v", state, err)
	}
	thirdPrincipal := nodePrincipal{NodeID: third.nodeID, AccountID: third.accountID, NodeType: nodeTypeRelay}
	thirdClaim, err := s.claimRecordingPresentationV2(context.Background(), thirdPrincipal)
	if err != nil || thirdClaim == nil || thirdClaim.ID != third.taskID {
		t.Fatalf("third internal claim=%+v err=%v", thirdClaim, err)
	}
	zero := int64(0)
	one := int64(1)
	forty := int64(40)
	stream := 0
	zeroInt := int64(0)
	completeBase := presentationV2Axis{
		Status: "complete", StreamIndex: &stream, UnitCount: &one, CanonicalSHA256: strings.Repeat("7", 64),
		TimeBaseNum: &one, TimeBaseDen: func() *int64 { v := int64(1000); return &v }(), FirstOrdinal: &zero,
		FirstTimestamp: &zero, EndOrdinal: &zero, EndTimestamp: &forty, NonmonotonicCount: &zeroInt,
		DuplicateCount: &zeroInt, HoleCount: &zeroInt, OverlapCount: &zeroInt,
	}
	demuxVideo := completeBase
	demuxVideo.Axis = "demux_video"
	demuxVideo.PacketEdges = []presentationV2PacketEdge{
		{Side: "leading", Rank: 1, Ordinal: 0, PTS: 0, DTS: 0, Duration: 40, TimeBaseNum: 1, TimeBaseDen: 1000, SideDataSHA256: strings.Repeat("8", 64), PayloadSHA256: strings.Repeat("9", 64)},
		{Side: "trailing", Rank: 1, Ordinal: 0, PTS: 0, DTS: 0, Duration: 40, TimeBaseNum: 1, TimeBaseDen: 1000, SideDataSHA256: strings.Repeat("8", 64), PayloadSHA256: strings.Repeat("9", 64)},
	}
	rawVideo := completeBase
	rawVideo.Axis = "raw_video"
	rawVideo.TimeBaseNum, rawVideo.TimeBaseDen = nil, nil
	rawVideo.FirstTimestamp, rawVideo.EndTimestamp = nil, nil
	rawVideo.RawExtents = []presentationV2RawExtent{
		{Side: "leading", Rank: 1, Ordinal: 0, Position: 0, SizeBytes: 100, SHA256: strings.Repeat("a", 64)},
		{Side: "trailing", Rank: 1, Ordinal: 0, Position: 0, SizeBytes: 100, SHA256: strings.Repeat("a", 64)},
	}
	video := completeBase
	video.Axis = "video_presentation"
	video.NormalizationProfile = "continuous-rational-presentation-v2.0"
	video.EditListSHA256 = strings.Repeat("b", 64)
	video.EditListKind = "none"
	video.VideoFrames = []presentationV2VideoEdge{
		{Side: "leading", Rank: 1, Ordinal: 0, PTS: 0, Duration: 40, TimeBaseNum: 1, TimeBaseDen: 1000},
		{Side: "trailing", Rank: 1, Ordinal: 0, PTS: 0, Duration: 40, TimeBaseNum: 1, TimeBaseDen: 1000},
	}
	completeAxes := []presentationV2Axis{demuxVideo, rawVideo, video}
	for _, axis := range []string{"demux_audio", "raw_audio", "audio_sample"} {
		completeAxes = append(completeAxes, presentationV2Axis{Axis: axis, Status: "not_present", Reason: "audio_not_present"})
	}
	completeBody := presentationV2Completion{ClaimToken: thirdClaim.ClaimToken.String(), RequestSHA256: thirdClaim.RequestSHA256, AuthoredStatus: "complete", TerminalReason: "all_axes_complete", Axes: completeAxes}
	rec = httptest.NewRecorder()
	s.handleRecordingPresentationV2Complete(rec, presentationTaskRequest(third.taskID, completeBody, thirdPrincipal))
	if rec.Code != http.StatusOK {
		t.Fatalf("typed complete fact status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRecordingPresentationV2ClaimSkipsStaleBacklogAndFencesConcurrentCallersPostgres(t *testing.T) {
	pool, cleanup := testPresentationV2Pool(t)
	defer cleanup()
	f := seedPresentationV2Task(t, pool, 31, 47331)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `UPDATE recording_presentation_v2_probe_tasks SET state='pending',retention_state='active',retention_method=staging_method,retention_device_id=staging_device_id,retention_inode_id=staging_inode_id,retention_identity_sha256=recording_presentation_v2_retention_identity(id,node_id,staging_method,staging_device_id,staging_inode_id,'',clip_size_bytes,clip_sha256,absolute_deadline_at),revision=revision+1 WHERE id=$1`, f.taskID); err != nil {
		t.Fatal(err)
	}
	for sequence := int64(2); sequence <= 11; sequence++ {
		addPresentationV2Task(t, pool, f, sequence, "expired")
	}
	for sequence := int64(12); sequence <= 21; sequence++ {
		addPresentationV2Task(t, pool, f, sequence, "exhausted")
	}
	s := &Server{pool: pool}
	principal := nodePrincipal{NodeID: f.nodeID, AccountID: f.accountID, NodeType: nodeTypeRelay}
	claimed, err := s.claimRecordingPresentationV2(ctx, principal)
	if err != nil || claimed == nil || claimed.ID != f.taskID {
		t.Fatalf("eligible task starved behind stale backlog: claimed=%+v err=%v", claimed, err)
	}
	var expiredReleases, exhaustedReleases int
	if err = pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE terminal_state='expired'),count(*) FILTER(WHERE terminal_state='unavailable') FROM recording_presentation_v2_release_authorizations WHERE node_id=$1`, f.nodeID).Scan(&expiredReleases, &exhaustedReleases); err != nil || expiredReleases != 10 || exhaustedReleases != 10 {
		t.Fatalf("bounded stale cleanup expired=%d exhausted=%d err=%v", expiredReleases, exhaustedReleases, err)
	}

	concurrentA := addPresentationV2Task(t, pool, f, 22, "ready")
	concurrentB := addPresentationV2Task(t, pool, f, 23, "ready")
	type result struct {
		task *presentationV2ClaimedTask
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			task, claimErr := s.claimRecordingPresentationV2(ctx, principal)
			results <- result{task: task, err: claimErr}
		}()
	}
	close(start)
	got := map[uuid.UUID]bool{}
	for range 2 {
		result := <-results
		if result.err != nil || result.task == nil {
			t.Fatalf("concurrent claim result=%+v err=%v", result.task, result.err)
		}
		got[result.task.ID] = true
	}
	if !got[concurrentA] || !got[concurrentB] || len(got) != 2 {
		t.Fatalf("concurrent claim ids=%v want %s,%s", got, concurrentA, concurrentB)
	}
}
