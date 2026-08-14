package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/dropletpool"
)

func TestRecordingSurrenderTransportPostgresLifecycleAndExpiryRecovery(t *testing.T) {
	pool, cleanup := testPresentationV2Pool(t)
	defer cleanup()
	f := seedPresentationV2Task(t, pool, 91001, 991001)
	ctx := context.Background()
	lease := uuid.New()
	if _, err := pool.Exec(ctx, `
		UPDATE nodes SET node_type='local_recorder',display_name='cloud-recovery' WHERE id=$1;
		INSERT INTO recorder_droplets(name,node_id,state,capacity,last_seen_at) VALUES('cloud-recovery',$1,'active',1,transaction_timestamp());
		UPDATE recordings SET capture_via='cloud' WHERE id=$4;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL WHERE id=$2;
		UPDATE recording_jobs SET status='leased',lease_owner='cloud-recovery',lease_expires_at=transaction_timestamp()+interval '3 minutes',lease_token=$3,attempt_count=attempt_count+1 WHERE id=$2;
	`, f.nodeID, f.jobID, lease, f.recordingID); err != nil {
		t.Fatal(err)
	}
	// Equal bytes are not an identity in v1. Two distinct upload-intent/sequence
	// units under the same lease generation must coexist; sequence remains the
	// structural concurrency fence.
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,audio_present,fire_at,clip_start_at,clip_end_at,capture_lease_token,capture_sequence)
		SELECT $1,$2,$3,sd.endpoint,sd.bucket,'same-sha-'||g::text||'.mp4','same-sha-'||g::text||'.mp4','video/mp4','mp4',1024,'etag',repeat('a',64),60000,'h264',false,transaction_timestamp(),transaction_timestamp()+g*interval '1 minute',transaction_timestamp()+(g+1)*interval '1 minute',$4,g
		FROM storage_destinations sd CROSS JOIN generate_series(1,2) g WHERE sd.id=$3
	`, f.recordingID, f.jobID, f.destinationID, lease); err != nil {
		t.Fatalf("distinct equal-SHA clips: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,audio_present,fire_at,clip_start_at,clip_end_at,capture_lease_token,capture_sequence)
		SELECT $1,$2,$3,endpoint,bucket,'forged-sequence.mp4','forged-sequence.mp4','video/mp4','mp4',1024,'etag',repeat('b',64),60000,'h264',false,transaction_timestamp(),transaction_timestamp(),transaction_timestamp()+interval '1 minute',$4,2 FROM storage_destinations WHERE id=$3
	`, f.recordingID, f.jobID, f.destinationID, lease); err == nil {
		t.Fatal("duplicate capture sequence committed")
	}
	producer := uuid.New()
	recoverySecret := strings.Repeat("1a", 32)
	recoverySecretBytes, err := hex.DecodeString(recoverySecret)
	if err != nil {
		t.Fatal(err)
	}
	secretDigest := sha256.Sum256(recoverySecretBytes)
	secretSHA := hex.EncodeToString(secretDigest[:])
	var recordingStreamID int64
	var recordingStreamURL, sourceURL, sourcePageURL, provider, externalID string
	if err = pool.QueryRow(ctx, `
		SELECT COALESCE(recording.stream_id,0),recording.stream_url,
		       COALESCE(stream.source_url,''),COALESCE(stream.source_page_url,''),
		       COALESCE(stream.provider,''),COALESCE(stream.external_id,'')
		FROM recordings recording
		LEFT JOIN streams stream ON stream.id=recording.stream_id
		WHERE recording.id=$1
	`, f.recordingID).Scan(&recordingStreamID, &recordingStreamURL, &sourceURL, &sourcePageURL, &provider, &externalID); err != nil {
		t.Fatal(err)
	}
	sourceSnapshot := producerSourceSnapshot(
		strconv.FormatInt(f.recordingID, 10), strconv.FormatInt(recordingStreamID, 10),
		recordingStreamURL, sourceURL, sourcePageURL, provider, externalID,
	)
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_capture_producers(id,recording_job_id,lease_token,capture_ordinal,worker_id,node_id,sealed_intent_limit,recovery_secret_sha256,source_snapshot_sha256)
		VALUES($1,$2,$3,1,'cloud-recovery',$4,2,$5,$6)
	`, producer, f.jobID, lease, f.nodeID, secretSHA, sourceSnapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recording_capture_producers(id,recording_job_id,lease_token,capture_ordinal,worker_id,node_id,sealed_intent_limit,recovery_secret_sha256,source_snapshot_sha256)
		VALUES($1,$2,$3,1,'cloud-recovery',$4,2,$5,$6)
		ON CONFLICT(recording_job_id,lease_token,capture_ordinal) DO NOTHING
	`, producer, f.jobID, lease, f.nodeID, secretSHA, sourceSnapshot); err != nil {
		t.Fatalf("exact producer reservation replay: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_capture_producers(id,recording_job_id,lease_token,capture_ordinal,worker_id,node_id,sealed_intent_limit,recovery_secret_sha256,source_snapshot_sha256) VALUES($1,$2,$3,1,'cloud-recovery',$4,2,$5,$6)`, uuid.New(), f.jobID, lease, f.nodeID, strings.Repeat("c", 64), sourceSnapshot); err == nil {
		t.Fatal("duplicate producer ordinal committed")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_job_recovery_grants(id,recording_job_id,lease_token,producer_id,recovery_secret_sha256,upload_grace_until) VALUES($1,$2,$3,$4,$5,transaction_timestamp()+interval '30 minutes')`, uuid.New(), f.jobID, lease, producer, secretSHA); err == nil {
		t.Fatal("upload-only recovery capability was granted before the main lease expired")
	}
	intentOne, intentTwo, intentThree := uuid.New(), uuid.New(), uuid.New()
	for index, intentID := range []uuid.UUID{intentOne, intentTwo, intentThree} {
		name := "recovery-seal-" + strconv.Itoa(index+1) + ".mp4"
		if index == 0 {
			name = "same-sha-1.mp4"
		}
		if _, err = pool.Exec(ctx, `INSERT INTO recording_upload_intents(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,max_size_bytes,status,expires_at) SELECT $1,$2,$3,id,endpoint,bucket,$4,$4,'video/mp4',4096,'pending',transaction_timestamp()+interval '1 hour' FROM storage_destinations WHERE id=$5`, intentID, f.recordingID, f.jobID, name, f.destinationID); err != nil {
			t.Fatal(err)
		}
	}
	var firstSegmentStartMs int64
	if err = pool.QueryRow(ctx, `SELECT (extract(epoch FROM clip_start_at)*1000)::bigint FROM recording_clips WHERE recording_job_id=$1 AND capture_lease_token=$2 AND capture_sequence=1`, f.jobID, lease).Scan(&firstSegmentStartMs); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `
		INSERT INTO recording_capture_artifact_seals(upload_intent_id,producer_id,capture_sequence,segment_start_ms,size_bytes,sha256)
		VALUES($1,$3,1,$5,1024,$4),($2,$3,2,$6,1024,$4)
	`, intentOne, intentTwo, producer, strings.Repeat("a", 64), firstSegmentStartMs, firstSegmentStartMs+60000); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_capture_artifact_seals(upload_intent_id,producer_id,capture_sequence,segment_start_ms,size_bytes,sha256) VALUES($1,$2,1,$4,1024,$3) ON CONFLICT(upload_intent_id) DO NOTHING`, intentOne, producer, strings.Repeat("a", 64), firstSegmentStartMs); err != nil {
		t.Fatalf("exact artifact seal replay: %v", err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_capture_artifact_seals(upload_intent_id,producer_id,capture_sequence,segment_start_ms,size_bytes,sha256) VALUES($1,$2,3,3000,1024,$3)`, intentThree, producer, strings.Repeat("c", 64)); err == nil {
		t.Fatal("producer sealed-intent hard bound was bypassed")
	}
	// A restarted worker that still owns the exact live lease generation can
	// recover its pre-reserved producer before expiry creates an upload grant.
	currentStatusRequest := httptest.NewRequest(http.MethodPost, "/api/v1/recording/recovery/producers/"+producer.String()+"/status", nil)
	currentStatusRoute := chi.NewRouteContext()
	currentStatusRoute.URLParams.Add("producerId", producer.String())
	currentStatusContext := context.WithValue(currentStatusRequest.Context(), chi.RouteCtxKey, currentStatusRoute)
	currentStatusContext = context.WithValue(currentStatusContext, nodePrincipalContextKey, nodePrincipal{NodeID: f.nodeID, AccountID: f.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: "cloud-recovery"})
	currentStatusResponse := httptest.NewRecorder()
	(&Server{pool: pool}).handleRecordingRecoveryStatus(currentStatusResponse, currentStatusRequest.WithContext(currentStatusContext))
	if currentStatusResponse.Code != http.StatusOK || !strings.Contains(currentStatusResponse.Body.String(), `"authority":"current_lease"`) {
		t.Fatalf("current lease recovery status=%d body=%s", currentStatusResponse.Code, currentStatusResponse.Body.String())
	}
	var firstClipID, secondClipID int64
	if err = pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE recording_job_id=$1 AND capture_lease_token=$2 AND capture_sequence=1`, f.jobID, lease).Scan(&firstClipID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT id FROM recording_clips WHERE recording_job_id=$1 AND capture_lease_token=$2 AND capture_sequence=2`, f.jobID, lease).Scan(&secondClipID); err != nil {
		t.Fatal(err)
	}
	forgedHeadTx, beginErr := pool.Begin(ctx)
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	if _, err = forgedHeadTx.Exec(ctx, `INSERT INTO recording_capture_artifact_results(upload_intent_id,result,clip_id,head_version) VALUES($1,'accepted_unique',$2,1)`, intentOne, secondClipID); err != nil {
		_ = forgedHeadTx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = forgedHeadTx.Exec(ctx, `UPDATE recording_job_unique_heads SET version=1,upload_intent_id=$3,clip_id=$4,capture_sequence=2,advanced_at=transaction_timestamp() WHERE recording_job_id=$1 AND lease_token=$2`, f.jobID, lease, intentOne, secondClipID); err != nil {
		_ = forgedHeadTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = forgedHeadTx.Commit(ctx); err == nil {
		t.Fatal("accepted-unique head committed with a clip that did not match the sealed artifact bytes")
	}
	headTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = headTx.Exec(ctx, `INSERT INTO recording_capture_artifact_results(upload_intent_id,result,clip_id,head_version) VALUES($1,'accepted_unique',$2,1)`, intentOne, firstClipID); err != nil {
		_ = headTx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = headTx.Exec(ctx, `UPDATE recording_job_unique_heads SET version=1,upload_intent_id=$3,clip_id=$4,capture_sequence=1,advanced_at=transaction_timestamp() WHERE recording_job_id=$1 AND lease_token=$2`, f.jobID, lease, intentOne, firstClipID); err != nil {
		_ = headTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = headTx.Commit(ctx); err != nil {
		t.Fatalf("accepted unique head commit: %v", err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_capture_artifact_results(upload_intent_id,result) VALUES($1,'security_revoked')`, intentTwo); err == nil {
		t.Fatal("artifact security result committed without an exact grant revocation")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_capture_producer_results(producer_id,result,detail_class) VALUES($1,'security_revoked','recovery_capability_revoked')`, producer); err == nil {
		t.Fatal("producer security result committed without an exact grant revocation")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL WHERE id=$1`, f.jobID); err == nil {
		t.Fatal("generic job transition cleared a lease with nonterminal durable capture authority")
	}
	attempt := uuid.New()
	attemptSHA := surrenderRequestSHA(recordingJobSurrenderRequest{TransportVersion: 1, AttemptID: attempt.String(), Reason: recordingJobSurrenderNoProgress})
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO recording_job_surrender_attempts(id,recording_job_id,lease_token,worker_id,node_id,reason,expected_head_version,spool_count,spool_bytes,in_flight_count,request_sha256) VALUES($1,$2,$3,'cloud-recovery',$4,'no_progress',0,0,0,0,$5)`, attempt, f.jobID, lease, f.nodeID, attemptSHA); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err == nil {
		t.Fatal("surrender attempt without terminal result committed")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_job_surrender_attempts(id,recording_job_id,lease_token,worker_id,node_id,reason,expected_head_version,spool_count,spool_bytes,in_flight_count,request_sha256) VALUES($1,$2,$3,'cloud-recovery',$4,'no_progress',0,0,0,0,$5)`, uuid.New(), f.jobID, lease, f.nodeID, strings.Repeat("d", 64)); err == nil {
		t.Fatal("noncanonical surrender request digest committed")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET lease_expires_at=transaction_timestamp()-interval '1 second' WHERE id=$1`, f.jobID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_capture_artifact_seals(upload_intent_id,producer_id,capture_sequence,segment_start_ms,size_bytes,sha256) VALUES($1,$2,3,$3,1024,$4)`, intentThree, producer, firstSegmentStartMs+120000, strings.Repeat("c", 64)); err == nil {
		t.Fatal("expired main lease sealed a new artifact before upload-only recovery authority existed")
	}
	var reclaimed int64
	if err = pool.QueryRow(ctx, `SELECT recording_surrender_reclaim_expired()`).Scan(&reclaimed); err != nil {
		t.Fatal(err)
	}
	if reclaimed != 1 {
		t.Fatalf("reclaimed=%d want 1", reclaimed)
	}
	var status string
	var owner, token *string
	var handoffUntil, changedAt time.Time
	if err = pool.QueryRow(ctx, `SELECT status,lease_owner,lease_token::text,handoff_until,updated_at FROM recording_jobs WHERE id=$1`, f.jobID).Scan(&status, &owner, &token, &handoffUntil, &changedAt); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || owner != nil || token != nil || !handoffUntil.Equal(changedAt) {
		t.Fatalf("job status=%s owner=%v token=%v handoff=%s changed=%s", status, owner, token, handoffUntil, changedAt)
	}
	var grantCount, grantIntentCount, expiryCount int
	var grace time.Duration
	if err = pool.QueryRow(ctx, `SELECT count(*),max(upload_grace_until-granted_at) FROM recording_job_recovery_grants WHERE recording_job_id=$1 AND lease_token=$2`, f.jobID, lease).Scan(&grantCount, &grace); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_job_lease_expiry_events WHERE recording_job_id=$1 AND lease_token=$2 AND NOT alternate_available`, f.jobID, lease).Scan(&expiryCount); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_job_recovery_grant_intents grant_intent JOIN recording_job_recovery_grants grant_row ON grant_row.id=grant_intent.grant_id WHERE grant_row.recording_job_id=$1 AND grant_row.lease_token=$2`, f.jobID, lease).Scan(&grantIntentCount); err != nil {
		t.Fatal(err)
	}
	if grantCount != 1 || grantIntentCount != 1 || expiryCount != 1 || grace != 30*time.Minute {
		t.Fatalf("grants=%d intents=%d expiry=%d grace=%s", grantCount, grantIntentCount, expiryCount, grace)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_job_recovery_grants SET revoked_at=transaction_timestamp(),revoke_reason='recovery_completed' WHERE producer_id=$1`, producer); err == nil {
		t.Fatal("recovery grant closed without an exact terminal producer result")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_job_recovery_grant_intents(grant_id,upload_intent_id) SELECT id,$2 FROM recording_job_recovery_grants WHERE producer_id=$1`, producer, intentThree); err == nil {
		t.Fatal("unsealed upload intent was attached to recovery grant")
	}
	var dropletID int64
	if err = pool.QueryRow(ctx, `SELECT id FROM recorder_droplets WHERE name='cloud-recovery'`).Scan(&dropletID); err != nil {
		t.Fatal(err)
	}
	store := dropletpool.NewStore(pool)
	if retired, retireErr := store.BeginDestroyIfIdle(ctx, dropletID); retireErr != nil || retired {
		t.Fatalf("active recovery ordinary destroy retired=%v err=%v", retired, retireErr)
	}
	if err = store.MarkDraining(ctx, dropletID); err != nil {
		t.Fatal(err)
	}
	if retired, retireErr := store.BeginForcedDestroyAfterDrainTimeout(ctx, dropletID); retireErr != nil || retired {
		t.Fatalf("active recovery forced destroy retired=%v err=%v", retired, retireErr)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/recording/recovery/producers/"+producer.String()+"/status", nil)
	request.Header.Set(recordingRecoveryProducerHeader, producer.String())
	request.Header.Set(recordingRecoverySecretHeader, recoverySecret)
	recovery, err := (&Server{pool: pool}).authenticateRecordingRecovery(request)
	if err != nil || recovery.ProducerID != producer || recovery.JobID != f.jobID || recovery.LeaseToken != lease {
		t.Fatalf("recovery=%+v err=%v", recovery, err)
	}
	request.Header.Set(recordingRecoverySecretHeader, strings.Repeat("2b", 32))
	if _, err = (&Server{pool: pool}).authenticateRecordingRecovery(request); err == nil {
		t.Fatal("wrong recovery secret authenticated")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_capture_producers SET worker_id='forged' WHERE id=$1`, producer); err == nil {
		t.Fatal("capture producer authority was mutable")
	}
	if _, err = pool.Exec(ctx, `TRUNCATE recording_job_lease_generations`); err == nil {
		t.Fatal("lease generation authority was truncatable")
	}
	if _, err = pool.Exec(ctx, `SELECT recording_surrender_revoke_recovery_grant(id,'operator_cleanup') FROM recording_job_recovery_grants WHERE producer_id=$1`, producer); err == nil {
		t.Fatal("untyped recovery revocation reason succeeded")
	}
	var revoked bool
	if err = pool.QueryRow(ctx, `SELECT recording_surrender_revoke_recovery_grant(id,'security_incident') FROM recording_job_recovery_grants WHERE producer_id=$1`, producer).Scan(&revoked); err != nil || !revoked {
		t.Fatalf("security revocation=%v err=%v", revoked, err)
	}
	var producerResult, revokeReason string
	if err = pool.QueryRow(ctx, `SELECT result,revoke_reason FROM recording_capture_producer_results JOIN recording_job_recovery_grants USING(producer_id) WHERE producer_id=$1`, producer).Scan(&producerResult, &revokeReason); err != nil || producerResult != "security_revoked" || revokeReason != "security_incident" {
		t.Fatalf("producer result=%q revoke=%q err=%v", producerResult, revokeReason, err)
	}
	// A crash after the server sealed the producer result but before the worker
	// removed its journal must be locally cleanable with the still-valid ordinary
	// node principal; no upload authority is granted by this read-only response.
	statusRequest := httptest.NewRequest(http.MethodPost, "/api/v1/recording/recovery/producers/"+producer.String()+"/status", nil)
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("producerId", producer.String())
	statusContext := context.WithValue(statusRequest.Context(), chi.RouteCtxKey, routeContext)
	statusContext = context.WithValue(statusContext, nodePrincipalContextKey, nodePrincipal{NodeID: f.nodeID, AccountID: f.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: "cloud-recovery"})
	statusRequest = statusRequest.WithContext(statusContext)
	statusResponse := httptest.NewRecorder()
	(&Server{pool: pool}).handleRecordingRecoveryStatus(statusResponse, statusRequest)
	if statusResponse.Code != http.StatusOK || !strings.Contains(statusResponse.Body.String(), `"producer_result":"security_revoked"`) {
		t.Fatalf("terminal producer status=%d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	if retired, retireErr := store.BeginDestroyIfIdle(ctx, dropletID); retireErr != nil || !retired {
		t.Fatalf("terminal recovery did not release host teardown: retired=%v err=%v", retired, retireErr)
	}

	cloud := seedPresentationV2Task(t, pool, 91003, 991003)
	cloudLease := uuid.New()
	cloudWorker := "cloud-surrender"
	if _, err = pool.Exec(ctx, `
		UPDATE nodes SET node_type='local_recorder',display_name=$2 WHERE id=$1;
		INSERT INTO recorder_droplets(name,node_id,state,capacity,last_seen_at) VALUES($2,$1,'active',1,transaction_timestamp());
		UPDATE recordings SET capture_via='cloud' WHERE id=$3;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL WHERE id=$4;
		UPDATE recording_jobs SET status='leased',lease_owner=$2,lease_expires_at=transaction_timestamp()+interval '3 minutes',lease_token=$5,attempt_count=attempt_count+1 WHERE id=$4
	`, cloud.nodeID, cloudWorker, cloud.recordingID, cloud.jobID, cloudLease); err != nil {
		t.Fatal(err)
	}
	surrenderAttempt := uuid.New()
	surrenderBody := recordingJobSurrenderRequest{TransportVersion: 1, AttemptID: surrenderAttempt.String(), Reason: recordingJobSurrenderDiskPressure, ErrorText: "local disk reserve reached"}
	callSurrender := func(body recordingJobSurrenderRequest) *httptest.ResponseRecorder {
		raw, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/recording/jobs/"+strconv.FormatInt(cloud.jobID, 10)+"/surrender", bytes.NewReader(raw))
		request.Header.Set(recordingLeaseTokenHeader, cloudLease.String())
		route := chi.NewRouteContext()
		route.URLParams.Add("id", strconv.FormatInt(cloud.jobID, 10))
		requestContext := context.WithValue(request.Context(), chi.RouteCtxKey, route)
		requestContext = context.WithValue(requestContext, nodePrincipalContextKey, nodePrincipal{NodeID: cloud.nodeID, AccountID: cloud.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: cloudWorker})
		request = request.WithContext(requestContext)
		response := httptest.NewRecorder()
		(&Server{pool: pool}).handleRecordingJobSurrender(response, request)
		return response
	}
	committed := callSurrender(surrenderBody)
	if committed.Code != http.StatusOK || !strings.Contains(committed.Body.String(), `"result":"committed"`) || !strings.Contains(committed.Body.String(), `"alternate_available":false`) {
		t.Fatalf("v1 no-alternate surrender status=%d body=%s", committed.Code, committed.Body.String())
	}
	replayed := callSurrender(surrenderBody)
	if replayed.Code != http.StatusOK || replayed.Body.String() != committed.Body.String() {
		t.Fatalf("v1 surrender replay status=%d first=%s replay=%s", replayed.Code, committed.Body.String(), replayed.Body.String())
	}
	tampered := surrenderBody
	tampered.ErrorText = "different bytes"
	if response := callSurrender(tampered); response.Code != http.StatusConflict {
		t.Fatalf("v1 surrender mismatched replay status=%d body=%s", response.Code, response.Body.String())
	}
	var surrenderRows int
	var retryDelay time.Duration
	if err = pool.QueryRow(ctx, `
		SELECT count(*),max(result.next_retry_at-result.result_at)
		FROM recording_job_surrender_attempts attempt
		JOIN recording_job_surrender_results result ON result.attempt_id=attempt.id
		WHERE attempt.recording_job_id=$1
	`, cloud.jobID).Scan(&surrenderRows, &retryDelay); err != nil {
		t.Fatal(err)
	}
	if surrenderRows != 1 || retryDelay != 0 {
		t.Fatalf("no-alternate surrender rows=%d retry_delay=%s", surrenderRows, retryDelay)
	}

	// Exercise the real producer-reservation and surrender handlers in both lock
	// orders. A row lock pauses the first handler only after it owns the common
	// job advisory fence; the second handler must wait, then observe the winner's
	// committed state rather than admitting a cross-fence producer or dropping a
	// newly reserved producer.
	waitJobFence := func(jobID int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			probe, probeErr := pool.Begin(ctx)
			if probeErr != nil {
				t.Fatal(probeErr)
			}
			var acquired bool
			probeErr = probe.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, recordingSurrenderJobLockKey(jobID)).Scan(&acquired)
			_ = probe.Rollback(ctx)
			if probeErr != nil {
				t.Fatal(probeErr)
			}
			if !acquired {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("handler did not acquire the recording surrender job fence")
	}
	leaseCloudRace := func(streamID, recordingID int64, worker string) (presentationV2Fixture, uuid.UUID) {
		t.Helper()
		fixture := seedPresentationV2Task(t, pool, streamID, recordingID)
		leaseToken := uuid.New()
		if _, setupErr := pool.Exec(ctx, `
			UPDATE nodes SET node_type='local_recorder',display_name=$2 WHERE id=$1;
			INSERT INTO recorder_droplets(name,node_id,state,capacity,last_seen_at) VALUES($2,$1,'active',1,transaction_timestamp());
			UPDATE recordings SET capture_via='cloud' WHERE id=$3;
			UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL WHERE id=$4;
			UPDATE recording_jobs SET status='leased',lease_owner=$2,lease_expires_at=transaction_timestamp()+interval '3 minutes',lease_token=$5,attempt_count=attempt_count+1 WHERE id=$4
		`, fixture.nodeID, worker, fixture.recordingID, fixture.jobID, leaseToken); setupErr != nil {
			t.Fatal(setupErr)
		}
		return fixture, leaseToken
	}
	callRaceSurrender := func(callCtx context.Context, fixture presentationV2Fixture, leaseToken uuid.UUID, worker string, attemptID uuid.UUID) *httptest.ResponseRecorder {
		t.Helper()
		body := recordingJobSurrenderRequest{TransportVersion: 1, AttemptID: attemptID.String(), Reason: recordingJobSurrenderNoProgress}
		raw, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/recording/jobs/"+strconv.FormatInt(fixture.jobID, 10)+"/surrender", bytes.NewReader(raw)).WithContext(callCtx)
		request.Header.Set(recordingLeaseTokenHeader, leaseToken.String())
		route := chi.NewRouteContext()
		route.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
		requestContext := context.WithValue(request.Context(), chi.RouteCtxKey, route)
		requestContext = context.WithValue(requestContext, nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker})
		response := httptest.NewRecorder()
		(&Server{pool: pool}).handleRecordingJobSurrender(response, request.WithContext(requestContext))
		return response
	}
	callRaceReserve := func(callCtx context.Context, fixture presentationV2Fixture, leaseToken uuid.UUID, worker string, producerID uuid.UUID) *httptest.ResponseRecorder {
		t.Helper()
		body := recordingCaptureProducerReserveRequest{ProducerID: producerID.String(), CaptureOrdinal: 1, SealedIntentLimit: 2, RecoverySecretSHA256: strings.Repeat("e", 64)}
		raw, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/recording/jobs/"+strconv.FormatInt(fixture.jobID, 10)+"/capture-producers", bytes.NewReader(raw)).WithContext(callCtx)
		request.Header.Set(recordingLeaseTokenHeader, leaseToken.String())
		route := chi.NewRouteContext()
		route.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
		requestContext := context.WithValue(request.Context(), chi.RouteCtxKey, route)
		requestContext = context.WithValue(requestContext, nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker})
		response := httptest.NewRecorder()
		(&Server{pool: pool}).handleRecordingCaptureProducerReserve(response, request.WithContext(requestContext))
		return response
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET status='canceled',updated_at=transaction_timestamp() WHERE id=ANY($1) AND status='pending'`, []int64{f.jobID, cloud.jobID}); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET state='draining' WHERE name='cloud-surrender'`); err != nil {
		t.Fatal(err)
	}
	dynamicOwner, dynamicLease := leaseCloudRace(91006, 991006, "cloud-dynamic-owner")
	dynamicAlternate := seedPresentationV2Task(t, pool, 91007, 991007)
	if _, err = pool.Exec(ctx, `
		UPDATE nodes SET node_type='local_recorder',display_name='cloud-dynamic-alternate' WHERE id=$1;
		INSERT INTO recorder_droplets(name,node_id,state,capacity,last_seen_at) VALUES('cloud-dynamic-alternate',$1,'active',1,transaction_timestamp());
		UPDATE recording_jobs SET status='canceled',updated_at=transaction_timestamp() WHERE id=$2
	`, dynamicAlternate.nodeID, dynamicAlternate.jobID); err != nil {
		t.Fatal(err)
	}
	dynamicSurrender := callRaceSurrender(ctx, dynamicOwner, dynamicLease, "cloud-dynamic-owner", uuid.New())
	if dynamicSurrender.Code != http.StatusOK || !strings.Contains(dynamicSurrender.Body.String(), `"result":"committed"`) || !strings.Contains(dynamicSurrender.Body.String(), `"alternate_available":true`) {
		t.Fatalf("capacity-backed surrender=%d body=%s", dynamicSurrender.Code, dynamicSurrender.Body.String())
	}
	if _, err = pool.Exec(ctx, `UPDATE recorder_droplets SET state='draining',last_seen_at=transaction_timestamp() WHERE name='cloud-dynamic-alternate'`); err != nil {
		t.Fatal(err)
	}
	leaseRequest := httptest.NewRequest(http.MethodPost, "/api/v1/recording/jobs/lease", nil)
	leaseRequest.Header.Set(recordingLeaseTokenSupportedHeader, "true")
	leaseRequest = leaseRequest.WithContext(context.WithValue(leaseRequest.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: dynamicOwner.nodeID, AccountID: dynamicOwner.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: "cloud-dynamic-owner"}))
	leaseResponse := httptest.NewRecorder()
	(&Server{pool: pool}).handleRecordingJobsLease(leaseResponse, leaseRequest)
	if leaseResponse.Code != http.StatusOK || !strings.Contains(leaseResponse.Body.String(), `"job_id":`+strconv.FormatInt(dynamicOwner.jobID, 10)) || !strings.Contains(leaseResponse.Body.String(), `"surrender_transport_version":1`) {
		t.Fatalf("same-owner no-alternate recovery lease=%d body=%s", leaseResponse.Code, leaseResponse.Body.String())
	}

	surrenderFirst, surrenderFirstLease := leaseCloudRace(91004, 991004, "cloud-race-surrender-first")
	surrenderFirstBlocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = surrenderFirstBlocker.Exec(ctx, `SELECT 1 FROM recording_jobs WHERE id=$1 FOR UPDATE`, surrenderFirst.jobID); err != nil {
		_ = surrenderFirstBlocker.Rollback(ctx)
		t.Fatal(err)
	}
	raceCtx, raceCancel := context.WithTimeout(ctx, 10*time.Second)
	surrenderFirstResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		surrenderFirstResult <- callRaceSurrender(raceCtx, surrenderFirst, surrenderFirstLease, "cloud-race-surrender-first", uuid.New())
	}()
	waitJobFence(surrenderFirst.jobID)
	lateProducer := uuid.New()
	reserveAfterSurrender := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		reserveAfterSurrender <- callRaceReserve(raceCtx, surrenderFirst, surrenderFirstLease, "cloud-race-surrender-first", lateProducer)
	}()
	if err = surrenderFirstBlocker.Commit(ctx); err != nil {
		raceCancel()
		t.Fatal(err)
	}
	firstSurrenderResponse, lateReserveResponse := <-surrenderFirstResult, <-reserveAfterSurrender
	raceCancel()
	if firstSurrenderResponse.Code != http.StatusOK || !strings.Contains(firstSurrenderResponse.Body.String(), `"result":"committed"`) {
		t.Fatalf("surrender-first response=%d body=%s", firstSurrenderResponse.Code, firstSurrenderResponse.Body.String())
	}
	if lateReserveResponse.Code != http.StatusConflict {
		t.Fatalf("reserve-after-surrender response=%d body=%s", lateReserveResponse.Code, lateReserveResponse.Body.String())
	}

	producerFirst, producerFirstLease := leaseCloudRace(91005, 991005, "cloud-race-producer-first")
	producerFirstBlocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = producerFirstBlocker.Exec(ctx, `SELECT 1 FROM recording_job_lease_generations WHERE recording_job_id=$1 AND lease_token=$2 FOR UPDATE`, producerFirst.jobID, producerFirstLease); err != nil {
		_ = producerFirstBlocker.Rollback(ctx)
		t.Fatal(err)
	}
	raceCtx, raceCancel = context.WithTimeout(ctx, 10*time.Second)
	producerFirstID := uuid.New()
	reserveFirstResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		reserveFirstResult <- callRaceReserve(raceCtx, producerFirst, producerFirstLease, "cloud-race-producer-first", producerFirstID)
	}()
	waitJobFence(producerFirst.jobID)
	surrenderAfterReserve := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		surrenderAfterReserve <- callRaceSurrender(raceCtx, producerFirst, producerFirstLease, "cloud-race-producer-first", uuid.New())
	}()
	if err = producerFirstBlocker.Commit(ctx); err != nil {
		raceCancel()
		t.Fatal(err)
	}
	firstReserveResponse, lateSurrenderResponse := <-reserveFirstResult, <-surrenderAfterReserve
	raceCancel()
	if firstReserveResponse.Code != http.StatusOK {
		t.Fatalf("producer-first reserve=%d body=%s", firstReserveResponse.Code, firstReserveResponse.Body.String())
	}
	if lateSurrenderResponse.Code != http.StatusOK || !strings.Contains(lateSurrenderResponse.Body.String(), `"result":"ineligible_spool"`) {
		t.Fatalf("surrender-after-reserve=%d body=%s", lateSurrenderResponse.Code, lateSurrenderResponse.Body.String())
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_capture_producer_results(producer_id,result) VALUES($1,'abandoned_empty')`, producerFirstID); err != nil {
		t.Fatalf("terminalize producer-first empty authority: %v", err)
	}
	if terminalReplay := callRaceReserve(ctx, producerFirst, producerFirstLease, "cloud-race-producer-first", producerFirstID); terminalReplay.Code != http.StatusConflict {
		t.Fatalf("terminal producer reservation replay=%d body=%s", terminalReplay.Code, terminalReplay.Body.String())
	}

	// The centralized expiry authority must retain the pre-v1 relay behavior. It
	// may not infer a cloud alternate or create cloud recovery telemetry for a
	// relay recording merely because healthy droplets exist.
	relay := seedPresentationV2Task(t, pool, 91002, 991002)
	relayLease := uuid.New()
	if _, err = pool.Exec(ctx, `
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL WHERE id=$1;
		UPDATE recording_jobs SET status='leased',lease_owner=$2,lease_expires_at=transaction_timestamp()+interval '3 minutes',lease_token=$3,attempt_count=attempt_count+1 WHERE id=$1;
		UPDATE recording_jobs SET lease_expires_at=transaction_timestamp()-interval '1 second' WHERE id=$1
	`, relay.jobID, "node:"+strconv.FormatInt(relay.nodeID, 10), relayLease); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT recording_surrender_reclaim_expired()`).Scan(&reclaimed); err != nil {
		t.Fatal(err)
	}
	var relayStatus string
	var relayHandoff *string
	var relayEvents int
	if err = pool.QueryRow(ctx, `SELECT status,handoff_owner FROM recording_jobs WHERE id=$1`, relay.jobID).Scan(&relayStatus, &relayHandoff); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_job_lease_expiry_events WHERE recording_job_id=$1`, relay.jobID).Scan(&relayEvents); err != nil {
		t.Fatal(err)
	}
	if reclaimed != 1 || relayStatus != "pending" || relayHandoff != nil || relayEvents != 0 {
		t.Fatalf("relay reclaim=%d status=%q handoff=%v cloud_events=%d", reclaimed, relayStatus, relayHandoff, relayEvents)
	}
}
