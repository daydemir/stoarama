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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRecordingSurrenderTransportPostgresLifecycleAndExpiryRecovery(t *testing.T) {
	pool, cleanup := testPresentationV2Pool(t)
	defer cleanup()
	ctx := context.Background()

	cloud := seedPresentationV2Task(t, pool, 91001, 991001)
	cloudLease := uuid.New()
	if _, err := pool.Exec(ctx, `
		UPDATE nodes SET node_type='local_recorder',display_name='cloud-recovery' WHERE id=$1;
		INSERT INTO recorder_droplets(name,node_id,state,capacity,last_seen_at)
		VALUES('cloud-recovery',$1,'active',1,transaction_timestamp());
		UPDATE recordings SET capture_via='cloud' WHERE id=$2;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL WHERE id=$3;
		UPDATE recording_jobs SET status='leased',lease_owner='cloud-recovery',lease_expires_at=transaction_timestamp()+interval '3 minutes',lease_token=$4,attempt_count=attempt_count+1 WHERE id=$3;
	`, cloud.nodeID, cloud.recordingID, cloud.jobID, cloudLease); err != nil {
		t.Fatal(err)
	}

	// The legacy equal-SHA unique index remains a DB concurrency backstop. V1
	// units use their immutable intent/sequence identities and may carry equal
	// bytes without collapsing two distinct camera-time clips.
	legacyOne := poolSQLInsertClip(t, pool, cloud, cloudLease, 2, "legacy-one.mp4", strings.Repeat("a", 64), 0)
	firstTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = firstTx.Exec(ctx, legacyOne); err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan error, 1)
	go func() {
		_, insertErr := pool.Exec(ctx, poolSQLInsertClip(t, pool, cloud, cloudLease, 3, "legacy-two.mp4", strings.Repeat("a", 64), 0))
		secondDone <- insertErr
	}()
	select {
	case err = <-secondDone:
		t.Fatalf("parallel legacy insert did not wait for the DB backstop: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err = firstTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-secondDone; err == nil {
		t.Fatal("parallel legacy equal-SHA insert committed")
	}
	server := &Server{pool: pool}
	producerID := uuid.New()
	producerResponse := callProducerReserve(t, server, cloud, cloudLease, "cloud-recovery", producerID, 1)
	if producerResponse.Code != http.StatusOK {
		t.Fatalf("producer reserve=%d body=%s", producerResponse.Code, producerResponse.Body.String())
	}
	var sourceSnapshot, configSnapshot string
	var sourceRevision *int64
	if err = pool.QueryRow(ctx, `SELECT source_snapshot_sha256,capture_config_sha256,source_revision_head_id FROM recording_capture_producers WHERE id=$1`, producerID).Scan(&sourceSnapshot, &configSnapshot, &sourceRevision); err != nil {
		t.Fatal(err)
	}
	if len(sourceSnapshot) != 64 || len(configSnapshot) != 64 {
		t.Fatalf("source=%q config=%q", sourceSnapshot, configSnapshot)
	}

	intentIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	secrets := []string{strings.Repeat("1a", 32), strings.Repeat("2b", 32), strings.Repeat("3c", 32), strings.Repeat("4d", 32)}
	artifactResponse := callArtifactReserve(t, server, cloud, cloudLease, "cloud-recovery", producerID, intentIDs, secrets)
	if artifactResponse.Code != http.StatusNoContent {
		t.Fatalf("artifact reserve=%d body=%s", artifactResponse.Code, artifactResponse.Body.String())
	}
	// Exact replay is idempotent; one changed per-intent secret is not.
	if replay := callArtifactReserve(t, server, cloud, cloudLease, "cloud-recovery", producerID, intentIDs, secrets); replay.Code != http.StatusNoContent {
		t.Fatalf("artifact replay=%d body=%s", replay.Code, replay.Body.String())
	}
	tamperedSecrets := append([]string(nil), secrets...)
	tamperedSecrets[1] = strings.Repeat("5e", 32)
	if replay := callArtifactReserve(t, server, cloud, cloudLease, "cloud-recovery", producerID, intentIDs, tamperedSecrets); replay.Code != http.StatusConflict {
		t.Fatalf("tampered artifact replay=%d body=%s", replay.Code, replay.Body.String())
	}
	duplicateSecrets := append([]string(nil), secrets...)
	duplicateSecrets[1] = duplicateSecrets[0]
	if response := callArtifactReserve(t, server, cloud, cloudLease, "cloud-recovery", producerID, intentIDs, duplicateSecrets); response.Code != http.StatusBadRequest {
		t.Fatalf("duplicate artifact secret=%d body=%s", response.Code, response.Body.String())
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_upload_intents SET object_key=object_key||'-substituted' WHERE id=$1`, intentIDs[0]); err == nil {
		t.Fatal("bound pre-byte upload identity was mutable")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_upload_intents SET status='consumed' WHERE id=$1`, intentIDs[0]); err == nil {
		t.Fatal("bound pre-byte upload intent was consumable without its exact stored clip")
	}
	observationID, observationAttempt := uuid.New(), uuid.New()
	observationSHA := strings.Repeat("9", 64)
	observationAt := time.Now().UTC()
	if response := callTransportObservation(t, server, cloud, "cloud-recovery", cloudLease, observationID, observationAttempt, observationSHA, observationAt); response.Code != http.StatusNoContent {
		t.Fatalf("transport observation=%d body=%s", response.Code, response.Body.String())
	}
	acceptV1Artifact(t, pool, cloud, cloudLease, producerID, intentIDs[0], 5, strings.Repeat("b", 64))
	acceptV1Artifact(t, pool, cloud, cloudLease, producerID, intentIDs[1], 6, strings.Repeat("b", 64))
	if response := callTransportObservation(t, server, cloud, "cloud-recovery", cloudLease, observationID, observationAttempt, observationSHA, observationAt); response.Code != http.StatusNoContent {
		t.Fatalf("transport observation replay=%d body=%s", response.Code, response.Body.String())
	}
	var episodeState string
	if err = pool.QueryRow(ctx, `SELECT state FROM recording_surrender_transport_episodes WHERE recording_job_id=$1`, cloud.jobID).Scan(&episodeState); err != nil || episodeState != "resolved" {
		t.Fatalf("episode state=%q err=%v", episodeState, err)
	}
	forgedV1 := poolSQLInsertClip(t, pool, cloud, cloudLease, 9, "forged-v1.mp4", strings.Repeat("f", 64), 1)
	if _, err = pool.Exec(ctx, forgedV1); err == nil {
		t.Fatal("caller-selected v1 discriminator bypassed artifact provenance")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL WHERE id=$1`, cloud.jobID); err == nil {
		t.Fatal("generic writer cleared a lease with unresolved pre-byte artifact intents")
	}

	// A source writer that wins first produces one complete new snapshot; producer
	// reservation waits on the shared stream fence and never mixes old/new fields.
	sourceRace := seedPresentationV2Task(t, pool, 91002, 991002)
	sourceLease := uuid.New()
	if _, err = pool.Exec(ctx, `
		UPDATE nodes SET node_type='local_recorder',display_name='cloud-source-race' WHERE id=$1;
		INSERT INTO recorder_droplets(name,node_id,state,capacity,last_seen_at) VALUES('cloud-source-race',$1,'active',1,transaction_timestamp());
		UPDATE recordings SET capture_via='cloud' WHERE id=$2;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL WHERE id=$3;
		UPDATE recording_jobs SET status='leased',lease_owner='cloud-source-race',lease_expires_at=transaction_timestamp()+interval '3 minutes',lease_token=$4,attempt_count=attempt_count+1 WHERE id=$3;
	`, sourceRace.nodeID, sourceRace.recordingID, sourceRace.jobID, sourceLease); err != nil {
		t.Fatal(err)
	}
	sourceTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var oldURL string
	if err = sourceTx.QueryRow(ctx, `SELECT source_url FROM streams WHERE id=$1 FOR UPDATE`, sourceRace.streamID).Scan(&oldURL); err != nil {
		t.Fatal(err)
	}
	newURL := "https://official.example/source-race/live.m3u8"
	if _, err = sourceTx.Exec(ctx, `UPDATE streams SET source_url=$2 WHERE id=$1`, sourceRace.streamID, newURL); err != nil {
		t.Fatal(err)
	}
	if _, err = sourceTx.Exec(ctx, `INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url) VALUES($1,'test','source_crossing',$2,$3)`, sourceRace.streamID, oldURL, newURL); err != nil {
		t.Fatal(err)
	}
	raceDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		raceDone <- callProducerReserve(t, server, sourceRace, sourceLease, "cloud-source-race", uuid.New(), 1)
	}()
	select {
	case response := <-raceDone:
		t.Fatalf("producer crossed an uncommitted source change: %d %s", response.Code, response.Body.String())
	case <-time.After(100 * time.Millisecond):
	}
	if err = sourceTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if response := <-raceDone; response.Code != http.StatusOK {
		t.Fatalf("producer after source commit=%d body=%s", response.Code, response.Body.String())
	}
	producerFirst := seedPresentationV2Task(t, pool, 91005, 991005)
	producerFirstLease := uuid.New()
	if _, err = pool.Exec(ctx, `
		UPDATE nodes SET node_type='local_recorder',display_name='cloud-producer-first' WHERE id=$1;
		INSERT INTO recorder_droplets(name,node_id,state,capacity,last_seen_at) VALUES('cloud-producer-first',$1,'active',1,transaction_timestamp());
		UPDATE recordings SET capture_via='cloud' WHERE id=$2;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL WHERE id=$3;
		UPDATE recording_jobs SET status='leased',lease_owner='cloud-producer-first',lease_expires_at=transaction_timestamp()+interval '3 minutes',lease_token=$4,attempt_count=attempt_count+1 WHERE id=$3;
	`, producerFirst.nodeID, producerFirst.recordingID, producerFirst.jobID, producerFirstLease); err != nil {
		t.Fatal(err)
	}
	producerFirstID := uuid.New()
	if response := callProducerReserve(t, server, producerFirst, producerFirstLease, "cloud-producer-first", producerFirstID, 1); response.Code != http.StatusOK {
		t.Fatalf("producer-first reserve=%d body=%s", response.Code, response.Body.String())
	}
	var frozenRevision int64
	if err = pool.QueryRow(ctx, `SELECT source_revision_head_id FROM recording_capture_producers WHERE id=$1`, producerFirstID).Scan(&frozenRevision); err != nil {
		t.Fatal(err)
	}
	var priorURL string
	if err = pool.QueryRow(ctx, `SELECT source_url FROM streams WHERE id=$1`, producerFirst.streamID).Scan(&priorURL); err != nil {
		t.Fatal(err)
	}
	var successorRevision int64
	if err = pool.QueryRow(ctx, `WITH changed AS (
		UPDATE streams SET source_url=$2 WHERE id=$1 RETURNING id
	) INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url)
	SELECT id,'test','producer_first_crossing',$3,$2 FROM changed RETURNING id
	`, producerFirst.streamID, "https://official.example/producer-first-successor.m3u8", priorURL).Scan(&successorRevision); err != nil {
		t.Fatal(err)
	}
	if successorRevision <= frozenRevision {
		t.Fatalf("producer snapshot revision=%d successor=%d", frozenRevision, successorRevision)
	}
	if response := callProducerFinish(t, server, producerFirst, producerFirstLease, "cloud-producer-first", producerFirstID, "abandoned_empty", "no_artifact_reservation"); response.Code != http.StatusNoContent {
		t.Fatalf("finish zero-reservation producer=%d body=%s", response.Code, response.Body.String())
	}
	if _, err = pool.Exec(ctx, `
		INSERT INTO recording_capture_producers
		  (id,recording_job_id,lease_token,capture_ordinal,worker_id,node_id,sealed_intent_limit,source_snapshot_sha256,source_revision_head_id,capture_config_sha256)
		VALUES($1,$2,$3,2,'cloud-producer-first',$4,2,repeat('0',64),$5,repeat('1',64))
	`, uuid.New(), producerFirst.jobID, producerFirstLease, producerFirst.nodeID, successorRevision); err == nil {
		t.Fatal("caller-authored source/config snapshot bypassed the DB-derived binding")
	}

	// Expiry creates one exact upload-only capability for every reservation,
	// including reserved-unsealed artifacts, before the main fence is cleared.
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET lease_expires_at=transaction_timestamp()-interval '1 second' WHERE id=$1`, cloud.jobID); err != nil {
		t.Fatal(err)
	}
	var reclaimed int64
	if err = pool.QueryRow(ctx, `SELECT recording_surrender_reclaim_expired()`).Scan(&reclaimed); err != nil {
		t.Fatal(err)
	}
	if reclaimed < 1 {
		t.Fatalf("reclaimed=%d", reclaimed)
	}
	var grants, expiryEvents, observations, episodes int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_job_recovery_grants WHERE recording_job_id=$1 AND lease_token=$2`, cloud.jobID, cloudLease).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_job_lease_expiry_events WHERE recording_job_id=$1 AND lease_token=$2`, cloud.jobID, cloudLease).Scan(&expiryEvents); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_surrender_transport_observations WHERE recording_job_id=$1 AND lease_token=$2 AND observation_type='expired_reclaim'`, cloud.jobID, cloudLease).Scan(&observations); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_surrender_transport_episode_events WHERE recording_job_id=$1 AND lease_token=$2 AND event_type='opened' AND reason='expired_reclaim'`, cloud.jobID, cloudLease).Scan(&episodes); err != nil {
		t.Fatal(err)
	}
	if grants != 4 || expiryEvents != 1 || observations != 1 || episodes != 1 {
		t.Fatalf("grants=%d expiry=%d observations=%d episodes=%d", grants, expiryEvents, observations, episodes)
	}

	for index, intentID := range intentIDs {
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request.Header.Set(recordingRecoveryIntentHeader, intentID.String())
		request.Header.Set(recordingRecoverySecretHeader, secrets[index])
		recovery, authErr := server.authenticateRecordingRecovery(request)
		if authErr != nil || recovery.IntentID != intentID || recovery.ProducerID != producerID {
			t.Fatalf("intent=%s recovery=%+v err=%v", intentID, recovery, authErr)
		}
		request.Header.Set(recordingRecoveryIntentHeader, intentIDs[(index+1)%len(intentIDs)].String())
		if _, authErr = server.authenticateRecordingRecovery(request); authErr == nil {
			t.Fatal("one artifact secret authenticated a different intent")
		}
	}

	// PostgreSQL grants functions to PUBLIC by default; migration 0139 must revoke
	// every callable authority/helper while leaving trigger execution intact.
	for _, signature := range []string{
		"recording_surrender_reclaim_expired()",
		"recording_surrender_revoke_recovery_grant(uuid,text)",
		"recording_surrender_request_sha(uuid,text,text,bigint,uuid,bigint,integer,bigint,integer)",
	} {
		var publicCanExecute bool
		if err = pool.QueryRow(ctx, `
			SELECT EXISTS(
			  SELECT 1
			  FROM pg_proc procedure
			  CROSS JOIN LATERAL aclexplode(COALESCE(procedure.proacl,acldefault('f',procedure.proowner))) privilege
			  WHERE procedure.oid=$1::regprocedure AND privilege.grantee=0 AND privilege.privilege_type='EXECUTE'
			)
		`, signature).Scan(&publicCanExecute); err != nil {
			t.Fatal(err)
		}
		if publicCanExecute {
			t.Fatalf("PUBLIC can execute %s", signature)
		}
	}
	var publicSurrenderFunctions int
	if err = pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_proc procedure
		JOIN pg_namespace namespace ON namespace.oid=procedure.pronamespace AND namespace.nspname=current_schema()
		CROSS JOIN LATERAL aclexplode(COALESCE(procedure.proacl,acldefault('f',procedure.proowner))) privilege
		WHERE procedure.proname LIKE 'recording_surrender_%'
		  AND privilege.grantee=0 AND privilege.privilege_type='EXECUTE'
	`).Scan(&publicSurrenderFunctions); err != nil {
		t.Fatal(err)
	}
	if publicSurrenderFunctions != 0 {
		t.Fatalf("PUBLIC execute remains on %d surrender functions", publicSurrenderFunctions)
	}

	// Relay is a first-class v1 generation. It has no cloud-droplet heartbeat
	// dependency and no fabricated alternate-capacity exclusion.
	relay := seedPresentationV2Task(t, pool, 91003, 991003)
	if _, err = pool.Exec(ctx, `UPDATE nodes SET node_type='relay',display_name='relay-recovery' WHERE id=$1; UPDATE recordings SET capture_via='relay' WHERE id=$2; UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,scheduled_for=transaction_timestamp()-interval '1 minute' WHERE id=$3`, relay.nodeID, relay.recordingID, relay.jobID); err != nil {
		t.Fatal(err)
	}
	relayLease, err := server.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: relay.nodeID, AccountID: relay.accountID, NodeType: nodeTypeRelay}, true, 150, true)
	if err != nil {
		t.Fatal(err)
	}
	if relayLease.LeaseToken == nil || relayLease.SurrenderTransportVersion != 1 {
		t.Fatalf("relay lease=%+v", relayLease)
	}
	relaySurrender := callSurrender(t, server, relay, *relayLease.LeaseToken, "node:"+strconv.FormatInt(relay.nodeID, 10), nodeTypeRelay, uuid.New())
	if relaySurrender.Code != http.StatusOK || !strings.Contains(relaySurrender.Body.String(), `"result":"committed"`) || !strings.Contains(relaySurrender.Body.String(), `"alternate_available":false`) {
		t.Fatalf("relay surrender=%d body=%s", relaySurrender.Code, relaySurrender.Body.String())
	}
	// Keep this already-proved occurrence from winning the next relay lease; the
	// expiry fixture below must exercise same-job/same-owner recovery, not merely
	// prove that some other pending relay job was claimable.
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET scheduled_for=transaction_timestamp()+interval '1 hour' WHERE id=$1`, relay.jobID); err != nil {
		t.Fatal(err)
	}
	relayExpiry := seedPresentationV2Task(t, pool, 91006, 991006)
	if _, err = pool.Exec(ctx, `UPDATE nodes SET node_type='relay',display_name='relay-expiry' WHERE id=$1; UPDATE recordings SET capture_via='relay' WHERE id=$2; UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,scheduled_for=transaction_timestamp()-interval '1 minute' WHERE id=$3`, relayExpiry.nodeID, relayExpiry.recordingID, relayExpiry.jobID); err != nil {
		t.Fatal(err)
	}
	relayExpiredLease, err := server.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: relayExpiry.nodeID, AccountID: relayExpiry.accountID, NodeType: nodeTypeRelay}, true, 150, true)
	if err != nil || relayExpiredLease.LeaseToken == nil {
		t.Fatalf("relay expiry lease=%+v err=%v", relayExpiredLease, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET lease_expires_at=transaction_timestamp()-interval '1 second' WHERE id=$1`, relayExpiry.jobID); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT recording_surrender_reclaim_expired()`).Scan(&reclaimed); err != nil {
		t.Fatal(err)
	}
	var relayAlternate bool
	var relayHandoff, relayReclaimed time.Time
	if err = pool.QueryRow(ctx, `SELECT alternate_available,handoff_until,reclaimed_at FROM recording_job_lease_expiry_events WHERE recording_job_id=$1 AND lease_token=$2`, relayExpiry.jobID, relayExpiredLease.LeaseToken).Scan(&relayAlternate, &relayHandoff, &relayReclaimed); err != nil {
		t.Fatal(err)
	}
	if relayAlternate || !relayHandoff.Equal(relayReclaimed) {
		t.Fatalf("relay alternate=%v handoff=%s reclaimed=%s", relayAlternate, relayHandoff, relayReclaimed)
	}
	relayReclaimedLease, err := server.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: relayExpiry.nodeID, AccountID: relayExpiry.accountID, NodeType: nodeTypeRelay}, true, 150, true)
	if err != nil || relayReclaimedLease.JobID != relayExpiry.jobID || relayReclaimedLease.LeaseToken == nil || *relayReclaimedLease.LeaseToken == *relayExpiredLease.LeaseToken {
		t.Fatalf("relay same-owner recovery lease=%+v err=%v", relayReclaimedLease, err)
	}

	// Deferred attempt/result sealing is symmetric: neither half can commit alone.
	sealFixture := seedPresentationV2Task(t, pool, 91004, 991004)
	sealLease := uuid.New()
	if _, err = pool.Exec(ctx, `UPDATE nodes SET node_type='relay' WHERE id=$1; UPDATE recordings SET capture_via='relay' WHERE id=$2; UPDATE recording_jobs SET status='leased',lease_owner='node:'||$1::text,lease_token=$4,lease_expires_at=transaction_timestamp()+interval '2 minutes',attempt_count=attempt_count+1 WHERE id=$3`, sealFixture.nodeID, sealFixture.recordingID, sealFixture.jobID, sealLease); err != nil {
		t.Fatal(err)
	}
	attemptID := uuid.New()
	attemptSHA := surrenderRequestSHA(recordingJobSurrenderRequest{TransportVersion: 1, AttemptID: attemptID.String(), Reason: recordingJobSurrenderNoProgress})
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO recording_job_surrender_attempts(id,recording_job_id,lease_token,worker_id,node_id,reason,expected_head_version,spool_count,spool_bytes,in_flight_count,request_sha256) VALUES($1,$2,$3,$4,$5,'no_progress',0,0,0,0,$6)`, attemptID, sealFixture.jobID, sealLease, "node:"+strconv.FormatInt(sealFixture.nodeID, 10), sealFixture.nodeID, attemptSHA); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err == nil {
		t.Fatal("surrender attempt committed without one terminal result")
	}

	if _, err = pool.Exec(ctx, `TRUNCATE recording_capture_artifact_intents`); err == nil {
		t.Fatal("artifact intent authority was truncatable")
	}
}

func poolSQLInsertClip(t *testing.T, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, fixture presentationV2Fixture, lease uuid.UUID, sequence int64, objectKey, sha string, version int) string {
	t.Helper()
	var endpoint, bucket string
	if err := pool.QueryRow(context.Background(), `SELECT endpoint,bucket FROM storage_destinations WHERE id=$1`, fixture.destinationID).Scan(&endpoint, &bucket); err != nil {
		t.Fatal(err)
	}
	return "INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,audio_present,fire_at,clip_start_at,clip_end_at,capture_lease_token,capture_sequence,surrender_transport_version) VALUES(" +
		strconv.FormatInt(fixture.recordingID, 10) + "," + strconv.FormatInt(fixture.jobID, 10) + "," + strconv.FormatInt(fixture.destinationID, 10) + "," +
		"'" + endpoint + "','" + bucket + "','" + objectKey + "','" + objectKey + "','video/mp4','mp4',1024,'etag','" + sha + "',60000,'h264',false,transaction_timestamp(),transaction_timestamp()+interval '" + strconv.FormatInt(sequence, 10) + " minutes',transaction_timestamp()+interval '" + strconv.FormatInt(sequence+1, 10) + " minutes','" + lease.String() + "'," + strconv.FormatInt(sequence, 10) + "," + strconv.Itoa(version) + ")"
}

func acceptV1Artifact(t *testing.T, pool *pgxpool.Pool, fixture presentationV2Fixture, lease uuid.UUID, producer, intent uuid.UUID, sequence int64, sha string) {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	segmentStart := time.Date(2026, time.August, 14, 12, int(sequence), 0, 0, time.UTC)
	if _, err = tx.Exec(ctx, `
		INSERT INTO recording_capture_artifact_seals
			(upload_intent_id,producer_id,capture_sequence,segment_start_ms,size_bytes,sha256)
		VALUES($1,$2,$3,$4,1024,$5)
	`, intent, producer, sequence, segmentStart.UnixMilli(), sha); err != nil {
		t.Fatal(err)
	}
	var clipID int64
	if err = tx.QueryRow(ctx, `
		INSERT INTO recording_clips
			(recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,display_path,
			 mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,audio_present,fire_at,
			 clip_start_at,clip_end_at,capture_lease_token,capture_sequence,surrender_transport_version)
		SELECT $2,$3,upload.storage_destination_id,upload.endpoint,upload.bucket,upload.object_key,upload.display_path,
		       upload.mime_type,'mp4',1024,'etag',$4,60000,'h264',false,transaction_timestamp(),
		       $5,$5+interval '1 minute',$6,$7,1
		FROM recording_upload_intents upload WHERE upload.id=$1
		RETURNING id
	`, intent, fixture.recordingID, fixture.jobID, sha, segmentStart, lease, sequence).Scan(&clipID); err != nil {
		t.Fatal(err)
	}
	var headVersion int64
	if err = tx.QueryRow(ctx, `
		UPDATE recording_job_unique_heads
		SET version=version+1,upload_intent_id=$3,clip_id=$4,capture_sequence=$5,advanced_at=transaction_timestamp()
		WHERE recording_job_id=$1 AND lease_token=$2
		RETURNING version
	`, fixture.jobID, lease, intent, clipID, sequence).Scan(&headVersion); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `
		UPDATE recording_upload_intents SET status='consumed' WHERE id=$1;
		INSERT INTO recording_capture_artifact_results(upload_intent_id,result,clip_id,head_version)
		VALUES($1,'accepted_unique',$2,$3)
	`, intent, clipID, headVersion); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func callProducerReserve(t *testing.T, server *Server, fixture presentationV2Fixture, lease uuid.UUID, worker string, producer uuid.UUID, ordinal int64) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(recordingCaptureProducerReserveRequest{ProducerID: producer.String(), CaptureOrdinal: ordinal, SealedIntentLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	request.Header.Set(recordingLeaseTokenHeader, lease.String())
	route := chi.NewRouteContext()
	route.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker}))
	response := httptest.NewRecorder()
	server.handleRecordingCaptureProducerReserve(response, request)
	return response
}

func callProducerFinish(t *testing.T, server *Server, fixture presentationV2Fixture, lease uuid.UUID, worker string, producer uuid.UUID, result, detail string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"result": result, "detail_class": detail})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(recordingLeaseTokenHeader, lease.String())
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker}))
	route := chi.NewRouteContext()
	route.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	route.URLParams.Add("producerId", producer.String())
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
	response := httptest.NewRecorder()
	server.handleRecordingCaptureProducerFinish(response, request)
	return response
}

func callArtifactReserve(t *testing.T, server *Server, fixture presentationV2Fixture, lease uuid.UUID, worker string, producer uuid.UUID, intents []uuid.UUID, secrets []string) *httptest.ResponseRecorder {
	t.Helper()
	artifacts := make([]map[string]any, len(intents))
	for index, intentID := range intents {
		secretBytes, err := hex.DecodeString(secrets[index])
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(secretBytes)
		artifacts[index] = map[string]any{"intent_id": intentID.String(), "recovery_secret_sha256": hex.EncodeToString(digest[:]), "capture_sequence": index + 5}
	}
	body, err := json.Marshal(map[string]any{"artifacts": artifacts})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	request.Header.Set(recordingLeaseTokenHeader, lease.String())
	route := chi.NewRouteContext()
	route.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	route.URLParams.Add("producerId", producer.String())
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker}))
	response := httptest.NewRecorder()
	server.handleRecordingCaptureArtifactsReserve(response, request)
	return response
}

func callSurrender(t *testing.T, server *Server, fixture presentationV2Fixture, lease uuid.UUID, worker, nodeType string, attempt uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(recordingJobSurrenderRequest{TransportVersion: 1, AttemptID: attempt.String(), Reason: recordingJobSurrenderNoProgress})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	request.Header.Set(recordingLeaseTokenHeader, lease.String())
	route := chi.NewRouteContext()
	route.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeType, DisplayName: worker}))
	response := httptest.NewRecorder()
	server.handleRecordingJobSurrender(response, request)
	return response
}

func callTransportObservation(t *testing.T, server *Server, fixture presentationV2Fixture, worker string, lease, observationID, attemptID uuid.UUID, requestSHA string, observedAt time.Time) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"observations": []map[string]any{{
		"id": observationID, "lease_token": lease, "attempt_id": attemptID,
		"type": "transport_budget_exhausted", "error_class": "transport_error",
		"observed_at": observedAt, "request_sha256": requestSHA,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	route := chi.NewRouteContext()
	route.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker}))
	response := httptest.NewRecorder()
	server.handleRecordingSurrenderTransportObservations(response, request)
	return response
}
