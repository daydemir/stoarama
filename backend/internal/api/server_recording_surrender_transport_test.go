package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordingapi"
	"github.com/daydemir/stoarama/backend/internal/secretbox"
	"github.com/daydemir/stoarama/backend/internal/surrenderplan"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeRecordingRecoveryObjectStore struct {
	mu          sync.Mutex
	objects     map[string][]byte
	metadata    map[string]map[string]string
	versions    map[string]string
	copyStarted chan struct{}
	copyGate    chan struct{}
}

func (f *fakeRecordingRecoveryObjectStore) PutReader(_ context.Context, key, _ string, body io.Reader) (string, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = data
	return "quarantine-etag", nil
}

func (f *fakeRecordingRecoveryObjectStore) Copy(ctx context.Context, sourceKey, destinationKey, contentType string, metadata map[string]string) (r2.ObjectHead, error) {
	if f.copyStarted != nil {
		select {
		case f.copyStarted <- struct{}{}:
		default:
		}
	}
	if f.copyGate != nil {
		select {
		case <-ctx.Done():
			return r2.ObjectHead{}, ctx.Err()
		case <-f.copyGate:
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[sourceKey]
	if !ok {
		return r2.ObjectHead{}, errors.New("quarantine object is absent")
	}
	f.objects[destinationKey] = append([]byte(nil), data...)
	if f.metadata == nil {
		f.metadata = make(map[string]map[string]string)
	}
	if f.versions == nil {
		f.versions = make(map[string]string)
	}
	f.metadata[destinationKey] = maps.Clone(metadata)
	f.versions[destinationKey] = "fake-version-1"
	return r2.ObjectHead{ETag: "promoted-etag", VersionID: "fake-version-1", Metadata: maps.Clone(metadata), ContentType: contentType}, nil
}

func (f *fakeRecordingRecoveryObjectStore) Head(_ context.Context, key string) (r2.ObjectHead, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[key]
	if !ok {
		return r2.ObjectHead{}, errors.New("object is absent")
	}
	etag := "fake-etag"
	if _, promoted := f.versions[key]; promoted {
		etag = "promoted-etag"
	}
	return r2.ObjectHead{SizeBytes: int64(len(data)), ETag: etag, VersionID: f.versions[key], Metadata: maps.Clone(f.metadata[key]), ContentType: "video/mp4"}, nil
}

func (f *fakeRecordingRecoveryObjectStore) DeleteObjects(_ context.Context, keys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range keys {
		delete(f.objects, key)
	}
	return nil
}

func TestRecoveryUploadFailureClassificationPreservesRetryTruth(t *testing.T) {
	expectedSHA := strings.Repeat("a", 64)
	for _, test := range []struct {
		name     string
		err      error
		observed int64
		sha      string
		want     string
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: "timeout"},
		{name: "disconnect", err: io.ErrUnexpectedEOF, observed: 2, want: "disconnect"},
		{name: "slow", err: errors.New("recovery upload fell below minimum rate"), observed: 2, want: "slow"},
		{name: "ambiguous after all bytes", err: errors.New("connection reset after response"), observed: 8, sha: expectedSHA, want: "response_ambiguous"},
		{name: "storage rejection", err: errors.New("503"), observed: 2, want: "storage_5xx"},
		{name: "verified transport but wrong bytes", observed: 8, sha: strings.Repeat("b", 64), want: "hash_mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := recoveryUploadFailureResult(test.err, test.observed, 8, test.sha, expectedSHA); got != test.want {
				t.Fatalf("result=%q want=%q", got, test.want)
			}
		})
	}
}

func TestRecoveryPromotionHeadRequiresExactProviderIdentity(t *testing.T) {
	metadata := recoveryPromotionMetadata(uuid.MustParse("3f3620b7-2ed9-4ad7-b515-8b0649aa6e70"), 2, uuid.MustParse("43c3ca88-ad46-4949-a32d-e8954b3707ca"), 17, strings.Repeat("a", 64))
	copied := r2.ObjectHead{ETag: "etag-1", VersionID: "version-1", ContentType: "video/mp4"}
	exact := r2.ObjectHead{ETag: "etag-1", VersionID: "version-1", ContentType: "video/mp4", SizeBytes: 17, Metadata: maps.Clone(metadata)}
	if !recoveryPromotionHeadExact(exact, copied, 17, "video/mp4", metadata) {
		t.Fatal("exact promoted provider identity was rejected")
	}
	mutations := []r2.ObjectHead{
		{ETag: "etag-1", VersionID: "version-1", ContentType: "video/mp4", SizeBytes: 16, Metadata: maps.Clone(metadata)},
		{ETag: "etag-2", VersionID: "version-1", ContentType: "video/mp4", SizeBytes: 17, Metadata: maps.Clone(metadata)},
		{ETag: "etag-1", VersionID: "version-2", ContentType: "video/mp4", SizeBytes: 17, Metadata: maps.Clone(metadata)},
		{ETag: "etag-1", VersionID: "version-1", ContentType: "application/octet-stream", SizeBytes: 17, Metadata: maps.Clone(metadata)},
		{ETag: "etag-1", VersionID: "version-1", ContentType: "video/mp4", SizeBytes: 17, Metadata: map[string]string{}},
	}
	extra := maps.Clone(metadata)
	extra["unbound"] = "metadata"
	mutations = append(mutations, r2.ObjectHead{ETag: "etag-1", VersionID: "version-1", ContentType: "video/mp4", SizeBytes: 17, Metadata: extra})
	for index, head := range mutations {
		if recoveryPromotionHeadExact(head, copied, 17, "video/mp4", metadata) {
			t.Fatalf("provider identity mutation %d was accepted", index)
		}
	}
}

func TestRecordingRecoveryProxyRejectsUnboundedBodiesBeforeStorage(t *testing.T) {
	intentID := uuid.New()
	recovery := recordingRecoveryPrincipal{Authority: "capture_set", IntentID: intentID}
	for _, contentLength := range []int64{-1, 0, surrenderplan.RecoveryArtifactMaxBytes + 1} {
		request := httptest.NewRequest(http.MethodPut, "/", bytes.NewReader([]byte("x")))
		request.ContentLength = contentLength
		request.Header.Set(recordingRecoverySessionHeader, uuid.NewString())
		request = request.WithContext(context.WithValue(request.Context(), recordingRecoveryContextKey{}, recovery))
		route := chi.NewRouteContext()
		route.URLParams.Add("intentId", intentID.String())
		request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
		response := httptest.NewRecorder()
		(&Server{}).handleRecordingRecoveryProxyUpload(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("content_length=%d status=%d body=%s", contentLength, response.Code, response.Body.String())
		}
	}
}

func TestRecordingSurrenderTransportPostgresLifecycleAndExpiryRecovery(t *testing.T) {
	pool, cleanup := testPresentationV2Pool(t)
	defer cleanup()
	ctx := context.Background()

	cloud := seedPresentationV2Task(t, pool, 91001, 991001)
	cloudLease := uuid.New()
	if _, err := pool.Exec(ctx, `
		UPDATE nodes SET node_type='local_recorder',display_name='cloud-recovery' WHERE id=$1;
		INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'cloud-recovery-r10',repeat('5',64));
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
	canonicalHashes := func(timezone, dateStyle string) (string, string) {
		t.Helper()
		conn, acquireErr := pool.Acquire(ctx)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		defer conn.Release()
		if _, setErr := conn.Exec(ctx, `SELECT set_config('TimeZone',$1,false),set_config('DateStyle',$2,false)`, timezone, dateStyle); setErr != nil {
			t.Fatal(setErr)
		}
		var sourceHash, configHash string
		if queryErr := conn.QueryRow(ctx, `
			SELECT encode(sha256(convert_to(recording_surrender_source_snapshot($1)::text,'UTF8')),'hex'),
			       encode(sha256(convert_to(recording_surrender_capture_config_snapshot($1,$2,$3)::text,'UTF8')),'hex')
		`, cloud.recordingID, cloud.jobID, cloudLease).Scan(&sourceHash, &configHash); queryErr != nil {
			t.Fatal(queryErr)
		}
		return sourceHash, configHash
	}
	utcSource, utcConfig := canonicalHashes("UTC", "ISO, MDY")
	localSource, localConfig := canonicalHashes("Pacific/Auckland", "SQL, DMY")
	if utcSource != sourceSnapshot || utcConfig != configSnapshot || localSource != utcSource || localConfig != utcConfig {
		t.Fatalf("canonical snapshot digests changed with session settings: stored=(%s,%s) utc=(%s,%s) local=(%s,%s)", sourceSnapshot, configSnapshot, utcSource, utcConfig, localSource, localConfig)
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
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET surrender_transport_version=0 WHERE capture_lease_token=$1 AND capture_sequence=5`, cloudLease); err == nil {
		t.Fatal("v1 clip transport provenance was mutable")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET sha256=repeat('c',64) WHERE capture_lease_token=$1 AND capture_sequence=5`, cloudLease); err == nil {
		t.Fatal("v1 clip artifact identity was mutable")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=transaction_timestamp()+interval '1 hour' WHERE capture_lease_token=$1 AND capture_sequence=5`, cloudLease); err == nil {
		t.Fatal("v1 clip purge time was caller-authored")
	}
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
	var relayTokenID int64
	if err = pool.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'relay-r10',repeat('3',64)) RETURNING id`, relay.nodeID).Scan(&relayTokenID); err != nil {
		t.Fatal(err)
	}
	relayLease, err := server.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: relay.nodeID, AccountID: relay.accountID, NodeType: nodeTypeRelay, NodeTokenID: relayTokenID}, true, 150, true)
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
	var relayExpiryTokenID int64
	if err = pool.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'relay-expiry-r10',repeat('4',64)) RETURNING id`, relayExpiry.nodeID).Scan(&relayExpiryTokenID); err != nil {
		t.Fatal(err)
	}
	relayExpiredLease, err := server.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: relayExpiry.nodeID, AccountID: relayExpiry.accountID, NodeType: nodeTypeRelay, NodeTokenID: relayExpiryTokenID}, true, 150, true)
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
	relayReclaimedLease, err := server.leaseRelayRecordingJob(ctx, nodePrincipal{NodeID: relayExpiry.nodeID, AccountID: relayExpiry.accountID, NodeType: nodeTypeRelay, NodeTokenID: relayExpiryTokenID}, true, 150, true)
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

func TestRecordingSurrenderTransportR10PostgresAuthorityAndRaces(t *testing.T) {
	pool, cleanup := testPresentationV2Pool(t)
	defer cleanup()
	ctx := context.Background()
	fixture := seedPresentationV2Task(t, pool, 92001, 992001)
	if _, err := pool.Exec(ctx, `
		UPDATE nodes SET node_type='relay',display_name='r10-relay' WHERE id=$1;
		UPDATE recordings SET capture_via='relay' WHERE id=$2;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL,
		 scheduled_for=transaction_timestamp()-interval '1 minute' WHERE id=$3
	`, fixture.nodeID, fixture.recordingID, fixture.jobID); err != nil {
		t.Fatal(err)
	}
	var frozenSourceRevisionID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO stream_source_revisions(stream_id,actor,reason,previous_source_url,new_source_url)
		SELECT id,'test','r10_frozen_baseline',source_url,source_url FROM streams WHERE id=$1
		RETURNING id
	`, fixture.streamID).Scan(&frozenSourceRevisionID); err != nil {
		t.Fatal(err)
	}
	var oldTokenID int64
	if err := pool.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'r10-old',repeat('6',64)) RETURNING id`, fixture.nodeID).Scan(&oldTokenID); err != nil {
		t.Fatal(err)
	}
	secretsCipher, err := secretbox.NewFromBase64Key("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal(err)
	}
	sealedDestinationSecret, err := secretsCipher.Encrypt([]byte("r10-test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE storage_destinations SET access_key_id='r10-test-access',secret_access_key_enc=$2 WHERE id=$1`, fixture.destinationID, sealedDestinationSecret); err != nil {
		t.Fatal(err)
	}
	fakeStore := &fakeRecordingRecoveryObjectStore{objects: make(map[string][]byte)}
	server := &Server{pool: pool, secrets: secretsCipher, recoveryStorageFactory: func(context.Context, r2.Config) (recordingRecoveryObjectStore, error) {
		return fakeStore, nil
	}}
	// A source stop can catch FFmpeg after it opens a leaf but before the first
	// byte. The exact live-fence ACK is sufficient DB authority to terminalize
	// that immutable zero-size inode; it must not wait for lease expiry or create
	// recovery grants, admission blocks, successor rotation, or a requeue.
	zeroFixture := seedPresentationV2Task(t, pool, 92006, 992006)
	if _, err = pool.Exec(ctx, `
		UPDATE nodes SET node_type='relay',display_name='r10-zero-current' WHERE id=$1;
		UPDATE recordings SET capture_via='relay' WHERE id=$2;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_expires_at=NULL,lease_token=NULL,
		 scheduled_for=transaction_timestamp()-interval '1 minute' WHERE id=$3
	`, zeroFixture.nodeID, zeroFixture.recordingID, zeroFixture.jobID); err != nil {
		t.Fatal(err)
	}
	var zeroTokenID int64
	if err = pool.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'r10-zero',repeat('3',64)) RETURNING id`, zeroFixture.nodeID).Scan(&zeroTokenID); err != nil {
		t.Fatal(err)
	}
	zeroPrincipal := nodePrincipal{NodeID: zeroFixture.nodeID, AccountID: zeroFixture.accountID, NodeType: nodeTypeRelay, DisplayName: "r10-zero-current", NodeTokenID: zeroTokenID}
	zeroLease, err := server.leaseRelayRecordingJob(ctx, zeroPrincipal, true, 150, true)
	if err != nil || zeroLease.LeaseToken == nil || zeroLease.SurrenderTransportVersion != 1 {
		t.Fatalf("zero current lease=%+v err=%v", zeroLease, err)
	}
	zeroPlanID, zeroSetID, zeroProducerID := uuid.New(), uuid.New(), uuid.New()
	zeroPlanBody, _ := json.Marshal(map[string]any{
		"plan_id": zeroPlanID, "set_id": zeroSetID, "producer_id": zeroProducerID,
		"capture_ordinal": 1, "first_capture_sequence": 1,
	})
	zeroPlanRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(zeroPlanBody))
	zeroPlanRequest.Header.Set(recordingLeaseTokenHeader, zeroLease.LeaseToken.String())
	zeroPlanRequest = zeroPlanRequest.WithContext(context.WithValue(zeroPlanRequest.Context(), nodePrincipalContextKey, zeroPrincipal))
	zeroPlanRoute := chi.NewRouteContext()
	zeroPlanRoute.URLParams.Add("id", strconv.FormatInt(zeroFixture.jobID, 10))
	zeroPlanRequest = zeroPlanRequest.WithContext(context.WithValue(zeroPlanRequest.Context(), chi.RouteCtxKey, zeroPlanRoute))
	zeroPlanResponse := httptest.NewRecorder()
	server.handleRecordingCaptureSetPlan(zeroPlanResponse, zeroPlanRequest)
	if zeroPlanResponse.Code != http.StatusOK {
		t.Fatalf("zero current plan=%d body=%s", zeroPlanResponse.Code, zeroPlanResponse.Body.String())
	}
	var zeroPlan recordingapi.CaptureSetPlan
	if err = json.Unmarshal(zeroPlanResponse.Body.Bytes(), &zeroPlan); err != nil {
		t.Fatal(err)
	}
	zeroIdentity := surrenderplan.SetIdentity{
		SetID: zeroSetID, AccountID: zeroPlan.AccountID, RecordingID: zeroPlan.RecordingID, JobID: zeroPlan.JobID,
		LeaseToken: *zeroLease.LeaseToken, OriginClaimGeneration: zeroPlan.OriginClaimGeneration,
		ProducerID: zeroProducerID, SnapshotSHA256: zeroPlan.SourceSnapshotSHA256,
		DestinationNamingSHA256: zeroPlan.DestinationNamingSHA256, ArtifactCount: zeroPlan.ArtifactCount,
		MIME: "video/mp4", MaxBytes: surrenderplan.RecoveryArtifactMaxBytes,
	}
	zeroSeed := sha256.Sum256([]byte("r10-current-fence-zero-byte"))
	zeroTree, err := surrenderplan.BuildTree(zeroSeed, zeroIdentity)
	if err != nil {
		t.Fatal(err)
	}
	zeroRoot := zeroTree.Root()
	zeroCommitBody, _ := json.Marshal(map[string]any{"merkle_root_sha256": hex.EncodeToString(zeroRoot[:])})
	zeroCommitRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(zeroCommitBody))
	zeroCommitRequest.Header.Set(recordingLeaseTokenHeader, zeroLease.LeaseToken.String())
	zeroCommitRequest = zeroCommitRequest.WithContext(context.WithValue(zeroCommitRequest.Context(), nodePrincipalContextKey, zeroPrincipal))
	zeroCommitRoute := chi.NewRouteContext()
	zeroCommitRoute.URLParams.Add("id", strconv.FormatInt(zeroFixture.jobID, 10))
	zeroCommitRoute.URLParams.Add("planId", zeroPlanID.String())
	zeroCommitRequest = zeroCommitRequest.WithContext(context.WithValue(zeroCommitRequest.Context(), chi.RouteCtxKey, zeroCommitRoute))
	zeroCommitResponse := httptest.NewRecorder()
	server.handleRecordingCaptureSetCommit(zeroCommitResponse, zeroCommitRequest)
	if zeroCommitResponse.Code != http.StatusNoContent {
		t.Fatalf("zero current commit=%d body=%s", zeroCommitResponse.Code, zeroCommitResponse.Body.String())
	}
	if _, err = pool.Exec(ctx, `UPDATE streams SET source_url=source_url||'?r10-zero-stop=1' WHERE id=$1`, zeroFixture.streamID); err != nil {
		t.Fatal(err)
	}
	zeroArtifact, err := surrenderplan.DeriveArtifact(zeroSeed, zeroIdentity, 1)
	if err != nil {
		t.Fatal(err)
	}
	zeroProof, err := zeroTree.Proof(1)
	if err != nil {
		t.Fatal(err)
	}
	zeroProofValues := make([]string, len(zeroProof.Siblings))
	for index := range zeroProof.Siblings {
		zeroProofValues[index] = hex.EncodeToString(zeroProof.Siblings[index][:])
	}
	zeroACK := recordingapi.CaptureStopAck{
		AckID: uuid.NewString(), RetainedDirectoryDevice: 51, RetainedDirectoryInode: 52,
		Members: []recordingapi.CaptureStopAckMember{{
			Ordinal: 1, ArtifactID: zeroArtifact.ID.String(), CaptureSequence: 1,
			RecoverySecretSHA256: hex.EncodeToString(zeroArtifact.RecoverySecretHash[:]), Proof: zeroProofValues,
			Device: 51, Inode: 53, SizeBytes: 0, RelativeName: "seg-20260814-070000.mp4",
		}},
	}
	zeroACK.InventorySHA256, err = recordingapi.CaptureStopInventorySHA(zeroACK)
	if err != nil {
		t.Fatal(err)
	}
	zeroACKBody, _ := json.Marshal(zeroACK)
	zeroACKRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(zeroACKBody))
	zeroACKRequest.Header.Set(recordingLeaseTokenHeader, zeroLease.LeaseToken.String())
	zeroACKRequest = zeroACKRequest.WithContext(context.WithValue(zeroACKRequest.Context(), nodePrincipalContextKey, zeroPrincipal))
	zeroACKRoute := chi.NewRouteContext()
	zeroACKRoute.URLParams.Add("id", strconv.FormatInt(zeroFixture.jobID, 10))
	zeroACKRoute.URLParams.Add("setId", zeroSetID.String())
	zeroACKRequest = zeroACKRequest.WithContext(context.WithValue(zeroACKRequest.Context(), chi.RouteCtxKey, zeroACKRoute))
	zeroACKResponse := httptest.NewRecorder()
	server.handleRecordingCaptureSetStopAck(zeroACKResponse, zeroACKRequest)
	if zeroACKResponse.Code != http.StatusOK {
		t.Fatalf("zero current stop ack=%d body=%s", zeroACKResponse.Code, zeroACKResponse.Body.String())
	}
	var zeroACKResult recordingapi.CaptureStopAckResult
	if err = json.Unmarshal(zeroACKResponse.Body.Bytes(), &zeroACKResult); err != nil || len(zeroACKResult.NoBytesOrdinals) != 1 || zeroACKResult.NoBytesOrdinals[0] != 1 {
		t.Fatalf("zero current stop result=%+v err=%v", zeroACKResult, err)
	}
	zeroFinishRequest := httptest.NewRequest(http.MethodPost, "/", nil)
	zeroFinishRequest.Header.Set(recordingLeaseTokenHeader, zeroLease.LeaseToken.String())
	zeroFinishRequest = zeroFinishRequest.WithContext(context.WithValue(zeroFinishRequest.Context(), nodePrincipalContextKey, zeroPrincipal))
	zeroFinishRoute := chi.NewRouteContext()
	zeroFinishRoute.URLParams.Add("id", strconv.FormatInt(zeroFixture.jobID, 10))
	zeroFinishRoute.URLParams.Add("setId", zeroSetID.String())
	zeroFinishRequest = zeroFinishRequest.WithContext(context.WithValue(zeroFinishRequest.Context(), chi.RouteCtxKey, zeroFinishRoute))
	zeroFinishResponse := httptest.NewRecorder()
	server.handleRecordingCaptureSetFinish(zeroFinishResponse, zeroFinishRequest)
	if zeroFinishResponse.Code != http.StatusNoContent {
		t.Fatalf("zero current finish=%d body=%s", zeroFinishResponse.Code, zeroFinishResponse.Body.String())
	}
	var zeroResult, zeroJobStatus, zeroHeadState string
	var zeroGrants, zeroExpiries, zeroSuccessors int
	var zeroCurrentLease bool
	if err = pool.QueryRow(ctx, `
		SELECT result.result,job.status,job.lease_token=$3 AND job.lease_expires_at>transaction_timestamp(),head.state,
		       (SELECT count(*) FROM recording_capture_set_grants grant WHERE grant.set_id=$1),
		       (SELECT count(*) FROM recording_job_lease_expiry_events expiry WHERE expiry.recording_job_id=$2 AND expiry.lease_token=$3),
		       (SELECT count(*) FROM recording_worker_claim_successor_proposals successor WHERE successor.node_id=$4)
		FROM recording_capture_artifact_grant_results result
		JOIN recording_jobs job ON job.id=$2
		JOIN recording_worker_claim_heads head ON head.node_id=$4
		WHERE result.set_id=$1 AND result.ordinal=1
	`, zeroSetID, zeroFixture.jobID, zeroLease.LeaseToken, zeroFixture.nodeID).Scan(&zeroResult, &zeroJobStatus, &zeroCurrentLease, &zeroHeadState, &zeroGrants, &zeroExpiries, &zeroSuccessors); err != nil {
		t.Fatal(err)
	}
	if zeroResult != "acknowledged_no_bytes" || zeroJobStatus != "leased" || !zeroCurrentLease || zeroHeadState != "enabled" || zeroGrants != 0 || zeroExpiries != 0 || zeroSuccessors != 0 {
		t.Fatalf("zero current result=%q job=%q current=%v head=%q grants=%d expiries=%d successors=%d", zeroResult, zeroJobStatus, zeroCurrentLease, zeroHeadState, zeroGrants, zeroExpiries, zeroSuccessors)
	}
	// Compact planning takes the same source/claim fence as mutations. A writer
	// that wins first yields one new canonical plan; a mutation after the plan
	// makes commitment fail rather than launching with a mixed snapshot.
	planRace := seedPresentationV2Task(t, pool, 92003, 992003)
	if _, err = pool.Exec(ctx, `
		UPDATE nodes SET node_type='relay',display_name='r10-plan-race' WHERE id=$1;
		UPDATE recordings SET capture_via='relay' WHERE id=$2;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
		 scheduled_for=transaction_timestamp()-interval '1 minute' WHERE id=$3
	`, planRace.nodeID, planRace.recordingID, planRace.jobID); err != nil {
		t.Fatal(err)
	}
	var planRaceTokenID int64
	if err = pool.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'r10-plan-race',repeat('4',64)) RETURNING id`, planRace.nodeID).Scan(&planRaceTokenID); err != nil {
		t.Fatal(err)
	}
	planRacePrincipal := nodePrincipal{NodeID: planRace.nodeID, AccountID: planRace.accountID, NodeType: nodeTypeRelay, DisplayName: "r10-plan-race", NodeTokenID: planRaceTokenID}
	planRaceLease, err := server.leaseRelayRecordingJob(ctx, planRacePrincipal, true, 150, true)
	if err != nil || planRaceLease.LeaseToken == nil {
		t.Fatalf("plan race lease=%+v err=%v", planRaceLease, err)
	}
	callPlan := func(planID, setID, producerID uuid.UUID) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]any{"plan_id": planID, "set_id": setID, "producer_id": producerID, "capture_ordinal": 1, "first_capture_sequence": 1})
		request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		request.Header.Set(recordingLeaseTokenHeader, planRaceLease.LeaseToken.String())
		request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, planRacePrincipal))
		route := chi.NewRouteContext()
		route.URLParams.Add("id", strconv.FormatInt(planRace.jobID, 10))
		request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
		response := httptest.NewRecorder()
		server.handleRecordingCaptureSetPlan(response, request)
		return response
	}
	sourceTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sourceTx.Exec(ctx, `UPDATE streams SET source_url=source_url||'?writer-first=1' WHERE id=$1`, planRace.streamID); err != nil {
		t.Fatal(err)
	}
	racePlanID, raceSetID, raceProducerID := uuid.New(), uuid.New(), uuid.New()
	planDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { planDone <- callPlan(racePlanID, raceSetID, raceProducerID) }()
	select {
	case response := <-planDone:
		t.Fatalf("compact plan crossed an uncommitted source mutation: %d %s", response.Code, response.Body.String())
	case <-time.After(100 * time.Millisecond):
	}
	if err = sourceTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	planRaceResponse := <-planDone
	if planRaceResponse.Code != http.StatusOK {
		t.Fatalf("compact plan after source commit=%d body=%s", planRaceResponse.Code, planRaceResponse.Body.String())
	}
	if _, err = pool.Exec(ctx, `UPDATE streams SET source_url=source_url||'&after-plan=1' WHERE id=$1`, planRace.streamID); err != nil {
		t.Fatal(err)
	}
	commitRaceBody, _ := json.Marshal(map[string]any{"merkle_root_sha256": strings.Repeat("a", 64)})
	commitRaceRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(commitRaceBody))
	commitRaceRequest.Header.Set(recordingLeaseTokenHeader, planRaceLease.LeaseToken.String())
	commitRaceRequest = commitRaceRequest.WithContext(context.WithValue(commitRaceRequest.Context(), nodePrincipalContextKey, planRacePrincipal))
	commitRaceRoute := chi.NewRouteContext()
	commitRaceRoute.URLParams.Add("id", strconv.FormatInt(planRace.jobID, 10))
	commitRaceRoute.URLParams.Add("planId", racePlanID.String())
	commitRaceRequest = commitRaceRequest.WithContext(context.WithValue(commitRaceRequest.Context(), chi.RouteCtxKey, commitRaceRoute))
	commitRaceResponse := httptest.NewRecorder()
	server.handleRecordingCaptureSetCommit(commitRaceResponse, commitRaceRequest)
	if commitRaceResponse.Code != http.StatusConflict {
		t.Fatalf("compact commitment accepted a stale source plan: %d %s", commitRaceResponse.Code, commitRaceResponse.Body.String())
	}
	principal := nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeRelay, DisplayName: "r10-relay", NodeTokenID: oldTokenID}
	lease, err := server.leaseRelayRecordingJob(ctx, principal, true, 150, true)
	if err != nil || lease.LeaseToken == nil || lease.SurrenderTransportVersion != 1 {
		t.Fatalf("lease=%+v err=%v", lease, err)
	}
	planID, setID, producerID := uuid.New(), uuid.New(), uuid.New()
	planBody, _ := json.Marshal(map[string]any{
		"plan_id": planID, "set_id": setID, "producer_id": producerID,
		"capture_ordinal": 1, "first_capture_sequence": 1,
	})
	planRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(planBody))
	planRequest.Header.Set(recordingLeaseTokenHeader, lease.LeaseToken.String())
	planRequest = planRequest.WithContext(context.WithValue(planRequest.Context(), nodePrincipalContextKey, principal))
	planRoute := chi.NewRouteContext()
	planRoute.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	planRequest = planRequest.WithContext(context.WithValue(planRequest.Context(), chi.RouteCtxKey, planRoute))
	planResponse := httptest.NewRecorder()
	server.handleRecordingCaptureSetPlan(planResponse, planRequest)
	if planResponse.Code != http.StatusOK {
		t.Fatalf("plan=%d body=%s", planResponse.Code, planResponse.Body.String())
	}
	var plan recordingapi.CaptureSetPlan
	if err = json.Unmarshal(planResponse.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	var destinationLifetimeRestricted bool
	if err = pool.QueryRow(ctx, `
		SELECT EXISTS(
		 SELECT 1 FROM pg_constraint
		 WHERE conrelid='recording_capture_set_plans'::regclass
		   AND confrelid='storage_destinations'::regclass AND confdeltype='r'
		   AND conkey=ARRAY[(SELECT attnum FROM pg_attribute
		                       WHERE attrelid='recording_capture_set_plans'::regclass
		                         AND attname='storage_destination_id')]::smallint[])
	`).Scan(&destinationLifetimeRestricted); err != nil || !destinationLifetimeRestricted {
		t.Fatalf("frozen capture destination lacks a restrictive lifetime FK: %v", err)
	}
	identity := surrenderplan.SetIdentity{
		SetID: setID, AccountID: plan.AccountID, RecordingID: plan.RecordingID, JobID: plan.JobID,
		LeaseToken: *lease.LeaseToken, OriginClaimGeneration: plan.OriginClaimGeneration,
		ProducerID: producerID, SnapshotSHA256: plan.SourceSnapshotSHA256,
		DestinationNamingSHA256: plan.DestinationNamingSHA256, ArtifactCount: plan.ArtifactCount,
		MIME: "video/mp4", MaxBytes: surrenderplan.RecoveryArtifactMaxBytes,
	}
	var seed [32]byte
	for index := range seed {
		seed[index] = byte(index + 1)
	}
	tree, err := surrenderplan.BuildTree(seed, identity)
	if err != nil {
		t.Fatal(err)
	}
	root := tree.Root()
	commitBody, _ := json.Marshal(map[string]any{"merkle_root_sha256": hex.EncodeToString(root[:])})
	commitRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(commitBody))
	commitRequest.Header.Set(recordingLeaseTokenHeader, lease.LeaseToken.String())
	commitRequest = commitRequest.WithContext(context.WithValue(commitRequest.Context(), nodePrincipalContextKey, principal))
	commitRoute := chi.NewRouteContext()
	commitRoute.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	commitRoute.URLParams.Add("planId", planID.String())
	commitRequest = commitRequest.WithContext(context.WithValue(commitRequest.Context(), chi.RouteCtxKey, commitRoute))
	commitResponse := httptest.NewRecorder()
	server.handleRecordingCaptureSetCommit(commitResponse, commitRequest)
	if commitResponse.Code != http.StatusNoContent {
		t.Fatalf("commit=%d body=%s", commitResponse.Code, commitResponse.Body.String())
	}
	if _, err = pool.Exec(ctx, `
		INSERT INTO recording_capture_producer_stop_events(id,set_id,old_snapshot_generation,new_snapshot_generation)
		SELECT gen_random_uuid(),capture_set.id,plan.snapshot_generation,plan.snapshot_generation+1
		FROM recording_capture_reservation_sets capture_set
		JOIN recording_capture_set_plans plan ON plan.id=capture_set.plan_id
		WHERE capture_set.id=$1
	`, setID); err == nil {
		t.Fatal("caller forged a producer stop without a changed source snapshot")
	}
	artifact, err := surrenderplan.DeriveArtifact(seed, identity, 1)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := tree.Proof(1)
	if err != nil {
		t.Fatal(err)
	}
	proofValues := make([]string, len(proof.Siblings))
	for index := range proof.Siblings {
		proofValues[index] = hex.EncodeToString(proof.Siblings[index][:])
	}
	secretSHA := hex.EncodeToString(artifact.RecoverySecretHash[:])
	secondArtifact, err := surrenderplan.DeriveArtifact(seed, identity, 2)
	if err != nil {
		t.Fatal(err)
	}
	secondProof, err := tree.Proof(2)
	if err != nil {
		t.Fatal(err)
	}
	secondProofValues := make([]string, len(secondProof.Siblings))
	for index := range secondProof.Siblings {
		secondProofValues[index] = hex.EncodeToString(secondProof.Siblings[index][:])
	}
	thirdArtifact, err := surrenderplan.DeriveArtifact(seed, identity, 3)
	if err != nil {
		t.Fatal(err)
	}
	thirdProof, err := tree.Proof(3)
	if err != nil {
		t.Fatal(err)
	}
	thirdProofValues := make([]string, len(thirdProof.Siblings))
	thirdProofHash := sha256.New()
	_, _ = thirdProofHash.Write([]byte("stoarama.recording.capture-artifact-proof.v1\x00"))
	for index := range thirdProof.Siblings {
		thirdProofValues[index] = hex.EncodeToString(thirdProof.Siblings[index][:])
		_, _ = thirdProofHash.Write(thirdProof.Siblings[index][:])
	}
	if _, err = pool.Exec(ctx, `UPDATE streams SET source_url=source_url||'?r10-stop=1' WHERE id=$1`, fixture.streamID); err != nil {
		t.Fatal(err)
	}
	thirdProofJSON, _ := json.Marshal(thirdProofValues)
	if _, err = pool.Exec(ctx, `
		INSERT INTO recording_capture_materialized_artifacts
			(set_id,ordinal,artifact_id,recovery_secret_sha256,capture_sequence,proof,proof_sha256)
		VALUES($1,3,$2,$3,3,$4,$5)
	`, setID, thirdArtifact.ID, hex.EncodeToString(thirdArtifact.RecoverySecretHash[:]), thirdProofJSON, hex.EncodeToString(thirdProofHash.Sum(nil))); err == nil {
		t.Fatal("post-stop artifact outside an ACK inventory materialized")
	}
	emptyCoverage := map[string]any{"artifact_count": plan.ArtifactCount, "materialized_ordinals": []int{}, "unused_ranges": [][2]int{{1, plan.ArtifactCount}}}
	emptyCoverageJSON, _ := json.Marshal(emptyCoverage)
	if _, err = pool.Exec(ctx, `
		INSERT INTO recording_capture_set_results(set_id,result,coverage_ranges,coverage_sha256)
		VALUES($1,'abandoned',$2,encode(sha256(convert_to($2::jsonb::text,'UTF8')),'hex'))
	`, setID, emptyCoverageJSON); err == nil {
		t.Fatal("stopped capture set sealed without its exact stop ACK")
	}
	ack := recordingapi.CaptureStopAck{
		AckID: uuid.NewString(), RetainedDirectoryDevice: 41, RetainedDirectoryInode: 42,
		Members: []recordingapi.CaptureStopAckMember{{
			Ordinal: 1, ArtifactID: artifact.ID.String(), CaptureSequence: 1,
			RecoverySecretSHA256: secretSHA, Proof: proofValues,
			Device: 41, Inode: 43, SizeBytes: 100, RelativeName: "seg-20260814-070000.mp4",
		}, {
			Ordinal: 2, ArtifactID: secondArtifact.ID.String(), CaptureSequence: 2,
			RecoverySecretSHA256: hex.EncodeToString(secondArtifact.RecoverySecretHash[:]), Proof: secondProofValues,
			Device: 41, Inode: 44, SizeBytes: 100, RelativeName: "seg-20260814-070001.mp4",
		}},
	}
	ack.InventorySHA256, err = recordingapi.CaptureStopInventorySHA(ack)
	if err != nil {
		t.Fatal(err)
	}
	peerNodeID := fixture.nodeID + 500000
	if _, err = pool.Exec(ctx, `INSERT INTO nodes(id,account_id,display_name,node_type,status,last_heartbeat_at,relay_max_streams) VALUES($1,$2,'r10-relay-peer','relay','active',transaction_timestamp(),4)`, peerNodeID, fixture.accountID); err != nil {
		t.Fatal(err)
	}
	var peerTokenID int64
	if err = pool.QueryRow(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,'r10-peer',repeat('7',64)) RETURNING id`, peerNodeID).Scan(&peerTokenID); err != nil {
		t.Fatal(err)
	}
	// Keep unrelated exact predecessor fences live before recovery blocks new
	// claims. They prove the successor can heartbeat, complete, and fail old
	// fences immediately while taking new work, without revoking the predecessor
	// prematurely.
	unrelated := seedPresentationV2Task(t, pool, 92002, fixture.accountID)
	unrelatedLease := uuid.New()
	if _, err = pool.Exec(ctx, `
		UPDATE recordings SET capture_via='relay' WHERE id=$1;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL WHERE id=$2;
		UPDATE recording_jobs SET status='leased',lease_owner=$3,lease_token=$4,
		 lease_expires_at=transaction_timestamp()+interval '2 minutes',attempt_count=attempt_count+1,
		 lease_node_token_id=$5,lease_claim_generation=1,lease_credential_state='exact'
		 WHERE id=$2
	`, unrelated.recordingID, unrelated.jobID, "node:"+strconv.FormatInt(fixture.nodeID, 10), unrelatedLease, oldTokenID); err != nil {
		t.Fatal(err)
	}
	unrelatedFail := seedPresentationV2Task(t, pool, 92005, fixture.accountID)
	unrelatedFailLease := uuid.New()
	if _, err = pool.Exec(ctx, `
		UPDATE recordings SET capture_via='relay' WHERE id=$1;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL WHERE id=$2;
		UPDATE recording_jobs SET status='leased',lease_owner=$3,lease_token=$4,
		 lease_expires_at=transaction_timestamp()+interval '2 minutes',attempt_count=attempt_count+1,
		 lease_node_token_id=$5,lease_claim_generation=1,lease_credential_state='exact'
		 WHERE id=$2
	`, unrelatedFail.recordingID, unrelatedFail.jobID, "node:"+strconv.FormatInt(fixture.nodeID, 10), unrelatedFailLease, oldTokenID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_jobs SET lease_expires_at=transaction_timestamp()-interval '1 second' WHERE id=$1`, fixture.jobID); err != nil {
		t.Fatal(err)
	}
	var reclaimed int64
	if err = pool.QueryRow(ctx, `SELECT recording_surrender_reclaim_expired()`).Scan(&reclaimed); err != nil || reclaimed < 1 {
		t.Fatalf("reclaimed=%d err=%v", reclaimed, err)
	}
	if _, err = pool.Exec(ctx, `
		UPDATE recording_jobs SET status='leased',lease_owner=$2,lease_token=gen_random_uuid(),
		 lease_expires_at=transaction_timestamp()+interval '2 minutes'
		WHERE id=$1
	`, fixture.jobID, "node:"+strconv.FormatInt(fixture.nodeID, 10)); err == nil {
		t.Fatal("rollback/v0 claim bypassed the recovery-blocked claim head")
	}
	var alternate bool
	if err = pool.QueryRow(ctx, `SELECT alternate_available FROM recording_job_lease_expiry_events WHERE recording_job_id=$1 AND lease_token=$2`, fixture.jobID, lease.LeaseToken).Scan(&alternate); err != nil || !alternate {
		t.Fatalf("relay alternate=%v err=%v", alternate, err)
	}
	// Model the worker crash after fsyncing the stop ACK but before receiving
	// its response. The lease has now expired and been reclaimed, so replay must
	// be authorized by the exact set grant and immutable origin generation—not
	// by mutable current job ownership.
	ackBody, _ := json.Marshal(ack)
	wrongPrincipal := principal
	wrongPrincipal.NodeID, wrongPrincipal.NodeTokenID = peerNodeID, peerTokenID
	wrongRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(ackBody))
	wrongRequest.Header.Set(recordingLeaseTokenHeader, lease.LeaseToken.String())
	wrongRequest = wrongRequest.WithContext(context.WithValue(wrongRequest.Context(), nodePrincipalContextKey, wrongPrincipal))
	wrongRoute := chi.NewRouteContext()
	wrongRoute.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	wrongRoute.URLParams.Add("setId", setID.String())
	wrongRequest = wrongRequest.WithContext(context.WithValue(wrongRequest.Context(), chi.RouteCtxKey, wrongRoute))
	wrongResponse := httptest.NewRecorder()
	server.handleRecordingCaptureSetStopAck(wrongResponse, wrongRequest)
	if wrongResponse.Code != http.StatusConflict {
		t.Fatalf("cross-node post-expiry stop ack=%d body=%s", wrongResponse.Code, wrongResponse.Body.String())
	}
	ackRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(ackBody))
	ackRequest.Header.Set(recordingLeaseTokenHeader, lease.LeaseToken.String())
	ackRequest = ackRequest.WithContext(context.WithValue(ackRequest.Context(), nodePrincipalContextKey, principal))
	ackRoute := chi.NewRouteContext()
	ackRoute.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	ackRoute.URLParams.Add("setId", setID.String())
	ackRequest = ackRequest.WithContext(context.WithValue(ackRequest.Context(), chi.RouteCtxKey, ackRoute))
	ackResponse := httptest.NewRecorder()
	server.handleRecordingCaptureSetStopAck(ackResponse, ackRequest)
	if ackResponse.Code != http.StatusOK {
		t.Fatalf("post-expiry stop ack=%d body=%s", ackResponse.Code, ackResponse.Body.String())
	}
	newRaw := "sin_" + strings.Repeat("n", 48)
	newHash := sha256.Sum256([]byte(newRaw))
	proposalID := uuid.New()
	proposalBody, _ := json.Marshal(recordingClaimSuccessorProposalRequest{ProposalID: proposalID.String(), KeyPrefix: newRaw[:16], SecretSHA256: hex.EncodeToString(newHash[:])})
	proposalRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(proposalBody))
	proposalRequest = proposalRequest.WithContext(context.WithValue(proposalRequest.Context(), nodePrincipalContextKey, principal))
	proposalResponse := httptest.NewRecorder()
	server.handleRecordingClaimSuccessorPropose(proposalResponse, proposalRequest)
	if proposalResponse.Code != http.StatusTooEarly {
		t.Fatalf("successor before terminal recovery=%d body=%s", proposalResponse.Code, proposalResponse.Body.String())
	}
	if _, err = pool.Exec(ctx, `UPDATE storage_destinations SET endpoint='https://changed-after-capture.invalid' WHERE id=$1`, fixture.destinationID); err != nil {
		t.Fatal(err)
	}
	secretHex := hex.EncodeToString(artifact.RecoverySecret[:])
	authRequest := httptest.NewRequest(http.MethodPost, "/", nil)
	authRequest.Header.Set(recordingRecoveryIntentHeader, artifact.ID.String())
	authRequest.Header.Set(recordingRecoverySecretHeader, secretHex)
	recovery, err := server.authenticateRecordingRecovery(authRequest)
	if err != nil || recovery.Authority != "capture_set" || recovery.SetID != setID {
		t.Fatalf("recovery=%+v err=%v", recovery, err)
	}
	recoveryNode := nodePrincipal{NodeID: recovery.NodeID, AccountID: recovery.AccountID, NodeType: recovery.NodeType, DisplayName: recovery.WorkerID}
	payload := []byte("verified recovery payload")
	payloadSHA := sha256.Sum256(payload)
	segmentStart := time.Date(2026, time.August, 14, 7, 0, 0, 500_000, time.UTC)
	sealBody, _ := json.Marshal(recordingCaptureArtifactSealRequest{
		JobID: fixture.jobID, ProducerID: producerID.String(), CaptureSequence: 1,
		SetID: setID.String(), Ordinal: 1, SegmentStartUS: segmentStart.UnixMicro(),
		SizeBytes: int64(len(payload)), SHA256: hex.EncodeToString(payloadSHA[:]),
	})
	sealRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(sealBody))
	sealRequest = sealRequest.WithContext(context.WithValue(sealRequest.Context(), nodePrincipalContextKey, recoveryNode))
	sealRequest = sealRequest.WithContext(context.WithValue(sealRequest.Context(), recordingRecoveryContextKey{}, recovery))
	sealRoute := chi.NewRouteContext()
	sealRoute.URLParams.Add("intentId", artifact.ID.String())
	sealRequest = sealRequest.WithContext(context.WithValue(sealRequest.Context(), chi.RouteCtxKey, sealRoute))
	sealResponse := httptest.NewRecorder()
	server.handleRecordingCaptureArtifactSeal(sealResponse, sealRequest)
	if sealResponse.Code != http.StatusOK || strings.Contains(sealResponse.Body.String(), "upload_url") {
		t.Fatalf("recovery seal=%d body=%s", sealResponse.Code, sealResponse.Body.String())
	}
	var frozenIntentEndpoint, currentDestinationEndpoint string
	if err = pool.QueryRow(ctx, `
		SELECT intent.endpoint,destination.endpoint
		FROM recording_upload_intents intent JOIN storage_destinations destination ON destination.id=intent.storage_destination_id
		WHERE intent.id=$1
	`, artifact.ID).Scan(&frozenIntentEndpoint, &currentDestinationEndpoint); err != nil || frozenIntentEndpoint == currentDestinationEndpoint {
		t.Fatalf("recovery did not preserve frozen destination: intent=%q current=%q err=%v", frozenIntentEndpoint, currentDestinationEndpoint, err)
	}
	reportID := uuid.New()
	size := int64(len(payload))
	reportBody, _ := json.Marshal(recordingCaptureRecoveryReportRequest{ReportID: reportID.String(), ReportType: "sealed_bytes", SizeBytes: &size, SHA256: hex.EncodeToString(payloadSHA[:]), LocalObservedAt: time.Now().UTC()})
	reportRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(reportBody))
	reportRequest = reportRequest.WithContext(context.WithValue(reportRequest.Context(), recordingRecoveryContextKey{}, recovery))
	reportRoute := chi.NewRouteContext()
	reportRoute.URLParams.Add("intentId", artifact.ID.String())
	reportRequest = reportRequest.WithContext(context.WithValue(reportRequest.Context(), chi.RouteCtxKey, reportRoute))
	reportResponse := httptest.NewRecorder()
	server.handleRecordingRecoveryReport(reportResponse, reportRequest)
	if reportResponse.Code != http.StatusNoContent {
		t.Fatalf("report=%d body=%s", reportResponse.Code, reportResponse.Body.String())
	}
	if err = pool.QueryRow(ctx, `SELECT result FROM recording_capture_set_results WHERE set_id=$1`, setID).Scan(new(string)); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("sealed local bytes terminalized the set before verified promotion: %v", err)
	}

	newUploadRequest := func(sessionID string, artifactID uuid.UUID, recoveryPrincipal recordingRecoveryPrincipal) *http.Request {
		request := httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(payload))
		request.Header.Set(recordingRecoverySessionHeader, sessionID)
		request = request.WithContext(context.WithValue(request.Context(), recordingRecoveryContextKey{}, recoveryPrincipal))
		route := chi.NewRouteContext()
		route.URLParams.Add("intentId", artifactID.String())
		return request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
	}
	crashedSessionID := uuid.New()
	if _, err = pool.Exec(ctx, `
		INSERT INTO recording_recovery_upload_sessions(
		 id,set_id,ordinal,revision,node_id,account_id,declared_bytes,quarantine_key,deadline_at)
		VALUES($1,$2,1,1,$3,$4,$5,$6,transaction_timestamp()+interval '1 millisecond')
	`, crashedSessionID, setID, recovery.NodeID, recovery.AccountID, len(payload), ".stoarama-recovery/v1/crashed/"+crashedSessionID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `SELECT pg_sleep(0.01)`); err != nil {
		t.Fatal(err)
	}
	var reconciled int64
	if err = pool.QueryRow(ctx, `SELECT recording_surrender_reconcile_expired_upload_sessions()`).Scan(&reconciled); err != nil || reconciled != 1 {
		t.Fatalf("expired empty session reconciled=%d err=%v", reconciled, err)
	}
	var crashResult string
	if err = pool.QueryRow(ctx, `SELECT result FROM recording_recovery_upload_session_results WHERE session_id=$1 AND phase='upload'`, crashedSessionID).Scan(&crashResult); err != nil || crashResult != "timeout" {
		t.Fatalf("crashed session result=%q err=%v", crashResult, err)
	}
	stalledPromotionID := uuid.New()
	if _, err = pool.Exec(ctx, `
		INSERT INTO recording_recovery_upload_sessions(
		 id,set_id,ordinal,revision,node_id,account_id,declared_bytes,quarantine_key,deadline_at)
		VALUES($1,$2,1,2,$3,$4,$5,$6,transaction_timestamp()+interval '1 millisecond');
		INSERT INTO recording_recovery_upload_session_results(id,session_id,phase,result,observed_size,observed_sha256)
		VALUES(gen_random_uuid(),$1,'upload','quarantined',$5,$7)
	`, stalledPromotionID, setID, recovery.NodeID, recovery.AccountID, len(payload), ".stoarama-recovery/v1/stalled/"+stalledPromotionID.String(), hex.EncodeToString(payloadSHA[:])); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `SELECT pg_sleep(0.01)`); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT recording_surrender_reconcile_expired_upload_sessions()`).Scan(&reconciled); err != nil || reconciled != 1 {
		t.Fatalf("expired promotion reconciled=%d err=%v", reconciled, err)
	}
	if err = pool.QueryRow(ctx, `SELECT result FROM recording_recovery_upload_session_results WHERE session_id=$1 AND phase='promotion'`, stalledPromotionID).Scan(&crashResult); err != nil || crashResult != "aborted" {
		t.Fatalf("stalled promotion result=%q err=%v", crashResult, err)
	}
	responseLossSession := uuid.NewString()
	responseLossRequest := newUploadRequest(responseLossSession, artifact.ID, recovery)
	responseLossResponse := httptest.NewRecorder()
	server.handleRecordingRecoveryProxyUpload(responseLossResponse, responseLossRequest)
	if responseLossResponse.Code != http.StatusNoContent {
		t.Fatalf("initial recovery proxy=%d body=%s", responseLossResponse.Code, responseLossResponse.Body.String())
	}
	var promotedObjectKey string
	if err = pool.QueryRow(ctx, `SELECT object_key FROM recording_upload_intents WHERE id=$1`, artifact.ID).Scan(&promotedObjectKey); err != nil {
		t.Fatal(err)
	}
	fakeStore.mu.Lock()
	originalMetadata := maps.Clone(fakeStore.metadata[promotedObjectKey])
	fakeStore.metadata[promotedObjectKey]["stoarama-sha256"] = strings.Repeat("0", 64)
	fakeStore.mu.Unlock()
	corruptReplay := httptest.NewRecorder()
	server.handleRecordingRecoveryProxyUpload(corruptReplay, newUploadRequest(responseLossSession, artifact.ID, recovery))
	if corruptReplay.Code != http.StatusConflict {
		t.Fatalf("size-only promoted replay bypassed immutable metadata=%d body=%s", corruptReplay.Code, corruptReplay.Body.String())
	}
	fakeStore.mu.Lock()
	fakeStore.metadata[promotedObjectKey] = originalMetadata
	fakeStore.mu.Unlock()
	responseLossReplay := httptest.NewRecorder()
	server.handleRecordingRecoveryProxyUpload(responseLossReplay, newUploadRequest(responseLossSession, artifact.ID, recovery))
	if responseLossReplay.Code != http.StatusNoContent {
		t.Fatalf("recovery proxy response-loss replay=%d body=%s", responseLossReplay.Code, responseLossReplay.Body.String())
	}
	if _, err = pool.Exec(ctx, `
		UPDATE recording_recovery_upload_session_results SET provider_etag='forged'
		WHERE session_id=$1 AND phase='promotion'
	`, responseLossSession); err == nil {
		t.Fatal("promoted provider identity was mutable")
	}

	// A second retained artifact proves revocation can commit while provider copy
	// is blocked. Final promotion revalidation must reject the stale handler.
	secondAuthRequest := httptest.NewRequest(http.MethodPost, "/", nil)
	secondAuthRequest.Header.Set(recordingRecoveryIntentHeader, secondArtifact.ID.String())
	secondAuthRequest.Header.Set(recordingRecoverySecretHeader, hex.EncodeToString(secondArtifact.RecoverySecret[:]))
	secondRecovery, err := server.authenticateRecordingRecovery(secondAuthRequest)
	if err != nil {
		t.Fatal(err)
	}
	secondStart := segmentStart.Add(time.Second)
	secondSealBody, _ := json.Marshal(recordingCaptureArtifactSealRequest{
		JobID: fixture.jobID, ProducerID: producerID.String(), CaptureSequence: 2,
		SetID: setID.String(), Ordinal: 2, SegmentStartUS: secondStart.UnixMicro(),
		SizeBytes: int64(len(payload)), SHA256: hex.EncodeToString(payloadSHA[:]),
	})
	secondSealRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(secondSealBody))
	secondSealRequest = secondSealRequest.WithContext(context.WithValue(secondSealRequest.Context(), nodePrincipalContextKey, recoveryNode))
	secondSealRequest = secondSealRequest.WithContext(context.WithValue(secondSealRequest.Context(), recordingRecoveryContextKey{}, secondRecovery))
	secondSealRoute := chi.NewRouteContext()
	secondSealRoute.URLParams.Add("intentId", secondArtifact.ID.String())
	secondSealRequest = secondSealRequest.WithContext(context.WithValue(secondSealRequest.Context(), chi.RouteCtxKey, secondSealRoute))
	secondSealResponse := httptest.NewRecorder()
	server.handleRecordingCaptureArtifactSeal(secondSealResponse, secondSealRequest)
	if secondSealResponse.Code != http.StatusOK {
		t.Fatalf("second recovery seal=%d body=%s", secondSealResponse.Code, secondSealResponse.Body.String())
	}
	secondReportBody, _ := json.Marshal(recordingCaptureRecoveryReportRequest{ReportID: uuid.NewString(), ReportType: "sealed_bytes", SizeBytes: &size, SHA256: hex.EncodeToString(payloadSHA[:]), LocalObservedAt: time.Now().UTC()})
	secondReportRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(secondReportBody))
	secondReportRequest = secondReportRequest.WithContext(context.WithValue(secondReportRequest.Context(), recordingRecoveryContextKey{}, secondRecovery))
	secondReportRoute := chi.NewRouteContext()
	secondReportRoute.URLParams.Add("intentId", secondArtifact.ID.String())
	secondReportRequest = secondReportRequest.WithContext(context.WithValue(secondReportRequest.Context(), chi.RouteCtxKey, secondReportRoute))
	secondReportResponse := httptest.NewRecorder()
	server.handleRecordingRecoveryReport(secondReportResponse, secondReportRequest)
	if secondReportResponse.Code != http.StatusNoContent {
		t.Fatalf("second recovery report=%d body=%s", secondReportResponse.Code, secondReportResponse.Body.String())
	}

	fakeStore.copyStarted, fakeStore.copyGate = make(chan struct{}, 1), make(chan struct{})
	uploadRequest := newUploadRequest(uuid.NewString(), secondArtifact.ID, secondRecovery)
	uploadDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		out := httptest.NewRecorder()
		server.handleRecordingRecoveryProxyUpload(out, uploadRequest)
		uploadDone <- out
	}()
	select {
	case <-fakeStore.copyStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery proxy did not enter fenced promotion")
	}
	var operatorID int64
	if err = pool.QueryRow(ctx, `INSERT INTO users(email,name,is_operator) VALUES($1,'r10 recovery operator',true) RETURNING id`, fmt.Sprintf("r10-recovery-%d@example.test", time.Now().UnixNano())).Scan(&operatorID); err != nil {
		t.Fatal(err)
	}
	var forgedActorID int64
	if err = pool.QueryRow(ctx, `INSERT INTO users(email,name,is_operator) VALUES($1,'forged recovery actor',false) RETURNING id`, fmt.Sprintf("r10-forged-%d@example.test", time.Now().UnixNano())).Scan(&forgedActorID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO recording_capture_security_events(id,set_id,actor_user_id,reason,idempotency_key) VALUES(gen_random_uuid(),$1,$2,'suspected_seed_compromise',gen_random_uuid())`, setID, forgedActorID); err == nil {
		t.Fatal("unauthorized direct SQL actor forged recovery security authority")
	}
	revokeBody, _ := json.Marshal(recordingRecoverySecurityRevokeRequest{SetID: setID.String(), IdempotencyKey: uuid.NewString(), Reason: "suspected_seed_compromise"})
	revokeRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(revokeBody))
	revokeRequest = revokeRequest.WithContext(context.WithValue(revokeRequest.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: fixture.accountID, UserID: operatorID}))
	revokeRequest = revokeRequest.WithContext(context.WithValue(revokeRequest.Context(), adminOverrideKey, true))
	revokeDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		out := httptest.NewRecorder()
		server.handleAdminRecordingRecoverySecurityRevoke(out, revokeRequest)
		revokeDone <- out
	}()
	select {
	case out := <-revokeDone:
		if out.Code != http.StatusOK {
			t.Fatalf("security revoke during in-flight promotion=%d body=%s", out.Code, out.Body.String())
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("security revoke waited behind provider copy")
	}
	close(fakeStore.copyGate)
	if out := <-uploadDone; out.Code != http.StatusConflict {
		t.Fatalf("revoked in-flight recovery proxy=%d body=%s", out.Code, out.Body.String())
	}
	if _, authErr := server.authenticateRecordingRecovery(authRequest); authErr == nil {
		t.Fatal("security-revoked recovery capability remained usable")
	}
	staleUpload := httptest.NewRequest(http.MethodPut, "/", bytes.NewReader(payload))
	staleUpload.Header.Set(recordingRecoverySessionHeader, uuid.NewString())
	staleUpload = staleUpload.WithContext(context.WithValue(staleUpload.Context(), recordingRecoveryContextKey{}, recovery))
	staleRoute := chi.NewRouteContext()
	staleRoute.URLParams.Add("intentId", artifact.ID.String())
	staleUpload = staleUpload.WithContext(context.WithValue(staleUpload.Context(), chi.RouteCtxKey, staleRoute))
	staleResponse := httptest.NewRecorder()
	server.handleRecordingRecoveryProxyUpload(staleResponse, staleUpload)
	if staleResponse.Code != http.StatusConflict {
		t.Fatalf("revoked stale-context recovery upload=%d body=%s", staleResponse.Code, staleResponse.Body.String())
	}
	var setResult string
	if err = pool.QueryRow(ctx, `SELECT result FROM recording_capture_set_results WHERE set_id=$1`, setID).Scan(&setResult); err != nil || setResult != "security_revoked" {
		t.Fatalf("set result=%q err=%v", setResult, err)
	}
	proposalRequest = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(proposalBody))
	proposalRequest = proposalRequest.WithContext(context.WithValue(proposalRequest.Context(), nodePrincipalContextKey, principal))
	proposalResponse = httptest.NewRecorder()
	server.handleRecordingClaimSuccessorPropose(proposalResponse, proposalRequest)
	if proposalResponse.Code != http.StatusOK {
		t.Fatalf("successor proposal=%d body=%s", proposalResponse.Code, proposalResponse.Body.String())
	}
	var successor recordingapi.ClaimSuccessor
	if err = json.Unmarshal(proposalResponse.Body.Bytes(), &successor); err != nil {
		t.Fatal(err)
	}
	// A successor may acknowledge with its own bearer while the predecessor
	// continues heartbeating an unrelated exact fence. The predecessor retires
	// only after that fence is terminal; rotation never churns a healthy capture.
	ackPrincipal := principal
	ackPrincipal.NodeTokenID = successor.SuccessorTokenID
	ackSuccessor := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{}`)))
		request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, ackPrincipal))
		route := chi.NewRouteContext()
		route.URLParams.Add("proposalId", proposalID.String())
		request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
		response := httptest.NewRecorder()
		server.handleRecordingClaimSuccessorAck(response, request)
		return response
	}
	if response := ackSuccessor(); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"predecessor_retired":false`) {
		t.Fatalf("successor ack with live predecessor fence=%d body=%s", response.Code, response.Body.String())
	}
	heartbeatRequest := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{}`)))
	heartbeatRequest.Header.Set(recordingLeaseTokenHeader, unrelatedLease.String())
	heartbeatRequest = heartbeatRequest.WithContext(context.WithValue(heartbeatRequest.Context(), nodePrincipalContextKey, ackPrincipal))
	heartbeatRoute := chi.NewRouteContext()
	heartbeatRoute.URLParams.Add("id", strconv.FormatInt(unrelated.jobID, 10))
	heartbeatRequest = heartbeatRequest.WithContext(context.WithValue(heartbeatRequest.Context(), chi.RouteCtxKey, heartbeatRoute))
	heartbeatResponse := httptest.NewRecorder()
	server.handleRecordingJobHeartbeat(heartbeatResponse, heartbeatRequest)
	if heartbeatResponse.Code != http.StatusOK {
		t.Fatalf("successor could not heartbeat predecessor fence=%d body=%s", heartbeatResponse.Code, heartbeatResponse.Body.String())
	}
	completeJob := func(jobID int64, leaseToken uuid.UUID, presented nodePrincipal) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{}`)))
		request.Header.Set(recordingLeaseTokenHeader, leaseToken.String())
		request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, presented))
		route := chi.NewRouteContext()
		route.URLParams.Add("id", strconv.FormatInt(jobID, 10))
		request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
		response := httptest.NewRecorder()
		server.handleRecordingJobComplete(response, request)
		return response
	}
	failJob := func(jobID int64, leaseToken uuid.UUID, presented nodePrincipal) *httptest.ResponseRecorder {
		body, _ := json.Marshal(recordingJobFailRequest{ErrorText: "r10 fenced failure"})
		request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		request.Header.Set(recordingLeaseTokenHeader, leaseToken.String())
		request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, presented))
		route := chi.NewRouteContext()
		route.URLParams.Add("id", strconv.FormatInt(jobID, 10))
		request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
		response := httptest.NewRecorder()
		server.handleRecordingJobFail(response, request)
		return response
	}
	if response := completeJob(unrelated.jobID, unrelatedLease, ackPrincipal); response.Code != http.StatusOK {
		t.Fatalf("successor could not complete predecessor fence=%d body=%s", response.Code, response.Body.String())
	}
	if response := failJob(unrelatedFail.jobID, unrelatedFailLease, ackPrincipal); response.Code != http.StatusOK {
		t.Fatalf("successor could not fail predecessor fence=%d body=%s", response.Code, response.Body.String())
	}
	successorClaim := seedPresentationV2Task(t, pool, 92004, fixture.accountID)
	if _, err = pool.Exec(ctx, `
		UPDATE recordings SET capture_via='relay' WHERE id=$1;
		UPDATE recording_jobs SET status='pending',lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
		 scheduled_for=transaction_timestamp()-interval '1 minute' WHERE id=$2
	`, successorClaim.recordingID, successorClaim.jobID); err != nil {
		t.Fatal(err)
	}
	newClaim, claimErr := server.leaseRelayRecordingJob(ctx, ackPrincipal, true, 150, true)
	if claimErr != nil || newClaim.JobID != successorClaim.jobID || newClaim.LeaseToken == nil {
		t.Fatalf("successor did not claim new work while serving predecessor fence: lease=%+v err=%v", newClaim, claimErr)
	}
	if response := completeJob(successorClaim.jobID, *newClaim.LeaseToken, principal); response.Code != http.StatusConflict {
		t.Fatalf("predecessor credential completed successor fence=%d body=%s", response.Code, response.Body.String())
	}
	if response := completeJob(successorClaim.jobID, *newClaim.LeaseToken, ackPrincipal); response.Code != http.StatusOK {
		t.Fatalf("successor credential could not complete successor fence=%d body=%s", response.Code, response.Body.String())
	}
	if response := ackSuccessor(); response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"predecessor_retired":true`) {
		t.Fatalf("successor ack after predecessor fences drained=%d body=%s", response.Code, response.Body.String())
	}
	var headGeneration int64
	var headTokenID int64
	var headState string
	if err = pool.QueryRow(ctx, `SELECT generation,claim_token_id,state FROM recording_worker_claim_heads WHERE node_id=$1`, fixture.nodeID).Scan(&headGeneration, &headTokenID, &headState); err != nil || headGeneration != 2 || headTokenID != successor.SuccessorTokenID || headState != "enabled" {
		t.Fatalf("head generation=%d token=%d state=%q err=%v", headGeneration, headTokenID, headState, err)
	}
	var retirementEvents int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM recording_worker_claim_generation_events WHERE node_id=$1 AND generation=1 AND claim_token_id=$2 AND event_type='retired'`, fixture.nodeID, oldTokenID).Scan(&retirementEvents); err != nil || retirementEvents != 1 {
		t.Fatalf("retirement events=%d err=%v", retirementEvents, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE nodes SET status='disabled' WHERE id=$1`, peerNodeID); err != nil {
		t.Fatal(err)
	}
	recoveredLease, err := server.leaseRelayRecordingJob(ctx, ackPrincipal, true, 150, true)
	if err != nil || recoveredLease.JobID != fixture.jobID || recoveredLease.LeaseToken == nil || *recoveredLease.LeaseToken == *lease.LeaseToken {
		t.Fatalf("dynamic relay alternate waiver lease=%+v err=%v", recoveredLease, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_capture_set_grants SET upload_grace_until=transaction_timestamp()+interval '1 hour' WHERE set_id=$1`, setID); err == nil {
		t.Fatal("immutable set grant was mutable")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_worker_claim_heads SET generation=generation+1 WHERE node_id=$1`, fixture.nodeID); err == nil {
		t.Fatal("claim head advanced without a typed successor event")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO node_tokens(node_id,key_prefix,secret_hash,recording_claim_generation,recording_claim_purpose) VALUES($1,'forged-current',repeat('8',64),3,'claim_current')`, fixture.nodeID); err == nil {
		t.Fatal("caller forged an explicit current claim generation")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_worker_claim_successor_proposals SET successor_key_prefix='substituted' WHERE id=$1`, proposalID); err == nil {
		t.Fatal("claim successor proposal was mutable")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM recording_capture_set_grants WHERE set_id=$1`, setID); err == nil {
		t.Fatal("capture set recovery grant was deletable")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_capture_set_results SET result_at=result_at+interval '1 second' WHERE set_id=$1`, setID); err == nil {
		t.Fatal("capture set terminal result was mutable")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_worker_claim_heads SET state='recovery_blocked',blocked_at=transaction_timestamp(),block_reason='durable_recovery' WHERE node_id=$1`, peerNodeID); err == nil {
		t.Fatal("claim head recovery block committed without its immutable event")
	}
	if _, err = pool.Exec(ctx, `UPDATE node_tokens SET revoked_at=transaction_timestamp() WHERE node_id=$1 AND revoked_at IS NULL`, peerNodeID); err == nil {
		t.Fatal("recording claim credential retired without terminal events")
	}
	if _, err = pool.Exec(ctx, `UPDATE stream_source_revisions SET reason='substituted' WHERE id=$1`, frozenSourceRevisionID); err == nil {
		t.Fatal("referenced source revision was mutable")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM stream_source_revisions WHERE id=$1`, frozenSourceRevisionID); err == nil {
		t.Fatal("referenced source revision was deletable")
	}
	if _, err = pool.Exec(ctx, `TRUNCATE stream_source_revisions`); err == nil {
		t.Fatal("referenced source revision lineage was truncatable")
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
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker, NodeTokenID: surrenderTransportTestTokenID(t, server, fixture.nodeID)}))
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
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker, NodeTokenID: surrenderTransportTestTokenID(t, server, fixture.nodeID)}))
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
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker, NodeTokenID: surrenderTransportTestTokenID(t, server, fixture.nodeID)}))
	response := httptest.NewRecorder()
	server.handleRecordingCaptureArtifactsReserve(response, request)
	return response
}

func callSurrender(t *testing.T, server *Server, fixture presentationV2Fixture, lease uuid.UUID, worker, nodeType string, attempt uuid.UUID) *httptest.ResponseRecorder {
	t.Helper()
	var nodeTokenID int64
	if err := server.pool.QueryRow(context.Background(), `SELECT lease_node_token_id FROM recording_jobs WHERE id=$1`, fixture.jobID).Scan(&nodeTokenID); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(recordingJobSurrenderRequest{TransportVersion: 1, AttemptID: attempt.String(), Reason: recordingJobSurrenderNoProgress})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	request.Header.Set(recordingLeaseTokenHeader, lease.String())
	route := chi.NewRouteContext()
	route.URLParams.Add("id", strconv.FormatInt(fixture.jobID, 10))
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeType, DisplayName: worker, NodeTokenID: nodeTokenID}))
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
	tokenID := surrenderTransportTestTokenID(t, server, fixture.nodeID)
	request = request.WithContext(context.WithValue(request.Context(), nodePrincipalContextKey, nodePrincipal{NodeID: fixture.nodeID, AccountID: fixture.accountID, NodeType: nodeTypeLocalRecorder, DisplayName: worker, NodeTokenID: tokenID}))
	response := httptest.NewRecorder()
	server.handleRecordingSurrenderTransportObservations(response, request)
	return response
}

func surrenderTransportTestTokenID(t *testing.T, server *Server, nodeID int64) int64 {
	t.Helper()
	var tokenID int64
	err := server.pool.QueryRow(context.Background(), `SELECT id FROM node_tokens WHERE node_id=$1 AND revoked_at IS NULL ORDER BY id DESC LIMIT 1`, nodeID).Scan(&tokenID)
	if errors.Is(err, pgx.ErrNoRows) {
		prefix := fmt.Sprintf("surrender-test-%d", nodeID)
		err = server.pool.QueryRow(context.Background(), `INSERT INTO node_tokens(node_id,key_prefix,secret_hash) VALUES($1,$2,repeat('d',64)) RETURNING id`, nodeID, prefix).Scan(&tokenID)
	}
	if err != nil {
		t.Fatal(err)
	}
	return tokenID
}
