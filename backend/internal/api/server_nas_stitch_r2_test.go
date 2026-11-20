package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestR2StitchDurhamHoldReleasedOnlyByCommittedNASCertificate(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	media := bytes.Repeat([]byte{'x'}, 100)
	sha := cleanupSHA(media)
	const abortGeneration = "f5238abd-6063-4441-a712-a9647d10b986"
	abortKey := "stitch-staging/" + abortGeneration + "/" + sha + ".mp4"
	abort := durhamStageAbortProbe{deleteEntered: make(chan struct{}), deleteRelease: make(chan struct{})}
	var stageHeadResponse atomic.Int32
	provider := durhamStageTestProvider(t, media, sha, abortKey, &abort, &stageHeadResponse)
	defer provider.Close()
	defer abort.release()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := pgx.Identifier{fmt.Sprintf("durham_hold_%d", time.Now().UnixNano())}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	// The lock contention helpers hold several sessions open at once; pgxpool's
	// default (NumCPU, 4 on CI) starves the code under test.
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Pool close runs last: the deferred handlers release above must free their
	// held DELETE and session lock first, or Close waits on a checked-out conn.
	t.Cleanup(pool.Close)
	if err := db.MigrateUp(ctx, pool, findMigrationsDir(t)); err != nil {
		t.Fatal(err)
	}
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`
INSERT INTO accounts(id,email) VALUES(47,'durham-test@example.invalid');
INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(47,47,'fixture-key','fixture-key');
INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',47);
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(3,'test','3','test','test','https://storage.example.test','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc)
 VALUES(1,47,'test','https://storage.example.test','auto','stoarama','fixture',decode('00','hex'));
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(30,47,1,'test','https://storage.example.test','* * * * *','Asia/Tokyo','paused',3,now());
INSERT INTO nas_stitch_corpus(sha256,body) VALUES('5b2765ecb4f6483c8e48b5a6c958c7c0d8082764d4da57901626a22569c6346b',decode('00','hex'));`)
	_, windows, _ := Keep28Policy()
	hour := windows[3].Start.Truncate(time.Hour).UTC()
	end := hour.Add(time.Minute)
	const sourceKey = "managed/durham-fixture.mp4"
	const captureLease = "00000000-0000-0000-0000-000000000099"
	const path = "fixture/raw.mp4"
	exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path,capture_lease_token) VALUES(901,30,1,$6,'stoarama',$1,100,'source-etag',$2,$3,$3,$4,$5,$7)`, sourceKey, sha, hour, end, path, provider.URL, captureLease)
	exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at)
 VALUES(13,901,30,$1,100,$2,'present',now(),now())`, path, sha)
	exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path) VALUES(902,30,1,$1,'stoarama','managed/durham-fixture-2.mp4',100,
 'source-2-etag',$2,$3,$3,$4,'fixture/raw2.mp4')`, provider.URL, sha, end, end.Add(time.Minute))
	exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at)
 VALUES(13,902,30,'fixture/raw2.mp4',100,$1,'present',now(),now())`, sha)
	extra := addDurhamHours(t, pool, hour, provider.URL, sha)

	exec(`INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until)
 VALUES(1,3,$1,$1::timestamptz+interval '6 hours','test',clock_timestamp()+interval '1 hour')`, hour)
	bestStart := windows[3].Start.AddDate(0, 0, 7)
	exec(`INSERT INTO recording_clips(recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path) VALUES(30,1,$1,'stoarama','managed/keep-ranking.mp4',100,
 'test-etag',repeat('c',64),$2,$2,$3,'fixture/keep-ranking.mp4')`, provider.URL, bestStart, bestStart.AddDate(0, 0, 14))
	endpoint := cleanupSHA([]byte(provider.URL))
	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, endpoint)
	store, err := r2.New(ctx, r2.Config{Bucket: "stoarama", Endpoint: provider.URL,
		AccessKey: "test", SecretKey: "test", WriterDB: pool})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RetireStitchStage(ctx, 123, sha, r2.StitchStageEvidence{}, r2.StitchStagePicks{}, nil, false); err == nil ||
		!strings.Contains(err.Error(), "reviewed evidence") {
		t.Fatal("staging retirement was admitted without reviewed per-stage evidence", err)
	}

	planTx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer planTx.Rollback(ctx)
	set, err := PlanDurhamStitchHoldSet(ctx, planTx, []int64{1}, "stoarama", provider.URL, 1)
	if err != nil || len(set.Hours) != 1 || len(set.Hours[0].Inputs) != 2 {
		t.Fatalf("read-only Durham planner did not produce the complete hour: %v", err)
	}
	if err := planTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var plan nasStitchPlan
	if err := json.Unmarshal(set.Hours[0].Plan, &plan); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(set.Hours[0].Plan, []byte(captureLease)) ||
		plan.FeatureInputs[0].CaptureLeaseToken != cleanupSHA([]byte(captureLease)) {
		t.Fatal("Durham plan did not confine capture credentials to a digest")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if n, err := ImportDurhamStitchHoldSet(ctx, tx, set); n != 2 || err != nil {
		t.Fatalf("hold set import: %d %v", n, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	reserve := func() error {
		_, err := pool.Exec(ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 VALUES('raw',901,'stoarama',$1,100,$2,repeat('a',64))`, sourceKey, sha)
		return err
	}
	if err := reserve(); err == nil || !strings.Contains(err.Error(), "active R2 stitch input hold") {
		t.Fatal("stale reservation path did not obey the hold", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE nas_stitch_r2_input_holds`); err == nil || !strings.Contains(err.Error(), "evidence is durable") {
		t.Fatal("TRUNCATE bypassed durable input holds", err)
	}
	var id int64
	var attempt int
	var raw []byte
	var planDigest string
	if err := pool.QueryRow(ctx, nasDurhamClaimSQL).Scan(&id, &attempt, &raw, &planDigest); err != nil || attempt != 0 {
		t.Fatalf("exact held hour not claimable: %v", err)
	}
	t.Run("an issued local lease drains before Durham can claim", func(t *testing.T) {
		var localID int64
		if err := pool.QueryRow(ctx, `INSERT INTO nas_stitch_tasks(scope_id,recording_id,hour_at,plan,plan_sha256,
 state,attempt,lease_token,lease_expires_at) VALUES(1,30,$1,$2,$3,'leased',3,repeat('d',64),clock_timestamp()+interval '1 hour')
 RETURNING id`, hour, string(raw), planDigest).Scan(&localID); err != nil {
			t.Fatal(err)
		}
		exec(`UPDATE nas_stitch_r2_control SET enabled=true;
 UPDATE account_api_keys SET scopes=ARRAY['stoarama.nas_stitch'],secret_hash='nas-stitch:'||repeat('a',64) WHERE id=47`)
		server := &Server{pool: pool}
		w := cleanupCall(t, func(w http.ResponseWriter, r *http.Request) {
			server.handleDurhamStitchLease(w, r, nasStitchCaller{keyID: 47, accountID: 47, dedicated: true})
		}, 47, 47, "", "", nil)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"task":null`) {
			t.Fatal("Durham competed with an issued local lease", w.Code, w.Body.String())
		}
		var found int
		if err := pool.QueryRow(ctx, `SELECT attempt FROM nas_stitch_r2_hours WHERE id=$1`, id).Scan(&found); err != nil || found != 0 {
			t.Fatal("waiting for the local lease changed the Durham attempt", found, err)
		}
		exec(`UPDATE nas_stitch_tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, localID)
		exec(`UPDATE nas_stitch_r2_control SET enabled=false;
 UPDATE account_api_keys SET scopes=ARRAY['stoarama.pull'],secret_hash='fixture-key' WHERE id=47`)
	})

	exec(`UPDATE nas_stitch_r2_hours SET state='leased',attempt=1,lease_api_key_id=47,lease_token=repeat('e',64),
 lease_expires_at=clock_timestamp()+interval '1 minute',lease_toolchain_attempt=1,lease_toolchain_sha256=$2
 WHERE id=$1`, id, nasDurhamManifestSHA)
	exec(`INSERT INTO nas_stitch_r2_stage_objects(id,hour_id,attempt,api_key_id,bucket,endpoint_sha256,generation,relative_path,sha256,size_bytes)
 OVERRIDING SYSTEM VALUE VALUES(122,$1,1,47,'stoarama',$2,$3,'fixture/aborted.mp4',$4,100)`,
		id, endpoint, abortGeneration, sha)
	exec(`UPDATE nas_stitch_r2_stage_objects SET state='uploading',upload_id='earlier-abort',start_issued_at=clock_timestamp(),
 grants_drain_at=clock_timestamp()-interval '1 second' WHERE id=122;
 UPDATE nas_stitch_r2_stage_objects SET state='aborted' WHERE id=122`)
	exec(`UPDATE nas_stitch_r2_hours SET attempt=2,lease_toolchain_attempt=2,lease_toolchain_sha256=$2 WHERE id=$1`, id, nasDurhamManifestSHA)
	t.Run("predecessor cannot reuse the native lease profile", func(t *testing.T) {
		// The previous binary replaces the attempt and token without knowing the profile columns.
		if _, err := pool.Exec(ctx, `UPDATE nas_stitch_r2_hours SET state='leased',attempt=3,lease_api_key_id=47,
 lease_token=repeat('f',64),lease_expires_at=clock_timestamp()+interval '1 minute' WHERE id=$1`, id); err == nil ||
			!strings.Contains(err.Error(), "reviewed Durham lease toolchain required") {
			t.Fatal("legacy reclaim inherited the native profile", err)
		}
	})
	const insertStage = `INSERT INTO nas_stitch_r2_stage_objects(id,hour_id,attempt,api_key_id,bucket,endpoint_sha256,relative_path,sha256,size_bytes)
 OVERRIDING SYSTEM VALUE VALUES(123,$1,2,47,'stoarama',$2,$3,$4,100)`
	outputPath := plan.OutputFolder + "/run-000-" + sha + ".mp4"
	staleAdmission, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer staleAdmission.Rollback(ctx)
	var oldFence bool
	if err := staleAdmission.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM r2_retirement_writer_fences)`).Scan(&oldFence); err != nil || !oldFence {
		t.Fatal("staging admission did not establish its old fence snapshot", err)
	}
	exec(`DELETE FROM r2_retirement_writer_fences`)
	if _, err := staleAdmission.Exec(ctx, insertStage, id, endpoint, outputPath, sha); err == nil ||
		!strings.Contains(err.Error(), "staging admission requires READ COMMITTED") {
		t.Fatal("stale staging snapshot admitted a writer after fence removal", err)
	}
	staleAdmission.Rollback(ctx)
	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, endpoint)
	// Model the controller's preexisting matured fence in this disposable schema.
	exec(`ALTER TABLE r2_retirement_writer_fences DISABLE TRIGGER r2_writer_fence_transition;
 UPDATE r2_retirement_writer_fences SET drain_not_before=clock_timestamp()-interval '1 second';
 ALTER TABLE r2_retirement_writer_fences ENABLE TRIGGER r2_writer_fence_transition;`)
	exec(insertStage, id, endpoint, outputPath, sha)
	if _, err := pool.Exec(ctx, `DELETE FROM r2_retirement_writer_fences`); err == nil ||
		!strings.Contains(err.Error(), "have not drained") {
		t.Fatal("committed starting stage allowed fence removal", err)
	}
	exec(`UPDATE nas_stitch_r2_control SET enabled=true,max_staging_bytes=1000000`)
	lease := r2.StitchStageLease{HourID: id, APIKeyID: 47, Attempt: 2, Token: strings.Repeat("e", 64), PlanSHA: planDigest, Toolchain: nasDurhamManifestSHA}
	foreign := lease
	foreign.APIKeyID++
	if _, err := store.StartStitchStage(ctx, foreign, 123); err == nil {
		t.Fatal("wrong key obtained staging authority")
	}
	for i := 0; i < 2; i++ {
		upload, err := store.StartStitchStage(ctx, lease, 123)
		if err != nil || upload.State != "uploading" {
			t.Fatalf("initial or resumed multipart start: %v", err)
		}
	}
	part, err := store.PresignStitchStagePart(ctx, lease, 123, 1, sha)
	if err != nil {
		t.Fatal(err)
	}
	put, err := http.NewRequestWithContext(ctx, part.Method, part.URL, bytes.NewReader(media))
	if err != nil {
		t.Fatal(err)
	}
	put.Header = part.Headers.Clone()
	response, err := provider.Client().Do(put)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("exact staging grant was refused: %d", response.StatusCode)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE r2_retirement_writer_fences`); err == nil || !strings.Contains(err.Error(), "evidence is durable") {
		t.Fatal("TRUNCATE bypassed the writer fence with a live grant", err)
	}
	for i := 0; i < 2; i++ {
		if err := store.FinishStitchStage(ctx, lease, 123); err != nil {
			t.Fatalf("initial or resumed multipart completion: %v", err)
		}
	}
	result := nasStitchResult{Policy: nasStitchPolicy, PlanSHA: planDigest, Token: lease.Token,
		Seams: []nasStitchDecision{{Decision: "join"}},
		Outputs: []nasStitchOutput{{Path: plan.OutputFolder + "/run-000-" + sha + ".mp4", Size: int64(len(media)), SHA: sha,
			Provenance: reviewedDurhamProvenance(),
			Inputs: []nasStitchInputProof{{ID: 901, SHA: sha, Size: int64(len(media)), RetirementHeld: true},
				{ID: 902, SHA: sha, Size: int64(len(media)), RetirementHeld: true}}}}}
	stagedManifest, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	manifest := cleanupSHA(stagedManifest)
	var copyID int64
	var key string
	if err := pool.QueryRow(ctx, `SELECT object_key FROM nas_stitch_r2_stage_objects WHERE id=123`).Scan(&key); err != nil {
		t.Fatal(err)
	}
	wire := apiStageWire{endpoint: provider.URL}
	// evidence binds one exact stage generation, hour and attempt to the live
	// fence this disposable schema matured. It is derived here, never accepted
	// from free text.
	var fenceAt, fenceDrain time.Time
	if err := pool.QueryRow(ctx, `SELECT activated_at,drain_not_before FROM r2_retirement_writer_fences
 WHERE bucket='stoarama' AND endpoint_sha256=$1`, endpoint).Scan(&fenceAt, &fenceDrain); err != nil {
		t.Fatal(err)
	}
	// The stream sets come from a real reviewed picks file, exactly as the CLI
	// derives them. Nothing is hand-written into the evidence arrays.
	window := windows[plan.StreamID]
	picksRaw := fmt.Sprintf(`{"saved_at":"2026-11-19T00:00:00Z","choices":{"%d":{"choice":"st","keep_start_utc":%q,"keep_end_utc":%q}}}`,
		plan.StreamID, window.Start.Format("2006-01-02 15:04:05Z07:00"), window.End.Format("2006-01-02 15:04:05Z07:00"))
	picksSHA := cleanupSHA([]byte(picksRaw))
	// This package cannot call the command layer's picks parser, so the fixture
	// states the derivation it expects and the command layer's own parser is
	// covered by its unit tests. The retirement re-derives and compares.
	decided, ke := []int64{plan.StreamID}, []int64{}
	decision := func() r2.StitchStagePicks {
		return r2.StitchStagePicks{SHA256: picksSHA, Decided: decided, KE: ke}
	}
	evidence := func() r2.StitchStageEvidence {
		return r2.StitchStageEvidence{Version: 1, Bucket: "stoarama", EndpointSHA256: endpoint,
			ObjectID: 123, ObjectKey: key, SHA256: sha, SizeBytes: 100,
			HourID: id, Attempt: 2, PlanSHA256: planDigest, ManifestSHA256: manifest,
			FenceActivatedAt: fenceAt, DrainNotBefore: fenceDrain,
			DeployedWriterCoverage: "fixture: this disposable schema",
			ExternalWriterRace:     r2.StitchStageExternalWriterRace,
			PicksSHA256:            picksSHA,
			DecidedStreamIDs:       decided,
			KEStreamIDs:            ke}
	}
	item := NASR2CopyItem{Kind: NASR2CopyStitchOutput, SourceID: 123, RecordingID: plan.RecordingID, StreamID: plan.StreamID,
		RelativePath: plan.OutputFolder + "/run-000-" + sha + ".mp4", SHA256: sha, SizeBytes: 100,
		Bucket: "stoarama", ObjectKey: key, StartAt: hour, EndAt: end}
	data, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	request := nasR2CopyControl{Token: strings.Repeat("d", 64), Completion: NASR2CopyDownloaded,
		DownloadedBytes: 100, CommittedBytes: 100, FreeBytes: NASR2CopyFloor + NASR2CopyRangeBytes}
	request.StitchReceipt = &nasR2StitchReceipt{RelativePath: item.RelativePath, SHA: sha, Bytes: 100,
		Identity: [5]int64{100, 1, 2, 3, 4}, Fsynced: true}
	if err := pool.QueryRow(ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,
 policy_sha256,item,state,lease_token,lease_expires_at,etag)
 VALUES(13,'stitch_output',123,$1,$2,$3,'leased',$4,clock_timestamp()+interval '2 minutes','stage-etag')
 RETURNING id`, manifest, cleanupPolicySHA, string(data), request.Token).Scan(&copyID); err != nil {
		t.Fatal(err)
	}
	t.Run("split outputs certify together and cover their own clips", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		testCertifiedStitchSplitOutput(t, tx, id, copyID, endpoint, result, item, request)
	})

	receipt, err := json.Marshal(request.StitchReceipt)
	if err != nil {
		t.Fatal(err)
	}
	duplicateResult := result
	duplicateResult.Outputs = append([]nasStitchOutput(nil), result.Outputs...)
	first := result.Outputs[0].Inputs[0]
	duplicateResult.Outputs[0].Inputs = []nasStitchInputProof{first, first}
	duplicateInputs, err := json.Marshal(duplicateResult)
	if err != nil {
		t.Fatal(err)
	}
	legacyResult := result
	legacyResult.Outputs = append([]nasStitchOutput(nil), result.Outputs...)
	_, legacyProof := stitchUnitFixture()
	legacyResult.Outputs[0].Provenance = legacyProof.Outputs[0].Provenance
	legacyBody, err := json.Marshal(legacyResult)
	if err != nil {
		t.Fatal(err)
	}
	for _, altered := range []struct {
		name, payload, etag, version, body string
		committed                          int64
	}{
		{"empty_receipt", "{}", "stage-etag", "", string(stagedManifest), 100},
		{"wrong_etag", string(receipt), "other-etag", "", string(stagedManifest), 100},
		{"wrong_version", string(receipt), "stage-etag", "other-version", string(stagedManifest), 100},
		{"wrong_byte_count", string(receipt), "stage-etag", "", string(stagedManifest), 0},
		{"legacy_toolchain", string(receipt), "stage-etag", "", string(legacyBody), 100},
		{"empty_manifest", string(receipt), "stage-etag", "", "{}", 100},
		{"duplicate_disposition", string(receipt), "stage-etag", "", string(duplicateInputs), 100},
		{"duplicate_plan", string(receipt), "stage-etag", "", string(duplicateInputs), 100},
		{"mixed_type_duplicate", string(receipt), "stage-etag", "", string(duplicateInputs), 100},
	} {
		t.Run(altered.name, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			wantError := "pulled output coverage"
			if altered.name == "duplicate_plan" || altered.name == "mixed_type_duplicate" {
				if _, err := tx.Exec(ctx, `ALTER TABLE nas_stitch_r2_hours DISABLE TRIGGER nas_stitch_r2_hour_plan_immutable`); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(ctx, `UPDATE nas_stitch_r2_hours SET plan=jsonb_set(plan,'{clips,1}',
 CASE WHEN $2::boolean THEN jsonb_set(plan->'clips'->0,'{source_id}',to_jsonb(plan->'clips'->0->>'source_id'))
 ELSE plan->'clips'->0 END) WHERE id=$1`, id, altered.name == "mixed_type_duplicate"); err != nil {
					t.Fatal(err)
				}
				wantError = "input coverage changed"
			}
			if _, err := tx.Exec(ctx, `UPDATE nas_stitch_r2_hours
 SET state='staged',staged_manifest=$3,staged_manifest_sha256=$2 WHERE id=$1`, id, manifest, altered.body); err != nil {
				t.Fatal(err)
			}
			if altered.name == "legacy_toolchain" {
				var copyReady bool
				if err := tx.QueryRow(ctx, `SELECT nas_stitch_r2_native_copy_ready(123,$1)`, manifest).Scan(&copyReady); err != nil || copyReady {
					t.Fatal("legacy output entered the NAS copy queue", err)
				}
				probe, err := tx.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_, err = probe.Exec(ctx, `UPDATE nas_r2_copy_items SET lease_expires_at=clock_timestamp()+interval '1 minute'
 WHERE id=$1`, copyID)
				probe.Rollback(ctx)
				if err == nil || !strings.Contains(err.Error(), "reviewed Durham copy toolchain required") {
					t.Fatal("predecessor renewed a legacy NAS copy", err)
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE nas_r2_copy_items SET state='verified',lease_token=NULL,lease_expires_at=NULL,
 completion='downloaded',stitch_receipt=$2,stitch_result_sha256=repeat('a',64),etag=$3,version_id=$4,committed_bytes=$5
 WHERE id=$1`, copyID, altered.payload, altered.etag, altered.version, altered.committed); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO nas_stitch_r2_certificates(hour_id,connection_id,attempt,plan_sha256,manifest_sha256,receipt)
 SELECT $1,13,o.attempt,$2,$3,jsonb_build_object('objects',jsonb_build_array(jsonb_build_object('object_key',o.object_key,
  'etag',o.etag,'version_id',o.version_id,'last_modified',o.last_modified,'readback',q.stitch_receipt)))
 FROM nas_stitch_r2_stage_objects o JOIN nas_r2_copy_items q ON q.source_id=o.id AND q.kind='stitch_output'
 WHERE o.id=123`, id, planDigest, manifest); err == nil || !strings.Contains(err.Error(), wantError) {
				t.Fatal("altered NAS evidence passed the certificate guard", err)
			}
		})
	}
	exec(`UPDATE nas_stitch_r2_hours SET state='staged',staged_manifest=$3,staged_manifest_sha256=$2 WHERE id=$1`,
		id, manifest, string(stagedManifest))

	s := &Server{pool: pool, r2: store, cfg: config.Config{NASR2CopyEnabled: true}}
	t.Run("server controls staged copies on the reviewed NAS release", func(t *testing.T) {
		exec(`UPDATE nas_r2_copy_items SET state='pending',lease_token=NULL,lease_expires_at=NULL WHERE id=$1`, copyID)
		for _, check := range []struct {
			version                 string
			master, capability, get bool
		}{
			{NASR2CopyPagedClientVersion, true, true, false},
			{nasCleanupCertifiedStitchVersion, false, true, false},
			{nasCleanupCertifiedStitchVersion, true, false, true},
		} {
			exec(`UPDATE connections SET client_version=$1 WHERE id=13`, check.version)
			s.cfg.NASStitchEnabled = check.master
			w := cleanupCall(t, s.handleNASR2CopyLease, 47, 47, "", "", nasR2CopyControl{
				FreeBytes: request.FreeBytes, AcceptStitch: check.capability,
			}, "fixture-boot "+check.version)
			var reply struct{ Task *nasR2CopyTask }
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil || (reply.Task != nil) != check.get {
				t.Fatalf("staged copy gate differs for %+v: %d %s", check, w.Code, w.Body.String())
			}
			if check.get {
				if reply.Task.Item.Kind != NASR2CopyStitchOutput || reply.Task.ID != copyID {
					t.Fatal("reviewed client got a different staged output")
				}
				request.Token = reply.Task.Token
			}
		}
		w := cleanupCall(t, s.handleAdminNASStitchStatus, 47, 47, "", "", nil)
		var status struct {
			Copy struct {
				Control           string `json:"control"`
				Version           string `json:"client_version"`
				ServerEnabled     bool   `json:"server_enabled"`
				MaintenanceActive bool   `json:"maintenance_active"`
			} `json:"durham_copy"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || status.Copy.Control != "server" ||
			status.Copy.Version != nasCleanupCertifiedStitchVersion || !status.Copy.ServerEnabled || status.Copy.MaintenanceActive {
			t.Fatal("operator status did not show the effective server gate", w.Code, w.Body.String())
		}
	})
	exec(`UPDATE connections SET client_version=$1 WHERE id=13`, NASR2CopyPagedClientVersion)
	startRange := nasR2CopyControl{Token: request.Token, FreeBytes: request.FreeBytes, Length: item.SizeBytes}
	rangeResponse := cleanupCall(t, s.handleNASR2CopyRange, 47, 47, "id", fmt.Sprint(copyID), startRange, "fixture-boot "+NASR2CopyPagedClientVersion)
	var grant struct {
		RangeID string `json:"range_id"`
	}
	if rangeResponse.Code != http.StatusOK || json.Unmarshal(rangeResponse.Body.Bytes(), &grant) != nil || grant.RangeID == "" {
		t.Fatalf("NAS copy range was not granted: %d", rangeResponse.Code)
	}
	startRange.RangeID = grant.RangeID
	if w := cleanupCall(t, s.handleNASR2CopyCheckpoint, 47, 47, "id", fmt.Sprint(copyID), startRange); w.Code != http.StatusOK {
		t.Fatalf("NAS copy range did not acknowledge: %d", w.Code)
	}

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE nas_r2_copy_items SET etag='not-stage-etag' WHERE id=$1`, copyID); err != nil {
		t.Fatal(err)
	}
	if err := finishNASR2StitchCopy(ctx, tx, nasR2CopyTask{ID: copyID, Item: item}, request); err == nil ||
		!strings.Contains(err.Error(), "exact staged generation") {
		t.Fatal("copy completion accepted a malformed exact receipt", err)
	}
	tx.Rollback(ctx)

	// The worker can have published and fsynced the NAS file before the server
	// transaction is lost. In that case the next attempt must retain the hold.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := finishNASR2StitchCopy(ctx, tx, nasR2CopyTask{ID: copyID, Item: item}, request); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET clip_end_at=clip_end_at+interval '1 second' WHERE id=901`); err == nil {
		t.Fatal("catalog mutation crossed the open certificate transaction")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reserve(); err == nil {
		t.Fatal("certificate rollback released the raw")
	}
	var eligible int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_stitch_r2_certified_raw WHERE hour_id=$1`, id).Scan(&eligible); err != nil || eligible != 0 {
		t.Fatalf("rolled-back certificate exposed NAS cleanup eligibility: %d %v", eligible, err)
	}

	post := func(principal int64, req nasR2CopyControl) int {
		out := cleanupCall(t, s.handleNASR2CopyResult, 47, principal, "id", fmt.Sprint(copyID), req)
		if out.Code >= 500 {
			t.Logf("result response: %d %s", out.Code, out.Body.String())
		}
		return out.Code
	}
	if got := post(9, request); got != http.StatusForbidden {
		t.Fatalf("other key certified NAS custody: %d", got)
	}
	exec(`UPDATE recording_clips SET clip_end_at=$1 WHERE id=901`, hour.Add(time.Hour+time.Minute))
	if got := post(47, request); got != http.StatusConflict {
		t.Fatalf("changed input coverage released holds: %d", got)
	}
	if err := reserve(); err == nil {
		t.Fatal("changed input coverage no longer blocks reservation")
	}
	exec(`UPDATE recording_clips SET clip_end_at=$1 WHERE id=901`, end)

	exec(`DELETE FROM nas_inventory_files WHERE connection_id=13 AND clip_id=902`)
	if got := post(47, request); got != http.StatusConflict {
		t.Fatalf("certificate accepted a missing NAS input custody pair: %d", got)
	}
	exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at)
 VALUES(13,902,30,'fixture/raw2.mp4',100,$1,'present',now(),now())`, sha)
	exec(`UPDATE nas_r2_copy_items SET lease_expires_at=clock_timestamp()+interval '2 minutes' WHERE id=$1`, copyID)
	testStitchReceiptNeverWaits(t, pool, func() int { return post(47, request) }, copyID, id, sourceKey, "managed/durham-fixture-2.mp4")
	testStitchReceiptLockBound(t, pool, func() int { return post(47, request) }, copyID)
	if err := testStitchCertKeyContention(t, pool, func(tx pgx.Tx) error {
		return finishNASR2StitchCopy(ctx, tx, nasR2CopyTask{ID: copyID, Item: item}, request)
	}, sourceKey, "managed/keep-ranking.mp4", extra); err != nil {
		t.Fatalf("connection 13 certificate rejected: %v", err)
	}
	if got := post(47, request); got != http.StatusOK {
		t.Fatalf("exact result replay rejected: %d", got)
	}
	conflict := request
	conflict.StitchReceipt = &nasR2StitchReceipt{}
	if got := post(47, conflict); got != http.StatusConflict {
		t.Fatalf("changed result replay accepted: %d", got)
	}
	var certificates int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_stitch_r2_certificates
 WHERE hour_id=$1 AND plan_sha256=$2 AND manifest_sha256=$3`, id, planDigest, manifest).Scan(&certificates); err != nil || certificates != 1 {
		t.Fatalf("expected one exact durable certificate, got %d: %v", certificates, err)
	}
	legacy := cleanupCall(t, s.handleAccountNASStitchLease, 47, 47, "", "", nil)
	var legacyResponse struct {
		Task  *nasStitchTask
		Retry int `json:"retry_after_sec"`
	}
	if legacy.Code != 200 || json.Unmarshal(legacy.Body.Bytes(), &legacyResponse) != nil ||
		legacyResponse.Task != nil || legacyResponse.Retry != 60 {
		t.Fatal("local stitch lane did not yield to the enabled Durham queue", legacy.Code, legacy.Body.String())
	}
	if err := reserve(); err != nil {
		t.Fatalf("committed certificate failed to release the hold: %v", err)
	}
	var readGrantStillLive bool
	if err := pool.QueryRow(ctx, `SELECT range_until IS NULL AND stitch_read_grants_until>clock_timestamp()
 FROM nas_r2_copy_items WHERE id=$1`, copyID).Scan(&readGrantStillLive); err != nil || !readGrantStillLive {
		t.Fatal("copy completion shortened the signed range drain", err)
	}
	if err := store.RetireStitchStage(ctx, 123, sha, r2.StitchStageEvidence{}, r2.StitchStagePicks{}, nil, false); err == nil ||
		!strings.Contains(err.Error(), "reviewed evidence") {
		t.Fatal("staging retirement accepted missing per-stage evidence", err)
	}
	if err := store.RetireStitchStage(ctx, 123, sha, evidence(), decision(), wire, false); err == nil || !strings.Contains(err.Error(), "drain") {
		t.Fatal("certificate bypassed the pending signed grants", err)
	}
	// Advance only the stored grant clocks in this disposable schema to test
	// retirement without waiting for the full network drain windows.
	exec(`ALTER TABLE nas_stitch_r2_stage_objects DISABLE TRIGGER nas_stitch_r2_stage_guard;
 UPDATE nas_stitch_r2_stage_objects SET grants_drain_at=clock_timestamp()-interval '1 second' WHERE id=123;
 ALTER TABLE nas_stitch_r2_stage_objects ENABLE TRIGGER nas_stitch_r2_stage_guard;`)
	if err := store.RetireStitchStage(ctx, 123, sha, evidence(), decision(), wire, false); err == nil || !strings.Contains(err.Error(), "drain") {
		t.Fatal("staging retirement ignored the outstanding signed NAS read grant", err)
	}
	exec(`ALTER TABLE nas_r2_copy_items DISABLE TRIGGER nas_r2_stitch_copy_evidence_guard;
 UPDATE nas_r2_copy_items SET stitch_read_grants_until=clock_timestamp()-interval '1 second' WHERE kind='stitch_output';
 ALTER TABLE nas_r2_copy_items ENABLE TRIGGER nas_r2_stitch_copy_evidence_guard;`)
	// The evidence and the wire must name one exact bucket and endpoint, and the
	// stream sets must be the derivation of the pinned picks. Each negative here
	// is a value an operator could type; none may become authority.
	t.Run("evidence and picks binding cannot be widened", func(t *testing.T) {
		mismatched := apiStageWire{endpoint: strings.Replace(provider.URL, "127.0.0.1", "127.0.0.2", 1)}
		if mismatchEndpoint := evidence(); true {
			if err := store.RetireStitchStage(ctx, 123, sha, mismatchEndpoint, decision(), mismatched, false); err == nil ||
				!strings.Contains(err.Error(), "endpoint differs") {
				t.Fatalf("a wire at another endpoint was admitted: %v", err)
			}
		}
		otherEndpoint := evidence()
		otherEndpoint.EndpointSHA256 = strings.Repeat("f", 64)
		if err := store.RetireStitchStage(ctx, 123, sha, otherEndpoint, decision(), wire, false); err == nil ||
			!strings.Contains(err.Error(), "bind this exact generation") {
			t.Fatalf("evidence for another endpoint was admitted: %v", err)
		}
		// The committed stage row is authoritative. A client built for another
		// endpoint than the one certification recorded must refuse before any
		// wire, whatever its own config and the evidence both say.
		drifted, err := r2.New(ctx, r2.Config{Bucket: "stoarama",
			Endpoint: strings.Replace(provider.URL, "127.0.0.1", "127.0.0.2", 1), Region: "auto",
			AccessKey: "fake", SecretKey: "fake", WriterDB: pool})
		if err != nil {
			t.Fatal(err)
		}
		if err := drifted.RetireStitchStage(ctx, 123, sha, evidence(), decision(), wire, false); err == nil ||
			!strings.Contains(err.Error(), "committed endpoint differs") {
			t.Fatalf("a client configured for another endpoint was admitted: %v", err)
		}
		for _, tc := range []struct {
			name    string
			change  func(*r2.StitchStageEvidence, *r2.StitchStagePicks)
			wantErr string
		}{
			{"forged decided stream", func(e *r2.StitchStageEvidence, _ *r2.StitchStagePicks) {
				e.DecidedStreamIDs = []int64{plan.StreamID, 999999}
			}, "decided streams differ"},
			{"forged ke stream", func(e *r2.StitchStageEvidence, _ *r2.StitchStagePicks) {
				e.KEStreamIDs = []int64{plan.StreamID}
			}, "keep-everything streams differ"},
			{"evidence pins other picks", func(e *r2.StitchStageEvidence, _ *r2.StitchStagePicks) {
				e.PicksSHA256 = strings.Repeat("e", 64)
			}, "different owner picks"},
			{"retirement has no picks", func(_ *r2.StitchStageEvidence, p *r2.StitchStagePicks) {
				*p = r2.StitchStagePicks{}
			}, "different owner picks"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				e, p := evidence(), decision()
				tc.change(&e, &p)
				if err := store.RetireStitchStage(ctx, 123, sha, e, p, wire, false); err == nil ||
					!strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("%s was admitted: %v", tc.name, err)
				}
			})
		}
	})
	// Source-side vetoes are authoritative and fail closed even when the stage
	// key itself is clean. Every case mutates a real committed catalog row in
	// this disposable schema, with only the append-only guard disabled while
	// the fixture is installed.
	t.Run("source-side vetoes fail closed", func(t *testing.T) {
		base := evidence()
		// fixture installs and removes a mutation that the append-only evidence
		// guards would otherwise refuse, so each case starts from a clean schema.
		fixture := func(table, trigger, install, undo string) func() {
			return func() {
				t.Helper()
				if _, err := pool.Exec(ctx, `ALTER TABLE `+table+` DISABLE TRIGGER `+trigger); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx, install); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx, `ALTER TABLE `+table+` ENABLE TRIGGER `+trigger); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if _, err := pool.Exec(ctx, `ALTER TABLE `+table+` DISABLE TRIGGER `+trigger); err != nil {
						t.Error(err)
						return
					}
					if _, err := pool.Exec(ctx, undo); err != nil {
						t.Error(err)
					}
					if _, err := pool.Exec(ctx, `ALTER TABLE `+table+` ENABLE TRIGGER `+trigger); err != nil {
						t.Error(err)
					}
				})
			}
		}
		for _, tc := range []struct {
			name string
			// evidence and picks are changed together, so the binding gate passes
			// and the refusal must come from the source policy itself.
			evidence func(r2.StitchStageEvidence) r2.StitchStageEvidence
			picks    func(r2.StitchStagePicks) r2.StitchStagePicks
			install  func()
		}{
			// The pinned picks do not decide this hour's stream at all.
			{"undecided stream",
				func(e r2.StitchStageEvidence) r2.StitchStageEvidence { e.DecidedStreamIDs = []int64{}; return e },
				func(p r2.StitchStagePicks) r2.StitchStagePicks { p.Decided = []int64{}; return p }, nil},
			// The pinned picks keep this whole stream, so no stage of it is
			// retirement authority however clean the stage key looks.
			{"keep-everything stream",
				func(e r2.StitchStageEvidence) r2.StitchStageEvidence {
					e.KEStreamIDs = []int64{plan.StreamID}
					return e
				},
				func(p r2.StitchStagePicks) r2.StitchStagePicks { p.KE = []int64{plan.StreamID}; return p }, nil},
			// The 0220 owner thesis keep covers the source, not the stage key.
			{"owner thesis prefix", func(e r2.StitchStageEvidence) r2.StitchStageEvidence { return e },
				func(p r2.StitchStagePicks) r2.StitchStagePicks { return p },
				fixture("r2_owner_keep_prefixes", "r2_owner_keep_prefix_immutable",
					`INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision)
 VALUES('stoarama','raw/stream/`+fmt.Sprint(plan.StreamID)+`/','fixture thesis keep')`,
					`DELETE FROM r2_owner_keep_prefixes WHERE prefix='raw/stream/`+fmt.Sprint(plan.StreamID)+`/'`)},
			// A committed source that no longer matches its certified hold.
			{"changed committed source", func(e r2.StitchStageEvidence) r2.StitchStageEvidence { return e },
				func(p r2.StitchStagePicks) r2.StitchStagePicks { return p },
				fixture("recording_clips", "r2_inverse_source_identity_fence",
					`UPDATE recording_clips SET sha256=repeat('b',64) WHERE id=902`,
					`UPDATE recording_clips SET sha256='`+sha+`' WHERE id=902`)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if tc.install != nil {
					tc.install()
				}
				if err := store.RetireStitchStage(ctx, 123, sha, tc.evidence(base), tc.picks(decision()), wire, false); err == nil ||
					!strings.Contains(err.Error(), "provenance") {
					t.Fatalf("source veto %q was not refused: %v", tc.name, err)
				}
			})
		}
	})
	staleFence, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer staleFence.Rollback(ctx)
	var initialIntents int
	if err := staleFence.QueryRow(ctx, `SELECT count(*) FROM nas_stitch_r2_stage_retirements`).Scan(&initialIntents); err != nil ||
		initialIntents != 0 {
		t.Fatal("could not establish an old snapshot before staging DELETE", err)
	}
	// Two real invocations race across the separately committed one-shot
	// reservation while a RAW batch owns the executors' outer lock. The second
	// cannot send while the first is on the wire.
	release := holdSessionLock(t, pool, "7343226011173")
	// Registered before any assertion: a failure above must release the session
	// lock, the held DELETE and the staging-race transaction instead of leaving
	// them checked out for pool.Close.
	defer release()
	defer func() {
		abort.holdDelete.Store(false)
		abort.release()
	}()
	abort.holdDelete.Store(true)
	racing := make(chan error, 1)
	go func() { racing <- store.RetireStitchStage(ctx, 123, sha, evidence(), decision(), wire, false) }()
	within(t, abort.deleteEntered, "first staging DELETE")
	var reserved bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements WHERE object_id=123)`).Scan(&reserved); err != nil || !reserved {
		t.Fatal("first invocation reached the wire before its one-shot reservation committed", err)
	}
	if err := store.RetireStitchStage(ctx, 123, sha, evidence(), decision(), wire, false); err == nil || !strings.Contains(err.Error(), "stitch stage retirement busy") {
		t.Fatal("a concurrent staging retirement invocation was admitted", err)
	}
	abort.holdDelete.Store(false)
	abort.release()
	if err := within(t, racing, "first staging retirement"); err == nil || !strings.Contains(err.Error(), "outcome unconfirmed") {
		t.Fatalf("expected the provider to lose the DELETE result after both grants drained: %v", err)
	}
	release()
	var stagingAbsent bool
	if err := pool.QueryRow(ctx, `SELECT nas_cleanup_certified_stitch_staging_absent(certificate)
 FROM nas_stitch_r2_certified_raw WHERE clip_id=901`).Scan(&stagingAbsent); err != nil || stagingAbsent {
		t.Fatalf("unconfirmed staging DELETE became NAS unlink authority: %v %v", stagingAbsent, err)
	}
	if _, err := staleFence.Exec(ctx, `DELETE FROM r2_retirement_writer_fences`); err == nil || !strings.Contains(err.Error(), "READ COMMITTED") {
		t.Fatal("stale transaction snapshot removed the writer fence", err)
	}
	staleFence.Rollback(ctx)
	if _, err := pool.Exec(ctx, `DELETE FROM r2_retirement_writer_fences`); err == nil || !strings.Contains(err.Error(), "have not drained") {
		t.Fatal("unresolved staging delete allowed writer fence removal", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind)
 VALUES('stoarama',$1,'stage-etag',100,'metadata')`, key); err == nil || !strings.Contains(err.Error(), "protects dependency admission") {
		t.Fatal("unresolved staging delete allowed a conflicting delivery hold", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET object_key=$1,display_path='' WHERE id=901`, item.RelativePath); err == nil ||
		!strings.Contains(err.Error(), "protects dependency admission") {
		t.Fatal("derived raw path bypassed unresolved stage dependency", err)
	}
	if err := store.RetireStitchStage(ctx, 123, sha, evidence(), decision(), wire, false); err == nil ||
		!strings.Contains(err.Error(), "reconciliation") {
		t.Fatal("ambiguous staging delete allowed a second wire attempt", err)
	}
	if got := abort.deletes.Load(); got != 1 {
		t.Fatalf("staging retirement sent %d DELETEs", got)
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE nas_stitch_r2_stage_retirements
 SET absence_observed_at=statement_timestamp(),strict_absence_observed_at=statement_timestamp() WHERE object_id=123`); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, `SELECT nas_cleanup_certified_stitch_staging_absent(certificate)
 FROM nas_stitch_r2_certified_raw WHERE clip_id=901`).Scan(&stagingAbsent); err != nil || stagingAbsent {
		t.Fatalf("unresolved abort on the earlier attempt cleared staging: %v %v", stagingAbsent, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	t.Run("certified NAS cleanup waits for exact R2 retirement", func(t *testing.T) {
		var legacyObservedAt time.Time
		if err := pool.QueryRow(ctx, `UPDATE nas_stitch_r2_stage_retirements SET absence_observed_at=clock_timestamp()
 WHERE object_id=123 RETURNING absence_observed_at`).Scan(&legacyObservedAt); err != nil {
			t.Fatal(err)
		}
		testCertifiedStitchNASCleanup(t, s, id, sourceKey, &abort, func() {
			for _, mode := range []int32{0, 1, 2, 4, 5, 6, 7, 8, 9} {
				abort.response.Store(mode)
				if err := store.RetireStitchStage(ctx, 122, sha, r2.StitchStageEvidence{}, r2.StitchStagePicks{}, nil, true); err == nil {
					t.Fatalf("unconfirmed aborted stage accepted in provider case %d", mode)
				}
			}
			abort.response.Store(3)
			// Another staged object's retirement never fences this one.
			release := holdSessionLock(t, pool, stitchStageGuard(123))
			for range 2 {
				if err := store.RetireStitchStage(ctx, 122, sha, r2.StitchStageEvidence{}, r2.StitchStagePicks{}, nil, true); err != nil {
					t.Fatalf("clean aborted stage absence or exact replay refused: %v", err)
				}
			}
			release()
			var receipt, deletionIntent bool
			if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_stitch_r2_aborted_absences WHERE object_id=122),
 EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements WHERE object_id=122)`).Scan(&receipt, &deletionIntent); err != nil || !receipt || deletionIntent {
				t.Fatalf("aborted reconciliation did not preserve evidence separation: %v %v %v", receipt, deletionIntent, err)
			}
			for _, mode := range []int32{1, 2} {
				stageHeadResponse.Store(mode)
				if err := store.RetireStitchStage(ctx, 123, sha, r2.StitchStageEvidence{}, r2.StitchStagePicks{}, nil, true); err == nil {
					t.Fatalf("ambiguous staged HEAD supplemented a legacy receipt in provider case %d", mode)
				}
			}
			if err := pool.QueryRow(ctx, `SELECT nas_cleanup_certified_stitch_staging_absent(certificate)
 FROM nas_stitch_r2_certified_raw WHERE clip_id=901`).Scan(&stagingAbsent); err != nil || stagingAbsent {
				t.Fatalf("legacy staged receipt without strict confirmation cleared NAS gate: %v %v", stagingAbsent, err)
			}
			stageHeadResponse.Store(0)
			// The exact key is now unreadable at the provider, but this object's
			// own DELETE answered 503 and was never confirmed. Standing policy:
			// a later observation must never publish absence for an unresolved
			// intent. Reconcile stays refused and the row stays held.
			if err := store.RetireStitchStage(ctx, 123, sha, r2.StitchStageEvidence{}, r2.StitchStagePicks{}, nil, true); err == nil ||
				!strings.Contains(err.Error(), "remains unresolved") {
				t.Fatalf("a later absence observation published an unconfirmed intent: %v", err)
			}
			var stillHeld bool
			if err := pool.QueryRow(ctx, `SELECT absence_observed_at=$1 AND strict_absence_observed_at IS NULL
 FROM nas_stitch_r2_stage_retirements WHERE object_id=123`, legacyObservedAt).Scan(&stillHeld); err != nil || !stillHeld {
				t.Fatalf("an unconfirmed intent was edited by a later observation: %v %v", stillHeld, err)
			}
			// The gate below is hour-wide, so opening it requires this object to
			// be strictly absent. Model that as a *legacy truly confirmed row*:
			// a prior invocation's own successful DELETE recorded both the
			// original observation and its strict confirmation. This fixture
			// writes it directly to set up the gate; it is never something this
			// command may do from an unresolved intent, which is asserted above.
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := pool.Exec(ctx, sql, args...); err != nil {
					t.Fatal(err)
				}
			}
			exec(`UPDATE nas_stitch_r2_stage_retirements
 SET strict_absence_observed_at=clock_timestamp() WHERE object_id=123 AND strict_absence_observed_at IS NULL`)
		})
	})
	for _, change := range []string{
		`DELETE FROM nas_stitch_r2_stage_retirements WHERE object_id=123`,
		`UPDATE nas_stitch_r2_stage_retirements SET object_id=456 WHERE object_id=123`,
		`UPDATE nas_stitch_r2_stage_retirements SET absence_observed_at=NULL WHERE object_id=123`,
		`UPDATE nas_stitch_r2_stage_retirements SET strict_absence_observed_at=NULL WHERE object_id=123`,
	} {
		if _, err := pool.Exec(ctx, change); err == nil || !strings.Contains(err.Error(), "one-shot") {
			t.Fatal("staging retirement reservation history could be erased or reassigned", err)
		}
	}
}

type durhamStageAbortProbe struct {
	response, lists, heads atomic.Int32
	// deleteConfirmed answers a stage DELETE with 204 instead of the default
	// 503, so a fixture can model the one path that may publish absence: this
	// invocation's own answered DELETE followed by a strict absence read.
	deleteConfirmed atomic.Bool
	// Object DELETEs are counted. While holdDelete is set they wait for
	// release; the first closes deleteEntered.
	deletes                      atomic.Int32
	holdDelete                   atomic.Bool
	deleteEntered, deleteRelease chan struct{}
	entered, released            sync.Once
}

// release frees every held DELETE; the test defers it ahead of the server's
// Close so a regression fails instead of hanging.
func (p *durhamStageAbortProbe) release() { p.released.Do(func() { close(p.deleteRelease) }) }

func durhamStageTestProvider(t *testing.T, media []byte, sha, abortKey string, abort *durhamStageAbortProbe, stageHeadResponse *atomic.Int32) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	var started, part, completed, deleted bool
	var modified time.Time
	sum := sha256.Sum256(media)
	checksum := base64.StdEncoding.EncodeToString(sum[:])
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && !r.URL.Query().Has("uploadId") {
			abort.deletes.Add(1)
			if abort.holdDelete.Load() {
				abort.entered.Do(func() { close(abort.deleteEntered) })
				<-abort.deleteRelease
			}
		}
		mu.Lock()
		defer mu.Unlock()
		query := r.URL.Query()
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == http.MethodGet && r.URL.Path == "/stoarama" && query.Has("uploads") &&
			strings.HasPrefix(query.Get("prefix"), "stitch-staging/") {
			bucket, prefix := "stoarama", query.Get("prefix")
			markers := ""
			if prefix == abortKey {
				abort.lists.Add(1)
				switch abort.response.Load() {
				case 0:
					fmt.Fprintf(w, "<ListMultipartUploadsResult><Bucket>%s</Bucket><Prefix>%s</Prefix><IsTruncated>false</IsTruncated><Upload><Key>%s</Key><UploadId>still-live</UploadId></Upload></ListMultipartUploadsResult>", bucket, prefix, abortKey)
					return
				case 6:
					prefix = "stitch-staging/wrong-prefix"
				case 7:
					bucket = "wrong-bucket"
				case 8:
					markers = "<KeyMarker>past-key</KeyMarker>"
				case 9:
					markers = "<UploadIdMarker>past-upload</UploadIdMarker>"
				}
			}
			fmt.Fprintf(w, "<ListMultipartUploadsResult><Bucket>%s</Bucket><Prefix>%s</Prefix>%s<IsTruncated>false</IsTruncated></ListMultipartUploadsResult>", bucket, prefix, markers)
			return
		}
		if r.URL.Path == "/stoarama/"+abortKey {
			if r.Method != http.MethodHead {
				t.Errorf("abort reconciliation attempted %s", r.Method)
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			abort.heads.Add(1)
			switch abort.response.Load() {
			case 1:
				w.WriteHeader(http.StatusServiceUnavailable)
			case 2:
				w.WriteHeader(http.StatusOK)
			case 3:
				w.WriteHeader(http.StatusNotFound)
			case 4:
				w.Header().Set("x-amz-delete-marker", "true")
				w.WriteHeader(http.StatusNotFound)
			case 5:
				w.Header().Set("x-amz-error-code", "NoSuchBucket")
				w.WriteHeader(http.StatusNotFound)
			default:
				t.Error("abort reconciliation performed HEAD without a matching empty multipart listing")
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/stoarama/stitch-staging/") {
			t.Error("typed staging request escaped its namespace")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch {
		case r.Method == http.MethodPost && query.Has("uploads") && !started:
			started = true
			fmt.Fprint(w, "<InitiateMultipartUploadResult><UploadId>provider-fixture</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut && query.Get("uploadId") == "provider-fixture" && query.Get("partNumber") == "1" && started:
			data, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Equal(data, media) || r.Header.Get("X-Amz-Checksum-Sha256") != checksum {
				t.Error("staging request differs from its exact signed part")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			part = true
		case r.Method == http.MethodGet && query.Get("uploadId") == "provider-fixture" && part:
			fmt.Fprintf(w, "<ListPartsResult><IsTruncated>false</IsTruncated><Part><PartNumber>1</PartNumber><ETag>&#34;part-etag&#34;</ETag><Size>%d</Size><ChecksumSHA256>%s</ChecksumSHA256></Part></ListPartsResult>", len(media), checksum)
		case r.Method == http.MethodPost && query.Get("uploadId") == "provider-fixture" && r.Header.Get("If-None-Match") == "*" && part && !completed:
			completed = true
			modified = time.Now().UTC().Truncate(time.Second)
			fmt.Fprint(w, "<CompleteMultipartUploadResult><ETag>&#34;stage-etag&#34;</ETag></CompleteMultipartUploadResult>")
		case r.Method == http.MethodDelete && completed && !deleted && r.Header.Get("If-Match") == "" && r.URL.Query().Get("x-id") == "DeleteObject":
			deleted = true
			if abort.deleteConfirmed.Load() {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		case r.Method == http.MethodHead && deleted:
			switch stageHeadResponse.Load() {
			case 1:
				w.Header().Set("x-amz-delete-marker", "true")
			case 2:
				w.Header().Set("x-amz-error-code", "NoSuchBucket")
			}
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodHead && completed:
			w.Header().Set("ETag", `"stage-etag"`)
			w.Header().Set("Last-Modified", modified.Format(http.TimeFormat))
			w.Header().Set("Content-Length", fmt.Sprint(len(media)))
			w.Header().Set("X-Amz-Meta-Sha256", sha)
		default:
			t.Errorf("unexpected stitch stage method %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

// apiStageWire is the api package's test wire: one direct no-redirect HEAD and
// one direct DELETE, exactly the provider requests the cmd-level audited
// transport sends. The cmd package owns that transport's regression tests and
// this package owns the database, lock and evidence path.
type apiStageWire struct{ endpoint string }

func (w apiStageWire) EndpointSHA256() string { return cleanupSHA([]byte(w.endpoint)) }

// bound refuses any target that does not name this exact endpoint, so a
// mismatched provider pairing is visible here too.
func (w apiStageWire) bound(t r2.StitchStageTarget) error {
	if t.EndpointSHA256 != w.EndpointSHA256() || t.Bucket != "stoarama" {
		return fmt.Errorf("test wire endpoint/bucket differs: %q %q", t.EndpointSHA256, t.Bucket)
	}
	return nil
}

func (w apiStageWire) client() *http.Client {
	return &http.Client{Transport: http.DefaultTransport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

func (w apiStageWire) ExactHead(ctx context.Context, t r2.StitchStageTarget, deadline time.Time) (r2.ObjectHead, error) {
	if err := w.bound(t); err != nil {
		return r2.ObjectHead{}, err
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, w.endpoint+"/"+t.Bucket+"/"+t.Key, nil)
	if err != nil {
		return r2.ObjectHead{}, err
	}
	resp, err := w.client().Do(req)
	if err != nil {
		return r2.ObjectHead{}, err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return r2.ObjectHead{}, fmt.Errorf("head status %d", resp.StatusCode)
	}
	modified, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return r2.ObjectHead{LastModified: modified, ETag: strings.Trim(resp.Header.Get("ETag"), `"`),
		SizeBytes: size, SHA256Metadata: resp.Header.Get("X-Amz-Meta-Sha256")}, nil
}

func (w apiStageWire) DeleteOnce(ctx context.Context, t r2.StitchStageTarget, deadline time.Time, recheck func(context.Context) error) error {
	if err := w.bound(t); err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := recheck(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, w.endpoint+"/"+t.Bucket+"/"+t.Key+"?x-id=DeleteObject", nil)
	if err != nil {
		return err
	}
	resp, err := w.client().Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("delete status %d", resp.StatusCode)
	}
	return nil
}

func TestNASR2StitchReceiptIdentityRejectsJSONNormalization(t *testing.T) {
	for _, data := range []string{
		`[100,1,2,3]`, `[100,1,2,3,0,9]`, `[100,1,2,3,null]`, `[100,1.0,2,3,0]`, `[100,"1",2,3,0]`, `null`,
	} {
		var receipt nasR2StitchReceipt
		if err := json.Unmarshal([]byte(`{"identity":`+data+`}`), &receipt); err == nil {
			t.Fatalf("accepted non-exact NAS file identity %s", data)
		}
	}
	var receipt nasR2StitchReceipt
	if err := json.Unmarshal([]byte(`{"identity":[100,1,2,3,0]}`), &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Identity != [5]int64{100, 1, 2, 3, 0} {
		t.Fatal("exact receipt identity did not round-trip")
	}
}
