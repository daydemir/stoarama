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
	var abort durhamStageAbortProbe
	var stageHeadResponse atomic.Int32
	provider := durhamStageTestProvider(t, media, sha, abortKey, &abort, &stageHeadResponse)
	defer provider.Close()
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
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
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
	proofExpiry := time.Now().Add(time.Hour)
	if err := store.RetireStitchStage(ctx, 123, sha, proofExpiry, false); err == nil || !strings.Contains(err.Error(), "writer fence and drain") {
		t.Fatal("staging retirement accepted an undrained fence", err)
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
 lease_expires_at=clock_timestamp()+interval '1 minute' WHERE id=$1`, id)
	exec(`INSERT INTO nas_stitch_r2_stage_objects(id,hour_id,attempt,api_key_id,bucket,endpoint_sha256,generation,relative_path,sha256,size_bytes)
 OVERRIDING SYSTEM VALUE VALUES(122,$1,1,47,'stoarama',$2,$3,'fixture/aborted.mp4',$4,100)`,
		id, endpoint, abortGeneration, sha)
	exec(`UPDATE nas_stitch_r2_stage_objects SET state='uploading',upload_id='earlier-abort',start_issued_at=clock_timestamp(),
 grants_drain_at=clock_timestamp()-interval '1 second' WHERE id=122;
 UPDATE nas_stitch_r2_stage_objects SET state='aborted' WHERE id=122`)
	exec(`UPDATE nas_stitch_r2_hours SET attempt=2,lease_toolchain_attempt=2,lease_toolchain_sha256=$2 WHERE id=$1`, id, nasDurhamManifestSHA)
	t.Run("reclaimed legacy lease does not inherit the native profile", func(t *testing.T) {
		// The previous binary replaces the attempt and token without knowing the profile columns.
		token := strings.Repeat("f", 64)
		exec(`UPDATE nas_stitch_r2_hours SET state='leased',attempt=3,lease_api_key_id=47,lease_token=$2,
 lease_expires_at=clock_timestamp()+interval '1 minute' WHERE id=$1`, id, token)
		defer exec(`UPDATE nas_stitch_r2_hours SET attempt=2,lease_token=repeat('e',64) WHERE id=$1`, id)
		server := &Server{pool: pool}
		w := cleanupCall(t, func(w http.ResponseWriter, r *http.Request) {
			server.handleDurhamStitchStatus(w, r, nasStitchCaller{keyID: 47, accountID: 47}, id)
		}, 47, 47, "", "", durhamStitchCheckpoint{Attempt: 3, Token: token, PlanSHA: planDigest})
		if w.Code != http.StatusConflict {
			t.Fatalf("legacy reclaim inherited a native ready response: %d %s", w.Code, w.Body.String())
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
	_, legacy := stitchUnitFixture()
	legacyResult.Outputs[0].Provenance = legacy.Outputs[0].Provenance
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
	lockClip, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockClip.Rollback(ctx)
	var lockPID int32
	if err := lockClip.QueryRow(ctx, `SELECT pg_backend_pid() FROM recording_clips WHERE id=901 FOR UPDATE`).Scan(&lockPID); err != nil {
		t.Fatal(err)
	}
	posted := make(chan int, 1)
	go func() { posted <- post(47, request) }()
	for deadline := time.Now().Add(time.Second); ; {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, lockPID).Scan(&waiting)
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("certificate did not wait at its inventory pair lock: %v", err)
		}
		if waiting {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at)
 VALUES(13,902,30,'fixture/raw2.mp4',100,$1,'present',now(),now())`, sha)
	if err := lockClip.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if got := <-posted; got != http.StatusConflict {
		t.Fatalf("certificate accepted an inventory row inserted after its lock snapshot: %d", got)
	}
	if got := post(47, request); got != http.StatusOK {
		t.Fatalf("connection 13 certificate rejected: %d", got)
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
	if err := store.RetireStitchStage(ctx, 123, sha, time.Time{}, false); err == nil || !strings.Contains(err.Error(), "conditional staging delete proof") {
		t.Fatal("staging retirement accepted missing provider evidence", err)
	}
	if err := store.RetireStitchStage(ctx, 123, sha, proofExpiry, false); err == nil || !strings.Contains(err.Error(), "drain") {
		t.Fatal("certificate bypassed the pending signed grants", err)
	}
	// Advance only the stored grant clocks in this disposable schema to test
	// retirement without waiting for the full network drain windows.
	exec(`ALTER TABLE nas_stitch_r2_stage_objects DISABLE TRIGGER nas_stitch_r2_stage_guard;
 UPDATE nas_stitch_r2_stage_objects SET grants_drain_at=clock_timestamp()-interval '1 second' WHERE id=123;
 ALTER TABLE nas_stitch_r2_stage_objects ENABLE TRIGGER nas_stitch_r2_stage_guard;`)
	if err := store.RetireStitchStage(ctx, 123, sha, proofExpiry, false); err == nil || !strings.Contains(err.Error(), "drain") {
		t.Fatal("staging retirement ignored the outstanding signed NAS read grant", err)
	}
	exec(`ALTER TABLE nas_r2_copy_items DISABLE TRIGGER nas_r2_stitch_copy_evidence_guard;
 UPDATE nas_r2_copy_items SET stitch_read_grants_until=clock_timestamp()-interval '1 second' WHERE kind='stitch_output';
 ALTER TABLE nas_r2_copy_items ENABLE TRIGGER nas_r2_stitch_copy_evidence_guard;`)
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
	if err := store.RetireStitchStage(ctx, 123, sha, proofExpiry, false); err == nil ||
		!strings.Contains(err.Error(), "outcome unconfirmed") {
		t.Fatalf("expected the provider to lose the DELETE result after both grants drained: %v", err)
	}
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
	if err := store.RetireStitchStage(ctx, 123, sha, proofExpiry, false); err == nil ||
		!strings.Contains(err.Error(), "reconciliation") {
		t.Fatal("ambiguous staging delete allowed a second wire attempt", err)
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
				if err := store.RetireStitchStage(ctx, 122, sha, time.Time{}, true); err == nil {
					t.Fatalf("unconfirmed aborted stage accepted in provider case %d", mode)
				}
			}
			abort.response.Store(3)
			for range 2 {
				if err := store.RetireStitchStage(ctx, 122, sha, time.Time{}, true); err != nil {
					t.Fatalf("clean aborted stage absence or exact replay refused: %v", err)
				}
			}
			var receipt, deletionIntent bool
			if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_stitch_r2_aborted_absences WHERE object_id=122),
 EXISTS(SELECT 1 FROM nas_stitch_r2_stage_retirements WHERE object_id=122)`).Scan(&receipt, &deletionIntent); err != nil || !receipt || deletionIntent {
				t.Fatalf("aborted reconciliation did not preserve evidence separation: %v %v %v", receipt, deletionIntent, err)
			}
			for _, mode := range []int32{1, 2} {
				stageHeadResponse.Store(mode)
				if err := store.RetireStitchStage(ctx, 123, sha, time.Time{}, true); err == nil {
					t.Fatalf("ambiguous staged HEAD supplemented a legacy receipt in provider case %d", mode)
				}
			}
			if err := pool.QueryRow(ctx, `SELECT nas_cleanup_certified_stitch_staging_absent(certificate)
 FROM nas_stitch_r2_certified_raw WHERE clip_id=901`).Scan(&stagingAbsent); err != nil || stagingAbsent {
				t.Fatalf("legacy staged receipt without strict confirmation cleared NAS gate: %v %v", stagingAbsent, err)
			}
			stageHeadResponse.Store(0)
			if err := store.RetireStitchStage(ctx, 123, sha, time.Time{}, true); err != nil {
				t.Fatalf("staging retirement could not reconcile exact absence: %v", err)
			}
			var originalPreserved bool
			if err := pool.QueryRow(ctx, `SELECT absence_observed_at=$1 AND strict_absence_observed_at>=absence_observed_at
 FROM nas_stitch_r2_stage_retirements WHERE object_id=123`, legacyObservedAt).
				Scan(&originalPreserved); err != nil || !originalPreserved {
				t.Fatalf("strict staging reconciliation changed the legacy observation: %v %v", originalPreserved, err)
			}
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
}

func durhamStageTestProvider(t *testing.T, media []byte, sha, abortKey string, abort *durhamStageAbortProbe, stageHeadResponse *atomic.Int32) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	var started, part, completed, deleted bool
	var modified time.Time
	sum := sha256.Sum256(media)
	checksum := base64.StdEncoding.EncodeToString(sum[:])
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		case r.Method == http.MethodDelete && completed && !deleted && r.Header.Get("If-Match") == `"stage-etag"`:
			deleted = true
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
