package api

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/collation"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDurhamHoldReleasedOnlyByCommittedNASCertificate(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
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
	sha := strings.Repeat("a", 64)
	const sourceKey = "managed/durham-fixture.mp4"
	const path = "fixture/raw.mp4"
	exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,
 fire_at,clip_start_at,clip_end_at,display_path) VALUES(901,30,1,'https://storage.example.test','stoarama',$1,100,'source-etag',$2,$3,$3,$4,$5)`, sourceKey, sha, hour, end, path)
	exec(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at)
 VALUES(13,901,30,$1,100,$2,'present',now(),now())`, path, sha)
	exec(`INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until)
 VALUES(1,3,$1,$1::timestamptz+interval '6 hours','test',clock_timestamp()+interval '1 hour')`, hour)
	endpoint := cleanupSHA([]byte("https://storage.example.test"))
	exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, endpoint)

	plan := nasStitchPlan{Policy: nasStitchPolicy, Revision: nasStitchRevision, CorpusSHA: nasStitchCorpusSHA, KeepSHA: cleanupPolicySHA,
		StreamID: 3, RecordingID: 30, OutputFolder: "fixture_V85r_keep28/30/" + hour.Format("2006-01-02T15-04-05Z"),
		Eligible: []bool{true}, Clips: []nasCleanupItem{{SourceID: 901, RecordingID: 30, StreamID: 3, RelativePath: path,
			SizeBytes: 100, SHA256: sha, StartAt: hour, EndAt: end}},
		FeatureInputs: []collation.Clip{{ClipID: 901, RecordingID: 30, SHA256: sha, SizeBytes: 100, StartUTC: hour, EndUTC: end, NominalSeconds: 60}}}
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	set := DurhamStitchHoldSet{Bucket: "stoarama", EndpointSHA: endpoint, Hours: []DurhamStitchHour{{
		ScopeID: 1, Hour: hour, Plan: data, Inputs: []DurhamStitchInput{{ClipID: 901, Key: sourceKey, ETag: "source-etag", SHA: sha, Size: 100}},
	}}}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if n, err := ImportDurhamStitchHoldSet(ctx, tx, set); n != 1 || err != nil {
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
	var id int64
	var attempt int
	var raw []byte
	var planDigest string
	if err := pool.QueryRow(ctx, nasDurhamClaimSQL).Scan(&id, &attempt, &raw, &planDigest); err != nil || attempt != 0 {
		t.Fatalf("exact held hour not claimable: %v", err)
	}

	manifest := strings.Repeat("b", 64)
	exec(`UPDATE nas_stitch_r2_hours SET state='leased',attempt=1,lease_api_key_id=47,lease_token=repeat('e',64),
 lease_expires_at=clock_timestamp()+interval '1 minute' WHERE id=$1`, id)
	exec(`INSERT INTO nas_stitch_r2_stage_objects(id,hour_id,attempt,api_key_id,bucket,endpoint_sha256,relative_path,sha256,size_bytes)
 OVERRIDING SYSTEM VALUE VALUES(123,$1,1,47,'stoarama',$2,$3,$4,100)`,
		id, endpoint, plan.OutputFolder+"/run-000-"+sha+".mp4", sha)
	exec(`UPDATE nas_stitch_r2_stage_objects SET state='uploading',upload_id='provider-fixture' WHERE id=123;
 UPDATE nas_stitch_r2_stage_objects SET state='completing' WHERE id=123;
 UPDATE nas_stitch_r2_stage_objects SET state='staged',etag='stage-etag',last_modified=clock_timestamp() WHERE id=123;`)
	exec(`UPDATE nas_stitch_r2_hours SET state='staged',staged_manifest='{}',staged_manifest_sha256=$2 WHERE id=$1`, id, manifest)
	exec(`INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,stitch_receipt)
 SELECT 13,'stitch_output',o.id,$1,$2,jsonb_build_object('bucket',o.bucket,'object_key',o.object_key),'verified',
  jsonb_build_object('relative_path',o.relative_path,'sha256',o.sha256,'size_bytes',o.size_bytes,'fsynced',true)
 FROM nas_stitch_r2_stage_objects o WHERE o.id=123`, manifest, cleanupPolicySHA)
	cert := func(tx pgx.Tx, match string) error {
		_, err := tx.Exec(ctx, `INSERT INTO nas_stitch_r2_certificates(hour_id,connection_id,attempt,plan_sha256,manifest_sha256,receipt)
 SELECT $1,13,1,$2,$3,jsonb_build_object('objects',jsonb_build_array(jsonb_build_object('object_key',o.object_key,'etag',o.etag,
  'version_id',o.version_id,'last_modified',o.last_modified,'readback',q.stitch_receipt)))
 FROM nas_stitch_r2_stage_objects o JOIN nas_r2_copy_items q ON q.source_id=o.id AND q.kind='stitch_output'
 WHERE o.id=123`, id, planDigest, match)
		return err
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cert(tx, strings.Repeat("c", 64)); err == nil {
		t.Fatal("accepted mismatched manifest")
	}
	tx.Rollback(ctx)
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cert(tx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reserve(); err == nil {
		t.Fatal("certificate rollback released the raw")
	}
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := cert(tx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := reserve(); err != nil {
		t.Fatalf("committed certificate failed to release the hold: %v", err)
	}
}
