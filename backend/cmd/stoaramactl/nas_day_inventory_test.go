package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/db"
)

func TestReadNASDayInventoryDays(t *testing.T) {
	tsv := "recording_id\tstream_id\tlocal_date\tday_raw\n335\t17216\t2026-09-25\tR2\n335\t17216\t2026-09-26\tR2\n337\t78\t2026-08-27\tboth\n"
	days, err := readNASDayInventoryDays(strings.NewReader(tsv), []int64{335})
	if err != nil || fmt.Sprint(days) != "[{335 2026-09-25} {335 2026-09-26}]" {
		t.Fatalf("days %v err=%v", days, err)
	}
	if all, err := readNASDayInventoryDays(strings.NewReader(tsv), nil); err != nil || len(all) != 3 {
		t.Fatalf("all days %v err=%v", all, err)
	}
	for name, bad := range map[string]struct {
		tsv  string
		only []int64
	}{
		"no header columns": {"recording\tdate\n1\t2026-09-25\n", nil},
		"bad date":          {"recording_id\tlocal_date\n1\t2026-9-25\n", nil},
		"bad id":            {"recording_id\tlocal_date\n0\t2026-09-25\n", nil},
		"repeated day":      {"recording_id\tlocal_date\n1\t2026-09-25\n1\t2026-09-25\n", nil},
		"short line":        {"recording_id\tlocal_date\n1\n", nil},
		"absent recording":  {tsv, []int64{335, 999}},
		"empty":             {"", nil},
	} {
		if _, err := readNASDayInventoryDays(strings.NewReader(bad.tsv), bad.only); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// The operator queues whole days or none, reads per-day totals, and the
// restore queues an inventoried clip, unpurged and outside Keep Everything,
// from the path the inventory read.
func TestNASDayInventoryRequestStatusAndRestoreQueue(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "nas_day_inventory")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO accounts(id,email) VALUES(47,'nas-inventory@example.invalid');
INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(47,47,'fixture-key','fixture-key');
INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',47);
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(17216,'test','17216','brzeg','brzeg','https://storage.example.test','off','Europe/Warsaw','hls','video_manifest','video_live','continuous_video',30),
       (5,'test','5','unpinned','unpinned','https://storage.example.test','off','Europe/Warsaw','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,managed)
 VALUES(1,47,'test','https://storage.example.test','auto','stoarama','fixture',decode('00','hex'),true);
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(335,47,1,'brzeg','https://storage.example.test','* * * * *','Europe/Warsaw','paused',17216,now()),
       (336,47,1,'unpinned','https://storage.example.test','* * * * *','Europe/Warsaw','paused',5,now());
INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 SELECT 900+i,335,1,'https://storage.example.test','stoarama','managed/acct-47/c'||i||'.mp4',10,'etag',repeat(i::text,64),
  t,t,t+interval '2 minutes','recordings/335/continuous/c'||i||'.mp4'
 FROM generate_series(1,3) i, LATERAL (SELECT '2026-09-25T08:00:00+02:00'::timestamptz+i*interval '2 minutes' t) x`); err != nil {
		t.Fatal(err)
	}
	days := []nasDayInventoryDay{{335, "2026-09-25"}, {335, "2026-09-26"}}
	if _, err := requestNASDayInventories(ctx, pool, "gss", 3, false, days); err == nil || !strings.Contains(err.Error(), "nas_restore_day_scope") {
		t.Fatalf("unapproved days queued: %v", err)
	}
	if n, err := scopeNASDayInventories(ctx, pool, "GSS best-14", true, days); err != nil || n != 2 {
		t.Fatalf("scope dry run %d err=%v", n, err)
	}
	if n, err := scopeNASDayInventories(ctx, pool, "GSS best-14", false, days); err != nil || n != 2 {
		t.Fatalf("scope %d err=%v", n, err)
	}
	if n, err := scopeNASDayInventories(ctx, pool, "GSS best-14", false, days); err != nil || n != 0 {
		t.Fatalf("re-scope %d err=%v", n, err)
	}
	// One day outside the approved scope refuses the whole request.
	if _, err := requestNASDayInventories(ctx, pool, "gss", 3, false, append(days, nasDayInventoryDay{335, "2026-09-27"})); err == nil || !strings.Contains(err.Error(), "nas_restore_day_scope") {
		t.Fatalf("unapproved day queued: %v", err)
	}
	if _, err := requestNASDayInventories(ctx, pool, "gss", 3, false, append(days, nasDayInventoryDay{336, "2026-09-25"})); err == nil || !strings.Contains(err.Error(), "pinned window") {
		t.Fatalf("unpinned stream queued: %v", err)
	}
	if s, err := requestNASDayInventories(ctx, pool, "gss", 3, true, days); err != nil || s.Queued != 2 {
		t.Fatalf("dry run %+v err=%v", s, err)
	}
	if units, _ := loadNASDayInventoryStatus(ctx, pool, "gss"); len(units) != 0 {
		t.Fatalf("dry run or refused request queued %+v", units)
	}
	if s, err := requestNASDayInventories(ctx, pool, "gss", 3, false, days); err != nil || s.Queued != 2 {
		t.Fatalf("request %+v err=%v", s, err)
	}
	if s, err := requestNASDayInventories(ctx, pool, "gss", 3, false, days); err != nil || s.AlreadyQueued != 2 {
		t.Fatalf("re-request %+v err=%v", s, err)
	}

	// As the API records a completed scan: c1 at its plaza copy (exact bytes),
	// c2 matched with other bytes, one uncatalogued file and one joined file.
	plaza := "03_Brzeg/September/25-Friday/"
	if _, err := pool.Exec(ctx, `
UPDATE nas_day_inventory_units SET state='completed', completed_at=now() WHERE local_date='2026-09-25';
INSERT INTO nas_day_inventory_files(unit_id,relative_path,kind,size_bytes,mtime_ns,sha256,clip_id)
 SELECT u.id, f.p, f.k, 10, 1, f.h, f.c FROM nas_day_inventory_units u, (VALUES
  ('`+plaza+`c1.mp4','raw',repeat('1',64),901),
  ('`+plaza+`c2.mp4','raw',repeat('f',64),902),
  ('`+plaza+`other.mp4','raw',repeat('e',64),NULL),
  ('joined/`+plaza+`x_hour_08.mp4','joined',repeat('d',64),NULL)) f(p,k,h,c)
 WHERE u.local_date='2026-09-25'`); err != nil {
		t.Fatal(err)
	}
	units, err := loadNASDayInventoryStatus(ctx, pool, "gss")
	if err != nil || len(units) != 2 {
		t.Fatalf("status %+v err=%v", units, err)
	}
	if u := units[0]; u.State != "completed" || u.CatalogClips != 3 || u.FoundRaw != 3 || u.FoundJoined != 1 || u.Matched != 2 || u.MatchedSHAOK != 1 || u.Unmatched != 1 || u.RawBytes != 30 {
		t.Fatalf("day status %+v", u)
	}
	if u := units[1]; u.State != "pending" || u.FoundRaw != 0 || u.CatalogClips != 0 {
		t.Fatalf("pending day status %+v", u)
	}

	opts, err := parseNASRestoreRequestArgs([]string{"--recording-id", "335", "--from", "2026-09-25", "--to", "2026-09-26", "--label", "gss-raw"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s, err := requestNASRestores(ctx, pool, "stoarama", opts)
	if err != nil || s.Queued != 1 || s.Skipped["nas_unverified"] != 2 {
		t.Fatalf("restore request %+v err=%v", s, err)
	}
	var clip int64
	var path, target string
	if err := pool.QueryRow(ctx, `SELECT clip_id, relative_path, target FROM nas_restore_requests WHERE label='gss-raw'`).Scan(&clip, &path, &target); err != nil ||
		clip != 901 || path != plaza+"c1.mp4" || target != "original" {
		t.Fatalf("queued clip=%d path=%s target=%s err=%v", clip, path, target, err)
	}

	// Cancel withdraws the scope: nothing more is queued.
	if _, err := pool.Exec(ctx, `UPDATE nas_restore_requests SET state='canceled', completed_at=now() WHERE label='gss-raw';
UPDATE nas_day_inventory_units SET state='canceled' WHERE label='gss'`); err != nil {
		t.Fatal(err)
	}
	opts.label = "gss-raw-2"
	if s, err := requestNASRestores(ctx, pool, "stoarama", opts); err != nil || s.Queued != 0 || s.Skipped["nas_unverified"] != 3 {
		t.Fatalf("withdrawn scope still queued %+v err=%v", s, err)
	}
}

// With the writer fence installed, as in production, a restore request queues
// only rows nas_restore_admitted admits (an inventoried in-scope original or a
// purged Keep Everything original) and skips every other clip with its reason,
// instead of failing on the fence at the first insert.
func TestNASRestoreRequestQueuesOnlyAdmittedOriginalsPastTheWriterFence(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "nas_restore_fence")
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	// Stream 17216 is not Keep Everything; stream 78 is.
	if _, err := pool.Exec(ctx, `
INSERT INTO accounts(id,email) VALUES(47,'nas-fence@example.invalid');
INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(47,47,'fixture-key','fixture-key');
INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',47);
INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(17216,'test','17216','brzeg','brzeg','https://storage.example.test','off','Europe/Warsaw','hls','video_manifest','video_live','continuous_video',30),
       (78,'test','78','ke','ke','https://storage.example.test','off','Asia/Seoul','hls','video_manifest','video_live','continuous_video',30);
INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc,managed)
 VALUES(1,47,'test','https://storage.example.test','auto','stoarama','fixture',decode('00','hex'),true);
INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(379,47,1,'assisi','https://storage.example.test','* * * * *','Europe/Warsaw','paused',17216,now()),
       (337,47,1,'ke','https://storage.example.test','* * * * *','Asia/Seoul','paused',78,now());
-- 901 inventoried in scope; 902 inventoried in scope but its key has a pending R2 deletion;
-- 903 purged, NAS-verified the old way, outside scope; 904 purged, NAS-verified, Keep Everything.
INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path,purged_at)
 SELECT id,rec,1,'https://storage.example.test','stoarama','managed/acct-47/c'||id||'.mp4',10,'etag',repeat(chr(96+id-900),64),
  t,t,t+interval '2 minutes','recordings/'||rec||'/continuous/c'||id||'.mp4',purged
 FROM (VALUES (901,379,'2026-07-27T08:00:00+02:00'::timestamptz,NULL::timestamptz),(902,379,'2026-07-27T08:02:00+02:00',NULL),
              (903,379,'2026-07-26T08:00:00+02:00',now()),(904,337,'2026-07-27T08:00:00+09:00',now())) v(id,rec,t,purged);
INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at)
 SELECT 13,id,recording_id,display_path,size_bytes,sha256,'present',now(),now() FROM recording_clips WHERE id IN (903,904);
INSERT INTO nas_restore_day_scope(recording_id,local_date,reason) VALUES(379,'2026-07-27','GSS best-14');
INSERT INTO nas_day_inventory_units(connection_id,label,recording_id,local_date) VALUES(13,'gss',379,'2026-07-27');
UPDATE nas_day_inventory_units SET state='completed', completed_at=now();
INSERT INTO nas_day_inventory_files(unit_id,relative_path,kind,size_bytes,mtime_ns,sha256,clip_id)
 SELECT u.id,'32_Assisi/July/27-Monday/c'||c.id||'.mp4','raw',10,1,c.sha256,c.id FROM nas_day_inventory_units u, recording_clips c WHERE c.id IN (901,902);
INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',repeat('a',64));`); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role=replica;
INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 VALUES('raw',902,'stoarama','managed/acct-47/c902.mp4',10,repeat('b',64),repeat('c',64))`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for _, rec := range []string{"379", "337"} {
		opts, err := parseNASRestoreRequestArgs([]string{"--recording-id", rec, "--from", "2026-07-25", "--to", "2026-07-29", "--label", "gss-raw-" + rec}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		s, err := requestNASRestores(ctx, pool, "stoarama", opts)
		want := map[string]string{"379": "queued=1 skipped=map[not_admitted:1 retirement_pending:1]", "337": "queued=1 skipped=map[]"}[rec]
		if got := fmt.Sprintf("queued=%d skipped=%v", s.Queued, s.Skipped); err != nil || got != want {
			t.Fatalf("recording %s: %s err=%v, want %s", rec, got, err, want)
		}
	}
	var queued string
	if err := pool.QueryRow(ctx, `SELECT string_agg(clip_id||'@'||relative_path, ',' ORDER BY clip_id) FROM nas_restore_requests`).Scan(&queued); err != nil ||
		queued != "901@32_Assisi/July/27-Monday/c901.mp4,904@recordings/337/continuous/c904.mp4" {
		t.Fatalf("queued %s err=%v", queued, err)
	}
	// The gate itself is unchanged: the skipped clips stay fenced.
	for _, id := range []int{902, 903} {
		if _, err := pool.Exec(ctx, `INSERT INTO nas_restore_requests(connection_id,clip_id,recording_id,target,object_key,source_object_key,relative_path,content_type,size_bytes,sha256,label)
 SELECT 13,id,recording_id,'original',object_key,object_key,display_path,'video/mp4',size_bytes,sha256,'by-hand' FROM recording_clips WHERE id=$1`, id); err == nil ||
			!strings.Contains(err.Error(), "fenced") {
			t.Fatalf("clip %d queued past the writer fence by hand: %v", id, err)
		}
	}
}
