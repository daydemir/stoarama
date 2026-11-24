package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5/pgxpool"
)

const releaseMigration = "0218_r2_joined_batch_release.sql"

// migrateBefore applies the migrations before 0218, so a test can create the
// batches 0218's seed reads before running it and every later migration for real.
func migrateBefore(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	src := joinedPurgeMigrationsDir()
	dir := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() >= releaseMigration || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, e.Name()), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.MigrateUp(ctx, pool, dir); err != nil {
		t.Fatal(err)
	}
}

// relaxJoined drops the joined tables' unrelated column and check constraints
// in this disposable schema, so rows can carry only the identities under test.
func relaxJoined(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `DO $$ DECLARE c record; tables text[] := ARRAY['recording_joined_batches','recording_joined_source_snapshots','recording_joined_sources','recording_joined_hours','recording_joined_dry_runs','recording_joined_dry_run_scopes','recording_joined_snapshot_scopes']; BEGIN
 FOR c IN SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=ANY(tables) AND is_nullable='NO' AND column_name<>'id' LOOP
  BEGIN EXECUTE format('ALTER TABLE %I ALTER COLUMN %I DROP NOT NULL',c.table_name,c.column_name);
  EXCEPTION WHEN invalid_table_definition THEN NULL; END; END LOOP;
 FOR c IN SELECT conrelid::regclass::text t,conname FROM pg_constraint WHERE conrelid::regclass::text=ANY(tables) AND contype IN ('c','f') LOOP
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',c.t,c.conname); END LOOP; END $$`); err != nil {
		t.Fatal(err)
	}
}

func replica(t *testing.T, ctx context.Context, pool *pgxpool.Pool, queries ...string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, q := range append([]string{`SET LOCAL session_replication_role=replica`}, queries...) {
		if _, err = tx.Exec(ctx, q); err != nil {
			t.Fatal(q, err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestR2JoinedBatchRelease proves the owner's release of the frozen August
// batch reaches the R2 RAW lane only, for exactly the sources every joined
// claim lets go of, and that the migration's seed picks only that batch.
func TestR2JoinedBatchRelease(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "joined_release")
	migrateBefore(t, ctx, pool)
	relaxJoined(t, ctx, pool)
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatal(query, err)
		}
	}
	exec(`INSERT INTO accounts(id,email) VALUES(47,'release@test.invalid');
 INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES(3,'test','3','release','release','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc) VALUES(1,47,'test','https://test.invalid','auto','stoarama','fixture',decode('00','hex'));
 INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at) VALUES(30,47,1,'release','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now());`)
	// Clip 1: released. 2: also in the decoy batch. 3: decoy only. 4: snapshot
	// identity drifted. 5: allocated to a held hour. 6: under a live dry run.
	// 7: under a snapshotting scope. 8: under the batch's own completed dry run.
	keys := map[int64]string{}
	for id := int64(1); id <= 8; id++ {
		keys[id] = "managed/acct-47/release/" + string(rune('a'+id-1)) + ".mp4"
	}
	replica(t, ctx, pool,
		`INSERT INTO recording_clips(id,recording_id,recording_job_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 SELECT n,30,n,1,'https://test.invalid','stoarama','managed/acct-47/release/'||chr(96+n)||'.mp4',100,'etag',repeat('b',64),'2026-06-01','2026-06-01','2026-06-01 00:01:00','release/'||n FROM generate_series(1,8) n`,
		`INSERT INTO recording_joined_batches(id,batch_id,generation,connection_id,state) VALUES(5,'goodplus-20260821-generation-1',1,13,'frozen'),(6,'decoy',1,13,'frozen'),(7,'other-live',1,13,'snapshotting')`,
		`INSERT INTO recording_joined_source_snapshots(id,batch_record_id,clip_id,recording_id,bucket,object_key,ingest_etag,size_bytes,sha256,start_at,end_at)
 SELECT c.id*10+b,b,c.id,c.recording_id,c.bucket,c.object_key,c.etag,c.size_bytes,c.sha256,c.clip_start_at,c.clip_end_at FROM recording_clips c CROSS JOIN LATERAL (VALUES (5),(6)) v(b)
 WHERE (b=5 AND c.id<>3) OR (b=6 AND c.id IN (2,3))`,
		`UPDATE recording_joined_source_snapshots SET end_at=end_at+interval '1 second' WHERE clip_id=4`,
		`INSERT INTO recording_joined_hours(id,batch_record_id) VALUES(900,5)`,
		`INSERT INTO recording_joined_sources(id,source_snapshot_id,clip_id,hour_record_id) VALUES(9001,55,5,900)`,
		`INSERT INTO recording_joined_source_retention_holds(hour_record_id,reason_code) VALUES(900,'dual_recorder_overlap')`,
		`INSERT INTO recording_joined_dry_runs(id,batch_id,generation,connection_id) VALUES('00000000-0000-0000-0000-000000000006','never-frozen',1,13),('00000000-0000-0000-0000-000000000008','goodplus-20260821-generation-1',1,13)`,
		`INSERT INTO recording_joined_dry_run_scopes(dry_run_id,priority_ordinal,date_ordinal,recording_id,recording_job_id,high_water_clip_id) VALUES('00000000-0000-0000-0000-000000000006',1,1,30,6,6),('00000000-0000-0000-0000-000000000008',1,1,30,8,8)`,
		`INSERT INTO recording_joined_snapshot_scopes(batch_record_id,recording_id,recording_job_id,high_water_clip_id) VALUES(7,30,7,7)`)
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	var seeded []int64
	rows, err := pool.Query(ctx, `SELECT batch_record_id FROM r2_joined_batch_releases`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		seeded = append(seeded, id)
	}
	rows.Close()
	if len(seeded) != 1 || seeded[0] != 5 {
		t.Fatalf("seed released %v", seeded)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO r2_joined_batch_releases(batch_record_id,batch_id,generation,decision) VALUES(6,'decoy',1,'x')`); err == nil {
		t.Fatal("a second batch was released")
	}
	replica(t, ctx, pool, `INSERT INTO recording_joined_batches(id,batch_id,generation,state) VALUES(8,'goodplus-20260821-generation-1-copy',1,'frozen')`)
	if _, err = pool.Exec(ctx, `INSERT INTO r2_joined_batch_releases(batch_record_id,batch_id,generation,decision) VALUES(8,'goodplus-20260821-generation-1',1,'x')`); err == nil || !strings.Contains(err.Error(), "own frozen batch record") {
		t.Fatalf("decoy record under the pinned label: %v", err)
	}
	released := func(id int64) bool {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT r2_joined_source_released($1)`, id).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	for id, want := range map[int64]bool{1: true, 2: false, 3: false, 4: false, 5: false, 6: false, 7: false, 8: true} {
		if released(id) != want {
			t.Fatalf("clip %d released=%v, want %v", id, !want, want)
		}
	}
	held := func(id int64, raw bool) bool {
		var h bool
		if err := pool.QueryRow(ctx, inverseRawHeldSQL, id, "stoarama", keys[id], "", "release/x", strings.Repeat("b", 64), int64(100), raw).Scan(&h); err != nil {
			t.Fatal(err)
		}
		return h
	}
	if held(1, true) || held(8, true) || !held(1, false) || !held(2, true) || !held(5, true) || !held(6, true) {
		t.Fatal("RAW hold does not follow the release exactly, or the keep-28 lane lost its hold")
	}
	keySet := func(sql string) map[string]bool {
		rows, err := pool.Query(ctx, sql, "stoarama")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]bool{}
		for rows.Next() {
			var k string
			if err = rows.Scan(&k); err != nil {
				t.Fatal(err)
			}
			out[k] = true
		}
		return out
	}
	partitionHeld, partitionReleased := keySet(inverseHeldSQL), keySet(inverseJoinedReleasedSQL)
	if partitionHeld[keys[1]] || partitionHeld[keys[8]] || !partitionHeld[keys[2]] || !partitionHeld[keys[5]] || !partitionHeld[keys[6]] || len(partitionReleased) != 2 || !partitionReleased[keys[1]] || !partitionReleased[keys[8]] {
		t.Fatalf("partition held %v released %v", partitionHeld, partitionReleased)
	}
	// The purge exemption belongs to the RAW lane's exact intent only.
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=1`); err == nil {
		t.Fatal("bare purge of a released source was admitted")
	}
	intent := func(table, extraCols, extraVals string, id int64) {
		replica(t, ctx, pool, `INSERT INTO `+table+`(bucket,object_key,source_id,size_bytes,etag,last_modified,manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,choice,endpoint_sha256,stream_id`+extraCols+`)
 VALUES('stoarama','`+keys[id]+`',`+string(rune('0'+id))+`,100,'etag','2026-11-01',repeat('a',64),'060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811',repeat('b',64),repeat('c',64),'none',repeat('e',64),3`+extraVals+`)`)
	}
	intent("r2_keep28_intents", ",custody", ",'{}'::jsonb", 8)
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=8`); err == nil {
		t.Fatal("a keep-28 intent purged a released source")
	}
	// Production order: reserve writes the RAW intent, then probes the purge.
	a, entry := releaseApplier(t, ctx, pool, keys[1])
	if _, err = a.reserve(ctx, entry); err != nil {
		t.Fatalf("fresh released source could not be reserved: %v", err)
	}
	if decoy, decoyEntry := releaseApplier(t, ctx, pool, keys[2]); true {
		if _, err = decoy.reserve(ctx, decoyEntry); err == nil {
			t.Fatal("a source the decoy batch still claims was reserved")
		}
	}
	// With its intent pending, no new operator hold may land on its hour.
	replica(t, ctx, pool, `INSERT INTO recording_joined_hours(id,batch_record_id) VALUES(901,5)`,
		`INSERT INTO recording_joined_sources(id,source_snapshot_id,clip_id,hour_record_id) VALUES(9002,15,1,901)`)
	if _, err = pool.Exec(ctx, `INSERT INTO recording_joined_source_retention_holds(hour_record_id,reason_code) VALUES(901,'late_hold')`); err == nil || !strings.Contains(err.Error(), "already issued") {
		t.Fatalf("late retention hold over an issued retirement: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_joined_source_retention_holds SET hour_record_id=901 WHERE hour_record_id=900`); err == nil || !strings.Contains(err.Error(), "already issued") {
		t.Fatalf("moved retention hold over an issued retirement: %v", err)
	}
	rr, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rr.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`); err != nil {
		t.Fatal(err)
	}
	if _, err = rr.Exec(ctx, `INSERT INTO recording_joined_source_retention_holds(hour_record_id,reason_code) VALUES(900,'rr_hold')`); err == nil || !strings.Contains(err.Error(), "READ COMMITTED") {
		t.Fatalf("repeatable-read retention hold: %v", err)
	}
	rr.Rollback(ctx)
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=1`); err != nil {
		t.Fatalf("RAW purge of a released source refused: %v", err)
	}
	intent("r2_inverse_intents", "", "", 2)
	if _, err = pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=2`); err == nil {
		t.Fatal("a source the decoy batch still claims was purged")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM recording_clips WHERE id=1`); err == nil {
		t.Fatal("a released source was deleted outright")
	}
	for _, q := range []string{`UPDATE r2_joined_batch_releases SET decision='x'`, `DELETE FROM r2_joined_batch_releases`, `TRUNCATE r2_joined_batch_releases`} {
		if _, err = pool.Exec(ctx, q); err == nil {
			t.Fatal("release record changed:", q)
		}
	}
}

// The migration refuses to run if the named batch exists but is not the frozen
// generation the owner released.
func TestR2JoinedBatchReleaseRefusesAdvancedBatch(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "joined_release_advanced")
	migrateBefore(t, ctx, pool)
	relaxJoined(t, ctx, pool)
	replica(t, ctx, pool, `INSERT INTO recording_joined_batches(id,batch_id,generation,state) VALUES(5,'goodplus-20260821-generation-1',2,'frozen')`)
	if err := db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err == nil || !strings.Contains(err.Error(), "not the frozen generation-1 batch") {
		t.Fatalf("unexpected generation: %v", err)
	}
}

// A released joined source inside a pinned keep-28 window stays held, so the
// keep-28 lane never takes it; outside every window it is ordinary RAW.
func TestR2JoinedReleasedKeepsKeep28Held(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(28 * 24 * time.Hour)
	p := inverseProtection{Held: map[string]bool{}, Delivery: map[string]bool{}, KEFolders: map[string]bool{},
		JoinedReleased: map[string]bool{"in": true, "out": true},
		Windows:        map[int64]api.Keep28Window{3: {StreamID: 3, Timezone: "Asia/Tokyo", Start: start, End: end}},
		Picks:          r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: start.Format(time.RFC3339), KeepEndUTC: end.Format(time.RFC3339)}}}}
	inside, insideEnd := start.Add(time.Hour), start.Add(time.Hour+time.Minute)
	outside, outsideEnd := start.Add(-30*24*time.Hour), start.Add(-30*24*time.Hour+time.Minute)
	p.Identities = map[string][]inverseIdentity{
		"in":  {{StreamID: 3, Path: "s/in.mp4", Start: &inside, End: &insideEnd, Timezone: "Asia/Tokyo", Kind: "raw"}},
		"out": {{StreamID: 3, Path: "s/out.mp4", Start: &outside, End: &outsideEnd, Timezone: "Asia/Tokyo", Kind: "raw"}},
	}
	if class, reason := p.classify(r2.ObjectInfo{Key: "in", SizeBytes: 1}); class != inverseHeld || reason != "joined_source_snapshot" {
		t.Fatalf("released in-window source classified %s (%s)", class, reason)
	}
	if class, _ := p.classify(r2.ObjectInfo{Key: "out", SizeBytes: 1}); class != inverseDeletable {
		t.Fatalf("released outside-window source classified %s", class)
	}
}

// releaseApplier builds a minimal RAW-lane applier for one catalog key, with
// real drain evidence, partition files, picks and protection, so a test can
// drive reserve exactly as production does.
func releaseApplier(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key string) (*inverseApplier, inverseEntry) {
	t.Helper()
	policy, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	w := windows[3]
	picksPath := filepath.Join(root, "picks.json")
	picksData, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}})
	if err = os.WriteFile(picksPath, picksData, 0600); err != nil {
		t.Fatal(err)
	}
	picks, err := loadR2RetirePicks(picksPath)
	if err != nil {
		t.Fatal(err)
	}
	store := &inverseFakeStore{endpointSHA: strings.Repeat("c", 64), objects: map[string]r2.ObjectInfo{}}
	entry := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: key, ETag: "etag", SizeBytes: 100, LastModified: inverseRetirementCutoff().Add(-1500 * time.Millisecond)}, Class: inverseDeletable}
	store.objects[key] = entry.ObjectInfo
	var fences int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_retirement_writer_fences`).Scan(&fences); err != nil {
		t.Fatal(err)
	}
	if fences == 0 {
		replica(t, ctx, pool, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256,activated_at,drain_not_before) VALUES('stoarama','`+store.endpointSHA+`','2026-11-12T14:28:47.037031Z',clock_timestamp()-interval '1 hour')`)
	}
	var activated, deadline time.Time
	if err = pool.QueryRow(ctx, `SELECT activated_at,drain_not_before FROM r2_retirement_writer_fences`).Scan(&activated, &deadline); err != nil {
		t.Fatal(err)
	}
	drain := inverseDrainEvidence{Proof: inversePUTCompletionProof, Bucket: "stoarama", EndpointSHA256: store.endpointSHA, FenceActivatedAt: activated, DrainNotBefore: deadline, ObservedAt: time.Now().UTC(), DeployedWriterCoverage: "test fixture all admissions checked", AcceptedPUTCompletion: "test fixture no provider PUTs accepted"}
	drainPath := filepath.Join(root, "drain.json")
	data, _ := json.Marshal(drain)
	if err = os.WriteFile(drainPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	drainSHA, _ := inverseFileSHA(drainPath)
	manifest := filepath.Join(root, "partition.jsonl")
	data, _ = json.Marshal(entry)
	if err = os.WriteFile(manifest, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	manifestSHA, _ := inverseFileSHA(manifest)
	sum := inverseSummary{Version: 1, Bucket: "stoarama", EndpointSHA256: store.endpointSHA, CreatedAt: time.Now().UTC(), PolicySHA256: policy, PicksSHA256: picks.SHA256, ReadOnly: true}
	summaryPath := filepath.Join(root, "summary.json")
	data, _ = json.Marshal(sum)
	if err = os.WriteFile(summaryPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	summarySHA, _ := inverseFileSHA(summaryPath)
	o := inverseApplyOptions{Lane: inverseOutsideKeep28, Manifest: manifest, ManifestSHA: manifestSHA, Summary: summaryPath, SummarySHA: summarySHA, Picks: picksPath, Ledger: filepath.Join(root, "ledger.jsonl"), DrainEvidence: drainPath, DrainSHA: drainSHA, MaxItems: 1, MaxBytes: 100, Workers: 1}
	p, err := loadInverseProtection(ctx, pool, "stoarama", picks, windows)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := openInverseLedger(o.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ledger.close() })
	var epoch int64
	if err = pool.QueryRow(ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	return &inverseApplier{pool: pool, store: store, opts: o, summary: sum, protection: p, ledger: ledger, keEpoch: epoch}, entry
}
