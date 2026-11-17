package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/jackc/pgx/v5/pgxpool"
)

func finishFixture(t *testing.T, n int) (*pgxpool.Pool, string, string) {
	t.Helper()
	pool, _, _, _ := nasCopyQueueFixture(t, n, true)
	_, err := pool.Exec(context.Background(), `ALTER TABLE nas_cleanup_plans ADD COLUMN connection_id bigint DEFAULT 13, ADD COLUMN plan_sha256 text DEFAULT repeat('d',64);
 INSERT INTO nas_cleanup_plans(id,mode) VALUES(99,'retired');
 ALTER TABLE nas_cleanup_items ADD COLUMN updated_at timestamptz DEFAULT now();
 ALTER TABLE connections ADD COLUMN nas_storage_free_bytes bigint, ADD COLUMN nas_storage_reported_at timestamptz;
 CREATE TABLE nas_cleanup_batches(id bigint,mode text);
 ALTER TABLE connections ADD COLUMN nas_storage_client_version text NOT NULL DEFAULT '';
 CREATE TABLE nas_inventory_files(connection_id bigint,clip_id bigint,relative_path text,state text,sha256 text,size_bytes bigint,verified_at timestamptz);
 UPDATE connections SET nas_storage_free_bytes=10000000000000,nas_storage_reported_at=now(),nas_storage_client_version='r2-copy-v5';
 ALTER TABLE nas_r2_copy_items ADD COLUMN IF NOT EXISTS completion text DEFAULT '', ADD COLUMN IF NOT EXISTS committed_bytes bigint DEFAULT 0, ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();`)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	safety := filepath.Join(dir, "safety.json")
	copy := filepath.Join(dir, "copy.json")
	finishTestJSON(t, safety, finishSafety{UpdatedAt: time.Now().UTC(), RecoveryCheckedAt: time.Now().UTC(), RecoveryPlanID: 99, RecoveryPlanSHA: strings.Repeat("d", 64), LocalCheckedAt: time.Now().UTC(), MasterCheckedAt: time.Now().UTC(), KnownPlans: []int64{}, KnownBatches: []int64{}, Holds: map[string]int64{}})
	finishTestJSON(t, copy, finishCopy{Schema: "stoarama.nas-r2-copy-status.v1", UpdatedAt: time.Now().UTC(), BlockedByReason: map[string]nasCopyStat{}})
	return pool, safety, copy
}
func finishTestJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func finishRun(t *testing.T, pool *pgxpool.Pool, safety, copy string) finishStatus {
	t.Helper()
	status, err := finishTick(context.Background(), pool, safety, copy)
	if err != nil {
		t.Fatalf("tick: %v %+v", err, status)
	}
	return status
}
func TestFinishFeedPriorityIdempotenceAndTerminalStates(t *testing.T) {
	pool, safety, copy := finishFixture(t, 7)
	ctx := context.Background()
	// Stale exact inventory is tier1; stream78 is early tier2; stream3
	// without exact inventory is late tier3. No listing tables exist at all.
	_, windows, _ := api.Keep28Policy()
	early := windows[78]
	if _, err := pool.Exec(ctx, `INSERT INTO streams VALUES(78,$1)`, early.Timezone); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recordings VALUES(3,47,78,$1)`, early.Timezone); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET recording_id=3,clip_start_at=$1,clip_end_at=$2 WHERE id IN(1,2,3)`, early.Start, early.Start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files SELECT 13,id,display_path,'present',sha256,size_bytes,now()-interval '3 months' FROM recording_clips WHERE id IN(1,4)`); err != nil {
		t.Fatal(err)
	}
	selected, tier, err := finishCandidates(ctx, pool, 5000)
	if err != nil || tier != finishExactInventory || len(selected) != 2 || selected[0].SourceID != 4 || selected[1].SourceID != 1 {
		t.Fatalf("stream/id priority: tier=%v items=%v err=%v", tier, selected, err)
	}
	status := finishRun(t, pool, safety, copy)
	if status.ImportedHistoricalInventory != 2 || status.ImportedEarlyUnknown != 2 || status.ImportedLateUnknown != 3 || status.ImportedItems != 7 || status.QueueDepth != 7 || !status.Exhausted || status.ImportedManifests != 3 {
		t.Fatalf("tier feed: %+v", status)
	}
	rows, err := pool.Query(ctx, `SELECT source_id FROM nas_r2_copy_items ORDER BY id LIMIT 2`)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids[id] = true
	}
	rows.Close()
	if !ids[1] || !ids[4] {
		t.Fatalf("present not first: %v", ids)
	}
	_, err = pool.Exec(ctx, `UPDATE nas_r2_copy_items SET state=CASE WHEN source_id=2 THEN 'verified' ELSE 'blocked' END,attempts=7,last_error='retained'`)
	if err != nil {
		t.Fatal(err)
	}
	status = finishRun(t, pool, safety, copy)
	if status.ImportedItems != 0 || status.QueueDepth != 0 {
		t.Fatalf("requeued terminal: %+v", status)
	}
	var n int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE attempts=7 AND last_error='retained'`).Scan(&n); err != nil || n != 7 {
		t.Fatalf("terminal state changed: %d %v", n, err)
	}
}
func TestFinishStopsBeforeFeeding(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*finishSafety, *finishCopy)
		sql    string
		stop   finishStop
	}{
		{name: "recovery", mutate: func(s *finishSafety, c *finishCopy) { s.RecoveryProblems = 1 }, stop: finishRecovery},
		{name: "shutdown", mutate: func(s *finishSafety, c *finishCopy) { s.ShutdownFailed = true }, stop: finishShutdown},
		{name: "recovery error without latch", mutate: func(s *finishSafety, c *finishCopy) { v := "failed"; s.RecoveryError = &v }, stop: finishShutdown},
		{name: "local mismatch", mutate: func(s *finishSafety, c *finishCopy) { s.LocalMismatch = 1 }, stop: finishMismatch},
		{name: "last error", mutate: func(s *finishSafety, c *finishCopy) { v := "unexpected"; s.LastError = &v }, stop: finishShutdown},
		{name: "mismatch", mutate: func(s *finishSafety, c *finishCopy) { c.MismatchLatched = 1 }, stop: finishMismatch},
		{name: "unknown plan", sql: `INSERT INTO nas_cleanup_plans(id,mode) VALUES(91,'enabled')`, stop: finishUnknownPlan},
		{name: "unknown batch", sql: `INSERT INTO nas_cleanup_batches VALUES(92,'enabled')`, stop: finishUnknownPlan},
		{name: "master mismatch", mutate: func(s *finishSafety, c *finishCopy) { s.Master = true }, stop: finishMaster},
		{name: "unowned master", mutate: func(s *finishSafety, c *finishCopy) { s.Master = true; s.ExpectedMaster = true }, stop: finishMaster},
		{name: "stale underlying recovery", mutate: func(s *finishSafety, c *finishCopy) { s.RecoveryCheckedAt = time.Now().Add(-8 * time.Hour) }, stop: finishRecoveryStale},
		{name: "recovery plan missing", mutate: func(s *finishSafety, c *finishCopy) { s.RecoveryPlanID = 1234567 }, stop: finishRecoveryStale},
		{name: "issued changed", sql: `INSERT INTO nas_cleanup_items(plan_id,delete_ever_issued) VALUES(99,true)`, stop: finishRecoveryStale},
		{name: "stale safety", mutate: func(s *finishSafety, c *finishCopy) { s.UpdatedAt = time.Now().Add(-6 * time.Minute) }, stop: finishEvidence},
		{name: "stale copy", mutate: func(s *finishSafety, c *finishCopy) { c.UpdatedAt = time.Now().Add(-6 * time.Minute) }, stop: finishEvidence},
		{name: "future", mutate: func(s *finishSafety, c *finishCopy) { s.UpdatedAt = time.Now().Add(time.Minute) }, stop: finishEvidence},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool, safety, copy := finishFixture(t, 1)
			s, c, err := finishReadEvidence(safety, copy)
			if err != nil {
				t.Fatal(err)
			}
			if test.mutate != nil {
				test.mutate(&s, &c)
			}
			finishTestJSON(t, safety, s)
			finishTestJSON(t, copy, c)
			if test.sql != "" {
				if _, err = pool.Exec(context.Background(), test.sql); err != nil {
					t.Fatal(err)
				}
			}
			status, err := finishTick(context.Background(), pool, safety, copy)
			if !errors.Is(err, test.stop) || status.Stop != test.stop || status.CleanupShouldRun {
				t.Fatalf("stop: %v %+v", err, status)
			}
			var n int
			if err = pool.QueryRow(context.Background(), `SELECT count(*) FROM nas_r2_copy_items`).Scan(&n); err != nil || n != 0 {
				t.Fatalf("stop fed: %d %v", n, err)
			}
		})
	}
}
func TestFinishEvidenceMissingAndNullFailClosed(t *testing.T) {
	pool, safety, copy := finishFixture(t, 1)
	for _, raw := range []string{`{}`, `{"updated_at":null}`, `[]`, `invalid`} {
		if err := os.WriteFile(safety, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := finishTick(context.Background(), pool, safety, copy)
		if !errors.Is(err, finishEvidence) {
			t.Fatalf("accepted %s: %v", raw, err)
		}
	}
}
func TestFinishCleanupUsesFreshSpaceAndKnownOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, sql, reason string
		want              bool
	}{
		{"low", `UPDATE connections SET nas_storage_free_bytes=2299999999999,nas_storage_reported_at=now()`, "nas_headroom_below_threshold", true},
		{"boundary", `UPDATE connections SET nas_storage_free_bytes=2300000000000,nas_storage_reported_at=now()`, "nas_headroom_sufficient", false},
		{"belowfloor", `UPDATE connections SET nas_storage_free_bytes=1,nas_storage_reported_at=now()`, "nas_headroom_below_threshold", true},
		{"stale", `UPDATE connections SET nas_storage_free_bytes=1,nas_storage_reported_at=now()-interval '11 minutes'`, "nas_space_heartbeat_stale", false},
		{"future", `UPDATE connections SET nas_storage_free_bytes=1,nas_storage_reported_at=now()+interval '1 minute'`, "nas_space_heartbeat_stale", false},
		{"missing", `UPDATE connections SET nas_storage_free_bytes=NULL,nas_storage_reported_at=NULL`, "nas_space_heartbeat_missing", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, safety, copy := finishFixture(t, 0)
			s, c, err := finishReadEvidence(safety, copy)
			if err != nil {
				t.Fatal(err)
			}
			// A historical disk hold never decides the valve.
			s.Holds["free_space"] = 100
			s.KnownPlans = []int64{99}
			s.Master = true
			s.ExpectedMaster = true
			finishTestJSON(t, safety, s)
			finishTestJSON(t, copy, c)
			if _, err = pool.Exec(context.Background(), `UPDATE nas_cleanup_plans SET mode='enabled' WHERE id=99;`+tc.sql); err != nil {
				t.Fatal(err)
			}
			status := finishRun(t, pool, safety, copy)
			if status.CleanupShouldRun != tc.want || status.CleanupReason != tc.reason {
				t.Fatalf("wrong valve: %+v", status)
			}
		})
	}
}

func TestFinishManifestCapsAndEquality(t *testing.T) {
	_, w, _ := api.Keep28Policy()
	start := w[3].Start
	i := api.NASR2CopyItem{Kind: api.NASR2CopyRaw, SourceID: 1, RecordingID: 2, StreamID: 3, Bucket: "bucket", ObjectKey: "managed/a", RelativePath: "p/a.mp4", SizeBytes: 1, SHA256: strings.Repeat("a", 64), StartAt: start, EndAt: start.Add(time.Minute)}
	if _, _, _, err := finishManifest(t.TempDir(), []api.NASR2CopyItem{i, i}); err == nil {
		t.Fatal("duplicate accepted")
	}
	items := make([]api.NASR2CopyItem, 5001)
	for n := range items {
		items[n] = i
		items[n].SourceID = int64(n + 1)
	}
	if _, _, _, err := finishManifest(t.TempDir(), items); err == nil {
		t.Fatal("oversized manifest accepted")
	}
}
func TestFinishStatusAtomicWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status.json")
	status := finishStatus{Schema: "stoarama.finish-status.v1", UpdatedAt: time.Now().UTC(), Stop: finishRecovery}
	if err := finishWriteStatus(path, status); err != nil {
		t.Fatal(err)
	}
	var got finishStatus
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &got) != nil || got.Stop != finishRecovery {
		t.Fatal("status lost")
	}
}
func TestFinishFeedsOnlyDeficitAndHonorsByteCaps(t *testing.T) {
	pool, safety, copy := finishFixture(t, 3)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item) SELECT 13,'raw',n,repeat('a',64),repeat('b',64),'{}'::jsonb FROM generate_series(100,10097) n`)
	if err != nil {
		t.Fatal(err)
	}
	status := finishRun(t, pool, safety, copy)
	if status.QueueDepth != 10000 || status.ImportedItems != 2 || status.ImportedManifests != 1 {
		t.Fatalf("deficit: %+v", status)
	}
	status = finishRun(t, pool, safety, copy)
	if status.ImportedItems != 0 {
		t.Fatal("fed full queue")
	}
	pool2, safety2, copy2 := finishFixture(t, 3)
	if _, err = pool2.Exec(ctx, `UPDATE recording_clips SET size_bytes=60000000000`); err != nil {
		t.Fatal(err)
	}
	status = finishRun(t, pool2, safety2, copy2)
	if status.ImportedItems != 3 || status.ImportedManifests != 3 {
		t.Fatalf("byte caps: %+v", status)
	}
}
func TestFinishConcurrentTickDoesNotFeed(t *testing.T) {
	pool, safety, copy := finishFixture(t, 2)
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, finishLock); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, finishLock)
	status, err := finishTick(ctx, pool, safety, copy)
	if !errors.Is(err, finishBusy) || status.ImportedItems != 0 {
		t.Fatalf("concurrent feed: %v %+v", err, status)
	}
}

func TestFinishScopedRecoveryAndKnownHeldError(t *testing.T) {
	pool, safety, copy := finishFixture(t, 1)
	// Older issued permits do not participate: closed-plan recovery records own
	// their history. Only the declared current/latest plan is checked here.
	if _, err := pool.Exec(context.Background(), `INSERT INTO nas_cleanup_plans(id,mode) VALUES(1,'retired');INSERT INTO nas_cleanup_items(plan_id,delete_ever_issued) VALUES(1,true)`); err != nil {
		t.Fatal(err)
	}
	s, c, err := finishReadEvidence(safety, copy)
	if err != nil {
		t.Fatal(err)
	}
	held := "controller-held STOP after recovery-verified close"
	s.LastError = &held
	s.HeldError = &held
	finishTestJSON(t, safety, s)
	finishTestJSON(t, copy, c)
	status := finishRun(t, pool, safety, copy)
	if status.ImportedItems != 1 {
		t.Fatal("history blocked scoped recovery")
	}
	s.RecoveryPlanSHA = strings.Repeat("e", 64)
	finishTestJSON(t, safety, s)
	_, err = finishTick(context.Background(), pool, safety, copy)
	if !errors.Is(err, finishRecoveryStale) {
		t.Fatalf("wrong plan proof accepted: %v", err)
	}
}

func TestFinishRecoveryBindsUnchangedIssuedState(t *testing.T) {
	pool, safety, copy := finishFixture(t, 0)
	s, c, err := finishReadEvidence(safety, copy)
	if err != nil {
		t.Fatal(err)
	}
	s.RecoveryCheckedAt = time.Now().Add(-time.Hour)
	s.RecoveryIssued = 1
	s.RecoveryVerified = 1
	if _, err = pool.Exec(context.Background(), `INSERT INTO nas_cleanup_items(plan_id,delete_ever_issued,updated_at) VALUES(99,true,now()-interval '2 hours')`); err != nil {
		t.Fatal(err)
	}
	finishTestJSON(t, safety, s)
	finishTestJSON(t, copy, c)
	finishRun(t, pool, safety, copy) // An old real check is valid for unchanged state.
	if _, err = pool.Exec(context.Background(), `UPDATE nas_cleanup_items SET updated_at=now() WHERE plan_id=99`); err != nil {
		t.Fatal(err)
	}
	_, err = finishTick(context.Background(), pool, safety, copy)
	if !errors.Is(err, finishRecoveryStale) {
		t.Fatalf("same-count new deletion accepted: %v", err)
	}
}

func TestFinishLateTierReservesAdmissionAndTerminalWindow(t *testing.T) {
	pool, safety, copy := finishFixture(t, 3)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE recording_clips SET size_bytes=60000000000;
 UPDATE connections SET nas_storage_free_bytes=2300000000000,nas_storage_reported_at=now()-interval '1 minute';
 INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,completion,committed_bytes,updated_at)
 SELECT 13,'raw',id,repeat('a',64),repeat('b',64),jsonb_build_object('size_bytes',size),state,completion,committed,CASE WHEN id IN(103,104) THEN now()-interval '4 minutes' ELSE now() END
 FROM (VALUES(100,60000000000::bigint,'pending','',0::bigint),(101,60000000000,'leased','',0),(102,40000000000,'verified','downloaded',40000000000),(103,60000000000,'verified','verified_in_place',60000000000),(104,60000000000,'blocked','',0)) v(id,size,state,completion,committed);`)
	if err != nil {
		t.Fatal(err)
	}
	status := finishRun(t, pool, safety, copy)
	if status.ImportedItems != 2 || status.LateTierHold != finishLateBudgetHeld || status.Exhausted || status.QueueReservedBytes != 280000000000 {
		t.Fatalf("late budget: %+v", status)
	}
	// Reusing the same free-space heartbeat cannot spend its capacity twice.
	status = finishRun(t, pool, safety, copy)
	if status.ImportedItems != 0 {
		t.Fatal("reused capacity")
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_r2_copy_items SET state='verified',completion='downloaded',committed_bytes=60000000000,updated_at=now() WHERE source_id IN(1,2)`); err != nil {
		t.Fatal(err)
	}
	status = finishRun(t, pool, safety, copy)
	if status.ImportedItems != 0 {
		t.Fatal("forgot downloads since heartbeat")
	}
	if _, err = pool.Exec(ctx, `UPDATE connections SET nas_storage_free_bytes=2180000000000,nas_storage_reported_at=now()`); err != nil {
		t.Fatal(err)
	}
	status = finishRun(t, pool, safety, copy)
	if !status.CleanupShouldRun || status.LateTierHold != finishLateSpaceLow || status.ImportedItems != 0 {
		t.Fatalf("low-space valve: %+v", status)
	}
	if _, err = pool.Exec(ctx, `UPDATE connections SET nas_storage_free_bytes=2500000000000,nas_storage_reported_at=now()`); err != nil {
		t.Fatal(err)
	}
	status = finishRun(t, pool, safety, copy)
	if status.ImportedItems != 1 {
		t.Fatalf("new capacity did not resume: %+v", status)
	}
}

func TestFinishMissingStorageHoldsOnlyLateTier(t *testing.T) {
	pool, safety, copy := finishFixture(t, 1)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_free_bytes=NULL,nas_storage_reported_at=NULL`); err != nil {
		t.Fatal(err)
	}
	status := finishRun(t, pool, safety, copy)
	if status.ImportedItems != 0 || status.Exhausted || status.LateTierHold != finishLateSpaceUnknown || status.CleanupShouldRun {
		t.Fatalf("missing storage: %+v", status)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_inventory_files SELECT 13,id,display_path,'present',sha256,size_bytes,now()-interval '3 months' FROM recording_clips`); err != nil {
		t.Fatal(err)
	}
	status = finishRun(t, pool, safety, copy)
	if status.ImportedItems != 1 {
		t.Fatal("missing storage blocked verify-in-place tier")
	}
}

func TestFinishLateTierRetainsAmbiguousTerminalDebits(t *testing.T) {
	for _, completion := range []string{"downloaded", "verified_in_place"} {
		t.Run(completion, func(t *testing.T) {
			pool, safety, copy := finishFixture(t, 1)
			ctx := context.Background()
			if _, err := pool.Exec(ctx, `UPDATE recording_clips SET size_bytes=60000000000;UPDATE connections SET nas_storage_free_bytes=2300000000000,nas_storage_reported_at=now()`); err != nil {
				t.Fatal(err)
			}
			// Downloads can precede receipt of a cached free-space sample. A failed
			// result can also retry as verified-in-place with zero committed bytes.
			// Both outcomes retain the same full-size debit for T after terminal state.
			_, err := pool.Exec(ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,completion,committed_bytes,updated_at)
 SELECT 13,'raw',id,repeat('a',64),repeat('b',64),jsonb_build_object('size_bytes',60000000000::bigint),'verified',$1,0,now()-interval '1 minute' FROM generate_series(100,103) id`, completion)
			if err != nil {
				t.Fatal(err)
			}
			status := finishRun(t, pool, safety, copy)
			if status.ImportedItems != 0 || status.LateTierHold != finishLateBudgetHeld || status.QueueReservedBytes != 240000000000 {
				t.Fatalf("released ambiguous capacity: %+v", status)
			}
		})
	}
}

func TestFinishLateTierReceiptAgeAndInclusiveDebitBoundary(t *testing.T) {
	pool, safety, copy := finishFixture(t, 1)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_reported_at=now()-interval '91 seconds'`); err != nil {
		t.Fatal(err)
	}
	status := finishRun(t, pool, safety, copy)
	if status.CleanupShouldRun || status.LateTierHold != finishLateReceiptStale || status.ImportedItems != 0 {
		t.Fatalf("old receipt spent: %+v", status)
	}
	cutoff := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	for id, at := range []time.Time{cutoff, cutoff.Add(-time.Microsecond)} {
		if _, err := pool.Exec(ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,updated_at) VALUES(13,'raw',$1,repeat('a',64),repeat('b',64),'{"size_bytes":100}','verified',$2)`, 100+id, at); err != nil {
			t.Fatal(err)
		}
	}
	var reserved int64
	if err := pool.QueryRow(ctx, finishLateDebitsSQL, cutoff).Scan(&reserved); err != nil || reserved != 100 {
		t.Fatalf("T boundary not inclusive: %d %v", reserved, err)
	}
}

func TestFinishPublicationMarginTracksClientConstants(t *testing.T) {
	source, err := os.ReadFile("../../../clients/nas-pull/stoarama_pull.py")
	if err != nil {
		t.Fatal(err)
	}
	read := func(pattern string) int {
		t.Helper()
		m := regexp.MustCompile(pattern).FindSubmatch(source)
		if len(m) != 2 {
			t.Fatalf("client sampling/HTTP contract changed: %s", pattern)
		}
		v, err := strconv.Atoi(string(m[1]))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	heartbeat := read(`(?m)^HEARTBEAT_INTERVAL_SEC = ([0-9]+)$`)
	sampleFactor := read(`(?m)^STORAGE_TELEMETRY_MAX_AGE_SEC = HEARTBEAT_INTERVAL_SEC \* ([0-9]+)$`)
	timeout := read(`(?m)^HEARTBEAT_TIMEOUT_SEC = ([0-9]+)$`)
	if time.Duration(heartbeat*sampleFactor)*time.Second != finishClientStorageMaxAge || time.Duration(timeout)*time.Second != finishClientHeartbeatTimeout || finishPublicationMargin != finishClientStorageMaxAge+finishClientHeartbeatTimeout {
		t.Fatal("client bounds changed: update publication margin and retention proof")
	}
	asOf := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	if !finishLateReceiptFresh(asOf, asOf.Add(-70*time.Second)) || finishLateReceiptFresh(asOf, asOf.Add(-70*time.Second-time.Nanosecond)) {
		t.Fatal("publication margin boundary incorrect")
	}
}

func TestFinishPublicationDelayCannotRespentOldSample(t *testing.T) {
	pool, safety, copy := finishFixture(t, 1)
	ctx := context.Background()
	// sample t=0, terminal t=5, payload t=90, receipt t=100, tick t=187.
	// Receipt age87s alone passes90s, but age+M197s exceeds the180s window.
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_reported_at=now()-interval '87 seconds';INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,updated_at) VALUES(13,'raw',100,repeat('a',64),repeat('b',64),'{"size_bytes":100}','verified',now()-interval '182 seconds')`); err != nil {
		t.Fatal(err)
	}
	status := finishRun(t, pool, safety, copy)
	if status.ImportedItems != 0 || status.LateTierHold != finishLateReceiptStale || status.CleanupShouldRun {
		t.Fatalf("stale pre-publication capacity spent: %+v", status)
	}
}

func TestFinishLateBudgetRejectsLegacyStorageSamples(t *testing.T) {
	pool, safety, copy := finishFixture(t, 1)
	ctx := context.Background()
	for _, version := range []string{"", "r2-copy-v4", "v85r-nas-proof-v2.2", "r2-copy-v5-extra"} {
		if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version=$1`, version); err != nil {
			t.Fatal(err)
		}
		status := finishRun(t, pool, safety, copy)
		if status.ImportedItems != 0 || status.LateTierHold != finishLateClientUnknown || status.Exhausted {
			t.Fatalf("legacy sample admitted %q: %+v", version, status)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_client_version=$1`, api.NASStorageBudgetClientVersion); err != nil {
		t.Fatal(err)
	}
	status := finishRun(t, pool, safety, copy)
	if status.ImportedItems != 1 {
		t.Fatal("reviewed sample not admitted")
	}
}

func TestFinishDelayedLegacyTimelinesNeedNewV5Storage(t *testing.T) {
	for _, frame := range []struct {
		name                    string
		receiptAge, terminalAge time.Duration
	}{
		{"receipt87_terminal182", 87 * time.Second, 182 * time.Second},
		{"receipt50_terminal245", 50 * time.Second, 245 * time.Second},
	} {
		t.Run(frame.name, func(t *testing.T) {
			pool, safety, copy := finishFixture(t, 5)
			ctx := context.Background()
			if _, err := pool.Exec(ctx, `UPDATE recording_clips SET size_bytes=60000000000`); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_free_bytes=2500000000000,nas_storage_reported_at=clock_timestamp()-$1::bigint*interval '1 second',nas_storage_client_version=''`, int64(frame.receiptAge/time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,updated_at) SELECT 13,'raw',id,repeat('a',64),repeat('b',64),'{"size_bytes":60000000000}','verified',clock_timestamp()-$1::bigint*interval '1 second' FROM generate_series(100,103) id`, int64(frame.terminalAge/time.Second)); err != nil {
				t.Fatal(err)
			}
			// Both panel timelines have expired terminal debits. The old cached sample
			// must nevertheless remain ineligible, not become v5 through metadata alone.
			var expired int64
			if err := pool.QueryRow(ctx, finishLateDebitsSQL, time.Now().Add(-finishLateDebitWindow)).Scan(&expired); err != nil || expired != 0 {
				t.Fatalf("timeline fixture not expired: %d %v", expired, err)
			}
			status := finishRun(t, pool, safety, copy)
			if status.ImportedItems != 0 || status.LateTierHold != finishLateClientUnknown {
				t.Fatalf("legacy timeline spent: %+v", status)
			}
			// The real v5 producer samples after assembly: its new sample includes the
			// 240GB write. The handler binding test proves only a real v5 storage update
			// can assign this version, and omission preserves the old unknown binding.
			if _, err := pool.Exec(ctx, `UPDATE connections SET nas_storage_free_bytes=2260000000000,nas_storage_reported_at=clock_timestamp(),nas_storage_client_version=$1`, api.NASStorageBudgetClientVersion); err != nil {
				t.Fatal(err)
			}
			status = finishRun(t, pool, safety, copy)
			if status.ImportedItems != 0 || !status.CleanupShouldRun || status.LateTierHold != finishLateSpaceLow {
				t.Fatalf("fresh v5 sample not bounded: %+v", status)
			}
		})
	}
}
