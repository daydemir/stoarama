package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
 CREATE TABLE nas_listing_generations(connection_id bigint,generation text,started_at timestamptz,completed_at timestamptz);
 CREATE TABLE nas_listing_files(connection_id bigint,generation text,relative_path text,size_bytes bigint);
 INSERT INTO nas_listing_generations VALUES(13,'old',now()-interval '2 days',now()),(13,'new',now()-interval '1 day',now()),(13,'incomplete',now(),NULL);`)
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
	// IDs 2 and 4 are likely present only in the newest COMPLETED generation.
	// A stale-only hit (1), an incomplete-generation hit (3) are absent-tier;
	// a conflicting size (5) is held out rather than treated as absent.
	_, err := pool.Exec(ctx, `INSERT INTO nas_listing_files VALUES(13,'old','p/copy-0.mp4',100),(13,'new','p/copy-1.mp4',100),(13,'incomplete','p/copy-2.mp4',100),(13,'new','p/copy-3.mp4',100),(13,'new','p/copy-4.mp4',101)`)
	if err != nil {
		t.Fatal(err)
	}
	status, stop := finishTick(ctx, pool, safety, copy)
	if !errors.Is(stop, finishListingConflict) || status.ListingConflicts != 1 || len(status.ListingConflictIDs) != 1 || status.ListingConflictIDs[0] != 5 {
		t.Fatalf("lost listing conflict: %v %+v", stop, status)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=5`); err != nil {
		t.Fatal(err)
	}
	if status.ImportedItems != 6 || status.QueueDepth != 6 || status.Exhausted || status.ImportedManifests != 2 {
		t.Fatalf("feed: %+v", status)
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
	if !ids[2] || !ids[4] {
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
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE attempts=7 AND last_error='retained'`).Scan(&n); err != nil || n != 6 {
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
		{"missing", `SELECT 1`, "nas_space_heartbeat_missing", false},
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
