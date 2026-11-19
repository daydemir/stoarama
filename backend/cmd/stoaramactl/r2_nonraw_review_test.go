package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

func (f *nonrawFixture) collatedEntry() inverseEntry {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return inverseEntry{ObjectInfo: f.store.objects[f.key], Class: inverseCollated}
}

func (f *nonrawFixture) nonrawIntents(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_nonraw_intents WHERE object_key=$1`, f.key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// R2 reports HEAD Last-Modified in whole seconds while the listing keeps
// milliseconds; the collated wire check must compare at HEAD precision.
func TestR2NonrawHeadLastModifiedSecondPrecision(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_head_precision")
	f.store.mu.Lock()
	o := f.store.objects[f.key]
	o.LastModified = inverseRetirementCutoff().Add(-1500 * time.Millisecond)
	f.store.objects[f.key] = o
	f.store.mu.Unlock()
	a := f.applier(t, "run1")
	defer a.ledger.close()
	if ok, err := a.deleteNonraw(f.ctx, f.collatedEntry()); err != nil || !ok || f.deletes() != 1 {
		t.Fatalf("millisecond listing time: ok=%v err=%v deletes=%d", ok, err, f.deletes())
	}
}

// A joined dry-run scope covering a source protects that RAW clip, so it must
// protect every collated output built from it. A scope below the clip does not.
func TestR2NonrawJoinedDryRunScopeHoldsSource(t *testing.T) {
	for _, tc := range []struct {
		name      string
		highWater int64
		held      bool
	}{
		{"scope_below_source_is_unrelated", 7000000, false},
		{"scope_covering_source_holds_output", 7000001, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNonrawFixture(t, "nonraw_dry_scope")
			f.fixture(`INSERT INTO recording_jobs(id,recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,completed_at)
 VALUES(8000001,30,'2026-06-01','2026-06-01',60,'done','nonraw-scope','2026-06-02')`)
			f.fixture(`UPDATE recording_clips SET recording_job_id=8000001 WHERE id=$1`, f.clipID)
			f.fixture(`INSERT INTO recording_joined_dry_run_scopes(dry_run_id,recording_id,priority_ordinal,local_date,date_ordinal,recording_job_id,high_water_clip_id)
 VALUES('00000000-0000-0000-0000-00000000000a',30,1,'2026-06-01',1,8000001,$1)`, tc.highWater)
			a := f.applier(t, "run1")
			defer a.ledger.close()
			ok, err := a.deleteNonraw(f.ctx, f.collatedEntry())
			if tc.held {
				if err == nil || ok || f.deletes() != 0 || f.nonrawIntents(t) != 0 {
					t.Fatalf("dry-run scope did not hold: ok=%v err=%v deletes=%d", ok, err, f.deletes())
				}
				return
			}
			if err != nil || !ok || f.deletes() != 1 {
				t.Fatalf("unrelated scope held: ok=%v err=%v deletes=%d", ok, err, f.deletes())
			}
		})
	}
}

// Legacy PUT-completion proof never authorizes the collated lane, even with
// every other gate satisfied.
func TestR2NonrawRequiresOwnerFenceProof(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_completion_proof")
	var activated, deadline time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT activated_at,drain_not_before FROM r2_retirement_writer_fences`).Scan(&activated, &deadline); err != nil {
		t.Fatal(err)
	}
	completion := inverseDrainEvidence{Proof: inversePUTCompletionProof, Bucket: "stoarama", EndpointSHA256: f.sdk.EndpointSHA256(), FenceActivatedAt: activated, DrainNotBefore: deadline, ObservedAt: time.Now().UTC(),
		DeployedWriterCoverage: "test fixture all admissions checked", AcceptedPUTCompletion: "test fixture no provider PUTs accepted"}
	data, _ := json.Marshal(completion)
	f.drain = filepath.Join(f.root, "completion.json")
	if err := os.WriteFile(f.drain, data, 0600); err != nil {
		t.Fatal(err)
	}
	f.drainSHA, _ = inverseFileSHA(f.drain)
	a := f.applier(t, "run1")
	defer a.ledger.close()
	if _, err := a.deleteNonraw(f.ctx, f.collatedEntry()); err == nil || !strings.Contains(err.Error(), "owner fence policy proof") || f.deletes() != 0 || f.nonrawIntents(t) != 0 {
		t.Fatalf("completion proof authorized the collated lane: %v deletes=%d", err, f.deletes())
	}
}

// The RAW classifier protects a KE folder named in the R2 key, not only in the
// catalog path; a collated source must be protected the same way.
func TestR2NonrawSourceKeyKeepEverythingFolder(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(28 * 24 * time.Hour)
	outside := start.Add(-30 * 24 * time.Hour)
	outsideEnd := outside.Add(time.Minute)
	x := inverseCollatedAssessment{ID: 7, Key: "joined/nightly-collation-v2/objects/" + nonrawOutputSHA + ".mp4", Path: "nightly/video.mp4", SHA: nonrawOutputSHA, SizeBytes: 900, VerifiedAt: start,
		StreamID: 3, Start: &outside, End: &outsideEnd, Timezone: "Asia/Tokyo",
		Sources: []inverseCollatedSource{{ID: 1, StreamID: 3, Key: "managed/acct-47/KEFolder/a.mp4", Path: "s/a.mp4", Start: &outside, End: &outsideEnd, Timezone: "Asia/Tokyo"}}}
	p := inverseProtection{KEFolders: map[string]bool{},
		Windows: map[int64]api.Keep28Window{3: {StreamID: 3, Timezone: "Asia/Tokyo", Start: start, End: end}},
		Picks:   r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: start.Format(time.RFC3339), KeepEndUTC: end.Format(time.RFC3339)}}}}
	if got := assessCollated(p, x); got.Reason != "" || got.Keep28 {
		t.Fatalf("control held: %q keep28=%v", got.Reason, got.Keep28)
	}
	p.KEFolders["KEFolder"] = true
	if got := assessCollated(p, x); got.Reason != "keep_everything_path" {
		t.Fatalf("KE folder in source key not held: %q", got.Reason)
	}
}

// A purge that commits while the reservation waits on the source row lock must
// be seen: the reservation reassesses under the lock and writes nothing.
func TestR2NonrawReassessesAfterSourceLock(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_reassess")
	a := f.applier(t, "run1")
	defer a.ledger.close()
	blocker, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(f.ctx)
	if _, err = blocker.Exec(f.ctx, `SELECT 1 FROM recording_clips WHERE id=$1 FOR UPDATE`, f.clipID); err != nil {
		t.Fatal(err)
	}
	var pid int
	if err = blocker.QueryRow(f.ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := a.reserveNonraw(f.ctx, f.collatedEntry())
		done <- err
	}()
	// Wait until the reservation is blocked on the blocker's row lock.
	deadline := time.Now().Add(20 * time.Second)
	for {
		var waiting bool
		if err = f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("reservation finished before reaching the source lock: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("reservation never waited on the source lock")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err = blocker.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		t.Fatal(err)
	}
	if _, err = blocker.Exec(f.ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=$1`, f.clipID); err != nil {
		t.Fatal(err)
	}
	if err = blocker.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil {
		t.Fatal("reservation adopted a source purged while its lock was awaited")
	}
	if n := f.nonrawIntents(t); n != 0 || f.deletes() != 0 {
		t.Fatalf("purged source left intents %d deletes %d", n, f.deletes())
	}
	var reservations int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_retirement_reservations WHERE object_key=$1`, f.key).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("reservations %d %v", reservations, err)
	}
}
