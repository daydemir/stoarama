package api

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func fixtureMD5(f *cleanupFixture, key string) string {
	f.bucket.mu.Lock()
	defer f.bucket.mu.Unlock()
	sum := md5.Sum(f.bucket.objects[key])
	return hex.EncodeToString(sum[:])
}

func (f *cleanupFixture) recoveryOf(id int64) (string, string, string) {
	f.t.Helper()
	var recovery, mode, md5 string
	if err := f.pool.QueryRow(f.ctx, `SELECT recovery,coalesce(stage_timings->>'recovery_mode',''),coalesce(recovery_evidence->>'md5','') FROM nas_cleanup_items WHERE id=$1`, id).Scan(&recovery, &mode, &md5); err != nil {
		f.t.Fatal(err)
	}
	return recovery, mode, md5
}

// The NAS client's MD5 (from the same read as its SHA-256) matched against
// R2's single-part ETag proves R2 bytes == NAS bytes without a download;
// multipart ETags, mismatches or a missing MD5 fall back to the full SHA.
func TestNASCleanupDryRunMD5ETagRecovery(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("md5-1", syntheticFiles(100, 4, 10))
	tasks := f.leaseV2(64, 0, 0, "")
	if len(tasks) != 4 {
		t.Fatalf("lease %d", len(tasks))
	}
	keys := map[int64]string{}
	md5s := map[int64]string{}
	for _, task := range tasks {
		keys[task.ItemID] = task.Item.ObjectKey
		md5s[task.ItemID] = fixtureMD5(f, task.Item.ObjectKey)
	}
	// Item 1: multipart-style ETag; item 2: client MD5 differs from the ETag;
	// item 3: no MD5 sent. All three fall back to the full download.
	f.bucket.etags = map[string]string{keys[tasks[1].ItemID]: md5s[tasks[1].ItemID][:30] + "-2"}
	md5s[tasks[2].ItemID] = strings.Repeat("0", 32)
	delete(md5s, tasks[3].ItemID)
	for _, r := range f.observeMD5(tasks, nil, md5s) {
		if r.Status != 200 || r.Action != "would_delete" {
			t.Fatalf("dry run %+v", r)
		}
	}
	if recovery, mode, md5 := f.recoveryOf(tasks[0].ItemID); recovery != "r2_md5_etag_matched" || mode != "md5_etag" || md5 != md5s[tasks[0].ItemID] {
		t.Fatalf("matched MD5: %s %s %s", recovery, mode, md5)
	}
	for _, task := range tasks[1:] {
		if recovery, mode, _ := f.recoveryOf(task.ItemID); recovery != "r2_sha_verified" || mode != "full_sha" {
			t.Fatalf("fallback for %d: %s %s", task.ItemID, recovery, mode)
		}
	}
	if gets, _ := f.bucket.counts(); gets != 3 {
		t.Fatalf("only the three fallbacks may download: %d", gets)
	}
	// The catalogued upload ETag must agree too: without one, or with a
	// different one, a correct client MD5 is not enough.
	g := newCleanupFixture(t, 24)
	g.admit("md5-catalog", syntheticFiles(300, 3, 10))
	gt := g.leaseV2(64, 0, 0, "")
	gm := map[int64]string{}
	for _, task := range gt {
		gm[task.ItemID] = fixtureMD5(g, task.Item.ObjectKey)
	}
	if _, err := g.pool.Exec(g.ctx, `UPDATE recording_clips SET etag='' WHERE id=$1`, gt[1].Item.SourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := g.pool.Exec(g.ctx, `UPDATE recording_clips SET etag=repeat('1',32) WHERE id=$1`, gt[2].Item.SourceID); err != nil {
		t.Fatal(err)
	}
	g.observeMD5(gt, nil, gm)
	if recovery, _, _ := g.recoveryOf(gt[0].ItemID); recovery != "r2_md5_etag_matched" {
		t.Fatalf("quoted catalog ETag not matched: %s", recovery)
	}
	for _, task := range gt[1:] {
		if recovery, mode, _ := g.recoveryOf(task.ItemID); recovery != "r2_sha_verified" || mode != "full_sha" {
			t.Fatalf("catalog ETag missing/different must fall back: %s %s", recovery, mode)
		}
	}
	// Without a recovery store the outcome is "unchecked", not a 503 loop.
	h := newCleanupFixture(t, 24)
	h.admit("no-r2", syntheticFiles(400, 1, 10))
	h.s.r2 = nil
	ht := h.leaseV2(64, 0, 0, "")
	tx, err := h.pool.Begin(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := h.s.cleanupObserveOne(h.ctx, tx, 13, ht[0].ItemID, cleanupObserve{Attempt: ht[0].Attempt, PlanSHA: ht[0].PlanSHA, Outcome: "match", Proof: fixtureProof(ht[0].Item)}, &cleanupObserveEnv{})
	tx.Rollback(h.ctx)
	if r.status != 200 || r.body["recovery"] != "unchecked" {
		t.Fatalf("no recovery store: %d %v", r.status, r.body)
	}
	// MD5-matched items are enable-eligible and execute via the bound HEAD
	// (the multipart-style item stays bound to the ETag it was verified at).
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{p1}, []string{sha1}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	exec := f.leaseV2(64, 64, 1<<40, "")
	for _, r := range f.observe(exec, map[int64]string{exec[0].ItemID: strings.Repeat("a", 32)}) {
		if !r.Delete || r.Recovery != "r2_etag_bound" {
			t.Fatalf("execution %+v", r)
		}
	}
}

// A random ~1% of MD5 matches are also fully downloaded; any disagreement is
// a conflict that pauses the (dry-run) plan.
func TestNASCleanupDryRunMD5AuditDisagreementPausesPlan(t *testing.T) {
	old := cleanupMD5AuditRate
	cleanupMD5AuditRate = 1
	defer func() { cleanupMD5AuditRate = old }()
	f := newCleanupFixture(t, 24)
	p1, _, _ := f.admit("audit-1", syntheticFiles(100, 2, 10))
	tasks := f.leaseV2(64, 0, 0, "")
	md5s := map[int64]string{}
	for _, task := range tasks {
		md5s[task.ItemID] = fixtureMD5(f, task.Item.ObjectKey)
	}
	// Item 1's stored bytes no longer match, but the store still reports the
	// original ETag (which equals the client's MD5).
	f.bucket.etags = map[string]string{tasks[1].Item.ObjectKey: md5s[tasks[1].ItemID]}
	f.bucket.put(tasks[1].Item.ObjectKey, []byte(strings.Repeat("x", int(tasks[1].Item.SizeBytes))))
	results := f.observeMD5(tasks, nil, md5s)
	if recovery, mode, _ := f.recoveryOf(tasks[0].ItemID); recovery != "r2_sha_verified" || mode != "md5_etag+full_sha_audit" {
		t.Fatalf("audited match: %s %s", recovery, mode)
	}
	if results[1].Action != "conflict" || !results[1].Paused {
		t.Fatalf("audit disagreement: %+v", results[1])
	}
	if m, reason := f.planMode(p1); m != "paused" || !strings.Contains(reason, "audit") {
		t.Fatalf("dry-run plan not paused: %s %q", m, reason)
	}
}

// R2 work runs before (never inside) the observation transaction, in
// parallel, under a time budget; unfinished items are 503 with nothing saved.
func TestNASCleanupDryRunRecoveryIsParallelBoundedAndOutsideLocks(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.admit("parallel-1", syntheticFiles(100, 16, 20))
	f.bucket.getDelay = 300 * time.Millisecond
	tasks := f.leaseV2(64, 0, 0, "")
	started := time.Now()
	for _, r := range f.observe(tasks, nil) {
		if r.Status != 200 || r.Recovery != "r2_sha_verified" {
			t.Fatalf("%+v", r)
		}
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("16 downloads of 300 ms took %v: not parallel", elapsed)
	}
	// Without a prefetched result the observation does no R2 I/O at all.
	g := newCleanupFixture(t, 24)
	g.admit("parallel-2", syntheticFiles(200, 2, 20))
	tasks = g.leaseV2(64, 0, 0, "")
	tx, err := g.pool.Begin(g.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := g.s.cleanupObserveOne(g.ctx, tx, 13, tasks[0].ItemID, cleanupObserve{Attempt: tasks[0].Attempt, PlanSHA: tasks[0].PlanSHA, Outcome: "match", Proof: fixtureProof(tasks[0].Item)}, &cleanupObserveEnv{})
	tx.Rollback(context.Background())
	if gets, heads := g.bucket.counts(); r.status != 503 || gets != 0 || heads != 0 {
		t.Fatalf("observation without prefetch: %d gets %d heads %d", r.status, gets, heads)
	}
	// Budget exhausted: 503 per item, item stays leased, nothing recorded.
	old := nasCleanupRecoveryBudget
	nasCleanupRecoveryBudget = 100 * time.Millisecond
	defer func() { nasCleanupRecoveryBudget = old }()
	g.bucket.getDelay = time.Second
	for _, r := range g.observe(tasks, nil) {
		if r.Status != 503 {
			t.Fatalf("over-budget item %+v", r)
		}
	}
	var leased int
	if err := g.pool.QueryRow(g.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE state='leased' AND outcome IS NULL`).Scan(&leased); err != nil || leased != 2 {
		t.Fatalf("over-budget items changed: %d %v", leased, err)
	}
}

// Plan 1389 stall (2026-11-09): expired leases were only swept by the lease
// call, and a request repeating an item id was rejected as a whole, so a
// client replaying while a stale copy was still queued never recovered.
func TestNASCleanupExpiredLeasesSweptWithoutLeaseAndDuplicateIDsTolerated(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.admit("stall-1", syntheticFiles(100, 3, 10))
	tasks := f.leaseV2(64, 0, 0, "")
	// Duplicate item id in one request: processed once, same answer twice.
	o := map[string]any{"item_id": tasks[0].ItemID, "attempt": tasks[0].Attempt, "plan_sha256": tasks[0].PlanSHA, "outcome": "match", "proof": fixtureProof(tasks[0].Item), "error": "", "request_id": strings.Repeat("a", 32)}
	w := cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": "t", "observations": []map[string]any{o, o}})
	if w.Code != 200 || strings.Count(w.Body.String(), `"would_delete"`) != 2 {
		t.Fatalf("duplicate ids: %d %s", w.Code, w.Body.String())
	}
	if gets, _ := f.bucket.counts(); gets != 1 {
		t.Fatalf("duplicate was processed twice: %d downloads", gets)
	}
	// The remaining leases expire; no lease call follows. Housekeeping (run
	// by the heartbeat and every batched call) returns them to pending.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET lease_expires_at=now()-interval '1 minute' WHERE state='leased'`); err != nil {
		t.Fatal(err)
	}
	f.s.cleanupHousekeeping(f.ctx, 13)
	var leased, pending int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER (WHERE state='leased'),count(*) FILTER (WHERE state='pending') FROM nas_cleanup_items`).Scan(&leased, &pending); err != nil || leased != 0 || pending != 2 {
		t.Fatalf("expired leases not swept: leased %d pending %d %v", leased, pending, err)
	}
	// A stale observation of the old attempt is refused; a fresh lease works.
	stale := f.observe(tasks[1:2], nil)
	if stale[0].Status != 409 {
		t.Fatalf("stale observation after sweep: %+v", stale[0])
	}
	if again := f.leaseV2(64, 0, 0, ""); len(again) != 2 || again[0].Attempt != 2 {
		t.Fatalf("re-lease after sweep: %+v", again)
	}
}

// Only byte-identical duplicates are collapsed; a differing copy gets its own
// 409, and conflicting duplicate receipts pause the plan.
func TestNASCleanupConflictingDuplicatesAreRefused(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.admit("dups-1", syntheticFiles(100, 1, 10))
	tasks := f.leaseV2(64, 0, 0, "")
	o := map[string]any{"item_id": tasks[0].ItemID, "attempt": tasks[0].Attempt, "plan_sha256": tasks[0].PlanSHA, "outcome": "match", "proof": fixtureProof(tasks[0].Item), "error": "", "request_id": strings.Repeat("a", 32)}
	other := map[string]any{}
	for k, v := range o {
		other[k] = v
	}
	other["request_id"] = strings.Repeat("b", 32)
	w := cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": "t", "observations": []map[string]any{o, other}})
	var r struct {
		Results []batchResult `json:"results"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil || w.Code != 200 || r.Results[0].Status != 200 || r.Results[1].Status != 409 {
		t.Fatalf("differing duplicate observation: %d %s", w.Code, w.Body.String())
	}

	s := f.s
	s.cfg.NASCleanupDeletionEnabled = true
	safeguardPlan(t, f.ctx, s, 70, "enabled", 10)
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='delete_issued',delete_ever_issued=true,attempt=1,proof_sha256=repeat('c',64) WHERE plan_id=70`); err != nil {
		t.Fatal(err)
	}
	receipt := map[string]any{"item_id": 70000, "attempt": 1, "plan_sha256": strings.Repeat("b", 64), "proof_sha256": strings.Repeat("c", 64), "outcome": "deleted", "error": ""}
	same := cleanupCall(t, s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "t", "receipts": []map[string]any{receipt, receipt}})
	if same.Code != 200 || strings.Count(same.Body.String(), `"status":200`) != 2 {
		t.Fatalf("identical duplicate receipts: %d %s", same.Code, same.Body.String())
	}
	if m, _ := f.planMode(70); m != "enabled" {
		t.Fatal("identical duplicate receipts paused the plan")
	}
	conflict := map[string]any{}
	for k, v := range receipt {
		conflict[k] = v
	}
	conflict["outcome"], conflict["error"] = "unverified", "x"
	w = cleanupCall(t, s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "t", "receipts": []map[string]any{receipt, conflict}})
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":409`) {
		t.Fatalf("conflicting duplicate receipts: %d %s", w.Code, w.Body.String())
	}
	if m, reason := f.planMode(70); m != "paused" || !strings.Contains(reason, "conflicting duplicate") {
		t.Fatalf("conflicting receipts did not pause: %s %q", m, reason)
	}
}
