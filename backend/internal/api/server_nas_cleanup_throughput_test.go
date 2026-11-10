package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Shared with cmd/stoaramactl and prepare_keep28_plans.py.
func TestNASCleanupBatchDigestVector(t *testing.T) {
	got := cleanupBatchDigest("060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811", []int64{1388, 1389}, []string{"a4a8bc7137d732ec9bf68df6e823b7ce4527e7cb4a13afdd05b87bc544601ccc", strings.Repeat("b", 64)})
	if got != "f2e8d37dca0fcb75e902fc2a1e4271022dc8cc23b5f6f1d4cbca53b9be8e3328" {
		t.Fatalf("batch digest changed: %s", got)
	}
}

// The fingerprint must change whenever any input of the best-14 replay
// changes, and only then.
func TestNASCleanupRankingsFingerprintCoversReplayInputs(t *testing.T) {
	f := newCleanupFixture(t, 24)
	fingerprint := func() (string, string) {
		t.Helper()
		var best, fp, alone, winner string
		if err := f.pool.QueryRow(f.ctx, cleanupRankingsWithFingerprintSQL, 3).Scan(&best, &fp); err != nil {
			t.Fatal(err)
		}
		if err := f.pool.QueryRow(f.ctx, cleanupRankingsFingerprintSQL, 3).Scan(&alone); err != nil || alone != fp {
			t.Fatalf("fingerprint statements disagree %q %q %v", alone, fp, err)
		}
		if err := f.pool.QueryRow(f.ctx, cleanupCurrentRankingsSQL, 3).Scan(&winner); err != nil || winner != best {
			t.Fatalf("combined replay differs from the replay %q %q %v", winner, best, err)
		}
		return best, fp
	}
	best, base := fingerprint()
	if best != "2026-09-19" || !strings.HasPrefix(base, "336:") {
		t.Fatalf("fixture %s %s", best, base)
	}
	changes := []struct{ name, apply, revert string }{
		{"bounds moved by one microsecond", `UPDATE recording_clips SET clip_end_at=clip_end_at+interval '1 microsecond' WHERE id=5000000`, `UPDATE recording_clips SET clip_end_at=clip_end_at-interval '1 microsecond' WHERE id=5000000`},
		{"new clip", `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) VALUES(9,2,'x',repeat('a',64),1,'x.mp4','2026-07-01','2026-07-01 00:00:01','bucket')`, `DELETE FROM recording_clips WHERE id=9`},
		{"quarantine", `INSERT INTO recording_collation_hours VALUES(77,2,1,'{"clips":[{"clip":{"clip_id":5000001},"disposition":"quarantined"}]}')`, `DELETE FROM recording_collation_hours WHERE id=77`},
		{"timezone", `UPDATE streams SET local_timezone='Asia/Seoul' WHERE id=3`, `UPDATE streams SET local_timezone='Asia/Tokyo' WHERE id=3`},
	}
	for _, c := range changes {
		if _, err := f.pool.Exec(f.ctx, c.apply); err != nil {
			t.Fatal(c.name, err)
		}
		if _, fp := fingerprint(); fp == base {
			t.Fatalf("%s left the fingerprint unchanged", c.name)
		}
		if _, err := f.pool.Exec(f.ctx, c.revert); err != nil {
			t.Fatal(c.name, err)
		}
		if _, fp := fingerprint(); fp != base {
			t.Fatalf("%s revert did not restore the fingerprint", c.name)
		}
	}
	// Rows the replay ignores (another stream, invalid interval) do not matter.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO streams VALUES(4,'UTC');INSERT INTO recordings VALUES(5,47,4,'UTC');INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) VALUES(10,5,'y',repeat('a',64),1,'y.mp4','2026-07-01','2026-07-02','bucket'),(11,2,'z',repeat('a',64),1,'z.mp4','2026-07-01','2026-07-01','bucket')`); err != nil {
		t.Fatal(err)
	}
	if _, fp := fingerprint(); fp != base {
		t.Fatal("ignored rows changed the fingerprint")
	}
}

// End to end on the server: dry run with full R2 verification, one batch
// approval for two plans, execution without re-downloading, idempotent
// replays, auto-advance and completion.
func TestNASCleanupBatchApprovalExecutesPlansWithoutRedownload(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, items1 := f.admit("throughput-1", syntheticFiles(100, 3, 2))
	p2, sha2, items2 := f.admit("throughput-2", syntheticFiles(200, 2, 2))
	var same bool
	if err := f.pool.QueryRow(f.ctx, `SELECT bool_and(i.item=p.manifest->(i.ordinal-1) AND i.size_bytes=(i.item->>'size_bytes')::bigint) FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id`).Scan(&same); err != nil || !same {
		t.Fatalf("item copies differ from the manifest: %v", err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET item='{}' WHERE id=$1`, items1[0]); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("item copy was editable: %v", err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET size_bytes=size_bytes+1 WHERE id=$1`, items1[0]); err == nil {
		t.Fatal("item size was editable")
	}

	f.dryRun()
	gets, _ := f.bucket.counts()
	if gets != 5 {
		t.Fatalf("dry run must byte-verify every R2 copy once: %d GETs", gets)
	}
	var evidence int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE recovery_evidence->>'etag'<>'' AND state='would_delete'`).Scan(&evidence); err != nil || evidence != 5 {
		t.Fatalf("recovery evidence %d %v", evidence, err)
	}

	// Approval gates.
	if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 6); code != 409 || !strings.Contains(body, "master switch") {
		t.Fatalf("batch enabled with master off: %d %s", code, body)
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	w := cleanupCall(t, f.s.handleAdminNASCleanupBatchEnable, 47, 5, "", "", map[string]any{"plan_ids": []int64{p1, p2}, "batch_sha256": strings.Repeat("a", 64), "reason": "x", "approval_hours": 6, "allow_long_approval": true})
	if w.Code != 409 || !strings.Contains(w.Body.String(), cleanupBatchDigest(cleanupPolicySHA, []int64{p1, p2}, []string{sha1, sha2})) {
		t.Fatalf("wrong batch digest accepted: %d %s", w.Code, w.Body.String())
	}
	for _, hours := range []int{0, 25} {
		if code, _ := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, hours); code != 400 {
			t.Fatalf("approval_hours %d accepted", hours)
		}
	}
	w = cleanupCall(t, f.s.handleAdminNASCleanupBatchEnable, 47, 5, "", "", map[string]any{"plan_ids": []int64{p1, p2}, "batch_sha256": cleanupBatchDigest(cleanupPolicySHA, []int64{p1, p2}, []string{sha1, sha2}), "reason": "x", "approval_hours": 6})
	if w.Code != 400 || !strings.Contains(w.Body.String(), "allow_long_approval") {
		t.Fatalf("approval above 4h without the explicit flag: %d %s", w.Code, w.Body.String())
	}
	if code, body := f.enableBatch([]int64{p1, p1}, []string{sha1, sha1}, 6); code != 400 {
		t.Fatalf("duplicate plan accepted: %d %s", code, body)
	}
	if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 6); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	if m, _ := f.planMode(p1); m != "enabled" {
		t.Fatalf("first plan %s", m)
	}
	if m, _ := f.planMode(p2); m != "dry_run" {
		t.Fatalf("second plan enabled early: %s", m)
	}
	if code, body := safeguardControl(t, f.s, p2, "enabled", nil); code != 409 {
		t.Fatalf("single plan enabled beside a batch: %d %s", code, body)
	}
	var rankings []byte
	if err := f.pool.QueryRow(f.ctx, `SELECT execution_rankings FROM nas_cleanup_plans WHERE id=$1`, p1).Scan(&rankings); err != nil || !strings.Contains(string(rankings), `"best_start": "2026-09-19"`) {
		t.Fatalf("activation did not record the replay: %s %v", rankings, err)
	}

	// Execution leases honour the client's caps and are idempotent.
	if tasks := f.leaseV2(64, 0, 1<<40, ""); len(tasks) != 0 {
		t.Fatalf("execution leased beyond a zero file cap: %d", len(tasks))
	}
	if tasks := f.leaseV2(64, 64, 1, ""); len(tasks) != 0 {
		t.Fatal("execution leased beyond the byte cap")
	}
	first := f.leaseV2(64, 2, 1<<40, strings.Repeat("1", 32))
	again := f.leaseV2(64, 2, 1<<40, strings.Repeat("1", 32))
	if len(first) != 1 || len(again) != 1 || first[0].ItemID != again[0].ItemID || first[0].Attempt != again[0].Attempt || !first[0].ExpiresAt.Equal(again[0].ExpiresAt) || len(first[0].ApprovedProof) == 0 {
		t.Fatalf("lease replay %+v %+v", first, again)
	}
	if rest := f.leaseV2(64, 64, 1<<40, strings.Repeat("2", 32)); len(rest) != 0 {
		t.Fatalf("leased beside outstanding execution: %+v", rest)
	}
	var tasks []cleanupTask
	var results []batchResult
	for n, itemID := range items1 {
		current := first
		if n > 0 {
			current = f.leaseV2(64, 64, 1<<40, "")
		}
		if len(current) != 1 || current[0].ItemID != itemID {
			t.Fatalf("serial lease: %+v", current)
		}
		ids := map[int64]string{itemID: fmt.Sprintf("%032x", n+1)}
		permit := f.observe(current, ids)
		if permit[0].Status != 200 || !permit[0].Delete || permit[0].Recovery != "r2_etag_bound" {
			t.Fatalf("permit: %+v", permit)
		}
		replay := f.observe(current, ids)
		if !replay[0].Delete || !replay[0].Replayed || !replay[0].ExpiresAt.Equal(permit[0].ExpiresAt) {
			t.Fatalf("permit replay: %+v", replay)
		}
		if other := f.observe(current, map[int64]string{itemID: strings.Repeat("9", 32)}); other[0].Delete {
			t.Fatal("different request obtained permit")
		}
		if got := f.leaseV2(64, 64, 1<<40, ""); len(got) != 0 {
			t.Fatal("lease overtook permit")
		}
		for _, r := range f.receipts(current, permit, "deleted") {
			if r["status"].(float64) != 200 {
				t.Fatalf("receipt: %+v", r)
			}
		}
		tasks = append(tasks, current...)
		results = append(results, permit...)
	}
	if g, heads := f.bucket.counts(); g != 5 || heads < 8 {
		t.Fatalf("execution re-downloaded R2 media: gets %d heads %d", g, heads)
	}
	var modes []string
	rows, err := f.pool.Query(f.ctx, `SELECT stage_timings->>'rankings_mode'||'/'||(stage_timings->>'recovery_mode') FROM nas_cleanup_items WHERE plan_id=$1`, p1)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var m string
		rows.Scan(&m)
		modes = append(modes, m)
	}
	rows.Close()
	if strings.Join(modes, ",") != "fingerprint/etag_bound,fingerprint/etag_bound,fingerprint/etag_bound" {
		t.Fatalf("execution did not use the fast paths: %v", modes)
	}
	var issued int
	var labels string
	if err := f.pool.QueryRow(f.ctx, `SELECT string_agg(DISTINCT recovery,',') FROM nas_cleanup_items WHERE plan_id=$1`, p1).Scan(&labels); err != nil || labels != "r2_etag_bound" {
		t.Fatalf("delete-time recovery must be labelled r2_etag_bound: %q %v", labels, err)
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE delete_ever_issued`).Scan(&issued); err != nil || issued != 3 {
		t.Fatalf("issued %d %v", issued, err)
	}
	for _, r := range f.receipts(tasks, results, "deleted") {
		if r["status"].(float64) != 200 {
			t.Fatalf("receipt %+v", r)
		}
	}
	// Receipt replay is idempotent.
	f.receipts(tasks, results, "deleted")
	if m, reason := f.planMode(p1); m != "paused" || !strings.HasPrefix(reason, "batch-completed") {
		t.Fatalf("finished plan %s %q", m, reason)
	}
	if m, reason := f.planMode(p2); m != "enabled" || !strings.HasPrefix(reason, "batch ") {
		t.Fatalf("batch did not advance: %s %q", m, reason)
	}
	for _, itemID := range items2 {
		current := f.leaseV2(64, 64, 1<<40, "")
		if len(current) != 1 || current[0].ItemID != itemID {
			t.Fatalf("second plan serial lease: %+v", current)
		}
		permit := f.observe(current, map[int64]string{itemID: strings.Repeat("3", 32)})
		if !permit[0].Delete {
			t.Fatalf("second plan permit: %+v", permit)
		}
		f.receipts(current, permit, "deleted")
	}
	if m, _ := f.batchMode(); m != "completed" {
		t.Fatalf("batch %s", m)
	}
	if m, _ := f.planMode(p2); m != "paused" {
		t.Fatalf("last plan left %s", m)
	}
	var enabled int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_plans WHERE mode='enabled'`).Scan(&enabled); err != nil || enabled != 0 {
		t.Fatal("plan left enabled after completion")
	}
}

// Any anomaly pauses the plan and its batch; the batch never re-enables a
// stopped plan; a changed replay input falls back to the full replay.
func TestNASCleanupBatchAnomaliesPauseAndFallBackToFullChecks(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("anomaly-1", syntheticFiles(100, 2, 10))
	p2, sha2, _ := f.admit("anomaly-2", syntheticFiles(200, 2, 10))
	p3, sha3, _ := f.admit("anomaly-3", syntheticFiles(300, 2, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	// Replace one R2 object with different bytes of the same size: the ETag
	// differs, so the bound check is a conflict (no permit) and everything pauses.
	tasks := f.leaseV2(64, 64, 1<<40, "")
	if len(tasks) != 1 {
		t.Fatalf("lease %d", len(tasks))
	}
	results := f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})
	if !results[0].Delete {
		t.Fatalf("first permit: %+v", results)
	}
	f.receipts(tasks, results, "deleted")
	tasks = f.leaseV2(64, 64, 1<<40, "")
	f.bucket.put(tasks[0].Item.ObjectKey, []byte(strings.Repeat("x", int(tasks[0].Item.SizeBytes))))
	results = f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("b", 32)})
	if results[0].Delete || results[0].Action != "conflict" || !results[0].Paused {
		t.Fatalf("replaced R2 object: %+v", results)
	}
	if m, _ := f.planMode(p1); m != "paused" {
		t.Fatal("plan not paused")
	}
	if m, reason := f.batchMode(); m != "paused" || !strings.Contains(reason, "observed conflict") {
		t.Fatalf("batch not paused: %s %q", m, reason)
	}
	f.s.cleanupAdvanceBatchIfIdle(f.ctx)
	if m, _ := f.planMode(p2); m != "dry_run" {
		t.Fatalf("paused batch advanced: %s", m)
	}

	// A fresh batch for p2: an extra clip changes the fingerprint, so the full
	// replay runs (still the pinned winner, so the permit is issued).
	if code, body := f.enableBatch([]int64{p2}, []string{sha2}, 1); code != 200 {
		t.Fatalf("second batch: %d %s", code, body)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) VALUES(9,2,'x',repeat('a',64),1,'x.mp4','2026-07-01','2026-07-01 00:00:10','bucket')`); err != nil {
		t.Fatal(err)
	}
	tasks = f.leaseV2(64, 1, 1<<40, "")
	results = f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("c", 32)})
	var mode string
	if err := f.pool.QueryRow(f.ctx, `SELECT stage_timings->>'rankings_mode' FROM nas_cleanup_items WHERE id=$1`, tasks[0].ItemID).Scan(&mode); err != nil || mode != "replay" || !results[0].Delete {
		t.Fatalf("changed inputs must fall back to the full replay: %s %+v %v", mode, results[0], err)
	}
	f.receipts(tasks, results, "deleted")
	// Now make the replay pick a different winner: KEEP, pause plan and batch.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) SELECT 6000000+n,2,'w',repeat('a',64),1,'w.mp4',timestamptz '2026-06-01 00:00:00+09'+n*interval '1 day',timestamptz '2026-06-01 00:00:00+09'+(n+1)*interval '1 day','bucket' FROM generate_series(0,20) n`); err != nil {
		t.Fatal(err)
	}
	tasks = f.leaseV2(64, 1, 1<<40, "")
	results = f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("d", 32)})
	if results[0].Delete || results[0].Action != "keep" || !results[0].Paused {
		t.Fatalf("changed best14 winner did not keep and pause: %+v", results[0])
	}
	if m, _ := f.batchMode(); m != "paused" {
		t.Fatal("batch not paused on a policy keep")
	}
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM recording_clips WHERE id>=6000000 AND id<6000100`); err != nil {
		t.Fatal(err)
	}

	// Gates at batch enable: plans must be policy-current dry-run plans with a
	// fresh complete dry run and no issued deletion.
	if code, body := f.enableBatch([]int64{p2}, []string{sha2}, 1); code != 409 {
		t.Fatalf("plan with issued deletions re-batched: %d %s", code, body)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET updated_at=now()-interval '25 hours' WHERE plan_id=$1`, p3); err != nil {
		t.Fatal(err)
	}
	if code, body := f.enableBatch([]int64{p3}, []string{sha3}, 1); code != 409 || !strings.Contains(body, "fresh complete dry run") {
		t.Fatalf("stale dry run batched: %d %s", code, body)
	}
}

// Advance re-checks every gate at the moment it enables the next plan.
func TestNASCleanupBatchAdvanceRegatesNextPlan(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("advance-1", syntheticFiles(100, 1, 10))
	p2, sha2, _ := f.admit("advance-2", syntheticFiles(200, 1, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 1); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	// The second plan's dry run goes stale before its turn.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET updated_at=now()-interval '25 hours' WHERE plan_id=$1`, p2); err != nil {
		t.Fatal(err)
	}
	tasks := f.leaseV2(64, 64, 1<<40, "")
	results := f.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})
	f.receipts(tasks, results, "deleted")
	if m, reason := f.batchMode(); m != "paused" || !strings.Contains(reason, "not enableable") {
		t.Fatalf("stale next plan enabled: %s %q", m, reason)
	}
	if m, _ := f.planMode(p2); m != "dry_run" {
		t.Fatal("stale plan enabled")
	}

	// Approval expiry: a batch whose window has (nearly) ended never advances.
	g := newCleanupFixture(t, 24)
	q1, s1, _ := g.admit("expiry-1", syntheticFiles(100, 1, 10))
	q2, s2, _ := g.admit("expiry-2", syntheticFiles(200, 1, 10))
	g.dryRun()
	g.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := g.enableBatch([]int64{q1, q2}, []string{s1, s2}, 1); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	tasks = g.leaseV2(64, 64, 1<<40, "")
	results = g.observe(tasks, map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)})
	if _, err := g.pool.Exec(g.ctx, `UPDATE nas_cleanup_batches SET approved_until=now()+interval '10 seconds'`); err != nil {
		t.Fatal(err)
	}
	g.receipts(tasks, results, "deleted")
	if m, reason := g.batchMode(); m != "paused" || !strings.Contains(reason, "approval expired") {
		t.Fatalf("expired batch advanced: %s %q", m, reason)
	}
	// A paused batch cannot be re-enabled (identity trigger).
	if _, err := g.pool.Exec(g.ctx, `UPDATE nas_cleanup_batches SET mode='enabled',approved_until=now()+interval '1 hour'`); err == nil {
		t.Fatal("paused batch re-enabled in place")
	}
}

// The lease never hands out execution work the rolling budget could not
// permit (which would only auto-pause), and protocol 2 never mixes dry-run
// and execution work in one response.
func TestNASCleanupLeaseV2BudgetAndIsolation(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("budget-1", syntheticFiles(100, 2, 10))
	f.dryRun()
	f.admit("budget-dry", syntheticFiles(300, 2, 10))
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{p1}, []string{sha1}, 1); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	safeguardPlan(t, f.ctx, f.s, 50, "paused", nasCleanupMaxIssuedBytes24h-10)
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='deleted',delete_ever_issued=true,updated_at=now() WHERE plan_id=50`); err != nil {
		t.Fatal(err)
	}
	tasks := f.leaseV2(64, 64, 1<<40, "")
	for _, task := range tasks {
		if len(task.ApprovedProof) != 0 {
			t.Fatalf("execution leased beyond the budget: %+v", task)
		}
	}
	if len(tasks) != 2 {
		t.Fatalf("dry-run work should still flow: %d", len(tasks))
	}
	if m, _ := f.planMode(p1); m != "enabled" {
		t.Fatal("budget precheck paused the plan")
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET updated_at=now()-interval '25 hours' WHERE plan_id=50`); err != nil {
		t.Fatal(err)
	}
	// Leftover dry-run tasks are leased; then execution tasks come alone.
	tasks = f.leaseV2(64, 64, 1<<40, "")
	if len(tasks) != 1 || len(tasks[0].ApprovedProof) == 0 {
		t.Fatalf("execution lease %+v", tasks)
	}
	for _, bad := range []map[string]any{
		{"client_version": "t", "protocol": 2, "limit": 65, "execution_files": 0, "execution_bytes": 0},
		{"client_version": "t", "protocol": 2, "limit": 8},
		{"client_version": "t", "protocol": 2, "limit": 8, "execution_files": -1, "execution_bytes": 0},
		{"client_version": "t", "protocol": 1, "limit": 8, "execution_files": 1, "execution_bytes": 1},
		{"client_version": "t", "protocol": 2, "limit": 8, "execution_files": 1, "execution_bytes": 1, "request_id": "short"},
	} {
		if w := cleanupCall(t, f.s.handleAccountNASCleanupLease, 47, 5, "", "", bad); w.Code != 400 {
			t.Fatalf("invalid lease accepted: %v %d", bad, w.Code)
		}
	}
	// Batched requests validate shape and identity.
	if w := cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": "t", "observations": []map[string]any{}}); w.Code != 400 {
		t.Fatal("empty observation batch accepted")
	}
	w := cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 48, 6, "", "", map[string]any{"client_version": "t", "observations": []map[string]any{{"item_id": tasks[0].ItemID, "attempt": tasks[0].Attempt, "plan_sha256": tasks[0].PlanSHA, "reconciliation_protocol": nasCleanupReconciliationProtocol, "outcome": "match", "proof": fixtureProof(tasks[0].Item), "error": ""}}})
	var r struct {
		Results []batchResult `json:"results"`
	}
	json.Unmarshal(w.Body.Bytes(), &r)
	if w.Code != 200 || len(r.Results) != 1 || r.Results[0].Status != 409 || r.Results[0].Delete {
		t.Fatalf("foreign connection observed another account's item: %d %s", w.Code, w.Body.String())
	}
	_ = time.Now
}

// Council follow-ups: a replayed permit is only returned while execution is
// still authorized; expired approvals pause plan and batch; enable needs ETag
// evidence; batched and single observations never deadlock.
func TestNASCleanupCouncilFollowUps(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("council-1", syntheticFiles(100, 4, 10))
	p2, sha2, _ := f.admit("council-2", syntheticFiles(200, 1, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true

	// Enable refuses an item without dry-run ETag evidence.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET recovery_evidence=NULL WHERE id=(SELECT min(id) FROM nas_cleanup_items WHERE plan_id=$1)`, p2); err != nil {
		t.Fatal(err)
	}
	if code, body := f.enableBatch([]int64{p2}, []string{sha2}, 1); code != 409 || !strings.Contains(body, "ETag evidence") {
		t.Fatalf("plan without ETag evidence enabled: %d %s", code, body)
	}
	if code, body := safeguardControl(t, f.s, p2, "enabled", map[string]any{"plan_sha256": sha2}); code != 409 {
		t.Fatalf("single enable without ETag evidence: %d %s", code, body)
	}

	if code, body := f.enableBatch([]int64{p1}, []string{sha1}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	tasks := f.leaseV2(64, 2, 1<<40, "")
	ids := map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)}
	results := f.observe(tasks, ids)
	if !results[0].Delete {
		t.Fatalf("permits %+v", results)
	}
	// Master switch off: the identical retry no longer returns the permit.
	f.s.cfg.NASCleanupDeletionEnabled = false
	if r := f.observe(tasks[:1], ids); r[0].Delete || r[0].Action != "reconcile" {
		t.Fatalf("replay with master off returned a permit: %+v", r[0])
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	// Approval expired: neither the replay nor any new lease executes, and the
	// next lease pauses the plan and its batch.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET approved_until=now()-interval '1 second' WHERE id=$1`, p1); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_batches SET approved_until=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if r := f.observe(tasks[:1], ids); r[0].Delete {
		t.Fatalf("replay after approval expiry returned a permit: %+v", r[0])
	}
	if got := f.leaseV2(64, 64, 1<<40, ""); len(got) != 0 {
		t.Fatalf("expired approval leased work: %d", len(got))
	}
	if m, reason := f.planMode(p1); m != "paused" || !strings.Contains(reason, "approval expired") {
		t.Fatalf("expired plan left %s %q", m, reason)
	}
	if m, reason := f.batchMode(); m != "paused" || !strings.Contains(reason, "approval expired") {
		t.Fatalf("expired batch left %s %q", m, reason)
	}
	f.receipts(tasks, results, "deleted") // outstanding permits may still be receipted
	// Nothing stays enabled, so a new single enable is not blocked.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET recovery_evidence='{"etag":"x"}' WHERE plan_id=$1`, p2); err != nil {
		t.Fatal(err)
	}
	if code, body := safeguardControl(t, f.s, p2, "enabled", map[string]any{"plan_sha256": sha2}); code != 200 {
		t.Fatalf("enable after expiry: %d %s", code, body)
	}
}

// Single-item and batched observations of one enabled plan race without
// deadlock aborts (both take the plan fence before the budget lock).
func TestNASCleanupBatchAndSingleObservationsShareLockOrder(t *testing.T) {
	f := newCleanupFixture(t, 24)
	p1, sha1, _ := f.admit("locks-1", syntheticFiles(100, 40, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{p1}, []string{sha1}, 2); code != 200 {
		t.Fatalf("batch enable: %d %s", code, body)
	}
	tasks := f.leaseV2(64, 64, 1<<40, "")
	if len(tasks) != 1 {
		t.Fatalf("lease %d", len(tasks))
	}
	done := make(chan string, 40)
	for n := 0; n < 20; n++ {
		single, batch := tasks[0], tasks[0]
		go func() {
			w := cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(single.ItemID), cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: single.Attempt, PlanSHA: single.PlanSHA, Outcome: "match", Proof: fixtureProof(single.Item), ClientVersion: "t", RequestID: strings.Repeat("a", 32)})
			done <- fmt.Sprintf("%d %s", w.Code, w.Body.String())
		}()
		go func() {
			w := cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": "t", "observations": []map[string]any{{"item_id": batch.ItemID, "attempt": batch.Attempt, "plan_sha256": batch.PlanSHA, "reconciliation_protocol": nasCleanupReconciliationProtocol, "outcome": "match", "proof": fixtureProof(batch.Item), "error": "", "client_version": "t", "request_id": strings.Repeat("a", 32)}}})
			done <- fmt.Sprintf("%d %s", w.Code, w.Body.String())
		}()
	}
	for n := 0; n < 40; n++ {
		if r := <-done; !strings.HasPrefix(r, "200 ") || !strings.Contains(r, `"delete":true`) {
			t.Fatalf("concurrent observation: %s", r)
		}
	}
}
