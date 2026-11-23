package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// The 20-day policy is a new plan binding. A keep28 plan is inert: it is never
// leased, resumed, enabled, rechecked, batched or retired automatically, and an
// honest old receipt stays exactly as recorded. The 20-day plan is the only
// thing the modern lanes will touch.
func TestNASCleanupKeep28PlansAreInertUnderThe20DayPin(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	s.cfg.NASCleanupDeletionEnabled = true
	// Stream 3's active window starts 2026-09-15T15:00Z. A clip inside it is
	// KEEP; the old 28-day window started 2026-09-11T15:00Z, so the interval
	// [2026-09-12,2026-09-13) is inside BOTH (never eligible) while
	// [2026-09-05,2026-09-06) is new-eligible only under keep28 if the 20-day
	// window were ignored. It is KEEP under the active policy too, so both
	// probes below confirm the active window and not the legacy one.
	if _, err := pool.Exec(ctx, `INSERT INTO recording_clips VALUES(1,2,NULL,'object',repeat('a',64),3,'a/b.mp4','2026-09-12','2026-09-12 00:01:00','bucket');
 INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES
 (1,47,13,'keep28-dry','060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811','[{"kind":"raw","source_id":1}]',repeat('b',64),'dry_run'),
 (2,47,13,'keep20-dry','bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393','[{"kind":"raw","source_id":1}]',repeat('c',64),'dry_run');
 INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state) VALUES(1,1,1,'pending'),(2,2,1,'pending');
 INSERT INTO nas_cleanup_frozen_streams(plan_id,stream_id,validated) VALUES(2,3,true);
 UPDATE connections SET client_version='cleanup-batch-v8' WHERE id=13;`); err != nil {
		t.Fatal(err)
	}
	tasks := func() []cleanupTask {
		t.Helper()
		w := cleanupCall(t, s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{"client_version": nasCleanupItemTruthClientVersion, "protocol": 1, "limit": 8})
		var r struct {
			Tasks []cleanupTask `json:"tasks"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &r) != nil {
			t.Fatalf("lease: %d %s", w.Code, w.Body.String())
		}
		return r.Tasks
	}
	if got := tasks(); len(got) != 1 || got[0].PlanSHA != strings.Repeat("c", 64) {
		t.Fatalf("keep28 plan was leased: %+v", got)
	}
	control := func(id, sha, mode string) int {
		return cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", id,
			map[string]any{"plan_sha256": sha, "mode": mode, "reason": "policy binding probe", "allow_nas_only": false}).Code
	}
	if control("1", strings.Repeat("b", 64), "dry_run") != 409 ||
		control("1", strings.Repeat("b", 64), "enabled") != 409 {
		t.Fatal("keep28 plan resumed or armed under the 20-day pin")
	}
	if control("2", strings.Repeat("c", 64), "dry_run") != 200 {
		t.Fatal("current 20-day plan could not stay in its dry run")
	}
	recheck := cleanupCall(t, s.handleAdminNASCleanupRecheck, 47, 5, "id", "1",
		map[string]any{"expected_plan_sha256": strings.Repeat("b", 64), "reason": "policy binding probe"})
	if recheck.Code != 409 {
		t.Fatalf("keep28 plan was rechecked under the 20-day pin: %d %s", recheck.Code, recheck.Body.String())
	}
	batch := cleanupCall(t, s.handleAdminNASCleanupBatchEnable, 47, 5, "", "",
		map[string]any{"plan_ids": []int64{1}, "batch_sha256": cleanupBatchDigest(strings.Repeat("b", 64), []int64{1}, []string{strings.Repeat("b", 64)}), "reason": "probe", "approval_hours": 1})
	if batch.Code != 409 {
		t.Fatalf("keep28 plan joined a batch: %d %s", batch.Code, batch.Body.String())
	}
	// The status projection is honest: the old plan is explicitly stale.
	status := cleanupCall(t, s.handleAdminNASCleanupStatus, 47, 5, "id", "1", nil)
	var detail map[string]any
	if status.Code != 200 || json.Unmarshal(status.Body.Bytes(), &detail) != nil ||
		detail["policy_current"] != false || detail["policy_sha256"] != cleanupLegacyKeep28SHA {
		t.Fatalf("stale plan status differs: %d %s", status.Code, status.Body.String())
	}
	// An honest receipt recorded under the old policy is preserved verbatim by
	// the observation path (which refuses the old plan). A retired old plan
	// keeps its item evidence byte-for-byte.
	if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,delete_ever_issued,attempt,proof_sha256,outcome)
 VALUES(3,1,2,'deleted',true,1,repeat('d',64),'{"outcome":"deleted","unlink_attempted":true}'::jsonb)`); err != nil {
		t.Fatal(err)
	}
	before := cleanupCall(t, s.handleAdminNASCleanupStatus, 47, 5, "id", "1", nil).Body.String()
	if retire := cleanupCall(t, s.handleAdminNASCleanupRetireSuperseded, 47, 5, "", "",
		map[string]any{"reason": "20-day supersedes keep28"}); retire.Code != 200 {
		t.Fatalf("retire preview: %d %s", retire.Code, retire.Body.String())
	}
	after := cleanupCall(t, s.handleAdminNASCleanupStatus, 47, 5, "id", "1", nil).Body.String()
	if before != after {
		t.Fatal("preview changed honest old receipt evidence")
	}
	if code := control("1", strings.Repeat("b", 64), "paused"); code != 200 {
		t.Fatalf("stale plan could not be paused: %d", code)
	}
	var outcome string
	if err := pool.QueryRow(ctx, `SELECT outcome::text FROM nas_cleanup_items WHERE id=3`).Scan(&outcome); err != nil ||
		outcome != `{"outcome": "deleted", "unlink_attempted": true}` {
		t.Fatalf("old receipt evidence rewritten: %q %v", outcome, err)
	}
}
