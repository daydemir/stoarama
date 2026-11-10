package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const safeguardPolicy = "060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811"

// Safeguard 1: only a streamed-SHA-verified R2 recovery copy can ever yield a
// deletion permit; nas_only_at_check never does.
func TestNASCleanupPermitRequiresVerifiedR2Copy(t *testing.T) {
	until := time.Now().Add(time.Hour)
	past := time.Now().Add(-time.Minute)
	if !cleanupMayIssue("enabled", true, &until, true, "r2_sha_verified") {
		t.Fatal("verified recovery copy did not permit")
	}
	for _, recovery := range []string{"nas_only_at_check", "unchecked", "r2_present_unverified", "conflict", ""} {
		if cleanupMayIssue("enabled", true, &until, true, recovery) {
			t.Fatalf("permit issued without a verified R2 copy: %q", recovery)
		}
	}
	for name, ok := range map[string]bool{
		"dry_run":      cleanupMayIssue("dry_run", true, &until, true, "r2_sha_verified"),
		"paused":       cleanupMayIssue("paused", true, &until, true, "r2_sha_verified"),
		"master_off":   cleanupMayIssue("enabled", false, &until, true, "r2_sha_verified"),
		"no_approval":  cleanupMayIssue("enabled", true, nil, true, "r2_sha_verified"),
		"expired":      cleanupMayIssue("enabled", true, &past, true, "r2_sha_verified"),
		"proof_change": cleanupMayIssue("enabled", true, &until, false, "r2_sha_verified"),
	} {
		if ok {
			t.Fatalf("permit issued with %s", name)
		}
	}
}

func safeguardControl(t *testing.T, s *Server, id int64, mode string, extra map[string]any) (int, string) {
	t.Helper()
	body := map[string]any{"plan_sha256": strings.Repeat("b", 64), "mode": mode, "reason": "safeguard fixture"}
	for k, v := range extra {
		body[k] = v
	}
	w := cleanupCall(t, s.handleAdminNASCleanupControl, 47, 5, "id", fmt.Sprint(id), body)
	return w.Code, w.Body.String()
}

func safeguardPlan(t *testing.T, ctx context.Context, s *Server, id int64, mode string, sizes ...int64) {
	t.Helper()
	items := []map[string]any{}
	for n, size := range sizes {
		items = append(items, map[string]any{"kind": "raw", "source_id": 1000*id + int64(n), "stream_id": 3, "size_bytes": size})
	}
	manifest, _ := json.Marshal(items)
	approved := "NULL"
	reason := ""
	if mode == "enabled" {
		approved, reason = "now()+interval '1 hour'", "fixture"
	}
	if _, err := s.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode,approved_until,approval_reason) VALUES($1,47,13,$6,$2,$3,repeat('b',64),$4,%s,$5)`, approved), id, safeguardPolicy, string(manifest), mode, reason, fmt.Sprintf("safeguard-%d", id)); err != nil {
		t.Fatal(err)
	}
	for n := range sizes {
		if _, err := s.pool.Exec(ctx, `INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,proof,recovery,recovery_evidence) VALUES($1,$2,$3,'would_delete','{}','r2_sha_verified','{"etag":"fixture"}')`, 1000*id+int64(n), id, n+1); err != nil {
			t.Fatal(err)
		}
	}
}

// Safeguard 1 (control/DB): allow_nas_only is rejected for every mode and by
// the database itself.
func TestNASCleanupNASOnlyRejectedEverywhere(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	s.cfg.NASCleanupDeletionEnabled = true
	safeguardPlan(t, ctx, s, 1, "dry_run", 10)
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET recovery='nas_only_at_check' WHERE plan_id=1`); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"enabled", "paused", "dry_run"} {
		if code, body := safeguardControl(t, s, 1, mode, map[string]any{"allow_nas_only": true}); code != 400 || !strings.Contains(body, "NAS-only deletion was removed") {
			t.Fatalf("%s with allow_nas_only: %d %s", mode, code, body)
		}
	}
	if code, body := safeguardControl(t, s, 1, "enabled", nil); code != 409 || !strings.Contains(body, "verified R2 recovery copy") {
		t.Fatalf("NAS-only item enabled: %d %s", code, body)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_plans SET allow_nas_only=true WHERE id=1`); err == nil || !strings.Contains(err.Error(), "nas_cleanup_plans_no_nas_only") {
		t.Fatalf("database accepted allow_nas_only: %v", err)
	}
}

// Safeguard 2: one enabled plan at a time and a 100 GB per-plan byte cap.
func TestNASCleanupOneEnabledPlanAndPlanByteCap(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	s.cfg.NASCleanupDeletionEnabled = true
	safeguardPlan(t, ctx, s, 1, "dry_run", 10, 20)
	safeguardPlan(t, ctx, s, 2, "dry_run", 10)
	safeguardPlan(t, ctx, s, 3, "dry_run", nasCleanupMaxPlanBytes-1, 2)
	safeguardPlan(t, ctx, s, 4, "dry_run", nasCleanupMaxPlanBytes-1, 1)
	if code, body := safeguardControl(t, s, 1, "enabled", nil); code != 200 {
		t.Fatalf("first enable: %d %s", code, body)
	}
	if code, body := safeguardControl(t, s, 2, "enabled", nil); code != 409 || !strings.Contains(body, "already enabled") {
		t.Fatalf("second concurrent enabled plan accepted: %d %s", code, body)
	}
	// An expired approval still counts: the plan must be paused explicitly.
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_plans SET approved_until=now()-interval '1 minute' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if code, _ := safeguardControl(t, s, 2, "enabled", nil); code != 409 {
		t.Fatal("expired-but-enabled plan did not block a second enable")
	}
	if code, body := safeguardControl(t, s, 1, "paused", nil); code != 200 {
		t.Fatalf("pause: %d %s", code, body)
	}
	if code, body := safeguardControl(t, s, 2, "enabled", nil); code != 200 {
		t.Fatalf("enable after pause: %d %s", code, body)
	}
	if code, _ := safeguardControl(t, s, 2, "paused", nil); code != 200 {
		t.Fatal("pause 2")
	}
	if code, body := safeguardControl(t, s, 3, "enabled", nil); code != 409 || !strings.Contains(body, "per-plan deletion cap") {
		t.Fatalf("plan above 100 GB enabled: %d %s", code, body)
	}
	if code, body := safeguardControl(t, s, 4, "enabled", nil); code != 200 {
		t.Fatalf("plan at exactly 100 GB refused: %d %s", code, body)
	}
	var enabled int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_plans WHERE mode='enabled'`).Scan(&enabled); err != nil || enabled != 1 {
		t.Fatalf("enabled plans %d %v", enabled, err)
	}
}

// Safeguard 2: rolling 24h budget of issued deletion bytes across all plans.
func TestNASCleanupRollingDeletionBudget(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	if nasCleanupMaxIssuedBytes24h != 5_000_000_000_000 {
		t.Fatal("the 24h deletion budget is a reviewed constant (5 TB)")
	}
	safeguardPlan(t, ctx, s, 1, "paused", nasCleanupMaxIssuedBytes24h-100_000_000_000, 50_000_000_000)
	safeguardPlan(t, ctx, s, 2, "paused", 40_000_000_000)
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='deleted',delete_ever_issued=true,updated_at=now()-interval '1 hour' WHERE id=1000;
UPDATE nas_cleanup_items SET state='deleted',delete_ever_issued=true,updated_at=now()-interval '25 hours' WHERE id=1001;`); err != nil {
		t.Fatal(err)
	}
	allows := func(size int64) bool {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		ok, err := cleanupBudgetAllows(ctx, tx, size)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !allows(100_000_000_000) {
		t.Fatal("permit within the 24h budget refused")
	}
	if allows(100_000_000_001) || allows(0) {
		t.Fatal("permit beyond the 24h budget (or empty) issued")
	}
	// A not-yet-receipted issued delete counts too, from any plan.
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='delete_issued',delete_ever_issued=true WHERE id=2000`); err != nil {
		t.Fatal(err)
	}
	if allows(60_000_000_001) || !allows(60_000_000_000) {
		t.Fatal("issued delete of another plan not counted")
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET updated_at=now()-interval '25 hours' WHERE id IN (1000,2000)`); err != nil {
		t.Fatal(err)
	}
	if !allows(nasCleanupMaxIssuedBytes24h) {
		t.Fatal("budget did not roll after 24h")
	}
}

// Safeguard 3: any anomaly during execution pauses the enabled plan.
func TestNASCleanupExecutionAnomaliesAutoPause(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	s := &Server{pool: pool}
	s.cfg.NASCleanupDeletionEnabled = true
	mode := func(id int64) (string, string) {
		t.Helper()
		var m, reason string
		if err := pool.QueryRow(ctx, `SELECT mode,approval_reason FROM nas_cleanup_plans WHERE id=$1`, id).Scan(&m, &reason); err != nil {
			t.Fatal(err)
		}
		return m, reason
	}
	lease := func() []cleanupTask {
		t.Helper()
		w := cleanupCall(t, s.handleAccountNASCleanupLease, 47, 5, "", "", map[string]any{"client_version": "test", "protocol": 1, "reconciliation_protocol": nasCleanupReconciliationProtocol})
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
		var r struct {
			Tasks []cleanupTask `json:"tasks"`
		}
		json.Unmarshal(w.Body.Bytes(), &r)
		return r.Tasks
	}

	// A conflict (e.g. the NAS client's own keep-window guard refusing) during
	// execution pauses the plan.
	safeguardPlan(t, ctx, s, 1, "enabled", 10, 10)
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='pending' WHERE plan_id=1`); err != nil {
		t.Fatal(err)
	}
	tasks := lease()
	if len(tasks) != 1 || tasks[0].ItemID != 1000 {
		t.Fatalf("execution lease %+v", tasks)
	}
	w := cleanupCall(t, s.handleAccountNASCleanupObserve, 47, 5, "itemId", "1000", cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: 1, PlanSHA: strings.Repeat("b", 64), Outcome: "conflict", Error: "client keep-window guard refused", ClientVersion: "test"})
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"plan_paused":true`) {
		t.Fatalf("conflict observation: %d %s", w.Code, w.Body.String())
	}
	if m, reason := mode(1); m != "paused" || !strings.HasPrefix(reason, "auto-paused: item 1000 observed conflict: client keep-window guard") {
		t.Fatalf("plan not auto-paused: %s %q", m, reason)
	}
	if len(lease()) != 0 {
		t.Fatal("paused plan still leased execution work")
	}

	// An execution lease that expires without a report pauses the plan.
	safeguardPlan(t, ctx, s, 2, "enabled", 10)
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='leased',attempt=1,lease_expires_at=now()-interval '1 minute' WHERE plan_id=2`); err != nil {
		t.Fatal(err)
	}
	if got := lease(); len(got) != 0 {
		t.Fatalf("expired execution lease re-leased: %+v", got)
	}
	if m, _ := mode(2); m != "paused" {
		t.Fatalf("expired execution lease left plan %s", m)
	}

	// Receipts: anything but a clean deletion pauses; the honest sidecar
	// failure state is recorded as such.
	safeguardPlan(t, ctx, s, 3, "enabled", 10)
	safeguardPlan(t, ctx, s, 4, "enabled", 10)
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='delete_issued',delete_ever_issued=true,attempt=1,proof_sha256=repeat('c',64) WHERE plan_id IN (3,4)`); err != nil {
		t.Fatal(err)
	}
	receipt := func(item, outcome string) string {
		t.Helper()
		w := cleanupCall(t, s.handleAccountNASCleanupResult, 47, 5, "itemId", item, map[string]any{"attempt": 1, "plan_sha256": strings.Repeat("b", 64), "proof_sha256": strings.Repeat("c", 64), "outcome": outcome, "error": ""})
		if w.Code != 200 {
			t.Fatalf("%s receipt: %d %s", outcome, w.Code, w.Body.String())
		}
		return w.Body.String()
	}
	if body := receipt("4000", "deleted"); !strings.Contains(body, `"plan_paused":false`) {
		t.Fatal(body)
	}
	if m, _ := mode(4); m != "enabled" {
		t.Fatal("clean deletion paused its plan")
	}
	if body := receipt("3000", "media_deleted_sidecar_failed"); !strings.Contains(body, `"plan_paused":true`) {
		t.Fatal(body)
	}
	receipt("3000", "media_deleted_sidecar_failed") // idempotent replay
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM nas_cleanup_items WHERE id=3000`).Scan(&state); err != nil || state != "media_deleted_sidecar_failed" {
		t.Fatalf("honest receipt state %q %v", state, err)
	}
	if m, reason := mode(3); m != "paused" || !strings.Contains(reason, "media_deleted_sidecar_failed") {
		t.Fatalf("sidecar failure left plan %s %q", m, reason)
	}
	// Unverified receipt.
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='delete_issued' WHERE id=4000`); err != nil {
		t.Fatal(err)
	}
	receipt("4000", "unverified")
	if m, _ := mode(4); m != "paused" {
		t.Fatal("unverified receipt did not pause")
	}
}

// Council fix: concurrent non-permit observations of one enabled plan both
// auto-pause it; they must serialize on the per-plan fence, never deadlock.
func TestNASCleanupConcurrentAutoPauseDoesNotDeadlock(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := &Server{pool: pool}
	s.cfg.NASCleanupDeletionEnabled = true
	observe := func(item int64) *httptest.ResponseRecorder {
		return cleanupCall(t, s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(item), cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: 1, PlanSHA: strings.Repeat("b", 64), Outcome: "conflict", Error: "fixture", ClientVersion: "test"})
	}
	// Deterministic ordering: the fence is taken before any plan row lock.
	safeguardPlan(t, ctx, s, 1, "enabled", 10)
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='leased',attempt=1,lease_connection_id=13,lease_client_boot_id='fixture-boot',lease_client_version='be17fd73',lease_expires_at=now()+interval '10 minutes' WHERE plan_id=1`); err != nil {
		t.Fatal(err)
	}
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = holder.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupPlanLockBase+1); err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- observe(1000) }()
	waiting := false
	for n := 0; n < 200 && !waiting; n++ {
		if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("enabled-plan observation did not wait on the per-plan fence")
	}
	// While it waits it holds no lock on the plan row: an exclusive row lock
	// is immediately available.
	if _, err = holder.Exec(ctx, `SELECT id FROM nas_cleanup_plans WHERE id=1 FOR UPDATE NOWAIT`); err != nil {
		t.Fatalf("waiting observation already holds the plan row: %v", err)
	}
	if err = holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if w := <-done; w.Code != 200 || !strings.Contains(w.Body.String(), `"plan_paused":true`) {
		t.Fatalf("fenced observation: %d %s", w.Code, w.Body.String())
	}
	// Racing pairs on the same enabled plan always complete.
	for round := int64(2); round < 12; round++ {
		safeguardPlan(t, ctx, s, round, "enabled", 10, 10)
		if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_items SET state='leased',attempt=1,lease_connection_id=13,lease_client_boot_id='fixture-boot',lease_client_version='be17fd73',lease_expires_at=now()+interval '10 minutes' WHERE plan_id=$1`, round); err != nil {
			t.Fatal(err)
		}
		results := make(chan *httptest.ResponseRecorder, 2)
		for n := int64(0); n < 2; n++ {
			go func(item int64) { results <- observe(item) }(1000*round + n)
		}
		for n := 0; n < 2; n++ {
			if w := <-results; w.Code != 200 {
				t.Fatalf("round %d concurrent observation: %d %s", round, w.Code, w.Body.String())
			}
		}
		var mode string
		if err := pool.QueryRow(ctx, `SELECT mode FROM nas_cleanup_plans WHERE id=$1`, round).Scan(&mode); err != nil || mode != "paused" {
			t.Fatalf("round %d plan %s %v", round, mode, err)
		}
	}
}
