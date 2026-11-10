package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func recheckCall(t *testing.T, s *Server, id int64, sha, scope string) *httptest.ResponseRecorder {
	t.Helper()
	return cleanupCall(t, s.handleAdminNASCleanupRecheck, 47, 5, "id", fmt.Sprint(id), map[string]any{"expected_plan_sha256": sha, "reason": "retry transient recovery check", "scope": scope})
}

func TestNASCleanupRecheckScopesAndUntouchedStates(t *testing.T) {
	for _, scope := range []string{"", "unverified", "all"} {
		t.Run("scope="+scope, func(t *testing.T) {
			pool := cleanupTestPool(t)
			ctx := context.Background()
			s := &Server{pool: pool}
			safeguardPlan(t, ctx, s, 1, "dry_run")
			// Keep even old approval fields unchanged: recheck is not approval.
			if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_plans SET approval_reason='old approval',approved_until=now()-interval '1 hour' WHERE id=1`); err != nil {
				t.Fatal(err)
			}
			states := []string{"would_delete", "would_delete", "would_delete", "would_delete", "would_delete", "would_delete", "unverified", "keep", "conflict", "missing", "delete_issued", "deleted", "pending", "leased"}
			recoveries := []string{"r2_sha_verified", "r2_md5_etag_matched", "r2_present_unverified", "unchecked", "nas_only_at_check", "future_recovery"}
			before := map[int64]string{}
			reset := map[int64]bool{}
			for n, state := range states {
				id := int64(n + 1)
				recovery := "unchecked"
				if n < len(recoveries) {
					recovery = recoveries[n]
				}
				if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,recovery,attempt,lease_expires_at,lease_request_id,proof,proof_sha256,recovery_evidence,policy_reason,outcome,stage_timings,permit_expires_at,updated_at) VALUES($1,1,$2,$3,$4,9,now()+interval '1 hour',repeat('c',32),'{}',repeat('d',64),'{"etag":"old"}','old','{}','{"old":1}',now()+interval '1 minute',now()-interval '2 days')`, id, n+2, state, recovery); err != nil {
					t.Fatal(err)
				}
				reset[id] = state == "unverified" || (state == "would_delete" && (scope == "all" || n >= 2))
				var snapshot string
				if err := pool.QueryRow(ctx, `SELECT to_jsonb(i)::text FROM nas_cleanup_items i WHERE id=$1`, id).Scan(&snapshot); err != nil {
					t.Fatal(err)
				}
				before[id] = snapshot
			}
			var planBefore, planAfter string
			if err := pool.QueryRow(ctx, `SELECT to_jsonb(p)::text FROM nas_cleanup_plans p WHERE id=1`).Scan(&planBefore); err != nil {
				t.Fatal(err)
			}
			w := recheckCall(t, s, 1, strings.Repeat("b", 64), scope)
			var result struct {
				Reset int `json:"reset_items"`
			}
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil {
				t.Fatalf("recheck %d %s", w.Code, w.Body.String())
			}
			want := 5
			if scope == "all" {
				want = 7
			}
			if result.Reset != want {
				t.Fatalf("reset %d want %d", result.Reset, want)
			}
			for id, shouldReset := range reset {
				var after string
				if err := pool.QueryRow(ctx, `SELECT to_jsonb(i)::text FROM nas_cleanup_items i WHERE id=$1`, id).Scan(&after); err != nil {
					t.Fatal(err)
				}
				if !shouldReset {
					if after != before[id] {
						t.Fatalf("untouched item %d changed: %s", id, after)
					}
					continue
				}
				var clean bool
				if err := pool.QueryRow(ctx, `SELECT state='pending' AND attempt=9 AND recheck_attempt_base=9 AND lease_expires_at IS NULL AND lease_request_id IS NULL AND proof IS NULL AND proof_sha256 IS NULL AND recovery='unchecked' AND recovery_evidence IS NULL AND policy_reason='' AND outcome IS NULL AND stage_timings='{}' AND permit_expires_at IS NOT NULL AND NOT delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, id).Scan(&clean); err != nil || !clean {
					t.Fatalf("reset item %d not clean: %v %s", id, err, after)
				}
			}
			if err := pool.QueryRow(ctx, `SELECT to_jsonb(p)::text FROM nas_cleanup_plans p WHERE id=1`).Scan(&planAfter); err != nil || planAfter != planBefore {
				t.Fatalf("plan authority changed: %v %s", err, planAfter)
			}
			// A retry does not repeatedly refresh timestamps or reset active leases.
			w = recheckCall(t, s, 1, strings.Repeat("b", 64), scope)
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"reset_items":0`) {
				t.Fatalf("repeat: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestNASCleanupRecheckRefusesUnsafePlans(t *testing.T) {
	for _, test := range []struct{ name, mode, sha, setup string }{
		{"enabled", "enabled", strings.Repeat("b", 64), ""},
		{"paused", "paused", strings.Repeat("b", 64), ""},
		{"retired", "dry_run", strings.Repeat("b", 64), `UPDATE nas_cleanup_plans SET mode='retired' WHERE id=1`},
		{"sha mismatch", "dry_run", strings.Repeat("a", 64), ""},
		{"issued elsewhere in plan", "dry_run", strings.Repeat("b", 64), `UPDATE nas_cleanup_items SET delete_ever_issued=true,state='keep' WHERE id=1001`},
		{"other account", "dry_run", strings.Repeat("b", 64), `ALTER TABLE nas_cleanup_plans DISABLE TRIGGER nas_cleanup_plan_identity;UPDATE nas_cleanup_plans SET account_id=48 WHERE id=1`},
		{"other connection", "dry_run", strings.Repeat("b", 64), `ALTER TABLE nas_cleanup_plans DISABLE TRIGGER nas_cleanup_plan_identity;UPDATE nas_cleanup_plans SET connection_id=14 WHERE id=1`},
		{"old policy", "dry_run", strings.Repeat("b", 64), `ALTER TABLE nas_cleanup_plans DISABLE TRIGGER nas_cleanup_plan_identity;UPDATE nas_cleanup_plans SET report_sha256=repeat('c',64) WHERE id=1`},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool := cleanupTestPool(t)
			ctx := context.Background()
			s := &Server{pool: pool}
			safeguardPlan(t, ctx, s, 1, test.mode, 1, 1)
			if test.setup != "" {
				if _, err := pool.Exec(ctx, test.setup); err != nil {
					t.Fatal(err)
				}
			}
			var before, after string
			q := `SELECT jsonb_agg(to_jsonb(i) ORDER BY id)::text FROM nas_cleanup_items i`
			if err := pool.QueryRow(ctx, q).Scan(&before); err != nil {
				t.Fatal(err)
			}
			w := recheckCall(t, s, 1, test.sha, "all")
			if w.Code != 409 {
				t.Fatalf("unsafe recheck: %d %s", w.Code, w.Body.String())
			}
			if err := pool.QueryRow(ctx, q).Scan(&after); err != nil || after != before {
				t.Fatalf("refusal mutated items: %v", err)
			}
		})
	}
}

func TestNASCleanupRecheckRequiresServiceAndValidRequest(t *testing.T) {
	s := &Server{}
	s.cfg.ServiceToken = "fixture-service"
	path := "/api/v1/service/nas-cleanup/plans/1/recheck"
	if pullPathAllowed(http.MethodPost, path) {
		t.Fatal("pull key can recheck")
	}
	for _, token := range []string{"", "pull-key", "fixture-service"} {
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.router().ServeHTTP(w, r)
		want := 401
		if token == "fixture-service" {
			want = 400
		}
		if w.Code != want {
			t.Fatalf("token class service=%t: %d %s", token == "fixture-service", w.Code, w.Body.String())
		}
	}
	for _, body := range []map[string]any{
		{"expected_plan_sha256": strings.Repeat("b", 64), "reason": " ", "scope": "all"},
		{"expected_plan_sha256": strings.Repeat("b", 64), "reason": strings.Repeat("x", 501)},
		{"expected_plan_sha256": strings.Repeat("g", 64), "reason": "retry"},
		{"expected_plan_sha256": strings.Repeat("b", 64), "reason": "retry", "scope": "enabled"},
	} {
		w := cleanupCall(t, s.handleAdminNASCleanupRecheck, 47, 5, "id", "1", body)
		if w.Code != 400 {
			t.Fatalf("invalid request %v: %d", body, w.Code)
		}
	}
}

func TestNASCleanupRecheckLeasesNewAttemptAndRefreshesEnableEvidence(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, ids := f.admit("recheck", syntheticFiles(100, 2, 10))
	old := f.leaseV2(64, 0, 0, "")
	f.observe(old, nil)
	f.s.cfg.NASCleanupDeletionEnabled = true
	// One transient recovery failure and one otherwise good, expired proof.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET recovery='r2_present_unverified' WHERE id=$1;`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET updated_at=now()-interval '2 days' WHERE id=$1`, ids[1]); err != nil {
		t.Fatal(err)
	}
	enable := func(want int) {
		t.Helper()
		code, body := safeguardControl(t, f.s, id, "enabled", map[string]any{"plan_sha256": sha})
		if code != want {
			t.Fatalf("enable %d want %d: %s", code, want, body)
		}
	}
	enable(409)
	if w := recheckCall(t, f.s, id, sha, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	enable(409) // Pending observations cannot enable, even though updated_at is fresh.
	next := f.leaseV2(64, 0, 0, "")
	if len(next) != 1 || next[0].ItemID != ids[0] || next[0].Attempt <= old[0].Attempt || len(next[0].ApprovedProof) != 0 {
		t.Fatalf("bad recheck lease: %+v", next)
	}
	if r := f.observe(old[:1], nil)[0]; r.Status != 409 || r.Delete {
		t.Fatalf("stale observation accepted: %+v", r)
	}
	gets, _ := f.bucket.counts()
	if r := f.observe(next, nil)[0]; r.Status != 200 || r.Action != "would_delete" || r.Delete || r.Recovery != "r2_sha_verified" {
		t.Fatalf("re-observation: %+v", r)
	}
	if after, _ := f.bucket.counts(); after <= gets {
		t.Fatal("reset did not perform full recovery verification")
	}
	enable(409) // Other item's proof is still older than 24h.
	if w := recheckCall(t, f.s, id, sha, "all"); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	enable(409)
	for _, r := range f.observe(f.leaseV2(64, 0, 0, ""), nil) {
		if r.Status != 200 || r.Action != "would_delete" || r.Delete {
			t.Fatalf("all: %+v", r)
		}
	}
	enable(200)
}

func TestNASCleanupRecheckGrantsThreeFreshLeaseAttempts(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, ids := f.admit("retries", syntheticFiles(100, 1, 1))
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='unverified',attempt=9 WHERE id=$1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	if w := recheckCall(t, f.s, id, sha, ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	for attempt := 10; attempt <= 12; attempt++ {
		tasks := f.leaseV2(1, 0, 0, strings.Repeat(fmt.Sprint(attempt-9), 32))
		if len(tasks) != 1 || tasks[0].Attempt != attempt {
			t.Fatalf("attempt %d: %+v", attempt, tasks)
		}
		if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET lease_expires_at=now()-interval '1 second' WHERE id=$1`, ids[0]); err != nil {
			t.Fatal(err)
		}
	}
	if tasks := f.leaseV2(1, 0, 0, ""); len(tasks) != 0 {
		t.Fatalf("fourth lease: %+v", tasks)
	}
	var state string
	if err := f.pool.QueryRow(f.ctx, `SELECT state FROM nas_cleanup_items WHERE id=$1`, ids[0]).Scan(&state); err != nil || state != "unverified" {
		t.Fatalf("retry cap: %s %v", state, err)
	}
}

func TestNASCleanupRecheckWaitsForObservationFence(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, ids := f.admit("fence", syntheticFiles(100, 1, 1))
	f.observe(f.leaseV2(1, 0, 0, ""), nil)
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = tx.Exec(f.ctx, cleanupObservationLoadSQL, ids[0], 13); err != nil {
		t.Fatal(err)
	}
	// Context expiry proves the handler cannot overtake the shared plan lock.
	w := cleanupCall(t, func(w http.ResponseWriter, routed *http.Request) {
		bounded, cancel := context.WithTimeout(routed.Context(), 150*time.Millisecond)
		defer cancel()
		f.s.handleAdminNASCleanupRecheck(w, routed.WithContext(bounded))
	}, 47, 5, "id", fmt.Sprint(id), map[string]any{"expected_plan_sha256": sha, "reason": "fence", "scope": "all"})
	if w.Code != 409 {
		t.Fatalf("recheck overtook observation: %d %s", w.Code, w.Body.String())
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if w = recheckCall(t, f.s, id, sha, "all"); w.Code != 200 {
		t.Fatalf("recheck after fence: %d %s", w.Code, w.Body.String())
	}
}
