package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func closeCall(t *testing.T, s *Server, id int64, sha string) *httptest.ResponseRecorder {
	t.Helper()
	return cleanupCall(t, s.handleAdminNASCleanupClose, 47, 5, "id", fmt.Sprint(id), map[string]any{"expected_plan_sha256": sha, "reason": "split never-issued remainder"})
}

func closeOverrideCall(t *testing.T, s *Server, id int64, sha string) *httptest.ResponseRecorder {
	t.Helper()
	return cleanupCall(t, s.handleAdminNASCleanupClose, 47, 5, "id", fmt.Sprint(id), map[string]any{"expected_plan_sha256": sha, "reason": "split never-issued remainder", "operator_override": true})
}

func TestNASCleanupCloseRetainsRecoveryAndReplansOnlyNeverIssued(t *testing.T) {
	f := newCleanupFixture(t, 1)
	id, sha, _ := f.admit("close-original", syntheticFiles(100, 4, 4))
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused',approved_until=now()-interval '1 hour' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state=CASE WHEN ordinal=1 THEN 'deleted' WHEN ordinal=2 THEN 'unverified' WHEN ordinal=3 THEN 'leased' ELSE 'pending' END,delete_ever_issued=ordinal<=2,attempt=4,lease_expires_at=now()-interval '1 second',proof='{}',outcome='{"error":"deadline"}' WHERE plan_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	var before, after string
	snapshot := `SELECT jsonb_agg(to_jsonb(i) ORDER BY ordinal)::text FROM nas_cleanup_items i WHERE plan_id=$1`
	if err := f.pool.QueryRow(f.ctx, snapshot, id).Scan(&before); err != nil {
		t.Fatal(err)
	}
	w := closeOverrideCall(t, f.s, id, sha)
	if w.Code != 200 {
		t.Fatalf("close: %d %s", w.Code, w.Body.String())
	}
	if mode, reason := f.planMode(id); mode != "retired" || reason != "operator override: split never-issued remainder" {
		t.Fatalf("mode/reason: %s %s", mode, reason)
	}
	if err := f.pool.QueryRow(f.ctx, snapshot, id).Scan(&after); err != nil || before != after {
		t.Fatalf("item evidence changed: %v", err)
	}
	// Lost-response retry preserves the original reason, approval and evidence.
	if w2 := closeOverrideCall(t, f.s, id, sha); w2.Code != 200 || w2.Body.String() != w.Body.String() {
		t.Fatalf("non-idempotent close: %s", w2.Body.String())
	}
	for _, clip := range []int64{100, 101, 102, 103} {
		var held bool
		if err := f.pool.QueryRow(f.ctx, `SELECT nas_cleanup_clip_held($1)`, clip).Scan(&held); err != nil || !held {
			t.Fatalf("recovery hold lost for %d: %v", clip, err)
		}
		if _, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=$1`, clip); err == nil {
			t.Fatalf("purge allowed for %d", clip)
		}
	}
	// Master ON proves terminality is the gate, rather than configuration.
	f.s.cfg.NASCleanupDeletionEnabled = true
	for _, mode := range []string{"enabled", "dry_run", "paused"} {
		w := cleanupCall(t, f.s.handleAdminNASCleanupControl, 47, 5, "id", fmt.Sprint(id), map[string]any{"plan_sha256": sha, "reason": "must refuse", "mode": mode})
		if w.Code != 409 {
			t.Fatalf("terminal plan accepted %s: %d %s", mode, w.Code, w.Body.String())
		}
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='dry_run' WHERE id=$1`, id); err == nil {
		t.Fatal("DB allowed reopening retired plan")
	}
	var raw []byte
	var items []nasCleanupItem
	if err := f.pool.QueryRow(f.ctx, `SELECT manifest FROM nas_cleanup_plans WHERE id=$1`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatal(err)
	}
	// Same-label retry returns the immutable closed plan, never reopens it.
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	retryID, _, err := persistNASCleanupPlan(f.ctx, tx, "close-original", items)
	if err != nil || retryID != id {
		t.Fatalf("label retry: %d %v", retryID, err)
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	for n, item := range items {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		newID, _, err := persistNASCleanupPlan(f.ctx, tx, fmt.Sprintf("new-%d", n), []nasCleanupItem{item})
		if n >= 2 {
			if err != nil {
				t.Fatalf("never-issued source rejected: %v", err)
			}
			if err = tx.Commit(f.ctx); err != nil {
				t.Fatal(err)
			}
			if mode, _ := f.planMode(newID); mode != "dry_run" {
				t.Fatal("new plan armed")
			}
			var state string
			if err = f.pool.QueryRow(f.ctx, `SELECT state FROM nas_cleanup_items WHERE plan_id=$1`, newID).Scan(&state); err != nil || state != "pending" {
				t.Fatalf("fresh dry run not required: %s %v", state, err)
			}
		} else {
			if err == nil {
				t.Fatalf("issued source admitted: %d", n)
			}
			tx.Rollback(f.ctx)
		}
	}
	if len(f.leaseV2(64, 64, 1e9, "")) != 2 {
		t.Fatal("closed plan leaked lease work")
	}
}

func TestNASCleanupCloseRefusals(t *testing.T) {
	cases := []struct {
		name, mode, setup, sha string
		want                   int
	}{
		{"dry run", "dry_run", "", "", 409},
		{"enabled", "enabled", "", "", 409},
		{"wrong digest", "paused", "", strings.Repeat("c", 64), 409},
		{"bad digest", "paused", "", "bad", 400},
		{"live lease", "paused", `UPDATE nas_cleanup_items SET state='leased',lease_expires_at=now()+interval '1 hour' WHERE id=1000`, "", 409},
		{"expired permit unresolved", "paused", `UPDATE nas_cleanup_items SET state='delete_issued',delete_ever_issued=true,permit_expires_at=now()-interval '1 hour' WHERE id=1000`, "", 409},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := cleanupTestPool(t)
			ctx := context.Background()
			s := &Server{pool: pool}
			safeguardPlan(t, ctx, s, 1, tc.mode, 3)
			if tc.setup != "" {
				if _, err := pool.Exec(ctx, tc.setup); err != nil {
					t.Fatal(err)
				}
			}
			sha := tc.sha
			if sha == "" {
				sha = strings.Repeat("b", 64)
			}
			w := closeCall(t, s, 1, sha)
			if w.Code != tc.want {
				t.Fatalf("got %d want %d: %s", w.Code, tc.want, w.Body.String())
			}
			if mode, _ := (&cleanupFixture{t: t, ctx: ctx, pool: pool}).planMode(1); mode != tc.mode {
				t.Fatal("refusal changed mode")
			}
		})
	}
}

func TestNASCleanupCloseDrainsObservationFence(t *testing.T) {
	f := newCleanupFixture(t, 1)
	id, sha, ids := f.admit("close-fence", syntheticFiles(100, 1, 1))
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = tx.Exec(f.ctx, cleanupObservationLoadSQL, ids[0], 13); err != nil {
		t.Fatal(err)
	}
	w := cleanupCall(t, func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 150*time.Millisecond)
		defer cancel()
		f.s.handleAdminNASCleanupClose(w, r.WithContext(ctx))
	}, 47, 5, "id", fmt.Sprint(id), map[string]any{"expected_plan_sha256": sha, "reason": "fence"})
	if w.Code != 409 {
		t.Fatalf("close overtook observation: %d %s", w.Code, w.Body.String())
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if w = closeCall(t, f.s, id, sha); w.Code != 200 {
		t.Fatalf("close after fence: %d %s", w.Code, w.Body.String())
	}
}

func TestNASCleanupReservationLegacyAndIncompleteEvidence(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	// Legacy rows do not have item copies. The authoritative ordinal manifest
	// still excludes issued raw/collated identities, even under an old policy.
	if _, err := pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES
 (1,47,13,'legacy',repeat('a',64),'[{"kind":"raw","source_id":1},{"kind":"raw","source_id":2},{"kind":"collated","source_id":3},{"kind":"collated","source_id":4},{"kind":"raw","source_id":5}]',repeat('b',64),'retired'),
 (2,47,13,'other-policy-paused',repeat('a',64),'[{"kind":"raw","source_id":6}]',repeat('b',64),'paused');
 INSERT INTO nas_cleanup_items(plan_id,ordinal,state,delete_ever_issued) VALUES
 (1,1,'unverified',true),(1,2,'pending',false),(1,3,'deleted',true),(1,4,'pending',false),(2,1,'pending',false); SELECT setval('nas_cleanup_plans_id_seq',2)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		kind     string
		id       int64
		reserved bool
	}{
		{"raw", 1, true}, {"raw", 2, false}, {"collated", 3, true}, {"collated", 4, false}, {"raw", 5, true}, {"raw", 6, true},
	} {
		var reserved bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_reserved_files WHERE kind=$1 AND source_id=$2)`, tc.kind, tc.id).Scan(&reserved); err != nil || reserved != tc.reserved {
			t.Fatalf("reservation %s/%d: %v %v", tc.kind, tc.id, reserved, err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = persistNASCleanupPlan(ctx, tx, fmt.Sprintf("new-%s-%d", tc.kind, tc.id), []nasCleanupItem{{Kind: tc.kind, SourceID: tc.id}})
		tx.Rollback(ctx)
		if (err != nil) != tc.reserved {
			t.Fatalf("admission %s/%d: %v", tc.kind, tc.id, err)
		}
	}
}

func TestNASCleanupCloseServiceAuthAndInput(t *testing.T) {
	s := &Server{}
	s.cfg.ServiceToken = "service"
	for _, token := range []string{"", "pull", "service"} {
		r := httptest.NewRequest("POST", "/api/v1/service/nas-cleanup/plans/1/close", strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.router().ServeHTTP(w, r)
		want := 401
		if token == "service" {
			want = 400
		}
		if w.Code != want {
			t.Fatalf("auth token class=%t: %d %s", token == "service", w.Code, w.Body.String())
		}
	}
	if pullPathAllowed("POST", "/api/v1/service/nas-cleanup/plans/1/close") {
		t.Fatal("pull credential allowed close")
	}
	for _, reason := range []string{"", "  ", strings.Repeat("x", 501)} {
		w := cleanupCall(t, s.handleAdminNASCleanupClose, 47, 5, "id", "1", map[string]any{"expected_plan_sha256": strings.Repeat("b", 64), "reason": reason})
		if w.Code != 400 {
			t.Fatalf("invalid reason accepted: %d", w.Code)
		}
	}
	pool := cleanupTestPool(t)
	s.pool = pool
	if _, err := pool.Exec(context.Background(), `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES(1,48,14,'foreign',repeat('a',64),'[]',repeat('b',64),'paused')`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 999} {
		if w := closeCall(t, s, id, strings.Repeat("b", 64)); w.Code != 409 {
			t.Fatalf("foreign/missing plan accepted: %d", w.Code)
		}
	}
}

// Execute the actual Python producer's SQL, rather than a copy of its filter.
func TestNASCleanupCloseKeep28Producer(t *testing.T) {
	f := newCleanupFixture(t, 1)
	id, sha, _ := f.admit("producer-original", syntheticFiles(100, 3, 3))
	if _, err := f.pool.Exec(f.ctx, `CREATE TABLE nas_inventory_files(connection_id bigint,clip_id bigint,state text,relative_path text,sha256 text,size_bytes bigint);
 CREATE TABLE recording_collation_output_acks(output_id bigint,connection_id bigint,sha256 text,size_bytes bigint);
 INSERT INTO nas_inventory_files SELECT 13,id,'present',display_path,lower(sha256),size_bytes FROM recording_clips WHERE id BETWEEN 100 AND 102`); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("python3", "-c", `import sys
sys.path.insert(0,'../../scripts/nas-cleanup')
import prepare_keep28_plans as k
k.query=lambda sql, psql: print(sql)
k.stream_candidates(k.load_policy(k.POLICY_PATH)[3], 'bucket', 'unused')`)
	sql, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	list := func() []map[string]any {
		t.Helper()
		var raw []byte
		if err := f.pool.QueryRow(f.ctx, string(sql)).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var result []map[string]any
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	if rows := list(); len(rows) != 0 {
		t.Fatalf("active plan re-included: %v", rows)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='unverified',delete_ever_issued=true WHERE plan_id=$1 AND ordinal=1`, id); err != nil {
		t.Fatal(err)
	}
	if rows := list(); len(rows) != 0 {
		t.Fatalf("paused plan re-included before close: %v", rows)
	}
	if w := closeCall(t, f.s, id, sha); w.Code != 409 {
		t.Fatalf("uncertain producer plan closed without owner: %d", w.Code)
	}
	if w := closeOverrideCall(t, f.s, id, sha); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	rows := list()
	if len(rows) != 2 || rows[0]["source_id"] != float64(101) || rows[1]["source_id"] != float64(102) {
		t.Fatalf("producer remainder: %v", rows)
	}
}

func TestNASCleanupCloseNoIssuedReleasesHoldAndStopsBatch(t *testing.T) {
	f := newCleanupFixture(t, 1)
	id, sha, _ := f.admit("close-no-issued", syntheticFiles(100, 1, 1))
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_batches(account_id,connection_id,batch_sha256,report_sha256,plan_ids,plan_sha256s,mode,approval_reason,approved_until) VALUES(47,13,repeat('a',64),$1,ARRAY[$2::bigint],ARRAY[$3::text],'enabled','old approval',now()+interval '1 hour')`, cleanupPolicySHA, id, sha); err != nil {
		t.Fatal(err)
	}
	if w := closeCall(t, f.s, id, sha); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if mode, _ := f.batchMode(); mode != "paused" {
		t.Fatal("batch could re-enable closed plan")
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET purged_at=now() WHERE id=100`); err != nil {
		t.Fatalf("no-issued hold retained: %v", err)
	}
}

func TestNASCleanupAdmissionReservationFence(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	first, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback(ctx)
	items := []nasCleanupItem{{Kind: "raw", SourceID: 100}}
	if _, _, err = persistNASCleanupPlan(ctx, first, "first", items); err != nil {
		t.Fatal(err)
	}
	// Admission must not monopolize the public-read fence while waiting on
	// R2/catalog I/O. Public delivery and source references can still proceed.
	public, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer public.Rollback(ctx)
	boundedPublic, cancelPublic := context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = public.Exec(boundedPublic, `SELECT pg_advisory_xact_lock_shared($1)`, nasCleanupPublicFence)
	cancelPublic()
	if err != nil {
		t.Fatalf("admission blocked public reads: %v", err)
	}
	public.Rollback(ctx)
	second, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback(ctx)
	bounded, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	_, _, err = persistNASCleanupPlan(bounded, second, "second", items)
	cancel()
	if err == nil {
		t.Fatal("parallel admission overtook reservation fence")
	}
	second.Rollback(ctx)
	if err = first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	retry, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Rollback(ctx)
	if _, _, err = persistNASCleanupPlan(ctx, retry, "second", items); err == nil || !strings.Contains(err.Error(), "already reserved") {
		t.Fatalf("duplicate admitted after fence: %v", err)
	}
}

func TestNASCleanupCloseUncertaintyRequiresOperatorOverride(t *testing.T) {
	for _, tc := range []struct{ name, state, outcome string }{
		{"unknown result", "unverified", `{}`},
		{"sidecar failure", "media_deleted_sidecar_failed", `{}`},
		{"unresolved permit", "delete_issued", `{}`},
		{"conflict", "conflict", `{}`},
		{"missing", "missing", `{}`},
		{"uncertified keep", "keep", `{}`},
		{"contradicted keep", "keep", `{"certified_refusal":{},"contradictory_receipt":{"outcome":"deleted"}}`},
		{"contradicted deleted", "deleted", `{"contradictory_receipt":{"outcome":"unverified"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCleanupFixture(t, 1)
			id, sha, _ := f.admit("uncertain-close", syntheticFiles(100, 1, 1))
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, id); err != nil {
				t.Fatal(err)
			}
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state=$2,outcome=$3::jsonb,delete_ever_issued=true WHERE plan_id=$1`, id, tc.state, tc.outcome); err != nil {
				t.Fatal(err)
			}
			var before, after string
			const snapshot = `SELECT jsonb_agg(to_jsonb(i))::text FROM nas_cleanup_items i WHERE plan_id=$1`
			if err := f.pool.QueryRow(f.ctx, snapshot, id).Scan(&before); err != nil {
				t.Fatal(err)
			}
			if w := closeCall(t, f.s, id, sha); w.Code != 409 {
				t.Fatalf("close: %d %s", w.Code, w.Body.String())
			}
			if mode, _ := f.planMode(id); mode != "paused" {
				t.Fatalf("refusal changed mode: %s", mode)
			}
			if w := closeOverrideCall(t, f.s, id, sha); w.Code != 200 {
				t.Fatalf("override: %d %s", w.Code, w.Body.String())
			}
			if mode, reason := f.planMode(id); mode != "retired" || !strings.HasPrefix(reason, "operator override: ") {
				t.Fatalf("override not audited: %s %s", mode, reason)
			}
			if err := f.pool.QueryRow(f.ctx, snapshot, id).Scan(&after); err != nil || before != after {
				t.Fatalf("evidence changed: %v", err)
			}
			var held bool
			if err := f.pool.QueryRow(f.ctx, `SELECT nas_cleanup_clip_held(100)`).Scan(&held); err != nil || !held {
				t.Fatalf("issued recovery hold lost: %v", err)
			}
		})
	}
}

func TestNASCleanupCloseOverrideCannotBypassLiveLease(t *testing.T) {
	f := newCleanupFixture(t, 1)
	id, sha, _ := f.admit("live-lease-close", syntheticFiles(100, 1, 1))
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='leased',lease_expires_at=now()+interval '1 hour' WHERE plan_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if w := closeOverrideCall(t, f.s, id, sha); w.Code != 409 {
		t.Fatalf("live lease overridden: %d %s", w.Code, w.Body.String())
	}
}
