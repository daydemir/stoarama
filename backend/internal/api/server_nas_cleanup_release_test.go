package api

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func releaseCall(t *testing.T, s *Server, id int64, sha string) *httptest.ResponseRecorder {
	return cleanupCall(t, s.handleAdminNASCleanupReleaseUnexecuted, 47, 5, "id", fmt.Sprint(id), map[string]any{"expected_plan_sha256": sha, "reason": "audited no-unlink checkpoint ordering"})
}
func TestNASCleanupReleaseUnexecutedPreservesEvidenceAndRefusesAmbiguity(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, ids := f.admit("release", syntheticFiles(100, 7, 7))
	no := false
	yes := true
	receipt := cleanupReceipt{Attempt: 2, PlanSHA: sha, ProofSHA: strings.Repeat("a", 64), Outcome: "unverified", Error: "recovered observation permit was not executed", UnlinkAttempted: &no, RefusalReason: "observation_replayed"}
	originals := map[int64][]byte{}
	for n, itemID := range ids {
		r := receipt
		switch n {
		case 1:
			r.UnlinkAttempted = nil
			r.RefusalReason = "" // pre-#445: even the exact old error cannot certify no unlink
		case 2:
			r.UnlinkAttempted = &yes
			r.RefusalReason = ""
		case 3:
			r.Error = "client interrupted after permit; operator reconciliation required"
			r.RefusalReason = ""
		case 4:
			r.RefusalReason = "deadline_exceeded"
			r.Error = "cleanup observation deadline exceeded (30 s); not unlinked"
		case 5:
			r.RefusalReason = "pre_unlink_failure"
			r.Error = "unknown error"
		case 6:
			r.Attempt = 1 // mismatched receipt identity
		}
		raw, _ := json.Marshal(r)
		originals[itemID] = raw
		if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='unverified',attempt=2,delete_ever_issued=true,proof_sha256=$2,outcome=$3::jsonb,permit_expires_at=now()-interval '1 minute' WHERE id=$1`, itemID, receipt.ProofSHA, string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	// Close is a prerequisite; a paused/original plan never loses reservations.
	if w := releaseCall(t, f.s, id, sha); w.Code != 409 {
		t.Fatalf("active plan released: %d", w.Code)
	}
	f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, id)
	if w := closeCall(t, f.s, id, sha); w.Code != 200 {
		t.Fatalf("close: %d %s", w.Code, w.Body.String())
	}
	if w := releaseCall(t, f.s, id, strings.Repeat("f", 64)); w.Code != 409 {
		t.Fatal("wrong digest released")
	}
	for attempt := 0; attempt < 2; attempt++ {
		w := releaseCall(t, f.s, id, sha)
		var response struct {
			Released  []int64 `json:"released_item_ids"`
			Protected []int64 `json:"protected_item_ids"`
		}
		json.Unmarshal(w.Body.Bytes(), &response)
		if w.Code != 200 || len(response.Released) != 2 || response.Released[0] != ids[0] || response.Released[1] != ids[4] || len(response.Protected) != 5 {
			t.Fatalf("release: %d %s", w.Code, w.Body.String())
		}
	}
	for n, itemID := range ids {
		var held, issued bool
		var state string
		var raw []byte
		f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_reserved_files WHERE source_id=$1)`, 100+n).Scan(&held)
		f.pool.QueryRow(f.ctx, `SELECT state,delete_ever_issued,outcome FROM nas_cleanup_items WHERE id=$1`, itemID).Scan(&state, &issued, &raw)
		if held != (n != 0 && n != 4) || !issued || state != "unverified" || !nasSpaceAuditJSONEqual(raw, originals[itemID]) {
			t.Fatalf("evidence/reservation changed item=%d held=%t issued=%t state=%s", itemID, held, issued, state)
		}
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = persistNASCleanupPlan(f.ctx, tx, fmt.Sprintf("new-%d", n), []nasCleanupItem{{Kind: "raw", SourceID: int64(100 + n)}})
		tx.Rollback(f.ctx)
		if (err != nil) != held {
			t.Fatalf("admission disagrees with reservation: %v", err)
		}
	}
	var hold bool
	if err := f.pool.QueryRow(f.ctx, `SELECT nas_cleanup_clip_held(100)`).Scan(&hold); err != nil || !hold {
		t.Fatalf("recovery hold released: %v %v", hold, err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_unexecuted_releases SET reason='rewrite'`); err == nil {
		t.Fatal("audit mutable")
	}
	var audits int
	f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_unexecuted_releases`).Scan(&audits)
	if audits != 2 {
		t.Fatalf("audit repeats: %d", audits)
	}
}

func TestNASCleanupIncidentReleaseEvidence(t *testing.T) {
	raw, err := os.ReadFile("testdata/cleanup_incident_receipts.json")
	if err != nil {
		t.Fatal(err)
	}
	var items []struct {
		PlanID   int             `json:"plan_id"`
		ItemID   int             `json:"item_id"`
		Attempt  int             `json:"attempt"`
		PlanSHA  string          `json:"plan_sha256"`
		ProofSHA string          `json:"proof_sha256"`
		Receipt  json.RawMessage `json:"receipt"`
	}
	if err = json.Unmarshal(raw, &items); err != nil {
		t.Fatal(err)
	}
	eligible, protected := 0, 0
	for _, i := range items {
		safe := cleanupNoUnlinkReceipt(i.Receipt, i.Attempt, i.PlanSHA, i.ProofSHA)
		if safe != (i.PlanID == 1391) {
			t.Errorf("unexpected classification plan=%d item=%d safe=%t", i.PlanID, i.ItemID, safe)
		}
		if safe {
			eligible++
		} else {
			protected++
		}
	}
	if eligible != 16 || protected != 12 {
		t.Fatalf("incident totals eligible=%d protected=%d", eligible, protected)
	}
}

func TestNASCleanupReleaseUnexecutedServiceAuth(t *testing.T) {
	s := &Server{}
	s.cfg.ServiceToken = "service"
	for _, token := range []string{"", "pull", "service"} {
		r := httptest.NewRequest("POST", "/api/v1/service/nas-cleanup/plans/1/release-unexecuted", strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.router().ServeHTTP(w, r)
		want := 401
		if token == "service" {
			want = 400
		}
		if w.Code != want {
			t.Fatalf("auth: %d %s", w.Code, w.Body.String())
		}
	}
	if pullPathAllowed("POST", "/api/v1/service/nas-cleanup/plans/1/release-unexecuted") {
		t.Fatal("pull can release reservations")
	}
}

func TestNASCleanupReleaseUnexecutedRefusesLiveAndMissingPermit(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, ids := f.admit("live-permit", syntheticFiles(200, 2, 2))
	no := false
	for n, itemID := range ids {
		receipt, _ := json.Marshal(cleanupReceipt{Attempt: 2, PlanSHA: sha, ProofSHA: strings.Repeat("a", 64), Outcome: "unverified", Error: "recovered observation permit was not executed", UnlinkAttempted: &no, RefusalReason: "observation_replayed"})
		_, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='unverified',attempt=2,delete_ever_issued=true,proof_sha256=repeat('a',64),outcome=$2::jsonb,permit_expires_at=CASE WHEN $3::int=0 THEN NULL ELSE now()+interval '1 minute' END WHERE id=$1`, itemID, string(receipt), n)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if w := closeCall(t, f.s, id, sha); w.Code != 200 {
		t.Fatalf("close: %d %s", w.Code, w.Body.String())
	}
	w := releaseCall(t, f.s, id, sha)
	if w.Code != 409 {
		t.Fatalf("live/missing permit released: %d %s", w.Code, w.Body.String())
	}
	var count int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_unexecuted_releases`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("refusal wrote release: %d %v", count, err)
	}
}
