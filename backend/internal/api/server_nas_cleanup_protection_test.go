package api

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestNASCleanupAllIncidentItemsRemainPermanentlyReserved(t *testing.T) {
	f := newCleanupFixture(t, 1)
	raw, err := os.ReadFile("testdata/cleanup_incident_receipts.json")
	if err != nil {
		t.Fatal(err)
	}
	var evidence []struct {
		PlanID   int64           `json:"plan_id"`
		ItemID   int64           `json:"item_id"`
		Attempt  int             `json:"attempt"`
		PlanSHA  string          `json:"plan_sha256"`
		ProofSHA string          `json:"proof_sha256"`
		Receipt  json.RawMessage `json:"receipt"`
	}
	if err = json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	for planID, expected := range map[int64]int{1390: 12, 1391: 16} {
		var manifest []map[string]any
		var sha string
		for _, item := range evidence {
			if item.PlanID == planID {
				manifest = append(manifest, map[string]any{"kind": "raw", "source_id": item.ItemID})
				sha = item.PlanSHA
			}
		}
		if len(manifest) != expected {
			t.Fatalf("plan %d evidence=%d want=%d", planID, len(manifest), expected)
		}
		body, _ := json.Marshal(manifest)
		if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES($1,47,13,$2,$3,$4::jsonb,$5,'paused')`, planID, fmt.Sprintf("incident-%d", planID), cleanupPolicySHA, string(body), sha); err != nil {
			t.Fatal(err)
		}
		ordinal := 0
		for _, item := range evidence {
			if item.PlanID != planID {
				continue
			}
			ordinal++
			if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,delete_ever_issued,attempt,proof_sha256,outcome,permit_expires_at) VALUES($1,$2,$3,'unverified',true,$4,$5,$6::jsonb,now()-interval '1 hour')`, item.ItemID, planID, ordinal, item.Attempt, item.ProofSHA, string(item.Receipt)); err != nil {
				t.Fatal(err)
			}
		}
		if w := closeCall(t, f.s, planID, sha); w.Code != 200 {
			t.Fatalf("close: %d %s", w.Code, w.Body.String())
		}
		var protected int
		if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_reserved_files WHERE label=$1`, fmt.Sprintf("incident-%d", planID)).Scan(&protected); err != nil || protected != len(manifest) {
			t.Fatalf("plan %d protected=%d want=%d: %v", planID, protected, len(manifest), err)
		}
	}
	if len(evidence) != 28 {
		t.Fatalf("incident evidence count: %d", len(evidence))
	}
	// The old service-token route is gone, even for an authenticated operator.
	f.s.cfg.ServiceToken = "fixture"
	r := httptest.NewRequest("POST", "/api/v1/service/nas-cleanup/plans/1391/release-unexecuted", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer fixture")
	w := httptest.NewRecorder()
	f.s.router().ServeHTTP(w, r)
	if w.Code != 404 {
		t.Fatalf("removed endpoint: %d %s", w.Code, w.Body.String())
	}
}
