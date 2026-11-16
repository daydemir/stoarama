package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

func admissionCall(t *testing.T, f *cleanupFixture, handler http.HandlerFunc, method, path string, id int64, body any, agents []string, session string) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(data))
	req.Header["User-Agent"] = agents
	if session != "" {
		req.Header.Set(cleanupClientSessionHeader, session)
	}
	route := chi.NewRouteContext()
	route.URLParams.Add("id", fmt.Sprint(id))
	route.URLParams.Add("itemId", fmt.Sprint(id))
	ctx := context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, AuthType: "api_key", APIKeyID: ptrInt64(5), KeyScopes: []string{accountScopePull}})
	req = req.WithContext(context.WithValue(ctx, chi.RouteCtxKey, route))
	out := httptest.NewRecorder()
	f.s.confineAccountScope(f.s.fenceNASDelivery(handler)).ServeHTTP(out, req)
	return out
}

func TestNASClientAdmissionExactVersions(t *testing.T) {
	for _, tc := range []struct {
		version              string
		cleanup, copy, proof bool
	}{
		{nasCleanupExecutionClientVersion, true, false, false},
		{string(nasCleanupV6R2Copy), true, false, false},
		{nasCleanupBulkDurabilityClientVersion, true, false, false},
		{nasCleanupR2CopyClientVersion, true, true, false},
		{"v85r-nas-proof-v1", true, true, false},
		{nasCleanupProofClientVersion, true, true, true},
		{nasCleanupR2CopyControlClientVersion, true, true, true},
		{nasCleanupItemTruthClientVersion, true, true, true},
		{"development", false, false, false},
		{"", false, false, false},
		{"test", false, false, false},
		{"01234567", false, false, false},
		{"r2-copy-v3-extra", false, false, false},
		{"v85r-nas-proof-v2 ", false, false, false},
	} {
		for purpose, want := range map[nasConnectionPurpose]bool{nasConnectionCleanup: tc.cleanup, nasConnectionCopy: tc.copy, nasConnectionProof: tc.proof} {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.Header.Set("User-Agent", "stoarama-nas-pull/"+tc.version)
			if got, ok := nasRequestWorkVersion(r, purpose); ok != want || (ok && got != tc.version) {
				t.Fatalf("version=%q purpose=%d got=%q,%t want=%t", tc.version, purpose, got, ok, want)
			}
		}
	}
	for _, agents := range [][]string{nil, {""}, {"python-urllib/3.14"}, {"stoarama-nas-pull/r2-copy-v3", "stoarama-nas-pull/development"}} {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header["User-Agent"] = agents
		if _, ok := nasRequestWorkVersion(r, nasConnectionCopy); ok {
			t.Fatalf("ambiguous or unstamped transport admitted: %v", agents)
		}
	}
}

func TestNASClientAdmissionCopyAuthenticatesThenRefusesUnstamped(t *testing.T) {
	f, server := authenticatedCopyFixture(t)
	for _, tc := range []struct{ ua, heartbeat string }{
		{"stoarama-nas-pull/development", nasCleanupR2CopyControlClientVersion},
		{"stoarama-nas-pull/unknown", nasCleanupR2CopyControlClientVersion},
		{"", nasCleanupR2CopyControlClientVersion},
		{"stoarama-nas-pull/" + nasCleanupR2CopyControlClientVersion, "development"},
		{"stoarama-nas-pull/" + nasCleanupR2CopyControlClientVersion, nasCleanupProofClientVersion},
	} {
		if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, tc.heartbeat); err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPost, server.URL+copyControlLeasePath, strings.NewReader(`{"free_bytes":3000000000000}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer fixture-key")
		req.Header.Set("User-Agent", tc.ua)
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusConflict && response.StatusCode != http.StatusForbidden {
			t.Fatalf("unstamped or mismatched caller admitted: %d", response.StatusCode)
		}
	}
	var attempts, nonpending int
	var authenticated bool
	if err := f.pool.QueryRow(f.ctx, `SELECT coalesce(sum(attempts),0),count(*) FILTER(WHERE state<>'pending') FROM nas_r2_copy_items`).Scan(&attempts, &nonpending); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT last_used_at IS NOT NULL FROM account_api_keys WHERE id=5`).Scan(&authenticated); err != nil {
		t.Fatal(err)
	}
	_, heads := f.bucket.counts()
	if !authenticated || attempts != 0 || nonpending != 0 || heads != 0 {
		t.Fatalf("refusal crossed admission: auth=%t attempts=%d states=%d heads=%d", authenticated, attempts, nonpending, heads)
	}
}

func TestNASClientAdmissionCopyRangeRefusesButTerminalTruthSurvives(t *testing.T) {
	f, item := copyTestFixture(t)
	f.s.cfg.NASR2CopyEnabled = true
	task := copyLease(t, f)
	control := nasR2CopyControl{Token: task.Token, FreeBytes: NASR2CopyFloor + 1e9, Length: item.SizeBytes}
	path := fmt.Sprintf("/api/v1/account/connections/r2-copy/%d/range", task.ID)
	if out := admissionCall(t, f, f.s.handleNASR2CopyRange, http.MethodPost, path, task.ID, control, []string{"stoarama-nas-pull/development"}, ""); out.Code != http.StatusConflict {
		t.Fatalf("development range admitted: %d %s", out.Code, out.Body)
	}
	var rangeID *string
	if err := f.pool.QueryRow(f.ctx, `SELECT range_id FROM nas_r2_copy_items WHERE id=$1`, task.ID).Scan(&rangeID); err != nil || rangeID != nil {
		t.Fatalf("denied range mutated authority: %v %v", rangeID, err)
	}
	grant := copyCall(f, "range", task.ID, control)
	if grant.Code != http.StatusOK {
		t.Fatalf("reviewed range failed: %d %s", grant.Code, grant.Body)
	}
	var data struct {
		RangeID string `json:"range_id"`
	}
	if err := json.Unmarshal(grant.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	control.RangeID = data.RangeID
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version='development' WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	path = fmt.Sprintf("/api/v1/account/connections/r2-copy/%d/checkpoint", task.ID)
	if out := admissionCall(t, f, f.s.handleNASR2CopyCheckpoint, http.MethodPost, path, task.ID, control, []string{"stoarama-nas-pull/development"}, ""); out.Code != http.StatusOK {
		t.Fatalf("existing range completion lost: %d %s", out.Code, out.Body)
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_inventory_files VALUES(13,$1,$2,$3,$4,'present',now(),now(),$2||'.stoarama.json',100,repeat('b',64))`, item.SourceID, item.RelativePath, item.SHA256, item.SizeBytes); err != nil {
		t.Fatal(err)
	}
	path = fmt.Sprintf("/api/v1/account/connections/r2-copy/%d/result", task.ID)
	if out := admissionCall(t, f, f.s.handleNASR2CopyResult, http.MethodPost, path, task.ID, control, []string{"stoarama-nas-pull/development"}, ""); out.Code != http.StatusOK {
		t.Fatalf("inventory-bound result lost: %d %s", out.Code, out.Body)
	}
}

func TestNASClientAdmissionCleanupBlocksLeaseAndPermitReplay(t *testing.T) {
	f := newCleanupFixture(t, 1)
	id, sha, _ := f.admit("admission", syntheticFiles(910, 1, 1))
	lease := map[string]any{"client_version": "development", "protocol": 1}
	if out := admissionCall(t, f, f.s.handleAccountNASCleanupLease, http.MethodPost, "/api/v1/account/connections/cleanup/lease", 0, lease, []string{"stoarama-nas-pull/development"}, "fixture-boot development"); out.Code != http.StatusConflict {
		t.Fatalf("development dry lease admitted: %d %s", out.Code, out.Body)
	}
	if out := admissionCall(t, f, f.s.handleAccountNASCleanupLease, http.MethodPost, "/api/v1/account/connections/cleanup/lease", 0, lease, []string{"stoarama-nas-pull/" + f.version}, "fixture-boot "+f.version); out.Code != http.StatusConflict {
		t.Fatalf("unstamped body borrowed reviewed transport: %d %s", out.Code, out.Body)
	}
	var attempts int
	if err := f.pool.QueryRow(f.ctx, `SELECT sum(attempt) FROM nas_cleanup_items WHERE plan_id=$1`, id).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("denied dry lease changed attempts: %d %v", attempts, err)
	}
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 1); code != http.StatusOK {
		t.Fatalf("enable: %d %s", code, body)
	}
	tasks := f.leaseV2(1, 1, 1<<30, "")
	results := f.observe(tasks, nil)
	if len(results) != 1 || !results[0].Delete {
		t.Fatalf("reviewed permit not issued: %+v", results)
	}
	var before []byte
	if err := f.pool.QueryRow(f.ctx, `SELECT to_jsonb(i) FROM nas_cleanup_items i WHERE id=$1`, tasks[0].ItemID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	_, heads := f.bucket.counts()
	observation := map[string]any{"attempt": tasks[0].Attempt, "plan_sha256": sha, "reconciliation_protocol": nasCleanupReconciliationProtocol, "outcome": "match", "proof": fixtureProof(tasks[0].Item), "client_version": f.version}
	path := fmt.Sprintf("/api/v1/account/connections/cleanup/%d/observe", tasks[0].ItemID)
	out := admissionCall(t, f, f.s.handleAccountNASCleanupObserve, http.MethodPost, path, tasks[0].ItemID, observation, []string{"stoarama-nas-pull/development"}, "fixture-boot "+f.version)
	if out.Code != http.StatusConflict {
		t.Fatalf("development replay admitted: %d %s", out.Code, out.Body)
	}
	var after []byte
	if err := f.pool.QueryRow(f.ctx, `SELECT to_jsonb(i) FROM nas_cleanup_items i WHERE id=$1`, tasks[0].ItemID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	_, lateHeads := f.bucket.counts()
	if !bytes.Equal(before, after) || lateHeads != heads {
		t.Fatal("denied permit replay changed item or performed R2 work")
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version='development' WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	receipt := map[string]any{"client_version": "development", "receipts": []map[string]any{{"item_id": tasks[0].ItemID, "attempt": tasks[0].Attempt, "plan_sha256": sha, "proof_sha256": results[0].ProofSHA, "outcome": "deleted", "error": ""}}}
	out = admissionCall(t, f, f.s.handleAccountNASCleanupResultBatch, http.MethodPost, "/api/v1/account/connections/cleanup/result-batch", 0, receipt, []string{"stoarama-nas-pull/development"}, "fixture-boot "+f.version)
	if out.Code != http.StatusOK {
		t.Fatalf("valid terminal evidence discarded: %d %s", out.Code, out.Body)
	}
	var state string
	if err := f.pool.QueryRow(f.ctx, `SELECT state FROM nas_cleanup_items WHERE id=$1`, tasks[0].ItemID).Scan(&state); err != nil || state != "deleted" {
		t.Fatalf("terminal evidence not recorded: %s %v", state, err)
	}
}

func TestNASClientAdmissionProofRefusesUnstampedAndPreservesOFF(t *testing.T) {
	f := newCleanupFixture(t, 1)
	f.s.cfg.NASStitchProofTaskID = 1
	for _, agents := range [][]string{{"stoarama-nas-pull/development"}, nil, {"stoarama-nas-pull/unknown"}} {
		out := admissionCall(t, f, f.s.handleAccountNASStitchProof, http.MethodGet, "/api/v1/account/connections/stitch/proof", 0, nil, agents, "")
		if out.Code != http.StatusConflict {
			t.Fatalf("unreviewed proof request reached source lookup: %d %s", out.Code, out.Body)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version='development' WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	out := admissionCall(t, f, f.s.handleAccountNASStitchProof, http.MethodGet, "/api/v1/account/connections/stitch/proof", 0, nil, []string{"stoarama-nas-pull/" + nasCleanupProofClientVersion}, "")
	if out.Code != http.StatusForbidden {
		t.Fatalf("reviewed request borrowed development heartbeat: %d %s", out.Code, out.Body)
	}
	f.s.cfg.NASStitchProofTaskID = 0
	out = admissionCall(t, f, f.s.handleAccountNASStitchProof, http.MethodGet, "/api/v1/account/connections/stitch/proof", 0, nil, []string{"stoarama-nas-pull/development"}, "")
	if out.Code != http.StatusConflict || !strings.Contains(out.Body.String(), "isolated proof selector inactive") {
		t.Fatalf("account OFF gained a version dependency: %d %s", out.Code, out.Body)
	}
	server := &Server{}
	for _, ua := range []string{"", "stoarama-operator/1", "stoarama-nas-pull/development"} {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/service/nas-stitch/proof", nil)
		r.Header.Set("User-Agent", ua)
		out := httptest.NewRecorder()
		server.handleAdminNASStitchProof(out, r)
		if out.Code != http.StatusConflict || !strings.Contains(out.Body.String(), "isolated proof selector inactive") {
			t.Fatalf("OFF gained UA or DB dependency: %d %s", out.Code, out.Body)
		}
	}
}
