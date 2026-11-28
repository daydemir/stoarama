package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
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
		{nasCleanupPermitTransportClientVersion, true, false, false},
		{nasCleanupFreshPermitClientVersion, true, true, false},
		{nasCleanupR2CopyControlClientVersion, true, true, true},
		{NASR2CopyPagedClientVersion, true, true, false},
		{nasCleanupR2CopyProgressClientVersion, true, true, false},
		{nasCleanupR2CopyDiagnosticsClientVersion, true, true, false},
		{nasLowMemoryCopyClientVersion, false, true, false},
		{nasLowMemoryCopyClientVersion + "-extra", false, false, false},
		{nasLowMemoryCleanupClientVersion, true, true, false},
		{nasLowMemoryCleanupClientVersion + "-extra", false, false, false},
		{nasKeep20CleanupClientVersion, true, true, false},
		{nasFastCopyClientVersion, true, true, false},
		{nasFastCopyClientVersion + "-extra", false, false, false},
		{nasTunableCopyClientVersion, true, true, false},
		{nasTunableCopyClientVersion + "-extra", false, false, false},
		{nasTunableDayClientVersion, true, true, false},
		{nasTunableDayClientVersion + "-extra", false, false, false},
		{nasCleanupDayModesClientVersion, true, true, false},
		{nasCleanupDayModesClientVersion + "-extra", false, false, false},
		{nasCleanupDayKEClientVersion, true, true, false},
		{nasCleanupDayKEClientVersion + "-extra", false, false, false},
		{nasOutputRestoreClientVersion, true, true, false},
		{nasOutputRestoreClientVersion + "-extra", false, false, false},
		{nasDayInventoryClientVersion, true, true, false},
		{nasInstalledReceiptClientVersion, false, true, false},
		{nasInstalledReceiptClientVersion + "-extra", false, false, false},
		{nasDayInventoryClientVersion + "-extra", false, false, false},
		{nasKeep20CleanupClientVersion + "-extra", false, false, false},
		{"r2-copy-v5-extra", false, false, false},
		{"r2-copy-v4-extra", false, false, false},
		{nasCleanupItemTruthClientVersion, true, true, true},
		{"development", false, false, false},
		{"", false, false, false},
		{"test", false, false, false},
		{"01234567", false, false, false},
		{"r2-copy-v3-extra", false, false, false},
		{nasCleanupFreshPermitClientVersion + "-extra", false, false, false},
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

func TestNASClientAdmissionFreshPermitCopyAuthenticated(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*cleanupFixture) string
		want   int
	}{
		{"matching release", func(f *cleanupFixture) string { return f.version }, http.StatusOK},
		{"copy disabled", func(f *cleanupFixture) string {
			f.s.cfg.NASR2CopyEnabled = false
			return f.version
		}, http.StatusConflict},
		{"request mismatch", func(f *cleanupFixture) string { return "development" }, http.StatusConflict},
		{"heartbeat mismatch", func(f *cleanupFixture) string {
			if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version='development' WHERE id=13`); err != nil {
				f.t.Fatal(err)
			}
			return f.version
		}, http.StatusForbidden},
		{"maintenance", func(f *cleanupFixture) string {
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_maintenance SET active=true`); err != nil {
				f.t.Fatal(err)
			}
			return f.version
		}, http.StatusConflict},
		{"source changed", func(f *cleanupFixture) string {
			if _, err := f.pool.Exec(f.ctx, `UPDATE recording_clips SET sha256=repeat('b',64) WHERE id=901`); err != nil {
				f.t.Fatal(err)
			}
			return f.version
		}, http.StatusOK},
		{"queue policy changed", func(f *cleanupFixture) string {
			if _, err := f.pool.Exec(f.ctx, `UPDATE nas_r2_copy_items SET policy_sha256=repeat('b',64)`); err != nil {
				f.t.Fatal(err)
			}
			return f.version
		}, http.StatusConflict},
		{"source held", func(f *cleanupFixture) string {
			id, _, _ := f.admit("copy-source-held", syntheticFiles(9901, 1, 1))
			if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_source_holds(plan_id,clip_id) VALUES($1,901)`, id); err != nil {
				f.t.Fatal(err)
			}
			return f.version
		}, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, server := authenticatedCopyFixture(t)
			f.useClientVersion(nasCleanupFreshPermitClientVersion)
			version := tc.mutate(f)
			req, err := http.NewRequest(http.MethodPost, server.URL+copyControlLeasePath, strings.NewReader(`{"free_bytes":3000000000000}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer fixture-key")
			req.Header.Set("User-Agent", "stoarama-nas-pull/"+version)
			response, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != tc.want {
				t.Fatalf("status=%d want=%d", response.StatusCode, tc.want)
			}
			var attempts, leased int
			if err := f.pool.QueryRow(f.ctx, `SELECT coalesce(sum(attempts),0),count(*) FILTER(WHERE state='leased') FROM nas_r2_copy_items`).Scan(&attempts, &leased); err != nil {
				t.Fatal(err)
			}
			want := 0
			if tc.name == "matching release" {
				want = 1
				var body struct {
					Task nasR2CopyTask `json:"task"`
				}
				if err := json.NewDecoder(response.Body).Decode(&body); err != nil || body.Task.ID <= 0 || body.Task.Item.SourceID != 901 || body.Task.ETag == "" {
					t.Fatalf("matching release did not receive a source-bound copy task: %v", err)
				}
			}
			if attempts != want || leased != want {
				t.Fatalf("unexpected queue mutation: attempts=%d leased=%d want=%d", attempts, leased, want)
			}
		})
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

func TestNASClientAdmissionLegacyProofRetiredAndPreservesOFF(t *testing.T) {
	f := newCleanupFixture(t, 1)
	f.s.cfg.NASStitchProofTaskID = 1
	for _, agents := range [][]string{{"stoarama-nas-pull/development"}, nil, {"stoarama-nas-pull/unknown"}} {
		out := admissionCall(t, f, f.s.handleAccountNASStitchProof, http.MethodGet, "/api/v1/account/connections/stitch/proof", 0, nil, agents, "")
		if out.Code != http.StatusConflict || !strings.Contains(out.Body.String(), "legacy native proof admission retired") {
			t.Fatalf("legacy proof route admitted work: %d %s", out.Code, out.Body)
		}
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version='development' WHERE id=13`); err != nil {
		t.Fatal(err)
	}
	out := admissionCall(t, f, f.s.handleAccountNASStitchProof, http.MethodGet, "/api/v1/account/connections/stitch/proof", 0, nil, []string{"stoarama-nas-pull/" + nasCleanupProofClientVersion}, "")
	if out.Code != http.StatusConflict || !strings.Contains(out.Body.String(), "legacy native proof admission retired") {
		t.Fatalf("legacy proof retirement gained a heartbeat dependency: %d %s", out.Code, out.Body)
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

// Persist the old handler's accepted payload through the unchanged transaction
// primitive, so replay tests cover an actual pre-fix issued outcome without
// disabling any authority or immutability guard.
func seedLegacyBatchPermit(t *testing.T, f *cleanupFixture, task cleanupTask, observation cleanupObserve) {
	t.Helper()
	fresh, err := f.s.cleanupRegisterObservation(f.ctx, observation.RequestID, task.ItemID)
	if err != nil || !fresh {
		t.Fatalf("legacy history fixture: %t %v", fresh, err)
	}
	prefetched := f.s.cleanupPrefetchRecovery(f.ctx, 13, "", map[int64]string{task.ItemID: ""})
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	result := f.s.cleanupObserveOne(f.ctx, tx, 13, task.ItemID, observation, &cleanupObserveEnv{
		session:       cleanupClientSession{boot: "fixture-boot", version: f.version},
		freshRequests: map[int64]bool{task.ItemID: true}, prefetched: prefetched,
	})
	if result.status != 200 || !result.write || result.body["delete"] != true {
		t.Fatalf("legacy issued fixture: %+v", result)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNASClientAdmissionBatchChildVersion(t *testing.T) {
	for _, mode := range []string{"fresh", "issued-replay", "mixed"} {
		for _, version := range []string{"development", "", nasCleanupProofClientVersion} {
			t.Run(mode+"/"+version, func(t *testing.T) {
				f := newCleanupFixture(t, 1)
				count := 1
				if mode == "mixed" {
					count = 2
				}
				id, sha, _ := f.admit("nested-admission", syntheticFiles(920, count, 1))
				if mode != "mixed" {
					f.dryRun()
					f.s.cfg.NASCleanupDeletionEnabled = true
					if code, body := f.enableBatch([]int64{id}, []string{sha}, 1); code != http.StatusOK {
						t.Fatalf("enable: %d %s", code, body)
					}
				}
				tasks := f.leaseV2(count, count, 1<<30, "")
				if len(tasks) != count {
					t.Fatalf("lease count: %d", len(tasks))
				}
				observations := make([]cleanupBatchObservation, len(tasks))
				for n, task := range tasks {
					observations[n] = cleanupBatchObservation{ItemID: task.ItemID, cleanupObserve: cleanupObserve{
						Attempt: task.Attempt, PlanSHA: sha, ReconciliationProtocol: nasCleanupReconciliationProtocol,
						Outcome: "match", Proof: fixtureProof(task.Item), ClientVersion: f.version, RequestID: fmt.Sprintf("%032x", n+1),
					}}
				}
				observations[len(observations)-1].ClientVersion = version
				if mode == "issued-replay" {
					seedLegacyBatchPermit(t, f, tasks[0], observations[0].cleanupObserve)
				}
				snapshot := func() string {
					t.Helper()
					var rows string
					if err := f.pool.QueryRow(f.ctx, `SELECT jsonb_agg(to_jsonb(i) ORDER BY id)::text FROM nas_cleanup_items i WHERE plan_id=$1`, id).Scan(&rows); err != nil {
						t.Fatal(err)
					}
					return rows
				}
				before := snapshot()
				var history int
				if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_observation_requests`).Scan(&history); err != nil {
					t.Fatal(err)
				}
				gets, heads := f.bucket.counts()
				out := admissionCall(t, f, f.s.handleAccountNASCleanupObserveBatch, http.MethodPost, "/api/v1/account/connections/cleanup/observe-batch", 0,
					map[string]any{"client_version": f.version, "observations": observations}, []string{"stoarama-nas-pull/" + f.version}, "fixture-boot "+f.version)
				if out.Code != http.StatusConflict || strings.Contains(out.Body.String(), `"delete":true`) {
					t.Fatalf("nested version borrowed outer identity: %d %s", out.Code, out.Body)
				}
				var afterHistory int
				if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_observation_requests`).Scan(&afterHistory); err != nil {
					t.Fatal(err)
				}
				lateGets, lateHeads := f.bucket.counts()
				if snapshot() != before || afterHistory != history || lateGets != gets || lateHeads != heads {
					t.Fatal("inconsistent batch changed an item/history or performed R2 I/O")
				}
			})
		}
	}
}

func TestNASClientAdmissionModernNativeIdentity(t *testing.T) {
	f, server := authenticatedCopyFixture(t)
	const activation = "22222222-2222-4222-8222-222222222222"
	f.s.cfg.NASStitchProofTaskID = 1
	f.s.cfg.NASStitchProofActivationID = activation
	path := "/api/v1/account/connections/stitch/evidence/admission?run_id=11111111-1111-4111-8111-111111111111&activation_id=" + activation
	call := func(path string, agents []string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer fixture-key")
		req.Header["User-Agent"] = agents
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var body struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, body.Error
	}
	for _, tc := range []struct {
		name      string
		agents    []string
		heartbeat string
		status    int
		message   string
	}{
		{"development-before-heartbeat", []string{"stoarama-nas-pull/development"}, nasCleanupR2CopyControlClientVersion, 409, "reviewed NAS client release required"},
		{"missing", nil, nasCleanupR2CopyControlClientVersion, 409, "reviewed NAS client release required"},
		{"unknown", []string{"stoarama-nas-pull/r2-copy-v3-extra"}, nasCleanupR2CopyControlClientVersion, 409, "reviewed NAS client release required"},
		{"development-heartbeat", []string{"stoarama-nas-pull/r2-copy-v3"}, "development", 403, "no connection for this key"},
		{"different-heartbeat", []string{"stoarama-nas-pull/r2-copy-v3"}, nasCleanupProofClientVersion, 403, "no connection for this key"},
		// The fixture intentionally has no proof source tables. Matching identity
		// must reach the existing read-only source gate, never return a task.
		{"matching-reaches-existing-source-gate", []string{"stoarama-nas-pull/r2-copy-v3"}, nasCleanupR2CopyControlClientVersion, 409, "current scoped proof facts unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.useClientVersion(tc.heartbeat)
			if status, message := call(path, tc.agents); status != tc.status || message != tc.message {
				t.Fatalf("native identity admission: %d %q", status, message)
			}
		})
	}
	f.useClientVersion(nasCleanupR2CopyControlClientVersion)
	// Go's HTTP client emits only the first User-Agent. Exercise the parsed
	// multi-valued header directly instead of silently testing one valid value.
	duplicate := admissionCall(t, f, f.s.handleAccountNASStitchEvidenceAdmission, http.MethodGet, path, 0, nil,
		[]string{"stoarama-nas-pull/r2-copy-v3", "stoarama-nas-pull/development"}, "")
	if duplicate.Code != http.StatusConflict || !strings.Contains(duplicate.Body.String(), "reviewed NAS client release required") {
		t.Fatalf("ambiguous native request admitted: %d %s", duplicate.Code, duplicate.Body)
	}
	// OFF and readback-only requests retain metadata authorization and their
	// existing mode/source gates; neither can start native work.
	for _, tc := range []struct {
		mode          int
		path, message string
	}{
		{0, "/api/v1/account/connections/stitch/evidence/admission", "isolated evidence mode inactive or request differs"},
		{2, "/api/v1/account/connections/stitch/evidence/admission?producer_profile_sha256=" + strings.Repeat("a", 64), "current scoped proof facts unavailable"},
	} {
		f.s.cfg.NASStitchProofTaskID = tc.mode
		if status, message := call(tc.path, []string{"stoarama-nas-pull/development"}); status != 409 || message != tc.message {
			t.Fatalf("metadata mode%d changed: %d %q", tc.mode, status, message)
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
	gets, heads := f.bucket.counts()
	if !authenticated || attempts != 0 || nonpending != 0 || gets != 0 || heads != 0 {
		t.Fatalf("proof admission affected work: auth=%t attempts=%d states=%d get=%d head=%d", authenticated, attempts, nonpending, gets, heads)
	}
}

func TestInstalledReceiptVersionReadOnlyAdmission(t *testing.T) {
	v := nasInstalledReceiptClientVersion
	if nasCleanupSupportsExecution(v) || nasCleanupSupportsGroupedExecution(v) || nasClientSupportsStitchProof(v) || slices.Contains(nasCleanupDayVersions, v) || slices.Contains(nasCleanupDayModeVersions, v) || slices.Contains(nasCleanupDayKEVersions, v) {
		t.Fatal("receipt release gained cleanup or stitch capability")
	}
	if !nasFastCopyLine(v) || !nasLowMemoryRawCopyOnly(v) || !nasCopySupportsVersion(v) || !SupportsNASStorageBudget(v) || !slices.Contains(nasDayInventoryVersions, v) {
		t.Fatal("receipt release lost existing read-only/copy eligibility")
	}
}
