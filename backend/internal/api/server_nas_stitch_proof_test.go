package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func proofPlanFixture() nasStitchPlan {
	raw, err := os.ReadFile("testdata/nas_stitch_proof_source.json")
	if err != nil {
		panic(err)
	}
	var p nasStitchPlan
	if err := json.Unmarshal(raw, &p); err != nil {
		panic(err)
	}

	return p
}
func TestNASStitchProofBindsExactPair(t *testing.T) {
	p := proofPlanFixture()
	proof, err := boundedNASStitchProof(p, nasStitchProofSourceSHA)
	if err != nil || len(proof.Plan.Clips) != 2 || !strings.Contains(proof.Plan.OutputFolder, "_V85r_proof_keep28_v2/") || proof.PlanSHA == nasStitchProofSourceSHA {
		t.Fatalf("proof: %+v %v", proof, err)
	}
	for _, change := range []func(*nasStitchPlan){func(p *nasStitchPlan) { p.KeepSHA = "changed" }, func(p *nasStitchPlan) { p.Clips[0].SourceID++ }, func(p *nasStitchPlan) { p.Clips[1].SizeBytes++ }, func(p *nasStitchPlan) { p.Joins[0] = false }, func(p *nasStitchPlan) { p.Frozen[0] = nil }} {
		p := proofPlanFixture()
		change(&p)
		if _, err := boundedNASStitchProof(p, nasStitchProofSourceSHA); err == nil {
			t.Fatal("changed proof admitted")
		}
	}
	if stitchProofSelector(1, 13, nasCleanupProofClientVersion) != 1 || stitchProofSelector(2, 13, nasCleanupProofClientVersion) != 0 || stitchProofSelector(1, 14, nasCleanupProofClientVersion) != 0 {
		t.Fatal("proof selector widened")
	}
}

func TestNASStitchProofHeartbeatPreservesJoinedClientContract(t *testing.T) {
	for version, eligible := range map[string]bool{
		"cleanup-batch-v7": false, "r2-copy-v2": false, "v85r-nas-proof-v1": false,
		"v85r-nas-proof-v2": true, "r2-copy-v3": true, "cleanup-batch-v8": true,
		"development": false, "r2-copy-v3-extra": false, "cleanup-batch-v80": false,
	} {
		for _, selector := range []int{0, 1} {
			response := connectionHeartbeatResponse{OK: true, JoinedDeliveryAccepted: true, JoinedProtocolVersion: 1, JoinedProtocolGeneration: 1, StitchProofTaskID: stitchProofSelector(selector, 13, version)}
			raw, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "stitch_proof_task_id") != (selector == 1 && eligible) {
				t.Fatalf("selector leaked to unqualified client: %s %s", version, raw)
			}
			cmd := exec.Command("python3", "-I", "-S", "-B", "-c", `import importlib.util,json,sys,threading
from types import SimpleNamespace
s=importlib.util.spec_from_file_location('client',sys.argv[1]);p=importlib.util.module_from_spec(s);s.loader.exec_module(p)
r=SimpleNamespace(joined_completion_lock=threading.RLock(),lock=threading.Lock(),joined_protocol_generation=0,joined_protocol_version=0,joined_transfer=None,joined_work_condition=threading.Condition())
p.Runtime.apply_joined_protocol_response(r,json.load(sys.stdin))
assert r.joined_protocol_version==1,'proof selector disabled joined delivery'
`, "../../../clients/nas-pull/stoarama_pull.py")
			cmd.Env = []string{}
			cmd.Stdin = bytes.NewReader(raw)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("serialized heartbeat rejected: %s %v %s", raw, err, output)
			}
		}
	}
}

func TestNASStitchProofCombinedCapabilitySelectors(t *testing.T) {
	for version, eligible := range map[string]bool{
		"v85r-nas-proof-v2": true, "r2-copy-v3": true, "cleanup-batch-v8": true,
		"": false, "development": false, "r2-copy-v2": false, "v85r-nas-proof-v1": false,
		"r2-copy-v3-extra": false, "cleanup-batch-v80": false,
	} {
		for _, connection := range []int64{13, 14} {
			for _, selector := range []int{0, 1, 2} {
				want := 0
				if eligible && connection == 13 && selector == 1 {
					want = 1
				}
				if version == nasCleanupR2CopyControlClientVersion && connection == 13 && selector == 2 {
					want = 2
				}
				if got := stitchProofSelector(selector, connection, version); got != want {
					t.Fatalf("version=%q connection=%d selector=%d got=%d want=%d", version, connection, selector, got, want)
				}
			}
		}
	}
}

func TestNASStitchProofPullMiddlewareAllowsOnlyExactRead(t *testing.T) {
	id := int64(5)
	principal := accountPrincipal{AccountID: 47, AuthType: "api_key", APIKeyID: &id, KeyScopes: []string{accountScopePull}}
	path := "/api/v1/account/connections/stitch/proof"
	if runConfine(principal, http.MethodGet, path) != http.StatusOK {
		t.Fatal("proof GET blocked by pull scope")
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		if runConfine(principal, method, path) != http.StatusForbidden {
			t.Fatal("proof mutation admitted")
		}
	}
	for _, neighbor := range []string{path + "/", path + "/other", path + "x"} {
		if runConfine(principal, http.MethodGet, neighbor) != http.StatusForbidden {
			t.Fatal("neighbor proof route admitted")
		}
	}
}
func TestNASStitchLegacyAdmissionRefusesAcrossUnchangedAdmissionFence(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	f := &cleanupFixture{pool: pool, ctx: ctx}
	stitchMigration(t, f)
	raw, _ := json.Marshal(proofPlanFixture())
	if _, err := pool.Exec(ctx, `INSERT INTO streams VALUES(17208,'Europe/Prague');INSERT INTO recordings VALUES(444,47,17208,'Europe/Prague');INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until)VALUES(1,17208,'2026-08-09T09:00Z','2026-08-09T10:01Z','proof only',now()+interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256,state) VALUES(1,1,444,'2026-08-09T09:00Z',$1,$2,'leased')`, string(raw), nasStitchProofSourceSHA); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../../infra/sql/migrations/0187_r2_writer_admission_fence.sql")
	if err != nil {
		t.Fatal(err)
	}
	text := string(migration)
	prefix := strings.Split(text, "-- SQL admissions")[0]
	admission := strings.Split(strings.Split(text, "CREATE FUNCTION r2_retirement_guard_admission()")[1], "CREATE TRIGGER r2_writer_recordings")[0]
	if _, err = pool.Exec(ctx, prefix+"CREATE FUNCTION r2_retirement_guard_admission()"+admission+`CREATE TRIGGER r2_writer_stitch BEFORE INSERT OR UPDATE ON nas_stitch_tasks FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission('state','pending,leased');CREATE TRIGGER r2_writer_insert BEFORE INSERT ON nas_stitch_outputs FOR EACH ROW EXECUTE FUNCTION r2_retirement_guard_admission();INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256)VALUES('stoarama',repeat('a',64));`); err != nil {
		t.Fatal(err)
	}
	server := &Server{pool: pool}
	server.cfg.NASStitchProofTaskID = 1
	keyID := int64(5)
	principal := accountPrincipal{AccountID: 47, AuthType: "api_key", APIKeyID: &keyID, KeyScopes: []string{accountScopePull}}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/account/connections/stitch/proof", nil)
	req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
	w := httptest.NewRecorder()
	server.confineAccountScope(http.HandlerFunc(server.handleAccountNASStitchProof)).ServeHTTP(w, req)
	if w.Code != 409 {
		t.Fatalf("legacy native admission was not retired: %d %s", w.Code, w.Body)
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_stitch_tasks SET attempt=attempt+1 WHERE id=1`); err == nil {
		t.Fatal("ordinary re-lease escaped fence")
	}
	if _, err = pool.Exec(ctx, `INSERT INTO nas_stitch_outputs(task_id,relative_path,sha256,size_bytes,proof)VALUES(1,'proof',repeat('b',64),1,'{}')`); err == nil {
		t.Fatal("catalogue insert escaped fence")
	}

}

func TestNASStitchProofTransportSurvivesServerRedaction(t *testing.T) {
	line := "V85r proof-v1 " + strings.Repeat("a", 64) + " " + strings.Repeat("b", 64) + " 0/1 " + strings.Repeat("a1", 512)
	if redactNASClientLogLine(line) != line {
		t.Fatal("proof transport changed by server redaction")
	}
	if !nasCleanupSupportsBulk(nasCleanupProofClientVersion) || !nasCleanupSupportsGroupedExecution(nasCleanupProofClientVersion) {
		t.Fatal("proof release regressed reviewed bulk cleanup")
	}
	testNASCleanupReviewedReleaseGates(t, nasCleanupProofClientVersion)
}

func TestNASStitchProofDiagnosticTransportPersistsExactBoundedChunks(t *testing.T) {
	pool := cleanupTestPool(t)
	s := &Server{pool: pool}
	message := "V85r proof-v1 " + strings.Repeat("a", 64) + " " + strings.Repeat("b", 64) + " 0/1 " + strings.Repeat("a1", 512)
	lines := make([]map[string]any, 64)
	for n := range lines {
		lines[n] = map[string]any{"seq": n + 1, "at": "2026-11-15T00:00:00Z", "level": "INFO", "message": message}
	}
	w := cleanupCall(t, s.handleAccountNASClientLogs, 47, 5, "", "", map[string]any{"client_version": nasCleanupProofClientVersion, "boot_id": "proof-boot", "lines": lines})
	if w.Code != 200 {
		t.Fatalf("proof upload failed: %d %s", w.Code, w.Body)
	}
	req := httptest.NewRequest(http.MethodGet, "/x?since=1h&grep=V85r+proof-v1&limit=2048", nil)
	route := chi.NewRouteContext()
	route.URLParams.Add("id", "13")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
	w = httptest.NewRecorder()
	s.handleAdminNASClientLogs(w, req)
	var result struct {
		Lines []struct {
			Message string `json:"message"`
		} `json:"lines"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.Lines) != 64 {
		t.Fatalf("proof transport read failed: %d %s", w.Code, w.Body)
	}
	for _, line := range result.Lines {
		if line.Message != message {
			t.Fatal("proof chunk changed in persisted roundtrip")
		}
	}
}

func TestNASStitchProofValidationRequiresMatchingFreshNASReadback(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	stitchMigration(t, &cleanupFixture{pool: pool, ctx: ctx})
	p := proofPlanFixture()
	raw, _ := json.Marshal(p)
	if _, err := pool.Exec(ctx, `INSERT INTO streams VALUES(17208,'Europe/Prague');INSERT INTO recordings VALUES(444,47,17208,'Europe/Prague');INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until)VALUES(1,17208,'2026-08-09T09:00Z','2026-08-09T10:01Z','proof only',now()+interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256,state)VALUES(1,1,444,'2026-08-09T09:00Z',$1,$2,'leased')`, string(raw), nasStitchProofSourceSHA); err != nil {
		t.Fatal(err)
	}
	proof, _ := boundedNASStitchProof(p, nasStitchProofSourceSHA)
	_, q := stitchUnitFixture()
	q.Token = ""
	q.PlanSHA = proof.PlanSHA
	q.LeftOut = nil
	q.Seams = q.Seams[:1]
	q.Seams[0].LeftID = 373218
	q.Seams[0].RightID = 373262
	q.Seams[0].Frozen = true
	q.Outputs[0].Path = proof.Plan.OutputFolder + "/proof.mp4"
	for n, c := range proof.Plan.Clips {
		// Synthetic physical observation; immutable source plan has no NAS inode.
		c.Frozen = [5]int64{c.SizeBytes, 1, 2, int64(n + 10), 77}
		c.FrozenSidecar = [5]int64{300, 1, 2, int64(n + 100), 77}
		q.Outputs[0].Inputs[n].ID = c.SourceID
		q.Outputs[0].Inputs[n].SHA = c.SHA256
		q.Outputs[0].Inputs[n].Size = c.SizeBytes
		q.Outputs[0].Inputs[n].RawProof = fixtureProof(c)
	}
	if _, err := validateNASStitchResult(proof.Plan, q); err != nil {
		t.Fatal(err)
	}
	request := map[string]any{"result": q, "receipt_sha256": strings.Repeat("1", 64), "run_id": "00000000-0000-4000-8000-000000000001", "client_artifact_sha256": strings.Repeat("2", 64)}
	status := func() map[string]any {
		return map[string]any{"kind": "nas-only-v85r-proof-v1", "state": "proof_registered_locally", "readback_at": time.Now().UTC().Format(time.RFC3339Nano), "readback_boot_id": "fixture-boot", "receipt_sha256": request["receipt_sha256"], "run_id": request["run_id"], "client_artifact_sha256": request["client_artifact_sha256"], "output_sha256": q.Outputs[0].SHA, "output_size_bytes": q.Outputs[0].Size}
	}
	server := &Server{pool: pool}
	for _, change := range []func(map[string]any){nil, func(s map[string]any) { s["state"] = "paused" }, func(s map[string]any) { s["readback_at"] = time.Now().Add(-3 * time.Minute).UTC().Format(time.RFC3339) }, func(s map[string]any) { s["readback_boot_id"] = "old-boot" }, func(s map[string]any) { s["client_artifact_sha256"] = strings.Repeat("3", 64) }, func(s map[string]any) { s["output_size_bytes"] = 1 }, func(s map[string]any) { s["run_id"] = "another-run" }} {
		s := status()
		if change != nil {
			change(s)
		}
		raw, _ := json.Marshal(s)
		if _, err := pool.Exec(ctx, `UPDATE connections SET client_version='v85r-nas-proof-v2',nas_stitch_status=$1,nas_stitch_status_at=now(),last_seen_at=now() WHERE id=13`, string(raw)); err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(request)
		w := httptest.NewRecorder()
		server.handleAdminNASStitchProofVerify(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
		want := http.StatusConflict
		if change == nil {
			want = http.StatusOK
		}
		if w.Code != want {
			t.Fatalf("validation got %d want %d: %s", w.Code, want, w.Body)
		}
	}
	rawStatus, _ := json.Marshal(status())
	for _, version := range []string{"r2-copy-v2", "r2-copy-v3", "cleanup-batch-v8", "development", "v85r-nas-proof-v2-extra"} {
		if _, err := pool.Exec(ctx, `UPDATE connections SET client_version=$2,nas_stitch_status=$1,nas_stitch_status_at=now(),last_seen_at=now() WHERE id=13`, string(rawStatus), version); err != nil {
			t.Fatal(err)
		}
		// Capability is not current-reader identity. Legacy v1 cannot bind a
		// different producer/reader; the new readback contract is separate.
		body, _ := json.Marshal(request)
		w := httptest.NewRecorder()
		server.handleAdminNASStitchProofVerify(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
		if w.Code != http.StatusConflict {
			t.Fatalf("previous client proof accepted after drift to %s", version)
		}
	}

	var state string
	var count int
	if err := pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM nas_stitch_outputs) FROM nas_stitch_tasks WHERE id=1`).Scan(&state, &count); err != nil || state != "leased" || count != 0 {
		t.Fatalf("validation mutated writer state: %s %d %v", state, count, err)
	}
}

func TestNASStitchProofRuntimeOffObserverContract(t *testing.T) {
	for _, selector := range []int{0, 1, 2, -1, 3} {
		server := &Server{} // No DB, environment credential or admission dependency.
		server.cfg.NASStitchProofTaskID = selector
		server.cfg.NASStitchProofActivationID = "invalid-nonce-must-not-block-OFF"
		w := httptest.NewRecorder()
		server.handleAdminNASStitchProof(w, httptest.NewRequest(http.MethodGet, "/synthetic", nil))
		var body map[string]any
		if json.Unmarshal(w.Body.Bytes(), &body) != nil || len(body) != 1 || w.Code != 409 {
			t.Fatal("runtime observer response differs")
		}
		expected := "legacy native proof admission retired; activation protocol required"
		if selector == 0 {
			expected = "isolated proof selector inactive"
		}
		if body["error"] != expected {
			t.Fatal("OFF/retired distinction differs")
		}
		evidence, _ := json.Marshal(map[string]any{"selector": selector, "status": w.Code, "body": string(w.Body.Bytes())})
		t.Logf("OFF_OBSERVER_RESPONSE=%s", evidence)
	}
}
