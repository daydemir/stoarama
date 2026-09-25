package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestValidateNASHostFacts(t *testing.T) {
	limit, load, exec := 2.5, 0.4, true
	valid := &nasHostFacts{Machine: "x86_64", System: "Linux", KernelRelease: "4.4.302+", PythonVersion: "3.13.7",
		CPUModel: "AMD Ryzen Embedded V1500B", CPUCount: 8, AffinityCPUs: 8, CgroupCPULimit: &limit,
		MemTotalBytes: 8 << 30, MemAvailableBytes: 2 << 30, StateExecAllowed: &exec, Load1: &load}
	if err := validateNASHostFacts(valid); err != nil {
		t.Fatalf("valid host rejected: %v", err)
	}
	if err := validateNASHostFacts(nil); err != nil {
		t.Fatalf("absent host rejected: %v", err)
	}
	zero, negative := 0.0, int64(-1)
	for name, mutate := range map[string]func(h *nasHostFacts){
		"machine token": func(h *nasHostFacts) { h.Machine = "x86 64" },
		"control char":  func(h *nasHostFacts) { h.CPUModel = "bad\nmodel" },
		"long kernel":   func(h *nasHostFacts) { h.KernelRelease = strings.Repeat("k", 300) },
		"cpu count":     func(h *nasHostFacts) { h.CPUCount = -1 },
		"memory":        func(h *nasHostFacts) { h.MemTotalBytes = -1 },
		"cgroup cpu":    func(h *nasHostFacts) { h.CgroupCPULimit = &zero },
		"cgroup mem":    func(h *nasHostFacts) { h.CgroupMemoryLimitBytes = &negative },
	} {
		copy := *valid
		mutate(&copy)
		if err := validateNASHostFacts(&copy); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	started := time.Now()
	heartbeat := connectionHeartbeatRequest{ClientStartedAt: &started, Host: &nasHostFacts{Machine: "bad machine"}}
	if err := validateConnectionHeartbeat(heartbeat); err != nil {
		t.Fatalf("invalid optional host facts must not reject the heartbeat: %v", err)
	}
}

// An invalid host fact must cost the NAS neither its heartbeat nor the rest of
// the telemetry it carried: the handler drops only the host facts.
func TestHeartbeatDropsInvalidHostFactsKeepsTelemetry(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0162_nas_benchmarks.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	const accountID, apiKeyID = int64(47), int64(123)
	if _, err := pool.Exec(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2)`, accountID, apiKeyID); err != nil {
		t.Fatal(err)
	}
	s := &Server{pool: pool}
	call := func(body string) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/connections/heartbeat", strings.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey,
			accountPrincipal{AccountID: accountID, APIKeyID: ptrInt64(apiKeyID)}))
		rec := httptest.NewRecorder()
		s.handleAccountConnectionHeartbeat(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("heartbeat status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
	read := func() (total, free *int64, host []byte) {
		if err := pool.QueryRow(ctx, `SELECT nas_storage_total_bytes,nas_storage_free_bytes,nas_host FROM connections WHERE api_key_id=$1`, apiKeyID).Scan(&total, &free, &host); err != nil {
			t.Fatal(err)
		}
		return total, free, host
	}

	call(`{"storage":{"available":true,"total_bytes":1000,"free_bytes":250},"host":{"machine":"bad machine","cpu_count":4}}`)
	total, free, host := read()
	if total == nil || *total != 1000 || free == nil || *free != 250 {
		t.Fatalf("storage telemetry not persisted with invalid host: total=%v free=%v", total, free)
	}
	if host != nil {
		t.Fatalf("invalid host facts persisted: %s", host)
	}

	call(`{"storage":{"available":true,"total_bytes":1000,"free_bytes":200},"host":{"machine":"x86_64","cpu_count":4}}`)
	if _, _, host = read(); !strings.Contains(string(host), `"x86_64"`) {
		t.Fatalf("valid host facts not persisted: %s", host)
	}
	// A later invalid report must not erase the last good facts.
	call(`{"storage":{"available":true,"total_bytes":1000,"free_bytes":150},"host":{"machine":"x86 64"}}`)
	if _, free, host = read(); free == nil || *free != 150 || !strings.Contains(string(host), `"x86_64"`) {
		t.Fatalf("invalid host report clobbered state: free=%v host=%s", free, host)
	}
}

func TestValidateNASBenchmarkResult(t *testing.T) {
	ok := nasBenchmarkResultRequest{Status: "ok", Result: json.RawMessage(`{"stages":{}}`), ClientVersion: "abc12345"}
	if err := validateNASBenchmarkResult(ok); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	for name, req := range map[string]nasBenchmarkResultRequest{
		"status":        {Status: "done", Result: ok.Result},
		"error missing": {Status: "partial", Result: ok.Result},
		"no result":     {Status: "ok"},
		"array result":  {Status: "ok", Result: json.RawMessage(`[1]`)},
		"null result":   {Status: "ok", Result: json.RawMessage(`null`)},
		"huge result":   {Status: "ok", Result: json.RawMessage(`{"x":"` + strings.Repeat("a", nasBenchmarkMaxResultBytes) + `"}`)},
		"version":       {Status: "ok", Result: ok.Result, ClientVersion: "bad version"},
	} {
		if err := validateNASBenchmarkResult(req); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if err := validateNASBenchmarkResult(nasBenchmarkResultRequest{Status: "unsupported_arch", Error: "aarch64", Result: ok.Result}); err != nil {
		t.Fatalf("unsupported arch rejected: %v", err)
	}
}

func TestNASBenchmarkLifecycle(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0162_nas_benchmarks.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	const accountID, apiKeyID, otherKeyID = int64(47), int64(123), int64(124)
	var connectionID, otherConnectionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2) RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES($1,'nas_pull',$2) RETURNING id`, accountID, otherKeyID).Scan(&otherConnectionID); err != nil {
		t.Fatal(err)
	}
	hour := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	// seed inserts 60 one-minute clips starting at start; skip drops one clip
	// from the NAS inventory so that hour is not complete.
	seed := func(start time.Time, skip int) int64 {
		var recordingID int64
		if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name) VALUES($1,'plaza') RETURNING id`, accountID).Scan(&recordingID); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 60; i++ {
			sha := fmt.Sprintf("%064x", recordingID*1000+int64(i))
			var clipID int64
			at := start.Add(time.Duration(i) * time.Minute)
			if err := pool.QueryRow(ctx, `
				INSERT INTO recording_clips(recording_id,recording_job_id,size_bytes,sha256,clip_start_at,clip_end_at,duration_ms)
				VALUES($1,7,$2,$3,$4,$5,60000) RETURNING id`, recordingID, 1000+i, sha, at, at.Add(time.Minute)).Scan(&clipID); err != nil {
				t.Fatal(err)
			}
			if i == skip {
				continue
			}
			if _, err := pool.Exec(ctx, `
				INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state)
				VALUES($1,$2,$3,$4,$5,$6,'present')`, connectionID, clipID, recordingID,
				fmt.Sprintf("plaza/September/clip_%02d.mp4", i), 1000+i, sha); err != nil {
				t.Fatal(err)
			}
		}
		return recordingID
	}
	complete := seed(hour, -1)
	seed(hour.Add(time.Hour), 30) // newer, but one clip is not on the NAS

	s := &Server{pool: pool, cfg: config.Config{NASBenchmarkEnabled: false}}
	router := chi.NewRouter()
	router.Post("/claim", s.handleAccountConnectionBenchmarkClaim)
	router.Post("/benchmark/{benchmarkId}/result", s.handleAccountConnectionBenchmarkResult)
	call := func(keyID int64, path, body string) (int, string) {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey,
			accountPrincipal{AccountID: accountID, APIKeyID: ptrInt64(keyID)}))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	claim := func(keyID int64) nasBenchmarkClaimResponse {
		code, body := call(keyID, "/claim", `{"client_version":"abc12345","host":{"machine":"x86_64","cpu_count":4}}`)
		if code != http.StatusOK {
			t.Fatalf("claim status=%d body=%s", code, body)
		}
		var resp nasBenchmarkClaimResponse
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			t.Fatal(err)
		}
		return resp
	}

	if resp := claim(apiKeyID); resp.Enabled || resp.Task != nil || resp.RetryAfterSec != nasBenchmarkDisabledSec {
		t.Fatalf("disabled claim=%+v", resp)
	}
	s.cfg.NASBenchmarkEnabled = true
	first := claim(apiKeyID)
	if first.Task == nil || first.Task.RecordingID != complete || !first.Task.WindowStartAt.Equal(hour) ||
		len(first.Task.Clips) != 60 || first.Task.DeadlineSec != 1800 {
		t.Fatalf("first claim=%+v", first.Task)
	}
	if c := first.Task.Clips[0]; c.RelativePath != "plaza/September/clip_00.mp4" || c.SizeBytes != 1000 || len(c.SHA256) != 64 {
		t.Fatalf("first clip=%+v", c)
	}
	if again := claim(apiKeyID); again.Task != nil || !again.Enabled {
		t.Fatalf("claimed benchmark re-offered=%+v", again)
	}

	resultPath := fmt.Sprintf("/benchmark/%d/result", first.Task.BenchmarkID)
	result := `{"client_version":"abc12345","status":"ok","error":"","host":{"machine":"x86_64","cpu_count":4,"state_exec_allowed":true},
		"result":{"media_seconds":3600,"stages":{"full_decode":{"ok":true,"wall_s":90,"cpu_s":170}}}}`
	if code, body := call(otherKeyID, resultPath, result); code != http.StatusNotFound {
		t.Fatalf("foreign report status=%d body=%s", code, body)
	}
	if code, body := call(apiKeyID, resultPath, result); code != http.StatusOK {
		t.Fatalf("report status=%d body=%s", code, body)
	}
	if code, _ := call(apiKeyID, resultPath, result); code != http.StatusConflict {
		t.Fatalf("duplicate report status=%d", code)
	}
	var state, status, machine string
	var cpu float64
	if err := pool.QueryRow(ctx, `SELECT b.state,b.status,(b.result#>>'{stages,full_decode,cpu_s}')::float8,c.nas_host->>'machine'
		FROM nas_benchmarks b JOIN connections c ON c.id=b.connection_id WHERE b.id=$1`, first.Task.BenchmarkID).Scan(&state, &status, &cpu, &machine); err != nil {
		t.Fatal(err)
	}
	if state != "reported" || status != "ok" || cpu != 170 || machine != "x86_64" {
		t.Fatalf("stored state=%s status=%s cpu=%v machine=%s", state, status, cpu, machine)
	}
	// The automatic benchmark runs once; nothing more until an operator asks.
	if resp := claim(apiKeyID); resp.Task != nil {
		t.Fatalf("auto benchmark re-ran=%+v", resp.Task)
	}

	// Operator re-run pinned to an hour; an expired lease is re-offered, then abandoned.
	var pinned int64
	if err := pool.QueryRow(ctx, `INSERT INTO nas_benchmarks(connection_id,requested_by,recording_id,window_start_at)
		VALUES($1,'operator',$2,$3) RETURNING id`, connectionID, complete, hour.Add(30*time.Minute)).Scan(&pinned); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= nasBenchmarkMaxClaims; attempt++ {
		resp := claim(apiKeyID)
		if resp.Task == nil || resp.Task.BenchmarkID != pinned || len(resp.Task.Clips) != 30 {
			t.Fatalf("pinned claim %d=%+v", attempt, resp.Task)
		}
		if _, err := pool.Exec(ctx, `UPDATE nas_benchmarks SET lease_expires_at=now()-interval '1 second' WHERE id=$1`, pinned); err != nil {
			t.Fatal(err)
		}
	}
	if resp := claim(apiKeyID); resp.Task != nil {
		t.Fatalf("benchmark offered past max claims=%+v", resp.Task)
	}
	var claims int
	if err := pool.QueryRow(ctx, `SELECT state,claim_count FROM nas_benchmarks WHERE id=$1`, pinned).Scan(&state, &claims); err != nil {
		t.Fatal(err)
	}
	if state != "abandoned" || claims != nasBenchmarkMaxClaims {
		t.Fatalf("expired benchmark state=%s claims=%d", state, claims)
	}
	// A late report is still recorded.
	late := `{"client_version":"abc12345","status":"partial","error":"deadline","result":{"deadline_hit":true}}`
	if code, body := call(apiKeyID, fmt.Sprintf("/benchmark/%d/result", pinned), late); code != http.StatusOK {
		t.Fatalf("late report status=%d body=%s", code, body)
	}

	// The other connection has nothing on its NAS: its first benchmark is abandoned with a reason.
	if resp := claim(otherKeyID); resp.Task != nil {
		t.Fatalf("empty NAS got a task=%+v", resp.Task)
	}
	var reason string
	if err := pool.QueryRow(ctx, `SELECT state,error FROM nas_benchmarks WHERE connection_id=$1`, otherConnectionID).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "abandoned" || !strings.Contains(reason, "no complete hour") {
		t.Fatalf("empty NAS benchmark state=%s reason=%q", state, reason)
	}
}
