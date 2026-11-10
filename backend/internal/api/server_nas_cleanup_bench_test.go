package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

// TestNASCleanupClientServerThroughput runs the real NAS client cleanup
// coordinator (clients/nas-pull/stoarama_pull.py via bench_cleanup.py) against
// these handlers over HTTP, with a fake NAS directory, local PostgreSQL and a
// fake R2. It always checks correctness end to end on a small fixture. With
// STOARAMA_CLEANUP_BENCH=1 it also measures files/second for:
//
//	before: protocol 1 client, single-plan approval, full best-14 replay and
//	        full R2 re-download per file (the pre-2026-11 per-item work)
//	after:  protocol 2 client, one batch approval for several plans,
//	        fingerprint + conditional-HEAD rechecks, batched round trips
//
// with a simulated NAS->API round trip and R2 latencies (defaults below,
// modelled on the plan-1388 pilot; override with STOARAMA_CLEANUP_BENCH_RTT_MS,
// _GET_MS, _HEAD_MS, _FILES).
func TestNASCleanupClientServerThroughput(t *testing.T) {
	if os.Getenv("STOARAMA_TEST_DATABASE_URL") == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	// The NAS app runs Python 3.13 (clients/nas-pull/docker-compose.yml).
	python := os.Getenv("STOARAMA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	if exec.Command(python, "-c", "import sys; sys.exit(sys.version_info < (3, 11))").Run() != nil {
		t.Skip("python3 >= 3.11 unavailable (set STOARAMA_TEST_PYTHON)")
	}
	bench := os.Getenv("STOARAMA_CLEANUP_BENCH") != ""
	env := func(name string, small, large int) int {
		if !bench {
			return small
		}
		var v int
		if _, err := fmt.Sscan(os.Getenv(name), &v); err == nil && v > 0 {
			return v
		}
		return large
	}
	rtt := time.Duration(env("STOARAMA_CLEANUP_BENCH_RTT_MS", 0, 100)) * time.Millisecond
	getDelay := time.Duration(env("STOARAMA_CLEANUP_BENCH_GET_MS", 0, 600)) * time.Millisecond
	headDelay := time.Duration(env("STOARAMA_CLEANUP_BENCH_HEAD_MS", 0, 30)) * time.Millisecond
	afterFiles := env("STOARAMA_CLEANUP_BENCH_FILES", 48, 2000)
	beforeFiles := env("STOARAMA_CLEANUP_BENCH_BEFORE_FILES", 6, 40)
	seedPerDay := env("STOARAMA_CLEANUP_BENCH_SEED_PER_DAY", 24, 1440)
	fileSize := env("STOARAMA_CLEANUP_BENCH_FILE_BYTES", 4096, 128*1024)
	script, _ := filepath.Abs("../../../clients/nas-pull/bench_cleanup.py")
	report := map[string]any{"rtt_ms": rtt.Milliseconds(), "r2_get_ms": getDelay.Milliseconds(), "r2_head_ms": headDelay.Milliseconds(), "file_bytes": fileSize, "replay_clips": 14 * seedPerDay}

	prepare := func(root string, first int64, count int) []cleanupFixtureFile {
		t.Helper()
		out, err := exec.Command(python, script, "prepare", "--root", root, "--count", fmt.Sprint(count), "--size", fmt.Sprint(fileSize), "--first-clip", fmt.Sprint(first), "--per-dir", "250").Output()
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		var files []cleanupFixtureFile
		if err := json.Unmarshal(out, &files); err != nil || len(files) != count {
			t.Fatalf("prepare output %v", err)
		}
		return files
	}
	run := func(api, root, state string, protocol int) (float64, int) {
		t.Helper()
		cmd := exec.Command(python, script, "run", "--api", api, "--root", root, "--state", state, "--protocol", fmt.Sprint(protocol), "--max-seconds", "1500")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("client run: %v\n%s\n%s", err, tail(stdout.String()), tail(stderr.String()))
		}
		for _, line := range strings.Split(stdout.String(), "\n") {
			if strings.HasPrefix(line, "BENCH_RESULT ") {
				var r struct {
					Seconds float64 `json:"seconds"`
					Deleted int     `json:"ledger_deleted"`
				}
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "BENCH_RESULT ")), &r); err != nil {
					t.Fatal(err)
				}
				return r.Seconds, r.Deleted
			}
		}
		t.Fatalf("no result:\n%s\n%s", tail(stdout.String()), tail(stderr.String()))
		return 0, 0
	}
	serve := func(f *cleanupFixture, faults float64) string {
		router := chi.NewRouter()
		rng := rand.New(rand.NewSource(7))
		var mu sync.Mutex
		router.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(rtt)
				key := int64(5)
				r = r.WithContext(context.WithValue(r.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: &key}))
				mu.Lock()
				roll := rng.Float64()
				mu.Unlock()
				// Execution observations now fail closed on the first lost
				// response. Their non-destructive reconciliation is covered
				// by TestNASCleanupExecutionDeadlineClientServer; this
				// throughput scenario measures retries of the other calls.
				if f.s.cfg.NASCleanupDeletionEnabled && strings.HasSuffix(r.URL.Path, "/observe") {
					next.ServeHTTP(w, r)
					return
				}
				switch {
				case roll < faults:
					// Proxy error before the request reaches the API.
					http.Error(w, "bad gateway", http.StatusBadGateway)
				case roll < 2*faults:
					// The API committed, but the response was lost.
					next.ServeHTTP(httptest.NewRecorder(), r)
					http.Error(w, "bad gateway", http.StatusBadGateway)
				default:
					next.ServeHTTP(w, r)
				}
			})
		})
		router.Post("/account/connections/cleanup/lease", f.s.handleAccountNASCleanupLease)
		router.Post("/account/connections/cleanup/{itemId}/observe", f.s.handleAccountNASCleanupObserve)
		router.Post("/account/connections/cleanup/{itemId}/result", f.s.handleAccountNASCleanupResult)
		router.Post("/account/connections/cleanup/observe-batch", f.s.handleAccountNASCleanupObserveBatch)
		router.Post("/account/connections/cleanup/result-batch", f.s.handleAccountNASCleanupResultBatch)
		srv := httptest.NewServer(router)
		t.Cleanup(srv.Close)
		return srv.URL
	}
	verify := func(f *cleanupFixture, files []cleanupFixtureFile, deleted int) {
		t.Helper()
		var states []byte
		if err := f.pool.QueryRow(f.ctx, `SELECT jsonb_object_agg(state,n) FROM (SELECT state,count(*) n FROM nas_cleanup_items GROUP BY state) q`).Scan(&states); err != nil {
			t.Fatal(err)
		}
		if string(states) != fmt.Sprintf(`{"deleted": %d}`, len(files)) || deleted != len(files) {
			t.Fatalf("not every file was deleted exactly once: %s ledger %d", states, deleted)
		}
		for _, file := range files {
			if _, err := os.Stat(file.Path); !os.IsNotExist(err) {
				t.Fatalf("file survived: %s", file.Path)
			}
			if _, err := os.Stat(file.Path + ".stoarama.json"); !os.IsNotExist(err) {
				t.Fatalf("sidecar survived: %s", file.Path)
			}
		}
		var paused int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_plans WHERE approval_reason LIKE 'auto-paused%'`).Scan(&paused); err != nil || paused != 0 {
			t.Fatalf("plans auto-paused: %d %v", paused, err)
		}
	}
	newScenario := func(name string) (*cleanupFixture, string, string) {
		f := newCleanupFixture(t, seedPerDay)
		root := filepath.Join(t.TempDir(), "nas")
		state := filepath.Join(t.TempDir(), "state-"+name)
		return f, root, state
	}

	// After: protocol 2, one batch approval over several plans.
	{
		f, root, state := newScenario("after")
		files := prepare(root, 100000, afterFiles)
		plans, per := []int64{}, (afterFiles+3)/4
		shas := []string{}
		for n := 0; n*per < afterFiles; n++ {
			id, sha, _ := f.admit(fmt.Sprintf("bench-after-%d", n), files[n*per:min(afterFiles, (n+1)*per)])
			plans, shas = append(plans, id), append(shas, sha)
		}
		f.bucket.headDelay, f.bucket.getDelay = headDelay, getDelay
		api := serve(f, 0)
		dryStarted := time.Now()
		run(api, root, state, 2)
		report["after_dry_run_files_per_sec"] = float64(afterFiles) / time.Since(dryStarted).Seconds()
		var md5Matched int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE recovery='r2_md5_etag_matched'`).Scan(&md5Matched); err != nil {
			t.Fatal(err)
		}
		report["after_dry_run_md5_matched"] = md5Matched
		f.s.cfg.NASCleanupDeletionEnabled = true
		if code, body := f.enableBatch(plans, shas, 24); code != 200 {
			t.Fatalf("batch enable: %d %s", code, body)
		}
		getsBefore, _ := f.bucket.counts()
		seconds, deleted := run(api, root, state, 2)
		verify(f, files, deleted)
		if gets, _ := f.bucket.counts(); gets != getsBefore {
			t.Fatalf("execution re-downloaded %d R2 objects", gets-getsBefore)
		}
		if m, _ := f.batchMode(); m != "completed" {
			t.Fatalf("batch %s", m)
		}
		report["after_files"], report["after_plans"], report["after_seconds"] = afterFiles, len(plans), seconds
		report["after_files_per_sec"] = float64(afterFiles) / seconds
	}

	// Lost responses and proxy errors in retryable calls (protocol 2 only):
	// retries are idempotent and every file is deleted once. Execution
	// observations are excluded: lost permits require reconciliation.
	{
		f, root, state := newScenario("faults")
		files := prepare(root, 200000, min(afterFiles, 200))
		id, sha, _ := f.admit("bench-faults", files)
		api := serve(f, 0.03)
		run(api, root, state, 2)
		f.s.cfg.NASCleanupDeletionEnabled = true
		if code, body := f.enableBatch([]int64{id}, []string{sha}, 24); code != 200 {
			t.Fatalf("batch enable: %d %s", code, body)
		}
		_, deleted := run(api, root, state, 2)
		verify(f, files, deleted)
	}

	// Dry run without the MD5 shortcut (protocol 2, full download + SHA per
	// file, parallel and outside locks).
	if bench {
		f, root, state := newScenario("dry-full")
		files := prepare(root, 400000, min(afterFiles, 500))
		f.admit("bench-dry-full", files)
		f.bucket.headDelay, f.bucket.getDelay = headDelay, getDelay
		cleanupLeaseAdvertisesMD5 = false
		api := serve(f, 0)
		started := time.Now()
		run(api, root, state, 2)
		report["dry_run_full_sha_files_per_sec"] = float64(len(files)) / time.Since(started).Seconds()
		cleanupLeaseAdvertisesMD5 = true
	}

	// Before: protocol 1 with the old per-item server work.
	{
		f, root, state := newScenario("before")
		files := prepare(root, 300000, beforeFiles)
		id, sha, _ := f.admit("bench-before", files)
		f.bucket.headDelay, f.bucket.getDelay = headDelay, getDelay
		api := serve(f, 0)
		dryStarted := time.Now()
		run(api, root, state, 1)
		report["before_dry_run_protocol1_files_per_sec"] = float64(beforeFiles) / time.Since(dryStarted).Seconds()
		f.s.cfg.NASCleanupDeletionEnabled = true
		if code, body := safeguardControl(t, f.s, id, "enabled", map[string]any{"approval_hours": 24, "allow_long_approval": true, "plan_sha256": sha}); code != 200 {
			t.Fatalf("enable: %d %s", code, body)
		}
		// Without activation or dry-run evidence every observation runs the
		// full live replay and re-downloads + hashes the R2 copy.
		if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET execution_rankings=NULL;UPDATE nas_cleanup_items SET recovery_evidence=NULL`); err != nil {
			t.Fatal(err)
		}
		seconds, deleted := run(api, root, state, 1)
		verify(f, files, deleted)
		report["before_files"], report["before_seconds"] = beforeFiles, seconds
		report["before_files_per_sec"] = float64(beforeFiles) / seconds
	}
	var replayMS float64
	{
		f := newCleanupFixture(t, seedPerDay)
		started := time.Now()
		var best string
		if err := f.pool.QueryRow(f.ctx, cleanupCurrentRankingsSQL, 3).Scan(&best); err != nil {
			t.Fatal(err)
		}
		replayMS = float64(time.Since(started).Microseconds()) / 1000
		started = time.Now()
		var fp string
		if err := f.pool.QueryRow(f.ctx, cleanupRankingsFingerprintSQL, 3).Scan(&fp); err != nil {
			t.Fatal(err)
		}
		report["replay_ms"], report["fingerprint_ms"] = replayMS, float64(time.Since(started).Microseconds())/1000
	}
	if before, ok := report["before_files_per_sec"].(float64); ok && before > 0 {
		report["speedup"] = report["after_files_per_sec"].(float64) / before
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	t.Logf("NAS cleanup throughput:\n%s", b)
	if out := os.Getenv("STOARAMA_CLEANUP_BENCH_OUT"); out != "" {
		_ = os.WriteFile(out, b, 0o644)
	}
}

func tail(s string) string {
	if len(s) > 4000 {
		return s[len(s)-4000:]
	}
	return s
}
