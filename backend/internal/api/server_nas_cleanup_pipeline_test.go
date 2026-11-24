package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

func TestNASCleanupSuccessorPipelineClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, false, nasCleanupInventoryClientVersion)
}

func TestNASCleanupExecutionGroupClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, nasCleanupInventoryClientVersion)
}

func TestNASCleanupBulkDurabilityClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, nasCleanupBulkDurabilityClientVersion)
}

func TestNASCleanupFreshPermitClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, nasCleanupFreshPermitClientVersion)
}

func TestNASR2CopyV4FreshPermitClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, nasCleanupR2CopyProgressClientVersion)
}

func TestNASR2CopyV6FreshPermitClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, NASR2CopyPagedClientVersion)
}

func TestNASR2CopyV5FreshPermitClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, nasCleanupR2CopyDiagnosticsClientVersion)
}

func TestNASLowMemoryCleanupFreshPermitClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, nasLowMemoryCleanupClientVersion)
	testNASCleanupPipelineClientServer(t, true, nasKeep20CleanupClientVersion)
}

func TestNASR2CopyV8FastCleanupPipelineClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, nasTunableDayClientVersion)
}

func TestNASCleanupPermitTransportClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, nasCleanupPermitTransportClientVersion)
}

func TestNASCleanupItemTruthClientServer(t *testing.T) {
	testNASCleanupPipelineClientServer(t, true, nasCleanupItemTruthClientVersion)
}

func testNASCleanupPipelineClientServer(t *testing.T, grouped bool, groupVersion string) {
	for _, pipeline := range []bool{false, true} {
		t.Run(fmt.Sprint(pipeline), func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			version := nasCleanupPipelineClientVersion
			// Keep correctness/overlap assertions in every run. The original
			// throughput-sized fixture belongs to the existing opt-in bench.
			count, fileSize := 8, 4096
			apiDelay, headDelay := 10*time.Millisecond, 20*time.Millisecond
			if os.Getenv("STOARAMA_CLEANUP_BENCH") != "" {
				count, fileSize = 16, 1048576
				apiDelay, headDelay = 40*time.Millisecond, 80*time.Millisecond
				if grouped {
					apiDelay, headDelay = 200*time.Millisecond, 200*time.Millisecond
				}
			}
			if grouped {
				if pipeline {
					version = groupVersion
				}
			}
			if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, version); err != nil {
				t.Fatal(err)
			}
			python := os.Getenv("STOARAMA_TEST_PYTHON")
			if !filepath.IsAbs(python) {
				t.Fatal("set STOARAMA_TEST_PYTHON to an absolute path for the pinned NAS/support-floor interpreter; do not inherit PATH")
			}
			versionBytes, err := exec.Command(python, "--version").CombinedOutput()
			if err != nil {
				t.Fatalf("pinned Python: %v %s", err, versionBytes)
			}
			t.Logf("explicit cleanup interpreter=%s version=%s", python, strings.TrimSpace(string(versionBytes)))
			script, _ := filepath.Abs("../../../clients/nas-pull/bench_cleanup.py")
			root, state := filepath.Join(t.TempDir(), "nas"), filepath.Join(t.TempDir(), "state")
			raw, err := exec.Command(python, script, "prepare", "--root", root, "--count", fmt.Sprint(count*2), "--size", fmt.Sprint(fileSize), "--first-clip", "100000").Output()
			if err != nil {
				t.Fatal(err)
			}
			var files []cleanupFixtureFile
			if err = json.Unmarshal(raw, &files); err != nil {
				t.Fatal(err)
			}
			current, sha, _ := f.admit("pipeline-current", files[:count])
			var overlap atomic.Bool
			var active, peak atomic.Int32
			router := chi.NewRouter()
			router.Use(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					time.Sleep(apiDelay)
					key := int64(5)
					r = r.WithContext(context.WithValue(r.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: &key}))
					if strings.HasSuffix(r.URL.Path, "/observe") && f.s.cfg.NASCleanupDeletionEnabled {
						n := active.Add(1)
						defer active.Add(-1)
						for old := peak.Load(); n > old; old = peak.Load() {
							if peak.CompareAndSwap(old, n) {
								break
							}
						}
					}
					if strings.HasSuffix(r.URL.Path, "/observe-batch") && f.s.cfg.NASCleanupDeletionEnabled {
						var executing bool
						if err := f.pool.QueryRow(r.Context(), `SELECT mode='enabled' FROM nas_cleanup_plans WHERE id=$1`, current).Scan(&executing); err != nil {
							t.Error(err)
						}
						if executing {
							overlap.Store(true)
						}
					}
					next.ServeHTTP(w, r)
				})
			})
			router.Post("/account/connections/cleanup/lease", f.s.handleAccountNASCleanupLease)
			router.Post("/account/connections/cleanup/{itemId}/observe", f.s.handleAccountNASCleanupObserve)
			router.Post("/account/connections/cleanup/{itemId}/reconcile", f.s.handleAccountNASCleanupReconcile)
			router.Post("/account/connections/cleanup/observe-batch", f.s.handleAccountNASCleanupObserveBatch)
			router.Post("/account/connections/cleanup/result-batch", f.s.handleAccountNASCleanupResultBatch)
			run := func(pipelined bool) float64 {
				t.Helper()
				srv := httptest.NewServer(router)
				defer srv.Close()
				args := []string{script, "run", "--api", srv.URL, "--root", root, "--state", state, "--client-version", version, "--max-seconds", "60"}
				if grouped {
					args = append(args, "--proof-delay-ms", "180", "--fsync-delay-ms", "100")
				}
				if pipelined {
					args = append(args, "--pipeline")
				}
				raw, err := exec.Command(python, args...).CombinedOutput()
				if err != nil {
					t.Fatalf("client: %v\n%s", err, raw)
				}
				if nasFastCopyLine(version) && pipelined &&
					(!strings.Contains(string(raw), `"permit_ms":`) || !strings.Contains(string(raw), `"unlink_ms":`) ||
						!strings.Contains(string(raw), `"receipt_ms":`) || !strings.Contains(string(raw), `"group_files":8`)) {
					t.Fatalf("missing fast cleanup phases: %s", raw)
				}
				for _, line := range strings.Split(string(raw), "\n") {
					if strings.HasPrefix(line, "BENCH_RESULT ") {
						var result struct {
							Seconds float64  `json:"seconds"`
							Errors  []string `json:"errors"`
						}
						if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "BENCH_RESULT ")), &result); err != nil || len(result.Errors) != 0 {
							t.Fatalf("client result: %v %s", err, line)
						}
						return result.Seconds
					}
				}
				t.Fatalf("missing result: %s", raw)
				return 0
			}
			run(false) // Real NAS proof, journal and R2 evidence for the current plan.
			successor, _, _ := f.admit("pipeline-next", files[count:])
			f.bucket.headDelay = headDelay
			f.s.cfg.NASCleanupDeletionEnabled = true
			if code, body := safeguardControl(t, f.s, current, "enabled", map[string]any{"plan_sha256": sha}); code != 200 {
				t.Fatalf("enable: %d %s", code, body)
			}
			seconds := run(pipeline)
			expectedPeak := int32(1)
			if grouped && pipeline {
				expectedPeak = 0
			}
			if peak.Load() != expectedPeak || overlap.Load() != pipeline {
				t.Fatalf("execution peak %d, dry-run overlap %t", peak.Load(), overlap.Load())
			}
			var deleted, checked, issued int
			if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER(WHERE plan_id=$1 AND state='deleted'),count(*) FILTER(WHERE plan_id=$2 AND state='would_delete'),count(*) FILTER(WHERE plan_id=$2 AND delete_ever_issued) FROM nas_cleanup_items`, current, successor).Scan(&deleted, &checked, &issued); err != nil || deleted != count || checked != count || issued != 0 {
				t.Fatalf("deleted=%d successor checked=%d issued=%d: %v", deleted, checked, issued, err)
			}
			for n, file := range files {
				_, err := os.Stat(file.Path)
				if n < count && !os.IsNotExist(err) || n >= count && err != nil {
					t.Fatalf("file %d existence: %v", n, err)
				}
			}
			t.Logf("grouped=%t pipeline=%t: %d serial deletions + %d successor dry runs in %.3fs; execution observation peak=%d", grouped, pipeline, count, count, seconds, peak.Load())
		})
	}
}
