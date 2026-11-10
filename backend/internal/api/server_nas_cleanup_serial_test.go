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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Delay the real policy fingerprint query while the actual plan fence is
// held. Sixteen simultaneous requests would need >15 s to drain that lock.
// No production hook or deadline change is used by this harness.
type cleanupSlowPolicyTracer struct{ calls atomic.Int32 }

func (t *cleanupSlowPolicyTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if data.SQL == cleanupRankingsFingerprintSQL {
		t.calls.Add(1)
		select {
		case <-time.After(1100 * time.Millisecond):
		case <-ctx.Done():
		}
	}
	return ctx
}
func (*cleanupSlowPolicyTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestNASCleanupSixteenConcurrentLeasesSlowPolicyClientServer(t *testing.T) {
	f := newCleanupFixture(t, 24)
	python := os.Getenv("STOARAMA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	script, _ := filepath.Abs("../../../clients/nas-pull/bench_cleanup.py")
	root, state := filepath.Join(t.TempDir(), "nas"), filepath.Join(t.TempDir(), "state")
	raw, err := exec.Command(python, script, "prepare", "--root", root, "--count", "16", "--size", "4096", "--first-clip", "100000").CombinedOutput()
	if err != nil {
		t.Fatalf("prepare: %v %s", err, raw)
	}
	var files []cleanupFixtureFile
	if err = json.Unmarshal(raw, &files); err != nil {
		t.Fatal(err)
	}
	id, sha, _ := f.admit("sixteen-slow-policy", files)
	var active, peak atomic.Int32
	router := chi.NewRouter()
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := int64(5)
			r = r.WithContext(context.WithValue(r.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: &key}))
			if f.s.cfg.NASCleanupDeletionEnabled && strings.HasSuffix(r.URL.Path, "/observe") {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	})
	router.Post("/account/connections/cleanup/lease", f.s.handleAccountNASCleanupLease)
	router.Post("/account/connections/cleanup/{itemId}/observe", f.s.handleAccountNASCleanupObserve)
	router.Post("/account/connections/cleanup/{itemId}/reconcile", f.s.handleAccountNASCleanupReconcile)
	router.Post("/account/connections/cleanup/{itemId}/result", f.s.handleAccountNASCleanupResult)
	router.Post("/account/connections/cleanup/observe-batch", f.s.handleAccountNASCleanupObserveBatch)
	router.Post("/account/connections/cleanup/result-batch", f.s.handleAccountNASCleanupResultBatch)
	srv := httptest.NewServer(router)
	defer srv.Close()
	run := func(concurrent bool) cleanupDeadlineClientResult {
		t.Helper()
		args := []string{script, "run", "--api", srv.URL, "--root", root, "--state", state, "--protocol", "2"}
		if concurrent {
			args = append(args, "--simultaneous-tasks", "16")
		}
		raw, err := exec.Command(python, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("client: %v %s", err, raw)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "BENCH_RESULT ") {
				var result cleanupDeadlineClientResult
				if err = json.Unmarshal([]byte(strings.TrimPrefix(line, "BENCH_RESULT ")), &result); err != nil {
					t.Fatal(err)
				}
				return result
			}
		}
		t.Fatalf("no result: %s", raw)
		return cleanupDeadlineClientResult{}
	}
	if dry := run(false); dry.Unlinks != 0 || len(dry.Errors) != 0 {
		t.Fatalf("dry run: %+v", dry)
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	tracer := &cleanupSlowPolicyTracer{}
	cfg := f.pool.Config()
	cfg.ConnConfig.Tracer = tracer
	slowPool, err := pgxpool.NewWithConfig(f.ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer slowPool.Close()
	f.s.pool = slowPool
	result := run(true)
	if len(result.Errors) != 0 || result.Deleted != 16 || result.Unlinks != 32 || len(result.Outcomes) != 16 {
		t.Fatalf("execution: %+v", result)
	}
	for _, o := range result.Outcomes {
		if !o.Unlink || o.ObserveMS >= 15000 {
			t.Fatalf("late/unverified result: %+v", o)
		}
	}
	if tracer.calls.Load() != 16 || peak.Load() != 1 {
		t.Fatalf("policy calls=%d execution peak=%d", tracer.calls.Load(), peak.Load())
	}
	var deleted, unverified, slowPolicy int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER(WHERE state='deleted'),count(*) FILTER(WHERE state='unverified'),count(*) FILTER(WHERE (stage_timings->>'policy_ms')::int>=1000) FROM nas_cleanup_items WHERE plan_id=$1`, id).Scan(&deleted, &unverified, &slowPolicy); err != nil {
		t.Fatal(err)
	}
	if deleted != 16 || unverified != 0 || slowPolicy != 16 {
		t.Fatalf("deleted=%d unverified=%d slow-policy=%d", deleted, unverified, slowPolicy)
	}
	t.Logf("16 simultaneous leased tasks, 1.1 s policy each: %s, peak requests=%d", fmt.Sprint(result.Seconds), peak.Load())
}
