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

// Delay the real policy cache lookup while the actual plan fence is
// held. Sixteen simultaneous requests would need >15 s to drain that lock.
// No production hook or deadline change is used by this harness.
type cleanupSlowPolicyTracer struct{ calls atomic.Int32 }

func (t *cleanupSlowPolicyTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if data.SQL == cleanupRankingsCacheLoadSQL {
		t.calls.Add(1)
		select {
		case <-time.After(1100 * time.Millisecond):
		case <-ctx.Done():
		}
	}
	return ctx
}
func (*cleanupSlowPolicyTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestNASCleanupSixteenContendingWorkersSlowPolicyClientServer(t *testing.T) {
	testNASCleanupSerialClientServer(t, 0, false)
}

func TestNASCleanupDedicatedLaneWithSlowJoinedDeliveryClientServer(t *testing.T) {
	testNASCleanupSerialClientServer(t, 5, false)
}

// Production plan 1394 stalled after its first execution window. Keep the
// real 15 s drain and 2 s retry; long joined writers require raw credits while
// the raw listing initially returns 502s. Every file must still be deleted.
func TestNASCleanupDrainTimeoutRecoveryClientServer(t *testing.T) {
	testNASCleanupSerialClientServer(t, 35, false)
}

// Plan 1404: a singleton execution_batch lease must survive the real 15 s
// writer drain timeout by returning its unissued work, then finishing on retry.
func TestNASCleanupGroupDrainTimeoutRecoveryClientServer(t *testing.T) {
	testNASCleanupSerialClientServer(t, 35, true)
}

func testNASCleanupSerialClientServer(t *testing.T, joinedSeconds int, grouped bool) {
	slowJoined := joinedSeconds > 0
	drainRecovery := joinedSeconds > 15
	f := newCleanupFixture(t, 24)
	version := nasCleanupExecutionClientVersion
	count := 16
	if slowJoined {
		version = nasCleanupFenceProgressClientVersion
		if grouped {
			version = nasCleanupDurableGroupClientVersion
			count = 1
		}
		if _, err := f.pool.Exec(f.ctx, `UPDATE connections SET client_version=$1 WHERE id=13`, version); err != nil {
			t.Fatal(err)
		}
	}
	python := os.Getenv("STOARAMA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	script, _ := filepath.Abs("../../../clients/nas-pull/bench_cleanup.py")
	root, state := filepath.Join(t.TempDir(), "nas"), filepath.Join(t.TempDir(), "state")
	raw, err := exec.Command(python, script, "prepare", "--root", root, "--count", fmt.Sprint(count), "--size", "4096", "--first-clip", "100000").CombinedOutput()
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
			if f.s.cfg.NASCleanupDeletionEnabled && (strings.HasSuffix(r.URL.Path, "/observe") || strings.HasSuffix(r.URL.Path, "/observe-batch")) {
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
	var listCalls atomic.Int32
	router.Get("/account/clips", func(w http.ResponseWriter, r *http.Request) {
		if listCalls.Add(1) <= 2 {
			w.WriteHeader(502)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"clips":[]}`))
	})
	router.Post("/account/connections/cleanup/lease", f.s.handleAccountNASCleanupLease)
	router.Post("/account/connections/cleanup/{itemId}/observe", f.s.handleAccountNASCleanupObserve)
	router.Post("/account/connections/cleanup/{itemId}/reconcile", f.s.handleAccountNASCleanupReconcile)
	router.Post("/account/connections/cleanup/{itemId}/result", f.s.handleAccountNASCleanupResult)
	router.Post("/account/connections/cleanup/observe-batch", f.s.handleAccountNASCleanupObserveBatch)
	router.Post("/account/connections/cleanup/result-batch", f.s.handleAccountNASCleanupResultBatch)
	run := func(concurrent bool) cleanupDeadlineClientResult {
		t.Helper()
		// Drain every handler before changing the fixture's pool/config
		// between dry run and execution; keep-alive handlers may still unwind.
		srv := httptest.NewServer(router)
		defer srv.Close()
		args := []string{script, "run", "--api", srv.URL, "--root", root, "--state", state, "--protocol", "2", "--client-version", version}
		if concurrent {
			args = append(args, "--simultaneous-tasks", "16", "--max-seconds", "90")
		}
		if slowJoined && f.s.cfg.NASCleanupDeletionEnabled {
			args = append(args, "--slow-joined-seconds", fmt.Sprint(joinedSeconds))
			if drainRecovery {
				args = append(args, "--drain-timeout-recovery", "--max-seconds", "90")
			}
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
	result := run(!slowJoined)
	if len(result.Errors) != 0 || result.Deleted != count || result.Unlinks != 2*count || (!grouped && len(result.Outcomes) != count) {
		t.Fatalf("execution: %+v", result)
	}
	returnedOutcomes := 0
	for _, o := range result.Outcomes {
		if grouped && o.JournalPhase == "returned" && !o.Unlink {
			returnedOutcomes++
			continue
		}
		if !o.Unlink || o.ObserveMS >= 15000 {
			t.Fatalf("late/unverified result: %+v", o)
		}
	}
	if grouped && (returnedOutcomes < 1 || len(result.Outcomes) != count+returnedOutcomes) {
		t.Fatalf("missing durable return diagnostics: %+v", result)
	}
	if tracer.calls.Load() != int32(count) || peak.Load() != 1 {
		t.Fatalf("policy calls=%d execution peak=%d", tracer.calls.Load(), peak.Load())
	}
	var deleted, unverified, slowPolicy int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER(WHERE state='deleted'),count(*) FILTER(WHERE state='unverified'),count(*) FILTER(WHERE (stage_timings->>'policy_ms')::int>=1000) FROM nas_cleanup_items WHERE plan_id=$1`, id).Scan(&deleted, &unverified, &slowPolicy); err != nil {
		t.Fatal(err)
	}
	if deleted != count || unverified != 0 || slowPolicy != count {
		t.Fatalf("deleted=%d unverified=%d slow-policy=%d", deleted, unverified, slowPolicy)
	}
	rate := 60 * float64(count) / result.Seconds
	if slowJoined && ((!drainRecovery && rate < 15) || result.JoinedCalls < 2 || result.JoinedCalls > 4 || result.RawCalls == 0) {
		t.Fatalf("slow joined work interleaved between executions: rate=%.1f/min joined_calls=%d result=%+v", rate, result.JoinedCalls, result)
	}
	if drainRecovery {
		var returned, busy int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_reconciliations WHERE observation->>'return_lease'='true' AND action='pending'`).Scan(&returned); err != nil {
			t.Fatal(err)
		}
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM nas_cleanup_items WHERE plan_id=$1 AND state IN ('leased','delete_issued')`, id).Scan(&busy); err != nil {
			t.Fatal(err)
		}
		if result.DrainTimeouts < 1 || returned < 1 || busy != 0 || listCalls.Load() < 3 || result.Seconds >= 90 {
			t.Fatalf("no sustained recovery: returned=%d busy=%d list_calls=%d result=%+v", returned, busy, listCalls.Load(), result)
		}
	}
	t.Logf("%d files, slow joined=%t, 1.1 s policy each: %.3f s, %.1f files/min, joined calls=%d, raw calls=%d, peak requests=%d", count, slowJoined, result.Seconds, rate, result.JoinedCalls, result.RawCalls, peak.Load())
}
