package api

import (
	"bytes"
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

func TestNASCleanupExecutionDeadlineIncludesAuthentication(t *testing.T) {
	old := nasCleanupExecutionBudget
	nasCleanupExecutionBudget = 150 * time.Millisecond
	defer func() { nasCleanupExecutionBudget = old }()
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%t", batch), func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			id, sha, _ := f.admit("auth-budget", syntheticFiles(100, 1, 1))
			f.dryRun()
			f.s.cfg.NASCleanupDeletionEnabled = true
			if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
				t.Fatalf("enable: %d %s", code, body)
			}
			task := f.leaseV2(1, 1, 1<<30, "")[0]
			_, err := f.pool.Exec(f.ctx, `ALTER TABLE accounts ADD COLUMN email text DEFAULT 'fixture@example.test', ADD COLUMN name text DEFAULT 'fixture', ADD COLUMN role text DEFAULT 'member', ADD COLUMN status text DEFAULT 'active';
CREATE TABLE account_api_keys(id bigint PRIMARY KEY,account_id bigint,secret_hash text,scopes text[],revoked_at timestamptz,expires_at timestamptz,last_used_at timestamptz)`)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.pool.Exec(f.ctx, `INSERT INTO account_api_keys(id,account_id,secret_hash,scopes) VALUES(5,47,$1,'{}')`, hashSecret("fixture-key")); err != nil {
				t.Fatal(err)
			}
			lock, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Rollback(f.ctx)
			if _, err = lock.Exec(f.ctx, `LOCK TABLE account_api_keys IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			unlocked := make(chan struct{})
			timer := time.AfterFunc(350*time.Millisecond, func() {
				_ = lock.Rollback(context.Background())
				close(unlocked)
			})
			defer timer.Stop()
			router := chi.NewRouter()
			router.Use(f.s.requireAccountAuth)
			router.Post("/cleanup/{itemId}/observe", f.s.handleAccountNASCleanupObserve)
			router.Post("/cleanup/observe-batch", f.s.handleAccountNASCleanupObserveBatch)
			body := map[string]any{"attempt": task.Attempt, "plan_sha256": task.PlanSHA, "reconciliation_protocol": nasCleanupReconciliationProtocol, "outcome": "match", "proof": fixtureProof(task.Item), "client_version": "test"}
			url := fmt.Sprintf("/cleanup/%d/observe", task.ItemID)
			if batch {
				body["item_id"] = task.ItemID
				body = map[string]any{"client_version": "test", "observations": []any{body}}
				url = "/cleanup/observe-batch"
			}
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("POST", url, bytes.NewReader(raw))
			req.Header.Set("Authorization", "Bearer fixture-key")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			<-unlocked
			if w.Code != 503 || !strings.Contains(w.Body.String(), `"retryable":true`) {
				t.Fatalf("slow authentication minted a late permit: %d %s", w.Code, w.Body.String())
			}
			var issued bool
			var state string
			if err = f.pool.QueryRow(f.ctx, `SELECT state,delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&state, &issued); err != nil || issued || state != "leased" {
				t.Fatalf("authentication timeout mutated authority: %s %t %v", state, issued, err)
			}
		})
	}
}

type cleanupDeadlineClientResult struct {
	Seconds       float64  `json:"seconds"`
	Unlinks       int      `json:"unlink_calls"`
	Deleted       int      `json:"ledger_deleted"`
	JoinedCalls   int      `json:"joined_calls"`
	RawCalls      int      `json:"raw_calls"`
	DrainTimeouts int      `json:"drain_timeouts"`
	Errors        []string `json:"errors"`
	Outcomes      []struct {
		ObserveMS   int64 `json:"observe_elapsed_ms"`
		ExecutionMS int64 `json:"execution_elapsed_ms"`
		Unlink      bool  `json:"unlink_attempted"`
	} `json:"execution_outcomes"`
}

// Plan 1390 regression: real client, disposable files, PostgreSQL handlers and
// ETag-bound HEAD. A 35 s HEAD must be cut off by the unmodified 15 s execution
// budget, with no issued permit and zero calls to unlink, in both protocols.
func TestNASCleanupExecutionDeadlineClientServer(t *testing.T) {
	if os.Getenv("STOARAMA_TEST_DATABASE_URL") == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	python := os.Getenv("STOARAMA_TEST_PYTHON")
	if python == "" {
		python = "python3"
	}
	script, _ := filepath.Abs("../../../clients/nas-pull/bench_cleanup.py")
	for _, protocol := range []int{1, 2} {
		for _, scenario := range []string{"normal", "slow", "lost-response"} {
			slow := scenario == "slow"
			t.Run(fmt.Sprintf("protocol%d/%s", protocol, scenario), func(t *testing.T) {
				f := newCleanupFixture(t, 24)
				root, state := filepath.Join(t.TempDir(), "nas"), filepath.Join(t.TempDir(), "state")
				raw, err := exec.Command(python, script, "prepare", "--root", root, "--count", "1", "--size", "4096", "--first-clip", "100000").CombinedOutput()
				if err != nil {
					t.Fatalf("prepare: %v %s", err, raw)
				}
				var files []cleanupFixtureFile
				if err = json.Unmarshal(raw, &files); err != nil {
					t.Fatal(err)
				}
				id, sha, _ := f.admit("deadline", files)
				var lost atomic.Bool
				router := chi.NewRouter()
				router.Use(func(next http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						key := int64(5)
						r = r.WithContext(context.WithValue(r.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: &key}))
						if scenario == "lost-response" && f.s.cfg.NASCleanupDeletionEnabled && strings.HasSuffix(r.URL.Path, "/observe") && !lost.Swap(true) {
							next.ServeHTTP(httptest.NewRecorder(), r)
							http.Error(w, "lost permit response", 502)
							return
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
				run := func(oneAttempt bool) cleanupDeadlineClientResult {
					t.Helper()
					args := []string{script, "run", "--api", srv.URL, "--root", root, "--state", state, "--protocol", fmt.Sprint(protocol)}
					if oneAttempt {
						args = append(args, "--one-attempt")
					}
					raw, err := exec.Command(python, args...).CombinedOutput()
					if err != nil {
						t.Fatalf("client: %v %s", err, raw)
					}
					for _, line := range strings.Split(string(raw), "\n") {
						if strings.HasPrefix(line, "BENCH_RESULT ") {
							var result cleanupDeadlineClientResult
							if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "BENCH_RESULT ")), &result); err != nil {
								t.Fatal(err)
							}
							return result
						}
					}
					t.Fatalf("no client result: %s", raw)
					return cleanupDeadlineClientResult{}
				}
				dry := run(true)
				if dry.Unlinks != 0 || len(dry.Errors) != 0 {
					t.Fatalf("dry run: %+v", dry)
				}
				f.s.cfg.NASCleanupDeletionEnabled = true
				if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
					t.Fatalf("enable: %d %s", code, body)
				}
				if slow {
					f.bucket.mu.Lock()
					f.bucket.headDelay = 35 * time.Second
					f.bucket.mu.Unlock()
				}
				result := run(true)
				var itemState string
				var issued bool
				var receipt []byte
				if err := f.pool.QueryRow(f.ctx, `SELECT state,delete_ever_issued,outcome FROM nas_cleanup_items WHERE plan_id=$1`, id).Scan(&itemState, &issued, &receipt); err != nil {
					t.Fatal(err)
				}
				if len(result.Outcomes) != 1 || result.Outcomes[0].ExecutionMS < result.Outcomes[0].ObserveMS {
					t.Fatalf("missing elapsed outcome: %+v", result)
				}
				if scenario == "lost-response" {
					if itemState != "delete_issued" || !issued || result.Unlinks != 0 || result.Deleted != 0 || result.Outcomes[0].Unlink || len(result.Errors) != 1 {
						t.Fatalf("lost response executed or discarded authority: state=%s issued=%t client=%+v", itemState, issued, result)
					}
					replay := run(false)
					if replay.Unlinks != 0 || replay.Deleted != 0 {
						t.Fatalf("replay unlinked: %+v", replay)
					}
					if err := f.pool.QueryRow(f.ctx, `SELECT state,delete_ever_issued,outcome FROM nas_cleanup_items WHERE plan_id=$1`, id).Scan(&itemState, &issued, &receipt); err != nil {
						t.Fatal(err)
					}
					if itemState != "keep" || !issued || !strings.Contains(string(receipt), `"unlink_attempted": false`) {
						t.Fatalf("reconciliation lost safe classification: %s %t %s", itemState, issued, receipt)
					}
				} else if slow {
					if itemState != "leased" || issued || result.Unlinks != 0 || result.Deleted != 0 || result.Outcomes[0].Unlink || len(result.Errors) != 1 || !strings.Contains(result.Errors[0], "503") {
						t.Fatalf("slow HEAD issued/acted: state=%s issued=%t client=%+v", itemState, issued, result)
					}
					if result.Seconds < 14 || result.Seconds > 20 {
						t.Fatalf("execution did not honor 15 s total budget: %+v", result)
					}
					for _, name := range []string{files[0].Path, files[0].Path + ".stoarama.json"} {
						if _, err := os.Stat(name); err != nil {
							t.Fatalf("refused file missing: %s: %v", name, err)
						}
					}
					if mode, _ := f.planMode(id); mode != "enabled" {
						t.Fatalf("timeout changed plan mode: %s", mode)
					}
				} else {
					if itemState != "deleted" || !issued || result.Deleted != 1 || result.Unlinks != 2 || !result.Outcomes[0].Unlink || len(result.Errors) != 0 {
						t.Fatalf("normal execution: state=%s client=%+v", itemState, result)
					}
					var saved map[string]any
					if err := json.Unmarshal(receipt, &saved); err != nil {
						t.Fatal(err)
					}
					if saved["unlink_attempted"] != true || saved["observe_elapsed_ms"] == nil || saved["execution_elapsed_ms"] == nil {
						t.Fatalf("receipt lost diagnostics: %s", receipt)
					}
				}
			})
		}
	}
}

// Also cover legacy batched execution and lock contention after R2 has already
// finished: shortening recovery alone would leave the original bug possible.
func TestNASCleanupExecutionDeadlineIncludesBatchAndLockWait(t *testing.T) {
	old := nasCleanupExecutionBudget
	nasCleanupExecutionBudget = 150 * time.Millisecond
	defer func() { nasCleanupExecutionBudget = old }()
	for _, batch := range []bool{false, true} {
		for _, lock := range []bool{false, true} {
			t.Run(fmt.Sprintf("batch=%t/lock=%t", batch, lock), func(t *testing.T) {
				f := newCleanupFixture(t, 24)
				id, sha, _ := f.admit("bounded", syntheticFiles(100, 1, 1))
				f.dryRun()
				f.s.cfg.NASCleanupDeletionEnabled = true
				if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
					t.Fatalf("enable: %d %s", code, body)
				}
				task := f.leaseV2(1, 1, 1<<30, "")[0]
				if lock {
					tx, err := f.pool.Begin(f.ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback(f.ctx)
					if _, err = tx.Exec(f.ctx, `SELECT pg_advisory_xact_lock($1)`, nasCleanupBudgetLock); err != nil {
						t.Fatal(err)
					}
				} else {
					f.bucket.mu.Lock()
					f.bucket.headDelay = 35 * time.Second
					f.bucket.mu.Unlock()
				}
				body := map[string]any{"item_id": task.ItemID, "attempt": task.Attempt, "plan_sha256": task.PlanSHA, "reconciliation_protocol": nasCleanupReconciliationProtocol, "outcome": "match", "proof": fixtureProof(task.Item), "client_version": "test"}
				started := time.Now()
				var w *httptest.ResponseRecorder
				if batch {
					w = cleanupCall(t, f.s.handleAccountNASCleanupObserveBatch, 47, 5, "", "", map[string]any{"client_version": "test", "observations": []map[string]any{body}})
				} else {
					delete(body, "item_id")
					w = cleanupCall(t, f.s.handleAccountNASCleanupObserve, 47, 5, "itemId", fmt.Sprint(task.ItemID), body)
				}
				if w.Code != 503 || !strings.Contains(w.Body.String(), `"retryable":true`) || strings.Contains(w.Body.String(), `"delete":true`) {
					t.Fatalf("deadline response: %d %s", w.Code, w.Body.String())
				}
				if time.Since(started) > 2*time.Second {
					t.Fatal("execution was not bounded")
				}
				var issued bool
				var state string
				if err := f.pool.QueryRow(f.ctx, `SELECT state,delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&state, &issued); err != nil || issued || state != "leased" {
					t.Fatalf("deadline mutated authority: %s %t %v", state, issued, err)
				}
			})
		}
	}
}

func TestNASCleanupNeverAttemptedReceiptPreservesSafetyAndDiagnostics(t *testing.T) {
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("refused", syntheticFiles(100, 1, 1))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(1, 1, 1<<30, "")[0]
	permit := f.observe([]cleanupTask{task}, nil)[0]
	body := map[string]any{"attempt": task.Attempt, "plan_sha256": sha, "proof_sha256": permit.ProofSHA, "outcome": "unverified", "error": "cleanup observation deadline exceeded (30 s); not unlinked", "observe_elapsed_ms": 35000, "execution_elapsed_ms": 35001, "unlink_attempted": false, "refusal_reason": "deadline_exceeded"}
	for n := 0; n < 2; n++ {
		w := cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(task.ItemID), body)
		if w.Code != 200 {
			t.Fatalf("receipt/replay: %d %s", w.Code, w.Body.String())
		}
	}
	var saved []byte
	var issued bool
	if err := f.pool.QueryRow(f.ctx, `SELECT outcome,delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&saved, &issued); err != nil {
		t.Fatal(err)
	}
	if !issued || !strings.Contains(string(saved), `"unlink_attempted": false`) || !strings.Contains(string(saved), `"observe_elapsed_ms": 35000`) {
		t.Fatalf("receipt lost evidence: %s issued=%t", saved, issued)
	}
	if mode, _ := f.planMode(id); mode != "paused" {
		t.Fatalf("legacy refusal bypassed safety pause: %s", mode)
	}
	body["outcome"] = "deleted"
	w := cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(task.ItemID), body)
	if w.Code != 400 {
		t.Fatalf("accepted contradictory diagnostics: %d %s", w.Code, w.Body.String())
	}
}

func TestNASCleanupDryRunKeepsRecoveryBudgetAndRefusesEnableRace(t *testing.T) {
	old := nasCleanupExecutionBudget
	nasCleanupExecutionBudget = 150 * time.Millisecond
	defer func() { nasCleanupExecutionBudget = old }()
	f := newCleanupFixture(t, 24)
	id, sha, _ := f.admit("dry-budget", syntheticFiles(100, 1, 1))
	tasks := f.leaseV2(1, 0, 0, "")
	f.bucket.getDelay = 300 * time.Millisecond
	if r := f.observe(tasks, nil)[0]; r.Status != 200 || r.Action != "would_delete" {
		t.Fatalf("dry-run budget shortened: %+v", r)
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := f.enableBatch([]int64{id}, []string{sha}, 2); code != 200 {
		t.Fatalf("enable: %d %s", code, body)
	}
	task := f.leaseV2(1, 1, 1<<30, "")[0]
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	result := f.s.cleanupObserveOne(f.ctx, tx, 13, task.ItemID, cleanupObserve{ReconciliationProtocol: nasCleanupReconciliationProtocol, Attempt: task.Attempt, PlanSHA: task.PlanSHA, Outcome: "match", Proof: fixtureProof(task.Item)}, &cleanupObserveEnv{dryRunOnly: true})
	if result.status != 503 || result.body["retryable"] != true || result.write {
		t.Fatalf("issued under dry-run context: %+v", result)
	}
}
