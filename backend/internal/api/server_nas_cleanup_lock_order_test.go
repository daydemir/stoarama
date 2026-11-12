package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type cleanupReceiptStartTracer struct {
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
	deadlocks atomic.Int32
}

func (tr *cleanupReceiptStartTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if d.SQL == `SELECT pg_advisory_xact_lock($1)` && len(d.Args) == 1 && d.Args[0] == nasCleanupPublicFence {
		tr.once.Do(func() {
			close(tr.started)
			select {
			case <-tr.release:
			case <-ctx.Done():
			}
		})
	}
	return ctx
}
func (tr *cleanupReceiptStartTracer) TraceQueryEnd(_ context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	var pe *pgconn.PgError
	if errors.As(d.Err, &pe) && pe.Code == "40P01" {
		tr.deadlocks.Add(1)
	}
}

// Stop the real pause/expiry after its plan UPDATE owns the row, before it
// reaches the batch UPDATE. On 7e5d222e a receipt owns the batch and waits for
// this plan. Its shorter deadlock_timeout makes it the 40P01 victim, so the
// contradictory receipt rolls back and leaves a falsely certain keep.
func TestNASCleanupReceiptLockOrder(t *testing.T) {
	for _, actor := range []string{"operator_pause", "approval_expiry"} {
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/batch=%t", actor, batch), func(t *testing.T) {
				f := newCleanupFixture(t, 24)
				pid, sha, ids := f.admit("lock-order", syntheticFiles(100, 1, 2))
				proof := strings.Repeat("c", 64)
				refusal := cleanupReceipt{Attempt: 1, PlanSHA: sha, ProofSHA: proof, Outcome: "unverified", UnlinkAttempted: new(bool), RefusalProtocol: 1, RefusalReason: "local_guard"}
				raw, _ := json.Marshal(refusal)
				f.dryRun()
				f.s.cfg.NASCleanupDeletionEnabled = true
				if code, body := f.enableBatch([]int64{pid}, []string{sha}, 2); code != 200 {
					t.Fatalf("enable: %d %s", code, body)
				}
				if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='keep',delete_ever_issued=true,attempt=1,proof_sha256=$2,outcome=$3::jsonb WHERE id=$1`, ids[0], proof, string(raw)); err != nil {
					t.Fatal(err)
				}
				const gate = int64(917468)
				if _, err := f.pool.Exec(f.ctx, `CREATE FUNCTION cleanup_pause_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.mode='enabled' AND NEW.mode='paused' THEN PERFORM pg_advisory_xact_lock(917468); END IF; RETURN NEW; END $$;
 CREATE TRIGGER cleanup_pause_gate BEFORE UPDATE ON nas_cleanup_plans FOR EACH ROW EXECUTE FUNCTION cleanup_pause_gate()`); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
				defer cancel()
				blocker, err := f.pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer blocker.Rollback(context.Background())
				if _, err = blocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, gate); err != nil {
					t.Fatal(err)
				}
				tr := &cleanupReceiptStartTracer{started: make(chan struct{}), release: make(chan struct{})}
				var release sync.Once
				defer release.Do(func() { close(tr.release) })
				cfg := f.pool.Config()
				cfg.ConnConfig.Tracer = tr
				cfg.ConnConfig.RuntimeParams["deadlock_timeout"] = "50ms"
				pool, err := pgxpool.NewWithConfig(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer pool.Close()
				receiptServer := &Server{pool: pool, r2: f.s.r2, cfg: f.s.cfg}
				done := make(chan *httptest.ResponseRecorder, 1)
				contradiction := map[string]any{"attempt": 1, "plan_sha256": sha, "proof_sha256": proof, "outcome": "deleted", "unlink_attempted": true}
				go func() {
					if batch {
						contradiction["item_id"] = ids[0]
						done <- cleanupCall(t, receiptServer.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "test", "receipts": []any{contradiction}})
					} else {
						done <- cleanupCall(t, receiptServer.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(ids[0]), contradiction)
					}
				}()
				select {
				case <-tr.started:
				case <-ctx.Done():
					t.Fatal("receipt did not reach fence")
				}
				if actor == "approval_expiry" {
					if _, err = f.pool.Exec(ctx, `UPDATE nas_cleanup_plans SET approved_until=now()-interval '1 minute' WHERE id=$1`, pid); err != nil {
						t.Fatal(err)
					}
				}
				acted := make(chan *httptest.ResponseRecorder, 1)
				go func() {
					if actor == "approval_expiry" {
						f.s.cleanupExpireApprovals(ctx)
						acted <- nil
					} else {
						acted <- cleanupCall(t, f.s.handleAdminNASCleanupControl, 0, 0, "id", fmt.Sprint(pid), map[string]any{"plan_sha256": sha, "mode": "paused", "reason": "operator lock probe"})
					}
				}()
				cleanupWaitBlocked(t, f, blocker.Conn().PgConn().PID())
				var actorPID uint32
				if err = f.pool.QueryRow(ctx, `SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1::integer=ANY(pg_blocking_pids(pid))`, blocker.Conn().PgConn().PID()).Scan(&actorPID); err != nil {
					t.Fatal(err)
				}
				release.Do(func() { close(tr.release) })
				cleanupWaitBlocked(t, f, actorPID)
				if err = blocker.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case w := <-acted:
					if w != nil && w.Code != 200 {
						t.Fatalf("pause: %d %s", w.Code, w.Body.String())
					}
				case <-ctx.Done():
					t.Fatal("pause did not complete")
				}
				var w *httptest.ResponseRecorder
				select {
				case w = <-done:
				case <-ctx.Done():
					t.Fatal("receipt did not complete")
				}
				if (!batch && w.Code != 409) || (batch && (w.Code != 200 || !strings.Contains(w.Body.String(), `"status":409`))) {
					t.Errorf("receipt: %d %s", w.Code, w.Body.String())
				}
				if tr.deadlocks.Load() != 0 {
					t.Errorf("receipt deadlocked despite canonical lock order")
				}
				var state, mode, bmode string
				var retained bool
				if err = f.pool.QueryRow(ctx, `SELECT i.state,p.mode,b.mode,i.outcome ? 'contradictory_receipt' FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id JOIN nas_cleanup_batches b ON p.id=ANY(b.plan_ids) WHERE i.id=$1`, ids[0]).Scan(&state, &mode, &bmode, &retained); err != nil {
					t.Fatal(err)
				}
				if state != "unverified" || mode != "paused" || bmode != "paused" || !retained {
					t.Fatalf("lost contradiction: item=%s plan=%s batch=%s evidence=%t", state, mode, bmode, retained)
				}
			})
		}
	}
}

// nextval is deliberately nontransactional: aborted writes/commits consume an
// attempt, so the real HTTP endpoint must create a new transaction to succeed.
func TestNASCleanupReceiptTransactionRetry(t *testing.T) {
	for _, phase := range []string{"write", "commit"} {
		for _, code := range []string{"40P01", "40001", "XX000"} {
			for _, batch := range []bool{false, true} {
				for _, failures := range []int{2, 3} {
					t.Run(fmt.Sprintf("%s/%s/batch=%t/failures=%d", phase, code, batch, failures), func(t *testing.T) {
						f := newCleanupFixture(t, 1)
						pid, sha, ids := f.admit("retry", syntheticFiles(100, 1, 2))
						proof := strings.Repeat("c", 64)
						refusal := cleanupReceipt{Attempt: 1, PlanSHA: sha, ProofSHA: proof, Outcome: "unverified", UnlinkAttempted: new(bool), RefusalProtocol: 1, RefusalReason: "local_guard"}
						raw, _ := json.Marshal(refusal)
						if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, pid); err != nil {
							t.Fatal(err)
						}
						if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='keep',delete_ever_issued=true,attempt=1,proof_sha256=$2,outcome=$3::jsonb WHERE id=$1`, ids[0], proof, string(raw)); err != nil {
							t.Fatal(err)
						}
						if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_batches(account_id,connection_id,batch_sha256,report_sha256,plan_ids,plan_sha256s,mode,approval_reason) VALUES(47,13,repeat('d',64),$1,$2,$3,'paused','test')`, cleanupPolicySHA, []int64{pid}, []string{sha}); err != nil {
							t.Fatal(err)
						}
						trigger := `CREATE TRIGGER cleanup_fail BEFORE UPDATE ON nas_cleanup_items FOR EACH ROW EXECUTE FUNCTION cleanup_fail()`
						if phase == "commit" {
							trigger = `CREATE CONSTRAINT TRIGGER cleanup_fail AFTER UPDATE ON nas_cleanup_items DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION cleanup_fail()`
						}
						sql := fmt.Sprintf(`CREATE SEQUENCE cleanup_failures;
 CREATE FUNCTION cleanup_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF nextval('cleanup_failures')<=%d THEN RAISE EXCEPTION 'injected receipt failure' USING ERRCODE='%s'; END IF; RETURN NEW; END $$;
 %s`, failures, code, trigger)
						if _, err := f.pool.Exec(f.ctx, sql); err != nil {
							t.Fatal(err)
						}
						receipt := map[string]any{"attempt": 1, "plan_sha256": sha, "proof_sha256": proof, "outcome": "deleted", "unlink_attempted": true}
						send := func() *httptest.ResponseRecorder {
							if batch {
								receipt["item_id"] = ids[0]
								return cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "test", "receipts": []any{receipt}})
							}
							return cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(ids[0]), receipt)
						}
						w := send()
						if failures == 3 {
							if w.Code != 503 || w.Header().Get("Retry-After") != "1" {
								t.Fatalf("failed save became terminal: %d %s", w.Code, w.Body.String())
							}
							w = send() // the client's durable journal replays the same bytes
						}
						if (!batch && w.Code != 409) || (batch && (w.Code != 200 || !strings.Contains(w.Body.String(), `"status":409`))) {
							t.Fatalf("receipt: %d %s", w.Code, w.Body.String())
						}
						var state, mode, bmode string
						var retained bool
						var attempts int
						if err := f.pool.QueryRow(f.ctx, `SELECT i.state,p.mode,b.mode,i.outcome ? 'contradictory_receipt',(SELECT last_value FROM cleanup_failures)::int FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id JOIN nas_cleanup_batches b ON p.id=ANY(b.plan_ids) WHERE i.id=$1`, ids[0]).Scan(&state, &mode, &bmode, &retained, &attempts); err != nil {
							t.Fatal(err)
						}
						if state != "unverified" || mode != "paused" || bmode != "paused" || !retained || attempts != failures+1 {
							t.Fatalf("retry lost evidence: item=%s plan=%s batch=%s retained=%t attempts=%d", state, mode, bmode, retained, attempts)
						}
					})
				}
			}
		}
	}
}
