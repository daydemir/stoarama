package api

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func cleanupWaitBlocked(t *testing.T, f *cleanupFixture, blocker uint32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1::integer=ANY(pg_blocking_pids(pid)))`, blocker).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("receipt/close did not reach the forced lock interleaving")
}

func TestNASCleanupCloseRacingContradictoryReceiptRefuses(t *testing.T) {
	f := newCleanupFixture(t, 1)
	pid, sha, ids := f.admit("close-race", syntheticFiles(100, 1, 2))
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_plans SET mode='paused' WHERE id=$1`, pid); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='delete_issued',delete_ever_issued=true,attempt=1,proof_sha256=repeat('c',64) WHERE id=$1`, ids[0]); err != nil {
		t.Fatal(err)
	}
	no, yes := false, true
	receipt := cleanupReceipt{Attempt: 1, PlanSHA: sha, ProofSHA: strings.Repeat("c", 64), Outcome: "unverified", UnlinkAttempted: &no, RefusalProtocol: 1, RefusalReason: "local_guard"}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if updated, _, _, err := cleanupApplyReceipt(f.ctx, tx, 13, ids[0], receipt); err != nil || updated != 1 {
		t.Fatalf("refusal: %d %v", updated, err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	// The runner's last GET sees a certain keep, before the conflicting result.
	var state string
	if err := f.pool.QueryRow(f.ctx, `SELECT state FROM nas_cleanup_items WHERE id=$1`, ids[0]).Scan(&state); err != nil || state != "keep" {
		t.Fatalf("last read: %s %v", state, err)
	}
	tx, err = f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	receipt.Outcome, receipt.UnlinkAttempted, receipt.RefusalProtocol, receipt.RefusalReason = "deleted", &yes, 0, ""
	if updated, _, _, err := cleanupApplyReceipt(f.ctx, tx, 13, ids[0], receipt); err != nil || updated != 0 {
		t.Fatalf("contradiction: %d %v", updated, err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- closeCall(t, f.s, pid, sha) }()
	cleanupWaitBlocked(t, f, tx.Conn().PgConn().PID())
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	w := <-done
	if w.Code != 409 {
		t.Fatalf("close bypassed owner: %d %s", w.Code, w.Body.String())
	}
	if mode, _ := f.planMode(pid); mode != "paused" {
		t.Fatalf("mode: %s", mode)
	}
}

type cleanupAdvanceRaceTracer struct {
	started chan uint32
	release chan struct{}
}

func (tr *cleanupAdvanceRaceTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(d.SQL, "UPDATE nas_cleanup_plans SET mode='enabled',allow_nas_only=false,approved_until=$2") {
		tr.started <- conn.PgConn().PID()
		select {
		case <-tr.release:
		case <-ctx.Done():
		}
	}
	return ctx
}
func (*cleanupAdvanceRaceTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestNASCleanupContradictionRacingAdvancePausesSuccessor(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint("batch=", batch), func(t *testing.T) {
			f := newCleanupFixture(t, 24)
			p1, sha1, _ := f.admit("refused-race", syntheticFiles(100, 1, 2))
			p2, sha2, _ := f.admit("successor-race", syntheticFiles(200, 1, 2))
			f.dryRun()
			f.s.cfg.NASCleanupDeletionEnabled = true
			if code, body := f.enableBatch([]int64{p1, p2}, []string{sha1, sha2}, 2); code != 200 {
				t.Fatalf("enable: %d %s", code, body)
			}
			task := f.leaseV2(1, 1, 1<<30, "")[0]
			permit := f.observe([]cleanupTask{task}, nil)[0]
			no := false
			tx, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(f.ctx)
			receipt := cleanupReceipt{Attempt: task.Attempt, PlanSHA: sha1, ProofSHA: permit.ProofSHA, Outcome: "unverified", UnlinkAttempted: &no, RefusalProtocol: 1, RefusalReason: "local_guard"}
			if updated, _, _, err := cleanupApplyReceipt(f.ctx, tx, 13, task.ItemID, receipt); err != nil || updated != 1 {
				t.Fatalf("refusal: %d %v", updated, err)
			}
			if err := tx.Commit(f.ctx); err != nil {
				t.Fatal(err)
			}
			tracer := &cleanupAdvanceRaceTracer{started: make(chan uint32, 1), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(tracer.release) })
			cfg := f.pool.Config()
			cfg.ConnConfig.Tracer = tracer
			pool, err := pgxpool.NewWithConfig(f.ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			advanceServer := *f.s
			advanceServer.pool = pool
			ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
			defer cancel()
			advanced := make(chan error, 1)
			go func() { advanced <- advanceServer.cleanupAdvanceBatch(ctx) }()
			var blocker uint32
			select {
			case blocker = <-tracer.started:
			case <-ctx.Done():
				t.Fatal("advance never reached successor enable")
			}
			done := make(chan *httptest.ResponseRecorder, 1)
			contradiction := map[string]any{"attempt": task.Attempt, "plan_sha256": sha1, "proof_sha256": permit.ProofSHA, "outcome": "deleted", "error": "later evidence", "unlink_attempted": true}
			go func() {
				if batch {
					contradiction["item_id"] = task.ItemID
					done <- cleanupCall(t, f.s.handleAccountNASCleanupResultBatch, 47, 5, "", "", map[string]any{"client_version": "test", "receipts": []any{contradiction}})
				} else {
					done <- cleanupCall(t, f.s.handleAccountNASCleanupResult, 47, 5, "itemId", fmt.Sprint(task.ItemID), contradiction)
				}
			}()
			cleanupWaitBlocked(t, f, blocker)
			release.Do(func() { close(tracer.release) })
			if err := <-advanced; err != nil {
				t.Fatal(err)
			}
			w := <-done
			if (!batch && w.Code != 409) || (batch && (w.Code != 200 || !strings.Contains(w.Body.String(), `"status":409`))) {
				t.Fatalf("contradiction: %d %s", w.Code, w.Body.String())
			}
			if mode, _ := f.batchMode(); mode != "paused" {
				t.Fatalf("batch: %s", mode)
			}
			for _, pid := range []int64{p1, p2} {
				if mode, _ := f.planMode(pid); mode != "paused" {
					t.Errorf("plan %d remained %s", pid, mode)
				}
			}
			if next := f.leaseV2(1, 1, 1<<30, ""); len(next) != 0 {
				t.Fatalf("successor still leasable: %+v", next)
			}
		})
	}
}
