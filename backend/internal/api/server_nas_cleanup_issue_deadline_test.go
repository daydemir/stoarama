package api

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Model cancellation arriving late at the driver: the final write reaches
// PostgreSQL after the deadline despite Go's pre-write check having passed.
type cleanupLateIssueTracer struct{ delayed bool }

func (tr *cleanupLateIssueTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(d.SQL, "UPDATE nas_cleanup_items SET state=$2,delete_ever_issued=") {
		tr.delayed = true
		deadline, _ := ctx.Deadline()
		time.Sleep(time.Until(deadline) + 30*time.Millisecond)
		return context.WithoutCancel(ctx)
	}
	return ctx
}
func (*cleanupLateIssueTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestNASCleanupDatabaseClockRefusesLatePermitWrite(t *testing.T) {
	f, _, tasks := cleanupFenceFixture(t, false)
	task := tasks[0]
	prefetched := f.s.cleanupPrefetchRecovery(f.ctx, 13, map[int64]string{task.ItemID: ""})
	cfg := f.pool.Config()
	tracer := &cleanupLateIssueTracer{}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(f.ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	ctx, cancel := context.WithTimeout(f.ctx, 150*time.Millisecond)
	defer cancel()
	req := cleanupObserve{Attempt: task.Attempt, PlanSHA: task.PlanSHA, Outcome: "match", Proof: fixtureProof(task.Item)}
	result := f.s.cleanupObserveOne(ctx, tx, 13, task.ItemID, req, &cleanupObserveEnv{
		session: cleanupClientSession{boot: "fixture-boot", version: nasCleanupExecutionClientVersion}, prefetched: prefetched,
	})
	if !tracer.delayed || result.status != 503 || result.write || result.body["delete"] == true {
		t.Fatalf("late write granted authority: %+v", result)
	}
	// Even committing with an uncancelled context cannot persist a permit.
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	var issued bool
	if err = f.pool.QueryRow(f.ctx, `SELECT delete_ever_issued FROM nas_cleanup_items WHERE id=$1`, task.ItemID).Scan(&issued); err != nil || issued {
		t.Fatalf("late write persisted authority: %t %v", issued, err)
	}
}
