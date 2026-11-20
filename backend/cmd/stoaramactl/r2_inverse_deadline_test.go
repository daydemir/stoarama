package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// beginTrace records the deadline of every transaction begin, and can delay one
// statement first, as a slow step ahead of the wire would.
type beginTrace struct {
	mu           sync.Mutex
	at, deadline []time.Time
	delaySQL     string
	delay        time.Duration
	delayed      atomic.Bool
}

func (b *beginTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if b.delaySQL != "" && data.SQL == b.delaySQL && b.delayed.CompareAndSwap(false, true) {
		time.Sleep(b.delay)
	}
	if strings.HasPrefix(data.SQL, "begin") {
		deadline, _ := ctx.Deadline()
		b.mu.Lock()
		b.at, b.deadline = append(b.at, time.Now()), append(b.deadline, deadline)
		b.mu.Unlock()
	}
	return ctx
}
func (*beginTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// The wire step acquires its connection under a bound of its own: it never
// spends the reservation's. A reservation that took 5s still leaves the wire
// connection nearly the full inverseDBTimeout, so reverting to the reservation's
// context would leave it about 5s short.
func TestKeep28WireConnectionGetsItsOwnFreshBound(t *testing.T) {
	f := newKeep28Fixture(t)
	tracer := &beginTrace{delaySQL: `SELECT clock_timestamp()`, delay: 5 * time.Second}
	config := f.pool.Config()
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(f.ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	a := f.applier(t)
	a.pool = pool
	if ok, err := a.delete(f.ctx, f.ctx, &inverseSendGate{}, f.entry(t)); err != nil || !ok {
		t.Fatalf("delete: %v %v", ok, err)
	}
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	// Begin 0 is the reservation's transaction; begin 1 is the wire connection.
	if len(tracer.at) < 3 {
		t.Fatalf("%d transaction begins, want reservation, wire and publication", len(tracer.at))
	}
	left := tracer.deadline[1].Sub(tracer.at[1])
	if left < inverseDBTimeout-2*time.Second {
		t.Fatalf("wire connection began with %v left of %v: it spent the reservation's bound", left, inverseDBTimeout)
	}
}

// Publication of a confirmed absence replaces the role's statement_timeout with
// its own bound, so a wait that outlasts the role's default still publishes.
func TestKeep28PublicationOverridesTheRoleStatementTimeout(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry(t)
	if _, err := a.reserve(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	// A role-level statement_timeout far below the wait this publication meets.
	config := f.pool.Config()
	config.ConnConfig.RuntimeParams["statement_timeout"] = "300ms"
	brief, err := pgxpool.NewWithConfig(f.ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer brief.Close()
	var timeout string
	if err = brief.QueryRow(f.ctx, `SHOW statement_timeout`).Scan(&timeout); err != nil || timeout != "300ms" {
		t.Fatalf("fixture role timeout %q: %v", timeout, err)
	}
	holder, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(f.ctx)
	if _, err = holder.Exec(f.ctx, `SELECT pg_advisory_xact_lock(7343226011103)`); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() {
		time.Sleep(700 * time.Millisecond)
		holder.Rollback(f.ctx)
		close(released)
	}()
	a.pool = brief
	if err = a.publishConfirmed(f.ctx, e); err != nil {
		<-released
		t.Fatalf("publication under the role's statement_timeout: %v", err)
	}
	<-released
}

// A parent cancel or deadline ends the job; it is not an unpublished key. Only
// the publication step's own 2-minute bound expiring holds the key.
func TestR2InversePublicationParentDeadlineStopsTheJob(t *testing.T) {
	// The classifier itself, at exactly the shape publication hands it: a step
	// context derived from a parent that already ended. The step inherits the
	// parent's DeadlineExceeded, so only checking the parent tells them apart.
	parent, cancelParent := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelParent()
	<-parent.Done()
	step, cancelStep := context.WithTimeout(parent, inverseDBTimeout)
	defer cancelStep()
	if inverseHoldUnpublished(parent, step, context.DeadlineExceeded) == nil {
		t.Fatal("a parent deadline produced no error")
	} else if inverseKeyHeld(inverseHoldUnpublished(parent, step, context.DeadlineExceeded)) {
		t.Fatal("a parent deadline held the key instead of stopping the job")
	}
	own, cancelOwn := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelOwn()
	<-own.Done()
	if !errors.Is(inverseHoldUnpublished(context.Background(), own, context.DeadlineExceeded), errInverseUnpublished) {
		t.Fatal("the step's own bound expiring did not hold the key")
	}

	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry(t)
	if _, err := a.reserve(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	// Hold the exclusive protection lock so publication waits well past the
	// parent's short deadline, but far inside its own 2-minute bound.
	holder, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(f.ctx)
	if _, err = holder.Exec(f.ctx, `SELECT pg_advisory_xact_lock(7343226011103)`); err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithTimeout(f.ctx, 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = a.publishConfirmed(parent, e)
	if err == nil || inverseKeyHeld(err) || errors.Is(err, errInverseUnpublished) {
		t.Fatalf("parent deadline held the key: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > inverseDBTimeout/2 {
		t.Fatalf("parent deadline was not this step's failure: %v after %v", err, time.Since(started))
	}
	// The key still holds no absence: the job stopped, it did not publish.
	var absent bool
	if err = f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM r2_keep28_events WHERE bucket='stoarama' AND object_key=$1)`, e.Key).Scan(&absent); err != nil || absent {
		t.Fatalf("stopped publication wrote an absence: %v", err)
	}
}
