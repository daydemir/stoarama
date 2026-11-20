package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// keep28Attempts counts the durable wire attempts for a key: a minted attempt
// is permanent, so no key may gain one after its wire deadline has passed.
func (f keep28Fixture) attempts(t *testing.T, key string) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_keep28_attempts WHERE bucket='stoarama' AND object_key=$1`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// authorizeOver runs one authorization against a fresh wire transaction and
// always disposes of both the transaction and any authority it returned.
func (f keep28Fixture) authorizeOver(t *testing.T, a *inverseApplier, admitCtx, wireCtx context.Context, e inverseEntry) error {
	t.Helper()
	tx, err := f.pool.Begin(wireCtx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	authority, err := a.authorize(admitCtx, wireCtx, tx, e, &inverseSendGate{})
	if authority != nil {
		authority.cancel()
		t.Fatal("authorize issued wire authority on a case that must refuse it")
	}
	return err
}

// Admission's bound is two minutes, the key's own wire budget is thirty
// seconds. Pre-issuance therefore takes the earliest of admission, the parent's
// deadline and the network timeout: a live admission alone must never let
// preflight outlive the wire deadline and still mint an attempt.
func TestR2InversePreIssuanceHonorsTheWireDeadlineNotAdmission(t *testing.T) {
	// slow_preflight finishes inside a live admission but past the wire
	// deadline. An admission-only bound would mint an attempt and reach the
	// wire; the earliest-deadline bound refuses before either.
	t.Run("slow_preflight", func(t *testing.T) {
		f := newKeep28Fixture(t)
		a := f.applier(t)
		e := f.entry(t)
		if _, err := a.reserve(f.ctx, e); err != nil {
			t.Fatal(err)
		}
		// A preflight HEAD that never answers cancels exactly at the wire deadline.
		f.store.mu.Lock()
		f.store.onHead = func(string) string { return "slow" }
		f.store.mu.Unlock()
		defer func() {
			f.store.mu.Lock()
			f.store.onHead = nil
			f.store.mu.Unlock()
		}()
		// Admission stays live for the whole case; only the wire parent is short.
		admitCtx, cancelAdmit := context.WithTimeout(f.ctx, 30*time.Second)
		defer cancelAdmit()
		wireCtx, cancelWire := context.WithTimeout(f.ctx, 400*time.Millisecond)
		defer cancelWire()
		started := time.Now()
		err := f.authorizeOver(t, a, admitCtx, wireCtx, e)
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("preflight outlived the wire deadline and still issued authority")
		}
		if admitCtx.Err() != nil {
			t.Fatal("admission expired, so the case never exercised a live admission")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("preflight was not stopped by the wire deadline: %v", err)
		}
		// 400ms wire, not 30s admission and not the full 1.2s preflight.
		if elapsed > 1100*time.Millisecond {
			t.Fatalf("pre-issuance ran %v, so it followed admission or the preflight instead of the wire deadline", elapsed)
		}
		if got := f.attempts(t, e.Key); got != 0 {
			t.Fatalf("%d durable attempts minted past the wire deadline", got)
		}
		if f.deletes() != 0 {
			t.Fatalf("%d DELETEs sent without a minted attempt", f.deletes())
		}
	})
	// attempt_insert_contention keeps the attempt table busy. Each insert is
	// bounded, so the retry loop must stop at the wire deadline instead of
	// waiting out the live admission.
	t.Run("attempt_insert_contention", func(t *testing.T) {
		f := newKeep28Fixture(t)
		a := f.applier(t)
		e := f.entry(t)
		if _, err := a.reserve(f.ctx, e); err != nil {
			t.Fatal(err)
		}
		holder, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Rollback(context.Background())
		if _, err = holder.Exec(f.ctx, `LOCK TABLE r2_keep28_attempts IN SHARE MODE`); err != nil {
			t.Fatal(err)
		}
		admitCtx, cancelAdmit := context.WithTimeout(f.ctx, 30*time.Second)
		defer cancelAdmit()
		wireCtx, cancelWire := context.WithTimeout(f.ctx, 400*time.Millisecond)
		defer cancelWire()
		started := time.Now()
		err = f.authorizeOver(t, a, admitCtx, wireCtx, e)
		elapsed := time.Since(started)
		if err == nil {
			t.Fatal("a busy attempt table still issued authority")
		}
		if admitCtx.Err() != nil {
			t.Fatal("admission expired, so the case never exercised a live admission")
		}
		if elapsed > 1100*time.Millisecond {
			t.Fatalf("pre-issuance ran %v, past the wire deadline", elapsed)
		}
		if got := f.attempts(t, e.Key); got != 0 {
			t.Fatalf("%d durable attempts minted against a busy table past the wire deadline", got)
		}
		if f.deletes() != 0 {
			t.Fatalf("%d DELETEs sent without a minted attempt", f.deletes())
		}
	})
}

// commitAckDelay blocks the return of an already acknowledged COMMIT until the
// traced context really expires, so the server's success reaches the caller
// late while the wire context stays healthy. It records that a successful
// COMMIT hook was observed, so the case can prove the ack actually happened
// rather than merely that some deadline error surfaced. Matching happens per
// query on the context TraceQueryStart returns, so it is safe under concurrent
// use and touches no production code.
type commitAckDelay struct {
	armed    atomic.Bool
	observed atomic.Bool
}

type commitAckKey struct{}

func (c *commitAckDelay) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(data.SQL)), "commit") && c.armed.CompareAndSwap(true, false) {
		return context.WithValue(ctx, commitAckKey{}, true)
	}
	return ctx
}

func (c *commitAckDelay) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if ctx.Value(commitAckKey{}) != true || data.Err != nil {
		return
	}
	// The COMMIT was acknowledged with no error; only its return is late. Hold
	// it until the traced context itself expires, which this test bounds.
	c.observed.Store(true)
	<-ctx.Done()
}

// stalledDeadlineContext reports a deadline that has already passed while its
// Done channel never closes and Err stays nil, exactly as a late timer delivery
// looks to a caller. An Err-only guard would let this context through.
type stalledDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c stalledDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (c stalledDeadlineContext) Done() <-chan struct{}       { return nil }
func (c stalledDeadlineContext) Err() error                  { return nil }

// An attempt COMMIT acknowledged after admission expired must not mint
// authority: the durable attempt stays held for reconciliation and no DELETE
// leaves. The case requires an observed successful COMMIT hook that is held
// until the traced context really expires, so it proves the post-acknowledgement
// refusal rather than any earlier deadline error.
func TestR2InverseLateAcknowledgedAttemptCommitNeverMints(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry(t)
	if _, err := a.reserve(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	tracer := &commitAckDelay{}
	config := f.pool.Config()
	config.ConnConfig = config.ConnConfig.Copy()
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(f.ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	a.pool = pool
	tracer.armed.Store(true)
	// Admission expires while the wire parent stays healthy, so only the late
	// acknowledgement can end pre-issuance.
	admitCtx, cancelAdmit := context.WithTimeout(f.ctx, 400*time.Millisecond)
	defer cancelAdmit()
	wireCtx, cancelWire := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancelWire()
	err = f.authorizeOver(t, a, admitCtx, wireCtx, e)
	if err == nil {
		t.Fatal("a late acknowledged attempt COMMIT still issued wire authority")
	}
	if !tracer.observed.Load() {
		t.Fatal("no successful COMMIT hook was observed, so the case never exercised a late acknowledgement")
	}
	if admitCtx.Err() == nil {
		t.Fatal("admission did not actually expire, so the acknowledgement was never late")
	}
	// Explicit post-acknowledgement refusal, not merely some deadline error.
	if !errors.Is(err, errInverseLateAttemptCommit) {
		t.Fatalf("late acknowledgement was not reported as its own refusal: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late acknowledgement did not preserve the deadline meaning: %v", err)
	}
	if wireCtx.Err() != nil {
		t.Fatal("the wire deadline ended the case instead of the admission bound")
	}
	if strings.Contains(err.Error(), "COMMIT outcome unknown") {
		t.Fatalf("an acknowledged COMMIT was reported as ambiguous: %v", err)
	}
	// The attempt is durable and held exactly once: no automatic retry mints a
	// second one, and nothing reached the wire.
	if got := f.attempts(t, e.Key); got != 1 {
		t.Fatalf("%d durable attempts after a late acknowledged COMMIT, want exactly 1", got)
	}
	if f.deletes() != 0 {
		t.Fatalf("%d DELETEs sent from authority that outlived its bound", f.deletes())
	}
	err = f.authorizeOver(t, a, f.ctx, f.ctx, e)
	if err == nil || !strings.Contains(err.Error(), "already issued") {
		t.Fatalf("a durable late-acknowledged attempt was re-minted: %v", err)
	}
}

// A context timer delivered late leaves Err nil past its deadline. The guard
// must still refuse, using the real monotonic clock against the context's own
// deadline, so a lagging timer cannot re-enable the wire.
func TestR2InversePreIssuanceExpiredResistsLateTimerDelivery(t *testing.T) {
	stalled := stalledDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	if stalled.Err() != nil {
		t.Fatal("the sentinel reported an error, so it cannot stand for a late timer")
	}
	if err := inversePreIssuanceExpired(stalled); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a past deadline with no timer delivery was not refused: %v", err)
	}
	// A live bound is still not expired, and an explicit error keeps its own
	// meaning.
	if err := inversePreIssuanceExpired(context.Background()); err != nil {
		t.Fatalf("an unbounded context was refused: %v", err)
	}
	live, cancelLive := context.WithTimeout(context.Background(), time.Minute)
	defer cancelLive()
	if err := inversePreIssuanceExpired(live); err != nil {
		t.Fatalf("a live bound was refused: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := inversePreIssuanceExpired(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("an explicit cancel lost its meaning: %v", err)
	}
}

// A known locked lock refusal that exhausts a configured quick cap must stay a
// safe transient: an already confirmed 404 still becomes Unpublished. An
// unexpected failure, a cancelled parent and an uncertain COMMIT stay hard.
func TestR2InverseKnownLockRefusalAtCapKeepsTheUnpublishedPath(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry(t)
	if _, err := a.reserve(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	// One attempt is the configured cap: the very first real pg_try_advisory_*
	// refusal exhausts it, so the case is a locked-DB witness, not a loop.
	previousRetries, previousDeadline := inverseAdmissionRetries, inverseAdmissionDeadline
	inverseAdmissionRetries = 1
	inverseAdmissionDeadline = 5 * time.Second
	t.Cleanup(func() { inverseAdmissionRetries, inverseAdmissionDeadline = previousRetries, previousDeadline })
	// A live session owns publication's second wide lock exclusively, so
	// pg_try_advisory_xact_lock(7343226011101) returns false.
	holder, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(context.Background())
	if _, err = holder.Exec(f.ctx, `SELECT pg_advisory_xact_lock(7343226011101)`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = a.publishAbsent(f.ctx, e, a.opts.ManifestSHA)
	if err == nil {
		t.Fatal("publication succeeded while a live session held its wide lock")
	}
	var busy inverseBusy
	if !errors.As(err, &busy) {
		// The busy result names no wait here (the refusal came from a NOWAIT
		// try), so only the bound and its cause are in the chain by design.
		t.Logf("exhausted cap carried no named wait, as expected: %v", err)
	}
	if !errors.Is(err, errInverseAdmissionBusy) {
		t.Fatalf("the exhausted cap did not report its bound: %v", err)
	}
	if !errors.Is(err, errInverseLockRefused) {
		t.Fatalf("a known pg_try_advisory_* refusal lost its safe meaning: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("the quick cap took %v", elapsed)
	}
	if !inverseDBTransient(f.ctx, f.ctx, err) {
		t.Fatalf("a known lock refusal was not transient: %v", err)
	}
	// Already confirmed 404: the absence is held as Unpublished, never lost.
	if held := inverseHoldUnpublished(f.ctx, f.ctx, err); !errors.Is(held, errInverseUnpublished) {
		t.Fatalf("a known refusal did not preserve the confirmed 404 as Unpublished: %v", held)
	}
	// Everything that is not that known refusal stays hard.
	parent, cancelParent := context.WithCancel(f.ctx)
	cancelParent()
	if inverseDBTransient(parent, f.ctx, err) || errors.Is(inverseHoldUnpublished(parent, f.ctx, err), errInverseUnpublished) {
		t.Fatal("a cancelled parent was treated as a safe retry")
	}
	for _, unknown := range []error{errors.New("unknown publication failure"), context.Canceled} {
		if inverseDBTransient(f.ctx, f.ctx, unknown) || errors.Is(inverseHoldUnpublished(f.ctx, f.ctx, unknown), errInverseUnpublished) {
			t.Fatalf("an unknown failure was treated as a safe retry: %v", unknown)
		}
	}
	// A confirmed DELETE whose publication COMMIT outcome is unknown is a hard
	// stop, never Unpublished.
	uncertain := errors.New("absence publication COMMIT outcome unknown")
	if held := inverseHoldUnpublished(f.ctx, f.ctx, uncertain); errors.Is(held, errInverseUnpublished) {
		t.Fatalf("an uncertain publication COMMIT was held as Unpublished: %v", held)
	}
}

// 0223's exact post-intent stitch outcome is a durable conflict, not a lock
// wait. It must hard-stop the key even though it shares SQLSTATE 55P03 with
// plain lock contention; it is never retried as a publication attempt and never
// reported as an unpublished absence.
func TestR2InversePostIntentStitchBusyIsAHardStop(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry(t)
	if _, err := a.reserve(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	// The witness raises exactly what 0223's ledger trigger raises, at the
	// publication insert, and counts every attempt to publish.
	if _, err := f.pool.Exec(f.ctx, `CREATE SEQUENCE inverse_publish_stitch_tries;
 CREATE FUNCTION inverse_publish_stitch_busy() RETURNS trigger LANGUAGE plpgsql AS $f$
 BEGIN PERFORM nextval('inverse_publish_stitch_tries');
  RAISE EXCEPTION 'stitch input hold admission is busy' USING ERRCODE='55P03'; END $f$;
 CREATE TRIGGER inverse_publish_stitch_busy BEFORE INSERT ON r2_keep28_events
 FOR EACH ROW EXECUTE FUNCTION inverse_publish_stitch_busy()`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.pool.Exec(f.ctx, `DROP TRIGGER inverse_publish_stitch_busy ON r2_keep28_events;
 DROP FUNCTION inverse_publish_stitch_busy(); DROP SEQUENCE inverse_publish_stitch_tries`)
	})
	started := time.Now()
	err := a.publishConfirmed(f.ctx, e)
	if err == nil {
		t.Fatal("the exact stitch admission outcome published an absence")
	}
	if inverseKeyHeld(err) || errors.Is(err, errInverseUnpublished) || errors.Is(err, errInverseWireAmbiguous) {
		t.Fatalf("an exact stitch conflict was reported as a held key: %v", err)
	}
	if inverseDBTransient(f.ctx, f.ctx, err) {
		t.Fatalf("an exact stitch conflict was treated as transient: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("an exact stitch conflict was retried for %v", elapsed)
	}
	var tries int
	if err = f.pool.QueryRow(f.ctx, `SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM inverse_publish_stitch_tries`).Scan(&tries); err != nil {
		t.Fatal(err)
	}
	if tries != 1 {
		t.Fatalf("an exact stitch conflict was retried %d times, want one hard stop", tries)
	}
	var durable int
	if err = f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_object_retirements WHERE object_key=$1)
 +(SELECT count(*) FROM r2_keep28_events WHERE object_key=$1)`, e.Key).Scan(&durable); err != nil || durable != 0 {
		t.Fatalf("a hard-stopped stitch conflict left durable publication: %d %v", durable, err)
	}
	// The exact classifier is shared with the pre-intent skip: both of 0223's
	// outcomes are stitch conflicts, and a lookalike is not.
	for _, c := range []struct {
		pg   pgconn.PgError
		busy bool
		skip bool
	}{
		{pgconn.PgError{Code: "55P03", Message: "stitch input hold admission is busy"}, true, true},
		{pgconn.PgError{Code: "55000", Message: "active R2 stitch input hold"}, true, true},
		{pgconn.PgError{Code: "55P03", Message: "canceling statement due to lock timeout"}, false, false},
		{pgconn.PgError{Code: "55P03", Message: "could not obtain lock on row in relation"}, false, false},
		{pgconn.PgError{Code: "55000", Message: "stitch input hold admission is busy"}, false, false},
	} {
		cause := &c.pg
		if got := inverseStitchRefusal(cause); got != c.busy {
			t.Fatalf("%s %q stitch refusal=%v", c.pg.Code, c.pg.Message, got)
		}
		if got := errors.Is(inverseStitchLedgerError(cause), errInverseStitchConflict); got != c.skip {
			t.Fatalf("%s %q pre-intent skip=%v", c.pg.Code, c.pg.Message, got)
		}
	}
}
