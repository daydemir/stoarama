package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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

// Pre-issuance runs under admission's 2-minute bound, which is far longer than
// the key's 30s wire context. Every pre-issuance step therefore takes the
// earliest of admission, the parent's deadline and the network timeout, and
// nothing is minted once that has passed: a slow HEAD or a contended attempt
// insert can never leave an attempt that no DELETE can ever use.
func TestR2InversePreIssuanceCannotMintPastTheWireDeadline(t *testing.T) {
	// slot prepares one key that is reserved but not authorized, and returns
	// the applier and entry plus the way to stop the interference.
	for name, tc := range map[string]struct {
		setup func(t *testing.T, f keep28Fixture) func()
	}{
		// A preflight HEAD that never answers would otherwise run to the whole
		// admission bound while the wire transaction holds the wide locks.
		"slow_preflight": {setup: func(t *testing.T, f keep28Fixture) func() {
			f.store.mu.Lock()
			f.store.onHead = func(string) string { return "slow" }
			f.store.mu.Unlock()
			return func() {
				f.store.mu.Lock()
				f.store.onHead = nil
				f.store.mu.Unlock()
			}
		}},
		// The attempt insert sits on its own connection, and its table can be
		// busy: the retry loop must still stop at the wire deadline, never
		// mint once it has passed.
		"attempt_insert_contention": {setup: func(t *testing.T, f keep28Fixture) func() {
			holder, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = holder.Exec(f.ctx, `LOCK TABLE r2_keep28_attempts IN SHARE MODE`); err != nil {
				t.Fatal(err)
			}
			return func() { holder.Rollback(context.Background()) }
		}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newKeep28Fixture(t)
			a := f.applier(t)
			e := f.entry(t)
			stop := tc.setup(t, f)
			defer stop()
			// A short fake parent stands in for the job's own 30s wire bound;
			// the real ledger, intent and attempt tables are used.
			parent, cancel := context.WithTimeout(f.ctx, 600*time.Millisecond)
			defer cancel()
			started := time.Now()
			ok, err := a.delete(parent, parent, &inverseSendGate{}, e)
			elapsed := time.Since(started)
			if ok || err == nil {
				t.Fatalf("%s minted a wire attempt past the wire deadline", name)
			}
			if inverseKeyHeld(err) || errors.Is(err, errInverseUnpublished) || errors.Is(err, errInverseWireAmbiguous) {
				t.Fatalf("%s held a key that never reached the wire: %v", name, err)
			}
			if elapsed > 10*time.Second {
				t.Fatalf("%s pre-issuance outlived the wire deadline by %v", name, elapsed)
			}
			if got := f.attempts(t, e.Key); got != 0 {
				t.Fatalf("%s minted %d durable attempts after the wire deadline", name, got)
			}
			if f.deletes() != 0 {
				t.Fatalf("%s sent %d DELETEs without a minted attempt", name, f.deletes())
			}
			// Nothing durable beyond the reservation was left behind: the same
			// key still authorizes normally once the interference ends, so the
			// pre-issuance failure consumed no attempt.
			stop()
			tx, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := a.authorize(f.ctx, f.ctx, tx, e, &inverseSendGate{})
			tx.Rollback(context.Background())
			if err != nil {
				t.Fatalf("%s left the key unissuable: %v", name, err)
			}
			authority.cancel()
			if got := f.attempts(t, e.Key); got != 1 {
				t.Fatalf("%s left %d durable attempts before a single authorization", name, got)
			}
		})
	}
}

// A chain of known lock contention that exhausts its retry bound must not lose
// PostgreSQL's own cause: publication has to see a real lock loss to retry or
// to hold a confirmed absence, never a bare "admission stayed busy".
func TestR2InverseExhaustedContentionKeepsItsTransientCause(t *testing.T) {
	// The real bound is 200 attempts; only the count is shortened here so the
	// same exhaustion branch runs quickly.
	previous := inverseAdmissionRetries
	inverseAdmissionRetries = 3
	t.Cleanup(func() { inverseAdmissionRetries = previous })

	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry(t)
	if _, err := a.reserve(f.ctx, e); err != nil {
		t.Fatal(err)
	}
	// Real contention after the wide locks, on a statement no NOWAIT covers:
	// publication's retirement insert needs ROW EXCLUSIVE, which the holder's
	// SHARE refuses. The transaction-local bound turns each wait into 55P03, so
	// the unnamed contention path runs and reaches its retry bound.
	holder, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(context.Background())
	if _, err = holder.Exec(f.ctx, `LOCK TABLE r2_object_retirements IN SHARE MODE`); err != nil {
		t.Fatal(err)
	}
	err = a.publishAbsent(f.ctx, e, a.opts.ManifestSHA)
	if err == nil {
		t.Fatal("publication succeeded while its retirement table was held")
	}
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != pgLockNotAvailable {
		t.Fatalf("exhausted contention lost its PostgreSQL lock cause: %v", err)
	}
	if !errors.Is(err, errInverseAdmissionBusy) {
		t.Fatalf("exhausted contention did not name its retry bound: %v", err)
	}
	if !inverseDBTransient(f.ctx, f.ctx, err) {
		t.Fatalf("a real lock loss after the retry bound was not transient: %v", err)
	}
	if held := inverseHoldUnpublished(f.ctx, f.ctx, err); !errors.Is(held, errInverseUnpublished) {
		t.Fatalf("a real lock loss after the retry bound did not hold the confirmed absence: %v", held)
	}
	// A generic error is never a safe retry, however the step ended.
	for _, unknown := range []error{errors.New("unknown publication failure"), context.Canceled} {
		if inverseDBTransient(f.ctx, f.ctx, unknown) || errors.Is(inverseHoldUnpublished(f.ctx, f.ctx, unknown), errInverseUnpublished) {
			t.Fatalf("an unknown failure was treated as safe retry: %v", unknown)
		}
	}
	// The caller's parent ending is a stopped job, never a held key.
	parent, cancelParent := context.WithCancel(f.ctx)
	cancelParent()
	if inverseDBTransient(parent, f.ctx, err) || errors.Is(inverseHoldUnpublished(parent, f.ctx, err), errInverseUnpublished) {
		t.Fatal("a cancelled parent held a key instead of stopping the job")
	}
	// With the holder gone the same publication succeeds, so nothing was
	// consumed or poisoned while the contention exhausted.
	if err = holder.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err = a.publishConfirmed(f.ctx, e); err != nil {
		t.Fatalf("publication did not recover after the contention ended: %v", err)
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
	// publication insert.
	if _, err := f.pool.Exec(f.ctx, `CREATE FUNCTION inverse_publish_stitch_busy() RETURNS trigger LANGUAGE plpgsql AS $f$
 BEGIN RAISE EXCEPTION 'stitch input hold admission is busy' USING ERRCODE='55P03'; END $f$;
 CREATE TRIGGER inverse_publish_stitch_busy BEFORE INSERT ON r2_keep28_events
 FOR EACH ROW EXECUTE FUNCTION inverse_publish_stitch_busy();
 CREATE SEQUENCE inverse_publish_stitch_tries`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.pool.Exec(f.ctx, `DROP TRIGGER inverse_publish_stitch_busy ON r2_keep28_events;
 DROP FUNCTION inverse_publish_stitch_busy(); DROP SEQUENCE inverse_publish_stitch_tries`)
	})
	// Count the inserts so a transient path would show more than one try.
	if _, err := f.pool.Exec(f.ctx, `CREATE OR REPLACE FUNCTION inverse_publish_stitch_busy() RETURNS trigger LANGUAGE plpgsql AS $f$
 BEGIN PERFORM nextval('inverse_publish_stitch_tries');
  RAISE EXCEPTION 'stitch input hold admission is busy' USING ERRCODE='55P03'; END $f$`); err != nil {
		t.Fatal(err)
	}
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
