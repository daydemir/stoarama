package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// tryWideLocks reports whether a fresh session can hold both wide cleanup locks
// right now. Every retire-side transaction takes them before any other lock, so
// this is the exact claim another writer needs.
func tryWideLocks(t *testing.T, pool *pgxpool.Pool) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var ok bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(7343226011103) AND pg_try_advisory_xact_lock(7343226011101)`).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

// wideLocksFreeWithin reports whether another session could take the wide
// cleanup locks at some point inside limit.
func wideLocksFreeWithin(t *testing.T, pool *pgxpool.Pool, limit time.Duration) bool {
	t.Helper()
	for deadline := time.Now().Add(limit); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if tryWideLocks(t, pool) {
			return true
		}
	}
	return tryWideLocks(t, pool)
}

// Batch initialization loads the KE path set and the owner keeps once, under
// both wide cleanup locks. Before its own bound, a session holding
// r2_owner_keep_prefixes ACCESS EXCLUSIVE made that read queue with the wide
// locks still held, so no other writer could take them for as long as the
// holder lived. The transaction-local bound turns that into SQLSTATE 55P03, so
// admission rolls back, awaits outside, and retries.
func TestR2InverseApplyInitializationBoundsProtectionReads(t *testing.T) {
	f := newKeep28Fixture(t)
	picks, err := loadR2RetirePicks(f.options.Picks)
	if err != nil {
		t.Fatal(err)
	}
	_, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	holder, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	release := releaseOnce(func() { holder.Rollback(context.Background()) })
	defer release()
	// The owner-keep read is one of the two sets initialization loads.
	if _, err = holder.Exec(f.ctx, `LOCK TABLE r2_owner_keep_prefixes IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	type admitted struct {
		protection inverseProtection
		epoch      int64
		err        error
	}
	done := make(chan admitted, 1)
	go func() {
		var epoch int64
		p, err := loadInverseApplyAdmission(f.ctx, f.pool, f.sdk, picks, windows, &epoch)
		done <- admitted{p, epoch, err}
	}()
	// The held table keeps initialization from finishing: it retries outside
	// the wide locks instead of queueing behind the holder.
	time.Sleep(4 * inverseLockTimeout)
	select {
	case got := <-done:
		t.Fatalf("initialization finished under a held owner-keep table: %+v", got)
	default:
	}
	if !wideLocksFreeWithin(t, f.pool, time.Second) {
		t.Fatal("no writer could take 7343226011103/7343226011101 within 1s while initialization retried behind its table lock")
	}
	release()
	got := within(t, done, "initialization after its holder released")
	if got.err != nil {
		t.Fatalf("initialization did not succeed after its holder released: %v", got.err)
	}
	if got.epoch <= 0 || got.protection.Picks.SHA256 == "" {
		t.Fatalf("initialization returned no usable protection: epoch %d", got.epoch)
	}
}

// releaseOnce makes a holder release safe to call twice: the deferred release
// and the explicit one the bounded case needs.
func releaseOnce(release func()) func() {
	var single sync.Once
	return func() { single.Do(release) }
}

// refreshAdmissionArgument reads back the exact candidate and its protection
// state that one refresh admission needs, so a test can call the admission
// directly instead of through the scheduling cursor.
func refreshAdmissionArgument(t *testing.T, f keep28Fixture, opts finishRefreshOptions) (api.NASR2CopyItem, string, string, int64, inverseProtection) {
	t.Helper()
	var itemJSON []byte
	if err := f.pool.QueryRow(f.ctx, `SELECT item FROM nas_r2_copy_items WHERE connection_id=13 AND kind='raw' AND source_id=9000001`).Scan(&itemJSON); err != nil {
		t.Fatal(err)
	}
	var item api.NASR2CopyItem
	if err := json.Unmarshal(itemJSON, &item); err != nil {
		t.Fatal(err)
	}
	var etag, policy string
	if err := f.pool.QueryRow(f.ctx, `SELECT coalesce(c.etag,''),i.policy_sha256 FROM nas_r2_copy_items i JOIN recording_clips c ON c.id=i.source_id WHERE i.source_id=9000001 AND i.connection_id=13`).Scan(&etag, &policy); err != nil {
		t.Fatal(err)
	}
	var epoch int64
	if err := f.pool.QueryRow(f.ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	picks, err := loadR2RetirePicks(opts.Picks)
	if err != nil {
		t.Fatal(err)
	}
	_, windows, _ := api.Keep28Policy()
	protection, err := loadInverseApplyProtection(f.ctx, f.pool, "stoarama", picks, windows)
	if err != nil {
		t.Fatal(err)
	}
	return item, etag, policy, epoch, protection
}

// unboundedSameStatement is the "remove the SET LOCAL" mutation of one site. It
// takes the same locks in the same order the real transaction takes and then
// runs the site's own conflicting statement with no lock bound. When that
// statement waits, the production path without its bound would hold the wide
// locks for exactly as long, which is the failure the rule removes.
func unboundedSameStatement(t *testing.T, ctx context.Context, pool *pgxpool.Pool, shared bool, stmt string, args []any) (waits bool, stop func()) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	// stop cancels the waiting statement and joins it before rolling back: one
	// transaction cannot take Rollback while its Exec is still in flight.
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("the unbound statement never returned after its cancel")
			}
			tx.Rollback(context.Background())
		})
	}
	mode := ""
	if shared {
		mode = "_shared"
	}
	for _, step := range []string{
		`SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`,
		`SELECT pg_advisory_xact_lock` + mode + `(7343226011103)`,
		`SELECT pg_advisory_xact_lock_shared(137,1)`,
		`SELECT pg_advisory_xact_lock` + mode + `(7343226011101)`,
	} {
		if _, err = tx.Exec(ctx, step); err != nil {
			cancel()
			tx.Rollback(context.Background())
			t.Fatal(err)
		}
	}
	go func() {
		_, err := tx.Exec(runCtx, stmt, args...)
		done <- err
	}()
	select {
	case err := <-done:
		stop()
		if err != nil {
			t.Fatalf("the unbound statement failed instead of waiting: %v", err)
		}
		return false, func() {}
	case <-time.After(3 * inverseLockTimeout):
		return true, stop
	}
}

// lockBoundOperation is one site where a statement could wait on a table or row
// lock while its transaction already holds the wide cleanup locks.
type lockBoundOperation struct {
	name string
	// shared is the mode the site takes the wide locks in.
	shared bool
	// conflicting and its args are that site's own statement.
	conflicting string
	args        []any
	// setup starts the fixture, takes the conflicting lock from another
	// session, and returns the operation to run, that session and the exact
	// arguments its own statement takes.
	setup func(t *testing.T) lockBoundFixture
}

// lockBoundFixture is one prepared site: the operation, the session holding its
// conflicting lock, and the site's own statement arguments.
type lockBoundFixture struct {
	run     func() error
	release func()
	pool    *pgxpool.Pool
	args    []any
}

// One rule replaces the per-statement NOWAIT whack-a-mole: right after a
// transaction takes the wide cleanup locks 7343226011103/7343226011101 it bounds
// every later lock wait, so a busy table or row becomes SQLSTATE 55P03 and
// admission takes its existing contention path instead of queueing under them.
// For each site the test holds the conflicting lock from another session and
// proves all three: another writer can take the wide locks within one second
// while the operation retries, the operation finishes once the holder releases,
// and the same statement reached without the bound waits while it holds them.
func TestR2AdmissionLockBoundNeverWaitsUnderWideLocks(t *testing.T) {
	for _, op := range []lockBoundOperation{
		{
			// A session holding the clip table ACCESS EXCLUSIVE refuses even the
			// ROW SHARE the reserve's locked catalog read needs. No row-level
			// NOWAIT covers that table lock.
			name:        "reserve_recording_clips_table",
			conflicting: inverseRawTargetSQL,
			args:        []any{"stoarama", "managed/acct-47/keep28/1.mp4"},
			setup: func(t *testing.T) lockBoundFixture {
				f := newKeep28Fixture(t)
				a := f.applier(t)
				e := f.entry(t)
				holder, err := f.pool.Begin(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = holder.Exec(f.ctx, `LOCK TABLE recording_clips IN ACCESS EXCLUSIVE MODE`); err != nil {
					t.Fatal(err)
				}
				return lockBoundFixture{
					run:     func() error { _, err := a.reserve(f.ctx, e); return err },
					release: releaseOnce(func() { holder.Rollback(context.Background()) }),
					pool:    f.pool,
				}
			},
		},
		{
			// The purge probe runs an UPDATE of recording_clips after the intent
			// exists. A SHARE holder lets the reserve read and lock its clip row,
			// then refuses the probe's ROW EXCLUSIVE update.
			name:        "reserve_purge_probe",
			conflicting: `UPDATE recording_clips SET purged_at=clock_timestamp() WHERE id=$1`,
			args:        []any{int64(9000001)},
			setup: func(t *testing.T) lockBoundFixture {
				f := newKeep28Fixture(t)
				a := f.applier(t)
				e := f.entry(t)
				holder, err := f.pool.Begin(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = holder.Exec(f.ctx, `LOCK TABLE recording_clips IN SHARE MODE`); err != nil {
					t.Fatal(err)
				}
				return lockBoundFixture{
					run:     func() error { _, err := a.reserve(f.ctx, e); return err },
					release: releaseOnce(func() { holder.Rollback(context.Background()) }),
					pool:    f.pool,
				}
			},
		},
		{
			// Refresh admission takes its copy row, then upgrades it to
			// refresh_pending. A session holding the copy table SHARE makes that
			// UPDATE wait while 1103/1101 are still held.
			name:        "refresh_copy_table",
			conflicting: `UPDATE nas_r2_copy_items SET state='refresh_pending',updated_at=clock_timestamp() WHERE id=$1`,
			args:        []any{int64(1)},
			setup: func(t *testing.T) lockBoundFixture {
				f := newKeep28Fixture(t)
				opts := seedRefreshCandidates(t, f, []int64{100})
				item, etag, policy, epoch, protection := refreshAdmissionArgument(t, f, opts)
				var copyID int64
				if err := f.pool.QueryRow(f.ctx, `SELECT id FROM nas_r2_copy_items WHERE connection_id=13 AND kind='raw' AND source_id=9000001`).Scan(&copyID); err != nil {
					t.Fatal(err)
				}
				holder, err := f.pool.Begin(f.ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = holder.Exec(f.ctx, `LOCK TABLE nas_r2_copy_items IN SHARE MODE`); err != nil {
					t.Fatal(err)
				}
				return lockBoundFixture{
					run: func() error {
						status := finishStatus{}
						ok, err := queueKeep28Refresh(f.ctx, f.pool, opts, protection, epoch, item, etag, policy, &status)
						if err == nil && !ok {
							return errors.New("refresh refused a candidate it had admitted before")
						}
						return err
					},
					release: releaseOnce(func() { holder.Rollback(context.Background()) }),
					pool:    f.pool,
					args:    []any{copyID},
				}
			},
		},
		{
			// Authorize commits the attempt row in its own transaction before it
			// hands out wire authority. A session holding that table SHARE must
			// refuse the insert, not queue it under the wire's shared wide locks.
			name:        "authorize_attempt_table",
			shared:      true,
			conflicting: `INSERT INTO r2_keep28_attempts(bucket,object_key) VALUES($1,$2)`,
			args:        []any{"stoarama", "managed/acct-47/keep28/1.mp4"},
			setup: func(t *testing.T) lockBoundFixture {
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
				if _, err = holder.Exec(f.ctx, `LOCK TABLE r2_keep28_attempts IN SHARE MODE`); err != nil {
					t.Fatal(err)
				}
				return lockBoundFixture{
					run: func() error {
						tx, _, err := admitWire(f.ctx, f.pool, func(ctx context.Context, tx pgx.Tx) (*inverseDeleteAuthority, error) {
							authority, err := a.authorize(ctx, f.ctx, tx, e, &inverseSendGate{})
							if authority != nil {
								authority.cancel()
							}
							return authority, err
						})
						if tx != nil {
							tx.Rollback(context.Background())
						}
						return err
					},
					release: releaseOnce(func() { holder.Rollback(context.Background()) }),
					pool:    f.pool,
				}
			},
		},
	} {
		t.Run(op.name, func(t *testing.T) {
			t.Run("bounded", func(t *testing.T) {
				site := op.setup(t)
				defer site.release()
				done := make(chan error, 1)
				go func() { done <- site.run() }()
				// The held lock keeps the operation from finishing; it retries.
				time.Sleep(4 * inverseLockTimeout)
				select {
				case err := <-done:
					t.Fatalf("the operation finished under a held lock, so the case never reproduced: %v", err)
				default:
				}
				if !wideLocksFreeWithin(t, site.pool, time.Second) {
					t.Fatal("no writer could take 7343226011103/7343226011101 within 1s while the operation retried behind its lock")
				}
				site.release()
				if err := within(t, done, op.name); err != nil {
					t.Fatalf("the operation did not succeed after its holder released: %v", err)
				}
			})
			t.Run("without_the_bound", func(t *testing.T) {
				site := op.setup(t)
				defer site.release()
				// The real operation is not run here: this case isolates the
				// bound. Only the wide locks and the site's own statement are
				// taken, so the wait belongs to exactly that statement.
				waits, stop := unboundedSameStatement(t, context.Background(), site.pool, op.shared, op.conflicting, site.args)
				defer stop()
				if !waits {
					t.Fatal("the same statement without the bound did not wait, so the bound is not what stops the wait")
				}
				if wideLocksFreeWithin(t, site.pool, 500*time.Millisecond) {
					t.Fatal("without the lock bound a writer still took the wide locks, so the bound is not what frees them")
				}
			})
		})
	}
}
