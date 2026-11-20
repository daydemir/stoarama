package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const stitchKeyLockMigration = "0223_r2_stitch_key_locks.sql"

func stitchBackendPID(t *testing.T, tx pgx.Tx) uint32 {
	t.Helper()
	var pid uint32
	if err := tx.QueryRow(context.Background(), `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	return pid
}

func waitBlockedBy(t *testing.T, pool *pgxpool.Pool, holder uint32, what string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		var waiting bool
		err := pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, holder).Scan(&waiting)
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("%s never waited: %v", what, err)
		}
		if waiting {
			return
		}
	}
}

func within[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(15 * time.Second):
		t.Fatalf("%s did not finish", what)
		panic("unreachable")
	}
}

// stitchCutoverFixture migrates to just before 0223 and stores one held-able
// RAW source. It returns the pool and the full migration directory.
func stitchCutoverFixture(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "stitch_cutover")
	full := joinedPurgeMigrationsDir()
	before := t.TempDir()
	entries, err := os.ReadDir(full)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() >= stitchKeyLockMigration {
			continue
		}
		body, err := os.ReadFile(filepath.Join(full, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(before, entry.Name()), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.MigrateUp(ctx, pool, before); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO accounts(id,email) VALUES(47,'cutover@test.invalid');
 INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(1,47,'cutover','fixture');
 INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',1);
 INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES(3,'test','3','cutover','cutover','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc) VALUES(1,47,'test','https://test.invalid','auto','stoarama','fixture',decode('00','hex'));
 INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at) VALUES(30,47,1,'cutover','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now());
 INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path) VALUES(9000001,30,1,'https://test.invalid','stoarama','managed/acct-47/cutover.mp4',100,'etag',repeat('b',64),'2026-06-01','2026-06-01','2026-06-01 00:01','cutover.mp4');
 INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at) VALUES(13,9000001,30,'cutover.mp4',100,repeat('b',64),'present',now(),now());
 INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until) VALUES(70,3,'2026-06-01','2026-06-02','cutover',now()+interval '1 day');
 INSERT INTO nas_stitch_r2_hours(scope_id,recording_id,hour_at,plan,plan_sha256,input_bytes) VALUES(70,30,'2026-06-01','{}',repeat('f',64),100)`); err != nil {
		t.Fatal(err)
	}
	return pool
}

const (
	stitchCutoverHold = `INSERT INTO nas_stitch_r2_input_holds(hour_id,bucket,object_key,catalog_clip_id,sha256,size_bytes,etag)
 SELECT h.id,c.bucket,c.object_key,c.id,repeat('b',64),100,c.etag FROM nas_stitch_r2_hours h,recording_clips c WHERE c.id=9000001`
	stitchCutoverReservation = `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 VALUES('raw',9000001,'stoarama','managed/acct-47/cutover.mp4',100,repeat('b',64),repeat('a',64))`
)

// Old binaries and in-flight transactions straddle the deploy. Each side of the
// stitch/retirement handshake must still see the other across 0223.
func TestR2StitchKeyLockCutoverKeepsHoldsOrdered(t *testing.T) {
	ctx := context.Background()
	t.Run("hold_written_by_the_old_trigger", func(t *testing.T) {
		pool := stitchCutoverFixture(t)
		hold, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer hold.Rollback(ctx)
		if _, err = hold.Exec(ctx, stitchCutoverHold); err != nil {
			t.Fatal(err)
		}
		migrated := make(chan error, 1)
		go func() { migrated <- db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()) }()
		waitBlockedBy(t, pool, stitchBackendPID(t, hold), "0223 behind an in-flight old hold")
		if err = hold.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = within(t, migrated, "0223"); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, stitchCutoverReservation); err == nil || !strings.Contains(err.Error(), "active R2 stitch input hold") {
			t.Fatalf("new ledger trigger crossed a hold written under the old trigger: %v", err)
		}
	})
	t.Run("ledger_written_by_the_old_trigger", func(t *testing.T) {
		pool := stitchCutoverFixture(t)
		ledger, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer ledger.Rollback(ctx)
		if _, err = ledger.Exec(ctx, stitchCutoverReservation); err != nil {
			t.Fatal(err)
		}
		if err = db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
			t.Fatal(err)
		}
		admitted := make(chan error, 1)
		go func() { _, err := pool.Exec(ctx, stitchCutoverHold); admitted <- err }()
		waitBlockedBy(t, pool, stitchBackendPID(t, ledger), "new hold admission behind an in-flight old ledger write")
		if err = ledger.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if err = within(t, admitted, "hold admission"); err == nil || !strings.Contains(err.Error(), "already has R2 retirement history") {
			t.Fatalf("new hold admission crossed a ledger row written under the old trigger: %v", err)
		}
	})
}

// keep28Hold stores a pending Durham hour covering the fixture's clips and
// returns an open transaction that has admitted the hold for key.
func keep28Hold(t *testing.T, f keep28Fixture, key string) pgx.Tx {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until)
 SELECT 70,3,date_trunc('hour',clip_start_at,'UTC'),date_trunc('hour',clip_start_at,'UTC')+interval '3 hours','keep28 hold',now()+interval '1 day' FROM recording_clips WHERE id=9000001;
 INSERT INTO nas_stitch_r2_hours(scope_id,recording_id,hour_at,plan,plan_sha256,input_bytes)
 SELECT 70,30,date_trunc('hour',clip_start_at,'UTC'),'{}',repeat('f',64),100 FROM recording_clips WHERE id=9000001`); err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback(context.Background()) })
	if _, err = tx.Exec(f.ctx, `INSERT INTO nas_stitch_r2_input_holds(hour_id,bucket,object_key,catalog_clip_id,sha256,size_bytes,etag)
 SELECT h.id,c.bucket,c.object_key,c.id,lower(c.sha256),c.size_bytes,c.etag FROM nas_stitch_r2_hours h,recording_clips c WHERE c.object_key=$1`, key); err != nil {
		t.Fatal(err)
	}
	return tx
}

func keep28Durable(t *testing.T, f keep28Fixture, key string) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_retirement_reservations WHERE object_key=$1)
 +(SELECT count(*) FROM r2_keep28_intents WHERE object_key=$1)+(SELECT count(*) FROM r2_keep28_attempts WHERE object_key=$1)`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	ledger, err := os.ReadFile(f.options.Ledger)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if strings.Contains(string(ledger), key) {
		n++
	}
	return n
}

// A held key, first in custody order, never stops or fills the batch; the
// eligible key behind it is removed. A committed hold is left out at selection,
// an open import's hold is skipped at reserve.
func TestKeep28SkipsDurhamHeldKeysBeforeIntent(t *testing.T) {
	for _, committed := range []bool{true, false} {
		t.Run(map[bool]string{true: "committed_hold", false: "open_import"}[committed], func(t *testing.T) {
			f := newKeep28FixtureCount(t, 2)
			held := "managed/acct-47/keep28/2.mp4"
			f.store.mu.Lock()
			object := f.store.objects[held]
			f.store.mu.Unlock()
			file, err := os.OpenFile(f.options.Manifest, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.NewEncoder(file).Encode(inverseEntry{ObjectInfo: object, Class: inverseKeep28}); err != nil {
				t.Fatal(err)
			}
			file.Close()
			f.options.ManifestSHA, _ = inverseFileSHA(f.options.Manifest)
			hold := keep28Hold(t, f, held)
			var skipped int64 = 1
			if committed {
				if err = hold.Commit(f.ctx); err != nil {
					t.Fatal(err)
				}
				f.options.MaxItems, skipped = 1, 0
			}
			result, err := applyInverse(f.ctx, f.pool, f.sdk, f.options)
			if err != nil || result.Deleted != 1 || result.StitchSkipped != skipped || f.deletes() != 1 {
				t.Fatalf("held key stopped or filled the batch: %+v %v deletes %d", result, err, f.deletes())
			}
			f.store.mu.Lock()
			_, heldPresent := f.store.objects[held]
			_, eligiblePresent := f.store.objects[f.key]
			f.store.mu.Unlock()
			if !heldPresent || eligiblePresent || keep28Durable(t, f, held) != 0 {
				t.Fatalf("held key was touched or eligible key kept: held %v eligible %v", heldPresent, eligiblePresent)
			}
		})
	}
}

// keep28HeadHook runs once, at the wire HEAD after the durable intent.
type keep28HeadHook struct {
	*inverseSDKStore
	hook func()
}

func (s *keep28HeadHook) Head(ctx context.Context, key string) (r2.ObjectHead, error) {
	if s.hook != nil {
		s.hook()
		s.hook = nil
	}
	return s.inverseSDKStore.Head(ctx, key)
}

// After the intent, the same stitch outcome is no longer a skip: it stops the
// batch and the key stays held for reconciliation.
func TestKeep28PostIntentStitchConflictHalts(t *testing.T) {
	f := newKeep28Fixture(t)
	admission, err := f.pool.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Release()
	defer admission.Exec(context.Background(), `SELECT pg_advisory_unlock_all()`)
	store := &keep28HeadHook{inverseSDKStore: f.sdk, hook: func() {
		if _, err := admission.Exec(f.ctx, `SELECT pg_advisory_lock(nas_stitch_r2_hold_lock('stoarama',$1))`, f.key); err != nil {
			t.Error(err)
		}
	}}
	result, err := applyInverse(f.ctx, f.pool, store, f.options)
	if err == nil || !strings.Contains(err.Error(), "hold admission is busy") || errors.Is(err, errInverseStitchConflict) ||
		result.Deleted != 0 || result.StitchSkipped != 0 || f.deletes() != 0 {
		t.Fatalf("post-intent stitch conflict did not stop the batch: %+v %v", result, err)
	}
	var intents int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_keep28_intents WHERE object_key=$1`, f.key).Scan(&intents); err != nil || intents != 1 {
		t.Fatalf("post-intent key lost its durable intent: %d %v", intents, err)
	}
}

// Keep-28 reserve locks the NAS custody row after the protection locks. A busy
// custody row is awaited with no protection lock held.
func TestKeep28ReserveAwaitsCustodyWithoutProtectionLocks(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	publication, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer publication.Rollback(f.ctx)
	if _, err = publication.Exec(f.ctx, `SELECT 1 FROM nas_inventory_files WHERE clip_id=9000001 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	reserved := make(chan error, 1)
	go func() { _, err := a.reserve(f.ctx, f.entry(t)); reserved <- err }()
	waitBlockedBy(t, f.pool, stitchBackendPID(t, publication), "keep-28 reserve behind its custody row")
	mutator, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer mutator.Rollback(f.ctx)
	prompt, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	if _, err = mutator.Exec(prompt, `SELECT pg_advisory_xact_lock(7343226011103),pg_advisory_xact_lock(7343226011101)`); err != nil {
		t.Fatalf("legacy mutator queued behind a reserve awaiting custody: %v", err)
	}
	mutator.Rollback(f.ctx)
	publication.Rollback(f.ctx)
	if err = within(t, reserved, "keep-28 reserve behind its custody row"); err != nil || keep28Durable(t, f, f.key) != 2 {
		t.Fatalf("keep-28 reserve did not complete after custody freed: %v", err)
	}
}

// Only 0223's exact stitch outcomes skip a key; another error with the same
// SQLSTATE stops the batch.
func TestR2InverseStitchLedgerIdentityIsExact(t *testing.T) {
	for _, c := range []struct {
		err  pgconn.PgError
		skip bool
	}{
		{pgconn.PgError{Code: "55P03", Message: "stitch input hold admission is busy"}, true},
		{pgconn.PgError{Code: "55000", Message: "active R2 stitch input hold"}, true},
		{pgconn.PgError{Code: "55P03", Message: "canceling statement due to lock timeout"}, false},
		{pgconn.PgError{Code: "55000", Message: "R2 retirement ledger requires READ COMMITTED"}, false},
		{pgconn.PgError{Code: "55000", Message: "stitch input hold admission is busy"}, false},
	} {
		if got := errors.Is(inverseStitchLedgerError(&c.err), errInverseStitchConflict); got != c.skip {
			t.Fatalf("%s %q skipped=%v", c.err.Code, c.err.Message, got)
		}
	}
}

// Keep-28 reserve takes no lock on the parent recording: a busy recording row
// neither stops nor delays it.
func TestKeep28ReserveIgnoresBusyParentRecording(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	parent, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Rollback(f.ctx)
	if _, err = parent.Exec(f.ctx, `SELECT 1 FROM recordings WHERE id=30 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	prompt, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	if _, err = a.reserve(prompt, f.entry(t)); err != nil || keep28Durable(t, f, f.key) != 2 {
		t.Fatalf("keep-28 reserve waited on or refused its busy parent recording: %v", err)
	}
}

// probeWideLocks reports whether a fresh transaction can take both wide
// protection locks right now, without queueing behind the caller.
func probeWideLocks(t *testing.T, pool *pgxpool.Pool) bool {
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

// The writer fence is read after the wide protection locks. Admission only
// tries it: a busy fence row is awaited holding nothing, then revalidated, so
// a delivery hold that needs 7343226011103 is never queued behind admission.
func TestR2AdmissionAwaitsWriterFenceOutsideWideLocks(t *testing.T) {
	f := newKeep28Fixture(t)
	a := f.applier(t)
	e := f.entry(t)
	var bucket, endpoint string
	if err := f.pool.QueryRow(f.ctx, `SELECT bucket,endpoint_sha256 FROM r2_retirement_writer_fences`).Scan(&bucket, &endpoint); err != nil {
		t.Fatal(err)
	}
	holdFence := func(t *testing.T) pgx.Tx {
		t.Helper()
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, `SELECT 1 FROM r2_retirement_writer_fences WHERE bucket=$1 AND endpoint_sha256=$2 FOR UPDATE`, bucket, endpoint); err != nil {
			t.Fatal(err)
		}
		return tx
	}

	fence := holdFence(t)
	defer fence.Rollback(f.ctx)
	reserved := make(chan error, 1)
	go func() { _, err := a.reserve(f.ctx, e); reserved <- err }()
	waitBlockedBy(t, f.pool, stitchBackendPID(t, fence), "reserve behind the writer fence")
	if !probeWideLocks(t, f.pool) {
		t.Fatal("reserve kept the wide protection locks while awaiting the writer fence")
	}
	if err := fence.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := within(t, reserved, "reserve behind the writer fence"); err != nil {
		t.Fatalf("reserve did not revalidate after the writer fence: %v", err)
	}

	fence = holdFence(t)
	defer fence.Rollback(f.ctx)
	wiring := make(chan error, 1)
	go func() {
		tx, _, err := admitWire(f.ctx, f.pool, func(tx pgx.Tx) (*inverseDeleteAuthority, error) {
			return a.authorize(f.ctx, tx, e, &inverseSendGate{})
		})
		if tx != nil {
			tx.Rollback(context.Background())
		}
		wiring <- err
	}()
	waitBlockedBy(t, f.pool, stitchBackendPID(t, fence), "wire behind the writer fence")
	if !probeWideLocks(t, f.pool) {
		t.Fatal("wire kept the wide protection locks while awaiting the writer fence")
	}
	if err := fence.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := within(t, wiring, "wire behind the writer fence"); err != nil {
		t.Fatalf("wire did not revalidate after the writer fence: %v", err)
	}
}

// Admission retries are bounded: an admission that keeps meeting a busy lock
// must fail with the named bound instead of looping, and a key whose own
// deadline expires first must report both that bound and the deadline.
func TestR2AdmissionRetriesAreBounded(t *testing.T) {
	f := newKeep28Fixture(t)
	// Admission that keeps reporting the same (freely lockable) wait never
	// settles; only the named cap ends it.
	attempts := 0
	_, err := admitInverse(f.ctx, inverseBegin(f.pool), func() (struct{}, error) {
		attempts++
		return struct{}{}, inverseBusy{{sql: `SELECT pg_advisory_xact_lock(7343226011000)`}}
	})
	if !errors.Is(err, errInverseAdmissionBusy) || attempts != inverseAdmissionRetries+1 {
		t.Fatalf("retry bound: attempts %d err %v", attempts, err)
	}

	a := f.applier(t)
	e := f.entry(t)
	owner, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Rollback(f.ctx)
	if _, err = owner.Exec(f.ctx, `SELECT lock_nas_r2_copy_key('stoarama',$1)`, f.key); err != nil {
		t.Fatal(err)
	}
	prompt, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()
	started := time.Now()
	if _, err = a.reserve(prompt, e); !errors.Is(err, errInverseAdmissionBusy) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired admission did not name its bound and deadline: %v", err)
	}
	if time.Since(started) > 15*time.Second {
		t.Fatalf("expired admission overran its deadline: %v", time.Since(started))
	}
	if !probeWideLocks(t, f.pool) {
		t.Fatal("expired admission left the wide protection locks held")
	}
}
