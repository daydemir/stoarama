package api

import (
	"context"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Exercise the actual migrated tables, FKs, checks, ordinary update/insert
// guards, fence admission and deferred seal validation. No copied tables,
// disabled triggers, replication role or fabricated claim is used.
func newR2JoinedRetirementFixture(t *testing.T, email string) (joinedHistoricalTier1Fixture, int64, int64, int64) {
	t.Helper()
	return newR2JoinedRetirementFixtureAtPhase(t, email, joinedFixtureAllMigrations)
}

func newR2JoinedRetirementFixtureAtPhase(t *testing.T, email string, phase joinedFixtureMigrationPhase) (joinedHistoricalTier1Fixture, int64, int64, int64) {
	t.Helper()
	ctx := context.Background()
	fixture := newJoinedHistoricalTier1FixtureAtMigrationPhase(t, email, true, func(pool *pgxpool.Pool) {
		for _, q := range []string{
			`SELECT setval(pg_get_serial_sequence('accounts','id'),46)`,
			`SELECT setval(pg_get_serial_sequence('connections','id'),12)`,
			// Explicit old creation defaults produce valid historical pending/sealed
			// fixtures while retaining every real row invariant and lifecycle guard.
			`ALTER TABLE recording_joined_hours ALTER COLUMN updated_at SET DEFAULT now()-interval '1 day'`,
			`ALTER TABLE recording_joined_artifacts ALTER COLUMN updated_at SET DEFAULT now()-interval '1 day'`,
		} {
			if _, err := pool.Exec(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
	}, phase)
	pool := fixture.pool
	if fixture.accountID != 47 || fixture.connectionID != 13 {
		t.Fatalf("scope %d/%d", fixture.accountID, fixture.connectionID)
	}
	req := fixture.req
	req.Apply = true
	req.ExpectedRequestSHA256 = fixture.plan.RequestSHA256
	if response, _ := fixture.call(req); response.Code != 200 {
		t.Fatalf("freeze %d %s", response.Code, response.Body.String())
	}
	finishJoinedTier1Fixture(t, fixture, req)
	var batch int64
	if err := pool.QueryRow(ctx, `SELECT id FROM recording_joined_batches WHERE batch_id=$1`, req.BatchID).Scan(&batch); err != nil {
		t.Fatal(err)
	}
	_, _, _, final := materializeJoinedCanonicalBatch(t, fixture, batch)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = final(tx); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var hour, artifact int64
	if err = pool.QueryRow(ctx, `SELECT min(id) FROM recording_joined_hours WHERE batch_record_id=$1 AND source_clip_count>0`, batch).Scan(&hour); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT min(id) FROM recording_joined_artifacts WHERE batch_record_id=$1 AND publication_state='sealed'`, batch).Scan(&artifact); err != nil {
		t.Fatal(err)
	}
	return fixture, batch, hour, artifact
}

func TestR2JoinedWriterRetirementRealSchema(t *testing.T) {
	ctx := context.Background()
	fixture, batch, hour, artifact := newR2JoinedRetirementFixture(t, "r2-joined-retirement@example.test")
	defer fixture.cleanup()
	pool := fixture.pool
	var tx pgx.Tx
	var err error

	snapshot := func(q interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}) string {
		t.Helper()
		var sha string
		if err := q.QueryRow(ctx, `SELECT r2_joined_writer_snapshot($1)`, batch).Scan(&sha); err != nil {
			t.Fatal(err)
		}
		return sha
	}
	original := snapshot(pool)
	digest := strings.Repeat("a", 64)
	// Before a fence, even a correctly bound run cannot authorize cancellation.
	if _, err = pool.Exec(ctx, `INSERT INTO r2_joined_writer_retirement_runs(id,batch_record_id,snapshot_sha256,proof_sha256,reviewer_reference,owner_evidence) VALUES($1,$2,$3,$4,'independent-test','[]')`, uuid.New(), batch, original, digest); err == nil {
		t.Fatal("unfenced audit admitted")
	}
	// Real live publication/hour claims are vetoed. Rollback leaves no history
	// or fake ownership in the fixture, then activate the immutable fence.
	for _, q := range []string{
		`UPDATE recording_joined_hours SET state='leased',attempt_count=1,claim_token=gen_random_uuid(),claimed_by='live-owner',lease_expires_at=now()+interval '1 hour',heartbeat_at=now() WHERE id=$1`,
		`UPDATE recording_joined_artifacts SET publication_state='publishing',publication_attempt_count=1,publication_token=gen_random_uuid(),publication_claimed_by='live-owner',publication_lease_expires_at=now()+interval '1 hour',publication_heartbeat_at=now() WHERE id=$1`,
	} {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		id := hour
		if strings.Contains(q, "artifacts") {
			id = artifact
		}
		if _, err = tx.Exec(ctx, q, id); err != nil {
			tx.Rollback(ctx)
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, digest); err != nil {
			tx.Rollback(ctx)
			t.Fatal(err)
		}
		current := snapshot(tx)
		_, err = tx.Exec(ctx, `SELECT r2_retire_dead_joined_batch($1,$2,$3,$4,'independent-test','[]')`, uuid.New(), batch, current, digest)
		if err == nil || !strings.Contains(err.Error(), "live/recent") {
			tx.Rollback(ctx)
			t.Fatalf("live claim accepted: %v", err)
		}
		tx.Rollback(ctx)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_joined_hours SET state='terminal_failed',failure_reason_code='owner_retired' WHERE id=$1`, hour); err == nil {
		t.Fatal("unaudited child cancellation admitted")
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='terminal_failed',failure_reason_code='owner_retired' WHERE id=$1`, artifact); err == nil {
		t.Fatal("unaudited artifact cancellation admitted")
	}
	// Wrong snapshots/owner rosters cannot create audit authority by INSERT.
	for _, tc := range []struct{ sha, proof string }{{strings.Repeat("0", 64), `[]`}, {original, `[{"owner":"invented","kind":"do_destroyed","resource_id":"123","evidence_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","observed_at":"2026-01-01T00:00:00Z"}]`}} {
		if _, err = pool.Exec(ctx, `INSERT INTO r2_joined_writer_retirement_runs(id,batch_record_id,snapshot_sha256,proof_sha256,reviewer_reference,owner_evidence) VALUES($1,$2,$3,$4,'independent-test',$5::jsonb)`, uuid.New(), batch, tc.sha, digest, tc.proof); err == nil {
			t.Fatal("forged run accepted")
		}
	}
	// A valid run and one exact authorized child still cannot COMMIT alone.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run := uuid.New()
	if _, err = tx.Exec(ctx, `INSERT INTO r2_joined_writer_retirement_runs(id,batch_record_id,snapshot_sha256,proof_sha256,reviewer_reference,owner_evidence) VALUES($1,$2,$3,$4,'independent-test','[]')`, run, batch, original, digest); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO r2_joined_writer_retirement_items SELECT $1,(e->>'kind')::r2_joined_retired_kind,(e->>'record_id')::bigint,e->>'before_sha256',e->>'after_sha256' FROM r2_joined_writer_retirement_runs r CROSS JOIN LATERAL jsonb_array_elements(r.expected_rows) e WHERE r.id=$1 AND e->>'kind'='hour' AND (e->>'record_id')::bigint=$2`, run, hour); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE recording_joined_hours SET state='terminal_failed',failure_reason_code='owner_retired',updated_at=transaction_timestamp() WHERE id=$1`, hour); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err == nil {
		t.Fatal("partial audit committed")
	}
	if snapshot(pool) != original {
		t.Fatal("failed commit left partial transition")
	}
	var rows int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM r2_joined_writer_retirement_runs`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("partial audit survived %d %v", rows, err)
	}
	// Transaction rollback after the complete operation also leaves no changes.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT r2_retire_dead_joined_batch($1,$2,$3,$4,'independent-test','[]')`, uuid.New(), batch, original, digest).Scan(&count); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	tx.Rollback(ctx)
	if snapshot(pool) != original {
		t.Fatal("rollback changed snapshot")
	}
	// Checking a deferred constraint early must not let later mutations diverge
	// from the audited after-image before COMMIT.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.QueryRow(ctx, `SELECT r2_retire_dead_joined_batch($1,$2,$3,$4,'independent-test','[]')`, uuid.New(), batch, original, digest).Scan(&count); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `SET CONSTRAINTS r2_joined_writer_run_complete IMMEDIATE`); err != nil {
		tx.Rollback(ctx)
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=false,updated_at=clock_timestamp() WHERE batch_record_id=$1`, batch)
	tx.Rollback(ctx)
	if err == nil {
		t.Fatal("early constraint validation allowed conflicting admission mutation")
	}
	if err = pool.QueryRow(ctx, `SELECT r2_retire_dead_joined_batch($1,$2,$3,$4,'independent-test','[]')`, uuid.New(), batch, original, digest).Scan(&count); err != nil {
		t.Fatal(err)
	}
	var blocked, audited int
	var paused bool
	var state string
	if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_joined_hours WHERE batch_record_id=$1 AND state IN ('pending','leased'))+(SELECT count(*) FROM recording_joined_artifacts WHERE batch_record_id=$1 AND publication_state IN ('sealed','publishing')),(SELECT count(*) FROM r2_joined_writer_retirement_items),(SELECT claims_paused FROM recording_joined_admission_controls WHERE batch_record_id=$1),(SELECT state FROM recording_joined_batches WHERE id=$1)`, batch).Scan(&blocked, &audited, &paused, &state); err != nil || blocked != 0 || count != audited || !paused || state != "terminal_failed" {
		t.Fatalf("atomic result blocked=%d count=%d audit=%d paused=%v state=%s err=%v", blocked, count, audited, paused, state, err)
	}
	if _, err = pool.Exec(ctx, `UPDATE recording_joined_admission_controls SET claims_paused=false,updated_at=clock_timestamp() WHERE batch_record_id=$1`, batch); err == nil {
		t.Fatal("committed retired admission reopened")
	}
	if _, err = pool.Exec(ctx, `DELETE FROM r2_joined_writer_retirement_items`); err == nil {
		t.Fatal("audit mutable")
	}
	if _, err = pool.Exec(ctx, `SELECT r2_retire_dead_joined_batch($1,$2,$3,$4,'independent-test','[]')`, uuid.New(), batch, original, digest); err == nil {
		t.Fatal("replay admitted")
	}
}

func TestR2JoinedWriterRetirementRejectsLostHistoricalOwners(t *testing.T) {
	ctx := context.Background()
	fixture, batch, hour, _ := newR2JoinedRetirementFixture(t, "r2-joined-history@example.test")
	defer fixture.cleanup()
	pool := fixture.pool
	// Follow the actual claim/reclaim lifecycle. The original A claim commits,
	// expires, and is overwritten by B through the unchanged production guard.
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='leased',attempt_count=1,claim_token=gen_random_uuid(),claimed_by='owner-A',lease_expires_at=now()+interval '1 millisecond',heartbeat_at=clock_timestamp() WHERE id=$1`, hour); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT pg_sleep(0.02)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET attempt_count=2,claim_token=gen_random_uuid(),claimed_by='owner-B',lease_expires_at=now()+interval '1 hour',heartbeat_at=clock_timestamp() WHERE id=$1`, hour); err != nil {
		t.Fatal(err)
	}
	var complete bool
	var owner string
	if err := pool.QueryRow(ctx, `SELECT r2_joined_writer_owner_inventory_complete($1),(SELECT claimed_by FROM recording_joined_hours WHERE id=$2)`, batch, hour).Scan(&complete, &owner); err != nil || complete || owner != "owner-B" {
		t.Fatalf("lost A owner was treated as covered complete=%v owner=%s err=%v", complete, owner, err)
	}
}

func TestR2JoinedWriterRetirementPreservesPublicationAttempts(t *testing.T) {
	ctx := context.Background()
	fixture, batch, _, artifact := newR2JoinedRetirementFixture(t, "r2-joined-attempts@example.test")
	defer fixture.cleanup()
	pool := fixture.pool
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='publishing',publication_attempt_count=1,publication_token=gen_random_uuid(),publication_claimed_by='owner-A',publication_lease_expires_at=now()+interval '1 hour',publication_heartbeat_at=now() WHERE id=$1`, artifact); err != nil {
		t.Fatal(err)
	}
	for _, attempts := range []int{0, 2} {
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='terminal_failed',publication_attempt_count=$2,failure_reason_code='worker_failed',publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL WHERE id=$1`, artifact, attempts); err == nil {
			t.Fatalf("terminal failure rewrote publication attempts to %d", attempts)
		}
	}
	// The ordinary live-worker failure remains supported, retaining its history.
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='terminal_failed',failure_reason_code='worker_failed',publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL WHERE id=$1`, artifact); err != nil {
		t.Fatal(err)
	}
	var complete bool
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT r2_joined_writer_owner_inventory_complete($1),(SELECT publication_attempt_count FROM recording_joined_artifacts WHERE id=$2)`, batch, artifact).Scan(&complete, &attempts); err != nil || complete || attempts != 1 {
		t.Fatalf("terminal owner history complete=%v attempts=%d err=%v", complete, attempts, err)
	}
}

func TestR2JoinedWriterRetirementRejectsLegacyErasedPublicationOwner(t *testing.T) {
	ctx := context.Background()
	// Create the bad state through the actual pre-0195 guard, commit it, then
	// apply 0195. No trigger is disabled or removed to manufacture this history.
	fixture, batch, _, artifact := newR2JoinedRetirementFixtureAtPhase(t, "r2-joined-legacy-attempts@example.test", joinedFixtureBeforeWriterRetirement)
	defer fixture.cleanup()
	pool := fixture.pool
	for _, query := range []string{
		`UPDATE recording_joined_artifacts SET publication_state='publishing',publication_attempt_count=1,publication_token=gen_random_uuid(),publication_claimed_by='lost-owner',publication_lease_expires_at=now()+interval '1 hour',publication_heartbeat_at=now() WHERE id=$1`,
		`UPDATE recording_joined_artifacts SET publication_state='terminal_failed',publication_attempt_count=0,failure_reason_code='worker_failed',publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL WHERE id=$1`,
	} {
		if _, err := pool.Exec(ctx, query, artifact); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version='0195_r2_joined_writer_retirement.sql'`); err != nil {
		t.Fatal(err)
	}
	if err := db.MigrateUp(ctx, pool, findMigrationsDir(t)); err != nil {
		t.Fatal(err)
	}
	var complete bool
	if err := pool.QueryRow(ctx, `SELECT r2_joined_writer_owner_inventory_complete($1)`, batch).Scan(&complete); err != nil || complete {
		t.Fatalf("legacy erased owner accepted complete=%v err=%v", complete, err)
	}
	digest := strings.Repeat("a", 64)
	if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, digest); err != nil {
		t.Fatal(err)
	}
	var snapshot string
	if err := pool.QueryRow(ctx, `SELECT r2_joined_writer_snapshot($1)`, batch).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`SELECT r2_retire_dead_joined_batch($1,$2,$3,$4,'independent-test','[]')`,
		`INSERT INTO r2_joined_writer_retirement_runs(id,batch_record_id,snapshot_sha256,proof_sha256,reviewer_reference,owner_evidence) VALUES($1,$2,$3,$4,'independent-test','[]')`,
	} {
		if _, err := pool.Exec(ctx, query, uuid.New(), batch, snapshot, digest); err == nil || !strings.Contains(err.Error(), "historical joined owner inventory is incomplete") {
			t.Fatalf("legacy owner loss bypassed audit admission: %v", err)
		}
	}
	var after string
	var runs int
	if err := pool.QueryRow(ctx, `SELECT r2_joined_writer_snapshot($1),(SELECT count(*) FROM r2_joined_writer_retirement_runs)`, batch).Scan(&after, &runs); err != nil || after != snapshot || runs != 0 {
		t.Fatalf("rejected legacy retirement changed rows snapshot=%s runs=%d err=%v", after, runs, err)
	}
}
