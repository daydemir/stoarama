package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func nasCopyQueueFixture(t *testing.T, count int, apply bool) (*pgxpool.Pool, *pgxpool.Pool, nasCopyHeader, []api.NASR2CopyItem) {
	t.Helper()
	pool := joinedPurgeTestPool(t, "nas_copy_queue")
	ctx := context.Background()
	_, err := pool.Exec(ctx, `CREATE TABLE connections(id bigint PRIMARY KEY);
CREATE TABLE streams(id bigint PRIMARY KEY,local_timezone text);
CREATE TABLE recordings(id bigint PRIMARY KEY,account_id bigint,stream_id bigint,cron_timezone text);
CREATE TABLE recording_clips(id bigint PRIMARY KEY,recording_id bigint,bucket text,object_key text,sha256 text,size_bytes bigint,display_path text,clip_start_at timestamptz,clip_end_at timestamptz,purged_at timestamptz);
CREATE INDEX nas_delivery_raw_writer_path ON recording_clips(display_path) WHERE display_path<>'';
CREATE TABLE nas_cleanup_reserved_files(kind text,source_id bigint);
CREATE TABLE nas_cleanup_source_holds(clip_id bigint,plan_id bigint);
CREATE TABLE nas_cleanup_plans(id bigint PRIMARY KEY,mode text);
CREATE TABLE nas_cleanup_items(plan_id bigint,delete_ever_issued boolean);
CREATE TABLE r2_retirement_reservations(kind text,source_id bigint,bucket text,object_key text,size_bytes bigint,sha256 text,manifest_sha256 text,retirement_class text);
CREATE TABLE r2_object_retirements(kind text,source_id bigint);
INSERT INTO connections VALUES(13);`)
	if err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../../infra/sql/migrations/0193_nas_r2_copy.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	migration, err = os.ReadFile("../../../infra/sql/migrations/0200_nas_r2_copy_progress.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	policy, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	window := windows[3]
	if _, err = pool.Exec(ctx, `WITH s AS (INSERT INTO streams VALUES(3,$1) RETURNING id) INSERT INTO recordings SELECT 2,47,id,$1 FROM s`, window.Timezone); err != nil {
		t.Fatal(err)
	}
	items := make([]api.NASR2CopyItem, count)
	for n := range items {
		i := api.NASR2CopyItem{Kind: api.NASR2CopyRaw, SourceID: int64(n + 1), RecordingID: 2, StreamID: 3,
			Bucket: "bucket", ObjectKey: fmt.Sprintf("managed/copy-%d.mp4", n), RelativePath: fmt.Sprintf("p/copy-%d.mp4", n),
			SizeBytes: 100, SHA256: strings.Repeat("a", 64), StartAt: window.Start, EndAt: window.Start.Add(time.Minute)}
		if _, err = pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,bucket,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, i.SourceID, i.RecordingID, i.Bucket, i.ObjectKey, i.SHA256, i.SizeBytes, i.RelativePath, i.StartAt, i.EndAt); err != nil {
			t.Fatal(err)
		}
		items[n] = i
	}
	cfg, err := nasR2CopyPoolConfig(os.Getenv("STOARAMA_TEST_DATABASE_URL"), apply)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = pool.Config().ConnConfig.RuntimeParams["search_path"]
	queue, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(queue.Close)
	return pool, queue, nasCopyHeader{Version: 1, PolicySHA256: policy, SourceSHA256: strings.Repeat("c", 64), Items: int64(count), Bytes: int64(count * 100)}, items
}

type nasCopyTestStage uint8

const (
	nasCopyTestSetup nasCopyTestStage = iota
	nasCopyTestSource
	nasCopyTestCommit
	nasCopyTestSourceFailure
)

// Gate an actual PostgreSQL transaction at one protocol phase. No SQL results
// or source decisions are mocked, and all transaction ends reach pgxpool.
type nasCopyGatePool struct {
	*pgxpool.Pool
	stage          nasCopyTestStage
	entered        chan struct{}
	release        chan struct{}
	failureRelease chan struct{}
	active         atomic.Int32
	maximum        atomic.Int32
	begun          atomic.Int32
}

func (p *nasCopyGatePool) gate(ctx context.Context, release <-chan struct{}) error {
	select {
	case <-release:
		return nil
	default:
	}
	p.entered <- struct{}{}
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *nasCopyGatePool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	p.begun.Add(1)
	n := p.active.Add(1)
	for old := p.maximum.Load(); n > old; old = p.maximum.Load() {
		if p.maximum.CompareAndSwap(old, n) {
			break
		}
	}
	return &nasCopyGateTx{Tx: tx, pool: p}, nil
}

type nasCopyGateTx struct {
	pgx.Tx
	pool *nasCopyGatePool
	once sync.Once
}

func (tx *nasCopyGateTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if tx.pool.stage == nasCopyTestSetup {
		if err := tx.pool.gate(ctx, tx.pool.release); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	return tx.Tx.Exec(ctx, sql, args...)
}

type nasCopyErrorRow struct{ err error }

func (r nasCopyErrorRow) Scan(...any) error { return r.err }

func (tx *nasCopyGateTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if tx.pool.stage == nasCopyTestSource || tx.pool.stage == nasCopyTestSourceFailure {
		release := tx.pool.release
		failing := tx.pool.stage == nasCopyTestSourceFailure && args[0] == int64(1)
		if failing {
			release = tx.pool.failureRelease
		}
		if err := tx.pool.gate(ctx, release); err != nil {
			return nasCopyErrorRow{err}
		}
		if failing {
			return nasCopyErrorRow{errors.New("injected source failure")}
		}
	}
	return tx.Tx.QueryRow(ctx, sql, args...)
}

func (tx *nasCopyGateTx) Commit(ctx context.Context) error {
	if tx.pool.stage == nasCopyTestCommit {
		if err := tx.pool.gate(ctx, tx.pool.release); err != nil {
			return err
		}
	}
	err := tx.Tx.Commit(ctx)
	tx.once.Do(func() { tx.pool.active.Add(-1) })
	return err
}

func (tx *nasCopyGateTx) Rollback(ctx context.Context) error {
	err := tx.Tx.Rollback(ctx)
	tx.once.Do(func() { tx.pool.active.Add(-1) })
	return err
}

func TestNASR2CopyQueueBoundsAndReadOnlyDefault(t *testing.T) {
	cfg, err := nasR2CopyPoolConfig("postgres://localhost/test?pool_max_conns=100&statement_timeout=0&work_mem=512MB&default_transaction_read_only=off", false)
	if err != nil {
		t.Fatal(err)
	}
	params := cfg.ConnConfig.RuntimeParams
	if cfg.MaxConns != 4 || params["statement_timeout"] != "10s" || params["work_mem"] != "16MB" || params["default_transaction_read_only"] != "on" {
		t.Fatal("operator URL bypassed fixed copy reader bounds")
	}
	pool, queue, h, items := nasCopyQueueFixture(t, 12, false)
	gate := &nasCopyGatePool{Pool: queue, stage: nasCopyTestSetup, entered: make(chan struct{}, 4), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- queueNASR2Copy(ctx, gate, h, strings.Repeat("d", 64), items, false) }()
	for range 4 {
		select {
		case <-gate.entered:
		case <-ctx.Done():
			t.Fatal("four independent transactions did not start")
		}
	}
	close(gate.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if gate.maximum.Load() != 4 || gate.active.Load() != 0 || queue.Stat().AcquiredConns() != 0 {
		t.Fatal("copy workers serialized, exceeded four, or failed to drain")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("read-only validation wrote work: count=%d err=%v", count, err)
	}
	if err := queueNASR2Copy(ctx, queue, h, strings.Repeat("d", 64), items[:1], true); err == nil {
		t.Fatal("read-only pool allowed an insert")
	}
}

func TestNASR2CopyQueueCancellationDrainsTransactions(t *testing.T) {
	for _, test := range []struct {
		name  string
		stage nasCopyTestStage
	}{{"setup", nasCopyTestSetup}, {"source", nasCopyTestSource}, {"commit", nasCopyTestCommit}} {
		t.Run(test.name, func(t *testing.T) {
			pool, queue, h, items := nasCopyQueueFixture(t, 12, true)
			gate := &nasCopyGatePool{Pool: queue, stage: test.stage, entered: make(chan struct{}, 4), release: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- queueNASR2Copy(ctx, gate, h, strings.Repeat("d", 64), items, true) }()
			for range 4 {
				select {
				case <-gate.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("workers did not reach cancellation phase")
				}
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancellation reported success")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("canceled transactions did not drain")
			}
			if gate.begun.Load() != 4 || gate.active.Load() != 0 {
				t.Fatalf("cancellation drain: begun=%d active=%d acquired=%d idle=%d total=%d", gate.begun.Load(), gate.active.Load(), queue.Stat().AcquiredConns(), queue.Stat().IdleConns(), queue.Stat().TotalConns())
			}
			// The CLI explicitly closes before log.Fatal, which skips defers.
			// A canceled setup may leave pgx asynchronously discarding a socket.
			queue.Close()
			// puddle's destructor signals Close before updating its statistics.
			// Check actual PostgreSQL sessions instead of that transient count.
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			for {
				var sessions int
				if err := pool.QueryRow(cleanup, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='stoarama-nas-r2-copy'`).Scan(&sessions); err != nil {
					t.Fatal(err)
				}
				if sessions == 0 {
					break
				}
				select {
				case <-cleanup.Done():
					t.Fatal("command boundary left database sessions active")
				case <-time.After(5 * time.Millisecond):
				}
			}
			var count int
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM nas_r2_copy_items`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("uncommitted work survived cancellation: count=%d err=%v", count, err)
			}
		})
	}
}

func TestNASR2CopyQueueFirstErrorCancelsAndDrainsOtherWorkers(t *testing.T) {
	pool, queue, h, items := nasCopyQueueFixture(t, 12, true)
	gate := &nasCopyGatePool{Pool: queue, stage: nasCopyTestSourceFailure, entered: make(chan struct{}, 4), release: make(chan struct{}), failureRelease: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- queueNASR2Copy(ctx, gate, h, strings.Repeat("d", 64), items, true) }()
	for range 4 {
		select {
		case <-gate.entered:
		case <-ctx.Done():
			t.Fatal("workers did not reach source lookup")
		}
	}
	close(gate.failureRelease) // The other three only unblock when work is canceled.
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "copy source 1: injected source failure") {
			t.Fatalf("first source error lost: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("first error did not cancel other workers")
	}
	if gate.begun.Load() != 4 || gate.active.Load() != 0 {
		t.Fatal("failure dispatched more work or returned before rollback")
	}
	queue.Close()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed validation queued work: count=%d err=%v", count, err)
	}
}

func TestNASR2CopyQueueIdempotencyAndConflictPreservesCustody(t *testing.T) {
	pool, queue, h, items := nasCopyQueueFixture(t, 8, true)
	ctx := context.Background()
	manifest := strings.Repeat("d", 64)
	// A parallel import can commit this suffix before an earlier item fails.
	if err := queueNASR2Copy(ctx, queue, h, manifest, items[4:], true); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_r2_copy_items SET state='verified',attempts=7`); err != nil {
		t.Fatal(err)
	}
	if err := queueNASR2Copy(ctx, queue, h, manifest, items, true); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE state='verified' AND attempts=7 AND manifest_sha256=$1 AND policy_sha256=$2`, manifest, h.PolicySHA256).Scan(&count); err != nil || count != 4 {
		t.Fatalf("idempotent restart changed custody or manifest binding: count=%d err=%v", count, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items WHERE state='pending' AND attempts=0 AND manifest_sha256=$1 AND policy_sha256=$2`, manifest, h.PolicySHA256).Scan(&count); err != nil || count != 4 {
		t.Fatalf("partial import did not finish the exact manifest: count=%d err=%v", count, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_r2_copy_items SET item=jsonb_set(item,'{size_bytes}','101') WHERE source_id=$1`, items[4].SourceID); err != nil {
		t.Fatal(err)
	}
	if err := queueNASR2Copy(ctx, queue, h, manifest, items, true); err == nil || !strings.Contains(err.Error(), "identity differs") {
		t.Fatalf("conflicting queue identity accepted: %v", err)
	}
	var old api.NASR2CopyItem
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT item FROM nas_r2_copy_items WHERE source_id=$1 AND state='verified' AND attempts=7`, items[4].SourceID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &old); err != nil || old.SizeBytes != 101 {
		t.Fatal("conflicting identity was overwritten")
	}
}

func TestNASR2CopyQueueRetainsSourceAndAliasVetoes(t *testing.T) {
	for _, sql := range []string{
		`INSERT INTO recording_clips SELECT id+1000,recording_id,bucket,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,purged_at FROM recording_clips`,
		`INSERT INTO nas_cleanup_reserved_files VALUES('raw',1)`,
		`INSERT INTO nas_cleanup_plans VALUES(1,'enabled'); INSERT INTO nas_cleanup_source_holds VALUES(1,1)`,
		`INSERT INTO nas_cleanup_plans VALUES(1,'retired'); INSERT INTO nas_cleanup_source_holds VALUES(1,1); INSERT INTO nas_cleanup_items VALUES(1,true)`,
		`INSERT INTO r2_retirement_reservations VALUES('raw',1)`,
		`INSERT INTO r2_object_retirements VALUES('raw',1)`,
		`UPDATE recording_clips SET sha256=repeat('b',64)`,
		`UPDATE recording_clips SET purged_at=now()`,
	} {
		t.Run(fmt.Sprint(sql[:20]), func(t *testing.T) {
			pool, queue, h, items := nasCopyQueueFixture(t, 1, true)
			ctx := context.Background()
			if _, err := pool.Exec(ctx, sql); err != nil {
				t.Fatal(err)
			}
			if err := queueNASR2Copy(ctx, queue, h, strings.Repeat("d", 64), items, true); err == nil {
				t.Fatal("source or destination veto bypassed")
			}
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM nas_r2_copy_items`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("refused source queued: count=%d err=%v", count, err)
			}
		})
	}
}

func TestNASCopyStatusDurableCountersAndReimport(t *testing.T) {
	pool, _, h, items := nasCopyQueueFixture(t, 4, true)
	ctx := context.Background()
	manifest := strings.Repeat("a", 64)
	if err := queueNASR2Copy(ctx, pool, h, manifest, items, true); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_r2_copy_items SET state='verified',completion='verified_in_place' WHERE source_id=$1`, items[0].SourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_r2_copy_items SET state='verified',completion='downloaded',downloaded_bytes=50,committed_bytes=100 WHERE source_id=$1`, items[1].SourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nas_r2_copy_items SET state='blocked',blocked_reason='checksum_mismatch',attempts=4 WHERE source_id=$1`, items[2].SourceID); err != nil {
		t.Fatal(err)
	}
	if err := queueNASR2Copy(ctx, pool, h, manifest, items, true); err != nil {
		t.Fatal(err)
	}
	status, err := loadNASCopyStatus(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if status.QueueDepth != 1 || status.Blocked != 1 || status.MismatchLatched != 1 || status.VerifiedInPlace.Objects != 1 || status.Downloaded.Objects != 1 || status.DownloadedBytes != 50 || status.CommittedBytes != 100 || status.ReLeasedItems != 1 || status.MaxAttempts != 4 || status.BlockedByReason["checksum_mismatch"].Objects != 1 {
		t.Fatalf("status %+v", status)
	}
}

// applyOne owns the global cleanup/public fence on its main transaction and
// reserve must independently COMMIT intent via another pooled connection. The
// trigger must neither self-block on that fence nor admit a protected key.
func TestNASR2CopyRetirementIntentUsesIndependentConnection(t *testing.T) {
	for _, class := range []r2RetireClass{"", r2RetireOwnerAccepted} {
		t.Run(string(class), func(t *testing.T) {
			pool, _, h, items := nasCopyQueueFixture(t, 1, true)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := queueNASR2Copy(ctx, pool, h, strings.Repeat("a", 64), items, true); err != nil {
				t.Fatal(err)
			}
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			if err := api.LockNASCleanupR2Retirement(ctx, tx); err != nil {
				t.Fatal(err)
			}
			applier := r2RetireApplier{pool: pool, digest: strings.Repeat("b", 64), scope: r2RetireScope{Class: class}}
			unrelated := r2RetireItem{r2RetireCandidate: r2RetireCandidate{Kind: "raw", SourceID: 9999, Bucket: "bucket", ObjectKey: "managed/unrelated.mp4", SizeBytes: 1, SHA256: strings.Repeat("c", 64)}}
			if err := applier.reserve(ctx, unrelated); err != nil {
				t.Fatalf("independent durable intent blocked by own outer transaction: %v", err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM r2_retirement_reservations WHERE source_id=9999`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("intent lost on outer rollback: count=%d err=%v", count, err)
			}
			protected := items[0]
			if _, err := pool.Exec(ctx, `UPDATE nas_r2_copy_items SET state='blocked',blocked_reason='checksum_mismatch'`); err != nil {
				t.Fatal(err)
			}
			if err := applier.reserve(ctx, r2RetireItem{r2RetireCandidate: r2RetireCandidate{Kind: "raw", SourceID: protected.SourceID, Bucket: protected.Bucket, ObjectKey: protected.ObjectKey, SizeBytes: protected.SizeBytes, SHA256: protected.SHA256}}); err == nil || !strings.Contains(err.Error(), "COPY custody") {
				t.Fatalf("protected intent accepted: %v", err)
			}
		})
	}
}
