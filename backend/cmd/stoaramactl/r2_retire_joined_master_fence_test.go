package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The apply transaction's authoritative master read must run on the apply
// transaction's own connection. Reading it through the pool takes the FOR SHARE
// lock on a different connection, holds it for nothing, and lets the retirement
// write proceed while another session flips the master ON.
func TestR2JoinedCancelApplyIdleFenceReadsMasterOnTheApplyTransaction(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run the joined-writer fence regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("joined_master_fence_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) }()
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	// One connection: any pool read inside the apply transaction deadlocks here
	// instead of silently taking the lock somewhere else.
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `
		CREATE TABLE nas_cleanup_plans(id bigint PRIMARY KEY, mode text NOT NULL);
		CREATE TABLE nas_cleanup_batches(id bigint PRIMARY KEY, mode text NOT NULL);
		CREATE TABLE nas_cleanup_items(id bigint PRIMARY KEY, state text NOT NULL);
		CREATE TABLE nas_cleanup_master(singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), enabled boolean NOT NULL);
		INSERT INTO nas_cleanup_master(enabled) VALUES(false);
	`); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := r2JoinedApplyIdleFence(ctx, tx); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("idle window refused: %v", err)
	}
	// The lock must belong to this very transaction: a second session's flip
	// has to wait until this one ends.
	flip := make(chan error, 1)
	go func() {
		connCfg, err := pgx.ParseConfig(databaseURL)
		if err != nil {
			flip <- err
			return
		}
		connCfg.RuntimeParams["search_path"] = schema
		conn, err := pgx.ConnectConfig(ctx, connCfg)
		if err != nil {
			flip <- err
			return
		}
		defer conn.Close(ctx)
		_, err = conn.Exec(ctx, `UPDATE nas_cleanup_master SET enabled=true`)
		flip <- err
	}()
	select {
	case <-flip:
		tx.Rollback(ctx)
		t.Fatal("master flip did not wait for the apply transaction's FOR SHARE lock")
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-flip:
		if err != nil {
			t.Fatalf("flip after apply transaction: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("flip never completed")
	}
	var on bool
	if err := pool.QueryRow(ctx, `SELECT enabled FROM nas_cleanup_master`).Scan(&on); err != nil || !on {
		t.Fatalf("flip lost: %v %v", on, err)
	}
	// With the master ON the fence refuses the retirement outright.
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := r2JoinedApplyIdleFence(ctx, tx2); err == nil || !strings.Contains(err.Error(), "master OFF") {
		_ = tx2.Rollback(ctx)
		t.Fatalf("master ON accepted for joined writer retirement: %v", err)
	}
	if err := tx2.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// A leased or enabled plan also refuses, before the master is read.
	if _, err := pool.Exec(ctx, `UPDATE nas_cleanup_master SET enabled=false;INSERT INTO nas_cleanup_items VALUES(1,'leased')`); err != nil {
		t.Fatal(err)
	}
	tx3, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx3.Rollback(ctx)
	if err := r2JoinedApplyIdleFence(ctx, tx3); err == nil || !strings.Contains(err.Error(), "global-idle") {
		t.Fatalf("non-idle window accepted: %v", err)
	}
}

// The idle window can change while the apply transaction waits for its locks:
// a plan enable that already read the master ON can commit an enabled plan and
// then an OFF can land ahead of the apply transaction in the master lock queue.
// Reading OFF alone would accept a window that is no longer idle. The fence
// therefore holds the cleanup public fence (which every enable/advance path
// takes exclusively) and re-checks the window after reading the master, so the
// retirement can never write while a cleanup plan is enabled.
func TestR2JoinedCancelIdleWindowIsNotStaleAcrossTheMasterWait(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL to run the joined-writer fence regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("joined_idle_stale_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	var conns []*pgx.Conn
	// Close every connection before dropping: DROP waits for open sessions that
	// still hold locks on the schema's objects.
	t.Cleanup(func() {
		for _, c := range conns {
			c.Close(context.Background())
		}
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	connect := func() *pgx.Conn {
		t.Helper()
		cfg, err := pgx.ParseConfig(databaseURL)
		if err != nil {
			t.Fatal(err)
		}
		cfg.RuntimeParams["search_path"] = schema
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
		return conn
	}
	setup := connect()
	if _, err := setup.Exec(ctx, `
		CREATE TABLE nas_cleanup_plans(id bigint PRIMARY KEY, mode text NOT NULL);
		CREATE TABLE nas_cleanup_batches(id bigint PRIMARY KEY, mode text NOT NULL);
		CREATE TABLE nas_cleanup_items(id bigint PRIMARY KEY, state text NOT NULL);
		CREATE TABLE nas_cleanup_master(singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), enabled boolean NOT NULL);
		INSERT INTO nas_cleanup_master(enabled) VALUES(true);
	`); err != nil {
		t.Fatal(err)
	}
	waitForLock := func(pid uint32, doing string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		var last string
		for time.Now().Before(deadline) {
			var waiting bool
			if err := setup.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, int32(pid)).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				return
			}
			_ = setup.QueryRow(ctx, `SELECT state||'/'||coalesce(wait_event_type,'-')||'/'||left(coalesce(query,''),40) FROM pg_stat_activity WHERE pid=$1`, int32(pid)).Scan(&last)
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("%s never queued on its lock (last state %q)", doing, last)
	}

	// E: a plan enable. It takes the exclusive public fence (as every enable
	// path does) and reads the master ON, so its window is still open.
	enable := connect()
	if _, err := enable.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	defer enable.Exec(context.Background(), `ROLLBACK`)
	if _, err := enable.Exec(ctx, `SELECT pg_advisory_xact_lock(7343226011101)`); err != nil {
		t.Fatal(err)
	}
	var on bool
	if err := enable.QueryRow(ctx, `SELECT enabled FROM nas_cleanup_master FOR SHARE`).Scan(&on); err != nil || !on {
		t.Fatalf("enable master read: %t %v", on, err)
	}
	// A: the apply transaction, which must wait on E's public fence.
	apply := connect()
	applyTx, err := apply.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer applyTx.Rollback(context.Background())
	applyDone := make(chan error, 1)
	go func() { applyDone <- r2JoinedApplyIdleFence(ctx, applyTx) }()
	waitForLock(apply.PgConn().PID(), "the apply transaction")

	// E commits an enabled plan and flips the master OFF (an emergency OFF does
	// not pause plans) while the apply transaction waits on the fence.
	if _, err := enable.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,mode) VALUES(1,'enabled')`); err != nil {
		t.Fatal(err)
	}
	if _, err := enable.Exec(ctx, `UPDATE nas_cleanup_master SET enabled=false`); err != nil {
		t.Fatal(err)
	}
	if _, err := enable.Exec(ctx, `COMMIT`); err != nil {
		t.Fatal(err)
	}
	err = <-applyDone
	if err == nil {
		t.Fatal("a stale idle window was accepted with an enabled plan committed during the wait")
	}
	if !strings.Contains(err.Error(), "global-idle") {
		t.Fatalf("wrong refusal for the stale window: %v", err)
	}
	if rbErr := applyTx.Rollback(ctx); rbErr != nil {
		t.Fatal(rbErr)
	}
	// The same fence with an idle window still refuses when the master is ON.
	if _, err := setup.Exec(ctx, `DELETE FROM nas_cleanup_plans; UPDATE nas_cleanup_master SET enabled=true`); err != nil {
		t.Fatal(err)
	}
	tx2, err := apply.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback(context.Background())
	if err := r2JoinedApplyIdleFence(ctx, tx2); err == nil || !strings.Contains(err.Error(), "master OFF") {
		t.Fatalf("master ON accepted for joined writer retirement: %v", err)
	}
}
