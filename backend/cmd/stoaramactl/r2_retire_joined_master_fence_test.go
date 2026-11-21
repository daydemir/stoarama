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

// The idle probe must be serialized against every cleanup path, not only against
// enable/advance: a lease or a dry-run observation takes the public fence shared,
// so an exclusive fence is what makes the window stable. This starts a shared
// public-fence holder (the shape of a lease/observation), and requires the apply
// fence to wait for it rather than probe a window that can still change.
func TestR2JoinedCancelFenceSerializesAgainstSharedCleanupReaders(t *testing.T) {
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
	schema := fmt.Sprintf("joined_fence_shared_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	var conns []*pgx.Conn
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
		INSERT INTO nas_cleanup_master(enabled) VALUES(false);
	`); err != nil {
		t.Fatal(err)
	}
	// A shared holder, exactly as a lease/dry-run observation fences itself.
	reader := connect()
	if _, err := reader.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	defer reader.Exec(context.Background(), `ROLLBACK`)
	if _, err := reader.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(7343226011101)`); err != nil {
		t.Fatal(err)
	}
	apply := connect()
	applyTx, err := apply.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer applyTx.Rollback(context.Background())
	applyDone := make(chan error, 1)
	go func() { applyDone <- r2JoinedApplyIdleFence(ctx, applyTx) }()
	// It must queue on the fence rather than complete against a changing window.
	deadline := time.Now().Add(10 * time.Second)
	queued := false
	for time.Now().Before(deadline) {
		var waiting bool
		if err := setup.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, int32(apply.PgConn().PID())).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			queued = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !queued {
		t.Fatal("the apply fence did not serialize against a shared cleanup reader")
	}
	if _, err := reader.Exec(ctx, `COMMIT`); err != nil {
		t.Fatal(err)
	}
	if err := <-applyDone; err != nil {
		t.Fatalf("apply fence with an idle window after the reader finished: %v", err)
	}
}

// A lease or observation that is still outstanding is not idle, and the check
// now runs under the exclusive fence. Removing either property fails this.
func TestR2JoinedCancelRefusesOutstandingCleanupWork(t *testing.T) {
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
	schema := fmt.Sprintf("joined_fence_busy_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`) }()
	cfg, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["search_path"] = schema
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `
		CREATE TABLE nas_cleanup_plans(id bigint PRIMARY KEY, mode text NOT NULL);
		CREATE TABLE nas_cleanup_batches(id bigint PRIMARY KEY, mode text NOT NULL);
		CREATE TABLE nas_cleanup_items(id bigint PRIMARY KEY, state text NOT NULL);
		CREATE TABLE nas_cleanup_master(singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), enabled boolean NOT NULL);
		INSERT INTO nas_cleanup_master(enabled) VALUES(false);
	`); err != nil {
		t.Fatal(err)
	}
	for name, sql := range map[string]string{
		"enabled plan":  `INSERT INTO nas_cleanup_plans(id,mode) VALUES(1,'enabled')`,
		"enabled batch": `INSERT INTO nas_cleanup_batches(id,mode) VALUES(1,'enabled')`,
		"leased item":   `INSERT INTO nas_cleanup_items(id,state) VALUES(1,'leased')`,
		"issued permit": `INSERT INTO nas_cleanup_items(id,state) VALUES(1,'delete_issued')`,
	} {
		if _, err := conn.Exec(ctx, `DELETE FROM nas_cleanup_plans; DELETE FROM nas_cleanup_batches; DELETE FROM nas_cleanup_items`); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ferr := r2JoinedApplyIdleFence(ctx, tx)
		tx.Rollback(ctx)
		if ferr == nil || !strings.Contains(ferr.Error(), "global-idle") {
			t.Fatalf("%s was treated as an idle window: %v", name, ferr)
		}
	}
}
