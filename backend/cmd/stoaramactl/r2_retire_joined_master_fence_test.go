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
