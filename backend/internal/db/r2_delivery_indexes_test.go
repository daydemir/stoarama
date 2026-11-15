package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestR2DeliveryIndexesMigration(t *testing.T) {
	dsn := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	schema := fmt.Sprintf("r2_delivery_indexes_%d", time.Now().UnixNano())
	exec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE SCHEMA " + schema)
	defer conn.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	exec("SET search_path TO " + schema)
	exec(`CREATE TABLE nas_inventory_files(relative_path text,sha256 text,size_bytes bigint,state text,padding text)`)
	// Install exactly the deployed readiness contract, without unrelated schema.
	deployed, err := os.ReadFile("../../../infra/sql/migrations/0192_delivery_only_r2_apply.sql")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(deployed), "CREATE FUNCTION r2_retirement_delivery_indexes_ready()")
	end := strings.Index(string(deployed)[start:], "$$;") + start + 3
	exec(string(deployed)[start:end])
	raw, err := os.ReadFile(filepath.Join("../../../infra/sql/migrations", "0194_r2_delivery_presence_indexes.sql"))
	if err != nil {
		t.Fatal(err)
	}
	migration := string(raw)
	apply := func(body string) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, body); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if err := apply(migration); err != nil {
		t.Fatal(err)
	}
	if err := apply(migration); err != nil {
		t.Fatal(err)
	}
	var ready bool
	if err := conn.QueryRow(ctx, `SELECT r2_retirement_delivery_indexes_ready()`).Scan(&ready); err != nil || !ready {
		t.Fatalf("gate remains closed: %t %v", ready, err)
	}
	// Use a reduced byte threshold to exercise the production-size refusal.
	exec(`DROP INDEX nas_inventory_r2_owner_path; DROP INDEX nas_inventory_r2_owner_content; INSERT INTO nas_inventory_files VALUES('path','sha',1,'present',repeat('x',1000))`)
	guarded := strings.ReplaceAll(migration, "134217728", "1")
	if err := apply(guarded); err == nil || !strings.Contains(err.Error(), "CONCURRENTLY") {
		t.Fatalf("large-table write-blocking build admitted: %v", err)
	}
	exec(`CREATE INDEX CONCURRENTLY nas_inventory_r2_owner_path ON nas_inventory_files(relative_path) WHERE state<>'missing'`)
	if err := apply(guarded); err == nil || !strings.Contains(err.Error(), "CONCURRENTLY") {
		t.Fatalf("partial precreation admitted: %v", err)
	}
	exec(`CREATE INDEX CONCURRENTLY nas_inventory_r2_owner_content ON nas_inventory_files(sha256,size_bytes) WHERE state<>'missing'`)
	if err := apply(guarded); err != nil {
		t.Fatal(err)
	}
	for _, wrong := range []string{
		`CREATE INDEX nas_inventory_r2_owner_path ON nas_inventory_files(relative_path DESC) WHERE state<>'missing'`,
		`CREATE INDEX nas_inventory_r2_owner_path ON nas_inventory_files(relative_path)`,
		`CREATE INDEX nas_inventory_r2_owner_path ON nas_inventory_files(relative_path text_pattern_ops) WHERE state<>'missing'`,
		`CREATE INDEX nas_inventory_r2_owner_path ON nas_inventory_files(relative_path) INCLUDE(padding) WHERE state<>'missing'`,
	} {
		exec(`DROP INDEX nas_inventory_r2_owner_path`)
		exec(wrong)
		if err := apply(migration); err == nil {
			t.Fatalf("incompatible index accepted: %s", wrong)
		}
	}
}
