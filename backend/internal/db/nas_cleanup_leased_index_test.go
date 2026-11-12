package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNASCleanupLeasedIndexMigrationAndQueryPlan(t *testing.T) {
	url := os.Getenv("STOARAMA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	schema := fmt.Sprintf("cleanup_lease_index_%d", time.Now().UnixNano())
	exec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE SCHEMA " + schema)
	defer conn.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	exec("SET search_path TO " + schema)
	raw, err := os.ReadFile(filepath.Join("../../../infra/sql/migrations", "0188_nas_cleanup_leased_index.sql"))
	if err != nil {
		t.Fatal(err)
	}
	migration := string(raw)
	// Partial-schema fixtures need no cleanup table.
	exec(migration)
	exec(`CREATE TABLE nas_cleanup_plans(id bigint PRIMARY KEY,connection_id bigint NOT NULL,mode text NOT NULL);
 CREATE TABLE nas_cleanup_items(id bigint PRIMARY KEY,plan_id bigint NOT NULL,state text NOT NULL,lease_expires_at timestamptz,payload text);
 CREATE INDEX nas_cleanup_pending ON nas_cleanup_items(plan_id,id) WHERE state IN ('pending','leased');
 INSERT INTO nas_cleanup_plans SELECT n,CASE WHEN n<=1403 THEN 13 ELSE 14 END,'retired' FROM generate_series(1,1404) n;
 INSERT INTO nas_cleanup_items SELECT n,1,'pending',NULL,repeat(md5(n::text),16) FROM generate_series(1,42406) n;
 INSERT INTO nas_cleanup_items SELECT 42406+n,1,'leased',now()+interval '1 hour','live' FROM generate_series(1,8) n;
 INSERT INTO nas_cleanup_items VALUES(50000,1404,'leased',now()-interval '1 hour','other connection');
 ANALYZE nas_cleanup_items; ANALYZE nas_cleanup_plans;`)
	query := `SELECT EXISTS(SELECT 1 FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id WHERE p.connection_id=13 AND i.state='leased' AND i.lease_expires_at<now())`
	explain := func() (string, float64) {
		t.Helper()
		var raw []byte
		if err := conn.QueryRow(ctx, "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) "+query).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var data []struct {
			ExecutionTime float64 `json:"Execution Time"`
		}
		if err := json.Unmarshal(raw, &data); err != nil {
			t.Fatal(err)
		}
		return string(raw), data[0].ExecutionTime
	}
	before, beforeMS := explain()
	if !strings.Contains(before, `"Rows Removed by Filter": 42414`) && !strings.Contains(before, `"Rows Removed by Filter": 42406`) {
		t.Fatalf("fixture did not scan pending backlog: %s", before)
	}
	exec(migration)
	exec(migration) // exact precreation is idempotent
	exec("ANALYZE nas_cleanup_items")
	after, afterMS := explain()
	if !strings.Contains(after, "nas_cleanup_items_leased_plan_expiry") || strings.Contains(after, `"Rows Removed by Filter": 424`) {
		t.Fatalf("expired lease check scanned backlog: %s", after)
	}
	t.Logf("42406 pending rows: expiry query %.3fms -> %.3fms; leased-state index used", beforeMS, afterMS)
	// The same plan-first partial index serves the retired-plan lock-scope lookup.
	var lockPlan []byte
	if err := conn.QueryRow(ctx, `EXPLAIN (ANALYZE,FORMAT JSON) SELECT id FROM nas_cleanup_plans p WHERE connection_id=13 AND (mode<>'retired' OR EXISTS(SELECT 1 FROM nas_cleanup_items i WHERE i.plan_id=p.id AND i.state='leased'))`).Scan(&lockPlan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(lockPlan), "nas_cleanup_items_leased_plan_expiry") {
		t.Fatalf("lock scope missed leased index: %s", lockPlan)
	}
	var expired bool
	if err := conn.QueryRow(ctx, query).Scan(&expired); err != nil || expired {
		t.Fatalf("other connection leaked: %v %v", expired, err)
	}
	exec("UPDATE nas_cleanup_items SET lease_expires_at=now()-interval '1 second' WHERE id=42407")
	if err := conn.QueryRow(ctx, query).Scan(&expired); err != nil || !expired {
		t.Fatalf("own expired lease missed: %v %v", expired, err)
	}
	// Exercise the deployed large-table branch at a smaller byte threshold.
	exec("DROP INDEX nas_cleanup_items_leased_plan_expiry")
	guarded := strings.ReplaceAll(migration, "134217728", "1024")
	if _, err := conn.Exec(ctx, guarded); err == nil || !strings.Contains(err.Error(), "CREATE INDEX CONCURRENTLY") {
		t.Fatalf("large-table guard: %v", err)
	}
	exec("CREATE INDEX CONCURRENTLY nas_cleanup_items_leased_plan_expiry ON nas_cleanup_items(plan_id,lease_expires_at) WHERE state='leased'")
	exec(guarded)
	for _, wrong := range []string{
		"CREATE INDEX nas_cleanup_items_leased_plan_expiry ON nas_cleanup_items(plan_id,lease_expires_at)",
		"CREATE INDEX nas_cleanup_items_leased_plan_expiry ON nas_cleanup_items(lease_expires_at,plan_id) WHERE state='leased'",
		"CREATE INDEX nas_cleanup_items_leased_plan_expiry ON nas_cleanup_items(plan_id,lease_expires_at) WHERE state='pending'",
	} {
		exec("DROP INDEX nas_cleanup_items_leased_plan_expiry")
		exec(wrong)
		if _, err := conn.Exec(ctx, migration); err == nil {
			t.Fatalf("accepted wrong precreated index: %s", wrong)
		}
	}
}
