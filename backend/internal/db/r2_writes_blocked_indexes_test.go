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

// The R2 delete executor calls r2_retirement_writes_blocked() twice per object,
// so a seq scan over millions of expired intents timed every delete out. These
// tests cover the two precreated expiry indexes, the refusal to build them on a
// populated table, and that the deployed function actually reaches them.
const r2WritesBlockedMigration = "0213_r2_writes_blocked_indexes.sql"

func r2WritesBlockedSchema(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("r2_writes_blocked_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	return pool, schema
}

func r2WritesBlockedMigrationDir(t *testing.T) string {
	t.Helper()
	source := filepath.Join("..", "..", "..", "infra", "sql", "migrations")
	body, err := os.ReadFile(filepath.Join(source, r2WritesBlockedMigration))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = os.WriteFile(filepath.Join(dir, r2WritesBlockedMigration), body, 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// r2WritesBlockedTables installs only the columns 0213 and the 0198 function
// read, with the NOT NULLs and checks the real tables carry.
func r2WritesBlockedTables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `
CREATE TABLE recording_upload_intents(id bigserial PRIMARY KEY, bucket text, object_key text NOT NULL, expires_at timestamptz NOT NULL);
CREATE TABLE nas_restore_requests(id bigserial PRIMARY KEY, object_key text NOT NULL, state text NOT NULL DEFAULT 'pending', put_expires_at timestamptz);
CREATE TABLE upload_intents(id bigserial PRIMARY KEY, bucket text NOT NULL, object_key text NOT NULL, expires_at timestamptz NOT NULL);
CREATE TABLE capture_session_leases(stream_id bigserial PRIMARY KEY, lease_expires_at timestamptz NOT NULL);
CREATE TABLE r2_retirement_writer_fences(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), bucket text NOT NULL, endpoint_sha256 text NOT NULL, activated_at timestamptz NOT NULL DEFAULT clock_timestamp(), drain_not_before timestamptz NOT NULL);
CREATE TABLE r2_retirement_writer_operations(id bigserial PRIMARY KEY, completed_at timestamptz);
CREATE TABLE r2_retirement_writer_capabilities(id bigserial PRIMARY KEY, expires_at timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
}

// migrateR2WritesBlocked applies exactly the 0198 function and then 0213, which
// is the deployed order.
func migrateR2WritesBlocked(t *testing.T, pool *pgxpool.Pool) error {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "DROP FUNCTION IF EXISTS r2_retirement_writes_blocked(text,text)"); err != nil {
		t.Fatal(err)
	}
	legacy, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", "0198_r2_blocked_writes.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(context.Background(), string(legacy)); err != nil {
		t.Fatal(err)
	}
	return MigrateUp(context.Background(), pool, r2WritesBlockedMigrationDir(t))
}

func TestR2WritesBlockedIndexesMigration(t *testing.T) {
	t.Run("empty database builds both indexes inline", func(t *testing.T) {
		pool, _ := r2WritesBlockedSchema(t)
		r2WritesBlockedTables(t, pool)
		if err := migrateR2WritesBlocked(t, pool); err != nil {
			t.Fatalf("empty database migration: %v", err)
		}
		ctx := context.Background()
		for name, want := range map[string][2]string{
			"r2wb_recording_upload_intents_expires_at": {"recording_upload_intents", "expires_at"},
			"r2wb_nas_restore_requests_put_expires_at": {"nas_restore_requests", "put_expires_at"},
		} {
			var table, column string
			var valid, unique bool
			var nkeys int
			var pred *string
			if err := pool.QueryRow(ctx, `SELECT i.indrelid::regclass::text,pg_get_indexdef(i.indexrelid,1,false),i.indisvalid,i.indisunique,i.indnkeyatts,i.indpred FROM pg_index i WHERE i.indexrelid=to_regclass($1)`, name).Scan(&table, &column, &valid, &unique, &nkeys, &pred); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if table != want[0] || column != want[1] || !valid || unique || nkeys != 1 || pred != nil {
				t.Fatalf("%s on %s(%s) valid=%t unique=%t keys=%d pred=%v", name, table, column, valid, unique, nkeys, pred)
			}
		}
		// A second pass is idempotent and leaves the same definition.
		if err := MigrateUp(ctx, pool, r2WritesBlockedMigrationDir(t)); err != nil {
			t.Fatalf("repeat migration: %v", err)
		}
	})

	t.Run("populated table without the index is refused", func(t *testing.T) {
		pool, _ := r2WritesBlockedSchema(t)
		r2WritesBlockedTables(t, pool)
		if _, err := pool.Exec(context.Background(), `INSERT INTO recording_upload_intents(bucket,object_key,expires_at) VALUES('b','k',now()-interval '1 day')`); err != nil {
			t.Fatal(err)
		}
		err := migrateR2WritesBlocked(t, pool)
		if err == nil || !strings.Contains(err.Error(), "precreate r2wb_recording_upload_intents_expires_at") {
			t.Fatalf("populated table admitted: %v", err)
		}
	})

	t.Run("populated tables succeed after a concurrent precreate", func(t *testing.T) {
		pool, _ := r2WritesBlockedSchema(t)
		r2WritesBlockedTables(t, pool)
		ctx := context.Background()
		// CREATE INDEX CONCURRENTLY cannot run inside the migration transaction,
		// so a populated database precreates both outside one, as operators do.
		for _, ddl := range []string{
			`CREATE INDEX CONCURRENTLY r2wb_recording_upload_intents_expires_at ON recording_upload_intents(expires_at)`,
			`CREATE INDEX CONCURRENTLY r2wb_nas_restore_requests_put_expires_at ON nas_restore_requests(put_expires_at)`,
		} {
			if _, err := pool.Exec(ctx, ddl); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pool.Exec(ctx, `INSERT INTO recording_upload_intents(bucket,object_key,expires_at) VALUES('b','k',now()-interval '1 day');
 INSERT INTO nas_restore_requests(object_key,put_expires_at) VALUES('k',now()-interval '1 day')`); err != nil {
			t.Fatal(err)
		}
		if err := migrateR2WritesBlocked(t, pool); err != nil {
			t.Fatalf("precreated populated database: %v", err)
		}
	})

	t.Run("another definition is refused", func(t *testing.T) {
		pool, _ := r2WritesBlockedSchema(t)
		r2WritesBlockedTables(t, pool)
		ctx := context.Background()
		for _, wrong := range []struct{ name, ddl string }{
			{"another column", `CREATE INDEX r2wb_recording_upload_intents_expires_at ON recording_upload_intents(bucket)`},
			{"partial index", `CREATE INDEX r2wb_recording_upload_intents_expires_at ON recording_upload_intents(expires_at) WHERE expires_at IS NOT NULL`},
			{"wrong order", `CREATE INDEX r2wb_recording_upload_intents_expires_at ON recording_upload_intents(expires_at DESC)`},
			{"unique", `CREATE UNIQUE INDEX r2wb_recording_upload_intents_expires_at ON recording_upload_intents(expires_at)`},
		} {
			if _, err := pool.Exec(ctx, `DROP INDEX IF EXISTS r2wb_recording_upload_intents_expires_at`); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, wrong.ddl); err != nil {
				t.Fatalf("%s: %v", wrong.name, err)
			}
			err := migrateR2WritesBlocked(t, pool)
			if err == nil || !strings.Contains(err.Error(), "r2wb_recording_upload_intents_expires_at is absent, invalid or has another definition") {
				t.Fatalf("%s admitted: %v", wrong.name, err)
			}
		}
		if _, err := pool.Exec(ctx, `DROP INDEX r2wb_recording_upload_intents_expires_at;
 CREATE INDEX r2wb_nas_restore_requests_put_expires_at ON nas_restore_requests(state)`); err != nil {
			t.Fatal(err)
		}
		err := migrateR2WritesBlocked(t, pool)
		if err == nil || !strings.Contains(err.Error(), "r2wb_nas_restore_requests_put_expires_at is absent, invalid or has another definition") {
			t.Fatalf("wrong restore index admitted: %v", err)
		}
	})
}

// A fresh database must migrate the whole chain and end with both indexes and
// the replacement function, which is what a new deployment runs.
func TestR2WritesBlockedMigrationRunsThroughMigrateUp(t *testing.T) {
	pool, _ := r2WritesBlockedSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := MigrateUp(ctx, pool, filepath.Join("..", "..", "..", "infra", "sql", "migrations")); err != nil {
		t.Fatalf("full migration chain: %v", err)
	}
	var indexes int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_index i JOIN pg_am a ON a.oid=(SELECT relam FROM pg_class WHERE oid=i.indexrelid)
 WHERE i.indexrelid IN (to_regclass('r2wb_recording_upload_intents_expires_at'),to_regclass('r2wb_nas_restore_requests_put_expires_at'))
  AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
  AND a.amname='btree' AND i.indnkeyatts=1 AND i.indnatts=1
  AND i.indexprs IS NULL AND i.indpred IS NULL AND i.indoption::text='0'`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 2 {
		t.Fatalf("fresh chain built %d of 2 expiry indexes", indexes)
	}
	// The replacement function must be the deployed 0213 body, not the 0198 one.
	var body string
	if err := pool.QueryRow(ctx, `SELECT pg_get_functiondef(to_regprocedure('r2_retirement_writes_blocked(text,text)'))`).Scan(&body); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"max(expires_at)", "max(put_expires_at)", "LEAST(now(), clock_timestamp())", "expires_at>cutoff", "lease_expires_at>cutoff", "drain_not_before<clock_timestamp()"} {
		if !strings.Contains(body, want) {
			t.Fatalf("fresh chain installed a function without %q: %s", want, body)
		}
	}
	if strings.Contains(body, "(SELECT clock_timestamp())") {
		t.Fatal("fresh chain kept the InitPlan clock form the planner cannot index")
	}
}

// The expiry probes must reach the indexes with a large expired backlog.
func TestR2WritesBlockedExpiryProbesUseIndexes(t *testing.T) {
	pool, _ := r2WritesBlockedSchema(t)
	r2WritesBlockedTables(t, pool)
	ctx := context.Background()
	if err := migrateR2WritesBlocked(t, pool); err != nil {
		t.Fatal(err)
	}
	// The planner favors the backward scan when the table has enough rows.
	if _, err := pool.Exec(ctx, `INSERT INTO recording_upload_intents(bucket,object_key,expires_at)
 SELECT 'b','k'||n,now()-interval '1 day' FROM generate_series(1,50000) n;
 INSERT INTO nas_restore_requests(object_key,put_expires_at)
 SELECT 'k'||n,now()-interval '1 day' FROM generate_series(1,50000) n;
 ANALYZE recording_upload_intents; ANALYZE nas_restore_requests`); err != nil {
		t.Fatal(err)
	}
	for _, q := range []struct{ table, column string }{
		{"recording_upload_intents", "expires_at"},
		{"nas_restore_requests", "put_expires_at"},
	} {
		var raw []byte
		subquery := fmt.Sprintf(`WITH expiry_cutoff AS MATERIALIZED (SELECT LEAST(now(), clock_timestamp()) AS cutoff)
 SELECT COALESCE((SELECT max(%s) FROM %s)>cutoff,false) FROM expiry_cutoff`, q.column, q.table)
		if err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+subquery).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		plan := string(raw)
		backwardIndex := (strings.Contains(plan, `"Node Type": "Index Only Scan"`) || strings.Contains(plan, `"Node Type": "Index Scan"`)) &&
			strings.Contains(plan, `"Scan Direction": "Backward"`)
		if !backwardIndex {
			t.Fatalf("%s did not scan backward: %s", subquery, plan)
		}
		if !strings.Contains(plan, "r2wb_"+q.table) {
			t.Fatalf("%s missed its index: %s", subquery, plan)
		}
		if strings.Contains(plan, "Seq Scan") {
			t.Fatalf("%s still seq scans: %s", subquery, plan)
		}
	}
	// A mature fence makes the installed function evaluate the writer vetoes.
	endpoint := strings.Repeat("a", 64)
	if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256,drain_not_before) VALUES('b',$1,now()-interval '1 minute')`, endpoint); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var blocked bool
	if err := pool.QueryRow(ctx, `SELECT r2_retirement_writes_blocked('b',$1)`, endpoint).Scan(&blocked); err != nil {
		t.Fatal(err)
	}
	t.Logf("mature-fence predicate with 50k expired rows per index took %s", time.Since(start))
	if !blocked {
		t.Fatal("an expired backlog vetoed a mature fence")
	}
}

// Each clocked surface alone must veto, and clear once drained, so the new
// probes keep every previous live-writer signal.
func TestR2WritesBlockedEveryClockedSurfaceVetoes(t *testing.T) {
	for _, tc := range []struct{ name, insert, clear string }{
		{"writer capability", `INSERT INTO r2_retirement_writer_capabilities(expires_at) VALUES(now()+interval '1 hour')`, `DELETE FROM r2_retirement_writer_capabilities`},
		{"recording intent", `INSERT INTO recording_upload_intents(bucket,object_key,expires_at) VALUES('b','k',now()+interval '1 hour')`, `DELETE FROM recording_upload_intents`},
		{"generic intent", `INSERT INTO upload_intents(bucket,object_key,expires_at) VALUES('b','k',now()+interval '1 hour')`, `DELETE FROM upload_intents`},
		{"capture lease", `INSERT INTO capture_session_leases(lease_expires_at) VALUES(now()+interval '1 hour')`, `DELETE FROM capture_session_leases`},
		{"restore PUT", `INSERT INTO nas_restore_requests(object_key,put_expires_at) VALUES('k',now()+interval '1 hour')`, `DELETE FROM nas_restore_requests`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, _ := r2WritesBlockedSchema(t)
			r2WritesBlockedTables(t, pool)
			ctx := context.Background()
			if err := migrateR2WritesBlocked(t, pool); err != nil {
				t.Fatal(err)
			}
			endpoint := strings.Repeat("a", 64)
			// A fence matured in the past for exactly this endpoint: only the
			// outstanding write can keep the answer false.
			if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256,drain_not_before) VALUES('b',$1,now()-interval '1 minute')`, endpoint); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, tc.insert); err != nil {
				t.Fatal(err)
			}
			var blocked bool
			if err := pool.QueryRow(ctx, `SELECT r2_retirement_writes_blocked('b',$1)`, endpoint).Scan(&blocked); err != nil {
				t.Fatal(err)
			}
			if blocked {
				t.Fatal("live writer did not veto retirement")
			}
			if _, err := pool.Exec(ctx, tc.clear); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, `SELECT r2_retirement_writes_blocked('b',$1)`, endpoint).Scan(&blocked); err != nil {
				t.Fatal(err)
			}
			if !blocked {
				t.Fatal("drained table did not unblock retirement")
			}
		})
	}
}

// The cutoff cannot advance beyond the transaction start. An expiry after
// that start must still veto retirement once the wall clock has advanced.
func TestR2WritesBlockedTreatsExpiryDuringTransactionAsLive(t *testing.T) {
	pool, _ := r2WritesBlockedSchema(t)
	r2WritesBlockedTables(t, pool)
	ctx := context.Background()
	if err := migrateR2WritesBlocked(t, pool); err != nil {
		t.Fatal(err)
	}
	endpoint := strings.Repeat("a", 64)
	if _, err := pool.Exec(ctx, `INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256,drain_not_before) VALUES('b',$1,now()-interval '1 minute')`, endpoint); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO recording_upload_intents(bucket,object_key,expires_at) VALUES('b','k',now()+interval '30 milliseconds')`); err != nil {
		t.Fatal(err)
	}
	// The transaction's now() predates the row's expiry for its whole lifetime.
	if _, err = tx.Exec(ctx, `SELECT pg_sleep(0.1)`); err != nil {
		t.Fatal(err)
	}
	var expired, blocked bool
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()>(SELECT expires_at FROM recording_upload_intents WHERE object_key='k'),r2_retirement_writes_blocked('b',$1)`, endpoint).Scan(&expired, &blocked); err != nil {
		t.Fatal(err)
	}
	if !expired {
		t.Fatal("fixture did not let the intent expire inside the transaction")
	}
	if blocked {
		t.Fatal("expiry during a transaction dropped a live writer")
	}
}
