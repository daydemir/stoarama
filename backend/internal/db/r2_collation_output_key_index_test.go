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

// The R2 inverse executor probes recording_collation_outputs by object_key twice
// per object inside its exclusive reserve lock. These tests cover the precreated
// index, the refusal to build it on a populated table, and that the probe uses it.
const r2CollationKeyMigration = "0215_r2_collation_output_key_index.sql"

func r2CollationKeySchema(t *testing.T) *pgxpool.Pool {
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
	schema := fmt.Sprintf("r2_collation_key_%d", time.Now().UnixNano())
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
	if _, err = pool.Exec(ctx, `CREATE TABLE recording_collation_outputs(id bigserial PRIMARY KEY, object_key text NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return pool
}

func migrateR2CollationKey(t *testing.T, pool *pgxpool.Pool) error {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "infra", "sql", "migrations", r2CollationKeyMigration))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err = os.WriteFile(filepath.Join(dir, r2CollationKeyMigration), body, 0o600); err != nil {
		t.Fatal(err)
	}
	return MigrateUp(context.Background(), pool, dir)
}

func TestR2CollationKeyIndexEmptyTableCreatesInlineAndProbeUsesIt(t *testing.T) {
	pool := r2CollationKeySchema(t)
	if err := migrateR2CollationKey(t, pool); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_outputs(object_key) SELECT 'joined/b/objects/'||lpad(to_hex(g),64,'0')||'.mp4' FROM generate_series(1,5000) g; ANALYZE recording_collation_outputs`); err != nil {
		t.Fatal(err)
	}
	var plan string
	if err := pool.QueryRow(ctx, `EXPLAIN (FORMAT JSON) SELECT NOT EXISTS(SELECT 1 FROM recording_collation_outputs o WHERE o.object_key='managed/acct-1/x.mp4')`).Scan(&plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, `"Index Name": "r2_collation_outputs_object_key"`) {
		t.Fatalf("probe does not use the index: %s", plan)
	}
}

func TestR2CollationKeyIndexRefusesPopulatedTableWithoutPrecreatedIndex(t *testing.T) {
	pool := r2CollationKeySchema(t)
	if _, err := pool.Exec(context.Background(), `INSERT INTO recording_collation_outputs(object_key) VALUES('joined/b/objects/x.mp4')`); err != nil {
		t.Fatal(err)
	}
	if err := migrateR2CollationKey(t, pool); err == nil || !strings.Contains(err.Error(), "precreate r2_collation_outputs_object_key concurrently") {
		t.Fatalf("populated table without index: %v", err)
	}
}

func TestR2CollationKeyIndexAcceptsOnlyExactPrecreatedDefinition(t *testing.T) {
	for _, tc := range []struct {
		ddl string
		ok  bool
	}{
		{`CREATE INDEX CONCURRENTLY r2_collation_outputs_object_key ON recording_collation_outputs(object_key)`, true},
		{`CREATE INDEX CONCURRENTLY r2_collation_outputs_object_key ON recording_collation_outputs(object_key DESC)`, false},
		{`CREATE INDEX CONCURRENTLY r2_collation_outputs_object_key ON recording_collation_outputs(object_key) WHERE id>0`, false},
		{`CREATE INDEX CONCURRENTLY r2_collation_outputs_object_key ON recording_collation_outputs USING hash(object_key)`, false},
		{`CREATE INDEX CONCURRENTLY r2_collation_outputs_object_key ON recording_collation_outputs(lower(object_key))`, false},
		{`CREATE INDEX CONCURRENTLY r2_collation_outputs_object_key ON recording_collation_outputs(object_key COLLATE "C")`, false},
		{`CREATE INDEX CONCURRENTLY r2_collation_outputs_object_key ON recording_collation_outputs(object_key text_pattern_ops)`, false},
	} {
		pool := r2CollationKeySchema(t)
		ctx := context.Background()
		if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_outputs(object_key) VALUES('joined/b/objects/x.mp4')`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, tc.ddl); err != nil {
			t.Fatal(err)
		}
		err := migrateR2CollationKey(t, pool)
		if tc.ok != (err == nil) {
			t.Fatalf("%s: err=%v", tc.ddl, err)
		}
		if !tc.ok && !strings.Contains(err.Error(), "has another definition") {
			t.Fatalf("%s: %v", tc.ddl, err)
		}
	}
}
