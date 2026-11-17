package api

import (
	"os"
	"strings"
	"testing"
)

func TestNASReservationLookupMatchesView(t *testing.T) {
	f, _ := copyTestFixture(t)
	// Missing items, duplicate identities, normalized string IDs, ordinal-specific
	// issuance, all plan modes, and a second connection/account remain reservations.
	_, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode)
 SELECT 100+n,CASE WHEN n=4 THEN 48 ELSE 47 END,CASE WHEN n=4 THEN 14 ELSE 13 END,'oracle-'||n,repeat('a',64),
 '[{"kind":"raw","source_id":1001},{"kind":"raw","source_id":"001002"},{"kind":"raw","source_id":1003},{"kind":"raw","source_id":1003},{"kind":"collated","source_id":1001}]'::jsonb,
 repeat('b',64),CASE n WHEN 1 THEN 'dry_run' WHEN 2 THEN 'paused' WHEN 4 THEN 'dry_run' ELSE 'retired' END FROM generate_series(1,4)n;
 INSERT INTO nas_cleanup_items(plan_id,ordinal,delete_ever_issued) SELECT 103,n,n=1 FROM generate_series(1,4)n;`)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"raw", "collated", "joined"} {
		for _, id := range []int64{1001, 1002, 1003, 9999} {
			var old, new bool
			if err = f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_reserved_files WHERE kind=$1 AND source_id=$2),nas_cleanup_file_reserved($1,$2)`, kind, id).Scan(&old, &new); err != nil || old != new {
				t.Fatalf("%s/%d old=%t new=%t err=%v", kind, id, old, new, err)
			}
		}
	}
	// Exercise actual index eligibility, rather than timing a tiny fixture.
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = tx.Exec(f.ctx, `SET LOCAL enable_seqscan=off`); err != nil {
		t.Fatal(err)
	}
	var plan string
	rows, err := tx.Query(f.ctx, `EXPLAIN SELECT nas_cleanup_file_reserved('raw',9999)`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan += line
	}
	rows.Close()
	// SQL scalar functions can be planned as a Result; test the function body too.
	rows, err = tx.Query(f.ctx, `EXPLAIN SELECT 1 FROM nas_cleanup_plans WHERE nas_cleanup_manifest_keys(manifest) @> ARRAY['raw:9999']`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan += line
	}
	rows.Close()
	if !strings.Contains(plan, "nas_cleanup_manifest_keys_idx") {
		t.Fatal(plan)
	}
}

func TestNASReservationMigrationRejectsUnsafePrerequisites(t *testing.T) {
	f, _ := copyTestFixture(t)
	body, err := os.ReadFile("../../../infra/sql/migrations/0206_nas_cleanup_reservation_lookup.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []string{
		`DROP INDEX nas_cleanup_manifest_keys_idx`,
		`DROP INDEX nas_cleanup_manifest_keys_idx; CREATE INDEX nas_cleanup_manifest_keys_idx ON nas_cleanup_plans(id)`,
		`CREATE OR REPLACE FUNCTION nas_cleanup_manifest_keys(manifest jsonb) RETURNS text[] LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT '{}'::text[] $$`,
	} {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, change); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, string(body)); err == nil {
			t.Fatalf("unsafe prerequisite accepted: %s", change)
		}
		tx.Rollback(f.ctx)
	}
}
