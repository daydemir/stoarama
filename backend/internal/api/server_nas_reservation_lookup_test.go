package api

import (
	"os"
	"strings"
	"testing"
)

func TestNASReservationLookupMatchesView(t *testing.T) {
	f, _ := copyTestFixture(t)
	// Missing items, duplicate identities, normalized string IDs, ordinal-specific
	// issuance, paused/dry-run plans, and reservations on a second account/connection.

	_, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode) VALUES
 (101,47,13,'oracle-dry',repeat('a',64),'[{"kind":"raw","source_id":1001}]',repeat('b',64),'dry_run'),
 (102,47,13,'oracle-paused',repeat('a',64),'[{"kind":"raw","source_id":"001002"}]',repeat('b',64),'paused'),
 (103,47,13,'oracle-retired',repeat('a',64),'[{"kind":"raw","source_id":1003},{"kind":"raw","source_id":"001004"},{"kind":"raw","source_id":1005},{"kind":"raw","source_id":1006},{"kind":"raw","source_id":1006},{"kind":"collated","source_id":1008}]',repeat('b',64),'retired'),
 (104,48,14,'oracle-foreign',repeat('a',64),'[{"kind":"raw","source_id":1007}]',repeat('b',64),'dry_run');
 INSERT INTO nas_cleanup_items(plan_id,ordinal,delete_ever_issued) VALUES(103,1,true),(103,2,false),(103,4,false),(103,5,true);`)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"raw", "collated", "joined"} {
		for _, id := range []int64{1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 9999} {
			want := kind == "raw" && id >= 1001 && id <= 1007 && id != 1004 || kind == "collated" && id == 1008
			var old, new bool
			if err = f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_reserved_files WHERE kind=$1 AND source_id=$2),nas_cleanup_file_reserved($1,$2)`, kind, id).Scan(&old, &new); err != nil || old != want || new != want {
				t.Fatalf("%s/%d old=%t new=%t want=%t err=%v", kind, id, old, new, want, err)
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
	body, err := os.ReadFile("../../../infra/sql/migrations/0207_nas_cleanup_reservation_lookup.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_plans(id,account_id,connection_id,label,report_sha256,manifest,plan_sha256) VALUES(99999,47,13,'guard-seed',repeat('a',64),'[]',repeat('b',64))`); err != nil {
		t.Fatal(err)
	}
	for _, change := range []struct{ sql, want string }{
		{`DROP INDEX nas_cleanup_manifest_keys_idx`, "precreate nas_cleanup_manifest_keys_idx"},
		{`DROP INDEX nas_cleanup_manifest_keys_idx; CREATE INDEX nas_cleanup_manifest_keys_idx ON nas_cleanup_plans(id)`, "not the exact valid reservation index"},
		{`CREATE OR REPLACE FUNCTION nas_cleanup_manifest_keys(manifest jsonb) RETURNS text[] LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$ SELECT '{}'::text[] $$`, "differs from the indexed immutable projection"},
	} {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, `DROP FUNCTION nas_cleanup_file_reserved(text,bigint)`); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, change.sql); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(f.ctx, string(body)); err == nil || !strings.Contains(err.Error(), change.want) {
			t.Fatalf("expected %s, got %v", change.want, err)
		}
		tx.Rollback(f.ctx)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = tx.Exec(f.ctx, `DROP FUNCTION nas_cleanup_file_reserved(text,bigint)`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f.ctx, string(body)); err != nil {
		t.Fatalf("valid prerequisites rejected: %v", err)
	}

}
