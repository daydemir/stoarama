package db

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestNASCleanupPermanentHoldRemovalGuards(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `CREATE SCHEMA permanent_hold_guard_test; SET LOCAL search_path=permanent_hold_guard_test; CREATE TABLE nas_cleanup_source_holds(plan_id bigint,clip_id bigint); INSERT INTO nas_cleanup_source_holds VALUES(1440,1278687); CREATE VIEW nas_cleanup_incident_roster AS SELECT 'raw'::text kind,1278687::bigint source_id,ARRAY[1278687]::bigint[] raw_source_ids; CREATE VIEW nas_cleanup_incident_sources AS SELECT unnest(raw_source_ids) clip_id FROM nas_cleanup_incident_roster`); err != nil {
		t.Fatal(err)
	}
	migration, err := os.ReadFile("../../../infra/sql/migrations/0209_nas_cleanup_permanent_holds.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{`DELETE FROM nas_cleanup_source_holds`, `TRUNCATE nas_cleanup_source_holds`, `DELETE FROM nas_cleanup_incident_roster`, `TRUNCATE nas_cleanup_incident_roster`, `DELETE FROM nas_cleanup_incident_sources`, `TRUNCATE nas_cleanup_incident_sources`} {
		if _, err = tx.Exec(ctx, `SAVEPOINT reject_removal`); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, sql); err == nil {
			t.Fatal("removal allowed", sql)
		} else if strings.Contains(sql, "source_holds") && !strings.Contains(err.Error(), "source holds are permanent") {
			t.Fatal("wrong hold guard", err)
		}
		if _, err = tx.Exec(ctx, `ROLLBACK TO SAVEPOINT reject_removal`); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_source_holds`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("hold changed %d %v", n, err)
	}
}
