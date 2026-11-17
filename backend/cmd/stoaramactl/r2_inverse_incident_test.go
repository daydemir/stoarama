package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

type inverseIncidentDescriptor struct {
	Kind       string  `json:"kind"`
	SourceID   int64   `json:"source_id"`
	Bucket     string  `json:"bucket"`
	Key        string  `json:"object_key"`
	RawSources []int64 `json:"raw_source_ids"`
}

func inverseIncidentDescriptors(t *testing.T) []inverseIncidentDescriptor {
	t.Helper()
	data, err := os.ReadFile("testdata/r2_inverse_incidents.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []inverseIncidentDescriptor
	if err = json.Unmarshal(data, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 53 {
		t.Fatal("unexpected incident descriptor count", len(rows))
	}
	return rows
}
func TestR2InverseCanonicalIncidentMigration(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_incident_roster")
	// Install the actual prior schema first, preserving the dependent view OIDs.
	pre := t.TempDir()
	entries, err := os.ReadDir(joinedPurgeMigrationsDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") || entry.Name() == "0202_r2_incident_roster.sql" {
			continue
		}
		source, err := filepath.Abs(filepath.Join(joinedPurgeMigrationsDir(), entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.Symlink(source, filepath.Join(pre, entry.Name())); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.MigrateUp(ctx, pool, pre); err != nil {
		t.Fatal(err)
	}
	var filesOID, sourcesOID uint32
	if err = pool.QueryRow(ctx, `SELECT 'nas_cleanup_incident_files'::regclass::oid,'nas_cleanup_incident_sources'::regclass::oid`).Scan(&filesOID, &sourcesOID); err != nil {
		t.Fatal(err)
	}
	var old []int64
	if err = pool.QueryRow(ctx, `SELECT array_agg(source_id ORDER BY source_id) FROM nas_cleanup_incident_files`).Scan(&old); err != nil || len(old) != 28 {
		t.Fatal(old, err)
	}
	// Independent schemas share advisory locks in one PostgreSQL database. A
	// migration may coexist with another migration, but never an active applier.
	applier, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = applier.Exec(ctx, `SELECT pg_advisory_lock(7343226011173)`); err != nil {
		t.Fatal(err)
	}
	err = db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir())
	if _, unlockErr := applier.Exec(ctx, `SELECT pg_advisory_unlock(7343226011173)`); unlockErr != nil {
		t.Fatal(unlockErr)
	}
	applier.Release()
	if err == nil || !strings.Contains(err.Error(), "R2 retirement apply is active") {
		t.Fatal("active applier did not refuse migration", err)
	}
	otherMigration, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer otherMigration.Rollback(ctx)
	if _, err = otherMigration.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(7343226011173)`); err != nil {
		t.Fatal(err)
	}
	if err = db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	// A migration's shared guard still prevents a new exclusive applier.
	probe, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var admitted bool
	err = probe.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(7343226011173)`).Scan(&admitted)
	probe.Rollback(ctx)
	if err != nil || admitted {
		t.Fatal("migration admitted concurrent apply", admitted, err)
	}
	if err = otherMigration.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var filesAfter, sourcesAfter uint32
	if err = pool.QueryRow(ctx, `SELECT 'nas_cleanup_incident_files'::regclass::oid,'nas_cleanup_incident_sources'::regclass::oid`).Scan(&filesAfter, &sourcesAfter); err != nil || filesOID != filesAfter || sourcesOID != sourcesAfter {
		t.Fatal("dependent view identity changed", err)
	}
	descriptors := inverseIncidentDescriptors(t)
	rawSet := map[int64]bool{}
	for _, d := range descriptors {
		var bucket, key string
		var dependencies []int64
		if err = pool.QueryRow(ctx, `SELECT bucket,object_key,raw_source_ids FROM nas_cleanup_incident_roster WHERE kind=$1 AND source_id=$2`, d.Kind, d.SourceID).Scan(&bucket, &key, &dependencies); err != nil || bucket != d.Bucket || key != d.Key || !reflect.DeepEqual(dependencies, d.RawSources) {
			t.Fatalf("descriptor changed: %s/%d %v", d.Kind, d.SourceID, err)
		}
		for _, id := range d.RawSources {
			rawSet[id] = true
		}
	}
	for _, id := range old {
		if !rawSet[id] {
			t.Fatal("old incident lost", id)
		}
	}
	ids, err := loadR2RetireIncidentSources(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	var expected []int64
	for id := range rawSet {
		expected = append(expected, id)
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })
	if !reflect.DeepEqual(ids, expected) {
		t.Fatal("raw dependency/kind namespace mismatch")
	}
	if len(ids) != 292 {
		t.Fatal("raw dependency count", len(ids))
	}
	p, err := loadInverseProtection(ctx, pool, "stoarama", r2RetirePicks{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// No catalog rows or plans exist: exact R2 identities remain held anyway.
	for _, d := range descriptors {
		if d.Key != "" {
			if class, _ := p.classify(r2.ObjectInfo{Key: d.Key}); class != inverseHeld {
				t.Fatal("incident key lost without catalog", d.Key, class)
			}
		}
	}
	for _, sql := range []string{`UPDATE nas_cleanup_incident_roster SET bucket='other'`, `DELETE FROM nas_cleanup_incident_roster`, `TRUNCATE nas_cleanup_incident_roster`, `INSERT INTO nas_cleanup_incident_roster VALUES('raw',1,'stoarama','new',ARRAY[1]::bigint[])`, `TRUNCATE nas_cleanup_incident_files`, `UPDATE nas_cleanup_incident_files SET source_id=1`} {
		if _, err = pool.Exec(ctx, sql); err == nil {
			t.Fatal("immutable roster mutation accepted", sql)
		}
	}
}
func TestR2InverseIncidentOutputAndReservationGuards(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_incident_guards")
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE VIEW nas_cleanup_incident_files AS SELECT 'raw'::text kind,296368::bigint source_id;
 CREATE VIEW nas_cleanup_incident_sources AS SELECT source_id clip_id FROM nas_cleanup_incident_files;
 CREATE TABLE nas_cleanup_r2_reserved_files(kind text,source_id bigint);
 CREATE TABLE recording_collation_outputs(id bigint,nas_relative_path text,object_key text,sha256 text,size_bytes bigint,start_at timestamptz,end_at timestamptz,source_clip_ids bigint[],collation_hour_id bigint);
 CREATE TABLE r2_retirement_reservations(kind text,source_id bigint,bucket text,object_key text,retirement_class text);`)
	body, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), "0202_r2_incident_roster.sql"))
	if err != nil {
		t.Fatal(err)
	}
	exec(string(body))
	exec(`CREATE TRIGGER test_incident_output BEFORE UPDATE OR DELETE ON recording_collation_outputs FOR EACH ROW EXECUTE FUNCTION guard_nas_cleanup_output();
 CREATE TRIGGER test_incident_reservation BEFORE INSERT ON r2_retirement_reservations FOR EACH ROW EXECUTE FUNCTION guard_r2_retirement_reservation();
 INSERT INTO recording_collation_outputs VALUES(1,'output','joined/output','sha',100,now(),now()+interval '1 minute',ARRAY[1278687]::bigint[],1);`)
	for _, sql := range []string{`UPDATE recording_collation_outputs SET source_clip_ids='{}' WHERE id=1`, `UPDATE recording_collation_outputs SET object_key='changed' WHERE id=1`, `DELETE FROM recording_collation_outputs WHERE id=1`} {
		if _, err = pool.Exec(ctx, sql); err == nil || !strings.Contains(err.Error(), "retention protected") {
			t.Fatal("incident dependency could mutate", err)
		}
	}
	key := ""
	for _, d := range inverseIncidentDescriptors(t) {
		if d.SourceID == 1278687 && d.Kind == "raw" {
			key = d.Key
		}
	}
	for _, row := range []struct {
		kind string
		id   int64
		key  string
	}{{"raw", 1278687, "managed/other"}, {"raw", 999, key}, {"collated", 1, "joined/output"}, {"collated", 152088, "joined/changed"}} {
		if _, err = pool.Exec(ctx, `INSERT INTO r2_retirement_reservations VALUES($1,$2,'stoarama',$3,'owner_accepted_outside_window')`, row.kind, row.id, row.key); err == nil || !strings.Contains(err.Error(), "permanent incident") {
			t.Fatal("incident reservation accepted", row, err)
		}
	}
}
