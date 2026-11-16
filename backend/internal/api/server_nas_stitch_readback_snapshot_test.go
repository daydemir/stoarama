package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestNASStitchReadbackSnapshotUsesScopedReadOnlyDBFacts(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	f := &cleanupFixture{pool: pool, ctx: ctx}
	stitchMigration(t, f)
	_, err := pool.Exec(ctx, `ALTER TABLE connections ADD COLUMN nas_host jsonb,ADD COLUMN nas_host_reported_at timestamptz,
 ADD COLUMN inventory_mismatches bigint;
 INSERT INTO streams VALUES(17208,'Europe/Prague');INSERT INTO recordings VALUES(444,47,17208,'Europe/Prague');
 INSERT INTO nas_stitch_scopes(id,stream_id,start_at,end_at,reason,approved_until) VALUES(1,17208,'2026-08-09T09:00Z','2026-08-09T10:01Z','SYNTHETIC read-only fixture',now()+interval '1 hour')`)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(proofPlanFixture())
	if _, err = pool.Exec(ctx, `INSERT INTO nas_stitch_tasks(id,scope_id,recording_id,hour_at,plan,plan_sha256,state) VALUES(1,1,444,'2026-08-09T09:00Z',$1,$2,'leased')`, string(raw), nasStitchProofSourceSHA); err != nil {
		t.Fatal(err)
	}
	status, _, _, _ := syntheticNASStitchReaderStatus(t)
	host, _ := json.Marshal(nasHostFacts{System: "Linux", Machine: "x86_64", CPUModel: "SYNTHETIC", CPUCount: 1, Release: &nasReleaseHealth{Stable: true}})
	statusBody, _ := json.Marshal(map[string]any{"stitch_reader": status})
	if _, err = pool.Exec(ctx, `UPDATE connections SET client_version=$1,client_boot_id=$2,last_seen_at=clock_timestamp(),nas_host=$3,nas_host_reported_at=clock_timestamp(),nas_stitch_status=$4,nas_stitch_status_at=clock_timestamp(),inventory_mismatches=0 WHERE id=13`, status.Reader.Version, status.Reader.BootID, string(host), string(statusBody)); err != nil {
		t.Fatal(err)
	}
	server := &Server{pool: pool}
	before := time.Now().UTC()
	value, err := server.nasStitchReadbackSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if value.Now.Before(before) || value.Now.After(time.Now().UTC()) || value.SourceSHA != nasStitchProofSourceSHA || value.Version != status.Reader.Version || !value.Facts.InventoryMismatches.Valid || value.Facts.InventoryMismatches.Int64 != 0 {
		t.Fatal("snapshot did not bind actual DB facts/time")
	}
	// The original scope, lease and plan are untouched; no catalogue row exists.
	var state, sha string
	var attempt int
	if err = pool.QueryRow(ctx, `SELECT state,plan_sha256,attempt FROM nas_stitch_tasks WHERE id=1`).Scan(&state, &sha, &attempt); err != nil || state != "leased" || sha != nasStitchProofSourceSHA || attempt != 0 {
		t.Fatalf("read mutated original task: %s %s %d %v", state, sha, attempt, err)
	}
	// Admission refusal does not turn a changed source into a new plan.
	if _, err = pool.Exec(ctx, `UPDATE nas_stitch_tasks SET plan_sha256=$1 WHERE id=1`, strings.Repeat("0", 64)); err == nil {
		t.Fatal("existing immutable task guard was bypassed")
	}
	if _, err = pool.Exec(ctx, `UPDATE nas_stitch_scopes SET paused=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err = server.nasStitchReadbackSnapshot(ctx); err == nil {
		t.Fatal("paused scope admitted")
	}
}
