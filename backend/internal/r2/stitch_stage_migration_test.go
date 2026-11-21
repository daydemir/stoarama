package r2

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 0230 lands on a schema that already carries deployed legacy evidence. This
// witness applies the real migration over a populated schema that still has the
// deployed 0219 guard and no 0230 column:
//
//   - both deployed row shapes survive byte-for-byte, with no new provenance
//     invented and no legacy stamp rejudged;
//   - both stay held afterwards, so a migration can never quietly unblock a
//     fence, a dependency or an owner hold.
//
// This is the upgrade the R2 controller will actually run, so a migration that
// edited or resolved existing intents fails here instead of in production.
func TestR2StitchStageProvenanceMigrationPreservesDeployedLegacyRows(t *testing.T) {
	// A real deployed schema: the chain stops immediately before 0230.
	f := newStageFixtureAt(t, "upgrade", "0230_nas_stitch_stage_wire_provenance.sql")
	f.seedHour(t, 500)
	f.stage(t, 601)
	f.stage(t, 602)

	// The deployed commands' own writes: a bare 404, and a later strict replay.
	// No guard is disabled: these are exactly what 0217 and 0219 admitted.
	f.insert(`INSERT INTO nas_stitch_r2_stage_retirements(object_id) VALUES(601),(602)`)
	if err := f.exec(`UPDATE nas_stitch_r2_stage_retirements SET absence_observed_at=clock_timestamp() WHERE object_id=601`); err != nil {
		t.Fatal(err)
	}
	if err := f.exec(`UPDATE nas_stitch_r2_stage_retirements SET absence_observed_at=clock_timestamp(),
	 strict_absence_observed_at=clock_timestamp() WHERE object_id=602`); err != nil {
		t.Fatal(err)
	}
	type deployed struct {
		id              int64
		absence, strict *time.Time
	}
	rows, err := f.pool.Query(f.ctx, `SELECT object_id,absence_observed_at,strict_absence_observed_at
	 FROM nas_stitch_r2_stage_retirements ORDER BY object_id`)
	if err != nil {
		t.Fatal(err)
	}
	var before []deployed
	for rows.Next() {
		var row deployed
		if err := rows.Scan(&row.id, &row.absence, &row.strict); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		before = append(before, row)
	}
	rows.Close()
	if len(before) != 2 || before[0].absence == nil || before[0].strict != nil || before[1].strict == nil {
		t.Fatalf("deployed legacy fixture differs: %+v", before)
	}

	// Apply the real migration now, exactly as the migrator would.
	path, err := filepath.Abs(filepath.Join("..", "..", "..", "infra", "sql", "migrations",
		"0230_nas_stitch_stage_wire_provenance.sql"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(f.ctx, string(body)); err != nil {
		t.Fatalf("0230 did not apply to a populated schema: %v", err)
	}
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}

	// Every deployed row survives exactly, with no wire provenance invented.
	for _, want := range before {
		var got struct{ absence, strict, wire *time.Time }
		if err := f.pool.QueryRow(f.ctx, `SELECT absence_observed_at,strict_absence_observed_at,wire_absence_observed_at
		 FROM nas_stitch_r2_stage_retirements WHERE object_id=$1`, want.id).
			Scan(&got.absence, &got.strict, &got.wire); err != nil {
			t.Fatal(err)
		}
		if !sameStamp(got.absence, want.absence) || !sameStamp(got.strict, want.strict) || got.wire != nil {
			t.Fatalf("0230 altered deployed row %d: %v %v %v", want.id, got.absence, got.strict, got.wire)
		}
		var resolved bool
		f.row(`SELECT nas_stitch_r2_stage_retirement_resolved(absence_observed_at,strict_absence_observed_at,
		 wire_absence_observed_at) FROM nas_stitch_r2_stage_retirements WHERE object_id=$1`, &resolved, want.id)
		if resolved {
			t.Fatalf("deployed legacy row %d became resolved after 0230", want.id)
		}
	}
	// A deployed row that 0230 left held still defends the fence.
	f.insert(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256,drain_not_before)
	 VALUES('stoarama',$1,clock_timestamp()-interval '1 second')`, stageFixtureEndpoint)
	err = f.exec(`DELETE FROM r2_retirement_writer_fences`)
	if err == nil || !strings.Contains(err.Error(), "have not drained") {
		t.Fatalf("a deployed legacy row stopped holding the fence after 0230: %v", err)
	}
	// 0230's column is wired to the deployed evidence, so a legacy stamp still
	// cannot become the new proof on the upgraded schema.
	if err := f.exec(`UPDATE nas_stitch_r2_stage_retirements SET wire_absence_observed_at=absence_observed_at
	 WHERE object_id=601`); err == nil || !strings.Contains(err.Error(), "one-shot") {
		t.Fatalf("the upgrade let a deployed legacy stamp become the wire proof: %v", err)
	}
	var improvised int
	f.row(`SELECT count(*) FROM nas_stitch_r2_stage_retirements WHERE wire_absence_observed_at IS NOT NULL`, &improvised)
	if improvised != 0 {
		t.Fatalf("0230 invented wire provenance for %d deployed rows", improvised)
	}
}

func sameStamp(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}
