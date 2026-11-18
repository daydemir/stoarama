package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
)

func TestFinishPermanentIncidentsWithoutReservations(t *testing.T) {
	pool, _, _ := finishFixture(t, 0)
	ctx := context.Background()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0202_r2_incident_roster.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Use the exact immutable production roster, with no production plan data.
	sql := string(migration)
	sql = sql[strings.Index(sql, "CREATE VIEW nas_cleanup_incident_roster AS"):strings.Index(sql, "-- Preserve compatibility")]
	if _, err = pool.Exec(ctx, `DROP VIEW nas_cleanup_incident_sources; DROP TABLE nas_cleanup_incident_roster;`+sql+`CREATE VIEW nas_cleanup_incident_sources AS SELECT DISTINCT unnest(raw_source_ids) clip_id FROM nas_cleanup_incident_roster;`); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	if err = pool.QueryRow(ctx, `SELECT array_agg(clip_id ORDER BY clip_id) FROM nas_cleanup_incident_sources`).Scan(&ids); err != nil || len(ids) != 292 {
		t.Fatalf("roster sources=%d err=%v", len(ids), err)
	}
	var mappings []struct {
		ItemID   int64 `json:"item_id"`
		SourceID int64 `json:"source_id"`
	}
	data, err := os.ReadFile("testdata/r2_inverse_incidents.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &mappings); err != nil {
		t.Fatal(err)
	}
	expected := map[int64]int64{339081: 1278687, 335808: 1389879, 335809: 811569, 335810: 1243693, 335811: 803836, 335812: 879308, 335813: 1455172, 335814: 1437485, 335815: 1459981}
	for _, m := range mappings {
		if id, ok := expected[m.ItemID]; ok {
			if id != m.SourceID {
				t.Fatal("cleanup-item mapping changed", m)
			}
			delete(expected, m.ItemID)
		}
	}
	if len(expected) != 0 {
		t.Fatal("missing cleanup-item mappings", expected)
	}
	policy, windows, _ := api.Keep28Policy()
	window := windows[3]
	header := nasCopyHeader{PolicySHA256: policy}
	items := make([]api.NASR2CopyItem, 0, len(ids)+1)
	for _, id := range ids {
		i := api.NASR2CopyItem{Kind: api.NASR2CopyRaw, SourceID: id, RecordingID: 2, StreamID: 3, Bucket: "bucket", ObjectKey: "managed/incident-" + time.Unix(id, 0).Format("150405") + ".mp4", RelativePath: "incident/" + time.Unix(id, 0).Format("20060102150405") + ".mp4", SizeBytes: 100, SHA256: strings.Repeat("a", 64), StartAt: window.Start, EndAt: window.Start.Add(time.Minute)}
		items = append(items, i)
	}
	// Key-only alias has no incident source identity.
	alias := items[0]
	alias.SourceID = 9000001
	alias.RelativePath = "incident/key-alias.mp4"
	if err = pool.QueryRow(ctx, `SELECT bucket,object_key FROM nas_cleanup_incident_roster WHERE source_id=1278687 AND kind='raw'`).Scan(&alias.Bucket, &alias.ObjectKey); err != nil {
		t.Fatal(err)
	}
	items = append(items, alias)
	for _, i := range items {
		if _, err = pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,bucket,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, i.SourceID, i.RecordingID, i.Bucket, i.ObjectKey, i.SHA256, i.SizeBytes, i.RelativePath, i.StartAt, i.EndAt); err != nil {
			t.Fatal(err)
		}
	}
	// Model all 247 dependency-only sources with source holds but no RAW
	// reservation, then remove those synthetic holds before installing 0209.
	if _, err = pool.Exec(ctx, `INSERT INTO nas_cleanup_plans(id,mode) VALUES(77,'enabled'); INSERT INTO nas_cleanup_source_holds SELECT clip_id,77 FROM nas_cleanup_incident_sources WHERE clip_id NOT IN(SELECT source_id FROM nas_cleanup_incident_roster WHERE kind='raw')`); err != nil {
		t.Fatal(err)
	}
	var holds int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM nas_cleanup_source_holds`).Scan(&holds); err != nil || holds != 247 {
		t.Fatalf("dependency-only holds=%d %v", holds, err)
	}
	for _, held := range []bool{true, false} {
		if !held {
			if _, err = pool.Exec(ctx, `DELETE FROM nas_cleanup_source_holds`); err != nil {
				t.Fatal(err)
			}
		}
		selected, _, err := finishCandidates(ctx, pool, 5000)
		if err != nil || len(selected) != 0 {
			t.Fatalf("held=%v selected=%v err=%v", held, selected, err)
		}
		for _, i := range items {
			if err = queueNASR2CopyItem(ctx, pool, header, strings.Repeat("b", 64), i, true); err == nil {
				t.Fatalf("incident imported held=%v source=%d", held, i.SourceID)
			}
		}
	}
	var queued, sources, reservations int
	if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM nas_r2_copy_items),(SELECT count(*) FROM recording_clips),(SELECT count(*) FROM nas_cleanup_reserved_files)`).Scan(&queued, &sources, &reservations); err != nil || queued != 0 || sources != 293 || reservations != 0 {
		t.Fatalf("unexpected writes queue=%d sources=%d reservations=%d %v", queued, sources, reservations, err)
	}
}
