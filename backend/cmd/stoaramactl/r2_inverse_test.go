package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

type inverseTestListing []r2.ObjectInfo

func (s inverseTestListing) Bucket() string { return "bucket" }
func (s inverseTestListing) ListPrefix(_ context.Context, prefix string, limit int, fn func(r2.ObjectInfo) error) error {
	if prefix != "" || limit != 0 {
		return errors.New("listing must cover full bucket")
	}
	for _, o := range s {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}
func inverseTestProtection() inverseProtection {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(28 * 24 * time.Hour)
	return inverseProtection{Identities: map[string][]inverseIdentity{}, Held: map[string]bool{}, Delivery: map[string]bool{}, KEFolders: map[string]bool{"KEFolder": true}, Windows: map[int64]api.Keep28Window{3: {StreamID: 3, Timezone: "UTC", Start: start, End: end}}, Picks: r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: start.Format(time.RFC3339), KeepEndUTC: end.Format(time.RFC3339)}, 4: {Choice: "all"}}}}
}
func TestR2InversePartitionAllClassesAndReconciliation(t *testing.T) {
	p := inverseTestProtection()
	w := p.Windows[3]
	before := w.Start.Add(-time.Hour)
	beforeEnd := w.Start.Add(-time.Minute)
	p.Identities["a"] = []inverseIdentity{{StreamID: 4}}
	p.Identities["b"] = []inverseIdentity{{StreamID: 3, Path: "file", Timezone: "UTC", Start: &w.Start, End: &w.End}}
	p.Held["d"] = true
	p.Delivery["e"] = true
	p.Identities["f"] = []inverseIdentity{{StreamID: 3, Path: "file", Timezone: "UTC", Start: &before, End: &beforeEnd}}
	listing := inverseTestListing{{Key: "a", SizeBytes: 1}, {Key: "b", SizeBytes: 2}, {Key: "d", SizeBytes: 3}, {Key: "e", SizeBytes: 4}, {Key: "experimental/v85-retired-WORK-custody/tool", SizeBytes: 5}, {Key: "f", SizeBytes: 6}, {Key: "z", SizeBytes: 7}}
	var out bytes.Buffer
	sum, err := partitionInverse(context.Background(), listing, p, "policy", &out)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Total.Objects != 7 || sum.Total.Bytes != 28 {
		t.Fatal(sum)
	}
	for _, class := range inverseClasses {
		if sum.Classes[class].Objects != 1 {
			t.Fatalf("class %s: %+v", class, sum.Classes)
		}
	}
	var entries []inverseEntry
	dec := json.NewDecoder(&out)
	for dec.More() {
		var e inverseEntry
		if err := dec.Decode(&e); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
	}
	if len(entries) != 7 {
		t.Fatal(entries)
	}
}
func TestR2InverseProtectionAliasesAndUncertainty(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*inverseProtection)
		key    string
		want   inverseClass
	}{
		{"KE path without catalog", func(*inverseProtection) {}, "managed/acct-47/KEFolder/x", inverseKE},
		{"KE partial path is unknown", func(*inverseProtection) {}, "managed/acct-47/KEFolder-extra/x", inverseUnknown},
		{"blocked overrides policy and exact inventory", func(p *inverseProtection) { p.Held["x"] = true; p.Identities["x"] = []inverseIdentity{{StreamID: 4}} }, "x", inverseHeld},
		{"delivery alias", func(p *inverseProtection) { p.Delivery["x"] = true }, "x", inverseDelivery},
		{"missing timing", func(p *inverseProtection) { p.Identities["x"] = []inverseIdentity{{StreamID: 3}} }, "x", inverseUnknown},
		{"one overlapping alias protects shared key", func(p *inverseProtection) {
			w := p.Windows[3]
			p.Identities["x"] = []inverseIdentity{{StreamID: 999}, {StreamID: 3, Path: "a", Timezone: "UTC", Start: &w.Start, End: &w.End}}
		}, "x", inverseKeep28},
		{"no window for stream", func(p *inverseProtection) { p.Identities["x"] = []inverseIdentity{{StreamID: 999}} }, "x", inverseUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := inverseTestProtection()
			tc.change(&p)
			if got, _ := p.classify(r2.ObjectInfo{Key: tc.key}); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
func TestR2InverseRefusesBrokenListing(t *testing.T) {
	for _, listing := range []inverseTestListing{{{Key: "a"}, {Key: "a"}}, {{Key: "b"}, {Key: "a"}}, {{Key: "a", SizeBytes: -1}}, {{Key: ""}}} {
		if _, err := partitionInverse(context.Background(), listing, inverseTestProtection(), "policy", &bytes.Buffer{}); err == nil {
			t.Fatal("accepted broken full listing", listing)
		}
	}
}
func TestR2InverseLiveHoldTables(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_holds")
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE TABLE recordings(id bigint,stream_id bigint,cron_timezone text);
 CREATE TABLE streams(id bigint,local_timezone text);
 CREATE TABLE recording_clips(id bigint,recording_id bigint,recording_job_id bigint DEFAULT 1,bucket text,object_key text,display_path text,clip_start_at timestamptz,clip_end_at timestamptz);
 CREATE TABLE recording_collation_hours(id bigint,recording_id bigint);
 CREATE TABLE recording_collation_outputs(id bigint,collation_hour_id bigint,object_key text,nas_relative_path text,start_at timestamptz,end_at timestamptz,source_clip_ids bigint[]);
 CREATE TABLE nas_cleanup_plans(id bigint,mode text,account_id bigint,connection_id bigint,label text,manifest jsonb);
 CREATE TABLE nas_cleanup_items(id bigserial,plan_id bigint,ordinal bigint,delete_ever_issued boolean);
 CREATE TABLE nas_cleanup_source_holds(plan_id bigint,clip_id bigint);
 CREATE TABLE r2_retirement_reservations(bucket text,object_key text);
 CREATE TABLE r2_object_retirements(bucket text,object_key text);
 CREATE TABLE nas_restore_requests(clip_id bigint,state text);
 CREATE TABLE recording_joined_source_snapshots(clip_id bigint);
 CREATE TABLE recording_joined_snapshot_scopes(recording_id bigint,recording_job_id bigint,high_water_clip_id bigint,batch_record_id bigint);
 CREATE TABLE recording_joined_batches(id bigint,state text);
 CREATE TABLE recording_joined_dry_run_scopes(recording_id bigint,recording_job_id bigint,high_water_clip_id bigint);
 CREATE TABLE nas_r2_copy_items(item jsonb,state text,blocked_reason text);
 CREATE TABLE delivery_keep_holds(bucket text,object_key text);
 CREATE TABLE delivery_nas_source_holds(clip_id bigint);
 CREATE TABLE delivery_keep_output_dependencies(output_id bigint,recording_id bigint);
 CREATE TABLE nas_cleanup_incident_files(kind text,source_id bigint);
 CREATE VIEW nas_cleanup_incident_sources AS SELECT source_id clip_id FROM nas_cleanup_incident_files;
 INSERT INTO recordings VALUES(1,3,'UTC'); INSERT INTO streams VALUES(3,'UTC');
 INSERT INTO recording_collation_hours VALUES(1,1);`)
	body, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), "0177_nas_cleanup_reserved_files.sql"))
	if err != nil {
		t.Fatal(err)
	}
	exec(string(body))
	for id, key := range []string{"reserved", "source_hold", "incident", "delivery_source", "blocked", "delivery_output", "ordinary", "snapshot_scoped", "dryrun_scoped"} {
		exec(`INSERT INTO recording_clips(id,recording_id,bucket,object_key,display_path,clip_start_at,clip_end_at) VALUES($1,1,'bucket',$2,$2,'2026-06-01','2026-06-02')`, id+1, key)
	}
	exec(`INSERT INTO recording_joined_batches VALUES(1,'snapshotting');
 INSERT INTO recording_joined_snapshot_scopes VALUES(1,8,8,1);
 INSERT INTO recording_joined_dry_run_scopes VALUES(1,9,9);
 UPDATE recording_clips SET recording_job_id=id WHERE id IN (8,9);
 INSERT INTO nas_cleanup_plans VALUES(1,'retired',47,13,'source','[]'),(2,'active',47,13,'reserved','[{"kind":"raw","source_id":1}]');
 INSERT INTO nas_cleanup_items(plan_id,ordinal,delete_ever_issued) VALUES(1,1,true); INSERT INTO nas_cleanup_source_holds VALUES(1,2);
 INSERT INTO nas_cleanup_incident_files VALUES('raw',3);
 INSERT INTO recording_collation_outputs VALUES(20,1,'incident_collated','incident-collated','2026-06-01','2026-06-02',ARRAY[3]::bigint[]);
 INSERT INTO r2_retirement_reservations VALUES('bucket','reservation'),('other','other_reservation');
 INSERT INTO r2_object_retirements VALUES('bucket','retired');
 INSERT INTO nas_r2_copy_items VALUES('{"bucket":"bucket","object_key":"blocked"}','blocked','checksum_mismatch'),('{"bucket":"other","object_key":"ordinary", "snapshot_scoped", "dryrun_scoped"}','blocked','final_conflict');
 INSERT INTO delivery_keep_holds VALUES('bucket','direct_delivery');
 INSERT INTO delivery_nas_source_holds VALUES(4);
 INSERT INTO recording_collation_outputs VALUES(21,1,'delivery_output','delivery-output','2026-06-01','2026-06-02',ARRAY[4]::bigint[]);`)
	p := inverseTestProtection()
	p, err = loadInverseProtection(ctx, pool, "bucket", p.Picks, p.Windows)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"reserved", "source_hold", "incident", "incident_collated", "reservation", "retired", "blocked", "snapshot_scoped", "dryrun_scoped"} {
		if got, _ := p.classify(r2.ObjectInfo{Key: key}); got != inverseHeld {
			t.Fatalf("%s got %s", key, got)
		}
	}
	for _, key := range []string{"direct_delivery", "delivery_source", "delivery_output"} {
		if got, _ := p.classify(r2.ObjectInfo{Key: key}); got != inverseDelivery {
			t.Fatalf("%s got %s", key, got)
		}
	}
	if got, _ := p.classify(r2.ObjectInfo{Key: "ordinary"}); got != inverseDeletable {
		t.Fatal(got)
	}
	// The frozen incident roster survives plan transitions independently.
	exec(`UPDATE nas_cleanup_plans SET mode='retired'; DELETE FROM nas_cleanup_source_holds; DELETE FROM nas_cleanup_items`)
	p, err = loadInverseProtection(ctx, pool, "bucket", p.Picks, p.Windows)
	if err != nil || !p.Held["incident"] || !p.Held["incident_collated"] {
		t.Fatal("lost incident protection", err)
	}
}

func TestR2InverseCatalogPathBoundsAndDeferredCollated(t *testing.T) {
	p := inverseTestProtection()
	w := p.Windows[3]
	a := w.Start.Add(-48 * time.Hour)
	b := w.Start.Add(-24 * time.Hour)
	identity := inverseIdentity{StreamID: 3, Path: "KEFolder/footage.mp4", Timezone: "UTC", Start: &a, End: &b, Kind: "collated"}
	p.Identities["joined/hash.mp4"] = []inverseIdentity{identity}
	if got, _ := p.classify(r2.ObjectInfo{Key: "joined/hash.mp4"}); got != inverseKE {
		t.Fatal(got)
	}
	identity.Path = "ordinary/file.mp4"
	p.Identities["joined/hash.mp4"] = []inverseIdentity{identity}
	if got, _ := p.classify(r2.ObjectInfo{Key: "joined/hash.mp4"}); got != inverseHeld {
		t.Fatal(got)
	}
	for _, year := range []int{1, 1970, 2019, 2028, 2099} {
		for _, kind := range []string{"raw", "source"} {
			a := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
			b := a.Add(time.Hour)
			p.Identities["raw"] = []inverseIdentity{{StreamID: 3, Path: "ordinary/file.mp4", Timezone: "UTC", Start: &a, End: &b, Kind: kind}}
			if got, _ := p.classify(r2.ObjectInfo{Key: "raw"}); got != inverseUnknown {
				t.Fatalf("year %d kind %s got %s", year, kind, got)
			}
		}
	}
	p.Identities["raw"] = []inverseIdentity{{StreamID: 3, Path: "file", Timezone: "UTC", Start: &a, End: &b}, {StreamID: 3, Path: "file", Timezone: "UTC", Start: &a, End: &b}}
	if got, _ := p.classify(r2.ObjectInfo{Key: "raw"}); got != inverseUnknown {
		t.Fatal("ambiguous aliases", got)
	}
}
