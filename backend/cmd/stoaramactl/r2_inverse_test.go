package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

type inverseTestListing []r2.ObjectInfo

func (s inverseTestListing) Bucket() string         { return "bucket" }
func (s inverseTestListing) EndpointSHA256() string { return r2tSHA }
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
		// The collated class exists only under the explicit reviewed owner
		// footage scope; default classification keeps collated keys deferred.
		if class == inverseCollated {
			if sum.Classes[class].Objects != 0 {
				t.Fatalf("default partition emitted collated class: %+v", sum.Classes)
			}
			continue
		}
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

func TestR2InversePartitionLastModifiedCountsAreOrthogonal(t *testing.T) {
	p := inverseTestProtection()
	w := p.Windows[3]
	start, end := w.Start.Add(-time.Hour), w.Start.Add(-time.Minute)
	p.Identities["ke"] = []inverseIdentity{{StreamID: 4}}
	p.Identities["eligible"] = []inverseIdentity{{StreamID: 3, Path: "file", Timezone: "UTC", Start: &start, End: &end}}
	listing := inverseTestListing{
		{Key: "ke", SizeBytes: 10, LastModified: inverseRetirementCutoff()},
		{Key: "unknown", SizeBytes: 20},
		{Key: "eligible", SizeBytes: 30, LastModified: inverseRetirementCutoff().Add(-time.Second)},
	}
	var out bytes.Buffer
	sum, err := partitionInverse(context.Background(), listing, p, "policy", &out)
	if err != nil || sum.Total.Objects != 3 || sum.Total.Bytes != 60 || sum.LastModifiedExcluded.Objects != 2 || sum.LastModifiedExcluded.Bytes != 30 {
		t.Fatalf("cutoff accounting: %+v %v", sum, err)
	}
	if sum.Classes[inverseKE].Objects != 1 || sum.Classes[inverseUnknown].Objects != 1 || sum.Classes[inverseDeletable].Objects != 1 {
		t.Fatal("cutoff accounting replaced protection classes", sum.Classes)
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
	for _, tc := range []struct {
		name, predicate string
		bad             r2.ObjectInfo
	}{
		{"duplicate", "duplicate_key", r2.ObjectInfo{Key: "a"}},
		{"negative size", "negative_size", r2.ObjectInfo{Key: "b", SizeBytes: -1}},
		{"empty key", "empty_key", r2.ObjectInfo{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listing := inverseTestListing{{Key: "a", SizeBytes: 1}, tc.bad, {Key: "z"}}
			var out bytes.Buffer
			sum, err := partitionInverse(context.Background(), listing, inverseTestProtection(), "policy", &out)
			want := fmt.Sprintf("predicate=%s previous=%q offending=%q size=%d", tc.predicate, "a", tc.bad.Key, tc.bad.SizeBytes)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("error=%v, want %s", err, want)
			}
			if sum.Total.Objects != 1 || strings.Count(out.String(), "\n") != 1 {
				t.Fatalf("emitted rejected entry: sum=%+v output=%s", sum.Total, &out)
			}
		})
	}
}

func TestR2InversePartitionR2PrefixOrderAndNonAdjacentDuplicate(t *testing.T) {
	// Exact pair reproduced by the read-only R2 LIST on 2026-11-18.
	listing := inverseTestListing{
		{Key: "relay-releases/latest-05739dfe.json.sig", ETag: "db6b3c924cd1eb6d1239fca1d8a26551", SizeBytes: 89},
		{Key: "relay-releases/latest-05739dfe.json", ETag: "ba2c866dae94df602275a78078bfc73c", SizeBytes: 3661},
	}
	var out bytes.Buffer
	sum, err := partitionInverse(context.Background(), listing, inverseTestProtection(), "policy", &out)
	if err != nil || sum.Total.Objects != 2 || sum.Total.Bytes != 3750 || strings.Count(out.String(), "\n") != 2 {
		t.Fatalf("sum=%+v error=%v output=%s", sum.Total, err, &out)
	}
	// The duplicate need not be adjacent and may carry different metadata.
	listing = append(listing, r2.ObjectInfo{Key: listing[0].Key, SizeBytes: 100})
	out.Reset()
	sum, err = partitionInverse(context.Background(), listing, inverseTestProtection(), "policy", &out)
	want := fmt.Sprintf("predicate=duplicate_key previous=%q offending=%q size=100", listing[1].Key, listing[0].Key)
	if err == nil || !strings.Contains(err.Error(), want) || sum.Total.Objects != 2 || strings.Count(out.String(), "\n") != 2 {
		t.Fatalf("sum=%+v error=%v output=%s", sum.Total, err, &out)
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
 CREATE TABLE recording_clips(id bigint,recording_id bigint,recording_job_id bigint DEFAULT 1,bucket text,object_key text,display_path text,clip_start_at timestamptz,clip_end_at timestamptz,sha256 text DEFAULT '',size_bytes bigint DEFAULT 1);
 CREATE TABLE recording_collation_hours(id bigint,recording_id bigint);
 CREATE TABLE recording_collation_outputs(id bigint,collation_hour_id bigint,object_key text,nas_relative_path text,start_at timestamptz,end_at timestamptz,source_clip_ids bigint[],sha256 text DEFAULT '',size_bytes bigint DEFAULT 1);
 CREATE TABLE nas_cleanup_plans(id bigint,mode text,account_id bigint,connection_id bigint,label text,manifest jsonb);
 CREATE TABLE nas_cleanup_items(id bigserial,plan_id bigint,ordinal bigint,delete_ever_issued boolean,state text DEFAULT 'pending');
 CREATE TABLE nas_cleanup_source_holds(plan_id bigint,clip_id bigint);
 CREATE TABLE r2_retirement_reservations(bucket text,object_key text);
 CREATE TABLE r2_object_retirements(bucket text,object_key text);
 CREATE TABLE nas_restore_requests(clip_id bigint,state text);
 CREATE TABLE recording_joined_source_snapshots(clip_id bigint,batch_record_id bigint DEFAULT 1);
 CREATE TABLE r2_joined_batch_releases(batch_record_id bigint);
 CREATE TABLE nas_stitch_r2_input_holds(bucket text,object_key text,released_at timestamptz);
 CREATE FUNCTION r2_joined_source_released(p_clip_id bigint) RETURNS boolean LANGUAGE sql STABLE AS $f$
  SELECT EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=p_clip_id)
   AND NOT EXISTS(SELECT 1 FROM recording_joined_source_snapshots s WHERE s.clip_id=p_clip_id
    AND NOT EXISTS(SELECT 1 FROM r2_joined_batch_releases r WHERE r.batch_record_id=s.batch_record_id)) $f$;
 CREATE TABLE r2_owner_keep_prefixes(bucket text,prefix text);
 CREATE TABLE r2_delivery_exemptions(bucket text,object_key text,class text);
 CREATE FUNCTION r2_delivery_exempt(text,text) RETURNS boolean LANGUAGE sql AS $f$ SELECT false $f$;
 CREATE TABLE r2_inverse_ke_epoch(epoch bigint); INSERT INTO r2_inverse_ke_epoch VALUES(0);
 CREATE TABLE recording_joined_snapshot_scopes(recording_id bigint,recording_job_id bigint,high_water_clip_id bigint,batch_record_id bigint);
 CREATE TABLE recording_joined_batches(id bigint,state text);
 CREATE TABLE recording_joined_dry_run_scopes(recording_id bigint,recording_job_id bigint,high_water_clip_id bigint);
 CREATE TABLE nas_r2_copy_items(item jsonb,state text,blocked_reason text);
 CREATE TABLE delivery_keep_holds(bucket text,object_key text,relative_path text DEFAULT '',sha256 text DEFAULT '',size_bytes bigint DEFAULT 1,etag text DEFAULT 'etag');
 CREATE TABLE delivery_nas_source_holds(clip_id bigint);
 CREATE TABLE delivery_keep_output_dependencies(output_id bigint,recording_id bigint);
 CREATE TABLE nas_cleanup_incident_files(kind text,source_id bigint);
 CREATE TABLE nas_cleanup_incident_roster(kind text,source_id bigint,bucket text,object_key text);
 CREATE VIEW nas_cleanup_incident_sources AS SELECT source_id clip_id FROM nas_cleanup_incident_files;
 INSERT INTO recordings VALUES(1,3,'UTC'); INSERT INTO streams VALUES(3,'UTC');
 INSERT INTO recording_collation_hours VALUES(1,1);`)
	body, err := os.ReadFile(filepath.Join(joinedPurgeMigrationsDir(), "0177_nas_cleanup_reserved_files.sql"))
	if err != nil {
		t.Fatal(err)
	}
	exec(string(body))
	for id, key := range []string{"reserved", "source_hold", "incident", "delivery_source", "blocked", "delivery_output", "ordinary", "snapshot_scoped", "dryrun_scoped", "delivery_alias", "delivery_normalized", "delivery_dependency", "delivery_output_alias"} {
		exec(`INSERT INTO recording_clips(id,recording_id,bucket,object_key,display_path,clip_start_at,clip_end_at) VALUES($1,1,'bucket',$2,$2,'2026-06-01','2026-06-02')`, id+1, key)
	}
	exec(`INSERT INTO recording_joined_batches VALUES(1,'snapshotting');
 INSERT INTO recording_joined_snapshot_scopes VALUES(1,8,8,1);
 INSERT INTO recording_joined_dry_run_scopes VALUES(1,9,9);
 UPDATE recording_clips SET recording_job_id=id WHERE id IN (8,9);
 INSERT INTO nas_cleanup_plans VALUES(1,'retired',47,13,'source','[]'),(2,'active',47,13,'reserved','[{"kind":"raw","source_id":1}]');
 INSERT INTO nas_cleanup_items(plan_id,ordinal,delete_ever_issued) VALUES(1,1,true); INSERT INTO nas_cleanup_source_holds VALUES(1,2);
 INSERT INTO nas_cleanup_incident_files VALUES('raw',3);
 INSERT INTO recording_collation_outputs(id,collation_hour_id,object_key,nas_relative_path,start_at,end_at,source_clip_ids) VALUES(20,1,'incident_collated','incident-collated','2026-06-01','2026-06-02',ARRAY[3]::bigint[]);
 INSERT INTO r2_retirement_reservations VALUES('bucket','reservation'),('other','other_reservation');
 INSERT INTO r2_object_retirements VALUES('bucket','retired');
 INSERT INTO nas_r2_copy_items VALUES('{"bucket":"bucket","object_key":"blocked"}','blocked','checksum_mismatch'),('{"bucket":"other","object_key":"ordinary"}','blocked','final_conflict');
 INSERT INTO delivery_keep_holds(bucket,object_key) VALUES('bucket','direct_delivery');
 INSERT INTO delivery_nas_source_holds VALUES(4);
 INSERT INTO recording_collation_outputs(id,collation_hour_id,object_key,nas_relative_path,start_at,end_at,source_clip_ids) VALUES(21,1,'delivery_output','delivery-output','2026-06-01','2026-06-02',ARRAY[4]::bigint[]);
 UPDATE recording_clips SET sha256=repeat('c',64),size_bytes=123 WHERE id=10;
 UPDATE recording_clips SET display_path='joined/normalize-held' WHERE id=11;
 INSERT INTO delivery_keep_holds(bucket,object_key,sha256,size_bytes) VALUES('bucket','content-alias',repeat('c',64),123);
 INSERT INTO delivery_keep_holds(bucket,object_key,relative_path) VALUES('bucket','normalized-alias','normalize-held'),('bucket','output-alias','alias-output');
 INSERT INTO recording_collation_outputs(id,collation_hour_id,object_key,nas_relative_path,start_at,end_at,source_clip_ids) VALUES(22,1,'live-held-output','live-output','2026-06-01','2026-06-02',ARRAY[12]::bigint[]),(23,1,'alias-held-output','joined/alias-output','2026-06-01','2026-06-02',ARRAY[13]::bigint[]);
 INSERT INTO delivery_keep_output_dependencies VALUES(22,1);`)
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
	for _, key := range []string{"direct_delivery", "delivery_source", "delivery_output", "delivery_alias", "delivery_normalized", "delivery_dependency", "delivery_output_alias"} {
		if got, _ := p.classify(r2.ObjectInfo{Key: key}); got != inverseDelivery {
			t.Fatalf("%s got %s", key, got)
		}
	}
	if got, _ := p.classify(r2.ObjectInfo{Key: "ordinary"}); got != inverseDeletable {
		t.Fatal(got)
	}
	t.Run("KE_alias_after_partition_uses_shared_normalization", func(t *testing.T) {
		exec(`UPDATE recording_clips SET display_path='F/candidate.mp4' WHERE id=7;INSERT INTO recordings VALUES(2,4,'UTC');INSERT INTO streams VALUES(4,'UTC')`)
		picks := p.Picks
		picks.Choices[4] = r2RetirePick{Choice: "all"}
		before, err := loadInverseProtection(ctx, pool, "bucket", picks, p.Windows)
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := before.classify(r2.ObjectInfo{Key: "ordinary"}); got != inverseDeletable {
			t.Fatal("bad pre-alias fixture", got)
		}
		exec(`INSERT INTO recording_clips(id,recording_id,bucket,object_key,display_path,clip_start_at,clip_end_at) VALUES(100,2,'bucket','hashed-ke-alias','joined/F/ke.mp4','2026-06-01','2026-06-02')`)
		apply, err := loadInverseApplyProtection(ctx, pool, "bucket", picks, p.Windows)
		if err != nil {
			t.Fatal(err)
		}
		partition, err := loadInverseProtection(ctx, pool, "bucket", picks, p.Windows)
		if err != nil {
			t.Fatal(err)
		}
		if !apply.KEFolders["F"] || !partition.KEFolders["F"] || apply.KEFolders["joined"] {
			t.Fatal("KE normalization drift", apply.KEFolders, partition.KEFolders)
		}
		before.KEFolders = apply.KEFolders
		if got, _ := before.classify(r2.ObjectInfo{Key: "ordinary"}); got != inverseKE {
			t.Fatal("fresh KE alias reached apply", got)
		}
	})
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
	// Unreviewed default: a collated key is deferred regardless of its identity.
	if got, _ := p.classify(r2.ObjectInfo{Key: "joined/hash.mp4"}); got != inverseHeld {
		t.Fatal(got)
	}
	// Under the reviewed collated scope, a KE path alias on the output protects it.
	p.ReviewedCollated = true
	p.Collated = map[string]inverseCollatedAssessment{"joined/hash.mp4": {Key: "joined/hash.mp4", Path: "KEFolder/footage.mp4", StreamID: 3, Start: &a, End: &b, Timezone: "UTC", SHA: "x"}}
	if got, _ := p.classify(r2.ObjectInfo{Key: "joined/hash.mp4"}); got != inverseKE {
		t.Fatalf("KE path alias not protected: %s", got)
	}
	identity.Path = "ordinary/file.mp4"
	p.Identities["joined/hash.mp4"] = []inverseIdentity{identity}
	p.Collated["joined/hash.mp4"] = inverseCollatedAssessment{Key: "joined/hash.mp4", Path: "ordinary/file.mp4", StreamID: 3, Start: &a, End: &b, Timezone: "UTC", SHA: "x"}
	if got, _ := p.classify(r2.ObjectInfo{Key: "joined/hash.mp4"}); got == inverseCollated || got == inverseDeletable {
		t.Fatalf("unmapped collated key became eligible: %s", got)
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

func TestR2InversePartitionTxReadOnlyCatalogBudget(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_partition_tx")
	tx, err := beginInversePartitionTx(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var timeout, readOnly string
	if err := tx.QueryRow(ctx, `SHOW statement_timeout`).Scan(&timeout); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(ctx, `SHOW transaction_read_only`).Scan(&readOnly); err != nil {
		t.Fatal(err)
	}
	if timeout != "30min" {
		t.Fatalf("statement_timeout got %q want 30min", timeout)
	}
	if readOnly != "on" {
		t.Fatalf("transaction_read_only got %q want on", readOnly)
	}
}

// inverseKEFoldersPreJoinOracleSQL is the pre-fix loadInverseKEFolders query: its
// KE collation-source branch ran EXISTS(source_clip_ids @> ARRAY[c.id]) per clip.
// Kept verbatim as the oracle for the join rewrite's equivalence test.
const inverseKEFoldersPreJoinOracleSQL = `SELECT DISTINCT folder FROM (
 SELECT split_part(regexp_replace(c.display_path,'^joined/',''),'/',1) folder FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE c.bucket=$1 AND r.stream_id=ANY($2::bigint[])
 UNION SELECT split_part(regexp_replace(o.nas_relative_path,'^joined/',''),'/',1) FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id WHERE r.stream_id=ANY($2::bigint[])
 UNION SELECT split_part(regexp_replace(c.display_path,'^joined/',''),'/',1) FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE r.stream_id=ANY($2::bigint[]) AND EXISTS(SELECT 1 FROM recording_collation_outputs o WHERE o.source_clip_ids @> ARRAY[c.id])) folders WHERE folder<>''`

func TestR2InverseKEFoldersMatchPreJoinOracle(t *testing.T) {
	ctx := context.Background()
	pool := joinedPurgeTestPool(t, "inverse_ke_folders")
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE TABLE recordings(id bigint,stream_id bigint,cron_timezone text DEFAULT 'UTC');
 CREATE TABLE recording_clips(id bigint,recording_id bigint,bucket text,object_key text,display_path text,clip_start_at timestamptz,clip_end_at timestamptz,sha256 text DEFAULT '',size_bytes bigint DEFAULT 1);
 CREATE TABLE recording_collation_hours(id bigint,recording_id bigint);
 CREATE TABLE recording_collation_outputs(id bigint,collation_hour_id bigint,object_key text,nas_relative_path text,start_at timestamptz,end_at timestamptz,source_clip_ids bigint[],sha256 text DEFAULT '',size_bytes bigint DEFAULT 1);
 INSERT INTO recordings VALUES(1,3,'UTC'),(2,4,'UTC');`)
	picks := r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "all"}, 4: {Choice: "none"}}}
	// KE stream (3) is Keep Everything; stream 4 is not.
	exec(`INSERT INTO recording_clips VALUES
 (1,1,'bucket','ke_shared','joined/KEShared/clip.mp4','2026-06-01','2026-06-02','',1),
 (2,1,'other','ke_foreign','joined/KEOnlyRaw/deep/clip.mp4','2026-06-01','2026-06-02','',1),
 (3,1,'other','ke_not_source','joined/KENotSource/clip.mp4','2026-06-01','2026-06-02','',1),
 (4,2,'other','non_ke_source','joined/NonKE/clip.mp4','2026-06-01','2026-06-02','',1),
 (5,1,'bucket','plain','PlainFolder/clip.mp4','2026-06-01','2026-06-02','',1),
 (6,1,'bucket','empty_folder','joined/','2026-06-01','2026-06-02','',1)`)
	exec(`INSERT INTO recording_collation_hours VALUES(1,1)
 ,(2,2)`)
	// One output with several source ids and a duplicate; clip 3 and 4 are not
	// sources of anything, clip 4 is a source only of the stream-4 hour's output.
	exec(`INSERT INTO recording_collation_outputs(id,collation_hour_id,object_key,nas_relative_path,start_at,end_at,source_clip_ids) VALUES
 (10,1,'collated_hour','joined/CollatedHour/hour.mp4','2026-06-01','2026-06-02',ARRAY[1,2,6,1]::bigint[]),
 (11,2,'non_ke_collated','joined/NonKECollated/hour.mp4','2026-06-01','2026-06-02',ARRAY[4,4]::bigint[])`)
	streams := []int64{3}
	rows, err := pool.Query(ctx, inverseKEFoldersPreJoinOracleSQL, "bucket", streams)
	if err != nil {
		t.Fatal(err)
	}
	oracle := map[string]bool{}
	for rows.Next() {
		var folder string
		if err = rows.Scan(&folder); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		oracle[folder] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	got, err := loadInverseKEFolders(ctx, pool, "bucket", picks)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"KEShared": true, "KEOnlyRaw": true, "CollatedHour": true, "PlainFolder": true}
	for folder := range want {
		if !got[folder] {
			t.Fatalf("missing folder %q in %v", folder, got)
		}
	}
	for folder := range got {
		if !oracle[folder] {
			t.Fatalf("folder %q absent from pre-join oracle %v", folder, oracle)
		}
	}
	for folder := range oracle {
		if !got[folder] {
			t.Fatalf("folder %q missing from join query %v", folder, got)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("folder set %v want %v", got, want)
	}
	// KEOnlyRaw comes only from branch 3 (no bucket filter, foreign bucket): it is
	// what proves the fixture exercises the rewritten branch at all.
	if !oracle["KEOnlyRaw"] || !got["KEOnlyRaw"] {
		t.Fatalf("fixture does not exercise branch 3: %v / %v", got, oracle)
	}
	for _, folder := range []string{"KENotSource", "NonKE", "NonKECollated", ""} {
		if got[folder] || oracle[folder] {
			t.Fatalf("unexpected folder %q in %v / %v", folder, got, oracle)
		}
	}
}

// Owner rule: Keep Everything stays in R2. A stream the keep20 policy marks KE
// (78) is protected by the inverse/keep28 and nonraw classifiers even when its
// pick is "none" and the footage lies outside every window.
func TestR2InverseKeepEverythingPolicyWinsOverPick(t *testing.T) {
	p := inverseTestProtection()
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	end := at.Add(time.Minute)
	p.Windows[78] = api.Keep28Window{StreamID: 78, Timezone: "UTC", Start: at.AddDate(0, -3, 0), End: at.AddDate(0, -2, 0)}
	p.Picks.Choices[78] = r2RetirePick{Choice: "none"}
	key := "managed/acct-47/78-raw.mp4"
	p.Identities[key] = []inverseIdentity{{StreamID: 78, Path: "r/78-raw.mp4", Start: &at, End: &end, Timezone: "UTC", Kind: "raw"}}
	if class, reason := p.classify(r2.ObjectInfo{Key: key, SizeBytes: 1}); class != inverseKE || reason != "keep_everything_stream" {
		t.Fatalf("KE raw classified %v %s", class, reason)
	}
	p.ReviewedCollated = true
	x := inverseCollatedAssessment{ID: 1, StreamID: 78, Key: "joined/goodplus-x/objects/" + strings.Repeat("a", 64) + ".mp4", Start: &at, End: &end, Timezone: "UTC"}
	if class, reason := p.classifyCollated(r2.ObjectInfo{Key: x.Key, SizeBytes: 1}, x, true); class != inverseKE || reason != "keep_everything_stream" {
		t.Fatalf("KE collated classified %v %s", class, reason)
	}
	if !p.keepEverythingStream(78) || !p.keepEverythingStream(4) || p.keepEverythingStream(3) {
		t.Fatal("Keep Everything is the policy set together with pick all")
	}
}
