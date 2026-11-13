package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

type deliveryRegistryStore struct {
	current, pinned []byte
	changed         bool
}

func (s *deliveryRegistryStore) Head(ctx context.Context, key string) (r2.ObjectHead, error) {
	b := s.current
	if strings.Contains(key, "generations/") {
		b = s.pinned
	}
	return r2.ObjectHead{ETag: "registry-etag", SizeBytes: int64(len(b))}, nil
}
func (s *deliveryRegistryStore) OpenExact(ctx context.Context, key, etag, version string) (io.ReadCloser, error) {
	b := s.current
	if strings.Contains(key, "generations/") {
		b = s.pinned
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
func TestDeliveryKeepRegistryClosureAndDisagreement(t *testing.T) {
	reg := globalDeliveryTestRegistry()
	reg.PublicDelivery = true
	reg.SelectionScope = "available"
	reg.Generation = strings.Repeat("a", 32)
	reg.Assets = map[string]globalDeliveryObject{"/public/metadata.json": reg.Assets["/fixture.txt"]}
	raw, _ := json.Marshal(reg)
	store := &deliveryRegistryStore{current: raw, pinned: raw}
	got, err := ReadPublicDeliveryKeep(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Objects) != 4 || got.Objects["original/cohort/video.mp4"].Key == "" || len(got.SHA256) != 64 {
		t.Fatal("incomplete delivery closure", got)
	}
	reg.Files["2"] = globalDeliveryObject{}
	store.pinned, _ = json.Marshal(reg)
	if _, err := ReadPublicDeliveryKeep(context.Background(), store); err == nil {
		t.Fatal("registry disagreement allowed deletion authority")
	}
}
func TestDeliveryKeepExactSeedAndNASDependencies(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	migration, err := os.ReadFile("../../../infra/sql/migrations/0189_delivery_keep_holds.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"delivery_keep_holds", "delivery_keep_output_dependencies"} {
		text := string(migration)
		start := strings.Index(text, "INSERT INTO "+table+"(")
		end := strings.Index(text[start:], ";\n")
		if _, err := pool.Exec(ctx, text[start:start+end+1]); err != nil {
			t.Fatal(err)
		}
	}
	var count, bytes int64
	if err := pool.QueryRow(ctx, `SELECT count(*),sum(size_bytes) FROM delivery_keep_holds`).Scan(&count, &bytes); err != nil || count != 32825 || bytes != 7310068418360 {
		t.Fatal(count, bytes, err)
	}
	// A held output protects its exact bytes and required raw inputs even when
	// outside keep28, regardless of a different NAS prefix/presentation path.
	_, err = pool.Exec(ctx, `INSERT INTO recording_clips(id,recording_id,object_key,sha256,size_bytes,display_path,clip_start_at,clip_end_at,bucket) VALUES(901,2,'managed/source',repeat('a',64),3,'source/raw.mp4','2026-08-01','2026-08-01 00:01','bucket');
 INSERT INTO recording_collation_hours(id,recording_id) VALUES(900,2);
 INSERT INTO recording_collation_outputs(id,collation_hour_id,nas_relative_path,object_key,sha256,size_bytes,start_at,end_at,source_clip_ids)
 SELECT 902,900,'output.mp4',object_key,repeat('b',64),size_bytes,'2026-08-01','2026-08-01 00:01',ARRAY[901::bigint] FROM delivery_keep_holds WHERE kind='video_collated' LIMIT 1;
 SELECT refresh_delivery_nas_source_holds()`)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []nasCleanupItem{{Kind: "raw", SourceID: 901, RelativePath: "source/raw.mp4", SHA256: strings.Repeat("a", 64), SizeBytes: 3}, {Kind: "collated", SourceID: 902, RelativePath: "joined/output.mp4", SHA256: strings.Repeat("b", 64), SizeBytes: 3}} {
		if err := deliveryNASUnheld(ctx, pool, i); err == nil {
			t.Fatal("delivery source/output unprotected", i)
		}
		i.PermitReason = nasStitchReason
		if err := (&Server{}).cleanupCurrentPolicyCheck(ctx, pool, i, false, nil, nil); err == nil || !strings.Contains(err.Error(), "DELIVERY KEEP HOLD") {
			t.Fatal("stitch inclusion bypassed hold", err)
		}
	}
	// Compare the optimized lookup to the deployed function against the real
	// 32,825-key seed, raw dependencies, joined aliases and content aliases.
	start := strings.Index(string(migration), "CREATE FUNCTION delivery_nas_held(")
	legacy := string(migration)[start:]
	legacy = legacy[:strings.Index(legacy, "$$;")+3]
	legacy = strings.Replace(legacy, "delivery_nas_held(", "delivery_nas_held_legacy(", 1)
	if _, err := pool.Exec(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	var differences int
	if err := pool.QueryRow(ctx, `WITH h AS MATERIALIZED (SELECT relative_path,sha256,size_bytes FROM delivery_keep_holds ORDER BY object_key LIMIT 5), examples AS (
 SELECT 'raw'::text k,901::bigint i,'source/raw.mp4'::text p,repeat('a',64) sha,3::bigint n
 UNION ALL SELECT 'collated',902,'joined/output.mp4',repeat('b',64),3
 UNION ALL SELECT 'raw',0,relative_path,sha256,size_bytes FROM h
 UNION ALL SELECT 'collated',0,'joined/'||relative_path,sha256,size_bytes FROM h
 UNION ALL SELECT 'raw',0,'absent/alias.mp4',sha256,size_bytes FROM h WHERE sha256<>''
 UNION ALL SELECT 'raw',0,'absent/alias.mp4',repeat('0',64),1
 ) SELECT count(*) FROM examples x
 WHERE delivery_nas_held(k,i,p,sha,n) IS DISTINCT FROM delivery_nas_held_legacy(k,i,p,sha,n)`).Scan(&differences); err != nil || differences != 0 {
		t.Fatal("hold lookup semantics changed", differences, err)
	}
	for _, function := range []string{"delivery_nas_held_legacy", "delivery_nas_held"} {
		started := time.Now()
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM generate_series(1,100) g WHERE `+function+`('raw',0,'absent/'||g,repeat('0',64),1)`).Scan(&count); err != nil || count != 0 {
			t.Fatal("missing identity classified held", count, err)
		}
		t.Logf("%s: 100 absent-file checks over real seed in %s", function, time.Since(started))
	}
	for _, sql := range []string{`DELETE FROM delivery_keep_holds`, `DELETE FROM delivery_nas_source_holds`, `DELETE FROM recording_clips WHERE id=901`, `UPDATE recording_clips SET display_path='other' WHERE id=901`} {
		if _, err := pool.Exec(ctx, sql); err == nil {
			t.Fatal("hold mutated", sql)
		}
	}
}
func TestDeliveryKeepAdmissionAndOldPermitVeto(t *testing.T) {
	f := newCleanupFixture(t, 1)
	files := syntheticFiles(901, 1, 1)
	pid, _, ids := f.admit("delivery-test", files)
	_, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('bucket','bench/901.mp4','etag',3,'live_file')`)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := f.pool.QueryRow(f.ctx, `SELECT manifest FROM nas_cleanup_plans WHERE id=$1`, pid).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var items []nasCleanupItem
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, _, err := persistNASCleanupPlan(f.ctx, tx, "new-delivery-plan", items); err == nil {
		t.Fatal("admission ignored delivery hold")
	}
	tx.Rollback(f.ctx)
	// Direct SQL models an older binary: database trigger must independently
	// refuse a first permit and a replay/update of an issued permit.
	if _, err := f.pool.Exec(f.ctx, `UPDATE nas_cleanup_items SET state='delete_issued',delete_ever_issued=true WHERE id=$1`, ids[0]); err == nil {
		t.Fatal("older server minted delivery deletion permit")
	}
}

func TestDeliveryKeepOldDryRunClassifiesHeldMemberAndReplays(t *testing.T) {
	f := newCleanupFixture(t, 24)
	pid, _, ids := f.admit("before-delivery-hold", syntheticFiles(901, 3, 10))
	tasks := f.leaseV2(3, 0, 0, "")
	if len(tasks) != 3 {
		t.Fatal("incomplete lease", len(tasks))
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_nas_source_holds(clip_id) VALUES(902)`); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 2; round++ {
		for n, result := range f.observe(tasks, nil) {
			want := "would_delete"
			if tasks[n].Item.SourceID == 902 {
				want = "keep"
			}
			if result.Status != 200 || result.Action != want || result.Delete {
				t.Fatalf("round %d: %+v", round, result)
			}
		}
	}
	if tasks := f.leaseV2(3, 0, 0, ""); len(tasks) != 0 {
		t.Fatal("terminal hold re-leased", tasks)
	}
	var held bool
	if err := f.pool.QueryRow(f.ctx, `SELECT state='keep' AND NOT delete_ever_issued AND policy_reason LIKE 'DELIVERY KEEP HOLD:%' FROM nas_cleanup_items WHERE id=$1`, ids[1]).Scan(&held); err != nil || !held {
		t.Fatal("hold classification lost", held, err)
	}
	f.s.cfg.NASCleanupDeletionEnabled = true
	var sha string
	if err := f.pool.QueryRow(f.ctx, `SELECT plan_sha256 FROM nas_cleanup_plans WHERE id=$1`, pid).Scan(&sha); err != nil {
		t.Fatal(err)
	}
	if code, body := safeguardControl(t, f.s, pid, "enabled", map[string]any{"plan_sha256": sha}); code == 200 {
		t.Fatal("held plan enabled", body)
	}
}

func TestDeliveryKeepCheckFailureCannotClassifyKeep(t *testing.T) {
	f := newCleanupFixture(t, 24)
	f.admit("hold-check-unavailable", syntheticFiles(901, 1, 10))
	tasks := f.leaseV2(1, 0, 0, "")
	if _, err := f.pool.Exec(f.ctx, `ALTER FUNCTION delivery_nas_held(text,bigint,text,text,bigint) RENAME TO hold_check_unavailable`); err != nil {
		t.Fatal(err)
	}
	r := f.observe(tasks, nil)[0]
	if r.Status != 503 || r.Delete {
		t.Fatal("unavailable hold check became a terminal decision", r)
	}
	var state string
	if err := f.pool.QueryRow(f.ctx, `SELECT state FROM nas_cleanup_items WHERE id=$1`, tasks[0].ItemID).Scan(&state); err != nil || state != "leased" {
		t.Fatal("failed hold check modified observation", state, err)
	}
}

func TestDeliveryKeepIssuedObservationReconcilesWithoutChangingEvidence(t *testing.T) {
	f := newCleanupFixture(t, 24)
	pid, sha, _ := f.admit("issued-before-hold", syntheticFiles(901, 1, 10))
	f.dryRun()
	f.s.cfg.NASCleanupDeletionEnabled = true
	if code, body := safeguardControl(t, f.s, pid, "enabled", map[string]any{"plan_sha256": sha}); code != 200 {
		t.Fatal(code, body)
	}
	tasks := f.leaseV2(1, 1, 1<<30, "")
	requests := map[int64]string{tasks[0].ItemID: strings.Repeat("a", 32)}
	if r := f.observe(tasks, requests)[0]; r.Status != 200 || !r.Delete {
		t.Fatal("missing initial permit", r)
	}
	var before, after string
	query := `SELECT to_jsonb(i)::text FROM nas_cleanup_items i WHERE id=$1`
	if err := f.pool.QueryRow(f.ctx, query, tasks[0].ItemID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	// Model a hold introduced after authority. Import normally fences this;
	// replay must still never return authority or rewrite the issued receipt.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_nas_source_holds(clip_id) VALUES(901)`); err != nil {
		t.Fatal(err)
	}
	if r := f.observe(tasks, requests)[0]; r.Status != 200 || r.Delete || r.Action != "reconcile" {
		t.Fatal("held authority replayed", r)
	}
	if err := f.pool.QueryRow(f.ctx, query, tasks[0].ItemID).Scan(&after); err != nil || before != after {
		t.Fatal("issued evidence changed", err)
	}
}
func TestDeliveryKeepLiveAgreementFailsClosed(t *testing.T) {
	pool := cleanupTestPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama','test/keep','etag',1,'asset')`); err != nil {
		t.Fatal(err)
	}
	var o DeliveryKeepIdentity
	if err := pool.QueryRow(ctx, `SELECT object_key,etag,size_bytes FROM delivery_keep_holds LIMIT 1`).Scan(&o.Key, &o.ETag, &o.Size); err != nil {
		t.Fatal(err)
	}
	snapshot := DeliveryKeepSnapshot{Objects: map[string]DeliveryKeepIdentity{o.Key: o}}
	if err := RequireDeliveryKeep(ctx, pool, snapshot); err != nil {
		t.Fatal(err)
	}
	o.Size++
	snapshot.Objects[o.Key] = o
	if err := RequireDeliveryKeep(ctx, pool, snapshot); err == nil {
		t.Fatal("live size disagreement accepted")
	}
	snapshot.Objects = map[string]DeliveryKeepIdentity{"new/link": {Key: "new/link", ETag: "new", Size: 1}}
	if err := RequireDeliveryKeep(ctx, pool, snapshot); err == nil {
		t.Fatal("new linked object without hold accepted")
	}
}

func TestDeliveryKeepAssemblyDependenciesAreExactAndComplete(t *testing.T) {
	repair := []byte(`{"repairs":[{"sha256":"` + strings.Repeat("b", 64) + `","recording_id":335,"size_bytes":10,"input_ids":[901,902],"input_parts":2}]}`)
	sum := sha256.Sum256(repair)
	keep := DeliveryKeepSnapshot{Objects: map[string]DeliveryKeepIdentity{"assembly": {Key: "experimental/assembly", SHA256: strings.Repeat("b", 64), Kind: "video_assembly", RecordingID: 335, Size: 10}, "growth": {Key: "datasets/x/growth.json", ETag: "e", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(repair)), Kind: "asset"}}}
	store := &deliveryRegistryStore{current: repair, pinned: repair}
	if err := keep.readAssemblyDependencies(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	if len(keep.OutputDependencies) != 2 || keep.OutputDependencies[901] != 335 {
		t.Fatal("assembly inputs omitted", keep.OutputDependencies)
	}
	store.current = append(repair, ' ')
	if err := keep.readAssemblyDependencies(context.Background(), store); err == nil {
		t.Fatal("changed assembly metadata accepted")
	}
}
