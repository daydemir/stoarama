package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/db"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5/pgxpool"
)

// nonrawOutputSHA matches the fake provider's reported x-amz-meta-sha256 so the
// HEAD identity check compares like with like; the key must bind this value.
const nonrawOutputSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

// nonrawFixture builds a disposable-PG schema with one collated output whose
// exact content-addressed key binds nonrawOutputSHA, plus one RAW source clip.
type nonrawFixture struct {
	pool     *pgxpool.Pool
	ctx      context.Context
	policy   string
	windows  map[int64]api.Keep28Window
	picks    string
	drain    string
	drainSHA string
	root     string
	clipID   int64
	outputID int64
	key      string
	store    *inverseFakeStore
	sdk      *inverseSDKStore
	deletes  func() int64
}

func newNonrawFixture(t *testing.T, prefix string) *nonrawFixture {
	t.Helper()
	ctx := context.Background()
	schemaPool := joinedPurgeTestPool(t, prefix)
	cfg := schemaPool.Config()
	cfg.MaxConns = 20
	cfg.MinConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = db.MigrateUp(ctx, pool, joinedPurgeMigrationsDir()); err != nil {
		t.Fatal(err)
	}
	policy, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	f := &nonrawFixture{pool: pool, ctx: ctx, policy: policy, windows: windows, root: t.TempDir()}
	f.exec(`INSERT INTO accounts(id,email) VALUES(47,'nonraw@test.invalid');
 INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(1,47,'test-nonraw','not-a-real-credential');
 INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',1);
 INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES(3,'test','3','nonraw','nonraw','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc) VALUES(1,47,'test','https://test.invalid','auto','stoarama','fixture',decode('00','hex'));
 INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at) VALUES(30,47,1,'nonraw','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now());`)
	// One RAW source outside every pinned keep28 window.
	f.exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 VALUES(7000001,30,1,'https://test.invalid','stoarama','managed/acct-47/nonraw/source.mp4',500,'src-etag',repeat('c',64),'2026-06-01','2026-06-01 00:00:00','2026-06-01 00:01:00','source/source.mp4')`)
	f.clipID = 7000001
	f.key = "joined/nightly-collation-v2/objects/" + nonrawOutputSHA + ".mp4"
	f.exec(`INSERT INTO recording_collation_hours(id,hour_id,batch_id,generation,recording_id,local_date,delivery_hour,status,manifest_object_key,manifest_sha256,manifest,source_clip_count,included_clip_count,excluded_clip_count,join_count,split_count)
 VALUES(1,'nonraw__recording-30__date-2026-06-01__hour-01__generation-2','nonraw',2,30,'2026-06-01',1,'collated','joined/nightly-collation-v2/manifest.json',repeat('a',64),'{}',1,1,0,0,0)`)
	f.exec(`INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(10000001,1,1,1,$1,'nightly/video.mp4',900,$2,'2026-06-01','2026-06-01 00:01:00',ARRAY[7000001]::bigint[],'2026-06-02T00:00:00Z')`, f.key, nonrawOutputSHA)
	f.outputID = 10000001

	w := windows[3]
	picksPath := filepath.Join(f.root, "picks.json")
	picksData, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}})
	if err = os.WriteFile(picksPath, picksData, 0600); err != nil {
		t.Fatal(err)
	}
	f.picks = picksPath

	// The durable fence must carry the exact SDK endpoint identity used at wire.
	f.store = &inverseFakeStore{objects: map[string]r2.ObjectInfo{}}
	f.sdk, f.deletes = inverseSDKTestStore(t, f.store)
	f.store.endpointSHA = f.sdk.EndpointSHA256()
	f.store.objects[f.key] = r2.ObjectInfo{Key: f.key, ETag: "collated-etag", SizeBytes: 900, LastModified: inverseRetirementCutoff().Add(-time.Second)}
	f.exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, f.sdk.EndpointSHA256())
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `SET LOCAL session_replication_role=replica; UPDATE r2_retirement_writer_fences SET activated_at='2026-11-12T14:28:47.037031Z'; UPDATE r2_retirement_writer_fences SET drain_not_before=clock_timestamp()-interval '1 hour'`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var activated, deadline time.Time
	if err = pool.QueryRow(ctx, `SELECT activated_at,drain_not_before FROM r2_retirement_writer_fences`).Scan(&activated, &deadline); err != nil {
		t.Fatal(err)
	}
	drain := inverseDrainEvidence{Proof: inverseOwnerFenceProof, Bucket: "stoarama", EndpointSHA256: f.sdk.EndpointSHA256(), FenceActivatedAt: activated, DrainNotBefore: deadline, ObservedAt: time.Now().UTC(),
		FencePolicy: &inverseFencePolicyEvidence{PartitionSHA256: strings.Repeat("d", 64), SummarySHA256: strings.Repeat("c", 64), LastModifiedBefore: inverseRetirementCutoff(), OwnerDecision: inverseEvidenceReference{Path: "../../../docs/records/r2-owner-fence-policy-20261118.md", SHA256: inverseOwnerDecisionSHA256}, ResidualAcceptance: inverseApprovedFenceResidual}}
	f.drain = filepath.Join(f.root, "drain.json")
	data, _ := json.Marshal(drain)
	if err = os.WriteFile(f.drain, data, 0600); err != nil {
		t.Fatal(err)
	}
	f.drainSHA, _ = inverseFileSHA(f.drain)
	return f
}

// fixture seeds rows the post-activation writer admission legitimately refuses.
func (f *nonrawFixture) fixture(query string, args ...any) {
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		panic(err)
	}
	if _, err = tx.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		panic(err)
	}
	if _, err = tx.Exec(f.ctx, query, args...); err != nil {
		panic(fmt.Sprintf("fixture %q: %v", query, err))
	}
	if err = tx.Commit(f.ctx); err != nil {
		panic(err)
	}
}

func (f *nonrawFixture) exec(query string, args ...any) {
	if _, err := f.pool.Exec(f.ctx, query, args...); err != nil {
		panic(fmt.Sprintf("fixture %q: %v", query, err))
	}
}

// TestR2NonrawMigrationScope pins the reviewed 0174 amendment on real PG: only
// best14 became eligible and staging/repaired stayed refused.
func TestR2NonrawMigrationScope(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_scope")
	for _, tc := range []struct {
		key  string
		ok   bool
		note string
	}{
		{"joined/best14-20261021/objects/x/1.mp4", true, "best14 eligible"},
		{"joined/nightly-collation-v2/objects/x/1.mp4", true, "nightly kept"},
		{"joined/goodplus-20260821-generation-1/objects/x/1.mp4", true, "goodplus kept"},
		{"joined/other-generation/objects/x/1.mp4", false, "unknown prefix refused"},
		{"staging/x/1.mp4", false, "staging refused"},
		{"repaired/x/1.mp4", false, "repaired refused"},
	} {
		// Receipts are append-only by design, so every probe runs in its own
		// rolled-back transaction instead of deleting evidence.
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, insertErr := tx.Exec(f.ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256)
 VALUES('collated',$1,3,'stoarama',$2,1,$3,'t',$4,$5,$6)`, 20000000+(f.outputID%1000)+int64(len(tc.key)), tc.key, strings.Repeat("b", 64), "none", strings.Repeat("d", 64), f.policy)
		if err = tx.Rollback(f.ctx); err != nil {
			t.Fatal(err)
		}
		if tc.ok != (insertErr == nil) {
			t.Fatalf("%s: err=%v", tc.note, insertErr)
		}
	}
}

// TestR2NonrawDeliveryGuardIsTypedAndScoped proves the 0190 exemption needs the
// reviewed class AND an exact matching typed intent, on both the reservation and
// the final receipt, and that ordinary delivery holds are still refused.
func TestR2NonrawDeliveryGuardIsTypedAndScoped(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_guard")
	f.exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',900,'metadata')`, f.key)

	_, err := f.pool.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) VALUES('collated',$1,'stoarama',$2,900,$3,$4)`, f.outputID, f.key, nonrawOutputSHA, strings.Repeat("d", 64))
	if err == nil {
		t.Fatal("ordinary collated reservation bypassed the delivery hold")
	}
	_, err = f.pool.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class) VALUES('collated',$1,'stoarama',$2,900,$3,$4,$5)`, f.outputID, f.key, nonrawOutputSHA, strings.Repeat("d", 64), inverseNonrawRetirementClass)
	if err == nil {
		t.Fatal("reviewed class without a matching typed intent bypassed the delivery hold")
	}
	f.insertIntent(t, strings.Repeat("d", 64))
	_, err = f.pool.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class) VALUES('collated',$1,'stoarama',$2,900,$3,$4,$5)`, f.outputID, f.key, nonrawOutputSHA, strings.Repeat("d", 64), inverseNonrawRetirementClass)
	if err != nil {
		t.Fatalf("reviewed reservation with matching intent refused: %v", err)
	}
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256) VALUES('collated',$1,3,'stoarama',$2,900,$3,$4,'none',$5,$6)`, f.outputID, f.key, nonrawOutputSHA, inverseNonrawReason, strings.Repeat("d", 64), f.policy); err != nil {
		t.Fatalf("exact final receipt refused: %v", err)
	}
	// A receipt for a different identity must not inherit the exemption.
	if _, err = f.pool.Exec(f.ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256) VALUES('collated',$1,3,'stoarama',$2,901,$3,$4,'none',$5,$6)`, f.outputID, f.key, nonrawOutputSHA, inverseNonrawReason, strings.Repeat("d", 64), f.policy); err == nil {
		t.Fatal("mismatched receipt inherited the reviewed exemption")
	}
}

func (f *nonrawFixture) insertIntent(t *testing.T, manifest string) {
	t.Helper()
	_, err := f.pool.Exec(f.ctx, `INSERT INTO r2_nonraw_intents(bucket,object_key,endpoint_sha256,kind,stream_id,source_id,source_ids,size_bytes,sha256,nas_relative_path,etag,last_modified,verification_at,manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,retirement_class,choice)
 VALUES('stoarama',$1,$2,'collated',3,$3,ARRAY[$4]::bigint[],900,$5,'nightly/video.mp4','collated-etag',$6,'2026-06-02T00:00:00Z',$7,$8,$9,$10,$11,'none')`,
		f.key, strings.Repeat("f", 64), f.outputID, f.clipID, nonrawOutputSHA, inverseRetirementCutoff().Add(-time.Second),
		manifest, f.policy, strings.Repeat("e", 64), strings.Repeat("a", 64), inverseNonrawRetirementClass)
	if err != nil {
		t.Fatal(err)
	}
}

// TestR2NonrawAdmissionAllowsUnrelatedFutureHolds is the critical 0201 property:
// after an approved delivery-scope reservation, unrelated hold writes on ALL
// five caller tables must still succeed, while a NEW overlapping write fails.
func TestR2NonrawAdmissionAllowsUnrelatedFutureHolds(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_admission")
	f.exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',900,'metadata')`, f.key)
	f.insertIntent(t, strings.Repeat("d", 64))
	f.exec(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class) VALUES('collated',$1,'stoarama',$2,900,$3,$4,$5)`, f.outputID, f.key, nonrawOutputSHA, strings.Repeat("d", 64), inverseNonrawRetirementClass)

	// Fixture rows that the post-fence writer admission correctly refuses are
	// seeded with replication role disabled; the admission behaviour under test
	// here is the 0211 hold guard, not the writer fence.
	f.fixture(`INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(10000002,1,2,2,'joined/nightly-collation-v2/objects/'||repeat('b',64)||'.mp4','nightly/unrelated.mp4',10,repeat('b',64),'2026-06-01','2026-06-01 00:01:00',ARRAY[7000001]::bigint[],'2026-06-02T00:00:00Z')`)

	// Unrelated future writes must all still work.
	f.exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama','datasets/other/unrelated.json','hold',10,'metadata')`)
	f.exec(`INSERT INTO delivery_nas_source_holds(clip_id) VALUES(7000999)`)
	f.exec(`INSERT INTO delivery_keep_output_dependencies(output_id,recording_id) VALUES(10000002,30)`)
	f.exec(`UPDATE recording_collation_hours SET delivery_hour=2 WHERE id=1`)

	// A genuinely new overlap still fails on each caller.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',900,'metadata')`, f.key); err == nil {
		t.Fatal("new key overlap was admitted")
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_nas_source_holds(clip_id) VALUES($1)`, f.clipID); err == nil {
		t.Fatal("new source overlap was admitted")
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_keep_output_dependencies(output_id,recording_id) VALUES($1,30)`, f.outputID); err == nil {
		t.Fatal("new output dependency overlap was admitted")
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE recording_collation_outputs SET nas_relative_path='nightly/moved.mp4' WHERE id=$1`, f.outputID); err == nil {
		t.Fatal("output identity overlap was admitted")
	}
	f.exec(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at) VALUES(31,47,1,'other','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now())`)
	if _, err := f.pool.Exec(f.ctx, `UPDATE recording_collation_hours SET recording_id=31 WHERE id=1`); err == nil {
		t.Fatal("hour recording overlap was admitted")
	}
}

// TestR2NonrawExecutorLane exercises positive selection plus every hold reason on
// real PostgreSQL with a fake provider, and proves no DELETE on ineligible keys.
func TestR2NonrawExecutorLane(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_exec")
	applier := f.applier(t, "run1")
	entry := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: f.key, ETag: "collated-etag", SizeBytes: 900, LastModified: inverseRetirementCutoff().Add(-time.Second)}, Class: inverseCollated}

	t.Run("positive", func(t *testing.T) {
		before := f.deletes()
		ok, err := applier.deleteNonraw(f.ctx, entry)
		if err != nil || !ok || f.deletes() != before+1 {
			t.Fatalf("ok=%v err=%v deletes=%d", ok, err, f.deletes())
		}
		var kind string
		if err = f.pool.QueryRow(f.ctx, `SELECT kind FROM r2_object_retirements WHERE object_key=$1`, f.key).Scan(&kind); err != nil || kind != "collated" {
			t.Fatalf("receipt %q %v", kind, err)
		}
		var events int
		if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_nonraw_events WHERE object_key=$1`, f.key).Scan(&events); err != nil || events != 1 {
			t.Fatal(events, err)
		}
	})

	t.Run("prior_attempt_never_redeletes", func(t *testing.T) {
		if _, err := applier.deleteNonraw(f.ctx, entry); err == nil {
			t.Fatal("already-issued key was retransmitted")
		}
	})

	// A delivery dependency is an ownership veto, not a protection: under the
	// reviewed scope the mapped output is still eligible. The dependency row is
	// what makes it footage; the bare hold alone would never be identity.
	t.Run("delivery_dependency_is_not_a_protection_in_reviewed_scope", func(t *testing.T) {
		g := newNonrawFixture(t, "nonraw_delivery_scope")
		g.exec(`INSERT INTO delivery_keep_output_dependencies(output_id,recording_id) VALUES($1,30)`, g.outputID)
		g.exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',900,'metadata')`, g.key)
		a := g.applier(t, "run1")
		ok, err := a.deleteNonraw(g.ctx, inverseEntry{ObjectInfo: r2.ObjectInfo{Key: g.key, ETag: "collated-etag", SizeBytes: 900, LastModified: inverseRetirementCutoff().Add(-time.Second)}, Class: inverseCollated})
		if err != nil || !ok || g.deletes() != 1 {
			t.Fatalf("reviewed delivery output not retired: ok=%v err=%v deletes=%d", ok, err, g.deletes())
		}
		var kind string
		if err = g.pool.QueryRow(g.ctx, `SELECT kind FROM r2_object_retirements WHERE object_key=$1`, g.key).Scan(&kind); err != nil || kind != "collated" {
			t.Fatalf("receipt %q %v", kind, err)
		}
	})

	t.Run("raw_ledger_never_receives_collated_id", func(t *testing.T) {
		var raw int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_inverse_intents WHERE object_key=$1`, f.key).Scan(&raw); err != nil || raw != 0 {
			t.Fatalf("collated key entered the RAW ledger: %d %v", raw, err)
		}
	})

	t.Run("held_reasons_are_reported_and_never_reach_wire", func(t *testing.T) {
		toolsKey := "experimental/v85-retired-WORK-custody/objects/" + nonrawOutputSHA + ".mp4"
		otherKey := "joined/other-generation/objects/" + nonrawOutputSHA + ".mp4"
		var caseN int
		// Each case gets its own schema, pool and subtest so cleanup releases
		// connections instead of accumulating them for the whole parent test.
		held := func(t *testing.T, key string, mutate func(g *nonrawFixture, entry inverseEntry)) {
			caseN++
			t.Run(fmt.Sprintf("case_%d", caseN), func(t *testing.T) {
				g := newNonrawFixture(t, fmt.Sprintf("nonraw_hold_%d", caseN))
				entry := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: key, ETag: "collated-etag", SizeBytes: 900, LastModified: inverseRetirementCutoff().Add(-time.Second)}, Class: inverseCollated}
				a := g.applier(t, "run1")
				mutate(g, entry)
				if _, err := a.deleteNonraw(g.ctx, entry); err == nil || g.deletes() != 0 {
					t.Fatalf("%s reached DELETE: %v deletes=%d", key, err, g.deletes())
				}
				var intents int
				if err := g.pool.QueryRow(g.ctx, `SELECT count(*) FROM r2_nonraw_intents WHERE object_key=$1`, key).Scan(&intents); err != nil {
					t.Fatal(err)
				}
				var retirements int
				if err := g.pool.QueryRow(g.ctx, `SELECT count(*) FROM r2_object_retirements WHERE object_key=$1`, key).Scan(&retirements); err != nil {
					t.Fatal(err)
				}
				if retirements != 0 {
					t.Fatalf("%s left a receipt", key)
				}
			})
		}

		// ANY source overlapping a pinned keep28 window holds the WHOLE output.
		held(t, "joined/nightly-collation-v2/objects/"+nonrawOutputSHA+".mp4", func(g *nonrawFixture, _ inverseEntry) {
			w := g.windows[3]
			g.exec(`UPDATE recording_clips SET clip_start_at=$1, clip_end_at=$2 WHERE id=7000001`, w.Start.Add(time.Minute), w.Start.Add(2*time.Minute))
		})
		// Keep-everything stream protects the output.
		held(t, "joined/nightly-collation-v2/objects/"+nonrawOutputSHA+".mp4", func(g *nonrawFixture, _ inverseEntry) {
			picksData, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "all"}}})
			if err := os.WriteFile(g.picks, picksData, 0600); err != nil {
				t.Fatal(err)
			}
		})
		// Content-addressed key that does not bind the catalog SHA is held.
		held(t, "joined/nightly-collation-v2/objects/"+nonrawOutputSHA+".mp4", func(g *nonrawFixture, _ inverseEntry) {
			g.fixture(`UPDATE recording_collation_outputs SET sha256=repeat('9',64) WHERE id=$1`, g.outputID)
		})
		// A shared output key is held rather than guessed.
		held(t, "joined/nightly-collation-v2/objects/"+nonrawOutputSHA+".mp4", func(g *nonrawFixture, _ inverseEntry) {
			g.fixture(`INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(10000003,1,2,2,$1,'nightly/shared.mp4',900,$2,'2026-06-01','2026-06-01 00:01:00',ARRAY[7000001]::bigint[],'2026-06-02T00:00:00Z')`, g.key, nonrawOutputSHA)
		})
		// Tool-prefix and unsupported-prefix keys are held by the prefix cut
		// itself, before any catalog lookup, so no catalog row is fabricated for
		// them; that is exactly the property ROOT requires.
		held(t, toolsKey, func(*nonrawFixture, inverseEntry) {})
		held(t, otherKey, func(*nonrawFixture, inverseEntry) {})
		// An unresolved source holds the output.
		held(t, "joined/nightly-collation-v2/objects/"+nonrawOutputSHA+".mp4", func(g *nonrawFixture, _ inverseEntry) {
			g.fixture(`UPDATE recording_clips SET purged_at=now() WHERE id=7000001`)
		})
	})
}

func (f *nonrawFixture) applier(t *testing.T, run string) *inverseApplier {
	t.Helper()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	// The applier's store must be the concrete SDK store: it is the only type
	// that implements the collated transport, and its endpoint identity is what
	// the durable fence and evidence are pinned to.
	store := f.sdk
	summary := inverseSummary{Version: 1, Bucket: "stoarama", EndpointSHA256: store.EndpointSHA256(), CreatedAt: time.Now().UTC(), PolicySHA256: f.policy, PicksSHA256: picks.SHA256, ReadOnly: true}
	o := inverseApplyOptions{Lane: inverseNonrawCollated, Manifest: f.root + "/partition.jsonl", ManifestSHA: strings.Repeat("d", 64), Summary: f.root + "/summary.json", SummarySHA: strings.Repeat("c", 64), Picks: f.picks, Ledger: filepath.Join(f.root, "ledger-"+run+".jsonl"), DrainEvidence: f.drain, DrainSHA: f.drainSHA, MaxItems: 100, MaxBytes: 1 << 30, Workers: 2}
	protection, err := loadInverseProtectionMode(f.ctx, f.pool, "stoarama", picks, f.windows, true)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := openNonrawLedger(o.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	var epoch int64
	if err = f.pool.QueryRow(f.ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	return &inverseApplier{pool: f.pool, store: store, opts: o, summary: summary, protection: protection, ledger: ledger, keEpoch: epoch}
}

// TestR2NonrawClassifyKeepsDefaults pins that default B1 classification is
// unchanged: collated keys stay deferred and delivery still holds.
func TestR2NonrawClassifyKeepsDefaults(t *testing.T) {
	o := r2.ObjectInfo{Key: "joined/nightly-collation-v2/objects/" + nonrawOutputSHA + ".mp4", SizeBytes: 900}
	ids := map[string][]inverseIdentity{o.Key: {{StreamID: 3, Kind: "collated"}}}
	def := inverseProtection{Picks: r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none"}}}, Identities: ids}
	if class, reason := def.classify(o); class != inverseHeld || reason != "collated_deferred" {
		t.Fatalf("default collated classification changed: %s %s", class, reason)
	}
	del := inverseProtection{Picks: r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none"}}}, Identities: ids, Delivery: map[string]bool{o.Key: true}}
	if class, _ := del.classify(o); class != inverseDelivery {
		t.Fatalf("default delivery veto changed: %s", class)
	}
}

// TestR2InverseToolsPrefixIsNeverDeletable is the broad ROOT property: across
// every classification mode and every tempting protection combination, no key in
// the retired WORK custody prefix may land in ANY deletable class. This is a
// prefix property over the 666 real objects, not a one-object regression.
func TestR2InverseToolsPrefixIsNeverDeletable(t *testing.T) {
	keys := []string{
		"experimental/v85-retired-WORK-custody/one.mp4",
		"experimental/v85-retired-WORK-custody/deep/dir/two.mp4",
		"experimental/v85-retired-WORK-custody/objects/" + nonrawOutputSHA + ".mp4",
		"experimental/v85-retired-WORK-custody/",
	}
	base := inverseProtection{
		Identities: map[string][]inverseIdentity{},
		Held:       map[string]bool{},
		Delivery:   map[string]bool{},
		KEFolders:  map[string]bool{},
		Windows:    map[int64]api.Keep28Window{3: {StreamID: 3, Timezone: "Asia/Tokyo"}},
		Picks:      r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none"}}},
	}
	variants := map[string]inverseProtection{}
	variants["default"] = base
	reviewed := base
	reviewed.ReviewedCollated, reviewed.ReviewedDelivery = true, true
	variants["reviewed_owner_scope"] = reviewed
	mapped := reviewed
	mapped.Collated = map[string]inverseCollatedAssessment{}
	for _, k := range keys {
		mapped.Collated[k] = inverseCollatedAssessment{ID: 1, StreamID: 3, SHA: nonrawOutputSHA}
		mapped.Identities[k] = []inverseIdentity{{StreamID: 3, Kind: "collated"}}
	}
	variants["reviewed_owner_scope_with_catalog_mapping"] = mapped
	delivery := mapped
	delivery.Delivery = map[string]bool{}
	for _, k := range keys {
		delivery.Delivery[k] = true
	}
	variants["reviewed_owner_scope_delivery_hold"] = delivery
	kept := mapped
	kept.Picks = r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "all"}}}
	variants["keep_everything_stream"] = kept

	for name, p := range variants {
		for _, key := range keys {
			o := r2.ObjectInfo{Key: key, SizeBytes: 1201990550, ETag: "big", LastModified: inverseRetirementCutoff().Add(-time.Second)}
			class, reason := p.classify(o)
			if class == inverseDeletable || class == inverseCollated {
				t.Fatalf("%s: tool-prefix key %q classified %s (%s)", name, key, class, reason)
			}
			if class != inverseTools || reason != "pinned_stitch_tools" {
				t.Fatalf("%s: tool-prefix key %q got %s (%s)", name, key, class, reason)
			}
			// The executor-side assessment must agree with the partition.
			x := assessCollated(p, inverseCollatedAssessment{ID: 1, StreamID: 3, Key: key, Path: key, SHA: nonrawOutputSHA, SizeBytes: 1})
			if x.Reason != "pinned_stitch_tools" || x.Keep28 {
				t.Fatalf("%s: tool-prefix key %q executor reason %q", name, key, x.Reason)
			}
		}
	}
}
