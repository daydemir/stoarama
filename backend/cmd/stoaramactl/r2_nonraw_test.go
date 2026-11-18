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

// TestR2NonrawSourceUncertaintyHolds is the ROOT "uncertainty must HOLD" property.
// A source with an absent pinned window, a nil or invalid span, or a missing or
// mismatched timezone may never fall through as "not overlapping" and become
// eligible: each case must be held and never deletable.
func TestR2NonrawSourceUncertaintyHolds(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(28 * 24 * time.Hour)
	window := api.Keep28Window{StreamID: 3, Timezone: "Asia/Tokyo", Start: start, End: end}
	inside := start.Add(time.Hour)
	outside := start.Add(-30 * 24 * time.Hour)
	outsideEnd := outside.Add(time.Minute)
	base := func() inverseProtection {
		return inverseProtection{
			Identities: map[string][]inverseIdentity{},
			Held:       map[string]bool{},
			Delivery:   map[string]bool{},
			KEFolders:  map[string]bool{},
			Windows:    map[int64]api.Keep28Window{3: window},
			Picks:      r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none", KeepStartUTC: start.Format(time.RFC3339), KeepEndUTC: end.Format(time.RFC3339)}}},
		}
	}
	// The output's OWN interval and timezone are classifier inputs, so a realistic
	// assessment must carry them; an empty one is (correctly) held as uncertain.
	output := func(sources ...inverseCollatedSource) inverseCollatedAssessment {
		ownStart, ownEnd := outside, outsideEnd
		return inverseCollatedAssessment{ID: 7, Key: "joined/nightly-collation-v2/objects/" + nonrawOutputSHA + ".mp4",
			Path: "nightly/video.mp4", SHA: nonrawOutputSHA, SizeBytes: 900, VerifiedAt: start,
			StreamID: 3, Start: &ownStart, End: &ownEnd, Timezone: "Asia/Tokyo", Sources: sources}
	}
	good := inverseCollatedSource{ID: 1, StreamID: 3, Path: "s/a.mp4", Start: &outside, End: &outsideEnd, Timezone: "Asia/Tokyo"}
	badEnd := outsideEnd.Add(-2 * time.Minute)

	for _, tc := range []struct {
		name   string
		mutate func(p *inverseProtection, x *inverseCollatedAssessment)
	}{
		{"absent_pinned_window", func(p *inverseProtection, _ *inverseCollatedAssessment) { delete(p.Windows, 3) }},
		{"nil_source_span", func(_ *inverseProtection, x *inverseCollatedAssessment) { x.Sources[0].Start = nil }},
		{"nil_source_end", func(_ *inverseProtection, x *inverseCollatedAssessment) { x.Sources[0].End = nil }},
		{"invalid_source_span", func(_ *inverseProtection, x *inverseCollatedAssessment) { e := badEnd; x.Sources[0].End = &e }},
		{"missing_source_timezone", func(_ *inverseProtection, x *inverseCollatedAssessment) { x.Sources[0].Timezone = "" }},
		{"mismatched_source_timezone", func(_ *inverseProtection, x *inverseCollatedAssessment) { x.Sources[0].Timezone = "UTC" }},
		{"empty_source_path", func(_ *inverseProtection, x *inverseCollatedAssessment) { x.Sources[0].Path = "" }},
		{"undecided_source_policy", func(p *inverseProtection, _ *inverseCollatedAssessment) {
			delete(p.Picks.Choices, 3)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base()
			x := output(good)
			tc.mutate(&p, &x)
			got := assessCollated(p, x)
			if got.Reason == "" && !got.Keep28 {
				t.Fatalf("%s: uncertain source was treated as eligible", tc.name)
			}
			// The partition must agree.
			p.Collated = map[string]inverseCollatedAssessment{got.Key: got}
			p.Identities[got.Key] = []inverseIdentity{{StreamID: 3, Kind: "collated", Path: got.Path, Start: &outside, End: &outsideEnd, Timezone: "Asia/Tokyo"}}
			class, reason := p.classify(r2.ObjectInfo{Key: got.Key, SizeBytes: 900})
			if class == inverseCollated || class == inverseDeletable {
				t.Fatalf("%s: partition classified %s (%s)", tc.name, class, reason)
			}
		})
	}
	// Control: a fully known non-overlapping source stays eligible, so the test
	// cannot pass by holding everything.
	p := base()
	got := assessCollated(p, output(good))
	if got.Reason != "" || got.Keep28 {
		t.Fatalf("control case wrongly held: %q keep28=%v", got.Reason, got.Keep28)
	}
	// Any single in-window source holds the whole output.
	inWindowEnd := inside.Add(time.Minute)
	inWindow := inverseCollatedSource{ID: 2, StreamID: 3, Path: "s/b.mp4", Start: &inside, End: &inWindowEnd, Timezone: "Asia/Tokyo"}
	if got := assessCollated(base(), output(good, inWindow)); !got.Keep28 {
		t.Fatal("in-window source did not mark the output keep28")
	}
}

// TestR2NonrawWireProtectionRace proves the collated wire path takes the shared
// protection envelope: a KE epoch change committed after the reservation must
// veto the DELETE instead of racing it.
func TestR2NonrawWireProtectionRace(t *testing.T) {
	// Each scenario needs its own schema and pool; the fixtures must not
	// overlap, so each runs as a subtest whose cleanup releases connections.
	t.Run("ke_epoch_change_after_reservation", func(t *testing.T) {
		f := newNonrawFixture(t, "nonraw_race_epoch")
		a := f.applier(t, "run1")
		entry := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: f.key, ETag: "collated-etag", SizeBytes: 900, LastModified: inverseRetirementCutoff().Add(-time.Second)}, Class: inverseCollated}
		// Reserve first, exactly as the real flow does, so the mutation lands
		// after the reservation transaction committed.
		if _, err := a.reserveNonraw(f.ctx, entry); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(f.ctx, `UPDATE r2_inverse_ke_epoch SET epoch=epoch+1`); err != nil {
			t.Fatal(err)
		}
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		if _, err = a.authorizeNonraw(f.ctx, tx, entry); err == nil {
			t.Fatal("KE epoch change after reservation still authorized wire")
		}
		if f.deletes() != 0 {
			t.Fatal("DELETE reached the provider after a protection change")
		}
	})

	t.Run("live_hold_after_reservation", func(t *testing.T) {
		f := newNonrawFixture(t, "nonraw_race_hold")
		a := f.applier(t, "run1")
		entry := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: f.key, ETag: "collated-etag", SizeBytes: 900, LastModified: inverseRetirementCutoff().Add(-time.Second)}, Class: inverseCollated}
		if _, err := a.reserveNonraw(f.ctx, entry); err != nil {
			t.Fatal(err)
		}
		// A NAS cleanup source hold on a source clip is a real live veto that did
		// not exist when the reservation committed, and it is inserted through an
		// ordinary admission path so the live closure really has to reload.
		var planID int64
		if err := f.pool.QueryRow(f.ctx, `INSERT INTO nas_cleanup_plans(account_id,connection_id,label,report_sha256,manifest,plan_sha256,mode)
 VALUES(47,13,'nonraw-race',repeat('d',64),jsonb_build_array(jsonb_build_object('kind','raw','source_id',$1::bigint)),repeat('e',64),'dry_run') RETURNING id`, f.clipID).Scan(&planID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO nas_cleanup_source_holds(plan_id,clip_id) VALUES($1,$2)`, planID, f.clipID); err != nil {
			t.Fatal(err)
		}
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		if _, err = a.authorizeNonraw(f.ctx, tx, entry); err == nil {
			t.Fatal("live hold added after reservation still authorized wire")
		}
		if f.deletes() != 0 {
			t.Fatal("DELETE reached the provider despite a live hold")
		}
	})
}

// rawDeliveryFixture builds a disposable-PG schema with RAW clips that carry an
// exact delivery_keep_holds row, so an exact-key delivery hold really is the
// only thing standing between the clip and deletion.
type rawDeliveryFixture struct {
	pool     *pgxpool.Pool
	ctx      context.Context
	policy   string
	picks    string
	drain    string
	drainSHA string
	root     string
	sdk      *inverseSDKStore
	store    *inverseFakeStore
	deletes  func() int64
	keys     []string
}

func newRawDeliveryFixture(t *testing.T, prefix string, holdKeys ...string) *rawDeliveryFixture {
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
	w := windows[3]
	root := t.TempDir()
	f := &rawDeliveryFixture{pool: pool, ctx: ctx, policy: policy, root: root}
	w3 := w
	picksPath := filepath.Join(root, "picks.json")
	picksData, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "none", KeepStartUTC: w3.Start.Format(time.RFC3339), KeepEndUTC: w3.End.Format(time.RFC3339)}}})
	if err = os.WriteFile(picksPath, picksData, 0600); err != nil {
		t.Fatal(err)
	}
	f.picks = picksPath
	f.exec(`INSERT INTO accounts(id,email) VALUES(47,'rawdelivery@test.invalid');
 INSERT INTO account_api_keys(id,account_id,key_prefix,secret_hash) VALUES(1,47,'test-rawdelivery','not-a-real-credential');
 INSERT INTO connections(id,account_id,kind,api_key_id) VALUES(13,47,'nas_pull',1);
 INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps) VALUES(3,'test','3','rawdelivery','rawdelivery','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30);
 INSERT INTO storage_destinations(id,account_id,name,endpoint,region,bucket,access_key_id,secret_access_key_enc) VALUES(1,47,'test','https://test.invalid','auto','stoarama','fixture',decode('00','hex'));
 INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at) VALUES(30,47,1,'rawdelivery','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now());`)
	f.store = &inverseFakeStore{objects: map[string]r2.ObjectInfo{}}
	f.sdk, f.deletes = inverseSDKTestStore(t, f.store)
	f.store.endpointSHA = f.sdk.EndpointSHA256()
	for i, key := range holdKeys {
		etag := fmt.Sprintf("delivery-etag-%d", i)
		f.store.objects[key] = r2.ObjectInfo{Key: key, ETag: etag, SizeBytes: 100, LastModified: inverseRetirementCutoff().Add(-time.Second)}
		f.exec(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 VALUES($1,30,1,'https://test.invalid','stoarama',$2,100,$3,$4,'2026-06-01'::timestamptz,'2026-06-01'::timestamptz,'2026-06-01 00:01:00'::timestamptz,'delivery/'||$5)`, 9100000+i, key, etag, strings.Repeat("b", 64), fmt.Sprintf("%d.mp4", i))
		f.exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,$2,100,'metadata')`, key, etag)
		f.keys = append(f.keys, key)
	}
	f.exec(`INSERT INTO r2_retirement_writer_fences(bucket,endpoint_sha256) VALUES('stoarama',$1)`, f.sdk.EndpointSHA256())
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`SET LOCAL session_replication_role=replica`, nil},
		{`UPDATE r2_retirement_writer_fences SET activated_at='2026-11-12T14:28:47.037031Z'`, nil},
		{`UPDATE r2_retirement_writer_fences SET drain_not_before=clock_timestamp()-interval '1 hour'`, nil},
	} {
		if _, err = tx.Exec(ctx, q.sql, q.args...); err != nil {
			t.Fatal(q.sql, err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}

// seed inserts with replication role disabled. Fixture rows are created by the
// test, not by a writer the activated fence is meant to exclude, so the writer
// admission must not be exercised by fixture setup itself.
func (f *rawDeliveryFixture) seed(query string, args ...any) {
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		panic(err)
	}
	if _, err = tx.Exec(f.ctx, `SET LOCAL session_replication_role=replica`); err != nil {
		panic(err)
	}
	if _, err = tx.Exec(f.ctx, query, args...); err != nil {
		panic(fmt.Sprintf("seed %q: %v", query, err))
	}
	if err = tx.Commit(f.ctx); err != nil {
		panic(err)
	}
}

func (f *rawDeliveryFixture) exec(query string, args ...any) {
	if _, err := f.pool.Exec(f.ctx, query, args...); err != nil {
		panic(fmt.Sprintf("fixture %q: %v", query, err))
	}
}

// applier builds a B1 outside-keep28 applier. Reviewed selects the owner fence
// policy, which is the only thing that can grant RAW delivery eligibility.
func (f *rawDeliveryFixture) applier(t *testing.T, reviewed bool) *inverseApplier {
	t.Helper()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	var activated, deadline time.Time
	if err = f.pool.QueryRow(f.ctx, `SELECT activated_at,drain_not_before FROM r2_retirement_writer_fences`).Scan(&activated, &deadline); err != nil {
		t.Fatal(err)
	}
	drain := inverseDrainEvidence{Proof: inversePUTCompletionProof, Bucket: "stoarama", EndpointSHA256: f.sdk.EndpointSHA256(), FenceActivatedAt: activated, DrainNotBefore: deadline, ObservedAt: time.Now().UTC(), DeployedWriterCoverage: "fixture", AcceptedPUTCompletion: "fixture"}
	if reviewed {
		drain = inverseDrainEvidence{Proof: inverseOwnerFenceProof, Bucket: "stoarama", EndpointSHA256: f.sdk.EndpointSHA256(), FenceActivatedAt: activated, DrainNotBefore: deadline, ObservedAt: time.Now().UTC(),
			FencePolicy: &inverseFencePolicyEvidence{PartitionSHA256: strings.Repeat("d", 64), SummarySHA256: strings.Repeat("c", 64), LastModifiedBefore: inverseRetirementCutoff(), OwnerDecision: inverseEvidenceReference{Path: "../../../docs/records/r2-owner-fence-policy-20261118.md", SHA256: inverseOwnerDecisionSHA256}, ResidualAcceptance: inverseApprovedFenceResidual}}
	}
	f.drain = filepath.Join(f.root, fmt.Sprintf("drain-%v.json", reviewed))
	data, _ := json.Marshal(drain)
	if err = os.WriteFile(f.drain, data, 0600); err != nil {
		t.Fatal(err)
	}
	f.drainSHA, _ = inverseFileSHA(f.drain)
	summary := inverseSummary{Version: 1, Bucket: "stoarama", EndpointSHA256: f.sdk.EndpointSHA256(), CreatedAt: time.Now().UTC(), PolicySHA256: f.policy, PicksSHA256: picks.SHA256, ReadOnly: true}
	o := inverseApplyOptions{Lane: inverseOutsideKeep28, Manifest: f.root + "/partition.jsonl", ManifestSHA: strings.Repeat("d", 64), Summary: f.root + "/summary.json", SummarySHA: strings.Repeat("c", 64), Picks: f.picks, Ledger: filepath.Join(f.root, "ledger-"+fmt.Sprint(reviewed)+".jsonl"), DrainEvidence: f.drain, DrainSHA: f.drainSHA, MaxItems: 100, MaxBytes: 1 << 30, Workers: 2, ReviewedDelivery: reviewed}
	protection, err := loadInverseProtection(f.ctx, f.pool, "stoarama", picks, windowsForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := openInverseLedger(o.Ledger)
	if err != nil {
		t.Fatal(err)
	}
	var epoch int64
	if err = f.pool.QueryRow(f.ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	return &inverseApplier{pool: f.pool, store: f.sdk, opts: o, summary: summary, protection: protection, ledger: ledger, keEpoch: epoch}
}

func windowsForTest(t *testing.T) map[int64]api.Keep28Window {
	t.Helper()
	_, windows, err := api.Keep28Policy()
	if err != nil {
		t.Fatal(err)
	}
	return windows
}

func (f *rawDeliveryFixture) entry(i int) inverseEntry {
	return inverseEntry{ObjectInfo: f.store.objects[f.keys[i]], Class: inverseDeletable}
}

// TestR2RawReviewedDeliveryIsEligible is the positive case: an exact-key delivery
// hold on a mapped RAW clip is eligible under the reviewed owner fence policy,
// through a typed RAW intent plus the matching reviewed reservation class.
func TestR2RawReviewedDeliveryIsEligible(t *testing.T) {
	key := "managed/acct-47/delivery/000.mp4"
	f := newRawDeliveryFixture(t, "raw_delivery_pos", key)
	a := f.applier(t, true)
	ok, err := a.delete(f.ctx, f.entry(0))
	if err != nil || !ok || f.deletes() != 1 {
		t.Fatalf("reviewed RAW delivery not retired: ok=%v err=%v deletes=%d", ok, err, f.deletes())
	}
	var class string
	if err = f.pool.QueryRow(f.ctx, `SELECT retirement_class FROM r2_retirement_reservations WHERE kind='raw' AND object_key=$1`, key).Scan(&class); err != nil || class != inverseNonrawRetirementClass {
		t.Fatalf("reservation class %q err %v", class, err)
	}
	var intentClass string
	if err = f.pool.QueryRow(f.ctx, `SELECT retirement_class FROM r2_inverse_intents WHERE object_key=$1`, key).Scan(&intentClass); err != nil || intentClass != inverseNonrawRetirementClass {
		t.Fatalf("intent class %q err %v", intentClass, err)
	}
	var receipts int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_object_retirements WHERE object_key=$1 AND kind='raw'`, key).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("receipt %d err %v", receipts, err)
	}
	a.ledger.close()
}

// TestR2RawDeliveryVetoesStayStrict is the negative case: without the reviewed
// scope, with a mismatched identity, or with an untyped/legacy intent, the exact
// delivery hold still wins and no DELETE happens.
func TestR2RawDeliveryVetoesStayStrict(t *testing.T) {
	t.Run("legacy_default_scope_still_vetoes", func(t *testing.T) {
		key := "managed/acct-47/delivery/100.mp4"
		f := newRawDeliveryFixture(t, "raw_delivery_legacy", key)
		a := f.applier(t, false)
		defer a.ledger.close()
		if ok, err := a.delete(f.ctx, f.entry(0)); err == nil || ok || f.deletes() != 0 {
			t.Fatalf("legacy scope deleted a delivery-held RAW key: ok=%v err=%v deletes=%d", ok, err, f.deletes())
		}
	})

	t.Run("reviewed_class_without_typed_intent_vetoes", func(t *testing.T) {
		key := "managed/acct-47/delivery/200.mp4"
		f := newRawDeliveryFixture(t, "raw_delivery_notyped", key)
		// The reservation alone, with the reviewed class but no typed intent, must
		// still be refused by 0190.
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class)
 SELECT 'raw',c.id,'stoarama',c.object_key,c.size_bytes,lower(c.sha256),repeat('d',64),$2 FROM recording_clips c WHERE c.object_key=$1`, key, inverseNonrawRetirementClass); err == nil {
			t.Fatal("reviewed class without a typed intent bypassed the delivery hold")
		}
	})

	t.Run("wrong_scope_class_on_typed_intent_vetoes", func(t *testing.T) {
		key := "managed/acct-47/delivery/300.mp4"
		f := newRawDeliveryFixture(t, "raw_delivery_wrongclass", key)
		// A typed intent with an empty (legacy) class must not authorize the class.
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_inverse_intents(bucket,object_key,endpoint_sha256,stream_id,source_id,size_bytes,etag,last_modified,manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,choice,retirement_class)
 SELECT 'stoarama',c.object_key,$2,3,c.id,c.size_bytes,c.etag,'2026-11-01T00:00:00Z',repeat('d',64),$3,repeat('e',64),repeat('a',64),'none','' FROM recording_clips c WHERE c.object_key=$1`, key, f.sdk.EndpointSHA256(), f.policy); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class)
 SELECT 'raw',c.id,'stoarama',c.object_key,c.size_bytes,lower(c.sha256),repeat('d',64),$2 FROM recording_clips c WHERE c.object_key=$1`, key, inverseNonrawRetirementClass); err == nil {
			t.Fatal("empty-class intent authorized the reviewed retirement class")
		}
	})

	t.Run("mismatched_identity_does_not_inherit_exemption", func(t *testing.T) {
		key := "managed/acct-47/delivery/400.mp4"
		f := newRawDeliveryFixture(t, "raw_delivery_mismatch", key)
		var other int64 = 9100999
		otherKey := "managed/acct-47/delivery/other.mp4"
		f.seed(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 VALUES($1,30,1,'https://test.invalid','stoarama',$2,100,'other-etag',repeat('b',64),'2026-06-01'::timestamptz,'2026-06-01'::timestamptz,'2026-06-01 00:01:00'::timestamptz,'delivery/other.mp4')`, other, otherKey)
		// A typed reviewed intent for a DIFFERENT key/source must not exempt this
		// delivery-held key: the exemption is exact identity, never the class alone.
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_inverse_intents(bucket,object_key,endpoint_sha256,stream_id,source_id,size_bytes,etag,last_modified,manifest_sha256,policy_sha256,picks_sha256,drain_evidence_sha256,choice,retirement_class)
 VALUES('stoarama',$1,$2,3,$3,100,'other-etag','2026-11-01T00:00:00Z',repeat('d',64),$4,repeat('e',64),repeat('a',64),'none',$5)`, otherKey, f.sdk.EndpointSHA256(), other, f.policy, inverseNonrawRetirementClass); err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class)
 SELECT 'raw',c.id,'stoarama',c.object_key,c.size_bytes,lower(c.sha256),repeat('d',64),$2 FROM recording_clips c WHERE c.object_key=$1`, key, inverseNonrawRetirementClass); err == nil {
			t.Fatal("mismatched identity inherited the reviewed exemption")
		}
	})
}

// TestR2NonrawOutputIntervalReloadedAtWire proves blocker (1): the output's OWN
// live interval is a classifier input, so changing it after approval must never
// authorize a DELETE. While an intent is pending the freeze refuses the edit
// outright; either way the wire must not run and nothing is deleted.
func TestR2NonrawOutputIntervalReloadedAtWire(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate string
	}{
		{"interval_moved_into_keep28", `UPDATE recording_collation_outputs SET start_at=$2, end_at=$3 WHERE id=$1`},
		// The interval itself already wraps the pinned window, so only the hour's
		// own timezone changes the pinned-window match.
		{"own_timezone_mismatched", `UPDATE streams SET local_timezone='UTC' WHERE id=3`},
		{"own_stream_moved", `UPDATE recording_collation_hours SET recording_id=31 WHERE id=1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNonrawFixture(t, "nonraw_interval_"+strings.ReplaceAll(tc.name, "_", ""))
			// A second recording on a stream with NO pinned window exists so the
			// hour-move case genuinely changes the output's pinned-window class.
			f.fixture(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(4,'test','4','nowindow','nowindow','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30)`)
			f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'other','https://test.invalid','* * * * *','Asia/Tokyo','paused',4,now())`)
			a := f.applier(t, "run1")
			entry := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: f.key, ETag: "collated-etag", SizeBytes: 900, LastModified: inverseRetirementCutoff().Add(-time.Second)}, Class: inverseCollated}
			// Reserve first, exactly as the real flow does.
			if _, err := a.reserveNonraw(f.ctx, entry); err != nil {
				t.Fatal(err)
			}
			w := f.windows[3]
			// The pending-intent freeze refuses this edit in normal operation; that is
			// asserted separately by TestR2NonrawPendingIntentFreezesOutputInterval.
			// Here the edit is applied with triggers disabled, exactly as an older
			// binary or a bypass would, and the KE epoch is deliberately left
			// unchanged, matching the reviewer's probe. This proves the WIRE BOUNDARY
			// re-derives the output's own live interval instead of trusting the
			// cached batch assessment.
			// Pass arguments only for the statements that take any; pgx rejects a
			// surplus argument rather than ignoring it.
			switch {
			case strings.Contains(tc.mutate, "$2"):
				f.fixture(tc.mutate, f.outputID, w.Start.Add(time.Minute), w.Start.Add(2*time.Minute))
			case strings.Contains(tc.mutate, "$1"):
				f.fixture(tc.mutate, f.outputID)
			default:
				f.fixture(tc.mutate)
			}
			tx, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(f.ctx)
			if _, err = a.authorizeNonraw(f.ctx, tx, entry); err == nil {
				t.Fatalf("%s: wire authorized after the output interval changed", tc.name)
			}
			if f.deletes() != 0 {
				t.Fatalf("%s: DELETE reached the provider", tc.name)
			}
		})
	}

	// Control: an unchanged output is still eligible at the same boundary, so this
	// cannot pass by refusing everything.
	t.Run("unchanged_control_is_eligible", func(t *testing.T) {
		f := newNonrawFixture(t, "nonraw_interval_control")
		a := f.applier(t, "run1")
		entry := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: f.key, ETag: "collated-etag", SizeBytes: 900, LastModified: inverseRetirementCutoff().Add(-time.Second)}, Class: inverseCollated}
		if _, err := a.reserveNonraw(f.ctx, entry); err != nil {
			t.Fatal(err)
		}
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		if _, err = a.authorizeNonraw(f.ctx, tx, entry); err != nil {
			t.Fatalf("unchanged output refused: %v", err)
		}
		a.ledger.close()
	})
}

// TestR2NonrawPendingIntentFreezesOutputInterval proves the second half of blocker
// (1): once a pending intent exists, the output's interval is frozen, so the
// mutation can never be applied behind the executor.
func TestR2NonrawPendingIntentFreezesOutputInterval(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_interval_freeze")
	a := f.applier(t, "run1")
	entry := inverseEntry{ObjectInfo: r2.ObjectInfo{Key: f.key, ETag: "collated-etag", SizeBytes: 900, LastModified: inverseRetirementCutoff().Add(-time.Second)}, Class: inverseCollated}
	if _, err := a.reserveNonraw(f.ctx, entry); err != nil {
		t.Fatal(err)
	}
	a.ledger.close()
	w := f.windows[3]
	for _, q := range []string{
		`UPDATE recording_collation_outputs SET start_at=start_at-interval '1 hour' WHERE id=$1`,
		`UPDATE recording_collation_outputs SET end_at=end_at+interval '1 hour' WHERE id=$1`,
	} {
		if _, err := f.pool.Exec(f.ctx, q, f.outputID); err == nil {
			t.Fatalf("pending intent did not freeze the output interval: %s", q)
		}
	}
	_ = w
}

// TestR2NonrawCollatedHoldFollowsReservedSources proves blocker (2): a delivery
// hold admitted on a collated OUTPUT that has a reserved/pending RAW source must
// still be vetoed by the transitive closure, while unrelated holds stay admitted.
func TestR2NonrawCollatedHoldFollowsReservedSources(t *testing.T) {
	t.Run("hold_on_output_with_reserved_source_is_vetoed", func(t *testing.T) {
		f := newNonrawFixture(t, "nonraw_closure_reserved")
		// Reserve the RAW source clip through the real B1 path.
		f.fixture(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 SELECT 'raw',c.id,c.bucket,c.object_key,c.size_bytes,lower(c.sha256),repeat('d',64) FROM recording_clips c WHERE c.id=$1`, f.clipID)
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',900,'metadata')`, f.key); err == nil {
			t.Fatal("hold on a collated output with a reserved source was admitted")
		}
	})

	t.Run("hold_on_output_with_pending_collated_retirement_is_vetoed", func(t *testing.T) {
		f := newNonrawFixture(t, "nonraw_closure_pending")
		f.insertIntent(t, strings.Repeat("d", 64))
		f.fixture(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256,retirement_class)
 VALUES('collated',$1,'stoarama',$2,900,$3,$4,$5)`, f.outputID, f.key, nonrawOutputSHA, strings.Repeat("d", 64), inverseNonrawRetirementClass)
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,'hold',900,'metadata')`, f.key); err == nil {
			t.Fatal("hold on an output with a pending collated retirement was admitted")
		}
		// Content and normalized-path aliases of the same output must be vetoed too.
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,sha256) VALUES('stoarama','datasets/alias/content.json','hold',900,'metadata',$1)`, nonrawOutputSHA); err == nil {
			t.Fatal("content alias of a pending collated output was admitted")
		}
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind,relative_path) VALUES('stoarama','datasets/alias/path.json','hold',900,'metadata','nightly/video.mp4')`); err == nil {
			t.Fatal("path alias of a pending collated output was admitted")
		}
	})

	t.Run("unrelated_hold_still_admitted", func(t *testing.T) {
		f := newNonrawFixture(t, "nonraw_closure_unrelated")
		f.fixture(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 SELECT 'raw',c.id,c.bucket,c.object_key,c.size_bytes,lower(c.sha256),repeat('d',64) FROM recording_clips c WHERE c.id=$1`, f.clipID)
		// A genuinely unrelated row must never be blocked by another key's closure.
		if _, err := f.pool.Exec(f.ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama','datasets/unrelated/other.json','hold',5,'metadata')`); err != nil {
			t.Fatalf("unrelated hold was blocked: %v", err)
		}
	})
}

// TestR2NonrawHourMoveJudgesOnlyItsOwnOutputs proves blocker (3): moving an hour
// whose outputs are unrelated to a reservation is admitted, while a move of an
// affected hour is refused.
func TestR2NonrawHourMoveJudgesOnlyItsOwnOutputs(t *testing.T) {
	t.Run("unrelated_empty_hour_can_move", func(t *testing.T) {
		f := newNonrawFixture(t, "nonraw_hour_unrelated")
		// A second recording holds one reserved clip that has nothing to do with
		// this hour.
		f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'other','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now())`)
		f.fixture(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 VALUES(7000002,31,1,'https://test.invalid','stoarama','managed/acct-47/nonraw/other.mp4',10,'o',repeat('d',64),'2026-06-01'::timestamptz,'2026-06-01'::timestamptz,'2026-06-01 00:01:00'::timestamptz,'other/o.mp4')`)
		f.fixture(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 SELECT 'raw',c.id,c.bucket,c.object_key,c.size_bytes,lower(c.sha256),repeat('d',64) FROM recording_clips c WHERE c.id=7000002`)
		// An hour with no affected outputs may move to that recording.
		f.fixture(`INSERT INTO recording_collation_hours(id,hour_id,batch_id,generation,recording_id,local_date,delivery_hour,status,manifest_object_key,manifest_sha256,manifest,source_clip_count,included_clip_count,excluded_clip_count,join_count,split_count)
 VALUES(2,'nonraw__recording-30__date-2026-06-02__hour-01__generation-2','nonraw',2,30,'2026-06-02',1,'collated','joined/nightly-collation-v2/m2.json',repeat('a',64),'{}',0,0,0,0,0)`)
		if _, err := f.pool.Exec(f.ctx, `UPDATE recording_collation_hours SET recording_id=31 WHERE id=2`); err != nil {
			t.Fatalf("unrelated hour move was blocked: %v", err)
		}
	})

	t.Run("affected_hour_move_is_refused", func(t *testing.T) {
		f := newNonrawFixture(t, "nonraw_hour_affected")
		f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'other','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now())`)
		f.fixture(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
 SELECT 'raw',c.id,c.bucket,c.object_key,c.size_bytes,lower(c.sha256),repeat('d',64) FROM recording_clips c WHERE c.id=$1`, f.clipID)
		// This hour's own source is reserved, so moving it is a new overlap.
		if _, err := f.pool.Exec(f.ctx, `UPDATE recording_collation_hours SET recording_id=31 WHERE id=1`); err == nil {
			t.Fatal("move of an affected hour was admitted")
		}
	})

	t.Run("affected_hour_pending_collated_retirement_is_refused", func(t *testing.T) {
		f := newNonrawFixture(t, "nonraw_hour_pending")
		f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'other','https://test.invalid','* * * * *','Asia/Tokyo','paused',3,now())`)
		f.insertIntent(t, strings.Repeat("d", 64))
		if _, err := f.pool.Exec(f.ctx, `UPDATE recording_collation_hours SET recording_id=31 WHERE id=1`); err == nil {
			t.Fatal("move of an hour with a pending collated retirement was admitted")
		}
	})
}

// TestR2InverseEpochCoversClassifierInputs enforces ROOT's rule: the frozen
// identity is exactly the classifier's input set. Every declared classifier input
// must bump the KE epoch in real PostgreSQL, and a catalog edit to one of them
// must therefore invalidate a cached approval. Adding a classifier input without
// widening the epoch fails here instead of silently escaping the freeze.
func TestR2InverseEpochCoversClassifierInputs(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_epoch_cover")
	epoch := func() int64 {
		t.Helper()
		var n int64
		if err := f.pool.QueryRow(f.ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// Each declared column must be covered by a statement-level epoch trigger.
	for _, in := range inverseClassifierInputs {
		var cols []string
		rows, err := f.pool.Query(f.ctx, `SELECT distinct a.attname FROM pg_trigger tg
 JOIN pg_class c ON c.oid=tg.tgrelid JOIN pg_proc p ON p.oid=tg.tgfoid
 CROSS JOIN LATERAL unnest(tg.tgattr::int2[]) col JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=col
 WHERE c.relname=$1 AND p.proname='advance_r2_inverse_ke_epoch' AND NOT tg.tgisinternal`, in.Table)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var name string
			if err = rows.Scan(&name); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			cols = append(cols, name)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		covered := map[string]bool{}
		for _, c := range cols {
			covered[c] = true
		}
		for _, want := range in.Columns {
			if !covered[want] {
				t.Fatalf("%s.%s is a classifier input but no epoch trigger covers it", in.Table, want)
			}
		}
		// Hold-covered inputs are asserted by behaviour below, not by the trigger.
		for _, held := range in.HoldCovered {
			if covered[held] {
				t.Fatalf("%s.%s is hold-covered but also epoch-versioned; that self-deadlocks the purge probe", in.Table, held)
			}
		}
	}

	// purged_at is hold-covered: it must NOT bump the epoch (that self-deadlocks the
	// pre-delete purge probe) but must still HOLD the key. The hold is proven by
	// TestR2NonrawExecutorLane and the B1 source-identity freeze tests.
	for _, in := range inverseClassifierInputs {
		for _, held := range in.HoldCovered {
			var covered bool
			if err := f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_trigger tg
 JOIN pg_class c ON c.oid=tg.tgrelid JOIN pg_proc p ON p.oid=tg.tgfoid
 CROSS JOIN LATERAL unnest(tg.tgattr::int2[]) col JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=col
 WHERE c.relname=$1 AND p.proname='advance_r2_inverse_ke_epoch' AND a.attname=$2 AND NOT tg.tgisinternal)`,
				in.Table, held).Scan(&covered); err != nil {
				t.Fatal(err)
			}
			if covered {
				t.Fatalf("%s.%s must stay hold-covered, not epoch-versioned", in.Table, held)
			}
		}
	}

	// And the epoch really moves for each such edit, so the cached approval dies.
	for _, tc := range []struct {
		name  string
		apply string
	}{
		{"own_interval", `UPDATE recording_collation_outputs SET start_at=start_at-interval '1 minute' WHERE id=1`},
		{"own_end", `UPDATE recording_collation_outputs SET end_at=end_at-interval '1 minute' WHERE id=1`},
		{"own_verified_at", `UPDATE recording_collation_outputs SET r2_verified_at=r2_verified_at-interval '1 second' WHERE id=1`},
		{"source_interval", `UPDATE recording_clips SET clip_start_at=clip_start_at-interval '1 minute' WHERE id=7000001`},

		{"source_sha256", `UPDATE recording_clips SET sha256=repeat('e',64) WHERE id=7000001`},
		{"source_etag", `UPDATE recording_clips SET etag='changed' WHERE id=7000001`},
		{"stream_timezone", `UPDATE streams SET local_timezone='UTC' WHERE id=3`},
		{"recording_cron_timezone", `UPDATE recordings SET cron_timezone='UTC' WHERE id=30`},
		{"recording_stream", `UPDATE recordings SET stream_id=NULL WHERE id=30`},
		{"hour_recording", `UPDATE recording_collation_hours SET recording_id=30 WHERE id=1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := epoch()
			// Interval and identity edits are all legitimate here: no intent is
			// pending in this fixture, so only the epoch contract is under test.
			if _, err := f.pool.Exec(f.ctx, tc.apply); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if after := epoch(); after <= before {
				t.Fatalf("%s did not bump the KE epoch (%d -> %d)", tc.name, before, after)
			}
		})
	}
}
