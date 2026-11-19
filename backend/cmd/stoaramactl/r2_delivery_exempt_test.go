package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
)

// The fixture's confident repair: a delivery page video_assembly object with no
// catalog row. Its content hash is the one the fake provider reports.
const exemptRepairKey = "experimental/stitching-confident-repairs/20261009/30/" + nonrawOutputSHA + "/30_hour_01_parts_01_02_v799.mp4"

func (f *nonrawFixture) pageHold(key, etag, kind string, size int64) {
	f.pageHoldSHA(key, etag, kind, size, nonrawOutputSHA)
}

func (f *nonrawFixture) pageHoldSHA(key, etag, kind string, size int64, sha string) {
	f.exec(`INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,recording_id,kind,sha256,relative_path)
 VALUES('stoarama',$1,$2,$3,30,$4,$5,'30_nonraw/June/Monday/30_hour_01_part_01.mp4')`, key, etag, size, kind, sha)
}

// withPageKeys adds the two page keys the lane tests retire: the catalog output
// as a collated page video and a confident repair present in the bucket.
func (f *nonrawFixture) withPageKeys() {
	f.pageHold(f.key, "collated-etag", "video_collated", 900)
	f.pageHold(exemptRepairKey, "repair-etag", "video_assembly", 700)
	f.store.objects[exemptRepairKey] = r2.ObjectInfo{Key: exemptRepairKey, ETag: "repair-etag", SizeBytes: 700, LastModified: inverseRetirementCutoff().Add(-time.Second)}
}

func (f *nonrawFixture) seed(t *testing.T, class deliveryExemptionClass, rows ...deliveryExemptionRow) error {
	t.Helper()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seedDeliveryExemptions(f.ctx, f.pool, class, rows, strings.Repeat("a", 64), picks)
	return err
}

func (f *nonrawFixture) mustSeed(t *testing.T, class deliveryExemptionClass, rows ...deliveryExemptionRow) {
	t.Helper()
	if err := f.seed(t, class, rows...); err != nil {
		t.Fatal(err)
	}
}

func (f *nonrawFixture) exempt(t *testing.T, key string) bool {
	t.Helper()
	var open bool
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_delivery_exempt('stoarama',$1)`, key).Scan(&open); err != nil {
		t.Fatal(err)
	}
	return open
}

func (f *nonrawFixture) retirePage() {
	f.exec(`INSERT INTO r2_delivery_page_retirement(decision) VALUES('test: the old page is down')`)
}

func collatedRow(f *nonrawFixture) deliveryExemptionRow {
	return deliveryExemptionRow{Bucket: "stoarama", ObjectKey: f.key, ETag: "collated-etag", SizeBytes: 900}
}

func repairRow() deliveryExemptionRow {
	return deliveryExemptionRow{Bucket: "stoarama", ObjectKey: exemptRepairKey, ETag: "repair-etag", SizeBytes: 700}
}

// TestR2NonrawExemptionAdmitsOnlyExactIdentity: an exemption lands only on a live
// page key with its exact ETag and size and a fitting class, and drift in the
// page row afterwards closes it again.
func TestR2NonrawExemptionAdmitsOnlyExactIdentity(t *testing.T) {
	f := newNonrawFixture(t, "exempt_identity")
	f.withPageKeys()
	for name, row := range map[string]deliveryExemptionRow{
		"etag":      {Bucket: "stoarama", ObjectKey: f.key, ETag: "other", SizeBytes: 900},
		"size":      {Bucket: "stoarama", ObjectKey: f.key, ETag: "collated-etag", SizeBytes: 901},
		"not_page":  {Bucket: "stoarama", ObjectKey: "joined/nightly-collation-v2/objects/" + strings.Repeat("c", 64) + ".mp4", ETag: "x", SizeBytes: 1},
		"bucket":    {Bucket: "other", ObjectKey: f.key, ETag: "collated-etag", SizeBytes: 900},
		"key_only":  {Bucket: "stoarama", ObjectKey: f.key + "x", ETag: "collated-etag", SizeBytes: 900},
		"wrong_fit": repairRow(),
	} {
		if err := f.seed(t, deliveryExemptOutsideWindow, row); err == nil {
			t.Fatalf("%s: inexact exemption admitted", name)
		}
	}
	if err := f.seed(t, deliveryExemptConfidentRepair, collatedRow(f)); err == nil {
		t.Fatal("a collated page video was admitted as a confident repair")
	}
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_delivery_exemptions`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("refused seeds left %d exemptions (%v)", n, err)
	}
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	if !f.exempt(t, f.key) || f.exempt(t, exemptRepairKey) {
		t.Fatal("exemption is not exactly the seeded key")
	}
	// Exemptions are permanent records.
	for _, q := range []string{`UPDATE r2_delivery_exemptions SET etag='x'`, `DELETE FROM r2_delivery_exemptions`, `TRUNCATE r2_delivery_exemptions`} {
		if _, err := f.pool.Exec(f.ctx, q); err == nil {
			t.Fatalf("%s succeeded", q)
		}
	}
	// A page row whose identity drifts (only a separately reviewed migration can
	// rewrite one) is held again.
	f.fixture(`UPDATE delivery_keep_holds SET etag='rewritten' WHERE object_key=$1`, f.key)
	if f.exempt(t, f.key) {
		t.Fatal("exemption survived a page identity change")
	}
}

// TestR2NonrawExemptionClassGates: each class opens only on its own condition.
func TestR2NonrawExemptionClassGates(t *testing.T) {
	f := newNonrawFixture(t, "exempt_gates")
	f.withPageKeys()
	inSHA := strings.Repeat("e", 64)
	inWindow := "joined/goodplus-20260821-generation-1/objects/" + inSHA + ".mp4"
	f.pageHoldSHA(inWindow, "in-etag", "video_collated", 300, inSHA)
	f.fixture(`INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(10000003,1,3,3,$1,'nightly/in.mp4',300,$2,'2026-06-01','2026-06-01 00:01:00',ARRAY[7000001]::bigint[],'2026-06-02T00:00:00Z')`, inWindow, inSHA)
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	f.mustSeed(t, deliveryExemptConfidentRepair, repairRow())
	f.mustSeed(t, deliveryExemptInWindow, deliveryExemptionRow{Bucket: "stoarama", ObjectKey: inWindow, ETag: "in-etag", SizeBytes: 300})

	if !f.exempt(t, f.key) {
		t.Fatal("outside_window is not open once seeded")
	}
	if f.exempt(t, exemptRepairKey) || f.exempt(t, inWindow) {
		t.Fatal("a page-retirement class opened while the page is up")
	}
	f.retirePage()
	if !f.exempt(t, exemptRepairKey) {
		t.Fatal("confident_repair stayed closed after the page came down")
	}
	if f.exempt(t, inWindow) {
		t.Fatal("in_window opened without NAS custody")
	}
	// The 0224 follow-up's hook, certifying one exact identity. A certificate for
	// any other output, ETag, size or content leaves the key held.
	certify := func(output int64, etag string, size int64, sha string) {
		f.exec(fmt.Sprintf(`CREATE OR REPLACE FUNCTION r2_delivery_nas_certified(p_output_id bigint,p_bucket text,p_key text,p_etag text,p_size bigint,p_sha256 text)
 RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT p_output_id=%d AND p_bucket='stoarama' AND p_key='%s' AND p_etag='%s' AND p_size=%d AND p_sha256='%s' $$`, output, inWindow, etag, size, sha))
	}
	for _, wrong := range []func(){
		func() { certify(f.outputID, "in-etag", 300, inSHA) },
		func() { certify(10000003, "other", 300, inSHA) },
		func() { certify(10000003, "in-etag", 301, inSHA) },
		func() { certify(10000003, "in-etag", 300, strings.Repeat("c", 64)) },
	} {
		wrong()
		if f.exempt(t, inWindow) {
			t.Fatal("in_window opened on a mismatched NAS certificate")
		}
	}
	certify(10000003, "in-etag", 300, inSHA)
	if !f.exempt(t, inWindow) {
		t.Fatal("in_window stayed closed with the page down and NAS custody")
	}
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_delivery_page_retirement(decision) VALUES('again')`); err == nil {
		t.Fatal("a second page retirement marker landed")
	}
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM r2_delivery_page_retirement`); err == nil {
		t.Fatal("the page retirement marker was removed")
	}
}

// TestR2NonrawExemptionHeldKeysRefuseStaleLedgerWrites: a binary that knows
// nothing of exemptions still cannot reserve or retire a page key unless it is
// exempt now and the row names the page's exact size and content.
func TestR2NonrawExemptionHeldKeysRefuseStaleLedgerWrites(t *testing.T) {
	f := newNonrawFixture(t, "exempt_stale")
	f.withPageKeys()
	ledger := func(kind, key string, id, size int64, sha string) error {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		if _, err = tx.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) VALUES($1,$2,'stoarama',$3,$4,$5,$6)`,
			kind, id, key, size, sha, strings.Repeat("d", 64)); err != nil {
			return err
		}
		_, err = tx.Exec(f.ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256) VALUES($1,$2,3,'stoarama',$3,$4,$5,'t','none',$6,$7)`,
			kind, id, key, size, sha, strings.Repeat("d", 64), f.policy)
		return err
	}
	refused := func(err error) bool { return err != nil && strings.Contains(err.Error(), "DELIVERY KEEP HOLD") }
	held := func(key string) bool {
		var h bool
		if err := f.pool.QueryRow(f.ctx, `SELECT r2_inverse_delivery_held(0,'stoarama',$1,'',$2,1)`, key, strings.Repeat("0", 64)).Scan(&h); err != nil {
			t.Fatal(err)
		}
		return h
	}
	if !refused(ledger("collated", f.key, f.outputID, 900, nonrawOutputSHA)) || !refused(ledger("delivery_repair", exemptRepairKey, 1, 700, nonrawOutputSHA)) {
		t.Fatal("a non-exempt page key reached the ledger")
	}
	if !held(f.key) || !held(exemptRepairKey) {
		t.Fatal("RAW delivery closure released a non-exempt page key")
	}
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	f.mustSeed(t, deliveryExemptConfidentRepair, repairRow())
	if !refused(ledger("delivery_repair", exemptRepairKey, 1, 700, nonrawOutputSHA)) {
		t.Fatal("a confident repair reached the ledger while the page is up")
	}
	if !refused(ledger("collated", f.key, f.outputID, 899, nonrawOutputSHA)) || !refused(ledger("collated", f.key, f.outputID, 900, strings.Repeat("c", 64))) {
		t.Fatal("an exempt key reached the ledger under another size or content")
	}
	if err := ledger("collated", f.key, f.outputID, 900, nonrawOutputSHA); err != nil {
		t.Fatalf("exact exempt key refused: %v", err)
	}
	if held(f.key) {
		t.Fatal("RAW delivery closure still holds an exempt exact key")
	}
	f.retirePage()
	if err := ledger("delivery_repair", exemptRepairKey, 1, 700, nonrawOutputSHA); err != nil {
		t.Fatalf("exact confident repair refused after page retirement: %v", err)
	}
	// A repair retires only under its own key family.
	if err := ledger("delivery_repair", f.key, 2, 900, nonrawOutputSHA); err == nil || !strings.Contains(err.Error(), "kind_prefix") {
		t.Fatal("delivery_repair receipt accepted a collated key")
	}
}

// TestR2NonrawExemptionSeedCommand: the roster digest pins the exact bytes, any
// stale identity refuses the whole class, and keep-everything streams are never
// exemptable.
func TestR2NonrawExemptionSeedCommand(t *testing.T) {
	f := newNonrawFixture(t, "exempt_seed")
	f.withPageKeys()
	var roster []byte
	for _, row := range []deliveryExemptionRow{collatedRow(f), {Bucket: "stoarama", ObjectKey: "datasets/x.json", ETag: "e", SizeBytes: 1}} {
		line, _ := json.Marshal(map[string]any{"bucket": row.Bucket, "object_key": row.ObjectKey, "etag": row.ETag, "size_bytes": row.SizeBytes, "output_id": 1})
		roster = append(append(roster, line...), '\n')
	}
	path := filepath.Join(f.root, "roster.jsonl")
	if err := os.WriteFile(path, roster, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(roster)
	digest := hex.EncodeToString(sum[:])
	if _, err := readDeliveryExemptionRoster(path, strings.Repeat("0", 64)); err == nil {
		t.Fatal("roster digest mismatch accepted")
	}
	rows, err := readDeliveryExemptionRoster(path, digest)
	if err != nil || len(rows) != 2 || rows[0] != collatedRow(f) {
		t.Fatalf("roster %v %v", rows, err)
	}
	dup := filepath.Join(f.root, "dup.jsonl")
	line, _ := json.Marshal(collatedRow(f))
	doubled := append(append(append(line, '\n'), line...), '\n')
	if err = os.WriteFile(dup, doubled, 0600); err != nil {
		t.Fatal(err)
	}
	dupSum := sha256.Sum256(doubled)
	if _, err = readDeliveryExemptionRoster(dup, hex.EncodeToString(dupSum[:])); err == nil {
		t.Fatal("repeated roster key accepted")
	}
	// datasets/x.json is not a page key: the whole class refuses, atomically.
	if err = f.seed(t, deliveryExemptOutsideWindow, rows...); err == nil {
		t.Fatal("roster with a stale identity seeded")
	}
	stale := collatedRow(f)
	stale.ETag = "stale"
	if err = f.seed(t, deliveryExemptOutsideWindow, stale); err == nil {
		t.Fatal("stale ETag seeded")
	}
	var n int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_delivery_exemptions`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("refused seeds left %d rows (%v)", n, err)
	}
	// Stream 3 becomes keep-everything: neither class may exempt its keys.
	data, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "all"}}})
	if err = os.WriteFile(f.picks, data, 0600); err != nil {
		t.Fatal(err)
	}
	if f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)) == nil || f.seed(t, deliveryExemptConfidentRepair, repairRow()) == nil {
		t.Fatal("a keep-everything stream key was exempted")
	}
}

// runBatch runs one real collated job batch over entries, with fresh owner-fence
// evidence and the job's exact durable readback.
func (f *nonrawFixture) runBatch(t *testing.T, name string, entries ...inverseEntry) (inverseApplySummary, error) {
	t.Helper()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(f.root, name)
	if err = os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	var manifest []byte
	for _, e := range entries {
		line, _ := json.Marshal(e)
		manifest = append(append(manifest, line...), '\n')
	}
	summary, _ := json.Marshal(inverseSummary{Version: 1, Bucket: "stoarama", EndpointSHA256: f.sdk.EndpointSHA256(), CreatedAt: time.Now().UTC(), PolicySHA256: f.policy, PicksSHA256: picks.SHA256, ReadOnly: true})
	o := inverseApplyOptions{Lane: inverseNonrawCollated, Manifest: filepath.Join(dir, "partition.jsonl"), Summary: filepath.Join(dir, "summary.json"), Picks: f.picks, Ledger: filepath.Join(dir, "ledger.jsonl"), Workers: 2, MaxItems: 10, MaxBytes: 1 << 30}
	for path, data := range map[string][]byte{o.Manifest: manifest, o.Summary: summary} {
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	o.ManifestSHA, _ = inverseFileSHA(o.Manifest)
	o.SummarySHA, _ = inverseFileSHA(o.Summary)
	_, result, err := runInverseJobBatch(f.ctx, f.pool, f.sdk, o, "../../../docs/records/r2-owner-fence-policy-20261118.md", filepath.Join(dir, "evidence.json"))
	return result, err
}

// TestR2NonrawExemptionLaneRetiresBothClasses runs the reviewed collated lane end
// to end over an outside-window page video (catalog row) and a confident repair
// (no catalog row): each stays on the provider until its gate opens, then leaves
// through one audited DELETE with an exact typed receipt.
func TestR2NonrawExemptionLaneRetiresBothClasses(t *testing.T) {
	f := newNonrawFixture(t, "exempt_lane")
	f.withPageKeys()
	collated := inverseEntry{ObjectInfo: f.store.objects[f.key], Class: inverseCollated}
	repair := inverseEntry{ObjectInfo: f.store.objects[exemptRepairKey], Class: inverseCollated}

	classify := func(key string) (inverseClass, string) {
		t.Helper()
		picks, err := loadR2RetirePicks(f.picks)
		if err != nil {
			t.Fatal(err)
		}
		p, err := loadInverseProtectionMode(f.ctx, f.pool, "stoarama", picks, f.windows, true)
		if err != nil {
			t.Fatal(err)
		}
		return p.classify(f.store.objects[key])
	}
	if class, reason := classify(f.key); class != inverseDelivery || reason != "delivery_page_keep" {
		t.Fatalf("unexempt page video classified %s (%s)", class, reason)
	}
	if _, err := f.runBatch(t, "unexempt", collated); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("unexempt page video reached the wire: %v deletes=%d", err, f.deletes())
	}

	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	f.mustSeed(t, deliveryExemptConfidentRepair, repairRow())
	if class, reason := classify(exemptRepairKey); class != inverseDelivery {
		t.Fatalf("confident repair left the page keep while the page is up: %s (%s)", class, reason)
	}
	if _, err := f.runBatch(t, "page_up", repair); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("confident repair reached the wire while the page is up: %v", err)
	}
	if class, reason := classify(f.key); class != inverseCollated || reason != "owner_delivery_page_outside_window" {
		t.Fatalf("exempt page video classified %s (%s)", class, reason)
	}
	result, err := f.runBatch(t, "outside_window", collated)
	if err != nil || result.Deleted != 1 || f.deletes() != 1 {
		t.Fatalf("outside_window batch: %+v %v deletes=%d", result, err, f.deletes())
	}

	f.retirePage()
	if class, reason := classify(exemptRepairKey); class != inverseCollated || reason != "owner_delivery_page_confident_repair" {
		t.Fatalf("exempt confident repair classified %s (%s)", class, reason)
	}
	result, err = f.runBatch(t, "confident_repair", repair)
	if err != nil || result.Deleted != 1 || result.Bytes != 700 || f.deletes() != 2 {
		t.Fatalf("confident_repair batch: %+v %v deletes=%d", result, err, f.deletes())
	}
	var exemptionID int64
	if err = f.pool.QueryRow(f.ctx, `SELECT id FROM r2_delivery_exemptions WHERE object_key=$1`, exemptRepairKey).Scan(&exemptionID); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string][3]any{
		f.key:           {"collated", f.outputID, "owner_delivery_page_outside_window"},
		exemptRepairKey: {"delivery_repair", exemptionID, "owner_delivery_page_confident_repair"},
	} {
		var kind, reason string
		var source int64
		if err = f.pool.QueryRow(f.ctx, `SELECT r.kind,r.source_id,r.reason FROM r2_object_retirements r
 JOIN r2_nonraw_intents i USING(bucket,object_key) JOIN r2_nonraw_attempts USING(bucket,object_key)
 JOIN r2_nonraw_events e USING(bucket,object_key) WHERE r.object_key=$1 AND i.kind=r.kind AND e.event='absent'`, key).Scan(&kind, &source, &reason); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if kind != want[0] || source != want[1] || reason != want[2] {
			t.Fatalf("%s receipt %s/%d/%s", key, kind, source, reason)
		}
	}
	if _, present := f.store.objects[exemptRepairKey]; present {
		t.Fatal("confident repair is still on the provider")
	}
}

// TestR2NonrawExemptionRepairIdentityAndKeepEverything: a confident repair whose
// listed ETag differs from its exemption, or whose stream became keep-everything,
// never reaches the wire.
func TestR2NonrawExemptionRepairIdentityAndKeepEverything(t *testing.T) {
	f := newNonrawFixture(t, "exempt_repair_hold")
	f.withPageKeys()
	f.mustSeed(t, deliveryExemptConfidentRepair, repairRow())
	f.retirePage()
	moved := inverseEntry{ObjectInfo: f.store.objects[exemptRepairKey], Class: inverseCollated}
	moved.ETag = "rewritten"
	if _, err := f.runBatch(t, "etag", moved); !errors.Is(err, errInverseNonrawIneligible) || !strings.Contains(err.Error(), "listed ETag") || f.deletes() != 0 {
		t.Fatalf("a repair with another ETag reached the wire: %v", err)
	}
	data, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "all"}}})
	if err := os.WriteFile(f.picks, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runBatch(t, "keep_everything", inverseEntry{ObjectInfo: f.store.objects[exemptRepairKey], Class: inverseCollated}); !errors.Is(err, errInverseNonrawIneligible) || !strings.Contains(err.Error(), "keep-everything") || f.deletes() != 0 {
		t.Fatalf("a keep-everything stream's repair reached the wire: %v", err)
	}
	var intents int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_nonraw_intents`).Scan(&intents); err != nil || intents != 0 {
		t.Fatalf("held repairs left %d intents (%v)", intents, err)
	}
}

// TestR2NonrawExemptionOnlyInWindowLiftsKeep28: a pinned-window output stays
// held under every exemption except an open in_window one.
func TestR2NonrawExemptionOnlyInWindowLiftsKeep28(t *testing.T) {
	key := "joined/nightly-collation-v2/objects/" + nonrawOutputSHA + ".mp4"
	for class, want := range map[deliveryExemptionClass]inverseClass{
		"": inverseHeld, deliveryExemptOutsideWindow: inverseHeld, deliveryExemptConfidentRepair: inverseHeld, deliveryExemptInWindow: inverseCollated,
	} {
		p := inverseProtection{ReviewedCollated: true, PageKeep: map[string]bool{}, Picks: r2RetirePicks{Choices: map[int64]r2RetirePick{3: {Choice: "none"}}},
			Collated: map[string]inverseCollatedAssessment{key: {ID: 1, StreamID: 3, Key: key, SHA: nonrawOutputSHA, Kind: inverseNonrawKindCollated, Keep28: true, Exemption: class}}}
		if got, reason := p.classify(r2.ObjectInfo{Key: key, SizeBytes: 1}); got != want {
			t.Fatalf("%q: keep28 output classified %s (%s)", class, got, reason)
		}
	}
}
