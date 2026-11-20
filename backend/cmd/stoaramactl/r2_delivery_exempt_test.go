package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
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

// pageGeneration is the synthetic serving registry generation the fixtures
// retire. The marker alone is inert; the registry state record is what makes the
// page actually down.
const pageGeneration = "0123456789abcdef0123456789abcdef"

// registryState records the live serving state of the page's own generation, the
// fact r2_delivery_page_retired() reads. Without it the marker is inert.
func (f *nonrawFixture) registryState(state string) {
	f.exec(`INSERT INTO r2_delivery_page_registry_events(generation,state,decision) VALUES($1,$2::r2_delivery_page_registry_state,'test: registry state')`, pageGeneration, state)
}

func (f *nonrawFixture) retirePage() {
	f.registryState("disabled")
	f.exec(`INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'test: the old page is down')`, pageGeneration)
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
 VALUES(10000003,1,3,3,$1,'nightly/in.mp4',300,$2,$3,$4,ARRAY[7000001]::bigint[],'2026-06-02T00:00:00Z')`, inWindow, inSHA, f.windows[3].Start.Add(time.Hour).Format(time.RFC3339), f.windows[3].Start.Add(2*time.Hour).Format(time.RFC3339))
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	f.mustSeed(t, deliveryExemptConfidentRepair, repairRow())
	f.mustSeed(t, deliveryExemptInWindow, deliveryExemptionRow{Bucket: "stoarama", ObjectKey: inWindow, ETag: "in-etag", SizeBytes: 300})

	if !f.exempt(t, f.key) {
		t.Fatal("outside_window is not open once seeded")
	}
	if f.exempt(t, exemptRepairKey) || f.exempt(t, inWindow) {
		t.Fatal("a page-retirement class opened while the page is up")
	}
	// The marker alone is never enough: the serving registry generation must also
	// be recorded disabled or retired. A generation still recorded as serving
	// leaves every page-retirement gate closed.
	f.registryState("serving")
	f.exec(`INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'test: premature marker')`, pageGeneration)
	var retired bool
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || retired {
		t.Fatalf("marker alone retired the page: %v %v", retired, err)
	}
	if f.exempt(t, exemptRepairKey) || f.exempt(t, inWindow) {
		t.Fatal("a marker over a still-serving generation opened a gate")
	}
	// Record the page's real disarm and the gate opens.
	f.registryState("disabled")
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || !retired {
		t.Fatalf("disabled registry did not retire the page: %v %v", retired, err)
	}
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
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'again')`, pageGeneration); err == nil {
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
	// receipt attempts ONLY the final receipt, so the reservation guard cannot
	// mask a hole in the receipt guard.
	receipt := func(kind, key string, id, size int64, sha string) error {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		_, err = tx.Exec(f.ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256) VALUES($1,$2,3,'stoarama',$3,$4,$5,'t','none',$6,$7)`,
			kind, id, key, size, sha, strings.Repeat("d", 64), f.policy)
		return err
	}
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
	// The final receipt is guarded on its own, without a reservation in front.
	if !refused(receipt("collated", f.key, f.outputID, 900, nonrawOutputSHA)) || !refused(receipt("delivery_repair", exemptRepairKey, 1, 700, nonrawOutputSHA)) {
		t.Fatal("a non-exempt page key reached the receipt")
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
	if !refused(receipt("collated", f.key, f.outputID, 899, nonrawOutputSHA)) || !refused(receipt("collated", f.key, f.outputID, 900, strings.Repeat("c", 64))) {
		t.Fatal("an exempt key reached the receipt under another size or content")
	}
	if err := ledger("collated", f.key, f.outputID, 900, nonrawOutputSHA); err != nil {
		t.Fatalf("exact exempt key refused: %v", err)
	}
	// The derived RAW delivery guard never reads exemptions.
	if !held(f.key) {
		t.Fatal("RAW delivery closure released an exempt key")
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
func (f *nonrawFixture) runBatch(t *testing.T, name string, lane inverseLane, entries ...inverseEntry) (inverseApplySummary, error) {
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
	o := inverseApplyOptions{Lane: lane, Manifest: filepath.Join(dir, "partition.jsonl"), Summary: filepath.Join(dir, "summary.json"), Picks: f.picks, Ledger: filepath.Join(dir, "ledger.jsonl"), Workers: 2, MaxItems: 10, MaxBytes: 1 << 30}
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
	if _, err := f.runBatch(t, "unexempt", inverseNonrawCollated, collated); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("unexempt page video reached the wire: %v deletes=%d", err, f.deletes())
	}

	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	f.mustSeed(t, deliveryExemptConfidentRepair, repairRow())
	if class, reason := classify(exemptRepairKey); class != inverseDelivery {
		t.Fatalf("confident repair left the page keep while the page is up: %s (%s)", class, reason)
	}
	if _, err := f.runBatch(t, "page_up", inverseNonrawCollated, repair); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("confident repair reached the wire while the page is up: %v", err)
	}
	if class, reason := classify(f.key); class != inverseCollated || reason != "owner_delivery_page_outside_window" {
		t.Fatalf("exempt page video classified %s (%s)", class, reason)
	}
	result, err := f.runBatch(t, "outside_window", inverseNonrawCollated, collated)
	if err != nil || result.Deleted != 1 || f.deletes() != 1 {
		t.Fatalf("outside_window batch: %+v %v deletes=%d", result, err, f.deletes())
	}

	f.retirePage()
	if class, reason := classify(exemptRepairKey); class != inverseCollated || reason != "owner_delivery_page_confident_repair" {
		t.Fatalf("exempt confident repair classified %s (%s)", class, reason)
	}
	result, err = f.runBatch(t, "confident_repair", inverseNonrawCollated, repair)
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
	if _, err := f.runBatch(t, "etag", inverseNonrawCollated, moved); !errors.Is(err, errInverseNonrawIneligible) || !strings.Contains(err.Error(), "listed ETag") || f.deletes() != 0 {
		t.Fatalf("a repair with another ETag reached the wire: %v", err)
	}
	data, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "all"}}})
	if err := os.WriteFile(f.picks, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runBatch(t, "keep_everything", inverseNonrawCollated, inverseEntry{ObjectInfo: f.store.objects[exemptRepairKey], Class: inverseCollated}); !errors.Is(err, errInverseNonrawIneligible) || !strings.Contains(err.Error(), "keep-everything") || f.deletes() != 0 {
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

// TestR2NonrawExemptionNeverReleasesDerivedRAW: with every exemption class
// seeded and every gate open, a RAW source of a delivery output is still held
// by the derived RAW delivery guard in an owner-fence (delivery mode) run: in
// the partition, at reserve and at live authorize, with no durable row and no
// DELETE. Only the separate reviewed per-source consumer may release it.
func TestR2NonrawExemptionNeverReleasesDerivedRAW(t *testing.T) {
	f := newNonrawFixture(t, "exempt_raw_source")
	const rawKey = "managed/acct-47/nonraw/source.mp4"
	f.store.objects[rawKey] = r2.ObjectInfo{Key: rawKey, ETag: "src-etag", SizeBytes: 500, LastModified: inverseRetirementCutoff().Add(-time.Second)}
	raw := inverseEntry{ObjectInfo: f.store.objects[rawKey], Class: inverseDeletable}
	classify := func() (inverseClass, string) {
		t.Helper()
		picks, err := loadR2RetirePicks(f.picks)
		if err != nil {
			t.Fatal(err)
		}
		p, err := loadInverseProtectionMode(f.ctx, f.pool, "stoarama", picks, f.windows, true)
		if err != nil {
			t.Fatal(err)
		}
		return p.classify(raw.ObjectInfo)
	}
	// Control: before the page holds exist, the source is ordinary RAW footage.
	if class, reason := classify(); class != inverseDeletable {
		t.Fatalf("control: RAW source classified %s (%s)", class, reason)
	}

	// The collated output that contains the source becomes a page video; all
	// three classes are seeded and every gate is opened.
	f.withPageKeys()
	inSHA := strings.Repeat("e", 64)
	inWindow := "joined/goodplus-20260821-generation-1/objects/" + inSHA + ".mp4"
	f.pageHoldSHA(inWindow, "in-etag", "video_collated", 300, inSHA)
	f.fixture(`INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(10000003,1,3,3,$1,'nightly/in.mp4',300,$2,$3,$4,ARRAY[7000001]::bigint[],'2026-06-02T00:00:00Z')`, inWindow, inSHA, f.windows[3].Start.Add(time.Hour).Format(time.RFC3339), f.windows[3].Start.Add(2*time.Hour).Format(time.RFC3339))
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	f.mustSeed(t, deliveryExemptConfidentRepair, repairRow())
	f.mustSeed(t, deliveryExemptInWindow, deliveryExemptionRow{Bucket: "stoarama", ObjectKey: inWindow, ETag: "in-etag", SizeBytes: 300})
	f.retirePage()
	f.exec(`CREATE OR REPLACE FUNCTION r2_delivery_nas_certified(p_output_id bigint,p_bucket text,p_key text,p_etag text,p_size bigint,p_sha256 text)
 RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT true $$`)
	for _, key := range []string{f.key, exemptRepairKey, inWindow} {
		if !f.exempt(t, key) {
			t.Fatalf("%s: gate not open", key)
		}
	}
	var derived bool
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_inverse_delivery_held($1,'stoarama',$2,'source/source.mp4',repeat('c',64),500)`, f.clipID, rawKey).Scan(&derived); err != nil || !derived {
		t.Fatalf("RAW source is not derived-held by its delivery output: %v %v", derived, err)
	}

	if class, reason := classify(); class != inverseDelivery {
		t.Fatalf("partition classified a derived-held RAW source %s (%s)", class, reason)
	}
	// The batch must fail with the RESERVE refusal itself, not merely some error:
	// the derived RAW delivery guard is what stops it.
	if _, err := f.runBatch(t, "raw_lane", inverseOutsideKeep28, raw); err == nil || !strings.Contains(err.Error(), "live key protection veto") || f.deletes() != 0 {
		t.Fatalf("delivery-mode RAW batch reached the wire: %v deletes=%d", err, f.deletes())
	}

	// Live authorize, directly, under the owner fence policy.
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	protection, err := loadInverseApplyProtection(f.ctx, f.pool, "stoarama", picks, f.windows)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := openInverseLedger(filepath.Join(f.root, "raw-authorize.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.close()
	var epoch int64
	if err = f.pool.QueryRow(f.ctx, `SELECT epoch FROM r2_inverse_ke_epoch`).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	a := &inverseApplier{pool: f.pool, store: f.sdk, protection: protection, ledger: ledger, keEpoch: epoch,
		opts:    inverseApplyOptions{Lane: inverseOutsideKeep28, Picks: f.picks, DrainEvidence: f.drain, DrainSHA: f.drainSHA, ManifestSHA: strings.Repeat("d", 64), SummarySHA: strings.Repeat("c", 64)},
		summary: inverseSummary{Version: 1, Bucket: "stoarama", EndpointSHA256: f.sdk.EndpointSHA256(), CreatedAt: time.Now().UTC(), PolicySHA256: f.policy, PicksSHA256: picks.SHA256, ReadOnly: true}}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.authorize(f.ctx, tx, raw, &inverseSendGate{})
	_ = tx.Rollback(f.ctx)
	if err == nil || !strings.Contains(err.Error(), "live key protection veto") {
		t.Fatalf("live authorize did not veto the derived-held RAW source: %v", err)
	}

	var rows int
	if err = f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_retirement_reservations WHERE object_key=$1)
 +(SELECT count(*) FROM r2_inverse_intents WHERE object_key=$1)+(SELECT count(*) FROM r2_inverse_attempts WHERE object_key=$1)
 +(SELECT count(*) FROM r2_object_retirements WHERE object_key=$1)
 +(SELECT count(*) FROM recording_clips WHERE object_key=$1 AND purged_at IS NOT NULL)`, rawKey).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("derived-held RAW source left %d durable writes (%v)", rows, err)
	}
	if _, present := f.store.objects[rawKey]; !present {
		t.Fatal("RAW source left the provider")
	}
}

// TestR2NonrawExemptionClassMustMatchLiveClassifier: the roster digest is bound
// to its class. A roster carrying in-window keys submitted as outside_window is
// refused by the live classifier, and nothing is written.
func TestR2NonrawExemptionClassMustMatchLiveClassifier(t *testing.T) {
	f := newNonrawFixture(t, "exempt_class_mismatch")
	inSHA := strings.Repeat("e", 64)
	inWindow := "joined/goodplus-20260821-generation-1/objects/" + inSHA + ".mp4"
	f.pageHoldSHA(inWindow, "in-etag", "video_collated", 300, inSHA)
	w := f.windows[3]
	f.fixture(`INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(10000003,1,3,3,$1,'nightly/in.mp4',300,$2,$3,$4,ARRAY[7000001]::bigint[],'2026-06-02T00:00:00Z')`,
		inWindow, inSHA, w.Start.Add(time.Minute).Format(time.RFC3339), w.Start.Add(2*time.Minute).Format(time.RFC3339))
	inRow := deliveryExemptionRow{Bucket: "stoarama", ObjectKey: inWindow, ETag: "in-etag", SizeBytes: 300}
	err := f.seed(t, deliveryExemptOutsideWindow, inRow)
	if err == nil || !strings.Contains(err.Error(), "in_window") || !strings.Contains(err.Error(), "outside_window") {
		t.Fatalf("in-window roster accepted as outside_window: %v", err)
	}
	var exemptions, approvals int
	if qErr := f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_delivery_exemptions),(SELECT count(*) FROM r2_delivery_exemption_approvals)`).Scan(&exemptions, &approvals); qErr != nil {
		t.Fatal(qErr)
	}
	if exemptions != 0 || approvals != 0 {
		t.Fatalf("refused seed wrote %d exemptions and %d approvals", exemptions, approvals)
	}
	// The same roster under its real class is accepted.
	if err = f.seed(t, deliveryExemptInWindow, inRow); err != nil {
		t.Fatalf("in-window roster refused under its own class: %v", err)
	}
}

// TestR2NonrawExemptionSeedRequiresPositiveStreamDecision: a missing pick, an
// undecided window or a keep-everything pick is never exemption authority.
func TestR2NonrawExemptionSeedRequiresPositiveStreamDecision(t *testing.T) {
	f := newNonrawFixture(t, "exempt_no_pick")
	f.withPageKeys()
	for name, choices := range map[string]map[string]any{
		"missing":      {},
		"all":          {"3": r2RetirePick{Choice: "all"}},
		"no_window":    {"3": r2RetirePick{Choice: "none"}},
		"other_window": {"3": r2RetirePick{Choice: "none", KeepStartUTC: "2020-01-01T00:00:00Z", KeepEndUTC: "2020-01-02T00:00:00Z"}},
	} {
		data, _ := json.Marshal(map[string]any{"choices": choices})
		if err := os.WriteFile(f.picks, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err == nil {
			t.Fatalf("%s: seeded without a positive non-keep-everything decision", name)
		}
	}
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_delivery_exemptions`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("refused seeds left %d exemptions (%v)", n, err)
	}
}

// TestR2NonrawPageStreamDivergesFromOutputStream: the page recording's own
// stream is rechecked at deletion time, so a page row on a keep-everything
// stream keeps the key even when the catalog output names a decided stream.
func TestR2NonrawPageStreamDivergesFromOutputStream(t *testing.T) {
	f := newNonrawFixture(t, "exempt_page_stream")
	f.withPageKeys()
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	// A second stream is keep-everything; the page recording now names it while
	// the collated output still names the decided stream 3.
	f.fixture(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(4,'test','4','pageke','pageke','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30)`)
	f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'pageke','https://test.invalid','* * * * *','Asia/Tokyo','paused',4,now())`)
	f.fixture(`UPDATE delivery_keep_holds SET recording_id=31 WHERE bucket='stoarama' AND object_key=$1`, f.key)
	if err := os.WriteFile(f.picks, mustPickJSON(t, f.windows[3], map[string]r2RetirePick{"4": {Choice: "all"}}), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runBatch(t, "diverged", inverseNonrawCollated, f.collatedEntry()); !errors.Is(err, errInverseNonrawIneligible) || !strings.Contains(err.Error(), "page recording stream") || f.deletes() != 0 {
		t.Fatalf("diverged page recording stream reached the wire: %v deletes=%d", err, f.deletes())
	}
}

// mustPickJSON writes a picks decision for stream 3 (the fixture's decided
// stream) plus extras.
func mustPickJSON(t *testing.T, w api.Keep28Window, extras map[string]r2RetirePick) []byte {
	t.Helper()
	choices := map[string]any{"3": r2RetirePick{Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}
	for stream, pick := range extras {
		choices[stream] = pick
	}
	data, err := json.Marshal(map[string]any{"choices": choices})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// collatedMemberEntry is one collated manifest entry for the fixture key.
func collatedMemberEntry(f *nonrawFixture) inverseEntry {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	return inverseEntry{ObjectInfo: f.store.objects[f.key], Class: inverseCollated}
}

// TestR2NonrawWirePartialResponseIsHardFailure: a collated DELETE whose answer is
// cut short still counts as answered, so it is a hard failure, never a lost
// response that holds only its key. It goes through the collated job batch.
func TestR2NonrawWirePartialResponseIsHardFailure(t *testing.T) {
	for _, mode := range []string{"503-truncated", "503-headers-cut"} {
		t.Run(mode, func(t *testing.T) {
			f := newNonrawFixture(t, "nonraw_wire_partial")
			f.store.mu.Lock()
			f.store.wire = map[string]string{f.key: mode}
			f.store.mu.Unlock()
			_, err := f.runBatch(t, "partial", inverseNonrawCollated, collatedMemberEntry(f))
			if err == nil || errors.Is(err, errInverseWireAmbiguous) || f.deletes() != 1 {
				t.Fatalf("%s: %v deletes=%d", mode, err, f.deletes())
			}
			var absent int
			if qErr := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_nonraw_events WHERE object_key=$1`, f.key).Scan(&absent); qErr != nil || absent != 0 {
				t.Fatalf("%s: absence published over a hard failure (%d, %v)", mode, absent, qErr)
			}
		})
	}
}

// TestR2NonrawWireFirstWriteAfterDeadlineSendsNothing: the collated lane shares
// the audited transport's first-write deadline check. A writer paused after its
// connection is up until the DELETE's deadline sends no request byte, and the
// failure is never a lost response.
func TestR2NonrawWireFirstWriteAfterDeadlineSendsNothing(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_wire_late")
	// The page key must be exemptable first; the wire gates are what this asserts.
	var sent atomic.Int64
	deadline := time.Now().Add(700 * time.Millisecond)
	template := newInverseDeleteHTTPTransport()
	dial := template.DialContext
	template.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &stallConn{Conn: conn, stall: func() {
			if time.Now().Before(deadline) {
				time.Sleep(time.Until(deadline) + 100*time.Millisecond)
			}
		}, sent: &sent}, nil
	}
	original := f.sdk.transport
	f.sdk.transport = template
	defer func() { f.sdk.transport = original }()
	a := f.applier(t, "run1")
	defer a.ledger.close()
	ctx, cancel := context.WithDeadline(f.ctx, deadline)
	defer cancel()
	ok, err := a.deleteNonraw(ctx, ctx, &inverseSendGate{}, collatedMemberEntry(f))
	if ok || err == nil || errors.Is(err, errInverseWireAmbiguous) || sent.Load() != 0 || f.deletes() != 0 {
		t.Fatalf("late first write: ok=%v err=%v sent=%d deletes=%d", ok, err, sent.Load(), f.deletes())
	}
}

// TestR2NonrawExemptionNeverReleasesDerivedRAWKeep28 is the B2 case of the same
// rule: a RAW source with otherwise-perfect keep-28 custody (a pinned-window
// clip with a fresh exact NAS proof, exactly what the keep-28 lane deletes on)
// is still held by the derived RAW delivery guard. Exemptions never release it.
func TestR2NonrawExemptionNeverReleasesDerivedRAWKeep28(t *testing.T) {
	f := newNonrawFixture(t, "exempt_raw_keep28")
	rawKey := "managed/acct-47/nonraw/source.mp4"
	// The delivery output that contains the source is a page video, exempted while
	// the source is still outside the pinned window.
	f.withPageKeys()
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	// Then put the source inside stream 3's pinned keep window and give it fresh,
	// exact custody, so every keep-28 gate except delivery would pass.
	f.fixture(`UPDATE recording_clips SET clip_start_at=$1,clip_end_at=$2 WHERE id=$3`,
		f.windows[3].Start.Add(time.Hour), f.windows[3].Start.Add(2*time.Hour), f.clipID)
	f.fixture(`INSERT INTO nas_inventory_files(connection_id,clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,client_updated_at,server_received_at,file_ctime_ns,file_inode,file_device,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256)
 VALUES(13,$1,30,'source/source.mp4',500,repeat('c',64),'present',clock_timestamp()-interval '1 hour',clock_timestamp(),clock_timestamp()-interval '2 hours',1,2,3,'source/source.mp4.stoarama.json',50,repeat('d',64))`, f.clipID)
	var derived bool
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_inverse_delivery_held($1,'stoarama',$2,'source/source.mp4',repeat('c',64),500)`, f.clipID, rawKey).Scan(&derived); err != nil || !derived {
		t.Fatalf("RAW source is not derived-held: %v %v", derived, err)
	}
	f.store.mu.Lock()
	f.store.objects[rawKey] = r2.ObjectInfo{Key: rawKey, ETag: "src-etag", SizeBytes: 500, LastModified: inverseRetirementCutoff().Add(-time.Second)}
	f.store.mu.Unlock()
	raw := inverseEntry{ObjectInfo: f.store.objects[rawKey], Class: inverseKeep28}
	// Keep-28 custody is otherwise valid; the delivery guard is the only veto.
	if _, err := f.runBatch(t, "keep28", inverseNASKeep28, raw); err == nil || !strings.Contains(err.Error(), "live key protection veto") || f.deletes() != 0 {
		t.Fatalf("keep-28 custody bypassed the derived RAW delivery guard: %v deletes=%d", err, f.deletes())
	}
}

// TestR2NonrawOffPageDependencyFollowsOwnerScopeButPendingCopyHolds: a collated
// output that is not a page key follows the 2026-11-18 owner scope, but anything
// whose NAS copy is still pending or blocked stays held.
func TestR2NonrawOffPageDependencyFollowsOwnerScopeButPendingCopyHolds(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_offpage_copy")
	f.fixture(`INSERT INTO delivery_keep_output_dependencies(output_id,recording_id) VALUES($1,30)`, f.outputID)
	copyItem := func(state, blocked, reason string) {
		f.fixture(`DELETE FROM nas_r2_copy_items`)
		f.fixture(`INSERT INTO nas_r2_copy_items(connection_id,kind,source_id,manifest_sha256,policy_sha256,item,state,blocked_reason)
 VALUES(13,'collated',$1,repeat('a',64),$2,$3,$4,$5)`, f.outputID, f.policy,
			fmt.Sprintf(`{"bucket":"stoarama","object_key":%q,"size_bytes":900}`, f.key), state, blocked)
		_ = reason
	}
	// Off-page and its NAS copy still waiting to be transferred: held.
	copyItem("pending", "", "waiting to be transferred")
	if _, err := f.runBatch(t, "pending_copy", inverseNonrawCollated, f.collatedEntry()); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("pending NAS copy did not hold the output: %v deletes=%d", err, f.deletes())
	}
	copyItem("blocked", "checksum_mismatch", "blocked")
	if _, err := f.runBatch(t, "blocked_copy", inverseNonrawCollated, f.collatedEntry()); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("blocked NAS copy did not hold the output: %v deletes=%d", err, f.deletes())
	}
	// Verified copy: the off-page dependency then follows the owner scope.
	copyItem("verified", "", "verified")
	if _, err := f.runBatch(t, "verified_copy", inverseNonrawCollated, f.collatedEntry()); err != nil || f.deletes() != 1 {
		t.Fatalf("verified off-page dependency did not follow the owner scope: %v deletes=%d", err, f.deletes())
	}
}

// TestR2NonrawPartitionComparesExemptIdentity: the partition reports an exempt
// key as a candidate only when the listing's ETag AND size equal the exempt
// page identity. Either kind of drift keeps it delivery-held.
func TestR2NonrawPartitionComparesExemptIdentity(t *testing.T) {
	f := newNonrawFixture(t, "exempt_partition_drift")
	f.withPageKeys()
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	p, err := loadInverseProtectionMode(f.ctx, f.pool, "stoarama", picks, f.windows, true)
	if err != nil {
		t.Fatal(err)
	}
	// Control: the exact exempt identity is the collated candidate.
	if class, reason := p.classify(r2.ObjectInfo{Key: f.key, ETag: "collated-etag", SizeBytes: 900}); class != inverseCollated {
		t.Fatalf("control: exact exempt identity classified %s (%s)", class, reason)
	}
	if class, reason := p.classify(r2.ObjectInfo{Key: f.key, ETag: "rewritten", SizeBytes: 900}); class != inverseDelivery || reason != "delivery_exemption_identity_drift" {
		t.Fatalf("ETag drift classified %s (%s)", class, reason)
	}
	if class, reason := p.classify(r2.ObjectInfo{Key: f.key, ETag: "collated-etag", SizeBytes: 901}); class != inverseDelivery || reason != "delivery_exemption_identity_drift" {
		t.Fatalf("size drift classified %s (%s)", class, reason)
	}
}

// TestR2NonrawPageRetiredMarkerRequiresDisabledRegistry: the CLI refuses to write
// the marker unless the named serving generation is already disabled or retired.
func TestR2NonrawPageRetiredMarkerRequiresDisabledRegistry(t *testing.T) {
	f := newNonrawFixture(t, "exempt_page_cli")
	// A generation still serving: the marker write must leave no marker behind.
	if _, err := f.pool.Exec(f.ctx, `SELECT 1`); err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f.ctx, `INSERT INTO r2_delivery_page_registry_events(generation,state,decision) VALUES($1,'serving','test: still serving')`, pageGeneration); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f.ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'test: premature')`, pageGeneration); err != nil {
		t.Fatal(err)
	}
	var retired bool
	if err = tx.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || retired {
		t.Fatalf("serving generation retired the page: %v %v", retired, err)
	}
	tx.Rollback(f.ctx)
	var markers int
	if err = f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_delivery_page_retirement`).Scan(&markers); err != nil || markers != 0 {
		t.Fatalf("a still-serving generation left %d markers", markers)
	}
	// Disabled first, then the marker; the function agrees and the CLI's check passes.
	f.retirePage()
	if err = f.pool.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || !retired {
		t.Fatalf("disabled generation did not retire the page: %v %v", retired, err)
	}
}
