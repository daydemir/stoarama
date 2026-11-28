package main

import (
	"bytes"
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
	"github.com/jackc/pgx/v5/pgconn"
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

// approvalDigest is the roster digest each fixture class is approved under. A
// digest binds to exactly one class, so each class names its own. It is derived
// from the exact roster bytes those rows encode, because an approval now validates
// the very bytes it records a digest for.
func approvalDigest(class deliveryExemptionClass, rows []deliveryExemptionRow) string {
	var buf bytes.Buffer
	for _, r := range rows {
		b, _ := json.Marshal(r)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	sum := sha256.Sum256(append([]byte("fixture:"+string(class)+":"), buf.Bytes()...))
	return hex.EncodeToString(sum[:])
}

// ensureApproval records the owner approval for a class's fixture roster once,
// through recordDeliveryExemptionApproval so the approval and validation share one
// transaction exactly as the CLI does.
func (f *nonrawFixture) ensureApproval(t *testing.T, class deliveryExemptionClass, rows []deliveryExemptionRow) string {
	t.Helper()
	digest := approvalDigest(class, rows)
	var exists bool
	if err := f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM r2_delivery_exemption_approvals WHERE roster_sha256=$1)`, digest).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		return digest
	}
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, class, digest, deliveryExemptionDecisions[class], rows, picks); err != nil {
		t.Fatal(err)
	}
	return digest
}

// seed models the real order: an approval was recorded when the roster matched
// (here written directly so a test can then drift the catalog), and seeding now
// consumes it. The seed's own classifier is what every seed-level refusal test
// exercises. Approval-time validation is proven separately by
// TestR2NonrawExemptionApprovalValidatesExactRoster.
func (f *nonrawFixture) seed(t *testing.T, class deliveryExemptionClass, rows ...deliveryExemptionRow) error {
	t.Helper()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seedDeliveryExemptions(f.ctx, f.pool, class, rows, f.ensureApprovalDirect(t, class, rows), picks)
	return err
}

// ensureApprovalDirect records an approval row directly, bypassing the approval's
// own validation. It models an approval recorded when the catalog matched, whose
// roster no longer matches now: the seed-level checks must still refuse it, so a
// catalog change after approval cannot become deletion authority.
func (f *nonrawFixture) ensureApprovalDirect(t *testing.T, class deliveryExemptionClass, rows []deliveryExemptionRow) string {
	t.Helper()
	digest := approvalDigest(class, rows)
	var exists bool
	if err := f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM r2_delivery_exemption_approvals WHERE roster_sha256=$1)`, digest).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		f.exec(`INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision) VALUES($1,$2,$3)`,
			string(class), digest, deliveryExemptionDecisions[class])
	}
	return digest
}

// seedUnapproved seeds with a digest that was never recorded as an approval.
func (f *nonrawFixture) seedUnapproved(t *testing.T, class deliveryExemptionClass, rows ...deliveryExemptionRow) error {
	t.Helper()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("unapproved:" + string(class)))
	_, err = seedDeliveryExemptions(f.ctx, f.pool, class, rows, hex.EncodeToString(sum[:]), picks)
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

// requirePageRetiredIsTheConstantContract pins the one page-retirement answer
// this release has. There is no quiesce row, retirement row, cutover authority or
// reader/ticket ledger any more, so nothing can arm, suppress or retire the page
// and the answer is always false. This is the contract kept for downstream
// callers; it is not authority, and no code path in this branch can change it.
func (f *nonrawFixture) requirePageRetiredIsTheConstantContract(t *testing.T) {
	t.Helper()
	var retired bool
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil {
		t.Fatalf("the page-retired contract is unreadable: %v", err)
	}
	if retired {
		t.Fatal("r2_delivery_page_retired() is true, but nothing in this release may retire the page")
	}
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

// TestR2NonrawExemptionClassGates: only outside_window opens; confident_repair and
// in_window are HARD-CLOSED in code, so no seeding, no committed retirement and no
// caller-supplied certificate can open them.
func TestR2NonrawExemptionClassGates(t *testing.T) {
	f := newNonrawFixture(t, "exempt_gates")
	f.withPageKeys()
	// outside_window seeds and opens on its own exact approval.
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	if !f.exempt(t, f.key) {
		t.Fatal("outside_window is not open once seeded")
	}
	// The hard-closed classes refuse at validate, approve and seed even with a
	// perfect page identity and a committed retirement.
	for _, class := range []deliveryExemptionClass{deliveryExemptConfidentRepair, deliveryExemptInWindow} {
		rows := []deliveryExemptionRow{repairRow()}
		picks, err := loadR2RetirePicks(f.picks)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validateDeliveryExemptions(f.ctx, f.pool, class, rows, picks); err == nil || !strings.Contains(err.Error(), "hard-closed") {
			t.Fatalf("%s validated despite being hard-closed: %v", class, err)
		}
		if err := f.seed(t, class, rows...); err == nil || !strings.Contains(err.Error(), "hard-closed") {
			t.Fatalf("%s seeded despite being hard-closed: %v", class, err)
		}
	}
	// Seed the closed classes directly at the SQL layer to prove even an exact row
	// cannot open them. Nothing in this release can retire the page, so the
	// closed-class row is refused while the page is up and stays refused.
	f.fixture(`INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision) VALUES('confident_repair',repeat('7',64),'x')`)
	f.fixture(`INSERT INTO r2_delivery_exemptions(bucket,object_key,etag,size_bytes,class,approval_id,decision,roster_sha256)
 SELECT 'stoarama',$1,'repair-etag',700,'confident_repair',id,'x',repeat('7',64) FROM r2_delivery_exemption_approvals WHERE roster_sha256=repeat('7',64)`, exemptRepairKey)
	if f.exempt(t, exemptRepairKey) {
		t.Fatal("confident_repair opened while the page is up")
	}
	f.requirePageRetiredIsTheConstantContract(t)
	if f.exempt(t, exemptRepairKey) {
		t.Fatal("confident_repair opened despite being hard-closed")
	}
}

// TestR2DeliveryPageKeysReachLedger: Deniz, 2026-11-24, "just drop the
// recordings its fine, we'll fix the links whenever". A delivery page key,
// exempt or not, is no longer refused at the reservation or the receipt.
func TestR2DeliveryPageKeysReachLedger(t *testing.T) {
	f := newNonrawFixture(t, "page_ledger")
	f.withPageKeys()
	// receipt writes ONLY the final receipt; ledger writes reservation then receipt.
	write := func(withReservation bool, kind, key string, id, size int64) error {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		if withReservation {
			if _, err = tx.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256) VALUES($1,$2,'stoarama',$3,$4,$5,$6)`,
				kind, id, key, size, nonrawOutputSHA, strings.Repeat("d", 64)); err != nil {
				return err
			}
		}
		_, err = tx.Exec(f.ctx, `INSERT INTO r2_object_retirements(kind,source_id,stream_id,bucket,object_key,size_bytes,sha256,reason,choice,manifest_sha256,policy_sha256) VALUES($1,$2,3,'stoarama',$3,$4,$5,'t','none',$6,$7)`,
			kind, id, key, size, nonrawOutputSHA, strings.Repeat("d", 64), f.policy)
		return err
	}
	for _, withReservation := range []bool{false, true} {
		if err := write(withReservation, "collated", f.key, f.outputID, 900); err != nil {
			t.Fatalf("collated page key refused (reservation=%t): %v", withReservation, err)
		}
		if err := write(withReservation, "delivery_repair", exemptRepairKey, 1, 700); err != nil {
			t.Fatalf("repair page key refused (reservation=%t): %v", withReservation, err)
		}
	}
	// A repair still retires only under its own key family.
	if err := write(true, "delivery_repair", f.key, 2, 900); err == nil || !strings.Contains(err.Error(), "kind_prefix") {
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
	// Seeding never creates its own approval: without the separate approve step
	// the whole class refuses and writes nothing.
	unapproved := f.seedUnapproved(t, deliveryExemptOutsideWindow, collatedRow(f))
	if unapproved == nil || !strings.Contains(unapproved.Error(), "no recorded owner approval") {
		t.Fatalf("seeding without a pre-recorded approval was allowed: %v", unapproved)
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

// TestR2NonrawExemptionLaneRetiresOutsideWindow proves the reviewed collated lane
// retires an outside_window page video end to end, while the hard-closed
// confident_repair class stays on the provider and off the wire.
func TestR2NonrawExemptionLaneRetiresOutsideWindow(t *testing.T) {
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
	if class, reason := classify(f.key); class != inverseCollated || reason != "owner_delivery_page_outside_window" {
		t.Fatalf("exempt page video classified %s (%s)", class, reason)
	}
	result, err := f.runBatch(t, "outside_window", inverseNonrawCollated, collated)
	if err != nil || result.Deleted != 1 || f.deletes() != 1 {
		t.Fatalf("outside_window batch: %+v %v deletes=%d", result, err, f.deletes())
	}

	// confident_repair is hard-closed, so the repair stays held and never
	// reaches the wire.
	f.requirePageRetiredIsTheConstantContract(t)
	if _, err := f.runBatch(t, "repair_held", inverseNonrawCollated, repair); !errors.Is(err, errInverseNonrawIneligible) {
		t.Fatalf("a hard-closed repair reached the wire: %v", err)
	}
	if _, present := f.store.objects[exemptRepairKey]; !present {
		t.Fatal("the hard-closed confident repair was removed from the provider")
	}
	// The outside_window receipt is exact.
	var kind, reason string
	var source int64
	if err = f.pool.QueryRow(f.ctx, `SELECT r.kind,r.source_id,r.reason FROM r2_object_retirements r
 JOIN r2_nonraw_intents i USING(bucket,object_key) JOIN r2_nonraw_attempts USING(bucket,object_key)
 JOIN r2_nonraw_events e USING(bucket,object_key) WHERE r.object_key=$1 AND i.kind=r.kind AND e.event='absent'`, f.key).Scan(&kind, &source, &reason); err != nil {
		t.Fatal(err)
	}
	if kind != "collated" || source != f.outputID || reason != "owner_delivery_page_outside_window" {
		t.Fatalf("outside_window receipt %s/%d/%s", kind, source, reason)
	}
}

// TestR2NonrawExemptionRepairIdentityAndKeepEverything: a confident repair whose
// listed ETag differs from its exemption, or whose stream became keep-everything,
// never reaches the wire.
func TestR2NonrawExemptionRepairIdentityAndKeepEverything(t *testing.T) {
	f := newNonrawFixture(t, "exempt_repair_hold")
	f.withPageKeys()
	// The class is hard-closed, so it cannot be seeded at all; a direct SQL row is
	// the strongest case, and the repair still must not reach the wire.
	f.fixture(`INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision) VALUES('confident_repair',repeat('7',64),'x')`)
	f.fixture(`INSERT INTO r2_delivery_exemptions(bucket,object_key,etag,size_bytes,class,approval_id,decision,roster_sha256)
 SELECT 'stoarama',$1,'repair-etag',700,'confident_repair',id,'x',repeat('7',64) FROM r2_delivery_exemption_approvals WHERE roster_sha256=repeat('7',64)`, exemptRepairKey)
	moved := inverseEntry{ObjectInfo: f.store.objects[exemptRepairKey], Class: inverseCollated}
	moved.ETag = "rewritten"
	if _, err := f.runBatch(t, "etag", inverseNonrawCollated, moved); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("a hard-closed repair with another ETag reached the wire: %v", err)
	}
	data, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": r2RetirePick{Choice: "all"}}})
	if err := os.WriteFile(f.picks, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runBatch(t, "keep_everything", inverseNonrawCollated, inverseEntry{ObjectInfo: f.store.objects[exemptRepairKey], Class: inverseCollated}); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("a hard-closed repair reached the wire: %v", err)
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
	// The in_window page row's sha256 is the catalog content, as the admission
	// trigger requires of a collated page exemption.
	f.exec(`UPDATE delivery_keep_holds SET sha256=$2 WHERE bucket='stoarama' AND object_key=$1`, inWindow, inSHA)
	// outside_window is seeded normally; the two hard-closed classes are written
	// directly at SQL to prove even an exact row cannot open them.
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	f.fixture(`INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision) VALUES('confident_repair',repeat('7',64),'x'),('in_window',repeat('8',64),'x')`)
	f.fixture(`INSERT INTO r2_delivery_exemptions(bucket,object_key,etag,size_bytes,class,approval_id,decision,roster_sha256)
 SELECT 'stoarama',$1,'repair-etag',700,'confident_repair',id,'x',repeat('7',64) FROM r2_delivery_exemption_approvals WHERE roster_sha256=repeat('7',64)`, exemptRepairKey)
	f.fixture(`INSERT INTO r2_delivery_exemptions(bucket,object_key,etag,size_bytes,class,approval_id,decision,roster_sha256)
 SELECT 'stoarama',$1,'in-etag',300,'in_window',id,'x',repeat('8',64) FROM r2_delivery_exemption_approvals WHERE roster_sha256=repeat('8',64)`, inWindow)
	f.requirePageRetiredIsTheConstantContract(t)
	// Only outside_window opens; the hard-closed classes stay closed.
	if !f.exempt(t, f.key) {
		t.Fatal("outside_window gate not open")
	}
	for _, key := range []string{exemptRepairKey, inWindow} {
		if f.exempt(t, key) {
			t.Fatalf("%s: a hard-closed gate opened", key)
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
	_, err = a.authorize(f.ctx, f.ctx, tx, raw, &inverseSendGate{})
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
	// The approval exists (the fixture recorded it before seeding); the refused
	// seed wrote no exemption at all.
	if exemptions != 0 || approvals != 1 {
		t.Fatalf("refused seed wrote %d exemptions and %d approvals", exemptions, approvals)
	}
	// The same roster under its real class is refused too: in_window is
	// hard-closed, so neither class may admit it.
	if err = f.seed(t, deliveryExemptInWindow, inRow); err == nil || !strings.Contains(err.Error(), "hard-closed") {
		t.Fatalf("in_window admitted the roster despite being hard-closed: %v", err)
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
	// The collated lane's copy veto is r2_nonraw_key_held. Assert it directly, and
	// separately assert the classifier answers "held / live_hold", so the pending
	// copy is provably the veto in play rather than a page keep, a keep28 overlap
	// or a missing exemption. Either assertion alone can be masked by the other,
	// which is exactly the trap this test exists to close.
	guards := func(name string, want bool) {
		t.Helper()
		var sqlHeld bool
		if err := f.pool.QueryRow(f.ctx, `SELECT r2_nonraw_key_held('stoarama',$1,'collated',$2)`, f.key, f.outputID).Scan(&sqlHeld); err != nil {
			t.Fatal(err)
		}
		if sqlHeld != want {
			t.Fatalf("%s: r2_nonraw_key_held=%v, want %v", name, sqlHeld, want)
		}
		picks, err := loadR2RetirePicks(f.picks)
		if err != nil {
			t.Fatal(err)
		}
		p, err := loadInverseProtectionMode(f.ctx, f.pool, "stoarama", picks, f.windows, true)
		if err != nil {
			t.Fatal(err)
		}
		class, reason := p.classify(f.store.objects[f.key])
		if got := class == inverseHeld && reason == "live_hold"; got != want {
			t.Fatalf("%s: classifier %s (%s), want held/live_hold=%v", name, class, reason, want)
		}
	}
	copyItem("pending", "", "waiting to be transferred")
	guards("pending", true)
	if _, err := f.runBatch(t, "pending_copy", inverseNonrawCollated, f.collatedEntry()); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("pending NAS copy did not hold the output: %v deletes=%d", err, f.deletes())
	}
	copyItem("blocked", "checksum_mismatch", "blocked")
	guards("blocked", true)
	if _, err := f.runBatch(t, "blocked_copy", inverseNonrawCollated, f.collatedEntry()); !errors.Is(err, errInverseNonrawIneligible) || f.deletes() != 0 {
		t.Fatalf("blocked NAS copy did not hold the output: %v deletes=%d", err, f.deletes())
	}
	// With the copy verified the guard is released, so this cannot pass by always
	// holding.
	copyItem("verified", "", "verified")
	guards("verified", false)
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

// TestR2NonrawExemptionApprovalBindsOneClassPerDigest: a roster digest approved
// for one class is refused under any other, in SQL as well as in the seed.
func TestR2NonrawExemptionApprovalBindsOneClassPerDigest(t *testing.T) {
	f := newNonrawFixture(t, "exempt_approval_class")
	f.withPageKeys()
	outsideRows := []deliveryExemptionRow{collatedRow(f)}
	digest := f.ensureApproval(t, deliveryExemptOutsideWindow, outsideRows)
	picksForApprove, pickErr := loadR2RetirePicks(f.picks)
	if pickErr != nil {
		t.Fatal(pickErr)
	}
	// The same digest, replayed under another class or for the same class, is
	// refused by the approval itself (which validates the bytes it is given) AND
	// by the SQL trigger's one-digest-one-class rule.
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptConfidentRepair, digest, deliveryExemptionDecisions[deliveryExemptConfidentRepair], []deliveryExemptionRow{repairRow()}, picksForApprove); err == nil {
		t.Fatal("one roster digest was approved for a second class")
	}
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, digest, deliveryExemptionDecisions[deliveryExemptOutsideWindow], outsideRows, picksForApprove); err == nil {
		t.Fatal("one roster digest was approved twice")
	}
	// The SQL trigger is what refuses it, not merely the helper: write it directly.
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, `INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision) VALUES('confident_repair',$1,'other')`, digest)
	if err == nil || !strings.Contains(err.Error(), "already approved") {
		t.Fatalf("the approval trigger did not bind the digest to one class: %v", err)
	}
	// Seeding under the other class with the same digest refuses too.
	picks, pickErr := loadR2RetirePicks(f.picks)
	if pickErr != nil {
		t.Fatal(pickErr)
	}
	if _, err = seedDeliveryExemptions(f.ctx, f.pool, deliveryExemptConfidentRepair, []deliveryExemptionRow{repairRow()}, digest, picks); err == nil {
		t.Fatal("a digest approved for outside_window seeded confident_repair")
	}
	if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err != nil || !f.exempt(t, f.key) {
		t.Fatalf("outside_window seeding broke: %v", err)
	}
	if f.exempt(t, exemptRepairKey) {
		t.Fatal("the refused class opened anyway")
	}
}

// TestR2NonrawSeedKeepsSourceHolds: the seed suppresses ONLY the candidate's own
// page veto. A joined dry-run scope over a source, or a duplicate source closure,
// still refuses the row exactly as the wire would.
func TestR2NonrawSeedKeepsSourceHolds(t *testing.T) {
	f := newNonrawFixture(t, "exempt_seed_sources")
	f.withPageKeys()
	f.ensureApproval(t, deliveryExemptOutsideWindow, []deliveryExemptionRow{collatedRow(f)})
	// A joined dry-run scope covering the output's only source.
	var recordingJob int64
	if err := f.pool.QueryRow(f.ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key,completed_at)
 VALUES(30,'2026-06-01','2026-06-01',60,'done','exempt-seed-scope','2026-06-02') RETURNING id`).Scan(&recordingJob); err != nil {
		t.Fatal(err)
	}
	f.fixture(`UPDATE recording_clips SET recording_job_id=$1 WHERE id=$2`, recordingJob, f.clipID)
	f.fixture(`INSERT INTO recording_joined_dry_run_scopes(dry_run_id,recording_id,priority_ordinal,local_date,date_ordinal,recording_job_id,high_water_clip_id)
 VALUES('00000000-0000-0000-0000-000000000001',30,1,'2026-06-01',1,$1,$2)`, recordingJob, f.clipID)
	if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err == nil {
		t.Fatal("a source under a joined dry-run scope was seeded")
	}
	if f.exempt(t, f.key) {
		t.Fatal("a source-held key opened")
	}
	// Duplicate source IDs never resolve into seeding authority either.
	f.fixture(`UPDATE recording_collation_outputs SET source_clip_ids=ARRAY[7000001,7000001]::bigint[] WHERE id=$1`, f.outputID)
	if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err == nil || !strings.Contains(err.Error(), "collated_sources_empty_duplicate_or_unresolved") {
		t.Fatalf("a duplicate source closure was seeded: %v", err)
	}
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_delivery_exemptions`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("refused seeds left %d exemptions (%v)", n, err)
	}
}

// TestR2NonrawSeedDecisionBindsThePageStream: the positive, window-bound decision
// must cover the stream the page recording actually names, including when that
// stream differs from the output's own stream.
func TestR2NonrawSeedDecisionBindsThePageStream(t *testing.T) {
	f := newNonrawFixture(t, "exempt_seed_page_stream")
	f.withPageKeys()
	f.ensureApproval(t, deliveryExemptOutsideWindow, []deliveryExemptionRow{collatedRow(f)})
	// The page recording moves to a second, decided stream; the catalog output
	// keeps stream 3.
	const pageStream = 158
	f.fixture(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(158,'test','158','pageside','pageside','https://test.invalid','off','Asia/Seoul','hls','video_manifest','video_live','continuous_video',30)`)
	f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'pageside','https://test.invalid','* * * * *','Asia/Seoul','paused',158,now())`)
	f.fixture(`UPDATE delivery_keep_holds SET recording_id=31 WHERE bucket='stoarama' AND object_key=$1`, f.key)
	pageWindow, ok := f.windows[pageStream]
	if !ok || pageWindow.Start.Equal(f.windows[3].Start) {
		t.Fatalf("stream %d has no distinct pinned window", pageStream)
	}
	decided := func(window api.Keep28Window) r2RetirePick {
		return r2RetirePick{Choice: "none", KeepStartUTC: window.Start.Format(time.RFC3339), KeepEndUTC: window.End.Format(time.RFC3339)}
	}
	// Stream 4's pick is made against another window: never authority.
	data, _ := json.Marshal(map[string]any{"choices": map[string]any{
		"3":                    decided(f.windows[3]),
		fmt.Sprint(pageStream): r2RetirePick{Choice: "none", KeepStartUTC: "2020-01-01T00:00:00Z", KeepEndUTC: "2020-01-02T00:00:00Z"},
	}})
	if err := os.WriteFile(f.picks, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err == nil {
		t.Fatal("seeded without a window-bound decision for the page stream")
	}
	// The page stream's own exact window is the decision that unlocks it.
	data, _ = json.Marshal(map[string]any{"choices": map[string]any{"3": decided(f.windows[3]), fmt.Sprint(pageStream): decided(pageWindow)}})
	if err := os.WriteFile(f.picks, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err != nil {
		t.Fatalf("the page stream's own window-bound decision was refused: %v", err)
	}
	if !f.exempt(t, f.key) {
		t.Fatal("outside_window did not open")
	}
}

// TestR2NonrawExemptionValidateIsReadOnlyAndClassifierExact: the pre-approval
// validation runs the executor's own classifier, refuses a mixed-source window
// the roster SQL alone would hand over, and writes nothing at all — no
// approval, no exemption, no state change.
func TestR2NonrawExemptionValidateIsReadOnlyAndClassifierExact(t *testing.T) {
	f := newNonrawFixture(t, "exempt_validate")
	f.withPageKeys()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	// A clean outside-window roster row validates, and the read-only step leaves
	// no approval and no exemption behind: it grants no authority.
	if after := f.counts(t); after != [2]int64{0, 0} {
		t.Fatalf("fixture started dirty: %v", after)
	}
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, []deliveryExemptionRow{collatedRow(f)}, picks); err != nil || n != 1 {
		t.Fatalf("clean roster did not validate: %d %v", n, err)
	}
	if after := f.counts(t); after != [2]int64{0, 0} {
		t.Fatalf("validation wrote state: %v", after)
	}

	// Now move one RAW source into the pinned window. The workload is a MIX: the
	// output straddles the window boundary, so the classifier marks it in_window.
	// It must not validate as outside_window, and it must not validate as
	// confident_repair either.
	w := f.windows[3]
	f.fixture(`UPDATE recording_clips SET clip_start_at=$1,clip_end_at=$2 WHERE id=$3`,
		w.Start.Add(-30*time.Minute).Format(time.RFC3339), w.Start.Add(30*time.Minute).Format(time.RFC3339), f.clipID)
	// The output's own interval still sits outside the window, so only the source
	// closure decides the class: this is exactly the mixed case the roster SQL
	// cannot see.
	for _, class := range []deliveryExemptionClass{deliveryExemptOutsideWindow, deliveryExemptConfidentRepair} {
		if _, err := validateDeliveryExemptions(f.ctx, f.pool, class, []deliveryExemptionRow{collatedRow(f)}, picks); err == nil {
			t.Fatalf("%s validated a mixed-source in-window output", class)
		}
	}
	// The in_window class is the classifier's answer for it, but it is hard-closed,
	// so validation refuses it as well: the class cannot be proven here.
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptInWindow, []deliveryExemptionRow{collatedRow(f)}, picks); err == nil || n != 0 || !strings.Contains(err.Error(), "hard-closed") {
		t.Fatalf("in_window was not hard-closed at validation: %d %v", n, err)
	}
	// And the seed step refuses the same wrong class, so validation and seeding
	// can never disagree. The only row a seed may leave is its owner approval,
	// which its own separate approve step writes; no exemption may exist.
	if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err == nil {
		t.Fatal("the seed admitted the mixed-source output as outside_window")
	}
	if after := f.counts(t); after[1] != 0 {
		t.Fatalf("a refused seed wrote an exemption: %v", after)
	}
}

// counts is the durable state validation and refused seeds must never touch.
// counts is the exemption state a step may change: approvals and exemptions.
// The page state is deliberately not part of it, because this release has no
// mechanism that can change the page.
func (f *nonrawFixture) counts(t *testing.T) [2]int64 {
	t.Helper()
	var approvals, exemptions int64
	if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_delivery_exemption_approvals),
 (SELECT count(*) FROM r2_delivery_exemptions)`).Scan(&approvals, &exemptions); err != nil {
		t.Fatal(err)
	}
	return [2]int64{approvals, exemptions}
}

// TestR2NonrawExemptionRepairKeepsKeFolder: a confident repair whose stored path
// contains a keep-everything folder component is refused by the seed's classifier
// for the same reason execution refuses it, so a roster can never be approved
// with a row the wire would hold.
func TestR2NonrawExemptionRepairKeepsKeFolder(t *testing.T) {
	f := newNonrawFixture(t, "exempt_repair_ke")
	f.withPageKeys()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	// The repair class is hard-closed, so even a clean repair does not validate.
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptConfidentRepair, []deliveryExemptionRow{repairRow()}, picks); err == nil || n != 0 || !strings.Contains(err.Error(), "hard-closed") {
		t.Fatalf("confident_repair was not hard-closed at validation: %d %v", n, err)
	}
	// Put a KE folder name in the repair's stored relative path. The seed's own
	// classifier must now hold it, so validate and seed both refuse.
	keFolder := "KE_Repair_Folder"
	f.fixture(`UPDATE delivery_keep_holds SET relative_path=$1 WHERE bucket='stoarama' AND object_key=$2`,
		keFolder+"/June/Monday/30_hour_01_part_01.mp4", exemptRepairKey)
	// Make the folder a KE folder by naming it on a keep-everything stream's
	// catalog path; the fixture's stream 3 is decided "none", so add a KE stream.
	f.fixture(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(4,'test','4','ke','ke','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30)`)
	f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'ke','https://test.invalid','* * * * *','Asia/Tokyo','paused',4,now())`)
	f.fixture(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 VALUES(7100001,31,1,'https://test.invalid','stoarama','managed/acct-47/ke/'||$1||'/clip.mp4',10,'ke-etag',repeat('d',64),'2026-06-01','2026-06-01 00:00:00','2026-06-01 00:01:00',$1||'/clip.mp4')`, keFolder)
	kePicks := filepath.Join(t.TempDir(), "ke-picks.json")
	data, _ := json.Marshal(map[string]any{"choices": map[string]any{
		"3": r2RetirePick{Choice: "none", KeepStartUTC: f.windows[3].Start.Format(time.RFC3339), KeepEndUTC: f.windows[3].End.Format(time.RFC3339)},
		"4": r2RetirePick{Choice: "all"},
	}})
	if err := os.WriteFile(kePicks, data, 0600); err != nil {
		t.Fatal(err)
	}
	kePicksLoaded, err := loadR2RetirePicks(kePicks)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptConfidentRepair, []deliveryExemptionRow{repairRow()}, kePicksLoaded); err == nil || n != 0 {
		t.Fatalf("a repair inside a KE folder validated: %d %v", n, err)
	}
	// The seed refuses it too, and the classifier's reason is the KE path rule the
	// collated lane applies, not some other hold.
	p, err := loadInverseProtectionMode(f.ctx, f.pool, "stoarama", kePicksLoaded, f.windows, true)
	if err != nil {
		t.Fatal(err)
	}
	x, err := loadSeedAssessment(f.ctx, f.pool, "stoarama", exemptRepairKey, p)
	if err != nil {
		t.Fatal(err)
	}
	if got := assessRepair(p, x); got.Reason != "keep_everything_path" {
		t.Fatalf("repair with a KE folder in its stored path assessed %q", got.Reason)
	}
}

// TestR2NonrawExemptionApprovalValidatesExactRoster: approval is not a
// procedural trust. It validates the exact roster bytes it is given, in its own
// transaction, so a digest for a roster the classifier refuses is never recorded.
func TestR2NonrawExemptionApprovalValidatesExactRoster(t *testing.T) {
	f := newNonrawFixture(t, "exempt_approve_exact")
	f.withPageKeys()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	good := []deliveryExemptionRow{collatedRow(f)}
	digest := approvalDigest(deliveryExemptOutsideWindow, good)
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, digest, deliveryExemptionDecisions[deliveryExemptOutsideWindow], good, picks); err != nil {
		t.Fatalf("a clean roster was not approved: %v", err)
	}
	var n int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_delivery_exemption_approvals`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("approval not recorded: %d %v", n, err)
	}

	// A roster whose key does not name its exact live page identity is refused,
	// and records nothing.
	bad := []deliveryExemptionRow{{Bucket: "stoarama", ObjectKey: f.key, ETag: "wrong", SizeBytes: 900}}
	badDigest := approvalDigest(deliveryExemptOutsideWindow, bad)
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, badDigest, deliveryExemptionDecisions[deliveryExemptOutsideWindow], bad, picks); err == nil {
		t.Fatal("a roster that does not match its live identity was approved")
	}
	// in_window is hard-closed, so an outside_window roster may never be approved
	// under it at all.
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptInWindow, digest, deliveryExemptionDecisions[deliveryExemptInWindow], good, picks); err == nil || !strings.Contains(err.Error(), "hard-closed") {
		t.Fatalf("in_window approved a roster despite being hard-closed: %v", err)
	}
	// A digest with no bytes is not evidence.
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, digest, "x", nil, picks); err == nil {
		t.Fatal("an approval with no roster bytes was recorded")
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM r2_delivery_exemption_approvals`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("a refused approval wrote state: %d %v", n, err)
	}
}

// TestR2NonrawExemptionReservedOutputRefused: a collated output with its OWN
// pending NAS cleanup reservation is refused at validate and at approve, not only
// at the wire, and the page hold is still separable from that reservation.
func TestR2NonrawExemptionReservedOutputRefused(t *testing.T) {
	f := newNonrawFixture(t, "exempt_reserved_output")
	f.withPageKeys()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	// A pending NAS cleanup plan that reserves this exact collated output.
	f.fixture(`INSERT INTO nas_cleanup_plans(id,account_id,connection_id,mode,approved_until,label,report_sha256,manifest,plan_sha256,approval_reason)
 VALUES(91,47,13,'enabled',clock_timestamp()+interval '1 hour','reserved-output',repeat('c',64),
  jsonb_build_array(jsonb_build_object('kind','collated','source_id',$1::bigint,'relative_path','nightly/video.mp4')),repeat('b',64),'fixture')`, f.outputID)
	rows := []deliveryExemptionRow{collatedRow(f)}
	if n, vErr := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, picks); vErr == nil || n != 0 {
		t.Fatalf("a reserved collated output validated: %d %v", n, vErr)
	}
	digest := approvalDigest(deliveryExemptOutsideWindow, rows)
	if _, aErr := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, digest, deliveryExemptionDecisions[deliveryExemptOutsideWindow], rows, picks); aErr == nil {
		t.Fatal("a reserved collated output was approved")
	}
	// The seed refuses it too: the reservation is not the page hold.
	if err := f.seed(t, deliveryExemptOutsideWindow, rows...); err == nil {
		t.Fatal("a reserved collated output seeded")
	}
	if f.exempt(t, f.key) {
		t.Fatal("a reserved output became exempt")
	}
}

// TestR2NonrawExemptionSizeDriftRefusedAtValidate: a roster size that differs
// from the live catalog size is refused at validate, so a stale roster cannot be
// approved against a changed identity.
func TestR2NonrawExemptionSizeDriftRefusedAtValidate(t *testing.T) {
	f := newNonrawFixture(t, "exempt_size_drift")
	f.withPageKeys()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	drifted := []deliveryExemptionRow{{Bucket: "stoarama", ObjectKey: f.key, ETag: "collated-etag", SizeBytes: 901}}
	if n, vErr := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, drifted, picks); vErr == nil || n != 0 {
		t.Fatalf("a size-drifted roster validated: %d %v", n, vErr)
	}
}

// TestR2NonrawExemptionValidateCatalogSizeAndOutputKEPath: the shared
// approve/validate/seed decision refuses a roster whose catalog size drifted from
// the page/roster size, and one whose COLLATED OUTPUT has a keep-everything folder
// in its own stored path — changing only that one field each time.
func TestR2NonrawExemptionValidateCatalogSizeAndOutputKEPath(t *testing.T) {
	f := newNonrawFixture(t, "exempt_catalog_size_ke")
	f.withPageKeys()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	rows := []deliveryExemptionRow{collatedRow(f)}
	// Control: the clean roster validates.
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, picks); err != nil || n != 1 {
		t.Fatalf("clean roster did not validate: %d %v", n, err)
	}
	// Change ONLY the catalog output's size so it no longer equals the page and
	// roster size. Nothing else moves.
	f.fixture(`UPDATE recording_collation_outputs SET size_bytes=901 WHERE id=$1`, f.outputID)
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, picks); err == nil || n != 0 {
		t.Fatalf("a catalog size drift validated: %d %v", n, err)
	}
	digest := approvalDigest(deliveryExemptOutsideWindow, rows)
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, digest, deliveryExemptionDecisions[deliveryExemptOutsideWindow], rows, picks); err == nil {
		t.Fatal("a catalog size drift was approved")
	}
	if err := f.seed(t, deliveryExemptOutsideWindow, rows...); err == nil {
		t.Fatal("a catalog size drift seeded")
	}
	// Restore the size; now change ONLY the output's stored path to sit in a KE
	// folder. The classifier must hold it, so validate, approve and seed all
	// refuse.
	f.fixture(`UPDATE recording_collation_outputs SET size_bytes=900 WHERE id=$1`, f.outputID)
	keFolder := "KE_Output_Folder"
	f.fixture(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 VALUES(7200001,30,1,'https://test.invalid','stoarama','managed/acct-47/ke2/clip.mp4',10,'ke2-etag',repeat('e',64),'2026-06-01','2026-06-01 00:00:00','2026-06-01 00:01:00',$1||'/clip.mp4')`, keFolder)
	f.fixture(`UPDATE recording_collation_outputs SET nas_relative_path=$1 WHERE id=$2`, keFolder+"/nightly/video.mp4", f.outputID)
	// A second, keep-everything stream whose catalog path names the folder, so the
	// folder is a real KE folder the classifier discovers.
	f.fixture(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(88,'test','88','keout','keout','https://test.invalid','off','Asia/Tokyo','hls','video_manifest','video_live','continuous_video',30)`)
	f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(32,47,1,'keout','https://test.invalid','* * * * *','Asia/Tokyo','paused',88,now())`)
	f.fixture(`INSERT INTO recording_clips(id,recording_id,storage_destination_id,endpoint,bucket,object_key,size_bytes,etag,sha256,fire_at,clip_start_at,clip_end_at,display_path)
 VALUES(7200002,32,1,'https://test.invalid','stoarama','managed/acct-47/keout/clip.mp4',10,'keout-etag',repeat('f',64),'2026-06-01','2026-06-01 00:00:00','2026-06-01 00:01:00',$1||'/keout.mp4')`, keFolder)
	kePicks := filepath.Join(t.TempDir(), "ke-output-picks.json")
	data, _ := json.Marshal(map[string]any{"choices": map[string]any{
		"3":  r2RetirePick{Choice: "none", KeepStartUTC: f.windows[3].Start.Format(time.RFC3339), KeepEndUTC: f.windows[3].End.Format(time.RFC3339)},
		"88": r2RetirePick{Choice: "all"},
	}})
	if err := os.WriteFile(kePicks, data, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadR2RetirePicks(kePicks)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, loaded); err == nil || n != 0 {
		t.Fatalf("an output in a KE folder validated: %d %v", n, err)
	}
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, digest, deliveryExemptionDecisions[deliveryExemptOutsideWindow], rows, loaded); err == nil {
		t.Fatal("an output in a KE folder was approved")
	}
	// The seed over the same picks refuses too; it must not be shown a weaker
	// view than validate and approve.
	if _, err := seedDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, f.ensureApprovalDirect(t, deliveryExemptOutsideWindow, rows), loaded); err == nil {
		t.Fatal("an output in a KE folder seeded")
	}
}

// TestR2NonrawExemptionRefusesBest14PageFamily: joined/best14-20261021/ is
// eligible footage for the wider 2026-11-18 owner scope, but it is not a page
// family. A best14 page key is refused by the shared validator and by the SQL
// admission trigger, so it can never take a page exemption.
func TestR2NonrawExemptionRefusesBest14PageFamily(t *testing.T) {
	f := newNonrawFixture(t, "exempt_best14")
	const best14SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	best14 := "joined/best14-20261021/objects/" + best14SHA + ".mp4"
	// A real catalog output and page row, so the ONLY reason to refuse is the
	// page family, not a missing identity.
	f.fixture(`INSERT INTO recording_collation_outputs(id,collation_hour_id,part,parts,object_key,nas_relative_path,size_bytes,sha256,start_at,end_at,source_clip_ids,r2_verified_at)
 VALUES(10000002,1,2,3,$1,'best14/video.mp4',800,$2,'2026-06-01','2026-06-01 00:01:00',ARRAY[7000001]::bigint[],'2026-06-02T00:00:00Z')`, best14, best14SHA)
	f.pageHoldSHA(best14, "best14-etag", "video_collated", 800, best14SHA)
	rows := []deliveryExemptionRow{{Bucket: "stoarama", ObjectKey: best14, ETag: "best14-etag", SizeBytes: 800}}
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	if n, vErr := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, picks); vErr == nil || n != 0 {
		t.Fatalf("a best14 page key validated: %d %v", n, vErr)
	}
	// Even a hand-written SQL exemption is refused by the admission trigger.
	f.fixture(`INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision) VALUES('outside_window',repeat('9',64),'x')`)
	var sqlErr error
	func() {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		_, sqlErr = tx.Exec(f.ctx, `INSERT INTO r2_delivery_exemptions(bucket,object_key,etag,size_bytes,class,approval_id,decision,roster_sha256)
 SELECT 'stoarama',$1,'best14-etag',800,'outside_window',id,'x',repeat('9',64) FROM r2_delivery_exemption_approvals WHERE roster_sha256=repeat('9',64)`, best14)
	}()
	if sqlErr == nil || !strings.Contains(sqlErr.Error(), "does not cover") {
		t.Fatalf("the SQL admission trigger admitted a best14 page exemption: %v", sqlErr)
	}
	if f.exempt(t, best14) {
		t.Fatal("a best14 page key became exempt")
	}
}

// TestR2NonrawExemptionRefusesPageContentMismatch: the exemption must bind the
// page row's exact content. A page row whose sha256 is empty or names a
// different object than the catalog output is refused at validate, approve and
// seed.
func TestR2NonrawExemptionRefusesPageContentMismatch(t *testing.T) {
	for name, pageSHA := range map[string]string{
		"empty":   "",
		"foreign": strings.Repeat("a", 64),
	} {
		t.Run(name, func(t *testing.T) {
			f := newNonrawFixture(t, "exempt_content_"+name)
			f.withPageKeys()
			f.fixture(`UPDATE delivery_keep_holds SET sha256=$2 WHERE bucket='stoarama' AND object_key=$1`, f.key, pageSHA)
			picks, err := loadR2RetirePicks(f.picks)
			if err != nil {
				t.Fatal(err)
			}
			rows := []deliveryExemptionRow{collatedRow(f)}
			if n, vErr := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, picks); vErr == nil || n != 0 {
				t.Fatalf("a %s page content validated: %d %v", name, n, vErr)
			}
			digest := approvalDigest(deliveryExemptOutsideWindow, rows)
			if _, aErr := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, digest, deliveryExemptionDecisions[deliveryExemptOutsideWindow], rows, picks); aErr == nil {
				t.Fatalf("a %s page content was approved", name)
			}
			if sErr := f.seed(t, deliveryExemptOutsideWindow, rows...); sErr == nil {
				t.Fatalf("a %s page content seeded", name)
			}
		})
	}
}

// TestR2NonrawExemptionRefusesAmbiguousPageWindow: "outside every window" is a
// claim about the PAGE recording's stream too. An output outside its own catalog
// stream's window but INSIDE the page stream's window is refused, and a page
// stream with no pinned window fails closed instead of counting as outside.
func TestR2NonrawExemptionRefusesAmbiguousPageWindow(t *testing.T) {
	f := newNonrawFixture(t, "exempt_page_window")
	f.withPageKeys()
	// The end of stream 158's window is past stream 3's, so the last hour
	// of stream 158's window isolates the page's interval check.
	const pageStream = 158
	f.fixture(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(158,'test','158','pageside','pageside','https://test.invalid','off','Asia/Seoul','hls','video_manifest','video_live','continuous_video',30)`)
	f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'pageside','https://test.invalid','* * * * *','Asia/Seoul','paused',158,now())`)
	f.fixture(`UPDATE delivery_keep_holds SET recording_id=31 WHERE bucket='stoarama' AND object_key=$1`, f.key)
	pageWindow, ok := f.windows[pageStream]
	if !ok || pageWindow.Start.Equal(f.windows[3].Start) {
		t.Fatalf("stream %d has no distinct pinned window", pageStream)
	}
	decided := func(w api.Keep28Window) r2RetirePick {
		return r2RetirePick{Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}
	}
	// Move the output INSIDE the page stream's window but still OUTSIDE stream 3's
	// window, so the catalog-stream classifier still calls it outside_window.
	inside := pageWindow.End.Add(-time.Hour)
	if inside.Before(f.windows[3].End) {
		t.Fatal("page fixture overlaps the catalog stream window")
	}
	f.fixture(`UPDATE recording_collation_outputs SET start_at=$1,end_at=$2 WHERE id=$3`,
		inside.Format(time.RFC3339), inside.Add(time.Minute).Format(time.RFC3339), f.outputID)
	data, _ := json.Marshal(map[string]any{"choices": map[string]any{"3": decided(f.windows[3]), fmt.Sprint(pageStream): decided(pageWindow)}})
	if err := os.WriteFile(f.picks, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err == nil || !strings.Contains(err.Error(), "page stream") {
		t.Fatalf("an output inside the page stream window seeded as outside_window: %v", err)
	}
	// A page stream with NO pinned window cannot prove "outside" either, even
	// with a positive decision for its own stream.
	const noWindowStream = 4
	f.fixture(`UPDATE delivery_keep_holds SET recording_id=30 WHERE bucket='stoarama' AND object_key=$1`, f.key)
	f.fixture(`UPDATE recordings SET stream_id=$1 WHERE id=31`, noWindowStream)
	f.fixture(`UPDATE delivery_keep_holds SET recording_id=31 WHERE bucket='stoarama' AND object_key=$1`, f.key)
	if _, known := f.windows[noWindowStream]; known {
		t.Skipf("stream %d unexpectedly has a pinned window", noWindowStream)
	}
	data, _ = json.Marshal(map[string]any{"choices": map[string]any{"3": decided(f.windows[3]), fmt.Sprint(noWindowStream): r2RetirePick{Choice: "none"}}})
	if err := os.WriteFile(f.picks, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err == nil || !strings.Contains(err.Error(), "page stream") {
		t.Fatalf("a page stream with no pinned window seeded as outside_window: %v", err)
	}
}

// pageStreamFixture gives the page row its OWN recording on a decided stream, so
// the page stream and the catalog output's stream are different live facts. The
// catalog output keeps stream 3 and the interval the caller names.
func (f *nonrawFixture) pageStreamFixture(t *testing.T, pageStream int64, start, end string) {
	t.Helper()
	f.fixture(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
  VALUES(158,'test','158','pageside','pageside','https://test.invalid','off','Asia/Seoul','hls','video_manifest','video_live','continuous_video',30)`)
	f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
  VALUES(31,47,1,'pageside','https://test.invalid','* * * * *','Asia/Seoul','paused',$1,now())`, pageStream)
	f.fixture(`UPDATE delivery_keep_holds SET recording_id=31 WHERE bucket='stoarama' AND object_key=$1`, f.key)
	f.fixture(`UPDATE recording_collation_outputs SET start_at=$1,end_at=$2 WHERE id=$3`, start, end, f.outputID)
	if err := os.WriteFile(f.picks, mustPickJSON(t, f.windows[3], map[string]r2RetirePick{fmt.Sprint(pageStream): {
		Choice: "none", KeepStartUTC: f.windows[pageStream].Start.Format(time.RFC3339), KeepEndUTC: f.windows[pageStream].End.Format(time.RFC3339)}}), 0600); err != nil {
		t.Fatal(err)
	}
}

// countNonrawWrites is every durable row a refused reservation or wire must not
// leave behind for this key.
func (f *nonrawFixture) countNonrawWrites(t *testing.T) [4]int64 {
	t.Helper()
	var out [4]int64
	if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_nonraw_intents WHERE object_key=$1),
 (SELECT count(*) FROM r2_retirement_reservations WHERE object_key=$1),
 (SELECT count(*) FROM r2_nonraw_attempts WHERE object_key=$1),
 (SELECT count(*) FROM r2_object_retirements WHERE object_key=$1)`, f.key).Scan(&out[0], &out[1], &out[2], &out[3]); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestR2NonrawPageAuthorityRefusedAtReserve: the reservation evaluates the SAME
// page authority the roster does, over the LIVE page stream and the exact catalog
// interval. A page recording that moves to another valid, decided stream whose
// window contains the output — or an interval that drifts into the distinct page
// stream's window after the seed — refuses even with an unchanged ETag and size.
func TestR2NonrawPageAuthorityRefusedAtReserve(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		pageStream                  int64
		start, end, drift, driftEnd string
	}{
		// Seeded outside stream 3's window, then the page recording is reassigned
		// to stream 158, whose window (2026-09-30..2026-10-28) contains the output:
		// same ETag, same size, same interval.
		{"page_reassigned_to_decided_stream", 3, "2026-10-20T00:00:00Z", "2026-10-20T00:01:00Z", "", ""},
		// Page stream is 158 from the start; the seed's interval is outside its
		// window, then the interval drifts into 158's window (2026-09-30..2026-10-28)
		// with the page row unchanged.
		{"interval_drifts_into_page_stream_window", 158, "2026-06-01T00:00:00Z", "2026-06-01T00:01:00Z", "2026-10-20T00:00:00Z", "2026-10-20T00:01:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newNonrawFixture(t, "exempt_page_authority")
			f.withPageKeys()
			f.pageStreamFixture(t, tc.pageStream, tc.start, tc.end)
			f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
			if !f.exempt(t, f.key) {
				t.Fatal("control: the row did not seed outside_window")
			}
			if tc.drift != "" {
				f.fixture(`UPDATE recording_collation_outputs SET start_at=$1,end_at=$2 WHERE id=$3`, tc.drift, tc.driftEnd, f.outputID)
			} else {
				// Reassign the page recording to stream 158, whose pinned window
				// contains the seeded interval. The listing identity never moves.
				w := f.windows[158]
				if err := os.WriteFile(f.picks, mustPickJSON(t, f.windows[3], map[string]r2RetirePick{"158": {
					Choice: "none", KeepStartUTC: w.Start.Format(time.RFC3339), KeepEndUTC: w.End.Format(time.RFC3339)}}), 0600); err != nil {
					t.Fatal(err)
				}
				f.exec(`UPDATE recordings SET stream_id=158 WHERE id=31`)
			}
			a := f.applier(t, "reserve")
			defer a.ledger.close()
			_, err := a.reserveNonraw(f.ctx, f.collatedEntry())
			if !errors.Is(err, errInverseNonrawIneligible) || !strings.Contains(err.Error(), "page") {
				t.Fatalf("reservation adopted a drifted page authority: %v", err)
			}
			if wrote := f.countNonrawWrites(t); wrote != [4]int64{} || f.deletes() != 0 {
				t.Fatalf("refused reservation wrote %v", wrote)
			}
		})
	}
}

// TestR2NonrawPageAuthorityRefusedAtFinalWire: a reservation that was valid when
// it committed cannot be spent after the page recording is reassigned to another
// valid, decided stream whose window contains the output. The final wire reload
// refuses with the unchanged listing ETag and size, and no DELETE is sent.
func TestR2NonrawPageAuthorityRefusedAtFinalWire(t *testing.T) {
	f := newNonrawFixture(t, "exempt_page_wire")
	f.withPageKeys()
	f.pageStreamFixture(t, 3, "2026-08-20T00:00:00Z", "2026-08-20T00:01:00Z")
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	entry := f.collatedEntry()
	reserve := f.applier(t, "reserve")
	if _, err := reserve.reserveNonraw(f.ctx, entry); err != nil {
		t.Fatalf("control: the exact page identity did not reserve: %v", err)
	}
	reserve.ledger.close()
	if wrote := f.countNonrawWrites(t); wrote != [4]int64{1, 1, 0, 0} {
		t.Fatalf("control wrote %v", wrote)
	}
	// Same ETag, same size, same interval: only the page recording's stream moves,
	// to another stream with a positive window-bound decision that contains it.
	f.exec(`UPDATE recordings SET stream_id=158 WHERE id=31`)
	a := f.applier(t, "wire")
	defer a.ledger.close()
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = a.authorizeNonraw(f.ctx, tx, entry, &inverseSendGate{}); !errors.Is(err, errInverseNonrawIneligible) || !strings.Contains(err.Error(), "page authority") {
		t.Fatalf("final wire adopted a reassigned page recording: %v", err)
	}
	if wrote := f.countNonrawWrites(t); wrote != [4]int64{1, 1, 0, 0} || f.deletes() != 0 {
		t.Fatalf("refused wire wrote %v or deleted %d", wrote, f.deletes())
	}
}

func TestR2NonrawOwnerKeepStreamAddedAfterBatchAdmission(t *testing.T) {
	f := newNonrawFixture(t, "late_owner_keep_stream")
	f.withPageKeys()
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	entry := f.collatedEntry()
	a := f.applier(t, "late_owner_keep_stream")
	defer a.ledger.close()
	if a.protection.OwnerKeepStreams[3] {
		t.Fatal("control started with an owner hold for stream 3")
	}
	if _, err := a.reserveNonraw(f.ctx, entry); err != nil {
		t.Fatalf("control reservation: %v", err)
	}
	f.exec(`INSERT INTO r2_owner_keep_prefixes(bucket,prefix,decision)
 VALUES('stoarama','raw/stream/3/','keep stream 3')`)
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err = a.authorizeNonraw(f.ctx, tx, entry, &inverseSendGate{}); !errors.Is(err, errInverseNonrawIneligible) || !strings.Contains(err.Error(), "owner_keep_stream") {
		t.Fatalf("wire did not see the live owner hold: %v", err)
	}
	if wrote := f.countNonrawWrites(t); wrote != [4]int64{1, 1, 0, 0} || f.deletes() != 0 {
		t.Fatalf("owner hold allowed writes %v or %d deletes", wrote, f.deletes())
	}
}

func TestR2NonrawPageAuthorityLocksRowsThroughWire(t *testing.T) {
	f := newNonrawFixture(t, "exempt_page_row_locks")
	f.withPageKeys()
	f.pageStreamFixture(t, 158, "2026-06-01T00:00:00Z", "2026-06-01T00:01:00Z")
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	entry := f.collatedEntry()
	a := f.applier(t, "wire")
	defer a.ledger.close()
	if _, err := a.reserveNonraw(f.ctx, entry); err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	authority, err := a.authorizeNonraw(f.ctx, tx, entry, &inverseSendGate{})
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.check(f.ctx, a.store.Bucket(), a.store.EndpointSHA256()); err != nil {
		t.Fatalf("control pre-send check: %v", err)
	}
	assertLocked := func(err error) {
		t.Helper()
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
			t.Fatalf("page authority row was not locked: %v", err)
		}
	}
	var key string
	assertLocked(f.pool.QueryRow(f.ctx, `SELECT object_key FROM delivery_keep_holds
 WHERE bucket='stoarama' AND object_key=$1 FOR UPDATE NOWAIT`, f.key).Scan(&key))
	probeDelete := func() error {
		writer, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Rollback(f.ctx)
		if _, err := writer.Exec(f.ctx, `SET LOCAL lock_timeout='150ms'`); err != nil {
			t.Fatal(err)
		}
		_, err = writer.Exec(f.ctx, `DELETE FROM recordings WHERE id=31`)
		return err
	}
	assertLocked(probeDelete())
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := probeDelete(); err != nil {
		t.Fatalf("control could not remove the page recording after wire exit: %v", err)
	}
}

func TestR2NonrawNoncanonicalHeadCannotIssue(t *testing.T) {
	f := newNonrawFixture(t, "nonraw_noncanonical_head")
	f.withPageKeys()
	f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
	entry := f.collatedEntry()
	a := f.applier(t, "head")
	defer a.ledger.close()
	if _, err := a.reserveNonraw(f.ctx, entry); err != nil {
		t.Fatal(err)
	}
	// Inject the value at the decoded HEAD boundary. The SDK normally
	// removes the HTTP header's framing quotes before returning it.
	a.store = f.store
	f.store.mu.Lock()
	head := f.store.objects[f.key]
	head.ETag = `"collated-etag"`
	f.store.objects[f.key] = head
	f.store.mu.Unlock()
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(f.ctx)
	if _, err := a.authorizeNonraw(f.ctx, tx, entry, &inverseSendGate{}); err == nil || !strings.Contains(err.Error(), "collated HEAD ETag") {
		t.Fatalf("wire accepted the noncanonical HEAD: %v", err)
	}
	if wrote := f.countNonrawWrites(t); wrote != [4]int64{1, 1, 0, 0} || f.deletes() != 0 {
		t.Fatalf("noncanonical HEAD allowed writes %v or %d deletes", wrote, f.deletes())
	}
}

// TestR2NonrawExemptionStrictIdentityRepresentations: the guard points demand the
// SAME exact representations the SQL compares and CHECKs. A case-folded sha256 or
// a quote-trimmed ETag that SQL would refuse is refused here instead of being
// normalized into authority; the canonical provider identity is unaffected.
func TestR2NonrawExemptionStrictIdentityRepresentations(t *testing.T) {
	f := newNonrawFixture(t, "exempt_strict_identity")
	f.withPageKeys()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	rows := []deliveryExemptionRow{collatedRow(f)}
	f.fixture(`INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision) VALUES('outside_window',repeat('6',64),'x')`)
	insertExemption := func(etag string) error {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		_, err = tx.Exec(f.ctx, `INSERT INTO r2_delivery_exemptions(bucket,object_key,etag,size_bytes,class,approval_id,decision,roster_sha256)
  SELECT 'stoarama',$1,$2,900,'outside_window',id,'x',repeat('6',64) FROM r2_delivery_exemption_approvals WHERE roster_sha256=repeat('6',64)`, f.key, etag)
		return err
	}
	// SQL's own ledger CHECK refuses an upper-case sha256, so no exemption may
	// open a gate whose receipt could never be written.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
  VALUES('collated',$1,'stoarama',$2,900,upper($3),$4)`, f.outputID, f.key, nonrawOutputSHA, strings.Repeat("d", 64)); err == nil {
		t.Fatal("SQL admitted an upper-case sha256 reservation")
	}
	// The page row's sha256 is upper-case: the folded comparison would have
	// matched the catalog, so validate, approve and seed must each refuse.
	f.fixture(`UPDATE delivery_keep_holds SET sha256=upper($2) WHERE bucket='stoarama' AND object_key=$1`, f.key, nonrawOutputSHA)
	if err := insertExemption(rows[0].ETag); err == nil || !strings.Contains(err.Error(), "carry the catalog sha256") {
		t.Fatalf("SQL admission accepted an upper-case page sha256: %v", err)
	}
	if n, vErr := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, picks); vErr == nil || n != 0 {
		t.Fatalf("an upper-case page sha256 validated: %d %v", n, vErr)
	}
	if _, aErr := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, approvalDigest(deliveryExemptOutsideWindow, rows), deliveryExemptionDecisions[deliveryExemptOutsideWindow], rows, picks); aErr == nil {
		t.Fatal("an upper-case page sha256 was approved")
	}
	if sErr := f.seed(t, deliveryExemptOutsideWindow, rows...); sErr == nil {
		t.Fatal("an upper-case page sha256 seeded")
	}
	// Restore the content hash and quote the page ETag. The exemption admission
	// trigger compares h.etag verbatim, so a quoted stored value is not the page
	// identity the SQL binds.
	f.fixture(`UPDATE delivery_keep_holds SET sha256=$2,etag='"'||etag||'"' WHERE bucket='stoarama' AND object_key=$1`, f.key, nonrawOutputSHA)
	quoted := rows[0]
	quoted.ETag = `"collated-etag"`
	if n, vErr := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, []deliveryExemptionRow{quoted}, picks); vErr == nil || n != 0 {
		t.Fatalf("a quote-trimmed page ETag validated: %d %v", n, vErr)
	}
	if sErr := f.seed(t, deliveryExemptOutsideWindow, quoted); sErr == nil {
		t.Fatal("a quote-trimmed page ETag seeded")
	}
	// And the SQL admission trigger refuses the canonical exemption against a
	// quoted page row: SQL compares h.etag and NEW.etag verbatim, so the provider's
	// canonical identity can never match a quote-padded stored value.
	if err := insertExemption(rows[0].ETag); err == nil || !strings.Contains(err.Error(), "exact live page identity") {
		t.Fatalf("SQL admission accepted a canonical ETag against a quoted page row: %v", err)
	}
	if err := insertExemption(quoted.ETag); err == nil || !strings.Contains(err.Error(), "r2_delivery_exemptions_etag_check") {
		t.Fatalf("SQL admission accepted a matching quoted ETag: %v", err)
	}
	padded := rows[0]
	padded.ETag += "\u00a0"
	f.fixture(`UPDATE delivery_keep_holds SET etag=$2 WHERE bucket='stoarama' AND object_key=$1`, f.key, padded.ETag)
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, []deliveryExemptionRow{padded}, picks); err == nil || n != 0 {
		t.Fatalf("Go admission accepted a matching Unicode-padded ETag: %d %v", n, err)
	}
	if err := insertExemption(padded.ETag); err == nil || !strings.Contains(err.Error(), "r2_delivery_exemptions_etag_check") {
		t.Fatalf("SQL admission accepted a matching Unicode-padded ETag: %v", err)
	}
	// The canonical identity is still exactly what the guards accept.
	f.fixture(`UPDATE delivery_keep_holds SET etag=$2 WHERE bucket='stoarama' AND object_key=$1`, f.key, "collated-etag")
	if n, vErr := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, picks); vErr != nil || n != 1 {
		t.Fatalf("the canonical identity was refused: %d %v", n, vErr)
	}
	f.mustSeed(t, deliveryExemptOutsideWindow, rows...)
	entry := f.collatedEntry()
	entry.ETag = `"collated-etag"`
	a := f.applier(t, "quoted")
	defer a.ledger.close()
	if _, err := a.reserveNonraw(f.ctx, entry); !errors.Is(err, errInverseNonrawIneligible) {
		t.Fatalf("a quoted listing ETag reserved: %v", err)
	}
	if wrote := f.countNonrawWrites(t); wrote != [4]int64{} || f.deletes() != 0 {
		t.Fatalf("the quoted listing ETag wrote %v", wrote)
	}
}

func (f *nonrawFixture) addReleasedOutputHold(t *testing.T) {
	t.Helper()
	// A cleanup plan reserves this exact collated output and is released by an
	// effective R2 release, so only the wider view still lists it.
	f.fixture(`INSERT INTO nas_cleanup_plans(id,account_id,connection_id,mode,label,report_sha256,manifest,plan_sha256)
  VALUES(91,47,13,'paused','released-output',repeat('c',64),
   jsonb_build_array(jsonb_build_object('kind','collated','source_id',$1::bigint,'relative_path','nightly/video.mp4')),repeat('b',64))`, f.outputID)
	f.fixture(`INSERT INTO nas_cleanup_items(id,plan_id,ordinal,state,delete_ever_issued,recovery,outcome)
  VALUES(9101,91,1,'deleted',true,'r2_sha_verified','{"outcome":"deleted"}')`)
	f.fixture(`INSERT INTO nas_cleanup_r2_releases(item_id,manifest_sha256,picks_sha256,policy_sha256,reason,authority)
  SELECT 9101,repeat('a',64),repeat('b',64),repeat('c',64),'fixture',jsonb_build_object('recovery',i.recovery,
   'receipt_sha256',encode(sha256(convert_to(i.outcome::text,'UTF8')),'hex'),
   'recovery_sha256',encode(sha256(convert_to(coalesce(i.recovery_evidence::text,''),'UTF8')),'hex'))
  FROM nas_cleanup_items i WHERE i.id=9101`)
	var r2View, wideView bool
	if err := f.pool.QueryRow(f.ctx, `SELECT EXISTS(SELECT 1 FROM nas_cleanup_r2_reserved_files WHERE kind='collated' AND source_id=$1),
  EXISTS(SELECT 1 FROM nas_cleanup_reserved_files WHERE kind='collated' AND source_id=$1)`, f.outputID).Scan(&r2View, &wideView); err != nil {
		t.Fatal(err)
	}
	if r2View || !wideView {
		t.Fatalf("fixture: r2 view=%v wide view=%v, want false/true", r2View, wideView)
	}
}

func TestR2NonrawExemptionRosterReadsClassifierHolds(t *testing.T) {
	f := newNonrawFixture(t, "exempt_classifier_holds")
	f.withPageKeys()
	picks, err := loadR2RetirePicks(f.picks)
	if err != nil {
		t.Fatal(err)
	}
	rows := []deliveryExemptionRow{collatedRow(f)}
	f.addReleasedOutputHold(t)
	var seedHeld bool
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_nonraw_key_held_except_page('stoarama',$1,'collated',$2)`, f.key, f.outputID).Scan(&seedHeld); err != nil || !seedHeld {
		t.Fatalf("seed statement held=%v (%v)", seedHeld, err)
	}
	p, err := loadInverseProtectionMode(f.ctx, f.pool, "stoarama", picks, f.windows, true)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Held[f.key] {
		t.Fatal("the live classifier did not hold the key")
	}
	if n, vErr := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, rows, picks); vErr == nil || n != 0 {
		t.Fatalf("a classifier-held row validated: %d %v", n, vErr)
	}
	if _, aErr := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptOutsideWindow, approvalDigest(deliveryExemptOutsideWindow, rows), deliveryExemptionDecisions[deliveryExemptOutsideWindow], rows, picks); aErr == nil {
		t.Fatal("a classifier-held row was approved")
	}
	if sErr := f.seed(t, deliveryExemptOutsideWindow, rows...); sErr == nil || !strings.Contains(sErr.Error(), "held by the live classifier") {
		t.Fatalf("a classifier-held row seeded: %v", sErr)
	}
	if f.exempt(t, f.key) {
		t.Fatal("a classifier-held row became exempt")
	}
	// A key with a committed retirement is refused by the same roster decision
	// rather than re-staged: the exemption opens the page hold, and the exact
	// reservation is then admitted, after which a second roster pass must refuse.
	g := newNonrawFixture(t, "exempt_retiring")
	g.withPageKeys()
	g.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(g))
	g.exec(`INSERT INTO r2_retirement_reservations(kind,source_id,bucket,object_key,size_bytes,sha256,manifest_sha256)
  VALUES('collated',$1,'stoarama',$2,900,$3,$4)`, g.outputID, g.key, nonrawOutputSHA, strings.Repeat("d", 64))
	gRows := []deliveryExemptionRow{collatedRow(g)}
	if err := g.seed(t, deliveryExemptOutsideWindow, gRows...); err == nil || !strings.Contains(err.Error(), "retirement reservation or receipt") {
		t.Fatalf("a retiring key was re-staged: %v", err)
	}
}

func TestR2NonrawLiveOutputHoldVetoesStaleBatch(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		t.Run(fmt.Sprintf("reserved=%v", reserved), func(t *testing.T) {
			f := newNonrawFixture(t, "exempt_late_cleanup_hold")
			f.withPageKeys()
			f.mustSeed(t, deliveryExemptOutsideWindow, collatedRow(f))
			entry := f.collatedEntry()
			a := f.applier(t, "stale")
			defer a.ledger.close()
			if a.protection.Held[f.key] {
				t.Fatal("control started with a cleanup hold")
			}
			var want [4]int64
			if reserved {
				if _, err := a.reserveNonraw(f.ctx, entry); err != nil {
					t.Fatal(err)
				}
				want = [4]int64{1, 1, 0, 0}
			}
			f.addReleasedOutputHold(t)
			var err error
			if !reserved {
				_, err = a.reserveNonraw(f.ctx, entry)
			} else {
				tx, beginErr := f.pool.Begin(f.ctx)
				if beginErr != nil {
					t.Fatal(beginErr)
				}
				defer tx.Rollback(f.ctx)
				_, err = a.authorizeNonraw(f.ctx, tx, entry, &inverseSendGate{})
			}
			if !errors.Is(err, errInverseNonrawIneligible) || !strings.Contains(err.Error(), "live_hold") {
				t.Fatalf("stale batch admitted a live output hold: %v", err)
			}
			if wrote := f.countNonrawWrites(t); wrote != want || f.deletes() != 0 {
				t.Fatalf("live output hold allowed writes %v or %d deletes", wrote, f.deletes())
			}
		})
	}
}
