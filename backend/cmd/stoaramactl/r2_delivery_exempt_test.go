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
	"github.com/jackc/pgx/v5"
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

// pageGeneration is the synthetic serving registry generation the fixtures name
// when the page is taken down, recorded for the record on the retirement row.
const pageGeneration = "0123456789abcdef0123456789abcdef"

// seedServingGeneration records the generation the serving API observed as the
// current pointer, which is what the authority admission binds to. It must run
// while the page still serves.
func (f *nonrawFixture) seedServingGeneration(generation string) {
	f.exec(`INSERT INTO r2_delivery_serving_generation(singleton,generation)
 VALUES(true,$1) ON CONFLICT(singleton) DO UPDATE SET generation=excluded.generation`, generation)
}

// writeCutoverEvidence inserts reviewed cutover authority with distinct per-fact
// artifacts and a derived deadline already in the past, for the fixture's
// generation. It models the operator's reviewed record, not an API observation.
func (f *nonrawFixture) writeCutoverEvidence(generation string) {
	f.exec(`INSERT INTO r2_delivery_cutover_authority(
 generation,decision,legacy_grant_boundary_at,legacy_signed_grant_sha256,legacy_origin_removed_at,
 legacy_origin_evidence_sha256,cache_expired_at,cache_evidence_sha256,rollback_routing_sha256,
 transfer_bound_at,transfer_evidence_sha256,approval_sha256,deadline_at)
 VALUES($1,'test: cutover',clock_timestamp()-interval '10 minutes',repeat('a',64),clock_timestamp()-interval '10 minutes',
  repeat('b',64),clock_timestamp()-interval '10 minutes',repeat('c',64),repeat('d',64),
  clock_timestamp()-interval '10 minutes',repeat('e',64),repeat('f',64),clock_timestamp()-interval '1 second')`, generation)
}

// retirePage arms, records reviewed cutover authority whose derived deadline has
// already passed, and commits the one serving state: exactly the staged path
// r2-inverse page-quiesce, page-cutover, page-retired runs. The fixture has no
// readers or capabilities, so the reader/capability drain is already complete;
// the authority is still required, because an empty ledger is not proof.
func (f *nonrawFixture) retirePage() {
	f.seedServingGeneration(pageGeneration)
	f.exec(`INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: arming the old page down')`)
	f.writeCutoverEvidence(pageGeneration)
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
	// There is exactly one retirement state, and it is the same row the serving
	// routes refuse on. Until it exists no gate opens; once it exists the function
	// and the gate agree immediately.
	var retired bool
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || retired {
		t.Fatalf("a serving page reported retired: %v %v", retired, err)
	}
	f.retirePage()
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || !retired {
		t.Fatalf("the committed retirement state did not retire the page: %v %v", retired, err)
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

// TestR2NonrawPageRetiredRefusesLiveTickets: taking the page down is proved in
// SQL, in the staged order. It refuses before quiesce, refuses a live reader,
// refuses a capability that may still be transferring even after its own
// capability expiry, and commits only once everything has drained.
func TestR2NonrawPageRetiredRefusesLiveTickets(t *testing.T) {
	f := newNonrawFixture(t, "exempt_page_cli")
	retire := func() (string, error) {
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Deferred from the start: a failing assertion must release the
		// connection, or the fixture's pool cleanup hangs waiting for it.
		defer tx.Rollback(f.ctx)
		at, err := retireDeliveryPage(f.ctx, tx, pageGeneration, "test: the old page is down")
		if err != nil {
			return "", err
		}
		if err = tx.Commit(f.ctx); err != nil {
			return "", err
		}
		return at, nil
	}
	if _, err := retire(); err == nil || !strings.Contains(err.Error(), "not quiesced") {
		t.Fatalf("retirement without quiesce was not refused: %v", err)
	}
	// One recorded capability, still live, minted before arming.
	f.exec(`INSERT INTO r2_delivery_download_tickets(bucket,object_key,etag,size_bytes,object_path_sha256,expires_at)
 VALUES('stoarama','joined/x.mp4','e',1,repeat('a',64),clock_timestamp()+interval '5 minutes')`)
	f.seedServingGeneration(pageGeneration)
	f.exec(`INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: the old page is down')`)
	// No cutover authority yet: refused, and a live capability could not be
	// retired anyway.
	if _, err := retire(); err == nil || !strings.Contains(err.Error(), "cutover authority") {
		t.Fatalf("retirement without cutover authority was not refused: %v", err)
	}
	// Reviewed authority bound to the observed serving generation, deadline past.
	f.writeCutoverEvidence(pageGeneration)
	var retired bool
	if _, err := retire(); err == nil || !strings.Contains(err.Error(), "unexpired") {
		t.Fatalf("retirement ignored a live recorded capability: %v", err)
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || retired {
		t.Fatalf("a refused retirement retired the page: %v %v", retired, err)
	}
	// Drain the recorded capability; the same call now commits, exactly once.
	f.fixture(`UPDATE r2_delivery_download_tickets SET expires_at=clock_timestamp()-interval '1 second'`)
	if at, err := retire(); err != nil || at == "" {
		t.Fatalf("retirement refused after the drain: %q %v", at, err)
	}
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || !retired {
		t.Fatalf("the committed retirement did not take effect: %v %v", retired, err)
	}
	// One committed state, append-only and unrepeatable.
	if _, err := f.pool.Exec(f.ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,'again')`, pageGeneration); err == nil {
		t.Fatal("a second page retirement state landed")
	}
	if _, err := f.pool.Exec(f.ctx, `DELETE FROM r2_delivery_page_retirement`); err == nil {
		t.Fatal("the page retirement state was removed")
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
	const pageStream = 78
	f.fixture(`INSERT INTO streams(id,provider,external_id,name,slug,source_url,recording_state,local_timezone,capture_type,source_family,execution_class,capture_family,expected_fps)
 VALUES(78,'test','78','pageside','pageside','https://test.invalid','off','UTC','hls','video_manifest','video_live','continuous_video',30)`)
	f.fixture(`INSERT INTO recordings(id,account_id,storage_destination_id,name,stream_url,cron_expr,cron_timezone,status,stream_id,paused_at)
 VALUES(31,47,1,'pageside','https://test.invalid','* * * * *','UTC','paused',78,now())`)
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
	if after := f.counts(t); after != [3]int64{0, 0, 0} {
		t.Fatalf("fixture started dirty: %v", after)
	}
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptOutsideWindow, []deliveryExemptionRow{collatedRow(f)}, picks); err != nil || n != 1 {
		t.Fatalf("clean roster did not validate: %d %v", n, err)
	}
	if after := f.counts(t); after != [3]int64{0, 0, 0} {
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
	// The in_window class is the classifier's answer for it, so that one passes
	// validation (its gate still stays closed until NAS custody and retirement).
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptInWindow, []deliveryExemptionRow{collatedRow(f)}, picks); err != nil || n != 1 {
		t.Fatalf("in_window mixed output did not validate as in_window: %d %v", n, err)
	}
	// And the seed step refuses the same wrong class, so validation and seeding
	// can never disagree. The only row a seed may leave is its owner approval,
	// which its own separate approve step writes; no exemption may exist.
	if err := f.seed(t, deliveryExemptOutsideWindow, collatedRow(f)); err == nil {
		t.Fatal("the seed admitted the mixed-source output as outside_window")
	}
	if after := f.counts(t); after[1] != 0 || after[2] != 0 {
		t.Fatalf("a refused seed wrote an exemption: %v", after)
	}
}

// counts is the durable state validation and refused seeds must never touch.
func (f *nonrawFixture) counts(t *testing.T) [3]int64 {
	t.Helper()
	var approvals, exemptions, retirements int64
	if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM r2_delivery_exemption_approvals),
 (SELECT count(*) FROM r2_delivery_exemptions),(SELECT count(*) FROM r2_delivery_page_retirement)`).
		Scan(&approvals, &exemptions, &retirements); err != nil {
		t.Fatal(err)
	}
	return [3]int64{approvals, exemptions, retirements}
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
	// The repair is clean and validates on its own.
	if n, err := validateDeliveryExemptions(f.ctx, f.pool, deliveryExemptConfidentRepair, []deliveryExemptionRow{repairRow()}, picks); err != nil || n != 1 {
		t.Fatalf("clean repair did not validate: %d %v", n, err)
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

// TestR2NonrawPageRetirementStagedProof: the CLI's own retirement path proves the
// staged order in SQL. Without quiesce it refuses; with quiesce but a live reader
// or a live capability it refuses; once both drain, the same call commits and
// r2_delivery_page_retired() agrees.
func TestR2NonrawPageRetirementStagedProof(t *testing.T) {
	f := newNonrawFixture(t, "exempt_page_retire")
	retire := func() (string, error) {
		tx, err := f.pool.BeginTx(f.ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(f.ctx)
		at, err := retireDeliveryPage(f.ctx, tx, pageGeneration, "test: staged proof")
		if err != nil {
			return "", err
		}
		if err = tx.Commit(f.ctx); err != nil {
			return "", err
		}
		return at, nil
	}
	if _, err := retire(); err == nil || !strings.Contains(err.Error(), "not quiesced") {
		t.Fatalf("retirement without quiesce was not refused: %v", err)
	}
	// A reader lease and a live capability both exist while the page still
	// serves; only then is retirement armed.
	f.exec(`INSERT INTO r2_delivery_public_readers(reader_id,expires_at) VALUES(repeat('a',32),clock_timestamp()+interval '2 minutes')`)
	f.exec(`INSERT INTO r2_delivery_download_tickets(bucket,object_key,etag,size_bytes,object_path_sha256,expires_at) VALUES('stoarama','x','e',1,repeat('a',64),clock_timestamp()+interval '2 minutes')`)
	f.seedServingGeneration(pageGeneration)
	f.exec(`INSERT INTO r2_delivery_page_quiesce(reason) VALUES('test: staged proof')`)
	// Reviewed cutover authority bound to the observed serving generation,
	// deadline already past, so the only remaining holds are the live reader and
	// the live capability.
	f.writeCutoverEvidence(pageGeneration)
	if _, err := retire(); err == nil || !strings.Contains(err.Error(), "admitted public readers") {
		t.Fatalf("retirement with a live reader was not refused: %v", err)
	}
	f.exec(`DELETE FROM r2_delivery_public_readers`)
	if _, err := retire(); err == nil || !strings.Contains(err.Error(), "unexpired") {
		t.Fatalf("retirement with a live capability was not refused: %v", err)
	}
	f.exec(`DELETE FROM r2_delivery_download_tickets`)
	at, err := retire()
	if err != nil || at == "" {
		t.Fatalf("drained retirement did not commit: %q %v", at, err)
	}
	var retired bool
	if err := f.pool.QueryRow(f.ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || !retired {
		t.Fatalf("r2_delivery_page_retired() disagrees with the committed row: %v %v", retired, err)
	}
	// The direct insert path is refused for the same reasons the CLI path is: an
	// operator cannot bypass the staged proof by writing the row by hand.
	if _, err := retire(); err == nil {
		t.Fatal("a second retirement committed")
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
	// The wrong class is refused too: the in-window output is not outside_window.
	if _, err := recordDeliveryExemptionApproval(f.ctx, f.pool, deliveryExemptInWindow, digest, deliveryExemptionDecisions[deliveryExemptInWindow], good, picks); err == nil || !strings.Contains(err.Error(), "outside_window") {
		t.Fatalf("an outside_window roster was approved as in_window: %v", err)
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

// TestDeliveryCutoverCanonicalPreservesSubsecondAndDistinctEvidence: the approval
// digest covers the exact instants the row stores (RFC3339Nano, no truncation),
// and the record requires a distinct artifact per fact including the transfer.
func TestDeliveryCutoverCanonicalPreservesSubsecondAndDistinctEvidence(t *testing.T) {
	f := newNonrawFixture(t, "exempt_cutover_canonical")
	base := time.Date(2026, 11, 20, 1, 2, 3, 456789000, time.UTC)
	e := deliveryCutoverEvidence{
		Generation:              pageGeneration,
		Decision:                "test: canonical",
		LegacyGrantBoundaryAt:   base,
		LegacySignedGrantSHA256: strings.Repeat("a", 64),
		LegacyOriginRemovedAt:   base.Add(time.Second),
		LegacyOriginEvidenceSHA: strings.Repeat("b", 64),
		CacheExpiredAt:          base.Add(2 * time.Second),
		CacheEvidenceSHA256:     strings.Repeat("c", 64),
		RollbackRoutingSHA256:   strings.Repeat("d", 64),
		TransferBoundAt:         base.Add(3 * time.Second),
		TransferEvidenceSHA256:  strings.Repeat("e", 64),
	}
	canonical, err := e.canonical()
	if err != nil {
		t.Fatal(err)
	}
	// The canonical bytes carry full subsecond precision, so the approval digest
	// names the same instant the row stores.
	if !strings.Contains(string(canonical), "2026-11-20T01:02:03.456789Z") {
		t.Fatalf("canonical bytes truncated the instant: %s", canonical)
	}
	sum := sha256.Sum256(canonical)
	approved := hex.EncodeToString(sum[:])
	// A reused evidence digest is refused before any insert.
	e.CacheEvidenceSHA256 = e.TransferEvidenceSHA256
	if _, err := recordDeliveryCutover(f.ctx, f.pool, e, approved); err == nil || !strings.Contains(err.Error(), "reused") {
		t.Fatalf("a reused evidence digest was accepted: %v", err)
	}
	// Restore it; a distinct record is still refused here only because the
	// authority must follow arming, proving the command cannot precede quiesce.
	e.CacheEvidenceSHA256 = strings.Repeat("c", 64)
	if _, err := recordDeliveryCutover(f.ctx, f.pool, e, approved); err == nil || !strings.Contains(err.Error(), "quiesce") {
		t.Fatalf("authority before quiesce was accepted: %v", err)
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
