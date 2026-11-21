package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// deliveryExemptionClass is 0225's r2_delivery_exemption_class: an owner-approved
// family of public delivery page keys that may lose page-keep protection once
// the class's gate (in SQL, r2_delivery_exempt) is open.
type deliveryExemptionClass string

const (
	deliveryExemptOutsideWindow   deliveryExemptionClass = "outside_window"
	deliveryExemptConfidentRepair deliveryExemptionClass = "confident_repair"
	deliveryExemptInWindow        deliveryExemptionClass = "in_window"
)

// deliveryExemptionDecisions is each class's verbatim owner decision, stored on
// every exemption row the class seeds.
var deliveryExemptionDecisions = map[deliveryExemptionClass]string{
	deliveryExemptOutsideWindow:   `Deniz 2026-11-19: "1. delete the "outside every window" set from r2 …" (4,348 collated delivery videos, 0.582 TB).`,
	deliveryExemptConfidentRepair: `Deniz 2026-11-19: "Delete them (Recommended)", answering "Delete the 3.38 TB from R2 once the page comes down. Saves ~$50/month and loses no unique footage." (9,232 video_assembly objects under experimental/stitching-confident-repairs/.)`,
	deliveryExemptInWindow:        `Deniz 2026-11-19: "File list only": copy and certify the 2.35 TB of in-window videos on the NAS, publish a static CSV+JSON manifest mapping each old link to its NAS path, take the old page down, then delete the in-window R2 copies.`,
}

// receiptReason is the r2_object_retirements reason an exempt key retires under.
func (c deliveryExemptionClass) receiptReason() string {
	return "owner_delivery_page_" + string(c)
}

// deliveryExemptionRow is one roster line. Extra roster fields are ignored; these
// four are the exact identity the exemption binds.
type deliveryExemptionRow struct {
	Bucket    string `json:"bucket"`
	ObjectKey string `json:"object_key"`
	ETag      string `json:"etag"`
	SizeBytes int64  `json:"size_bytes"`
}

// readDeliveryExemptionRoster reads a pinned JSONL roster. The digest covers the
// exact bytes parsed, and any malformed, incomplete or duplicate row fails.
func readDeliveryExemptionRoster(path, digest string) ([]deliveryExemptionRow, error) {
	if !r2RetireLowerHex64(digest) {
		return nil, errors.New("invalid roster SHA256")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if sum := sha256.Sum256(data); hex.EncodeToString(sum[:]) != digest {
		return nil, errors.New("roster differs from its approved digest")
	}
	var rows []deliveryExemptionRow
	seen := map[[2]string]bool{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		var row deliveryExemptionRow
		if err = json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, fmt.Errorf("roster line %d: %w", len(rows)+1, err)
		}
		id := [2]string{row.Bucket, row.ObjectKey}
		if row.Bucket == "" || row.ObjectKey == "" || row.ETag == "" || row.SizeBytes <= 0 || seen[id] {
			return nil, fmt.Errorf("roster line %d is incomplete or repeats a key", len(rows)+1)
		}
		seen[id] = true
		rows = append(rows, row)
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("roster is empty")
	}
	return rows, nil
}

// deliveryPageStreamDecided reports whether the owner made a positive,
// window-bound, non-keep-everything decision for the stream. A missing pick, a
// pick made against another window, or "all" is never exemption authority.
func deliveryPageStreamDecided(picks r2RetirePicks, windows map[int64]api.Keep28Window, stream int64) bool {
	if stream <= 0 {
		return false
	}
	w, ok := windows[stream]
	if !ok {
		return false
	}
	pick, known := picks.Choices[stream]
	if !known || pick.Choice == "all" {
		return false
	}
	return r2RetireEffectiveChoice(pick, known, w) != ""
}

// deliveryExemptionClassOf is the exemption class the collated executor's own
// live assessment implies: a catalog output inside a pinned window is in_window,
// one outside every pinned window is outside_window, and a confident repair
// (no catalog row) is confident_repair.
func deliveryExemptionClassOf(x inverseCollatedAssessment, ok bool) (deliveryExemptionClass, bool) {
	if !ok {
		return "", false
	}
	switch x.Kind {
	case inverseNonrawKindRepair:
		return deliveryExemptConfidentRepair, true
	case inverseNonrawKindCollated:
		if x.Keep28 {
			return deliveryExemptInWindow, true
		}
		return deliveryExemptOutsideWindow, true
	}
	return "", false
}

// inverseSeedHeldSQL is r2_nonraw_key_held WITHOUT its page-key term. The seed
// is deciding whether to exempt this exact page key, so its own page hold cannot
// be the veto; every other hold still refuses the row.
const inverseSeedHeldSQL = `SELECT
 r2_nonraw_key_held_except_page($1,$2,'collated',0)`

// inverseSeedBatchSQL is the collated executor's own batch statement with only
// its page-key hold term replaced by the page-free one. Everything else the
// classifier reads — source reservations, incidents, restores, joined snapshot
// and dry-run scopes, unresolved or duplicate sources, KE and pinned windows — is
// exactly the executor's, so a seed can never see a weaker answer than the wire.
var inverseSeedBatchSQL = strings.Replace(
	inverseNonrawBatchSQL,
	"r2_nonraw_key_held($1,o.object_key,'collated',o.id)",
	"r2_nonraw_key_held_except_page($1,o.object_key,'collated',o.id)",
	1,
)

// loadSeedAssessment loads one roster key's live assessment with ONLY its own page
// veto suppressed and no exemption consulted, so the seed sees exactly the class
// the collated executor would compute once the exemption exists. No other rule is
// dropped and no reason is cleared: a source-level hold or an unresolved source
// closure refuses the row here just as it would at the wire.
func loadSeedAssessment(ctx context.Context, q r2RetireQuerier, bucket, key string, p inverseProtection) (inverseCollatedAssessment, error) {
	kind, known := inverseNonrawKindOf(key)
	if !known {
		return inverseCollatedAssessment{}, errInverseNonrawIneligible
	}
	var x inverseCollatedAssessment
	if kind == inverseNonrawKindRepair {
		// A repair's identity is its exact page row; no exemption exists yet, so
		// read the same facts scanRepair does directly from that row.
		var shared, rawIdentities int64
		err := q.QueryRow(ctx, `SELECT h.object_key,coalesce(r.stream_id,0),h.size_bytes,h.sha256,h.relative_path,
 (SELECT count(*) FROM recording_clips rc WHERE rc.bucket=$1 AND rc.object_key=h.object_key),
 (SELECT count(*) FROM recording_collation_outputs d WHERE d.object_key=h.object_key)
 FROM delivery_keep_holds h LEFT JOIN recordings r ON r.id=h.recording_id
 WHERE h.bucket=$1 AND h.object_key=$2`, bucket, key).Scan(&x.Key, &x.StreamID, &x.SizeBytes, &x.SHA, &x.Path, &rawIdentities, &shared)
		if err != nil {
			return x, err
		}
		x.Kind, x.RawIdentities, x.Shared = kind, rawIdentities, shared != 0
		if x.Held, err = scanSeedHeld(ctx, q, bucket, key); err != nil {
			return x, err
		}
		return assessRepair(p, x), nil
	}
	rows, err := q.Query(ctx, inverseSeedBatchSQL+` WHERE o.object_key=$2`, bucket, key)
	if err != nil {
		return x, err
	}
	n := 0
	for rows.Next() {
		n++
		if x, err = scanCollated(rows); err != nil {
			rows.Close()
			return x, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil || n != 1 {
		return x, errInverseNonrawIneligible
	}
	// Re-assess with the executor's own function. scanCollated's own reasons (an
	// empty, duplicate or unresolved source closure) are never cleared.
	return assessCollated(p, x), nil
}

func scanSeedHeld(ctx context.Context, q r2RetireQuerier, bucket, key string) (bool, error) {
	var held bool
	err := q.QueryRow(ctx, inverseSeedHeldSQL, bucket, key).Scan(&held)
	return held, err
}

// deliveryExemptionApproval is one pre-recorded owner approval for a roster.
// Approvals are written by their own command step, with the decision text, BEFORE
// any seeding, so a seed can only ever consume authority the owner already gave.
type deliveryExemptionApproval struct {
	ID        int64                  `json:"id"`
	Class     deliveryExemptionClass `json:"class"`
	RosterSHA string                 `json:"roster_sha256"`
	Decision  string                 `json:"decision"`
	CreatedAt string                 `json:"created_at"`
}

// recordDeliveryExemptionApproval records the owner's approval for one roster
// digest under exactly one class. It is a separate step from seeding: seeding
// consumes an approval, it never creates one.
//
// It does NOT trust the caller that the roster already passed validation. When
// the roster content is supplied, the approval re-runs the SAME decision seeding
// runs, in the approval's own transaction, and only then inserts the digest. That
// makes "validated by the same exact class and roster bytes against the same
// catalog identities, atomically with the approval" true by construction: there
// is no window in which the catalog may have changed between a procedural check
// and the recorded digest, and no way to record an approval for a roster the
// classifier would refuse. A digest-only approval is refused: a digest is not
// evidence, only its bytes are.
func recordDeliveryExemptionApproval(ctx context.Context, pool *pgxpool.Pool, class deliveryExemptionClass, rosterSHA, decision string, rows []deliveryExemptionRow, picks r2RetirePicks) (deliveryExemptionApproval, error) {
	var out deliveryExemptionApproval
	out.Class, out.RosterSHA, out.Decision = class, rosterSHA, decision
	if _, ok := deliveryExemptionDecisions[class]; !ok {
		return out, fmt.Errorf("unknown exemption class %q", class)
	}
	if !r2RetireLowerHex64(rosterSHA) {
		return out, errors.New("invalid roster SHA256")
	}
	if strings.TrimSpace(decision) == "" {
		return out, errors.New("an approval needs the owner's decision text")
	}
	if len(rows) == 0 {
		return out, errors.New("an approval needs the exact roster bytes; a digest alone is not evidence")
	}
	if err := deliveryClassHardClosed(class); err != nil {
		return out, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(context.Background())
	// The approval and the proof share one transaction: the catalog identities
	// validated are the ones the digest is recorded against, and a refusal here
	// records nothing.
	if _, _, _, _, err = checkDeliveryExemptionRoster(ctx, tx, class, rows, picks); err != nil {
		return out, fmt.Errorf("roster digest %s does not pass its own class validation, so it is not approved: %w", rosterSHA, err)
	}
	err = tx.QueryRow(ctx, `INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision)
 VALUES($1::r2_delivery_exemption_class,$2,$3) RETURNING id,created_at::text`, string(class), rosterSHA, decision).Scan(&out.ID, &out.CreatedAt)
	if err != nil {
		return out, fmt.Errorf("roster digest %s cannot be approved for %s: %w", rosterSHA, class, err)
	}
	return out, tx.Commit(ctx)
}

// seedDeliveryExemptions admits one class from a pinned roster in a single READ
// COMMITTED transaction: all rows or none. Every row's class is recomputed with
// the same live classifier the collated executor uses, and it must equal the
// requested class. Each row must sit on a stream with a positive non-KE owner
// decision. 0225's admission trigger checks each row's exact live page identity,
// its class shape and its own class/roster approval.
func seedDeliveryExemptions(ctx context.Context, pool *pgxpool.Pool, class deliveryExemptionClass, rows []deliveryExemptionRow, rosterSHA string, picks r2RetirePicks) (int64, error) {
	if _, ok := deliveryExemptionDecisions[class]; !ok {
		return 0, fmt.Errorf("unknown exemption class %q", class)
	}
	if err := deliveryClassHardClosed(class); err != nil {
		return 0, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	buckets, keys, etags, sizes, err := checkDeliveryExemptionRoster(ctx, tx, class, rows, picks)
	if err != nil {
		return 0, err
	}
	// Seeding CONSUMES the owner's approval; it never creates one. The approval
	// must already exist for exactly this class and roster digest, and its own
	// recorded decision text is what the rows carry.
	var approval deliveryExemptionApproval
	if err = tx.QueryRow(ctx, `SELECT id,class::text,decision FROM r2_delivery_exemption_approvals WHERE roster_sha256=$1`, rosterSHA).
		Scan(&approval.ID, &approval.Class, &approval.Decision); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("roster digest %s has no recorded owner approval; record it first with r2-inverse approve", rosterSHA)
		}
		return 0, err
	}
	if approval.Class != class {
		return 0, fmt.Errorf("roster digest %s is approved for %s, not %s; an approved roster can never be replayed", rosterSHA, approval.Class, class)
	}
	tag, err := tx.Exec(ctx, `INSERT INTO r2_delivery_exemptions(bucket,object_key,etag,size_bytes,class,approval_id,decision,roster_sha256)
 SELECT r.bucket,r.object_key,r.etag,r.size_bytes,$5::r2_delivery_exemption_class,$6,$7,$8
 FROM unnest($1::text[],$2::text[],$3::text[],$4::bigint[]) r(bucket,object_key,etag,size_bytes)`,
		buckets, keys, etags, sizes, string(class), approval.ID, approval.Decision, rosterSHA)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() != int64(len(rows)) {
		return 0, fmt.Errorf("seeded %d of %d roster rows", tag.RowsAffected(), len(rows))
	}
	return tag.RowsAffected(), tx.Commit(ctx)
}

// deliveryClassHardClosed refuses the classes that this repository cannot prove.
// No authoritative evidence issuer exists to show the pointer is unchanged or
// that a direct-from-R2 transfer ended, so these stay closed in code rather than
// trusting a caller not to pass a digest-shaped claim. outside_window needs no
// external fact and is not closed here.
func deliveryClassHardClosed(class deliveryExemptionClass) error {
	if class == deliveryExemptConfidentRepair || class == deliveryExemptInWindow {
		return fmt.Errorf("class %s is hard-closed: no authoritative evidence issuer exists in this repo, so it cannot be proven and stays closed", class)
	}
	return nil
}

// deliveryPageCollatedFamily reports whether key is in one of the two reviewed
// collated families that may carry a page exemption. joined/best14-20261021/ is
// eligible footage for the 2026-11-18 owner scope, but it is not a page family:
// no page row under it may take a page exemption, and the classification of a
// best14 output can never be evidence that its page key lost page protection.
func deliveryPageCollatedFamily(key string) bool {
	return strings.HasPrefix(key, "joined/goodplus-") ||
		strings.HasPrefix(key, "joined/nightly-collation-v2/")
}

// deliveryPageKindFits is the exact page kind and key family each exemption class
// covers, the same shape 0225's admission trigger enforces. A repair is a
// video_assembly under the confident-repair prefix; both window classes are a
// video_collated under exactly the two reviewed page families.
func deliveryPageKindFits(kind string, class deliveryExemptionClass, key string) bool {
	switch class {
	case deliveryExemptConfidentRepair:
		return kind == "video_assembly" && strings.HasPrefix(key, inverseConfidentRepairPrefix)
	case deliveryExemptOutsideWindow, deliveryExemptInWindow:
		return kind == "video_collated" && deliveryPageCollatedFamily(key)
	}
	return false
}

// deliveryPageWindowOutside proves an outside_window candidate is outside the
// pinned window of the PAGE RECORDING's own stream, not only the output's.
//
// The owner decision is "outside every window". A key that is outside its
// catalog output's stream window but inside the page stream's window is exactly
// the ambiguous case that decision does not cover, so it is refused rather than
// approved as outside_window. A page row whose stream has no pinned window, or
// no positive decision for it, cannot establish the class at all: the answer is
// fail-closed, never "assume outside".
func deliveryPageWindowOutside(picks r2RetirePicks, windows map[int64]api.Keep28Window, stream int64, start, end time.Time) error {
	if stream <= 0 {
		return errors.New("page recording names no stream")
	}
	w, ok := windows[stream]
	if !ok {
		return fmt.Errorf("page stream %d has no pinned keep window, so outside_window is unprovable", stream)
	}
	if !r2RetireValidSpan(w.Start, w.End) {
		return fmt.Errorf("page stream %d has an invalid pinned keep window", stream)
	}
	if !deliveryPageStreamDecided(picks, windows, stream) {
		return fmt.Errorf("page stream %d has no positive non-keep-everything decision", stream)
	}
	if start.Before(w.End) && end.After(w.Start) {
		return fmt.Errorf("output overlaps the page stream %d pinned window", stream)
	}
	return nil
}

// checkDeliveryExemptionRoster is the ONE roster decision, shared by seeding and
// by the read-only validate step. It reads the live catalog through the collated
// executor's own batch statement and derives every row's class with the
// executor's own assess function, so neither an approved roster nor an operator
// preview can see a weaker answer than the wire. It returns the column vectors
// the insert uses. It writes nothing.
//
// The checks, in order, are exactly why a mixed-source window and a
// keep-everything page stream cannot pass:
//   - the page recording's live stream must carry a positive, window-bound,
//     non-keep-everything owner decision;
//   - the roster ETag and size must equal the live page row's exact identity;
//   - the key must have a live collated or repair assessment with no reason;
//   - the output's own stream must also be positively decided;
//   - the classifier's class must equal the requested one. assessCollated marks
//     the output keep28 when ANY source overlaps a pinned window, so a mixed
//     source closure is in_window and can never be admitted as outside_window.
func checkDeliveryExemptionRoster(ctx context.Context, tx pgx.Tx, class deliveryExemptionClass, rows []deliveryExemptionRow, picks r2RetirePicks) (buckets, keys, etags []string, sizes []int64, err error) {
	if len(rows) == 0 {
		return nil, nil, nil, nil, errors.New("roster is empty")
	}
	_, windows, err := api.Keep28Policy()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	buckets, keys, etags, sizes = make([]string, len(rows)), make([]string, len(rows)), make([]string, len(rows)), make([]int64, len(rows))
	for n, row := range rows {
		buckets[n], keys[n], etags[n], sizes[n] = row.Bucket, row.ObjectKey, row.ETag, row.SizeBytes
	}
	// The page recording's stream and the output's own stream must both be
	// decided, so a keep-everything or undecided stream is refused before any
	// row is written. The page stream is read live, not from the roster.
	pageStreams := map[string]int64{}
	pageIdentities := map[string]inversePageIdentity{}
	// The page row's own sha256 is the content the reservation guard compares.
	// An exemption whose page row cannot state it would open a gate the wire then
	// refuses, so it is read here and required to equal the catalog's.
	pageSHA := map[string]string{}
	// The exact page kind and key family each roster key must be is a catalog
	// fact, read live here so a repair cannot be admitted as a collated video or
	// the reverse by naming the other class.
	pageKinds := map[string]string{}
	streamRows, err := tx.Query(ctx, `SELECT h.object_key,coalesce(rec.stream_id,0),h.etag,h.size_bytes,h.kind,h.sha256 FROM delivery_keep_holds h
 LEFT JOIN recordings rec ON rec.id=h.recording_id WHERE h.bucket=$1 AND h.object_key=ANY($2::text[])`, rows[0].Bucket, keys)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for streamRows.Next() {
		var key, sha string
		var stream, size int64
		var etag, kind string
		if err = streamRows.Scan(&key, &stream, &etag, &size, &kind, &sha); err != nil {
			streamRows.Close()
			return nil, nil, nil, nil, err
		}
		pageStreams[key] = stream
		pageIdentities[key] = inversePageIdentity{ETag: strings.Trim(etag, `"`), SizeBytes: size}
		pageKinds[key] = kind
		pageSHA[key] = strings.ToLower(sha)
	}
	err = streamRows.Err()
	streamRows.Close()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	protection, err := loadInverseProtectionMode(ctx, tx, rows[0].Bucket, picks, windows, true)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, row := range rows {
		if row.Bucket != rows[0].Bucket {
			return nil, nil, nil, nil, errors.New("a class roster must name exactly one bucket")
		}
		page, pageKnown := pageIdentities[row.ObjectKey]
		if !pageKnown || page.ETag != strings.Trim(row.ETag, `"`) || page.SizeBytes != row.SizeBytes {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s does not name its exact live page identity", row.ObjectKey)
		}
		if !deliveryPageStreamDecided(picks, windows, pageStreams[row.ObjectKey]) {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s has no positive non-keep-everything page stream decision", row.ObjectKey)
		}
		x, err := loadSeedAssessment(ctx, tx, rows[0].Bucket, row.ObjectKey, protection)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s has no live collated or repair assessment: %w", row.ObjectKey, err)
		}
		computed, known := deliveryExemptionClassOf(x, true)
		if !known {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s has no live collated or repair assessment", row.ObjectKey)
		}
		// The page row's exact kind and key family must match the requested class,
		// in SQL shape as well as in the classifier's answer.
		if !deliveryPageKindFits(pageKinds[row.ObjectKey], class, row.ObjectKey) {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s is page kind %q, which does not fit class %s", row.ObjectKey, pageKinds[row.ObjectKey], class)
		}
		// The catalog size must equal the page size and the roster size: a roster
		// or page that drifted from the catalog identity is not authority.
		if x.SizeBytes != page.SizeBytes || x.SizeBytes != row.SizeBytes {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s catalog size %d differs from its page/roster size", row.ObjectKey, x.SizeBytes)
		}
		// A KE folder component in the output's own stored path protects it, the
		// same rule execution applies before the wire.
		if protection.keepEverythingPathComponent(x.Path) || protection.keepEverythingPathComponent(row.ObjectKey) {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s has a keep-everything folder component", row.ObjectKey)
		}
		if x.Reason != "" {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s is held by the live classifier (%s)", row.ObjectKey, x.Reason)
		}
		// Content identity parity with the guard that will admit the receipt.
		// guard_delivery_r2_retirement requires the page row's sha256 to equal the
		// receipt's, and the collated lane writes the catalog output's sha256. A
		// page row with no sha256, or one that names a different object, is not
		// authority for this key: it would pass this seed and then be refused at
		// the wire, or worse, be read as covering content it does not name.
		if x.Kind == inverseNonrawKindCollated {
			if !r2RetireLowerHex64(x.SHA) {
				return nil, nil, nil, nil, fmt.Errorf("roster key %s has no exact catalog sha256", row.ObjectKey)
			}
			if pageSHA[row.ObjectKey] != strings.ToLower(x.SHA) {
				return nil, nil, nil, nil, fmt.Errorf("roster key %s page sha256 does not name its catalog content", row.ObjectKey)
			}
		}
		if x.StreamID > 0 && !deliveryPageStreamDecided(picks, windows, x.StreamID) {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s has no positive non-keep-everything decision for its own stream", row.ObjectKey)
		}
		if computed != class {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s is %s under the live classifier, not %s", row.ObjectKey, computed, class)
		}
		// The owner decision is "outside every window". A key outside its own
		// output stream's window but inside the PAGE stream's window is exactly
		// the ambiguity that decision does not cover, so it is refused rather
		// than approved under outside_window. A page stream with no pinned
		// window fails closed instead of being treated as outside.
		if class == deliveryExemptOutsideWindow && x.Kind == inverseNonrawKindCollated {
			if x.Start == nil || x.End == nil {
				return nil, nil, nil, nil, fmt.Errorf("roster key %s has no catalog interval, so outside_window is unprovable", row.ObjectKey)
			}
			if err := deliveryPageWindowOutside(picks, windows, pageStreams[row.ObjectKey], *x.Start, *x.End); err != nil {
				return nil, nil, nil, nil, fmt.Errorf("roster key %s is not provably outside: %w", row.ObjectKey, err)
			}
		}
	}
	return buckets, keys, etags, sizes, nil
}

// validateDeliveryExemptions is the read-only step that must run before the
// owner approves a roster digest. It applies the SAME decision seeding applies,
// in a read-only snapshot, and writes nothing: it records no approval, seeds no
// row and grants no authority. It exists because the documented roster SQL only
// generates candidates; whether a candidate passes is the classifier's answer,
// and this is that answer without the side effect.
func validateDeliveryExemptions(ctx context.Context, pool *pgxpool.Pool, class deliveryExemptionClass, rows []deliveryExemptionRow, picks r2RetirePicks) (int, error) {
	if _, ok := deliveryExemptionDecisions[class]; !ok {
		return 0, fmt.Errorf("unknown exemption class %q", class)
	}
	if err := deliveryClassHardClosed(class); err != nil {
		return 0, err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	if _, _, _, _, err = checkDeliveryExemptionRoster(ctx, tx, class, rows, picks); err != nil {
		return 0, err
	}
	// Prove the transaction really is read-only before reporting success.
	if _, err = tx.Exec(ctx, `SET TRANSACTION READ ONLY`); err != nil {
		return 0, err
	}
	return len(rows), tx.Rollback(ctx)
}

// runR2InverseApprove records the owner's approval for one roster, under exactly
// one class, before seeding. It is deliberately its own step: an approval
// carries the owner's decision text and is written by a human decision, whereas
// seeding may only consume it.
//
// It re-validates the exact roster bytes it is given against the same class and
// catalog identities in the approval's own transaction. It cannot record a digest
// for a roster the validator would refuse, and it cannot record a digest without
// its bytes, so a procedural "I validated it earlier" claim is not authority.
func runR2InverseApprove(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse approve", flag.ExitOnError)
	className := fs.String("class", "", "outside_window, confident_repair or in_window")
	roster := fs.String("roster", "", "pinned JSONL roster whose bytes are approved")
	rosterSHA := fs.String("expected-roster-sha256", "", "the exact roster digest being approved")
	picksPath := fs.String("picks", "", "owner picks")
	picksSHA := fs.String("expected-picks-sha256", "", "approved owner picks digest")
	decision := fs.String("decision", "", "the owner's decision text for this roster")
	const usage = "usage: stoaramactl r2-inverse approve --class CLASS --roster FILE --expected-roster-sha256 HEX --picks FILE --expected-picks-sha256 HEX --decision TEXT"
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *roster == "" || *picksPath == "" || *decision == "" {
		log.Fatal(usage)
	}
	class := deliveryExemptionClass(*className)
	if _, ok := deliveryExemptionDecisions[class]; !ok {
		log.Fatal(usage)
	}
	rows, err := readDeliveryExemptionRoster(*roster, *rosterSHA)
	if err != nil {
		log.Fatal(err)
	}
	picks, err := loadR2RetirePicks(*picksPath)
	if err != nil {
		log.Fatal(err)
	}
	if picks.SHA256 != *picksSHA {
		log.Fatal("owner picks differ from the approved digest")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	approval, err := recordDeliveryExemptionApproval(ctx, pool, class, *rosterSHA, *decision, rows, picks)
	if err != nil {
		log.Fatal(err)
	}
	data, _ := json.Marshal(approval)
	fmt.Println(string(data))
}

func runR2InverseExempt(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse exempt", flag.ExitOnError)
	className := fs.String("class", "", "outside_window, confident_repair or in_window")
	roster := fs.String("roster", "", "pinned JSONL roster: bucket, object_key, etag, size_bytes")
	rosterSHA := fs.String("expected-roster-sha256", "", "approved roster digest")
	picksPath := fs.String("picks", "", "owner picks")
	picksSHA := fs.String("expected-picks-sha256", "", "approved owner picks digest")
	const usage = "usage: stoaramactl r2-inverse exempt --class outside_window|confident_repair|in_window --roster FILE --expected-roster-sha256 HEX --picks FILE --expected-picks-sha256 HEX"
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *roster == "" || *picksPath == "" {
		log.Fatal(usage)
	}
	class := deliveryExemptionClass(*className)
	if _, ok := deliveryExemptionDecisions[class]; !ok {
		log.Fatal(usage)
	}
	rows, err := readDeliveryExemptionRoster(*roster, *rosterSHA)
	if err != nil {
		log.Fatal(err)
	}
	picks, err := loadR2RetirePicks(*picksPath)
	if err != nil {
		log.Fatal(err)
	}
	if picks.SHA256 != *picksSHA {
		log.Fatal("owner picks differ from the approved digest")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	seeded, err := seedDeliveryExemptions(ctx, pool, class, rows, *rosterSHA, picks)
	if err != nil {
		log.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"class": class, "roster_sha256": *rosterSHA, "seeded": seeded})
	fmt.Println(string(data))
}

// runR2InverseValidate is the read-only pre-approval check. The documented roster
// queries only GENERATE candidates; this applies the same live classifier the
// collated executor uses, so a mixed-source window, a keep-everything page
// stream or a key held for any other reason is refused before the owner is asked
// to approve a digest. It writes nothing, records no approval and grants no
// authority: approval and seeding remain separate human steps.
func runR2InverseValidate(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse validate", flag.ExitOnError)
	className := fs.String("class", "", "outside_window, confident_repair or in_window")
	roster := fs.String("roster", "", "candidate JSONL roster: bucket, object_key, etag, size_bytes")
	rosterSHA := fs.String("expected-roster-sha256", "", "candidate roster digest")
	picksPath := fs.String("picks", "", "owner picks")
	picksSHA := fs.String("expected-picks-sha256", "", "approved owner picks digest")
	const usage = "usage: stoaramactl r2-inverse validate --class outside_window|confident_repair|in_window --roster FILE --expected-roster-sha256 HEX --picks FILE --expected-picks-sha256 HEX (read-only; run before approve)"
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *roster == "" || *picksPath == "" {
		log.Fatal(usage)
	}
	class := deliveryExemptionClass(*className)
	if _, ok := deliveryExemptionDecisions[class]; !ok {
		log.Fatal(usage)
	}
	rows, err := readDeliveryExemptionRoster(*roster, *rosterSHA)
	if err != nil {
		log.Fatal(err)
	}
	picks, err := loadR2RetirePicks(*picksPath)
	if err != nil {
		log.Fatal(err)
	}
	if picks.SHA256 != *picksSHA {
		log.Fatal("owner picks differ from the approved digest")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	validated, err := validateDeliveryExemptions(ctx, pool, class, rows, picks)
	if err != nil {
		log.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"class": class, "roster_sha256": *rosterSHA, "validated": validated, "read_only": true, "approval_recorded": false})
	fmt.Println(string(data))
}
