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

// deliveryPageRetiredDecision is the owner decision the page retirement row
// records. The controller sets it once, after the old page is taken down.
const deliveryPageRetiredDecision = `Deniz 2026-11-19: "File list only": the old delivery page is taken down and its R2 registry retired once the in-window videos are certified on the NAS and the static CSV+JSON manifest of old links to NAS paths is published.`

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
	streamRows, err := tx.Query(ctx, `SELECT h.object_key,coalesce(rec.stream_id,0),h.etag,h.size_bytes FROM delivery_keep_holds h
 LEFT JOIN recordings rec ON rec.id=h.recording_id WHERE h.bucket=$1 AND h.object_key=ANY($2::text[])`, rows[0].Bucket, keys)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for streamRows.Next() {
		var key string
		var stream, size int64
		var etag string
		if err = streamRows.Scan(&key, &stream, &etag, &size); err != nil {
			streamRows.Close()
			return nil, nil, nil, nil, err
		}
		pageStreams[key] = stream
		pageIdentities[key] = inversePageIdentity{ETag: strings.Trim(etag, `"`), SizeBytes: size}
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
		if x.Reason != "" {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s is held by the live classifier (%s)", row.ObjectKey, x.Reason)
		}
		if x.StreamID > 0 && !deliveryPageStreamDecided(picks, windows, x.StreamID) {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s has no positive non-keep-everything decision for its own stream", row.ObjectKey)
		}
		if computed != class {
			return nil, nil, nil, nil, fmt.Errorf("roster key %s is %s under the live classifier, not %s", row.ObjectKey, computed, class)
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

// retireDeliveryPage commits the ONE destructive predicate inside tx, after the
// SQL trigger has independently proved quiesce, reader drain and capability
// drain. It takes the exclusive public lock first, so no ticket, no admission and
// no registry publication can interleave between the proofs and the row, then
// proves with the same live function every serving route and every gate reads
// that the page is down.
func retireDeliveryPage(ctx context.Context, tx pgx.Tx, generation, decision string) (string, error) {
	if !r2RetireLowerHex32(generation) || strings.TrimSpace(decision) == "" {
		return "", errors.New("a retirement needs its serving generation and the owner's decision")
	}
	// Serialize against ticket minting, reader admission, publication and this
	// same command. After this exclusive lock no new public capability can be
	// recorded and no new reader admitted, so the proof below is complete.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(r2_delivery_public_lock())`); err != nil {
		return "", err
	}
	// Read the same proof the SQL trigger reads, so the operator sees WHY before
	// the trigger's exception. These counts are diagnostics, never authority: the
	// trigger alone decides, and it requires the cutover authority and its derived
	// deadline even when every count below is zero.
	var armed, liveReaders, liveTickets, hasAuthority, deadlinePassed int64
	if err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM r2_delivery_page_quiesce),
 (SELECT count(*) FROM r2_delivery_public_readers WHERE expires_at>clock_timestamp()),
 (SELECT count(*) FROM r2_delivery_download_tickets WHERE expires_at>clock_timestamp()),
 (SELECT count(*) FROM r2_delivery_cutover_authority WHERE singleton),
 (SELECT count(*) FROM r2_delivery_cutover_authority WHERE singleton AND clock_timestamp()>deadline_at)`).
		Scan(&armed, &liveReaders, &liveTickets, &hasAuthority, &deadlinePassed); err != nil {
		return "", err
	}
	if armed == 0 {
		return "", errors.New("refusing to retire the page: it is not quiesced; run r2-inverse page-quiesce first")
	}
	if hasAuthority == 0 {
		return "", errors.New("refusing to retire the page: no reviewed cutover authority; run r2-inverse page-cutover after proving the legacy origins gone, the caches expired and the transfers ended")
	}
	if deadlinePassed == 0 {
		return "", errors.New("refusing to retire the page: the cutover authority's verified drain deadline has not passed yet")
	}
	if liveReaders != 0 {
		return "", fmt.Errorf("refusing to retire the page while %d admitted public readers are still unexpired; retry after their leases expire", liveReaders)
	}
	if liveTickets != 0 {
		return "", fmt.Errorf("refusing to retire the page while %d download capabilities are still unexpired; retry after their expiry", liveTickets)
	}
	var at string
	if err := tx.QueryRow(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,$2) RETURNING retired_at::text`,
		generation, decision).Scan(&at); err != nil {
		return "", err
	}
	var retired bool
	if err := tx.QueryRow(ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || !retired {
		return "", errors.New("the retirement row did not take effect; refusing to report the page down")
	}
	return at, nil
}

// quiesceDeliveryPage commits the staging state that stops every NEW public
// reader. It is not the page going down: no exemption class opens, the page row
// is untouched, and the only effect is that new public requests are refused with
// 503 while the readers already admitted finish. It is the point of no return for
// rollback, so the runbook treats it as arming.
func quiesceDeliveryPage(ctx context.Context, tx pgx.Tx, reason string) (string, error) {
	if strings.TrimSpace(reason) == "" {
		return "", errors.New("quiescing the page needs its reason and owner decision")
	}
	// The exclusive lock makes arming atomic against admission and tickets, so
	// no reader or capability can slip in after the state commits.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(r2_delivery_public_lock())`); err != nil {
		return "", err
	}
	var at string
	if err := tx.QueryRow(ctx, `INSERT INTO r2_delivery_page_quiesce(reason) VALUES($1) RETURNING armed_at::text`, reason).Scan(&at); err != nil {
		return "", err
	}
	var serving bool
	if err := tx.QueryRow(ctx, `SELECT r2_delivery_public_serving()`).Scan(&serving); err != nil || serving {
		return "", errors.New("the quiesce row did not take effect; refusing to report the page armed")
	}
	return at, nil
}

// deliveryCutoverEvidence is the reviewed external cutover record an operator
// establishes outside this database. Every field is a fact, not a claim derived
// from the API's own traffic, and the whole record is pinned by approved_sha256
// so the approval names exact bytes.
type deliveryCutoverEvidence struct {
	Generation              string    `json:"generation"`
	Decision                string    `json:"decision"`
	LegacyGrantBoundaryAt   time.Time `json:"legacy_grant_boundary_at"`
	LegacySignedGrantSHA256 string    `json:"legacy_signed_grant_sha256"`
	LegacyOriginRemovedAt   time.Time `json:"legacy_origin_removed_at"`
	LegacyOriginEvidenceSHA string    `json:"legacy_origin_evidence_sha256"`
	CacheExpiredAt          time.Time `json:"cache_expired_at"`
	CacheEvidenceSHA256     string    `json:"cache_evidence_sha256"`
	RollbackRoutingSHA256   string    `json:"rollback_routing_sha256"`
	TransferBoundAt         time.Time `json:"transfer_bound_at"`
}

// canonical is the exact byte sequence approved_sha256 covers: sorted keys,
// RFC3339 UTC timestamps. It is what the operator reviews and pins.
func (e deliveryCutoverEvidence) canonical() ([]byte, error) {
	type doc struct {
		Generation              string `json:"generation"`
		Decision                string `json:"decision"`
		LegacyGrantBoundaryAt   string `json:"legacy_grant_boundary_at"`
		LegacySignedGrantSHA256 string `json:"legacy_signed_grant_sha256"`
		LegacyOriginRemovedAt   string `json:"legacy_origin_removed_at"`
		LegacyOriginEvidenceSHA string `json:"legacy_origin_evidence_sha256"`
		CacheExpiredAt          string `json:"cache_expired_at"`
		CacheEvidenceSHA256     string `json:"cache_evidence_sha256"`
		RollbackRoutingSHA256   string `json:"rollback_routing_sha256"`
		TransferBoundAt         string `json:"transfer_bound_at"`
	}
	return json.Marshal(doc{e.Generation, e.Decision,
		e.LegacyGrantBoundaryAt.UTC().Format(time.RFC3339), e.LegacySignedGrantSHA256,
		e.LegacyOriginRemovedAt.UTC().Format(time.RFC3339), e.LegacyOriginEvidenceSHA,
		e.CacheExpiredAt.UTC().Format(time.RFC3339), e.CacheEvidenceSHA256,
		e.RollbackRoutingSHA256, e.TransferBoundAt.UTC().Format(time.RFC3339)})
}

// recordDeliveryCutover writes the reviewed cutover authority for one generation,
// with its digest checked against the approval. It refuses an incomplete record,
// a future bound (a transfer cannot have ended after the evidence was written),
// or any field whose digest does not match the approved bytes. The derived
// deadline is computed by SQL, not here.
func recordDeliveryCutover(ctx context.Context, pool *pgxpool.Pool, e deliveryCutoverEvidence, approvedSHA string) (map[string]any, error) {
	if !r2RetireLowerHex32(e.Generation) || strings.TrimSpace(e.Decision) == "" {
		return nil, errors.New("cutover authority needs the serving generation and the owner decision")
	}
	for _, h := range []string{e.LegacySignedGrantSHA256, e.LegacyOriginEvidenceSHA, e.CacheEvidenceSHA256, e.RollbackRoutingSHA256} {
		if !r2RetireLowerHex64(h) {
			return nil, errors.New("cutover authority needs a 64-hex digest for every evidence field")
		}
	}
	canonical, err := e.canonical()
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	if !r2RetireLowerHex64(approvedSHA) || hex.EncodeToString(sum[:]) != approvedSHA {
		return nil, errors.New("cutover evidence differs from its approved digest; pin the exact bytes the operator reviewed")
	}
	if e.TransferBoundAt.After(time.Now().UTC()) || e.LegacyOriginRemovedAt.After(time.Now().UTC()) || e.CacheExpiredAt.After(time.Now().UTC()) {
		return nil, errors.New("legacy grant, origin, cache and transfer facts must already be in the past")
	}
	var at, deadline string
	err = pool.QueryRow(ctx, `INSERT INTO r2_delivery_cutover_authority(
 generation,decision,legacy_grant_boundary_at,legacy_signed_grant_sha256,legacy_origin_removed_at,
 legacy_origin_evidence_sha256,cache_expired_at,cache_evidence_sha256,rollback_routing_sha256,transfer_bound_at,approved_sha256,deadline_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'1970-01-01T00:00:00Z')
 RETURNING recorded_at::text,deadline_at::text`,
		e.Generation, e.Decision, e.LegacyGrantBoundaryAt, e.LegacySignedGrantSHA256,
		e.LegacyOriginRemovedAt, e.LegacyOriginEvidenceSHA, e.CacheExpiredAt,
		e.CacheEvidenceSHA256, e.RollbackRoutingSHA256, e.TransferBoundAt, approvedSHA).Scan(&at, &deadline)
	if err != nil {
		return nil, fmt.Errorf("cutover authority refused: %w", err)
	}
	return map[string]any{"recorded_at": at, "deadline_at": deadline, "approved_sha256": approvedSHA, "generation": e.Generation}, nil
}

func runR2InversePageCutover(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse page-cutover", flag.ExitOnError)
	evidencePath := fs.String("evidence", "", "reviewed cutover evidence JSON")
	evidenceSHA := fs.String("expected-evidence-sha256", "", "the approved canonical evidence digest")
	const usage = "usage: stoaramactl r2-inverse page-cutover --evidence FILE --expected-evidence-sha256 HEX (record reviewed cutover authority; run after page-quiesce)"
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *evidencePath == "" || !r2RetireLowerHex64(*evidenceSHA) {
		log.Fatal(usage)
	}
	raw, err := os.ReadFile(*evidencePath)
	if err != nil {
		log.Fatal(err)
	}
	var e deliveryCutoverEvidence
	if err = json.Unmarshal(raw, &e); err != nil {
		log.Fatal(err)
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	out, err := recordDeliveryCutover(ctx, pool, e, *evidenceSHA)
	if err != nil {
		log.Fatal(err)
	}
	data, _ := json.Marshal(out)
	fmt.Println(string(data))
}

// runR2InversePageQuiesce arms retirement. It is a separate, deliberate step so
// the runbook can prove the legacy origins are gone and the last five-minute URL
// has drained before the destructive row can exist at all.
func runR2InversePageQuiesce(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse page-quiesce", flag.ExitOnError)
	reason := fs.String("reason", "", "why the page is being armed for retirement, and the owner decision")
	const usage = "usage: stoaramactl r2-inverse page-quiesce --reason TEXT (arm retirement; stop admitting new public readers; not reversible)"
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || strings.TrimSpace(*reason) == "" {
		log.Fatal(usage)
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	at, err := quiesceDeliveryPage(ctx, tx, *reason)
	if err != nil {
		log.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"page_quiesced_at": at, "reason": *reason})
	fmt.Println(string(data))
}

// runR2InversePageRetired sets the committed serving state that takes the old
// public delivery page down. Every public delivery route reads that state live,
// so this command is the page going down, not a claim about it.
func runR2InversePageRetired(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse page-retired", flag.ExitOnError)
	generation := fs.String("generation", "", "the serving registry generation being retired (32 hex)")
	decision := fs.String("decision", "", "the owner's decision text taking the page down")
	const usage = "usage: stoaramactl r2-inverse page-retired --generation HEX32 --decision TEXT (run once, after the old delivery page is taken down)"
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !r2RetireLowerHex32(*generation) || strings.TrimSpace(*decision) == "" {
		log.Fatal(usage)
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	at, err := retireDeliveryPage(ctx, tx, *generation, *decision)
	if err != nil {
		log.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"page_retired_at": at, "generation": *generation, "decision": *decision})
	fmt.Println(string(data))
}

// r2RetireLowerHex32 accepts a lower-case 32-character hex generation id.
func r2RetireLowerHex32(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
