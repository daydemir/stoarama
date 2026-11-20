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
func recordDeliveryExemptionApproval(ctx context.Context, pool *pgxpool.Pool, class deliveryExemptionClass, rosterSHA, decision string) (deliveryExemptionApproval, error) {
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
	err := pool.QueryRow(ctx, `INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision)
 VALUES($1::r2_delivery_exemption_class,$2,$3) RETURNING id,created_at::text`, string(class), rosterSHA, decision).Scan(&out.ID, &out.CreatedAt)
	if err != nil {
		return out, fmt.Errorf("roster digest %s cannot be approved for %s: %w", rosterSHA, class, err)
	}
	return out, nil
}

// seedDeliveryExemptions admits one class from a pinned roster in a single READ
// COMMITTED transaction: all rows or none. Every row's class is recomputed with
// the same live classifier the collated executor uses, and it must equal the
// requested class. Each row must sit on a stream with a positive non-KE owner
// decision. 0225's admission trigger checks each row's exact live page identity,
// its class shape and its own class/roster approval.
func seedDeliveryExemptions(ctx context.Context, pool *pgxpool.Pool, class deliveryExemptionClass, rows []deliveryExemptionRow, rosterSHA string, picks r2RetirePicks) (int64, error) {
	_, ok := deliveryExemptionDecisions[class]
	if !ok {
		return 0, fmt.Errorf("unknown exemption class %q", class)
	}
	_, windows, err := api.Keep28Policy()
	if err != nil {
		return 0, err
	}
	buckets, keys, etags, sizes := make([]string, len(rows)), make([]string, len(rows)), make([]string, len(rows)), make([]int64, len(rows))
	for n, row := range rows {
		buckets[n], keys[n], etags[n], sizes[n] = row.Bucket, row.ObjectKey, row.ETag, row.SizeBytes
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())
	// The page recording's stream and the output's own stream must both be
	// decided, so a keep-everything or undecided stream is refused before any
	// row is written. The page stream is read live, not from the roster.
	pageStreams := map[string]int64{}
	pageIdentities := map[string]inversePageIdentity{}
	streamRows, err := tx.Query(ctx, `SELECT h.object_key,coalesce(rec.stream_id,0),h.etag,h.size_bytes FROM delivery_keep_holds h
 LEFT JOIN recordings rec ON rec.id=h.recording_id WHERE h.bucket=$1 AND h.object_key=ANY($2::text[])`, rows[0].Bucket, keys)
	if err != nil {
		return 0, err
	}
	for streamRows.Next() {
		var key string
		var stream, size int64
		var etag string
		if err = streamRows.Scan(&key, &stream, &etag, &size); err != nil {
			streamRows.Close()
			return 0, err
		}
		pageStreams[key] = stream
		pageIdentities[key] = inversePageIdentity{ETag: strings.Trim(etag, `"`), SizeBytes: size}
	}
	err = streamRows.Err()
	streamRows.Close()
	if err != nil {
		return 0, err
	}
	protection, err := loadInverseProtectionMode(ctx, tx, rows[0].Bucket, picks, windows, true)
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if row.Bucket != rows[0].Bucket {
			return 0, errors.New("a class roster must name exactly one bucket")
		}
		if !deliveryPageStreamDecided(picks, windows, pageStreams[row.ObjectKey]) {
			return 0, fmt.Errorf("roster key %s has no positive non-keep-everything page stream decision", row.ObjectKey)
		}
		page, pageKnown := pageIdentities[row.ObjectKey]
		if !pageKnown || page.ETag != strings.Trim(row.ETag, `"`) || page.SizeBytes != row.SizeBytes {
			return 0, fmt.Errorf("roster key %s does not name its exact live page identity", row.ObjectKey)
		}
		x, err := loadSeedAssessment(ctx, tx, rows[0].Bucket, row.ObjectKey, protection)
		if err != nil {
			return 0, fmt.Errorf("roster key %s has no live collated or repair assessment: %w", row.ObjectKey, err)
		}
		computed, known := deliveryExemptionClassOf(x, true)
		if !known {
			return 0, fmt.Errorf("roster key %s has no live collated or repair assessment", row.ObjectKey)
		}
		if x.Reason != "" {
			return 0, fmt.Errorf("roster key %s is held by the live classifier (%s)", row.ObjectKey, x.Reason)
		}
		if x.StreamID > 0 && !deliveryPageStreamDecided(picks, windows, x.StreamID) {
			return 0, fmt.Errorf("roster key %s has no positive non-keep-everything decision for its own stream", row.ObjectKey)
		}
		if computed != class {
			return 0, fmt.Errorf("roster key %s is %s under the live classifier, not %s", row.ObjectKey, computed, class)
		}
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

// runR2InverseApprove records the owner's approval for one roster digest, under
// exactly one class, before seeding. It is deliberately its own step: an approval
// carries the owner's decision text and is written by a human decision, whereas
// seeding may only consume it.
func runR2InverseApprove(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse approve", flag.ExitOnError)
	className := fs.String("class", "", "outside_window, confident_repair or in_window")
	rosterSHA := fs.String("roster-sha256", "", "the exact roster digest being approved")
	decision := fs.String("decision", "", "the owner's decision text for this roster")
	const usage = "usage: stoaramactl r2-inverse approve --class CLASS --roster-sha256 HEX --decision TEXT"
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *decision == "" {
		log.Fatal(usage)
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	approval, err := recordDeliveryExemptionApproval(ctx, pool, deliveryExemptionClass(*className), *rosterSHA, *decision)
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

// retireDeliveryPage sets the committed serving state inside tx, then proves with
// the same live function every serving route and every gate reads that the page is
// down. It refuses while any issued download capability is still unexpired, so the
// outstanding tickets are drained rather than left live behind a retired page.
func retireDeliveryPage(ctx context.Context, tx pgx.Tx, generation, decision string) (string, error) {
	if !r2RetireLowerHex32(generation) || strings.TrimSpace(decision) == "" {
		return "", errors.New("a retirement needs its serving generation and the owner's decision")
	}
	// Serialize against ticket minting: after this exclusive lock, no new public
	// capability can be recorded, so the drain below is complete.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, 7343226011104); err != nil {
		return "", err
	}
	var live int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM r2_delivery_download_tickets WHERE expires_at>clock_timestamp()`).Scan(&live); err != nil {
		return "", err
	}
	if live != 0 {
		return "", fmt.Errorf("refusing to retire the page while %d issued download tickets are still unexpired; wait for them to expire", live)
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
