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

// deliveryPageRetiredDecision is the owner decision the page retirement marker
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
 EXISTS(SELECT 1 FROM nas_cleanup_incident_roster i WHERE i.bucket=$1 AND i.object_key=$2)
 OR EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.bucket=$1 AND x.object_key=$2)
 OR EXISTS(SELECT 1 FROM nas_r2_copy_items ci WHERE (ci.state<>'verified' OR ci.blocked_reason<>'')
   AND ci.item->>'bucket'=$1 AND ci.item->>'object_key'=$2)
 OR EXISTS(SELECT 1 FROM r2_nonraw_intents ni WHERE ni.bucket=$1 AND ni.object_key=$2)
 OR EXISTS(SELECT 1 FROM r2_inverse_intents ri WHERE ri.bucket=$1 AND ri.object_key=$2)
 OR EXISTS(SELECT 1 FROM r2_keep28_intents ki WHERE ki.bucket=$1 AND ki.object_key=$2)
 OR EXISTS(SELECT 1 FROM r2_retirement_reservations rv WHERE rv.bucket=$1 AND rv.object_key=$2)
 OR EXISTS(SELECT 1 FROM nas_stitch_r2_input_holds sh WHERE sh.bucket=$1 AND sh.object_key=$2 AND sh.released_at IS NULL)
 OR r2_owner_keep_held($1,$2)`

// loadSeedAssessment loads one roster key's live assessment with its own page
// veto removed and no exemption consulted, so the seed sees exactly the class the
// collated executor would compute once the exemption exists. Every other rule
// (KE, sources, uncertainty, owner decision, pinned window) is the executor's.
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
	rows, err := q.Query(ctx, inverseNonrawBatchSQL+` WHERE o.object_key=$2`, bucket, key)
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
	// Replace the page-inclusive hold with the page-free one, then re-assess with
	// the executor's own function so no other liveness check is skipped.
	if x.Held, err = scanSeedHeld(ctx, q, bucket, key); err != nil {
		return x, err
	}
	x.Reason = ""
	return assessCollated(p, x), nil
}

func scanSeedHeld(ctx context.Context, q r2RetireQuerier, bucket, key string) (bool, error) {
	var held bool
	err := q.QueryRow(ctx, inverseSeedHeldSQL, bucket, key).Scan(&held)
	return held, err
}

// seedDeliveryExemptions admits one class from a pinned roster in a single READ
// COMMITTED transaction: all rows or none. Every row's class is recomputed with
// the same live classifier the collated executor uses, and it must equal the
// requested class. Each row must sit on a stream with a positive non-KE owner
// decision. 0225's admission trigger checks each row's exact live page identity,
// its class shape and its own class/roster approval.
func seedDeliveryExemptions(ctx context.Context, pool *pgxpool.Pool, class deliveryExemptionClass, rows []deliveryExemptionRow, rosterSHA string, picks r2RetirePicks) (int64, error) {
	decision, ok := deliveryExemptionDecisions[class]
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
	var approvalID int64
	if err = tx.QueryRow(ctx, `INSERT INTO r2_delivery_exemption_approvals(class,roster_sha256,decision)
 VALUES($1::r2_delivery_exemption_class,$2,$3) ON CONFLICT(class,roster_sha256) DO NOTHING RETURNING id`, string(class), rosterSHA, decision).Scan(&approvalID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
		if err = tx.QueryRow(ctx, `SELECT id FROM r2_delivery_exemption_approvals WHERE class=$1::r2_delivery_exemption_class AND roster_sha256=$2 AND decision=$3`,
			string(class), rosterSHA, decision).Scan(&approvalID); err != nil {
			return 0, fmt.Errorf("approved roster digest %s was recorded for another class or decision: %w", rosterSHA, err)
		}
	}
	tag, err := tx.Exec(ctx, `INSERT INTO r2_delivery_exemptions(bucket,object_key,etag,size_bytes,class,approval_id,decision,roster_sha256)
 SELECT r.bucket,r.object_key,r.etag,r.size_bytes,$5::r2_delivery_exemption_class,$6,$7,$8
 FROM unnest($1::text[],$2::text[],$3::text[],$4::bigint[]) r(bucket,object_key,etag,size_bytes)`,
		buckets, keys, etags, sizes, string(class), approvalID, decision, rosterSHA)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() != int64(len(rows)) {
		return 0, fmt.Errorf("seeded %d of %d roster rows", tag.RowsAffected(), len(rows))
	}
	return tag.RowsAffected(), tx.Commit(ctx)
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

// runR2InversePageRetired records that the old delivery page is down. The marker
// is inert until the serving generation it names is recorded disabled or
// retired, and this command refuses to write it unless that is already true: a
// marker can never be what makes the page down. It opens the confident_repair
// gate and half of the in_window gate, once, for good.
func runR2InversePageRetired(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("r2-inverse page-retired", flag.ExitOnError)
	generation := fs.String("generation", "", "the retired serving registry generation (32 hex)")
	registryState := fs.String("registry-state", "", "disabled or retired: the generation's live serving state")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || !r2RetireLowerHex32(*generation) || (*registryState != "disabled" && *registryState != "retired") {
		log.Fatal("usage: stoaramactl r2-inverse page-retired --generation HEX32 --registry-state disabled|retired (run once, after the old delivery page is taken down)")
	}
	pool := mustOpenPool(ctx, cfg)
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	// Record the live serving state first, so the marker always names a
	// generation whose disarm is already durable and append-only.
	if _, err = tx.Exec(ctx, `INSERT INTO r2_delivery_page_registry_events(generation,state,decision) VALUES($1,$2::r2_delivery_page_registry_state,$3)`,
		*generation, *registryState, deliveryPageRetiredDecision); err != nil {
		log.Fatal(fmt.Errorf("record the serving registry state first: %w", err))
	}
	var at string
	if err = tx.QueryRow(ctx, `INSERT INTO r2_delivery_page_retirement(generation,decision) VALUES($1,$2) RETURNING retired_at::text`,
		*generation, deliveryPageRetiredDecision).Scan(&at); err != nil {
		log.Fatal(err)
	}
	// The same live function every gate reads: refuse to leave a marker that the
	// page is still serving under.
	var retired bool
	if err = tx.QueryRow(ctx, `SELECT r2_delivery_page_retired()`).Scan(&retired); err != nil || !retired {
		log.Fatal("the serving registry generation is not disabled; refusing the retirement marker")
	}
	if err = tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"page_retired_at": at, "generation": *generation, "registry_state": *registryState})
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
