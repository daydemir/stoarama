package main

import (
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
	"sort"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5"
)

var r2RetireIncidentSources = []int64{296368, 296406, 296453, 296637, 296684, 296727, 296769, 296844, 296891, 296940, 296985, 297022, 297109, 297152, 297204, 297248, 297292, 297335, 297379, 297419, 297469, 297516, 297559, 297602, 297638, 297684, 297734, 297859}

type r2HoldReleaseItem struct {
	ItemID      int64            `json:"item_id"`
	PlanID      int64            `json:"plan_id"`
	PlanSHA     string           `json:"plan_sha256"`
	Recovery    string           `json:"recovery"`
	RecoverySHA string           `json:"recovery_sha256"`
	ReceiptSHA  string           `json:"receipt_sha256"`
	ItemSHA     string           `json:"item_sha256"`
	Candidate   r2RetireItem     `json:"candidate"`
	Window      api.Keep28Window `json:"keep28"`
}
type r2HoldReleaseManifest struct {
	Version   int                 `json:"version"`
	Tool      string              `json:"tool"`
	CreatedAt time.Time           `json:"created_at"`
	PolicySHA string              `json:"policy_sha256"`
	PicksSHA  string              `json:"picks_sha256"`
	Items     []r2HoldReleaseItem `json:"items"`
	Bytes     int64               `json:"bytes"`
}

func r2SHA(raw []byte) string { s := sha256.Sum256(raw); return hex.EncodeToString(s[:]) }

// These are accepted NAS deletion receipts, not merely missing inventory rows.
// The exact ETag recovery identity is checked here and again by the DB guard.
const r2ReleaseReceiptsSQL = `SELECT i.id,p.id,p.plan_sha256,coalesce(i.item,p.manifest->(i.ordinal-1)),i.outcome,i.recovery,i.recovery_evidence
 FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id
 WHERE p.connection_id=13 AND p.account_id=47 AND i.state='deleted' AND i.delete_ever_issued
 AND i.recovery IN ('r2_sha_verified','r2_etag_bound') AND i.outcome->>'outcome'='deleted' ORDER BY i.id`

type r2ReleaseReceipt struct {
	itemID, planID   int64
	planSHA          string
	item, outcome    []byte
	recovery         string
	recoveryEvidence []byte
	kind             string
	id               int64
}

func r2HoldReleasePlan(ctx context.Context, q r2RetireQuerier, picks r2RetirePicks, policy string, windows map[int64]api.Keep28Window) (r2HoldReleaseManifest, error) {
	m := r2HoldReleaseManifest{Version: 1, Tool: "r2-retire release-holds", CreatedAt: time.Now().UTC(), PicksSHA: picks.SHA256, PolicySHA: policy}
	rows, err := q.Query(ctx, r2ReleaseReceiptsSQL)
	if err != nil {
		return m, err
	}
	var receipts []r2ReleaseReceipt
	ids := map[string][]int64{"raw": {}, "collated": {}}
	for rows.Next() {
		var r r2ReleaseReceipt
		var recovery string
		var evidence []byte
		if err := rows.Scan(&r.itemID, &r.planID, &r.planSHA, &r.item, &r.outcome, &recovery, &evidence); err != nil {
			rows.Close()
			return m, err
		}
		var item r2RetireItem
		if json.Unmarshal(r.item, &item) != nil || item.SourceID <= 0 || (item.Kind != "raw" && item.Kind != "collated") {
			rows.Close()
			return m, errors.New("invalid cleanup receipt identity")
		}
		if recovery == "r2_etag_bound" {
			var proof struct {
				Key      string    `json:"object_key"`
				SHA      string    `json:"sha256"`
				Size     int64     `json:"size_bytes"`
				ETag     string    `json:"etag"`
				Verified time.Time `json:"verified_at"`
			}
			if json.Unmarshal(evidence, &proof) != nil || proof.Key != item.ObjectKey || proof.SHA != item.SHA256 || proof.Size != item.SizeBytes || proof.ETag == "" || proof.Verified.IsZero() {
				continue
			}
		}
		r.recovery, r.recoveryEvidence = recovery, evidence
		r.kind, r.id = item.Kind, item.SourceID
		receipts = append(receipts, r)
		ids[r.kind] = append(ids[r.kind], r.id)
	}
	rows.Close()
	if rows.Err() != nil {
		return m, rows.Err()
	}
	candidates := map[string]r2RetireCandidate{}
	scope := r2RetireScope{AccountID: 47, ConnectionID: 13, Bucket: "stoarama", Grace: 7 * 24 * time.Hour, NASBacked: true}
	has, err := r2RetireHasRetirementsTable(ctx, q)
	if err != nil {
		return m, err
	}
	scope.HasRetirements = has
	for _, kind := range []string{"raw", "collated"} {
		if len(ids[kind]) == 0 {
			continue
		}
		sql := r2RetireSQL(kind, scope, true)
		sql = strings.Replace(sql, "=$5", "=ANY($5::bigint[])", 1)
		rows, err := q.Query(ctx, sql, scope.AccountID, scope.ConnectionID, scope.Bucket, int64(scope.Grace/time.Second), ids[kind])
		if err != nil {
			return m, err
		}
		for rows.Next() {
			c, err := r2RetireScanCandidate(kind, rows, true)
			if err != nil {
				rows.Close()
				return m, err
			}
			candidates[retireIdentity(kind, c.SourceID)] = c
		}
		rows.Close()
		if rows.Err() != nil {
			return m, rows.Err()
		}
	}
	// Protect incident sources and any collated file containing one, regardless
	// of which plan admitted it or what its current receipt state says.
	incident := map[int64]bool{}
	for _, id := range r2RetireIncidentSources {
		incident[id] = true
	}
	collatedIncident := map[int64]bool{}
	rows, err = q.Query(ctx, `SELECT id FROM recording_collation_outputs WHERE source_clip_ids && $1::bigint[]`, r2RetireIncidentSources)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return m, err
		}
		collatedIncident[id] = true
	}
	rows.Close()
	if rows.Err() != nil {
		return m, rows.Err()
	}
	for _, r := range receipts {
		c, ok := candidates[retireIdentity(r.kind, r.id)]
		if !ok || !api.NASListingPathValid(c.RelativePath) {
			continue
		}
		if c.Kind == "raw" && incident[c.SourceID] || c.Kind == "collated" && collatedIncident[c.SourceID] {
			continue
		}
		var item r2RetireItem
		if json.Unmarshal(r.item, &item) != nil {
			return m, errors.New("invalid receipt")
		}
		if c.Bucket != item.Bucket || c.ObjectKey != item.ObjectKey || c.SizeBytes != item.SizeBytes || c.SHA256 != item.SHA256 || c.RelativePath != item.RelativePath || c.StreamID != item.StreamID || c.RecordingID != item.RecordingID || !c.StartAt.Equal(item.StartAt) || !c.EndAt.Equal(item.EndAt) {
			continue
		}
		// Release authority never covers kept footage, even if exact NAS inventory
		// would permit a separate kept-none retirement.
		w, ok := windows[c.StreamID]
		if !ok || !r2RetireOutside(w, c.StartAt, c.EndAt) || !r2RetireOutside(w, c.SourceStart, c.SourceEnd) {
			continue
		}
		c.Facts = r2RetireFacts{Purged: c.Facts.Purged, Retired: c.Facts.Retired, NASAbsent: true}
		reason, choice, eligible := r2RetireDecideMode(c, scope.Bucket, windows, picks.Choices, true)
		if !eligible || reason != r2RetireReason {
			continue
		}
		m.Items = append(m.Items, r2HoldReleaseItem{ItemID: r.itemID, PlanID: r.planID, PlanSHA: r.planSHA, Recovery: r.recovery, RecoverySHA: r2SHA(r.recoveryEvidence), ReceiptSHA: r2SHA(r.outcome), ItemSHA: r2SHA(r.item), Candidate: r2RetireItem{r2RetireCandidate: c, Reason: reason, Choice: choice}, Window: w})
		m.Bytes += c.SizeBytes
	}
	return m, nil
}

func applyR2HoldReleases(ctx context.Context, tx pgx.Tx, m r2HoldReleaseManifest, picks r2RetirePicks, policy, digest, reason string, windows map[int64]api.Keep28Window) error {
	if m.Version != 1 || m.Tool != "r2-retire release-holds" || m.PolicySHA != policy || m.PicksSHA != picks.SHA256 || m.CreatedAt.After(time.Now().Add(time.Minute)) || time.Since(m.CreatedAt) > 24*time.Hour || strings.TrimSpace(reason) == "" {
		return errors.New("release authority differs or expired")
	}
	if err := api.LockNASCleanupR2Retirement(ctx, tx); err != nil {
		return err
	}
	ids := make([]int64, 0, len(m.Items))
	for _, i := range m.Items {
		ids = append(ids, i.ItemID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	rows, err := tx.Query(ctx, `SELECT id FROM nas_cleanup_items WHERE id=ANY($1) ORDER BY id FOR UPDATE`, ids)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	rows.Close()
	if rows.Err() != nil {
		return rows.Err()
	}
	live, err := r2HoldReleasePlan(ctx, tx, picks, policy, windows)
	if err != nil {
		return err
	}
	byID := map[int64]r2HoldReleaseItem{}
	for _, i := range live.Items {
		byID[i.ItemID] = i
	}
	seen := map[int64]bool{}
	var total int64
	for _, item := range m.Items {
		want, _ := json.Marshal(item)
		got, _ := json.Marshal(byID[item.ItemID])
		if seen[item.ItemID] || !bytes.Equal(want, got) {
			return fmt.Errorf("release item %d changed or is protected", item.ItemID)
		}
		seen[item.ItemID] = true
		total += item.Candidate.SizeBytes
	}
	if total != m.Bytes {
		return errors.New("release totals differ")
	}
	// File content is also re-read inside the transaction, immediately before
	// committing authority. A changed pick invalidates the entire operation.
	latest, err := loadR2RetirePicks(picks.Path)
	if err != nil || latest.SHA256 != m.PicksSHA {
		return errors.New("picks changed during release")
	}
	for _, item := range m.Items {
		authority, _ := json.Marshal(item)
		if _, err := tx.Exec(ctx, `INSERT INTO nas_cleanup_r2_releases(item_id,manifest_sha256,picks_sha256,policy_sha256,authority,reason) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, item.ItemID, digest, picks.SHA256, policy, authority, reason); err != nil {
			return err
		}
	}
	return nil
}
func runR2HoldReleases(ctx context.Context, cfg config.Config, args []string, policy string, windows map[int64]api.Keep28Window) {
	fs := flag.NewFlagSet("r2-retire release-holds", flag.ExitOnError)
	picksPath := fs.String("picks", "", "saved owner picks")
	out := fs.String("out", "", "new dry-run manifest path")
	manifest := fs.String("manifest", "", "apply exact release manifest")
	digest := fs.String("expected-sha256", "", "exact manifest digest")
	reason := fs.String("reason", "", "operator audit reason")
	apply := fs.Bool("apply", false, "commit hold releases (never deletes R2)")
	fs.Parse(args)
	if fs.NArg() != 0 || *picksPath == "" || (!*apply && *out == "") || (*apply && (*manifest == "" || !r2RetireLowerHex64(*digest) || *reason == "")) {
		log.Fatal("release-holds requires --picks and --out; apply requires --apply --manifest --expected-sha256 --reason")
	}
	picks, err := loadR2RetirePicks(*picksPath)
	if err != nil {
		log.Fatal(err)
	}
	pool, err := r2RetireOpenPool(ctx, cfg, !*apply)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if !*apply {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
		if err != nil {
			log.Fatal(err)
		}
		defer tx.Rollback(ctx)
		m, err := r2HoldReleasePlan(ctx, tx, picks, policy, windows)
		if err != nil {
			log.Fatal(err)
		}
		raw, _ := json.MarshalIndent(m, "", "  ")
		raw = append(raw, '\n')
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			log.Fatal(err)
		}
		if _, err = f.Write(raw); err == nil {
			err = f.Sync()
		}
		f.Close()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("release-holds DRY RUN items=%d bytes=%d usd_month=%.2f sha256=%s path=%s", len(m.Items), m.Bytes, r2RetireUSD(m.Bytes), r2SHA(raw), *out)
		return
	}
	raw, err := os.ReadFile(*manifest)
	if err != nil || r2SHA(raw) != *digest {
		log.Fatal("release manifest digest differs")
	}
	var m r2HoldReleaseManifest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&m); err != nil {
		log.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = applyR2HoldReleases(ctx, tx, m, picks, policy, *digest, *reason, windows); err != nil {
		log.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	log.Printf("release-holds APPLIED items=%d bytes=%d manifest=%s picks=%s policy=%s reason=%q", len(m.Items), m.Bytes, *digest, picks.SHA256, policy, *reason)
}
