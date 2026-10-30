package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	nasSelectedVerifyAccountID    int64 = 47
	nasSelectedVerifyConnectionID int64 = 13
)

var nasSelectedVerifyLabelRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,79}$`)

type nasSelectedVerifyOptions struct {
	clipIDs, collatedIDs, joinedIDs []int64
	label, expectedSHA              string
	dryRun, asJSON                  bool
}
type nasSelectedVerifyManifestItem struct {
	Kind         string `json:"kind"`
	SourceID     int64  `json:"source_id"`
	RelativePath string `json:"relative_path"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
	R2Source     string `json:"r2_source"`
}
type nasSelectedVerifySummary struct {
	Label        string `json:"label"`
	AccountID    int64  `json:"account_id"`
	ConnectionID int64  `json:"connection_id"`
	PlanSHA256   string `json:"plan_sha256"`
	Items        int    `json:"items"`
	Bytes        int64  `json:"bytes"`
	DryRun       bool   `json:"dry_run"`
}

func parseNASSelectedVerifyRequestArgs(args []string) (nasSelectedVerifyOptions, error) {
	var o nasSelectedVerifyOptions
	fs := flag.NewFlagSet("nas-selected-verify request", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	ids := fs.String("clip-ids", "", "comma-separated raw clip IDs")
	collated := fs.String("collated-ids", "", "comma-separated collated output IDs")
	joined := fs.String("joined-ids", "", "comma-separated joined media artifact IDs")
	fs.StringVar(&o.label, "label", "", "immutable plan label")
	fs.StringVar(&o.expectedSHA, "expected-plan-sha256", "", "required on apply")
	fs.BoolVar(&o.dryRun, "dry-run", false, "print plan without inserting")
	fs.BoolVar(&o.asJSON, "json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if len(fs.Args()) != 0 {
		return o, errors.New("unexpected arguments")
	}
	for _, selection := range []struct {
		raw string
		dst *[]int64
	}{{*ids, &o.clipIDs}, {*collated, &o.collatedIDs}, {*joined, &o.joinedIDs}} {
		if selection.raw == "" {
			continue
		}
		seen := map[int64]bool{}
		for _, raw := range strings.Split(selection.raw, ",") {
			id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
			if err != nil || id <= 0 || seen[id] {
				return o, fmt.Errorf("invalid or duplicate source id %q", raw)
			}
			seen[id] = true
			*selection.dst = append(*selection.dst, id)
		}
	}
	count := len(o.clipIDs) + len(o.collatedIDs) + len(o.joinedIDs)
	if count < 1 || count > 10000 {
		return o, errors.New("select 1 to 10000 IDs with --clip-ids, --collated-ids or --joined-ids")
	}
	if !nasSelectedVerifyLabelRE.MatchString(o.label) {
		return o, errors.New("invalid --label")
	}
	if o.expectedSHA != "" && (len(o.expectedSHA) != 64 || !isLowerHex(o.expectedSHA)) {
		return o, errors.New("invalid --expected-plan-sha256")
	}
	if !o.dryRun && o.expectedSHA == "" {
		return o, errors.New("--expected-plan-sha256 is required unless --dry-run")
	}
	return o, nil
}
func isLowerHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil && strings.ToLower(s) == s
}

func validateNASSelectedVerifyIdentity(i nasSelectedVerifyManifestItem) error {
	p := i.RelativePath
	if p == "" || strings.TrimSpace(p) != p || path.IsAbs(p) || path.Clean(p) != p || strings.Contains(p, "\\") || strings.ContainsAny(p, "\x00\r\n") || i.SizeBytes <= 0 || i.SizeBytes > 32<<30 || len(i.SHA256) != 64 || !isLowerHex(i.SHA256) || i.R2Source == "" {
		return errors.New("incomplete canonical identity")
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".." || part == "." || part == "" {
			return errors.New("unsafe canonical path")
		}
	}
	first := strings.Split(p, "/")[0]
	if first == "#recycle" || ((first == "joined") != (i.Kind != "raw")) {
		return errors.New("canonical path does not match kind")
	}
	return nil
}

func requestNASSelectedVerify(ctx context.Context, pool *pgxpool.Pool, o nasSelectedVerifyOptions) (nasSelectedVerifySummary, error) {
	sum := nasSelectedVerifySummary{Label: o.label, AccountID: nasSelectedVerifyAccountID, ConnectionID: nasSelectedVerifyConnectionID, DryRun: o.dryRun}
	items := []nasSelectedVerifyManifestItem{}
	for _, selection := range []struct {
		kind, query string
		ids         []int64
	}{
		{"raw", `SELECT c.id,c.display_path,c.size_bytes,lower(c.sha256),c.object_key FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE r.account_id=$1 AND c.id=ANY($2) ORDER BY c.id`, o.clipIDs},
		{"collated", `SELECT o.id,'joined/'||o.nas_relative_path,o.size_bytes,lower(o.sha256),o.object_key FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id WHERE r.account_id=$1 AND o.id=ANY($2) ORDER BY o.id`, o.collatedIDs},
		{"joined", `SELECT id,'joined/'||batch_id||'/'||relative_path,expected_size_bytes,expected_sha256,object_key FROM recording_joined_artifacts WHERE account_id=$1 AND connection_id=13 AND artifact_kind='media' AND id=ANY($2) ORDER BY id`, o.joinedIDs},
	} {
		if len(selection.ids) == 0 {
			continue
		}
		rows, err := pool.Query(ctx, selection.query, nasSelectedVerifyAccountID, selection.ids)
		if err != nil {
			return sum, err
		}
		count := 0
		for rows.Next() {
			i := nasSelectedVerifyManifestItem{Kind: selection.kind}
			if err := rows.Scan(&i.SourceID, &i.RelativePath, &i.SizeBytes, &i.SHA256, &i.R2Source); err != nil {
				rows.Close()
				return sum, err
			}
			if err := validateNASSelectedVerifyIdentity(i); err != nil {
				rows.Close()
				return sum, fmt.Errorf("%s %d: %w", i.Kind, i.SourceID, err)
			}
			items = append(items, i)
			sum.Bytes += i.SizeBytes
			count++
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return sum, err
		}
		if count != len(selection.ids) {
			return sum, fmt.Errorf("selected %d distinct account %s items, expected %d", count, selection.kind, len(selection.ids))
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Kind != items[j].Kind {
			return items[i].Kind < items[j].Kind
		}
		return items[i].SourceID < items[j].SourceID
	})
	seenPaths := map[string]bool{}
	for _, i := range items {
		if seenPaths[i.RelativePath] {
			return sum, fmt.Errorf("duplicate NAS path %s", i.RelativePath)
		}
		seenPaths[i.RelativePath] = true
	}
	manifest, err := json.Marshal(items)
	if err != nil {
		return sum, err
	}
	digest := sha256.Sum256(manifest)
	sum.PlanSHA256 = hex.EncodeToString(digest[:])
	sum.Items = len(items)
	if o.dryRun {
		return sum, nil
	}
	if sum.PlanSHA256 != o.expectedSHA {
		return sum, fmt.Errorf("plan sha256 %s does not match expected", sum.PlanSHA256)
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sum, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var valid bool
	if err := tx.QueryRow(ctx, `SELECT account_id=$1 AND kind='nas_pull' FROM connections WHERE id=$2`, nasSelectedVerifyAccountID, nasSelectedVerifyConnectionID).Scan(&valid); err != nil || !valid {
		return sum, fmt.Errorf("required account 47 NAS connection 13 is unavailable")
	}
	var planID int64
	if err := tx.QueryRow(ctx, `INSERT INTO nas_selected_verify_plans(account_id,connection_id,label,manifest,plan_sha256) VALUES($1,$2,$3,$4,$5) ON CONFLICT(label) DO NOTHING RETURNING id`, nasSelectedVerifyAccountID, nasSelectedVerifyConnectionID, o.label, manifest, sum.PlanSHA256).Scan(&planID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return sum, err
		}
		var existingSHA string
		if err := tx.QueryRow(ctx, `SELECT plan_sha256 FROM nas_selected_verify_plans WHERE label=$1 AND account_id=$2 AND connection_id=$3`, o.label, sum.AccountID, sum.ConnectionID).Scan(&existingSHA); err != nil {
			return sum, err
		}
		if existingSHA != sum.PlanSHA256 {
			return sum, errors.New("label already belongs to a different immutable plan")
		}
		return sum, tx.Commit(ctx)
	}
	for n, i := range items {
		if _, err := tx.Exec(ctx, `INSERT INTO nas_selected_verify_items(plan_id,ordinal,kind,source_id,relative_path,size_bytes,sha256) VALUES($1,$2,$3,$4,$5,$6,$7)`, planID, n+1, i.Kind, i.SourceID, i.RelativePath, i.SizeBytes, i.SHA256); err != nil {
			return sum, err
		}
	}
	return sum, tx.Commit(ctx)
}

type nasSelectedVerifyReportItem struct {
	ID           int64   `json:"item_id"`
	Kind         string  `json:"kind"`
	SourceID     int64   `json:"source_id"`
	RelativePath string  `json:"relative_path"`
	SizeBytes    int64   `json:"size_bytes"`
	SHA256       string  `json:"sha256"`
	State        string  `json:"state"`
	Attempts     int     `json:"attempts"`
	CompletedAt  *string `json:"completed_at"`
	Error        string  `json:"error"`
}

func reportNASSelectedVerify(ctx context.Context, pool *pgxpool.Pool, label string) (map[string]any, error) {
	var planSHA, createdAt string
	if err := pool.QueryRow(ctx, `SELECT plan_sha256,created_at::text FROM nas_selected_verify_plans WHERE label=$1 AND account_id=$2 AND connection_id=$3`, label, nasSelectedVerifyAccountID, nasSelectedVerifyConnectionID).Scan(&planSHA, &createdAt); err != nil {
		return nil, err
	}
	rows, err := pool.Query(ctx, `SELECT i.id,i.kind,i.source_id,i.relative_path,i.size_bytes,i.sha256,i.state,i.attempts,i.completed_at::text,i.outcome_error FROM nas_selected_verify_items i JOIN nas_selected_verify_plans p ON p.id=i.plan_id WHERE p.label=$1 AND p.account_id=$2 AND p.connection_id=$3 ORDER BY i.ordinal`, label, nasSelectedVerifyAccountID, nasSelectedVerifyConnectionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []nasSelectedVerifyReportItem{}
	counts := map[string]int{}
	var bytes int64
	for rows.Next() {
		var i nasSelectedVerifyReportItem
		if err := rows.Scan(&i.ID, &i.Kind, &i.SourceID, &i.RelativePath, &i.SizeBytes, &i.SHA256, &i.State, &i.Attempts, &i.CompletedAt, &i.Error); err != nil {
			return nil, err
		}
		items = append(items, i)
		counts[i.State]++
		bytes += i.SizeBytes
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"label": label, "plan_sha256": planSHA, "created_at": createdAt, "account_id": nasSelectedVerifyAccountID, "connection_id": nasSelectedVerifyConnectionID, "counts": counts, "bytes": bytes, "items": items}, nil
}
func runNASSelectedVerify(ctx context.Context, cfg config.Config, args []string) {
	if len(args) > 0 && args[0] == "report" {
		fs := flag.NewFlagSet("nas-selected-verify report", flag.ExitOnError)
		label := fs.String("label", "", "plan label")
		asJSON := fs.Bool("json", false, "JSON output")
		_ = fs.Parse(args[1:])
		if !nasSelectedVerifyLabelRE.MatchString(*label) || len(fs.Args()) != 0 {
			log.Fatal("valid --label required")
		}
		pool := mustJoinedPurgePool(ctx, cfg)
		defer pool.Close()
		report, err := reportNASSelectedVerify(ctx, pool, *label)
		if err != nil {
			log.Fatal(err)
		}
		if *asJSON {
			_ = json.NewEncoder(os.Stdout).Encode(report)
		} else {
			fmt.Printf("label=%s plan_sha256=%s counts=%v bytes=%v\n", *label, report["plan_sha256"], report["counts"], report["bytes"])
		}
		return
	}
	if len(args) < 1 || args[0] != "request" {
		log.Fatal("usage: stoaramactl nas-selected-verify request [--clip-ids IDS] [--collated-ids IDS] [--joined-ids IDS] --label LABEL [--dry-run | --expected-plan-sha256 SHA] [--json]; or report --label LABEL [--json]")
	}
	o, err := parseNASSelectedVerifyRequestArgs(args[1:])
	if err != nil {
		log.Fatal(err)
	}
	pool := mustJoinedPurgePool(ctx, cfg)
	defer pool.Close()
	s, err := requestNASSelectedVerify(ctx, pool, o)
	if err != nil {
		log.Fatal(err)
	}
	if o.asJSON {
		_ = json.NewEncoder(os.Stdout).Encode(s)
		return
	}
	fmt.Printf("label=%s account=%d connection=%d plan_sha256=%s items=%d bytes=%d dry_run=%t\n", s.Label, s.AccountID, s.ConnectionID, s.PlanSHA256, s.Items, s.Bytes, s.DryRun)
}
