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
	clipIDs            []int64
	label, expectedSHA string
	dryRun, asJSON     bool
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
	for _, raw := range strings.Split(*ids, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil || id <= 0 {
			return o, fmt.Errorf("invalid clip id %q", raw)
		}
		o.clipIDs = append(o.clipIDs, id)
	}
	if len(o.clipIDs) == 0 {
		return o, errors.New("--clip-ids is required")
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

func requestNASSelectedVerify(ctx context.Context, pool *pgxpool.Pool, o nasSelectedVerifyOptions) (nasSelectedVerifySummary, error) {
	sum := nasSelectedVerifySummary{Label: o.label, AccountID: nasSelectedVerifyAccountID, ConnectionID: nasSelectedVerifyConnectionID, DryRun: o.dryRun}
	rows, err := pool.Query(ctx, `SELECT c.id,c.display_path,c.size_bytes,lower(c.sha256),c.object_key FROM recording_clips c JOIN recordings r ON r.id=c.recording_id WHERE r.account_id=$1 AND c.id=ANY($2) ORDER BY c.id`, nasSelectedVerifyAccountID, o.clipIDs)
	if err != nil {
		return sum, err
	}
	defer rows.Close()
	items := []nasSelectedVerifyManifestItem{}
	for rows.Next() {
		var i nasSelectedVerifyManifestItem
		i.Kind = "raw"
		if err := rows.Scan(&i.SourceID, &i.RelativePath, &i.SizeBytes, &i.SHA256, &i.R2Source); err != nil {
			return sum, err
		}
		if i.RelativePath == "" || strings.HasPrefix(i.RelativePath, "/") || strings.Contains(i.RelativePath, "..") || i.SizeBytes <= 0 || len(i.SHA256) != 64 {
			return sum, fmt.Errorf("clip %d has incomplete canonical identity", i.SourceID)
		}
		items = append(items, i)
		sum.Bytes += i.SizeBytes
	}
	if err := rows.Err(); err != nil {
		return sum, err
	}
	if len(items) != len(o.clipIDs) {
		return sum, fmt.Errorf("selected %d distinct account clips, expected %d", len(items), len(o.clipIDs))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].SourceID < items[j].SourceID })
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
	if err := tx.QueryRow(ctx, `INSERT INTO nas_selected_verify_plans(account_id,connection_id,label,manifest,plan_sha256) VALUES($1,$2,$3,$4,$5) RETURNING id`, nasSelectedVerifyAccountID, nasSelectedVerifyConnectionID, o.label, manifest, sum.PlanSHA256).Scan(&planID); err != nil {
		return sum, err
	}
	for n, i := range items {
		if _, err := tx.Exec(ctx, `INSERT INTO nas_selected_verify_items(plan_id,ordinal,kind,source_id,relative_path,size_bytes,sha256) VALUES($1,$2,$3,$4,$5,$6,$7)`, planID, n+1, i.Kind, i.SourceID, i.RelativePath, i.SizeBytes, i.SHA256); err != nil {
			return sum, err
		}
	}
	return sum, tx.Commit(ctx)
}

func runNASSelectedVerify(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] != "request" {
		log.Fatal("usage: stoaramactl nas-selected-verify request --clip-ids IDS --label LABEL [--dry-run | --expected-plan-sha256 SHA] [--json]")
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
