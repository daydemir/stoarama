package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

type nasSpaceAuditOptions struct {
	connectionID int64
	limit        int
	asJSON       bool
	download     string
}

type nasSpaceAuditRow struct {
	ConnectionID      int64           `json:"connection_id"`
	AuditID           string          `json:"audit_id"`
	State             string          `json:"state"`
	ClientVersion     string          `json:"client_version"`
	Summary           json.RawMessage `json:"summary"`
	ArtifactObjectKey string          `json:"artifact_object_key"`
	ArtifactSizeBytes int64           `json:"artifact_size_bytes"`
	ArtifactSHA256    string          `json:"artifact_sha256"`
	StartedAt         time.Time       `json:"started_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	CompletedAt       *time.Time      `json:"completed_at"`
}

func parseNASSpaceAuditArgs(args []string) (nasSpaceAuditOptions, error) {
	var opts nasSpaceAuditOptions
	if len(args) == 0 || args[0] != "report" {
		return opts, fmt.Errorf("usage: stoaramactl nas-space-audit report --connection-id ID [--limit 5 --json --download PATH]")
	}
	fs := flag.NewFlagSet("nas-space-audit report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Int64Var(&opts.connectionID, "connection-id", 0, "NAS connection id")
	fs.IntVar(&opts.limit, "limit", 5, "recent audits to show (1..100)")
	fs.BoolVar(&opts.asJSON, "json", false, "print JSON")
	fs.StringVar(&opts.download, "download", "", "save newest completed audit gzip artifact to a new file")
	if err := fs.Parse(args[1:]); err != nil {
		return opts, err
	}
	if len(fs.Args()) != 0 || opts.connectionID <= 0 || opts.limit < 1 || opts.limit > 100 {
		return opts, fmt.Errorf("--connection-id is required; --limit must be 1..100; unexpected arguments are refused")
	}
	if opts.download != "" && strings.TrimSpace(opts.download) == "" {
		return opts, fmt.Errorf("--download path is blank")
	}
	return opts, nil
}

func runNASSpaceAudit(ctx context.Context, cfg config.Config, args []string) {
	opts, err := parseNASSpaceAuditArgs(args)
	if err != nil {
		log.Fatal(err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	pc, err := nasInventoryPoolConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid space audit database configuration")
	}
	pc.ConnConfig.RuntimeParams["application_name"] = "stoarama-nas-space-audit-report"
	pool, err := pgxpool.NewWithConfig(queryCtx, pc)
	if err != nil {
		log.Fatal("connect space audit database failed")
	}
	defer pool.Close()
	rows, err := pool.Query(queryCtx, `SELECT connection_id,audit_id,state,client_version,summary,
		COALESCE(artifact_object_key,''),COALESCE(artifact_size_bytes,0),COALESCE(artifact_sha256,''),
		started_at,updated_at,completed_at FROM nas_space_audits
		WHERE connection_id=$1 ORDER BY started_at DESC,audit_id DESC LIMIT $2`, opts.connectionID, opts.limit)
	if err != nil {
		log.Fatalf("load space audits: %v", err)
	}
	audits := make([]nasSpaceAuditRow, 0)
	for rows.Next() {
		var row nasSpaceAuditRow
		if err := rows.Scan(&row.ConnectionID, &row.AuditID, &row.State, &row.ClientVersion, &row.Summary,
			&row.ArtifactObjectKey, &row.ArtifactSizeBytes, &row.ArtifactSHA256, &row.StartedAt, &row.UpdatedAt, &row.CompletedAt); err != nil {
			rows.Close()
			log.Fatal(err)
		}
		audits = append(audits, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		log.Fatalf("iterate space audits: %v", err)
	}
	if opts.download != "" {
		// Select independently of the display limit: an in-progress latest audit
		// must not hide the newest completed artifact.
		var row nasSpaceAuditRow
		err = pool.QueryRow(queryCtx, `SELECT connection_id,audit_id,artifact_object_key,artifact_size_bytes,artifact_sha256
			FROM nas_space_audits WHERE connection_id=$1 AND state='complete'
			ORDER BY completed_at DESC NULLS LAST,started_at DESC,audit_id DESC LIMIT 1`, opts.connectionID).
			Scan(&row.ConnectionID, &row.AuditID, &row.ArtifactObjectKey, &row.ArtifactSizeBytes, &row.ArtifactSHA256)
		if err != nil {
			log.Fatal("no completed audit artifact available")
		}
		if err := validateNASSpaceAuditArtifact(row); err != nil {
			log.Fatal(err)
		}
		downloadCtx, stop := context.WithTimeout(ctx, 10*time.Minute)
		defer stop()
		rawURL, err := mustArchiveR2Client(downloadCtx, cfg).PresignGet(downloadCtx, row.ArtifactObjectKey, 15*time.Minute)
		if err != nil {
			log.Fatal("sign audit artifact download failed")
		}
		if err := downloadNASSpaceAuditArtifact(downloadCtx, rawURL, opts.download, row); err != nil {
			log.Fatal(err)
		}
	}
	if err := writeNASSpaceAuditReport(os.Stdout, opts.connectionID, audits, opts.asJSON); err != nil {
		log.Fatal(err)
	}
}

func validateNASSpaceAuditArtifact(row nasSpaceAuditRow) error {
	expected := fmt.Sprintf("nas-space-audits/%d/%s/%s.jsonl.gz", row.ConnectionID, row.AuditID, row.ArtifactSHA256)
	if row.ConnectionID <= 0 || !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`).MatchString(row.AuditID) || row.ArtifactObjectKey != expected || row.ArtifactSizeBytes <= 0 || row.ArtifactSizeBytes > 2<<30 {
		return fmt.Errorf("invalid audit artifact identity")
	}
	b, err := hex.DecodeString(row.ArtifactSHA256)
	if err != nil || len(b) != sha256.Size || row.ArtifactSHA256 != strings.ToLower(row.ArtifactSHA256) {
		return fmt.Errorf("invalid audit artifact SHA-256")
	}
	return nil
}

func downloadNASSpaceAuditArtifact(ctx context.Context, rawURL, target string, row nasSpaceAuditRow) error {
	if err := validateNASSpaceAuditArtifact(row); err != nil {
		return err
	}
	f, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create audit download: %w", err)
	}
	// Keep failed/partial output: its pathname may have been replaced by an
	// unrelated file while the request was in flight. Never unlink by that name.
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return fmt.Errorf("inspect audit output: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fmt.Errorf("create audit artifact request failed")
	}
	req.Header.Set("Accept-Encoding", "identity")
	client := &http.Client{Timeout: 10 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	// net/http errors can contain the signed URL; never propagate them.
	if err != nil {
		return fmt.Errorf("audit artifact download failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("audit artifact download HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength >= 0 && resp.ContentLength != row.ArtifactSizeBytes {
		return fmt.Errorf("audit artifact size mismatch")
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, hash), io.LimitReader(resp.Body, row.ArtifactSizeBytes+1))
	if err != nil {
		return fmt.Errorf("read audit artifact failed")
	}
	if n != row.ArtifactSizeBytes || hex.EncodeToString(hash.Sum(nil)) != row.ArtifactSHA256 {
		return fmt.Errorf("audit artifact size or SHA-256 mismatch")
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync audit artifact: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close audit artifact: %w", err)
	}
	current, err := os.Lstat(target)
	if err != nil || !os.SameFile(opened, current) {
		return fmt.Errorf("audit output path changed during download; output paths preserved")
	}
	return nil
}

func writeNASSpaceAuditReport(out io.Writer, connectionID int64, audits []nasSpaceAuditRow, asJSON bool) error {
	if audits == nil {
		audits = []nasSpaceAuditRow{}
	}
	if asJSON {
		return json.NewEncoder(out).Encode(struct {
			ConnectionID int64              `json:"connection_id"`
			Audits       []nasSpaceAuditRow `json:"audits"`
		}{connectionID, audits})
	}
	if _, err := fmt.Fprintf(out, "connection=%d audits=%d\n", connectionID, len(audits)); err != nil {
		return err
	}
	for _, a := range audits {
		if _, err := fmt.Fprintf(out, "audit=%s state=%s client=%s started=%s updated=%s completed=%v artifact=%s bytes=%d sha256=%s\nsummary=%s\n",
			a.AuditID, a.State, a.ClientVersion, a.StartedAt.Format(time.RFC3339), a.UpdatedAt.Format(time.RFC3339), a.CompletedAt, a.ArtifactObjectKey, a.ArtifactSizeBytes, a.ArtifactSHA256, a.Summary); err != nil {
			return err
		}
	}
	return nil
}
