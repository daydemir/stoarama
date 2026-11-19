package main

import (
	"bytes"
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
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
)

func runNASStitch(ctx context.Context, cfg config.Config, args []string) {
	if len(args) == 0 {
		log.Fatal("usage: nas-stitch status|proof-plan|verify-proof|approve|pause|import-corpus|backlog|mint-durham-key|plan-durham|import-durham|control-durham|retire-durham-stage")
	}
	switch args[0] {
	case "mint-durham-key":
		runMintDurhamStitchKey(ctx, cfg, args[1:])
		return
	case "plan-durham":
		runDurhamHoldPlan(ctx, cfg, args[1:])
		return
	case "import-durham":
		runDurhamHoldImport(ctx, cfg, args[1:])
		return
	case "control-durham":
		runDurhamQueueControl(ctx, cfg, args[1:])
		return
	case "retire-durham-stage":
		runDurhamStageRetirement(ctx, cfg, args[1:])
		return
	case "backlog":
		runNASStitchBacklog(ctx, cfg, args[1:])
		return
	}
	fs := flag.NewFlagSet("nas-stitch "+args[0], flag.ExitOnError)
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend URL")
	token := fs.String("api-token", cfg.APIToken, "operator token")
	stream := fs.Int64("stream-id", 0, "exact owner-approved stream")
	scope := fs.Int64("scope-id", 0, "scope to pause")
	start := fs.String("start", "", "kept pilot start RFC3339")
	end := fs.String("end", "", "kept pilot end RFC3339")
	until := fs.String("approved-until", "", "explicit approval expiry RFC3339, <=180 days")
	reason := fs.String("reason", "", "owner approval reference")
	deletion := fs.Bool("approve-deletion", false, "owner authorizes fully proven stitched raw retirement")
	input := fs.String("input", "", "proof validation envelope or pinned frozen decision JSONL")
	fs.Parse(args[1:])
	switch args[0] {
	case "proof-plan":
		printJSON(mustAPIGet(ctx, *base, *token, "/api/v1/service/nas-stitch/proof"))
	case "verify-proof":
		raw, err := os.ReadFile(*input)
		if err != nil || len(raw) > 1<<20 {
			log.Fatal("bounded proof result required")
		}
		var result any
		if json.Unmarshal(raw, &result) != nil {
			log.Fatal("invalid proof result")
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, "/api/v1/service/nas-stitch/proof/verify", result))
	case "status":
		printJSON(mustAPIGet(ctx, *base, *token, "/api/v1/service/nas-stitch/status"))
	case "approve":
		a, e := time.Parse(time.RFC3339, *start)
		if e != nil {
			log.Fatal("--start RFC3339 required")
		}
		b, e := time.Parse(time.RFC3339, *end)
		if e != nil {
			log.Fatal("--end RFC3339 required")
		}
		u, e := time.Parse(time.RFC3339, *until)
		if e != nil {
			log.Fatal("--approved-until RFC3339 required")
		}
		if *stream <= 0 || *reason == "" {
			log.Fatal("--stream-id and --reason required")
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, "/api/v1/service/nas-stitch/scopes", map[string]any{"stream_id": *stream, "start_at": a, "end_at": b, "approved_until": u, "reason": *reason, "deletion_approved": *deletion}))
	case "pause":
		if *scope <= 0 {
			log.Fatal("--scope-id required")
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, fmt.Sprintf("/api/v1/service/nas-stitch/scopes/%d/pause", *scope), map[string]any{}))
	case "import-corpus":
		f, e := os.Open(*input)
		if e != nil {
			log.Fatal(e)
		}
		defer f.Close()
		raw, e := io.ReadAll(io.LimitReader(f, 64<<20+1))
		if e != nil || len(raw) > 64<<20 {
			log.Fatal("bounded corpus unavailable")
		}
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, *base+"/api/v1/service/nas-stitch/corpus", bytes.NewReader(raw))
		if e != nil {
			log.Fatal(e)
		}
		req.Header.Set("Authorization", "Bearer "+*token)
		req.Header.Set("Content-Type", "application/x-ndjson")
		c := http.Client{Timeout: 5 * time.Minute}
		resp, e := c.Do(req)
		if e != nil {
			log.Fatal("corpus import transport failed")
		}
		defer resp.Body.Close()
		body, e := io.ReadAll(io.LimitReader(resp.Body, 8192))
		if e != nil || resp.StatusCode != 200 {
			log.Fatalf("corpus import failed HTTP %d: %s", resp.StatusCode, body)
		}
		fmt.Println(string(body))
	default:
		log.Fatal("usage: nas-stitch status|proof-plan|verify-proof|approve|pause|import-corpus|backlog")
	}
}

func runDurhamStageRetirement(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("nas-stitch retire-durham-stage", flag.ExitOnError)
	id := fs.Int64("object-id", 0, "exact Durham staging object ID")
	digest := fs.String("sha256", "", "staging object SHA-256")
	apply := fs.Bool("apply", false, "authorize one stage-only wire attempt")
	reconcile := fs.Bool("reconcile", false, "record absence of an already issued staging target or drained abort")
	proof := fs.String("conditional-delete-proof", "", "reviewed provider conditional DELETE proof")
	proofSHA := fs.String("expected-delete-proof-sha256", "", "expected SHA-256 of the reviewed provider proof")
	fs.Parse(args)
	if fs.NArg() != 0 || *id <= 0 || !r2RetireLowerHex64(*digest) || *apply == *reconcile {
		log.Fatal("exact object ID, SHA-256 and one of --apply or --reconcile required")
	}
	pool, err := r2RetireOpenPool(ctx, cfg, false)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	store, err := r2.New(ctx, r2.Config{WriterDB: pool, Endpoint: cfg.R2Endpoint, Region: cfg.R2Region,
		Bucket: cfg.R2Bucket, AccessKey: cfg.R2AccessKeyID, SecretKey: cfg.R2SecretAccessKey})
	if err != nil {
		log.Fatal(err)
	}
	var proofExpires time.Time
	if *apply {
		proofExpires, err = r2RetireDeleteProofExpiry(*proof, *proofSHA, store.Bucket(), store.EndpointSHA256(), false, time.Now())
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := store.RetireStitchStage(ctx, *id, *digest, proofExpires, *reconcile); err != nil {
		log.Fatal(err)
	}
	printJSON(map[string]any{"object_id": *id, "absence_confirmed": true})
}

func runDurhamHoldImport(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("nas-stitch import-durham", flag.ExitOnError)
	input := fs.String("input", "", "reviewed exact Durham hold manifest")
	expected := fs.String("sha256", "", "independently reviewed manifest SHA-256")
	fs.Parse(args)
	if fs.NArg() != 0 || *input == "" || !r2RetireLowerHex64(*expected) {
		log.Fatal("reviewed Durham manifest and SHA-256 are required")
	}
	file, err := os.Open(*input)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > 1<<30 {
		log.Fatal("Durham hold manifest is not a bounded regular file")
	}
	digest := sha256.New()
	decoder := json.NewDecoder(io.TeeReader(io.LimitReader(file, info.Size()), digest))
	decoder.DisallowUnknownFields()
	var manifest api.DurhamStitchHoldSet
	if err := decoder.Decode(&manifest); err != nil {
		log.Fatal("invalid Durham hold manifest")
	}
	var extra json.RawMessage
	if decoder.Decode(&extra) != io.EOF || hex.EncodeToString(digest.Sum(nil)) != *expected {
		log.Fatal("Durham hold manifest differs from reviewed digest")
	}
	pool, err := r2RetireOpenPool(ctx, cfg, false)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)
	holds, err := api.ImportDurhamStitchHoldSet(ctx, tx, manifest)
	if err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	printJSON(map[string]any{"hours": len(manifest.Hours), "holds": holds, "manifest_sha256": *expected})
}

func runDurhamQueueControl(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("nas-stitch control-durham", flag.ExitOnError)
	enable := fs.Bool("enable", false, "admit R2-backed Durham stitch attempts")
	pause := fs.Bool("pause", false, "stop new grants and lease admission")
	maxBytes := fs.Int64("max-staging-bytes", 0, "controller-approved peak staging reservation")
	fs.Parse(args)
	if fs.NArg() != 0 || *enable == *pause || (*enable && *maxBytes <= 0) || (*pause && *maxBytes != 0) {
		log.Fatal("choose --pause or --enable with an approved --max-staging-bytes")
	}
	pool, err := r2RetireOpenPool(ctx, cfg, false)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	tag, err := pool.Exec(ctx, `UPDATE nas_stitch_r2_control SET enabled=$1,
 max_staging_bytes=CASE WHEN $1 THEN $2 ELSE max_staging_bytes END
 WHERE singleton AND (NOT $1 OR EXISTS(
   SELECT 1 FROM nas_stitch_r2_input_holds WHERE released_at IS NULL))`, *enable, *maxBytes)
	if err != nil || tag.RowsAffected() != 1 {
		log.Fatal("Durham control unavailable or no reviewed input holds")
	}
	printJSON(map[string]any{"enabled": *enable})
}

func runDurhamHoldPlan(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("nas-stitch plan-durham", flag.ExitOnError)
	scopeText := fs.String("scope-ids", "", "comma-separated approved scope IDs")
	limit := fs.Int("max-hours", 1, "maximum whole R2-backed hours to plan")
	out := fs.String("out", "", "new private hold manifest file")
	fs.Parse(args)
	if fs.NArg() != 0 || *out == "" {
		log.Fatal("approved scope IDs and a new output manifest are required")
	}
	var scopes []int64
	for _, value := range strings.Split(*scopeText, ",") {
		id, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil || id <= 0 {
			log.Fatal("scope IDs must be positive integers")
		}
		scopes = append(scopes, id)
	}
	pool, err := r2RetireOpenPool(ctx, cfg, true)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback(ctx)
	manifest, err := api.PlanDurhamStitchHoldSet(ctx, tx, scopes, cfg.R2Bucket, cfg.R2Endpoint, *limit)
	if err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	if len(manifest.Hours) == 0 {
		log.Fatal("no whole R2-backed hours in the selected scopes")
	}
	file, err := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	digest := sha256.New()
	if err := json.NewEncoder(io.MultiWriter(file, digest)).Encode(manifest); err != nil {
		file.Close()
		log.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		log.Fatal(err)
	}
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}
	printJSON(map[string]any{"hours": len(manifest.Hours), "manifest_sha256": hex.EncodeToString(digest.Sum(nil))})
}
