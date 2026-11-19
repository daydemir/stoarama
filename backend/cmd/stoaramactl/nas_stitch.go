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
	"time"

	"github.com/daydemir/stoarama/backend/internal/api"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
)

func runNASStitch(ctx context.Context, cfg config.Config, args []string) {
	if len(args) == 0 {
		log.Fatal("usage: nas-stitch status|proof-plan|verify-proof|approve|pause|import-corpus|backlog|mint-durham-key|import-durham|retire-durham-stage")
	}
	switch args[0] {
	case "mint-durham-key":
		runMintDurhamStitchKey(ctx, cfg, args[1:])
		return
	case "import-durham":
		runDurhamHoldImport(ctx, cfg, args[1:])
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
	id := fs.Int64("object-id", 0, "exact certified Durham staging object ID")
	digest := fs.String("sha256", "", "certified whole-file SHA-256")
	apply := fs.Bool("apply", false, "authorize one stage-only wire attempt")
	reconcile := fs.Bool("reconcile", false, "record absence of an already issued staging target")
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
	if err = store.RetireStitchStage(ctx, *id, *digest, *reconcile); err != nil {
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
