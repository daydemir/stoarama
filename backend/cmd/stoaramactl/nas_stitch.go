package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func runNASStitch(ctx context.Context, cfg config.Config, args []string) {
	if len(args) == 0 {
		log.Fatal("usage: nas-stitch status|proof-plan|verify-proof|approve|pause|import-corpus|backlog|mint-durham-key")
	}
	switch args[0] {
	case "mint-durham-key":
		runMintDurhamStitchKey(ctx, cfg, args[1:])
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
