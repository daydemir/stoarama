package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/daydemir/stoarama/backend/internal/config"
)

// Preparation and arming are separate subcommands. Preparing a plan only asks
// the NAS for read-only observations, including NAS-only candidates.
func runNASCleanup(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal("usage: nas-cleanup prepare|report|pause|enable")
	}
	fs := flag.NewFlagSet("nas-cleanup "+args[0], flag.ExitOnError)
	input := fs.String("input", "", "JSON {label,items:[{kind,source_id}]}; prepare only")
	id := fs.Int64("plan-id", 0, "exact plan id")
	sha := fs.String("expected-plan-sha256", "", "exact approved digest; control only")
	reason := fs.String("reason", "", "audited approval/pause reason")
	nasOnly := fs.Bool("allow-nas-only", false, "explicitly permit proven NAS-only content; default retain")
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend URL")
	token := fs.String("api-token", cfg.APIToken, "service operator API token")
	_ = fs.Parse(args[1:])
	if fs.NArg() != 0 {
		log.Fatal("unexpected cleanup arguments")
	}
	switch args[0] {
	case "prepare":
		if *input == "" {
			log.Fatal("--input required")
		}
		b, err := os.ReadFile(*input)
		if err != nil || len(b) > 1<<20 {
			log.Fatal("bounded cleanup input unavailable")
		}
		var body any
		if json.Unmarshal(b, &body) != nil {
			log.Fatal("invalid cleanup input JSON")
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, "/api/v1/service/nas-cleanup/plans", body))
	case "report":
		if *id <= 0 {
			log.Fatal("--plan-id required")
		}
		printJSON(mustAPIGet(ctx, *base, *token, fmt.Sprintf("/api/v1/service/nas-cleanup/plans/%d", *id)))
	case "pause", "enable":
		if *id <= 0 || len(*sha) != 64 || *reason == "" {
			log.Fatal("--plan-id,--expected-plan-sha256,--reason required")
		}
		mode := "paused"
		if args[0] == "enable" {
			mode = "enabled"
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, fmt.Sprintf("/api/v1/service/nas-cleanup/plans/%d/control", *id), map[string]any{"plan_sha256": *sha, "mode": mode, "reason": *reason, "allow_nas_only": *nasOnly}))
	default:
		log.Fatal("usage: nas-cleanup prepare|report|pause|enable")
	}
}
