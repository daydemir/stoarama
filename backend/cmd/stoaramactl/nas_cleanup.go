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
		log.Fatal("usage: nas-cleanup prepare|report|verify-recovery|pause|enable|resume-dry-run|retire-superseded")
	}
	if args[0] == "verify-recovery" {
		runNASCleanupVerifyRecovery(ctx, cfg, args[1:])
		return
	}
	fs := flag.NewFlagSet("nas-cleanup "+args[0], flag.ExitOnError)
	input := fs.String("input", "", "JSON {label,items:[{kind,source_id}]}; prepare only")
	id := fs.Int64("plan-id", 0, "exact plan id")
	sha := fs.String("expected-plan-sha256", "", "exact approved digest; control only")
	reason := fs.String("reason", "", "audited approval/pause reason")
	priority := fs.Int("priority", 0, "-100..100 scheduling priority; resume-dry-run only, no deletion authority")
	expectedPlans := fs.Int("expected-plans", 0, "retire-superseded only: exact count from the preview; 0 previews without changes")
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend URL")
	token := fs.String("api-token", cfg.APIToken, "service operator API token")
	_ = fs.Parse(args[1:])
	if fs.NArg() != 0 {
		log.Fatal("unexpected cleanup arguments")
	}
	prioritySet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "priority" {
			prioritySet = true
		}
	})
	if prioritySet && (args[0] != "resume-dry-run" || *priority < -100 || *priority > 100) {
		log.Fatal("--priority requires resume-dry-run and -100..100")
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
	case "retire-superseded":
		// Terminal, non-destructive retirement of plans bound to a superseded
		// cleanup policy. Without --expected-plans this is a read-only preview.
		if *reason == "" || *expectedPlans < 0 || *id != 0 || *sha != "" {
			log.Fatal("retire-superseded requires --reason and optionally --expected-plans N (from the preview)")
		}
		body := map[string]any{"reason": *reason}
		if *expectedPlans > 0 {
			body["expected_plans"] = *expectedPlans
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, "/api/v1/service/nas-cleanup/retire-superseded", body))
	case "report":
		if *id <= 0 {
			log.Fatal("--plan-id required")
		}
		printJSON(mustAPIGet(ctx, *base, *token, fmt.Sprintf("/api/v1/service/nas-cleanup/plans/%d", *id)))
	case "pause", "enable", "resume-dry-run":
		if *id <= 0 || len(*sha) != 64 || *reason == "" {
			log.Fatal("--plan-id,--expected-plan-sha256,--reason required")
		}
		mode := "paused"
		if args[0] == "enable" {
			mode = "enabled"
		}
		if args[0] == "resume-dry-run" {
			mode = "dry_run"
		}
		// NAS-only deletion was removed (the API rejects true); there is no flag.
		body := map[string]any{"plan_sha256": *sha, "mode": mode, "reason": *reason, "allow_nas_only": false}
		if prioritySet {
			body["priority"] = *priority
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, fmt.Sprintf("/api/v1/service/nas-cleanup/plans/%d/control", *id), body))
	default:
		log.Fatal("usage: nas-cleanup prepare|report|verify-recovery|pause|enable|resume-dry-run|retire-superseded")
	}
}
