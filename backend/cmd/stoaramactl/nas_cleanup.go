package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
)

// Preparation and arming are separate subcommands. Preparing a plan only asks
// the NAS for read-only observations, including NAS-only candidates.
func runNASCleanup(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal(nasCleanupUsage)
	}
	if args[0] == "verify-recovery" {
		runNASCleanupVerifyRecovery(ctx, cfg, args[1:])
		return
	}
	fs := flag.NewFlagSet("nas-cleanup "+args[0], flag.ExitOnError)
	input := fs.String("input", "", "JSON {label,items:[{kind,source_id}]}; prepare only")
	id := fs.Int64("plan-id", 0, "exact plan id")
	sha := fs.String("expected-plan-sha256", "", "exact plan digest; control/recheck only")
	reason := fs.String("reason", "", "audited control/recheck reason")
	all := fs.Bool("all", false, "recheck only: refresh every would_delete item as well as unverified items")
	priority := fs.Int("priority", 0, "-100..100 scheduling priority; resume-dry-run only, no deletion authority")
	expectedPlans := fs.Int("expected-plans", 0, "retire-superseded only: exact count from the preview; 0 previews without changes")
	approvalHours := fs.Int("approval-hours", 2, "enable/enable-batch: approval window 1..24 hours (above 4 requires --allow-long-approval)")
	allowLong := fs.Bool("allow-long-approval", false, "enable/enable-batch: explicitly allow an approval window above 4 hours")
	batchInput := fs.String("batch-input", "", "enable-batch: batch manifest written by prepare_keep28_plans.py --submit")
	batchSHA := fs.String("expected-batch-sha256", "", "enable-batch/pause-batch: exact approved batch digest")
	batchID := fs.Int64("batch-id", 0, "pause-batch/batch-report: batch id")
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
	if *all && args[0] != "recheck" {
		log.Fatal("--all requires recheck")
	}
	switch args[0] {
	case "recheck":
		if *id <= 0 || len(*sha) != 64 || strings.TrimSpace(*reason) == "" {
			log.Fatal("recheck requires --plan-id, --expected-plan-sha256 and --reason")
		}
		scope := "unverified"
		if *all {
			scope = "all"
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, fmt.Sprintf("/api/v1/service/nas-cleanup/plans/%d/recheck", *id), map[string]any{"expected_plan_sha256": *sha, "reason": *reason, "scope": scope}))
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
	case "enable-batch":
		// One owner approval for an ordered list of plans. Only the first plan
		// is enabled; each next plan is enabled by the backend after the
		// previous one finished cleanly and after passing every gate again.
		if *batchInput == "" || len(*batchSHA) != 64 || *reason == "" || *approvalHours < 1 || *approvalHours > 24 || (*approvalHours > 4 && !*allowLong) {
			log.Fatal("enable-batch requires --batch-input, --expected-batch-sha256, --reason and --approval-hours 1..24 (above 4 requires --allow-long-approval)")
		}
		b, err := os.ReadFile(*batchInput)
		if err != nil || len(b) > 4<<20 {
			log.Fatal("bounded batch manifest unavailable")
		}
		ids, digest, err := nasCleanupBatchManifest(b)
		if err != nil {
			log.Fatal(err)
		}
		if digest != *batchSHA {
			log.Fatalf("batch manifest digest %s differs from --expected-batch-sha256", digest)
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, "/api/v1/service/nas-cleanup/batches", map[string]any{"plan_ids": ids, "batch_sha256": digest, "reason": *reason, "approval_hours": *approvalHours, "allow_long_approval": *allowLong}))
	case "pause-batch":
		if *batchID <= 0 || len(*batchSHA) != 64 || *reason == "" {
			log.Fatal("pause-batch requires --batch-id, --expected-batch-sha256 and --reason")
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, fmt.Sprintf("/api/v1/service/nas-cleanup/batches/%d/control", *batchID), map[string]any{"batch_sha256": *batchSHA, "mode": "paused", "reason": *reason}))
	case "batch-report":
		if *batchID <= 0 {
			log.Fatal("--batch-id required")
		}
		printJSON(mustAPIGet(ctx, *base, *token, fmt.Sprintf("/api/v1/service/nas-cleanup/batches/%d", *batchID)))
	case "pause", "enable", "resume-dry-run":
		if *id <= 0 || len(*sha) != 64 || *reason == "" {
			log.Fatal("--plan-id,--expected-plan-sha256,--reason required")
		}
		if args[0] == "enable" && (*approvalHours < 1 || *approvalHours > 24 || (*approvalHours > 4 && !*allowLong)) {
			log.Fatal("--approval-hours is 1..24 (above 4 requires --allow-long-approval)")
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
		if args[0] == "enable" {
			body["approval_hours"] = *approvalHours
			if *allowLong {
				body["allow_long_approval"] = true
			}
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, fmt.Sprintf("/api/v1/service/nas-cleanup/plans/%d/control", *id), body))
	default:
		log.Fatal(nasCleanupUsage)
	}
}

const nasCleanupUsage = "usage: nas-cleanup prepare|report|verify-recovery|pause|enable|resume-dry-run|recheck|retire-superseded|enable-batch|pause-batch|batch-report"

// nasCleanupBatchManifest validates a batch manifest and recomputes its
// digest exactly as the backend does (cleanupBatchDigest): sha256 over
// "stoarama-nas-cleanup-batch-v1\npolicy <sha>\n" + "plan <id> <sha>\n"...
func nasCleanupBatchManifest(raw []byte) ([]int64, string, error) {
	var m struct {
		Policy string `json:"policy_sha256"`
		Plans  []struct {
			ID  int64  `json:"id"`
			SHA string `json:"plan_sha256"`
		} `json:"plans"`
		BatchSHA string `json:"batch_sha256"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, "", fmt.Errorf("invalid batch manifest: %w", err)
	}
	if len(m.Policy) != 64 || len(m.Plans) < 1 || len(m.Plans) > 500 {
		return nil, "", fmt.Errorf("batch manifest needs a policy digest and 1..500 plans")
	}
	var b strings.Builder
	b.WriteString("stoarama-nas-cleanup-batch-v1\npolicy " + m.Policy + "\n")
	ids := make([]int64, 0, len(m.Plans))
	seen := map[int64]bool{}
	for _, p := range m.Plans {
		if p.ID <= 0 || seen[p.ID] || len(p.SHA) != 64 {
			return nil, "", fmt.Errorf("batch manifest has an invalid or duplicate plan")
		}
		seen[p.ID] = true
		ids = append(ids, p.ID)
		b.WriteString("plan " + strconv.FormatInt(p.ID, 10) + " " + p.SHA + "\n")
	}
	sum := sha256.Sum256([]byte(b.String()))
	digest := hex.EncodeToString(sum[:])
	if m.BatchSHA != digest {
		return nil, "", fmt.Errorf("batch manifest digest field %q does not match its plans (%s)", m.BatchSHA, digest)
	}
	return ids, digest, nil
}
