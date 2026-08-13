package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func runNodes(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print("stoaramactl nodes enrollment-token create ...\nstoaramactl nodes relay-groups list|create|update|assign|unassign|delete ...\nstoaramactl nodes quarantine ...\n")
		return
	}
	if len(args) < 1 {
		fmt.Print("stoaramactl nodes enrollment-token create ...\nstoaramactl nodes relay-groups list|create|update|assign|unassign|delete ...\n")
		return
	}
	switch args[0] {
	case "enrollment-token":
		runNodesEnrollmentToken(ctx, cfg, args[1:])
	case "relay-groups":
		runNodesRelayGroups(ctx, cfg, args[1:])
	case "quarantine":
		runNodesQuarantine(ctx, cfg, args[1:])
	default:
		log.Fatalf("unknown nodes subcommand: %s", args[0])
	}
}

func runNodesQuarantine(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("nodes quarantine", flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "operator API token")
	nodeID := fs.Int64("node-id", 0, "exact relay node id")
	accountID := fs.Int64("expected-account-id", 0, "expected account id")
	groupID := fs.Int64("expected-relay-group-id", 0, "expected relay group id")
	cap := fs.Int("expected-relay-max-streams", 0, "expected node stream cap")
	status := fs.String("expected-status", "active", "expected node status")
	version := fs.String("expected-relay-version", "", "expected relay version")
	revision := fs.String("expected-source-revision", "", "expected source revision")
	started := fs.String("expected-relay-started-at", "", "expected relay start RFC3339")
	heartbeat := fs.String("expected-heartbeat-at", "", "expected heartbeat RFC3339")
	reason := fs.String("reason-code", "", "allowlisted quarantine reason")
	evidence := fs.String("evidence-sha256", "", "lowercase evidence digest")
	idempotency := fs.String("idempotency-key", "", "show-once operation UUID")
	_ = fs.Parse(args)
	if fs.NArg() != 0 || *nodeID <= 0 || *accountID <= 0 || *groupID <= 0 || *cap <= 0 || strings.TrimSpace(*version) == "" || strings.TrimSpace(*revision) == "" || strings.TrimSpace(*started) == "" || strings.TrimSpace(*heartbeat) == "" || strings.TrimSpace(*reason) == "" || strings.TrimSpace(*evidence) == "" || strings.TrimSpace(*idempotency) == "" {
		log.Fatal("nodes quarantine requires exact node/account/group/cap/runtime/timestamps/reason/evidence/idempotency fields")
	}
	payload := map[string]any{
		"idempotency_key": strings.TrimSpace(*idempotency), "expected_account_id": *accountID,
		"expected_relay_group_id": *groupID, "expected_relay_max_streams": *cap, "expected_status": strings.TrimSpace(*status),
		"expected_relay_version": strings.TrimSpace(*version), "expected_source_revision": strings.TrimSpace(*revision),
		"expected_relay_started_at": strings.TrimSpace(*started), "expected_heartbeat_at": strings.TrimSpace(*heartbeat),
		"reason_code": strings.TrimSpace(*reason), "evidence_sha256": strings.TrimSpace(*evidence),
	}
	printJSON(mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/admin/relay-nodes/"+strconv.FormatInt(*nodeID, 10)+"/quarantine", payload))
}

func runNodesRelayGroups(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || args[0] == "-h" || args[0] == "--help" {
		fmt.Print("stoaramactl nodes relay-groups list|create|update|assign|unassign|delete [options]\n")
		return
	}
	fs := flag.NewFlagSet("nodes relay-groups "+args[0], flag.ExitOnError)
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "account API token")
	id := fs.Int64("id", 0, "relay group id")
	name := fs.String("name", "", "relay group name")
	maxStreams := fs.Int("max-streams", 0, "group stream limit")
	bandwidthMbps := fs.Float64("bandwidth-mbps", 0, "conservative internet connection routing budget in Mbps")
	nodeID := fs.Int64("node-id", 0, "relay node id")
	_ = fs.Parse(args[1:])
	if math.IsNaN(*bandwidthMbps) || math.IsInf(*bandwidthMbps, 0) || (*bandwidthMbps != 0 && (*bandwidthMbps < 1 || *bandwidthMbps > 10000)) {
		log.Fatalf("--bandwidth-mbps must be between 1 and 10000")
	}
	base := strings.TrimSpace(*backendAPIURL)
	token := strings.TrimSpace(*apiToken)

	switch args[0] {
	case "list":
		printJSON(mustAPIRequest(ctx, http.MethodGet, base, token, "/api/v1/account/relay-groups", nil))
	case "create":
		if strings.TrimSpace(*name) == "" {
			log.Fatalf("--name is required")
		}
		payload := map[string]any{"name": strings.TrimSpace(*name)}
		if *maxStreams != 0 {
			payload["max_streams"] = *maxStreams
		}
		if *bandwidthMbps != 0 {
			payload["bandwidth_capacity_mbps"] = *bandwidthMbps
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, base, token, "/api/v1/account/relay-groups", payload))
	case "update":
		requireRelayGroupID(*id)
		payload := map[string]any{}
		if strings.TrimSpace(*name) != "" {
			payload["name"] = strings.TrimSpace(*name)
		}
		if *maxStreams != 0 {
			payload["max_streams"] = *maxStreams
		}
		if *bandwidthMbps != 0 {
			payload["bandwidth_capacity_mbps"] = *bandwidthMbps
		}
		if len(payload) == 0 {
			log.Fatalf("--name, --max-streams, or --bandwidth-mbps is required")
		}
		printJSON(mustAPIRequest(ctx, http.MethodPatch, base, token, "/api/v1/account/relay-groups/"+strconv.FormatInt(*id, 10), payload))
	case "assign":
		requireRelayGroupID(*id)
		if *nodeID <= 0 {
			log.Fatalf("--node-id must be positive")
		}
		printJSON(mustAPIRequest(ctx, http.MethodPut, base, token, "/api/v1/account/nodes/"+strconv.FormatInt(*nodeID, 10)+"/relay-group/"+strconv.FormatInt(*id, 10), nil))
	case "unassign":
		if *nodeID <= 0 {
			log.Fatalf("--node-id must be positive")
		}
		printJSON(mustAPIRequest(ctx, http.MethodDelete, base, token, "/api/v1/account/nodes/"+strconv.FormatInt(*nodeID, 10)+"/relay-group", nil))
	case "delete":
		requireRelayGroupID(*id)
		printJSON(mustAPIRequest(ctx, http.MethodDelete, base, token, "/api/v1/account/relay-groups/"+strconv.FormatInt(*id, 10), nil))
	default:
		log.Fatalf("unknown nodes relay-groups subcommand: %s", args[0])
	}
}

func requireRelayGroupID(id int64) {
	if id <= 0 {
		log.Fatalf("--id must be positive")
	}
}

func runNodesEnrollmentToken(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print("stoaramactl nodes enrollment-token create --owner-email EMAIL --node-type inference_node|local_recorder [--label LABEL --expires-at RFC3339] [--backend-api-url URL --api-token TOKEN]\n")
		return
	}
	if len(args) < 1 {
		fmt.Print("stoaramactl nodes enrollment-token create --owner-email EMAIL --node-type inference_node|local_recorder [--label LABEL --expires-at RFC3339] [--backend-api-url URL --api-token TOKEN]\n")
		return
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("nodes enrollment-token create", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		ownerEmail := fs.String("owner-email", "", "owner account email")
		nodeType := fs.String("node-type", "", "inference_node or local_recorder")
		label := fs.String("label", "", "optional label")
		expiresAt := fs.String("expires-at", "", "optional RFC3339 expiry")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		if strings.TrimSpace(*ownerEmail) == "" {
			log.Fatalf("--owner-email is required")
		}
		if strings.TrimSpace(*nodeType) == "" {
			log.Fatalf("--node-type is required")
		}
		if strings.TrimSpace(*nodeType) == "yt_relay_source" {
			log.Fatalf("yt_relay_source enrollment is disabled")
		}
		payload := mustAPIRequest(ctx, "POST", strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/node-enrollment-tokens", map[string]any{
			"owner_email": strings.TrimSpace(*ownerEmail),
			"node_type":   strings.TrimSpace(*nodeType),
			"label":       strings.TrimSpace(*label),
			"expires_at":  strings.TrimSpace(*expiresAt),
		})
		if *asJSON {
			printJSON(payload)
			return
		}
		fmt.Printf("token_prefix=%s node_type=%s expires_at=%v\n", fmt.Sprint(payload["token_prefix"]), fmt.Sprint(payload["node_type"]), payload["expires_at"])
		if token := strings.TrimSpace(fmt.Sprint(payload["token"])); token != "" {
			fmt.Printf("token=%s\n", token)
		}
	default:
		log.Fatalf("unknown nodes enrollment-token subcommand: %s", args[0])
	}
}
