package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func runNodes(ctx context.Context, cfg config.Config, args []string) {
	if len(args) >= 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Print("stoaramactl nodes enrollment-token create ...\nstoaramactl nodes relay-groups list|create|update|assign|unassign|delete ...\n")
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
	default:
		log.Fatalf("unknown nodes subcommand: %s", args[0])
	}
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
	nodeID := fs.Int64("node-id", 0, "relay node id")
	_ = fs.Parse(args[1:])
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
		if len(payload) == 0 {
			log.Fatalf("--name or --max-streams is required")
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
