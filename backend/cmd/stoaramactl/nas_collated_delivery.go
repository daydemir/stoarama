package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
)

const nasCollatedDeliveryUsage = `usage:
  stoaramactl nas-collated-delivery status --connection-id ID
  stoaramactl nas-collated-delivery set --connection-id ID [--enabled true|false] [--rate-mib-per-sec N] [--parallel N]

Controls generation-2 collated hour delivery to one NAS connection
(docs/NAS_JOINED_DELIVERY.md). The client applies the policy on its next page.`

func runNASCollatedDelivery(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || (args[0] != "status" && args[0] != "set") {
		log.Fatal(nasCollatedDeliveryUsage)
	}
	fs := flag.NewFlagSet("nas-collated-delivery "+args[0], flag.ExitOnError)
	connectionID := fs.Int64("connection-id", 0, "NAS connection id")
	enabled := fs.String("enabled", "", "true or false")
	rateMiB := fs.Int64("rate-mib-per-sec", 0, "download budget in MiB/s (1-1024)")
	parallel := fs.Int("parallel", 0, "parallel downloads (1-32)")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "admin API token")
	_ = fs.Parse(args[1:])
	if *connectionID <= 0 || len(fs.Args()) != 0 {
		log.Fatal(nasCollatedDeliveryUsage)
	}
	path := fmt.Sprintf("/api/v1/connections/%d/collated-delivery", *connectionID)
	base, token := strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken)
	if args[0] == "status" {
		printJSON(mustAPIGet(ctx, base, token, path))
		return
	}
	body := map[string]any{}
	switch strings.TrimSpace(*enabled) {
	case "true":
		body["enabled"] = true
	case "false":
		body["enabled"] = false
	case "":
	default:
		log.Fatal("--enabled must be true or false")
	}
	if *rateMiB != 0 {
		body["download_bytes_per_sec"] = *rateMiB << 20
	}
	if *parallel != 0 {
		body["download_parallel"] = *parallel
	}
	if len(body) == 0 {
		log.Fatal(nasCollatedDeliveryUsage)
	}
	printJSON(mustAPIRequest(ctx, http.MethodPost, base, token, path, body))
}
