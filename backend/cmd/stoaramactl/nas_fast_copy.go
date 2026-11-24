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

const nasFastCopyUsage = `usage:
  stoaramactl nas-fast-copy status --connection-id ID
  stoaramactl nas-fast-copy set --connection-id ID --workers N

Sets one NAS connection's fast-copy object streams (1-32, docs/NAS_R2_COPY.md).
The client applies it to its next lease batch.`

func runNASFastCopy(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 || (args[0] != "status" && args[0] != "set") {
		log.Fatal(nasFastCopyUsage)
	}
	fs := flag.NewFlagSet("nas-fast-copy "+args[0], flag.ExitOnError)
	connectionID := fs.Int64("connection-id", 0, "NAS connection id")
	workers := fs.Int("workers", 0, "parallel object streams (1-32)")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "admin API token")
	_ = fs.Parse(args[1:])
	if *connectionID <= 0 || len(fs.Args()) != 0 || (args[0] == "set") != (*workers != 0) {
		log.Fatal(nasFastCopyUsage)
	}
	path := fmt.Sprintf("/api/v1/connections/%d/r2-fast-copy", *connectionID)
	base, token := strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken)
	if args[0] == "status" {
		printJSON(mustAPIGet(ctx, base, token, path))
		return
	}
	printJSON(mustAPIRequest(ctx, http.MethodPost, base, token, path, map[string]any{"workers": *workers}))
}
