package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/daydemir/stoarama/backend/internal/config"
)

// nas-client logs reads the redacted log lines a NAS app uploaded (read-only).
func runNASClient(ctx context.Context, cfg config.Config, args []string) {
	runNASClientTo(ctx, cfg, args, os.Stdout)
}

func runNASClientTo(ctx context.Context, cfg config.Config, args []string, out io.Writer) {
	if len(args) < 1 || (args[0] != "logs" && args[0] != "release-quarantine") {
		log.Fatal("usage: nas-client {logs|release-quarantine} --connection-id ID [--since 1h] [--grep TEXT] [--limit N]")
	}
	fs := flag.NewFlagSet("nas-client logs", flag.ExitOnError)
	id := fs.Int64("connection-id", 0, "NAS connection id")
	since := fs.String("since", "1h", "window, e.g. 30m, 6h (max 168h)")
	grep := fs.String("grep", "", "case-insensitive substring filter")
	limit := fs.Int("limit", 2000, "most recent lines to print (1..10000)")
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend URL")
	token := fs.String("api-token", cfg.APIToken, "service operator API token")
	_ = fs.Parse(args[1:])
	if *id <= 0 || fs.NArg() != 0 {
		log.Fatal("--connection-id required")
	}
	if args[0] == "release-quarantine" {
		res := mustAPIRequest(ctx, http.MethodPost, *base, *token, fmt.Sprintf("/api/v1/service/nas-connections/%d/release-quarantine", *id), map[string]any{})
		_ = json.NewEncoder(out).Encode(res)
		return
	}
	q := url.Values{"since": {*since}, "limit": {fmt.Sprint(*limit)}}
	if *grep != "" {
		q.Set("grep", *grep)
	}
	res := mustAPIGet(ctx, *base, *token, fmt.Sprintf("/api/v1/service/nas-connections/%d/client-logs?%s", *id, q.Encode()))
	if latest, ok := res["latest_upload"].(map[string]any); ok {
		fmt.Fprintf(out, "# uploads=%v latest=%v version=%v boot=%v journal_entries=%v quarantined=%v dropped_lines=%v\n",
			res["uploads"], latest["received_at"], latest["client_version"], latest["boot_id"], latest["journal_entries"], latest["quarantined"], latest["dropped_lines"])
	} else {
		fmt.Fprintf(out, "# no uploads in the window\n")
	}
	lines, _ := res["lines"].([]any)
	for _, raw := range lines {
		line, _ := raw.(map[string]any)
		fmt.Fprintf(out, "%v %v [%v] %v\n", line["at"], line["level"], line["client_version"], line["message"])
	}
}
