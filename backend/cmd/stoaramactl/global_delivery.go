package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/daydemir/stoarama/backend/internal/config"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func runGlobalDelivery(ctx context.Context, cfg config.Config, args []string) {
	if len(args) == 0 || args[0] != "status" {
		fmt.Fprintln(os.Stderr, "usage: stoaramactl global-street-scores-delivery status [--server URL --api-key-env NAME]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet("global-street-scores-delivery status", flag.ExitOnError)
	server := fs.String("server", defaultBackendAPIURL(), "existing Stoarama server")
	envName := fs.String("api-key-env", "STOARAMA_API_TOKEN", "environment variable containing an existing account API key")
	_ = fs.Parse(args[1:])
	token := strings.TrimSpace(os.Getenv(*envName))
	if token == "" {
		fmt.Fprintln(os.Stderr, "existing account API key environment variable is empty")
		os.Exit(2)
	}
	endpoint := strings.TrimRight(*server, "/") + "/api/v1/account/global-street-scores-delivery/asset?path=" + url.QueryEscape("/api/coverage-downloads/catalog")
	request, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid server URL")
		os.Exit(1)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dataset status request failed")
		os.Exit(1)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "dataset status HTTP %d\n", response.StatusCode)
		os.Exit(1)
	}
	var catalog struct {
		Streams []json.RawMessage `json:"streams"`
	}
	if err := json.NewDecoder(response.Body).Decode(&catalog); err != nil || len(catalog.Streams) != 49 {
		fmt.Fprintln(os.Stderr, "dataset cohort validation failed")
		os.Exit(1)
	}
	fmt.Println(`{"available":true,"stream_count":49,"organization_id":47}`)
}
