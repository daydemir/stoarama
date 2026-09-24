package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/nasdelivery"
)

const recordingsNASDeliveryUsage = `usage:
  stoaramactl recordings nas-delivery status --account-id ID
  stoaramactl recordings nas-delivery set --account-id ID --recording-ids 1,2,3 --mode raw|collated_only --reason TEXT [--apply]

collated_only keeps a nas_pull recording's raw clips in R2 (not metered as
managed storage) and delivers the recording to the NAS only as collated hours.
The mode is frozen per clip at insert: it applies to clips recorded after the
switch. Without --apply, set is a dry run.`

func runRecordingNASDelivery(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal(recordingsNASDeliveryUsage)
	}
	switch args[0] {
	case "status":
		fs := flag.NewFlagSet("recordings nas-delivery status", flag.ExitOnError)
		accountID := fs.Int64("account-id", 0, "account id")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "admin API token")
		_ = fs.Parse(args[1:])
		if *accountID <= 0 || len(fs.Args()) != 0 {
			log.Fatal(recordingsNASDeliveryUsage)
		}
		printJSON(mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken),
			fmt.Sprintf("/api/v1/recordings/nas-delivery-mode?account_id=%d", *accountID)))
	case "set":
		fs := flag.NewFlagSet("recordings nas-delivery set", flag.ExitOnError)
		accountID := fs.Int64("account-id", 0, "account id every recording must belong to")
		recordingIDs := fs.String("recording-ids", "", "comma-separated nas_pull recording ids")
		mode := fs.String("mode", "", "raw or collated_only")
		reason := fs.String("reason", "", "audited reason")
		apply := fs.Bool("apply", false, "change the mode (default is a dry run)")
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "admin API token")
		_ = fs.Parse(args[1:])
		if *accountID <= 0 || strings.TrimSpace(*recordingIDs) == "" || !nasdelivery.ValidMode(strings.TrimSpace(*mode)) ||
			strings.TrimSpace(*reason) == "" || len(fs.Args()) != 0 {
			log.Fatal(recordingsNASDeliveryUsage)
		}
		printJSON(mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken),
			"/api/v1/recordings/nas-delivery-mode", map[string]any{
				"account_id": *accountID, "recording_ids": parseQualificationIDs(*recordingIDs),
				"mode": strings.TrimSpace(*mode), "reason": strings.TrimSpace(*reason), "dry_run": !*apply,
			}))
	default:
		log.Fatal(recordingsNASDeliveryUsage)
	}
}
