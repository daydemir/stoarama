package main

import (
	"context"
	"flag"
	"log"
	"strings"
)

// runNASCleanup exposes qualification only. There is intentionally no approval,
// execution, quarantine, release, upload, or deletion command.
func runNASCleanup(ctx context.Context, args []string) {
	if len(args) < 1 || args[0] != "candidate-create" {
		log.Fatal("nas-cleanup requires candidate-create")
	}
	fs := flag.NewFlagSet("nas-cleanup candidate-create", flag.ExitOnError)
	accountID := fs.Int64("account-id", 0, "account containing the paused recordings")
	connectionID := fs.Int64("connection-id", 0, "NAS connection with the complete inventory")
	recordingIDs := fs.String("recording-ids", "", "comma-separated explicit recording IDs")
	cookieFile := fs.String("session-cookie-file", "", "file containing an operator session cookie")
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	_ = fs.Parse(args[1:])
	if len(fs.Args()) != 0 || *accountID <= 0 || *connectionID <= 0 || strings.TrimSpace(*recordingIDs) == "" {
		log.Fatal("--account-id, --connection-id, and --recording-ids are required")
	}
	cookie, err := readCampaignSessionCookie(*cookieFile)
	if err != nil {
		log.Fatal(err)
	}
	printJSON(postRecordingSessionJSON(ctx, *base, cookie, "/api/v1/admin/nas-cleanup/candidates", map[string]any{
		"account_id": *accountID, "connection_id": *connectionID, "recording_ids": parseQualificationIDs(*recordingIDs),
	}))
}
