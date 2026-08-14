package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/google/uuid"
)

func runRecordingRecovery(ctx context.Context, cfg config.Config, args []string) {
	if len(args) == 0 || args[0] != "security-revoke" {
		log.Fatal("usage: stoaramactl recording-recovery security-revoke --set-id UUID [--ordinal N] --reason suspected_capability_compromise|suspected_seed_compromise --idempotency-key UUID [--backend-api-url URL --api-token TOKEN]")
	}
	fs := flag.NewFlagSet("recording-recovery security-revoke", flag.ExitOnError)
	setID := fs.String("set-id", "", "exact capture reservation set UUID")
	ordinal := fs.Int("ordinal", 0, "optional exact artifact ordinal; zero revokes the whole set")
	reason := fs.String("reason", "", "suspected_capability_compromise or suspected_seed_compromise")
	idempotencyKey := fs.String("idempotency-key", "", "operator-authored immutable request UUID")
	backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
	apiToken := fs.String("api-token", cfg.APIToken, "operator API token")
	_ = fs.Parse(args[1:])
	if _, err := uuid.Parse(strings.TrimSpace(*setID)); err != nil {
		log.Fatalf("--set-id must be a UUID: %v", err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(*idempotencyKey)); err != nil {
		log.Fatalf("--idempotency-key must be a UUID: %v", err)
	}
	if *ordinal < 0 {
		log.Fatal("--ordinal must be positive when supplied")
	}
	if *reason != "suspected_capability_compromise" && *reason != "suspected_seed_compromise" {
		log.Fatal("--reason must be suspected_capability_compromise or suspected_seed_compromise")
	}
	payload := map[string]any{
		"set_id":          strings.TrimSpace(*setID),
		"reason":          *reason,
		"idempotency_key": strings.TrimSpace(*idempotencyKey),
	}
	if *ordinal > 0 {
		payload["ordinal"] = *ordinal
	}
	printJSON(mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/admin/recording/recovery/security-revoke", payload))
}
