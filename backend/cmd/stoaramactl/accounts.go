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

func runAccounts(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		printAccountsUsage()
		return
	}
	switch args[0] {
	case "promote-admin":
		fs := flag.NewFlagSet("accounts promote-admin", flag.ExitOnError)
		backendAPIURL := fs.String("backend-api-url", defaultBackendAPIURL(), "backend API base URL")
		apiToken := fs.String("api-token", cfg.APIToken, "backend API token")
		email := fs.String("email", "", "account email")
		asJSON := fs.Bool("json", false, "print JSON")
		_ = fs.Parse(args[1:])
		emailValue := strings.ToLower(strings.TrimSpace(*email))
		if emailValue == "" {
			log.Fatalf("--email is required")
		}
		accounts := mustAPIGet(ctx, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), "/api/v1/admin/accounts")
		items, _ := accounts["items"].([]any)
		matches := make([]map[string]any, 0, 1)
		for _, raw := range items {
			it := asMap(raw)
			if strings.ToLower(strings.TrimSpace(fmt.Sprint(it["email"]))) == emailValue {
				matches = append(matches, it)
			}
		}
		if len(matches) != 1 {
			log.Fatalf("expected exactly one account for %s, found %d", emailValue, len(matches))
		}
		accountID := int64FromAny(matches[0]["id"])
		if accountID <= 0 {
			log.Fatalf("account %s has invalid id", emailValue)
		}
		out := mustAPIRequest(ctx, http.MethodPost, strings.TrimSpace(*backendAPIURL), strings.TrimSpace(*apiToken), fmt.Sprintf("/api/v1/admin/accounts/%d/promote-admin", accountID), map[string]any{})
		if *asJSON {
			printJSON(out)
			return
		}
		fmt.Printf("account promoted email=%s id=%d role=%s\n", emailValue, accountID, fmt.Sprint(out["role"]))
	default:
		log.Fatalf("unknown accounts subcommand: %s", args[0])
	}
}

func printAccountsUsage() {
	fmt.Print(`stoaramactl accounts commands:
  stoaramactl accounts promote-admin --email EMAIL [--backend-api-url URL --api-token TOKEN]
`)
}
