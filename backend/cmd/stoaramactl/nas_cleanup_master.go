package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/user"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/config"
)

type nasCleanupMasterAction string

const (
	nasCleanupMasterStatus nasCleanupMasterAction = "status"
	nasCleanupMasterOn     nasCleanupMasterAction = "on"
	nasCleanupMasterOff    nasCleanupMasterAction = "off"
)

const nasCleanupMasterUsage = `usage: nas-cleanup master status | master on|off --reason "..."`

const nasCleanupMasterPath = "/api/v1/service/nas-cleanup/master"

// nasCleanupMasterBody validates an on/off change into its audited API body.
func nasCleanupMasterBody(action nasCleanupMasterAction, reason, actor string) (map[string]any, error) {
	if action != nasCleanupMasterOn && action != nasCleanupMasterOff {
		return nil, errors.New(nasCleanupMasterUsage)
	}
	if strings.TrimSpace(reason) == "" {
		return nil, errors.New("master " + string(action) + " requires --reason")
	}
	return map[string]any{"enabled": action == nasCleanupMasterOn, "actor": actor, "reason": reason}, nil
}

// The durable NAS cleanup deletion master switch. on/off print the before and
// after state; repeating the current state changes nothing.
func runNASCleanupMaster(ctx context.Context, cfg config.Config, args []string) {
	if len(args) < 1 {
		log.Fatal(nasCleanupMasterUsage)
	}
	action := nasCleanupMasterAction(args[0])
	fs := flag.NewFlagSet("nas-cleanup master "+args[0], flag.ExitOnError)
	reason := fs.String("reason", "", "audited reason; on/off only")
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend URL")
	token := fs.String("api-token", cfg.APIToken, "service operator API token")
	_ = fs.Parse(args[1:])
	if fs.NArg() != 0 {
		log.Fatal(nasCleanupMasterUsage)
	}
	if action == nasCleanupMasterStatus {
		if *reason != "" {
			log.Fatal(nasCleanupMasterUsage)
		}
		printJSON(mustAPIGet(ctx, *base, *token, nasCleanupMasterPath))
		return
	}
	u, err := user.Current()
	if err != nil {
		log.Fatal("operator identity unavailable")
	}
	host, err := os.Hostname()
	if err != nil {
		log.Fatal("operator host unavailable")
	}
	body, err := nasCleanupMasterBody(action, *reason, u.Username+"@"+host)
	if err != nil {
		log.Fatal(err)
	}
	printJSON(mustAPIRequest(ctx, http.MethodPost, *base, *token, nasCleanupMasterPath, body))
}
