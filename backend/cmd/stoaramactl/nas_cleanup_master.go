package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
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

const nasCleanupMasterUsage = `usage: nas-cleanup master status | master on|off --reason "..." [--backend-api-url URL]`

const nasCleanupMasterPath = "/api/v1/service/nas-cleanup/master"

// nasCleanupMasterBody validates an on/off change into its audited API body.
// enabled is the operator's intent; claimed_by is the operator name the server
// records as evidence only. The server sets the authoritative actor itself.
func nasCleanupMasterBody(action nasCleanupMasterAction, reason, claimedBy string) (map[string]any, error) {
	if action != nasCleanupMasterOn && action != nasCleanupMasterOff {
		return nil, errors.New(nasCleanupMasterUsage)
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return nil, errors.New("master " + string(action) + " requires --reason")
	}
	return map[string]any{"enabled": action == nasCleanupMasterOn, "claimed_by": strings.TrimSpace(claimedBy), "reason": reason}, nil
}

// nasCleanupMasterControllerToken is the separate credential that may flip
// deletion authority. It is read only from the environment: a token on the
// command line would land in shell history and process listings. The runner's
// service token is deliberately not a fallback: an absent controller token
// fails closed.
func nasCleanupMasterControllerToken(cfg config.Config) string {
	return strings.TrimSpace(cfg.NASCleanupControllerToken)
}

// nasCleanupMasterBaseURL accepts only an absolute HTTPS origin without
// credentials, query or fragment: the controller credential must never travel
// over plaintext or to a redirectable path.
func nasCleanupMasterBaseURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("master changes require an explicit --backend-api-url or BACKEND_API_URL")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") {
		return "", errors.New("backend URL must be an HTTPS origin without credentials, path, query or fragment")
	}
	return raw, nil
}

// nasCleanupMasterDispatch is the testable command body: it parses args and
// performs the exact GET/POST against nasCleanupMasterPath. Any flag parse
// error (including --help) returns before a credential is resolved or a
// request is sent.
func nasCleanupMasterDispatch(ctx context.Context, cfg config.Config, args []string, do func(method, base, token, path string, body any) map[string]any) error {
	if len(args) < 1 {
		return errors.New(nasCleanupMasterUsage)
	}
	action := nasCleanupMasterAction(args[0])
	if action != nasCleanupMasterStatus && action != nasCleanupMasterOn && action != nasCleanupMasterOff {
		return errors.New(nasCleanupMasterUsage)
	}
	fs := flag.NewFlagSet("nas-cleanup master "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	reason := fs.String("reason", "", "audited reason; on/off only")
	base := fs.String("backend-api-url", defaultBackendAPIURL(), "backend HTTPS origin; defaults to BACKEND_API_URL")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New(nasCleanupMasterUsage)
	}
	endpoint, err := nasCleanupMasterBaseURL(*base)
	if err != nil {
		return err
	}
	credential := nasCleanupMasterControllerToken(cfg)
	if credential == "" {
		return errors.New("master changes require the controller credential (NAS_CLEANUP_CONTROLLER_TOKEN)")
	}
	if action == nasCleanupMasterStatus {
		if *reason != "" {
			return errors.New(nasCleanupMasterUsage)
		}
		printJSON(do(http.MethodGet, endpoint, credential, nasCleanupMasterPath, nil))
		return nil
	}
	u, err := user.Current()
	if err != nil {
		return errors.New("operator identity unavailable")
	}
	host, err := os.Hostname()
	if err != nil {
		return errors.New("operator host unavailable")
	}
	body, err := nasCleanupMasterBody(action, *reason, u.Username+"@"+host)
	if err != nil {
		return err
	}
	printJSON(do(http.MethodPost, endpoint, credential, nasCleanupMasterPath, body))
	return nil
}

// The durable NAS cleanup deletion master switch. on/off print the before and
// after state; repeating the current state changes nothing.
func runNASCleanupMaster(ctx context.Context, cfg config.Config, args []string) {
	if err := nasCleanupMasterDispatch(ctx, cfg, args, func(method, base, token, path string, body any) map[string]any {
		return mustAPIRequest(ctx, method, base, token, path, body)
	}); err != nil {
		log.Fatal(err)
	}
}
