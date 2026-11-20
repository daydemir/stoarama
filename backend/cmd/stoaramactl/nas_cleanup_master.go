package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"strings"
	"time"

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
	if strings.HasPrefix(reason, "-") {
		// `--reason --help` (or any flag-shaped value) is ambiguous: the
		// operator may have meant a flag, so refuse rather than record it.
		return nil, errors.New("--reason must be a plain reason, not a flag")
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

// nasCleanupMasterDefaultBaseURL is only the documented BACKEND_API_URL. The
// generic CLI default also consults INFERCTL_API_URL, which is not a documented
// origin for the controller credential, so it is never used here.
func nasCleanupMasterDefaultBaseURL() string {
	return strings.TrimSpace(os.Getenv("BACKEND_API_URL"))
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
	return "https://" + u.Host, nil
}

// nasCleanupMasterChange is the typed reply the API returns for a master flip.
// Only the authoritative `after.enabled` value proves the requested state, so
// every field is a pointer: a missing field and an explicit JSON null both
// decode to nil and are refused identically for OFF and ON. A plain bool would
// silently accept either as false and "confirm" an OFF that never happened.
type nasCleanupMasterChange struct {
	Before *struct {
		Enabled *bool `json:"enabled"`
	} `json:"before"`
	After *struct {
		Enabled   *bool   `json:"enabled"`
		ChangedAt *string `json:"changed_at"`
		Actor     *string `json:"actor"`
		ClaimedBy *string `json:"claimed_by"`
		Reason    *string `json:"reason"`
	} `json:"after"`
	Changed bool `json:"changed"`
}

// nasCleanupMasterRepeatedFlag refuses any flag supplied more than once, in
// either `--flag value` or `--flag=value` form. Only the two flags this command
// defines are considered, so the check can run before parsing and without a
// flag set.
func nasCleanupMasterRepeatedFlag(args []string) error {
	seen := map[string]bool{}
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") || arg == "-" || arg == "--" {
			continue
		}
		name := strings.TrimLeft(arg, "-")
		if n := strings.IndexByte(name, '='); n >= 0 {
			name = name[:n]
		}
		switch name {
		case "reason", "backend-api-url":
		default:
			continue // unknown flags are the flag set's error, not ours
		}
		if seen[name] {
			return errors.New("--" + name + " given more than once")
		}
		seen[name] = true
	}
	return nil
}

// nasCleanupMasterDispatch is the testable command body: it parses args and
// performs the exact GET/POST against nasCleanupMasterPath. Any flag parse
// error (including --help) returns before a credential is resolved or a
// request is sent.
func nasCleanupMasterDispatch(ctx context.Context, cfg config.Config, args []string, do func(method, base, token, path string, body any) (map[string]any, error)) error {
	if len(args) < 1 {
		return errors.New(nasCleanupMasterUsage)
	}
	action := nasCleanupMasterAction(args[0])
	if action != nasCleanupMasterStatus && action != nasCleanupMasterOn && action != nasCleanupMasterOff {
		return errors.New(nasCleanupMasterUsage)
	}
	// Reject a repeated flag before parsing: Go's flag package silently keeps
	// the last value, so `--reason a --reason b` would hide which reason the
	// operator meant to record.
	if err := nasCleanupMasterRepeatedFlag(args[1:]); err != nil {
		return err
	}
	fs := flag.NewFlagSet("nas-cleanup master "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	reason := fs.String("reason", "", "audited reason; on/off only")
	base := fs.String("backend-api-url", nasCleanupMasterDefaultBaseURL(), "backend HTTPS origin; defaults to BACKEND_API_URL")
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
	if action == nasCleanupMasterStatus && strings.TrimSpace(*reason) != "" {
		return errors.New(nasCleanupMasterUsage)
	}
	body := map[string]any(nil)
	if action != nasCleanupMasterStatus {
		u, err := user.Current()
		if err != nil {
			return errors.New("operator identity unavailable")
		}
		host, err := os.Hostname()
		if err != nil {
			return errors.New("operator host unavailable")
		}
		if body, err = nasCleanupMasterBody(action, *reason, u.Username+"@"+host); err != nil {
			return err
		}
	}
	// Resolve the credential only after every argument is known to be valid.
	credential := nasCleanupMasterControllerToken(cfg)
	if credential == "" {
		return errors.New("master changes require the controller credential (NAS_CLEANUP_CONTROLLER_TOKEN)")
	}
	method := http.MethodGet
	if body != nil {
		method = http.MethodPost
	}
	out, err := do(method, endpoint, credential, nasCleanupMasterPath, body)
	if err != nil {
		return err
	}
	if body == nil {
		printJSON(out)
		return nil
	}
	// The change is only proven by the typed authoritative state, not by a 2xx
	// alone, and that state must be exactly what was requested. An idempotent
	// repeat is fine: `changed` only reports whether this call flipped it.
	state, err := nasCleanupMasterState(out)
	if err != nil {
		return err
	}
	if want := action == nasCleanupMasterOn; *state.After.Enabled != want {
		return fmt.Errorf("master %s not applied: server reports enabled=%t", action, *state.After.Enabled)
	}
	printJSON(out)
	return nil
}

// nasCleanupMasterState decodes the typed reply and requires the authoritative
// `after.enabled` boolean to be present as a boolean. A missing `after`, a
// missing field, an explicit JSON null and a non-boolean (such as the string
// "true") are all refused: none of them proves the requested state.
func nasCleanupMasterState(out map[string]any) (nasCleanupMasterChange, error) {
	var state nasCleanupMasterChange
	raw, err := json.Marshal(out)
	if err != nil {
		return state, errors.New("master response not understood")
	}
	if err = json.Unmarshal(raw, &state); err != nil {
		return state, errors.New("master response not understood")
	}
	if state.After == nil || state.After.Enabled == nil {
		return state, errors.New("master response has no authoritative enabled state")
	}
	return state, nil
}

// nasCleanupMasterTransport is only ever set by tests that need a trusted TLS
// peer; production uses the default transport.
var nasCleanupMasterTransport http.RoundTripper

// nasCleanupMasterRefuseRedirect is the controller client's redirect policy: a
// 3xx could replay the bearer credential at another origin or path.
func nasCleanupMasterRefuseRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// nasCleanupMasterRequest performs the controller request itself. It refuses
// every redirect so the bearer credential can never be replayed to another
// origin or path, and it surfaces the HTTP status as an error. A status read is
// a bodyless GET: sending a body with GET is meaningless and some proxies
// reject it, so it is refused here rather than at the server.
func nasCleanupMasterRequest(ctx context.Context, method, base, token, path string, payload any) (map[string]any, error) {
	if strings.TrimSpace(method) == "" || strings.TrimSpace(token) == "" {
		return nil, errors.New("controller request needs a method and credential")
	}
	if strings.EqualFold(method, http.MethodGet) && payload != nil {
		return nil, errors.New("a controller status read carries no body")
	}
	endpoint, err := nasCleanupMasterBaseURL(base)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, errors.New("controller payload could not be encoded")
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint+path, body)
	if err != nil {
		return nil, errors.New("controller request could not be built")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "Stoarama-Operator/1.0")
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{
		Timeout:       45 * time.Second,
		CheckRedirect: nasCleanupMasterRefuseRedirect,
	}
	if nasCleanupMasterTransport != nil {
		client.Transport = nasCleanupMasterTransport
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("controller request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// The status is safe to report; the body may echo operator text.
		return nil, fmt.Errorf("controller request failed with status %d", resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return nil, errors.New("controller response could not be read")
	}
	return out, nil
}

// The durable NAS cleanup deletion master switch. on/off print the before and
// after state; repeating the current state changes nothing.
func runNASCleanupMaster(ctx context.Context, cfg config.Config, args []string) {
	if err := nasCleanupMasterDispatch(ctx, cfg, args, func(method, base, token, path string, body any) (map[string]any, error) {
		return nasCleanupMasterRequest(ctx, method, base, token, path, body)
	}); err != nil {
		log.Fatal(err)
	}
}
