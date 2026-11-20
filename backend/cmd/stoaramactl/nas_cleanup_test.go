package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestOperatorAPIRequestsIdentifyTheClient(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.UserAgent() != "Stoarama-Operator/1.0" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("operator request lost explicit client identity/auth")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	got := mustAPIGet(context.Background(), server.URL, "fixture", "/api/v1/service/nas-cleanup/plans/1")
	if calls != 1 || got["ok"] != true {
		t.Fatal("operator request did not complete")
	}
}

func TestNASCleanupMasterRequiresReasonAndTypedState(t *testing.T) {
	for _, reason := range []string{"", "  "} {
		for _, action := range []nasCleanupMasterAction{nasCleanupMasterOn, nasCleanupMasterOff} {
			if _, err := nasCleanupMasterBody(action, reason, "ops@host"); err == nil {
				t.Fatalf("master %s accepted reason %q", action, reason)
			}
		}
	}
	for _, action := range []nasCleanupMasterAction{nasCleanupMasterStatus, "yes", "ON"} {
		if _, err := nasCleanupMasterBody(action, "why", "ops@host"); err == nil {
			t.Fatalf("master %q accepted as a change", action)
		}
	}
	for action, want := range map[nasCleanupMasterAction]bool{nasCleanupMasterOn: true, nasCleanupMasterOff: false} {
		body, err := nasCleanupMasterBody(action, "plan 7", "ops@host")
		if err != nil || body["enabled"] != want || body["reason"] != "plan 7" || body["claimed_by"] != "ops@host" {
			t.Fatalf("master %s body %v %v", action, body, err)
		}
	}
}

// masterReply builds the exact typed controller reply for one action, so the
// dispatch tests exercise the same response contract the API serves.
func masterReply(before bool, on bool) map[string]any {
	return map[string]any{
		"before":  map[string]any{"enabled": before},
		"after":   map[string]any{"enabled": on, "changed_at": "2026-11-20T00:00:00Z", "actor": "nas-cleanup-controller", "claimed_by": "ops@host", "reason": "why"},
		"changed": before != on,
	}
}

// The CLI must dispatch the exact HTTP method and path for each action, and it
// must not fall back to the service token: deletion authority needs the
// controller credential.
func TestNASCleanupMasterDispatchMethodsURLsAndControllerCredential(t *testing.T) {
	type call struct {
		method, base, token, path string
		body                      map[string]any
	}
	var got []call
	do := func(method, base, token, path string, body any) (map[string]any, error) {
		payload, _ := body.(map[string]any)
		got = append(got, call{method, base, token, path, payload})
		if payload == nil {
			return masterReply(true, true), nil
		}
		return masterReply(false, payload["enabled"].(bool)), nil
	}
	cfg := config.Config{NASCleanupControllerToken: "controller-token-32-bytes-long-000000", ServiceToken: "runner-service-token", APIToken: "runner-service-token"}
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"status", "--backend-api-url", "https://api.test"}, do); err != nil {
		t.Fatal(err)
	}
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"off", "--reason", "stop now", "--backend-api-url", "https://api.test"}, do); err != nil {
		t.Fatal(err)
	}
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"on", "--reason", "  start  ", "--backend-api-url", "https://api.test"}, do); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("dispatch calls %+v", got)
	}
	if got[0].method != http.MethodGet || got[0].path != "/api/v1/service/nas-cleanup/master" || got[0].base != "https://api.test" || got[0].body != nil {
		t.Fatalf("status call %+v", got[0])
	}
	if got[1].method != http.MethodPost || got[1].path != "/api/v1/service/nas-cleanup/master" || got[1].body["enabled"] != false || got[1].body["reason"] != "stop now" {
		t.Fatalf("off call %+v", got[1])
	}
	if got[2].body["enabled"] != true || got[2].body["reason"] != "start" {
		t.Fatalf("on call %+v", got[2])
	}
	for _, c := range got {
		if c.token != cfg.NASCleanupControllerToken {
			t.Fatalf("dispatch used a non-controller credential: %q", c.token)
		}
		if _, forged := c.body["actor"]; forged {
			t.Fatalf("client supplied the authoritative actor: %+v", c.body)
		}
	}
	// Blank reasons are rejected after trimming, and no call is made.
	got = nil
	for _, reason := range []string{"", "   ", "\t"} {
		if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"on", "--reason", reason}, do); err == nil {
			t.Fatalf("blank reason %q accepted", reason)
		}
	}
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"off"}, do); err == nil {
		t.Fatal("missing reason accepted")
	}
	if len(got) != 0 {
		t.Fatalf("rejected reason still dispatched: %+v", got)
	}
	// Without a controller credential nothing is sent: the service token is
	// never a fallback.
	noCredential := config.Config{ServiceToken: cfg.ServiceToken, APIToken: cfg.APIToken}
	if err := nasCleanupMasterDispatch(context.Background(), noCredential, []string{"off", "--reason", "x", "--backend-api-url", "https://api.test"}, do); err == nil {
		t.Fatal("master change dispatched without the controller credential")
	}
	if len(got) != 0 {
		t.Fatalf("dispatch without controller credential: %+v", got)
	}
}

// A reply that does not carry the requested authoritative state must fail, even
// when the transport returned 2xx: an operator never sees success for a switch
// that did not move where they asked.
func TestNASCleanupMasterDispatchRequiresRequestedTypedState(t *testing.T) {
	cfg := config.Config{NASCleanupControllerToken: "controller-token-32-bytes-long-000000"}
	for name, reply := range map[string]map[string]any{
		"state unchanged":             masterReply(false, false),
		"opposite of request":         masterReply(true, false),
		"no after":                    {"changed": true},
		"after without bool":          {"before": map[string]any{"enabled": false}, "after": map[string]any{"actor": "x"}},
		"wrong after type":            {"after": "on"},
		"empty reply":                 {},
		"status-only shape":           {"ok": true},
		"other enabled value":         {"after": map[string]any{"enabled": "true"}},
		"explicit null":               {"after": map[string]any{"enabled": nil}},
		"null after":                  {"after": nil},
		"before but no after enabled": {"before": map[string]any{"enabled": false}, "after": map[string]any{}},
	} {
		do := func(method, base, token, path string, body any) (map[string]any, error) {
			return reply, nil
		}
		if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"on", "--reason", "why", "--backend-api-url", "https://api.test"}, do); err == nil {
			t.Fatalf("%s accepted as a proven ON", name)
		}
	}
	// A missing or null after.enabled must fail for OFF too, not read as false.
	for name, reply := range map[string]map[string]any{
		"off with null enabled":    {"after": map[string]any{"enabled": nil}, "changed": true},
		"off with no after":        {"changed": true},
		"off with no after object": {"ok": true},
	} {
		do := func(method, base, token, path string, body any) (map[string]any, error) {
			return reply, nil
		}
		if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"off", "--reason", "why", "--backend-api-url", "https://api.test"}, do); err == nil {
			t.Fatalf("%s accepted as a proven OFF", name)
		}
	}
	// A repeated ON (nothing to change) still proves the switch is ON.
	do := func(method, base, token, path string, body any) (map[string]any, error) {
		return masterReply(true, true), nil
	}
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"on", "--reason", "why", "--backend-api-url", "https://api.test"}, do); err != nil {
		t.Fatalf("idempotent ON rejected: %v", err)
	}
	// A status read needs no state at all.
	do = func(method, base, token, path string, body any) (map[string]any, error) {
		return map[string]any{"enabled": false}, nil
	}
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"status", "--backend-api-url", "https://api.test"}, do); err != nil {
		t.Fatalf("status read rejected: %v", err)
	}
}

// A flag error (unknown flag or --help) must stop the command before a
// credential is resolved or anything is sent: an abbreviated or mistyped
// invocation must never flip deletion authority.
func TestNASCleanupMasterDispatchStopsOnFlagErrors(t *testing.T) {
	sent := 0
	do := func(method, base, token, path string, body any) (map[string]any, error) {
		sent++
		return masterReply(false, false), nil
	}
	cfg := config.Config{NASCleanupControllerToken: "controller-token-32-bytes-long-000000"}
	for _, args := range [][]string{
		{"off", "--reason", "stop", "--help"},
		{"off", "--reason", "stop", "-h"},
		{"status", "--help"},
		{"off", "--reason", "stop", "--api-token", "controller-token-32-bytes-long-000000"},
		{"off", "--reason", "stop", "--unknown-flag"},
		{"off", "--reason"},
		{"off", "--reason", "stop", "--backend"},
		{"off", "--reason", "stop", "extra"},
		{"agent", "--reason", "stop"},
		{"off", "--reason", "a", "--reason", "b", "--backend-api-url", "https://api.test"},
		{"off", "--reason=a", "--reason=b", "--backend-api-url", "https://api.test"},
		{"status", "--backend-api-url", "https://a.test", "--backend-api-url", "https://b.test"},
		{"status", "--backend-api-url=https://a.test", "--backend-api-url=https://b.test"},
		{},
	} {
		if err := nasCleanupMasterDispatch(context.Background(), cfg, args, do); err == nil {
			t.Fatalf("flag error %v accepted", args)
		}
	}
	if sent != 0 {
		t.Fatalf("a flag error still sent %d request(s)", sent)
	}
}

// `--reason --help` is ambiguous: the value looks like a flag, so it is refused
// before dispatch rather than recorded as an audited reason.
func TestNASCleanupMasterDispatchRejectsFlagShapedReason(t *testing.T) {
	sent := 0
	do := func(method, base, token, path string, body any) (map[string]any, error) {
		sent++
		return masterReply(false, false), nil
	}
	cfg := config.Config{NASCleanupControllerToken: "controller-token-32-bytes-long-000000"}
	for _, args := range [][]string{
		{"off", "--reason", "--help"},
		{"off", "--reason=--help"},
		{"on", "--reason", "--backend-api-url", "https://api.test", "--backend-api-url", "https://api.test"},
		{"on", "--reason", "-h", "--backend-api-url", "https://api.test"},
	} {
		if err := nasCleanupMasterDispatch(context.Background(), cfg, args, do); err == nil {
			t.Fatalf("flag-shaped reason %v accepted", args)
		}
	}
	if sent != 0 {
		t.Fatalf("flag-shaped reason still dispatched: %d", sent)
	}
}

// The controller credential comes only from the environment. A token on the
// command line would be visible in shell history and process listings, so the
// flag is gone and its presence is a hard error, not a silent fallback.
func TestNASCleanupMasterDispatchRejectsTokenFlag(t *testing.T) {
	sent := 0
	do := func(method, base, token, path string, body any) (map[string]any, error) {
		sent++
		return masterReply(false, false), nil
	}
	if err := nasCleanupMasterDispatch(context.Background(), config.Config{}, []string{"status", "--api-token", "fixture-token-32-bytes-long-0000000", "--backend-api-url", "https://api.test"}, do); err == nil {
		t.Fatal("token-on-command-line flag accepted")
	}
	if err := nasCleanupMasterDispatch(context.Background(), config.Config{}, []string{"off", "--reason", "x", "--token", "fixture-token-32-bytes-long-0000000", "--backend-api-url", "https://api.test"}, do); err == nil {
		t.Fatal("token-on-command-line alias accepted")
	}
	if sent != 0 {
		t.Fatalf("token flag still dispatched: %d", sent)
	}
}

// The controller credential must never cross a plaintext or redirectable
// connection, so only a bare HTTPS origin is accepted.
func TestNASCleanupMasterDispatchRequiresHTTPSOrigin(t *testing.T) {
	sent := 0
	do := func(method, base, token, path string, body any) (map[string]any, error) {
		sent++
		return masterReply(true, true), nil
	}
	cfg := config.Config{NASCleanupControllerToken: "controller-token-32-bytes-long-000000"}
	for _, base := range []string{
		"http://api.test",
		"api.test",
		"https://user:pass@api.test",
		"https://api.test/path",
		"https://api.test/?q=1",
		"https://api.test/#f",
		"ftp://api.test",
		"https:///nohost",
		"",
	} {
		if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"status", "--backend-api-url", base}, do); err == nil {
			t.Fatalf("unsafe backend URL %q accepted", base)
		}
	}
	if sent != 0 {
		t.Fatalf("unsafe backend URL still dispatched: %d", sent)
	}
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"status", "--backend-api-url", "https://api.test"}, do); err != nil || sent != 1 {
		t.Fatalf("HTTPS origin rejected: %v sent=%d", err, sent)
	}
	// A trailing slash is the same origin, normalized for the request URL.
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"status", "--backend-api-url", "https://api.test/"}, do); err != nil || sent != 2 {
		t.Fatalf("HTTPS origin with trailing slash rejected: %v sent=%d", err, sent)
	}
}

// The only documented origin env var is BACKEND_API_URL. INFERCTL_API_URL is
// not documented for the controller credential, so it must never be used.
func TestNASCleanupMasterDefaultURLIgnoresInferctl(t *testing.T) {
	t.Setenv("INFERCTL_API_URL", "https://inferctl.test")
	t.Setenv("BACKEND_API_URL", "")
	if got := nasCleanupMasterDefaultBaseURL(); got != "" {
		t.Fatalf("undocumented INFERCTL origin used: %q", got)
	}
	sent := 0
	do := func(method, base, token, path string, body any) (map[string]any, error) {
		sent++
		return masterReply(true, true), nil
	}
	cfg := config.Config{NASCleanupControllerToken: "controller-token-32-bytes-long-000000"}
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"status"}, do); err == nil || sent != 0 {
		t.Fatalf("status dispatched with only INFERCTL_API_URL: err=%v sent=%d", err, sent)
	}
	t.Setenv("BACKEND_API_URL", "https://documented.test")
	if got := nasCleanupMasterDefaultBaseURL(); got != "https://documented.test" {
		t.Fatalf("BACKEND_API_URL not honoured: %q", got)
	}
	if err := nasCleanupMasterDispatch(context.Background(), cfg, []string{"status"}, do); err != nil || sent != 1 {
		t.Fatalf("documented default URL rejected: %v sent=%d", err, sent)
	}
}

// The dedicated controller request must refuse redirects: a 3xx could replay
// the bearer credential at another origin or path. Only a status code is ever
// reported, never a response body that might echo operator text.
func TestNASCleanupMasterRequestRefusesRedirectsAndLeaksNothing(t *testing.T) {
	var reached int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached++
		w.Write([]byte(`{"after":{"enabled":true}}`))
	}))
	defer target.Close()
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/api/v1/service/nas-cleanup/master", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	ctx := context.Background()
	// Trust the disposable TLS origin only; the redirect target is plain HTTP so
	// following it would be observable even without certificate trust.
	oldTransport := nasCleanupMasterTransport
	nasCleanupMasterTransport = origin.Client().Transport
	defer func() { nasCleanupMasterTransport = oldTransport }()
	_, err := nasCleanupMasterRequest(ctx, http.MethodGet, origin.URL, "controller-token-32-bytes-long-000000", "/api/v1/service/nas-cleanup/master", nil)
	if err == nil {
		t.Fatal("redirect was followed")
	}
	if reached != 0 {
		t.Fatalf("redirect reached the credential's target: %d", reached)
	}
	if strings.Contains(err.Error(), "controller-token") {
		t.Fatalf("error leaked the credential: %v", err)
	}
}

// A controller status read is a bodyless GET. The coding error of attaching a
// payload to it must fail locally instead of reaching the server.
func TestNASCleanupMasterRequestGetCarriesNoBody(t *testing.T) {
	var reached int
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached++
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"enabled":false}`))
	}))
	defer origin.Close()
	oldTransport := nasCleanupMasterTransport
	nasCleanupMasterTransport = origin.Client().Transport
	defer func() { nasCleanupMasterTransport = oldTransport }()
	ctx := context.Background()
	if _, err := nasCleanupMasterRequest(ctx, http.MethodGet, origin.URL, "controller-token-32-bytes-long-000000", nasCleanupMasterPath, map[string]any{"enabled": true}); err == nil {
		t.Fatal("a GET with a body was allowed")
	}
	if reached != 0 {
		t.Fatalf("a body-carrying GET reached the server: %d", reached)
	}
	if _, err := nasCleanupMasterRequest(ctx, http.MethodGet, origin.URL, "controller-token-32-bytes-long-000000", nasCleanupMasterPath, nil); err != nil {
		t.Fatalf("bodyless GET rejected: %v", err)
	}
	if reached != 1 {
		t.Fatalf("bodyless GET did not reach the server: %d", reached)
	}
}

// Repeated flags are refused before dispatch; a single occurrence still works.
func TestNASCleanupMasterRepeatedFlag(t *testing.T) {
	for _, args := range [][]string{
		{"--reason", "a", "--reason", "b"},
		{"--reason=a", "--reason=b"},
		{"--backend-api-url", "https://a.test", "--backend-api-url", "https://b.test"},
		{"--backend-api-url=https://a.test", "--backend-api-url=https://b.test"},
	} {
		if err := nasCleanupMasterRepeatedFlag(args); err == nil {
			t.Fatalf("repeated flag %v accepted", args)
		}
	}
	for _, args := range [][]string{
		{"--reason", "a"},
		{"--reason", "a", "--backend-api-url", "https://api.test"},
		{"--unknown", "--unknown"},
	} {
		if err := nasCleanupMasterRepeatedFlag(args); err != nil {
			t.Fatalf("distinct/unknown flags %v refused: %v", args, err)
		}
	}
}
