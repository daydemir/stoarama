package main

import (
	"context"
	"net/http"
	"net/http/httptest"
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

// The CLI must dispatch the exact HTTP method and path for each action, and it
// must not fall back to the service token: deletion authority needs the
// controller credential.
func TestNASCleanupMasterDispatchMethodsURLsAndControllerCredential(t *testing.T) {
	type call struct {
		method, base, token, path string
		body                      map[string]any
	}
	var got []call
	do := func(method, base, token, path string, body any) map[string]any {
		payload, _ := body.(map[string]any)
		got = append(got, call{method, base, token, path, payload})
		return map[string]any{"ok": true}
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

// A flag error (unknown flag or --help) must stop the command before a
// credential is resolved or anything is sent: an abbreviated or mistyped
// invocation must never flip deletion authority.
func TestNASCleanupMasterDispatchStopsOnFlagErrors(t *testing.T) {
	sent := 0
	do := func(method, base, token, path string, body any) map[string]any {
		sent++
		return map[string]any{"ok": true}
	}
	cfg := config.Config{NASCleanupControllerToken: "controller-token-32-bytes-long-000000"}
	for _, args := range [][]string{
		{"off", "--reason", "stop", "--help"},
		{"off", "--reason", "stop", "-h"},
		{"status", "--help"},
		{"off", "--reason", "stop", "--api-token", "controller-token-32-bytes-long-000000"},
		{"off", "--reason", "stop", "--unknown-flag"},
		{"off", "--reason"},
	} {
		if err := nasCleanupMasterDispatch(context.Background(), cfg, args, do); err == nil {
			t.Fatalf("flag error %v accepted", args)
		}
	}
	if sent != 0 {
		t.Fatalf("a flag error still sent %d request(s)", sent)
	}
}

// The controller credential comes only from the environment. A token on the
// command line would be visible in shell history and process listings, so the
// flag is gone and its presence is a hard error, not a silent fallback.
func TestNASCleanupMasterDispatchRejectsTokenFlag(t *testing.T) {
	sent := 0
	do := func(method, base, token, path string, body any) map[string]any {
		sent++
		return map[string]any{"ok": true}
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
	do := func(method, base, token, path string, body any) map[string]any {
		sent++
		return map[string]any{"ok": true}
	}
	cfg := config.Config{NASCleanupControllerToken: "controller-token-32-bytes-long-000000"}
	for _, base := range []string{
		"http://api.test",
		"api.test",
		"https://user:pass@api.test",
		"https://api.test/path",
		"https://api.test/?q=1",
		"https://api.test/#f",
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
}
