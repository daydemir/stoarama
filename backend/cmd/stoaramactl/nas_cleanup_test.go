package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
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
		if err != nil || body["enabled"] != want || body["reason"] != "plan 7" || body["actor"] != "ops@host" {
			t.Fatalf("master %s body %v %v", action, body, err)
		}
	}
}
