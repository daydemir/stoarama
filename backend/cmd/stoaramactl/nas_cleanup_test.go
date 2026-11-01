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
