package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestNASCleanupCLIClose(t *testing.T) {
	calls := 0
	wantOverride := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/api/v1/service/nas-cleanup/plans/1389/close" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("wrong service request")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		wantFields := 2
		if wantOverride {
			wantFields++
		}
		if len(body) != wantFields || (body["operator_override"] == true) != wantOverride || body["expected_plan_sha256"] != strings.Repeat("b", 64) || body["reason"] != "split remainder" {
			t.Errorf("close body: %v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"mode":"retired","deletion_enabled":false}`))
	}))
	defer server.Close()
	runNASCleanup(context.Background(), config.Config{APIToken: "fixture"}, []string{"close", "--plan-id", "1389", "--expected-plan-sha256", strings.Repeat("b", 64), "--reason", "split remainder", "--backend-api-url", server.URL})
	wantOverride = true
	runNASCleanup(context.Background(), config.Config{APIToken: "fixture"}, []string{"close", "--plan-id", "1389", "--expected-plan-sha256", strings.Repeat("b", 64), "--reason", "split remainder", "--operator-override", "--backend-api-url", server.URL})
	if calls != 2 {
		t.Fatalf("requests: %d", calls)
	}
}
