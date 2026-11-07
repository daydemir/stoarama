package main

import (
	"context"
	"encoding/json"
	"github.com/daydemir/stoarama/backend/internal/config"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNASCleanupCLIResumesOnlyReadOnlyPriority(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/api/v1/service/nas-cleanup/plans/42/control" || r.UserAgent() != "Stoarama-Operator/1.0" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("wrong operator request")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["mode"] != "dry_run" || body["priority"] != float64(100) || body["plan_sha256"] != strings.Repeat("b", 64) || body["allow_nas_only"] != false {
			t.Error("read-only resume/priority contract changed")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"mode":"dry_run"}`))
	}))
	defer server.Close()
	runNASCleanup(context.Background(), config.Config{APIToken: "fixture"}, []string{"resume-dry-run", "--plan-id", "42", "--expected-plan-sha256", strings.Repeat("b", 64), "--reason", "read-only priority fixture", "--priority", "100", "--backend-api-url", server.URL})
	if calls != 1 {
		t.Fatal("CLI did not exercise the owner API")
	}
}

func TestNASCleanupCLIRetireSupersededPreviewThenExactCount(t *testing.T) {
	var bodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/v1/service/nas-cleanup/retire-superseded" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("wrong retirement request")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		bodies = append(bodies, body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"plans":2}`))
	}))
	defer server.Close()
	cfg := config.Config{APIToken: "fixture"}
	runNASCleanup(context.Background(), cfg, []string{"retire-superseded", "--reason", "keep28", "--backend-api-url", server.URL})
	runNASCleanup(context.Background(), cfg, []string{"retire-superseded", "--reason", "keep28", "--expected-plans", "2", "--backend-api-url", server.URL})
	if len(bodies) != 2 || bodies[0]["expected_plans"] != nil || bodies[1]["expected_plans"] != float64(2) || bodies[1]["reason"] != "keep28" {
		t.Fatalf("preview/exact retirement contract changed: %v", bodies)
	}
}
