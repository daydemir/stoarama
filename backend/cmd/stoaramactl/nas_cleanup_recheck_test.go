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

func TestNASCleanupCLIRecheck(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(map[bool]string{false: "unverified", true: "all"}[all], func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "POST" || r.URL.Path != "/api/v1/service/nas-cleanup/plans/1389/recheck" || r.Header.Get("Authorization") != "Bearer fixture" {
					t.Error("wrong service request")
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				scope := "unverified"
				if all {
					scope = "all"
				}
				if len(body) != 3 || body["expected_plan_sha256"] != strings.Repeat("b", 64) || body["reason"] != "transient timeout" || body["scope"] != scope {
					t.Errorf("recheck body: %v", body)
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"mode":"dry_run","reset_items":1}`))
			}))
			defer server.Close()
			args := []string{"recheck", "--plan-id", "1389", "--expected-plan-sha256", strings.Repeat("b", 64), "--reason", "transient timeout", "--backend-api-url", server.URL}
			if all {
				args = append(args, "--all")
			}
			runNASCleanup(context.Background(), config.Config{APIToken: "fixture"}, args)
			if calls != 1 {
				t.Fatalf("requests: %d", calls)
			}
		})
	}
}
