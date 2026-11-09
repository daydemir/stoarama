package main

import (
	"context"
	"encoding/json"
	"github.com/daydemir/stoarama/backend/internal/config"
	"net/http"
	"net/http/httptest"
	"os"
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

// The same vector is pinned in internal/api (cleanupBatchDigest) and in
// backend/scripts/nas-cleanup/test_prepare_keep28_plans.py (batch_digest).
const batchDigestVector = "f2e8d37dca0fcb75e902fc2a1e4271022dc8cc23b5f6f1d4cbca53b9be8e3328"

func batchVectorManifest(digest string) []byte {
	b, _ := json.Marshal(map[string]any{"policy_sha256": "060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811", "batch_sha256": digest,
		"plans": []map[string]any{{"id": 1388, "plan_sha256": "a4a8bc7137d732ec9bf68df6e823b7ce4527e7cb4a13afdd05b87bc544601ccc"}, {"id": 1389, "plan_sha256": strings.Repeat("b", 64)}}})
	return b
}

func TestNASCleanupCLIEnableBatchSendsExactApprovedDigest(t *testing.T) {
	if _, _, err := nasCleanupBatchManifest(batchVectorManifest(strings.Repeat("0", 64))); err == nil {
		t.Fatal("manifest with a wrong digest field accepted")
	}
	ids, digest, err := nasCleanupBatchManifest(batchVectorManifest(batchDigestVector))
	if err != nil || digest != batchDigestVector || len(ids) != 2 || ids[0] != 1388 {
		t.Fatalf("batch digest differs from the shared vector: %v %s %v", ids, digest, err)
	}
	path := t.TempDir() + "/batch.json"
	if err := os.WriteFile(path, batchVectorManifest(batchDigestVector), 0o600); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/v1/service/nas-cleanup/batches" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("wrong batch request")
		}
		json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"mode":"enabled"}`))
	}))
	defer server.Close()
	runNASCleanup(context.Background(), config.Config{APIToken: "fixture"}, []string{"enable-batch", "--batch-input", path, "--expected-batch-sha256", batchDigestVector, "--reason", "owner approved streams 3", "--approval-hours", "12", "--backend-api-url", server.URL})
	if body["batch_sha256"] != batchDigestVector || body["approval_hours"] != float64(12) || len(body["plan_ids"].([]any)) != 2 {
		t.Fatalf("batch enable request %v", body)
	}
}
