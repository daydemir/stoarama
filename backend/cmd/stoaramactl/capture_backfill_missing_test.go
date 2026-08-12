package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/captureapi"
	"github.com/daydemir/stoarama/backend/internal/model"
)

func TestBackfillMissingEffectiveModeMapsLegacyRelayToDirect(t *testing.T) {
	stream := model.Stream{
		CaptureType:         "youtube_watch",
		ExecutionClass:      "youtube_relay",
		ExecutionConfigJSON: map[string]any{},
	}

	if got := backfillMissingEffectiveMode(stream); got != capture.ModeYouTubeLive {
		t.Fatalf("mode=%q want %q", got, capture.ModeYouTubeLive)
	}
}

func TestLoadCaptureBackfillMissingTargetsUsesOnlyExplicitIDs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer operator" {
			t.Fatal("missing operator auth")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{
			map[string]any{"stream": map[string]any{"id": 11, "slug": "eleven"}, "captures_success": 7},
			map[string]any{"stream": map[string]any{"id": 12, "slug": "twelve"}, "captures_success": 0},
			map[string]any{"stream": map[string]any{"id": 13, "slug": "thirteen"}, "captures_success": 9},
		}})
	}))
	defer server.Close()
	targets, err := loadCaptureBackfillMissingTargets(context.Background(), server.URL, "operator", 0, []int64{13, 11})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0].Stream.ID != 11 || targets[1].Stream.ID != 13 {
		t.Fatalf("targets=%+v", targets)
	}
	for _, target := range targets {
		if target.BackfillReason != "explicit_refresh" {
			t.Fatalf("reason=%q", target.BackfillReason)
		}
	}
}

func TestExplicitFrameRefreshIngestsAuthoritativeFrameWithoutHeartbeatOrLeak(t *testing.T) {
	var ingested map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/frame.jpg", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/jpeg")
		img := image.NewRGBA(image.Rect(0, 0, 2, 2))
		img.Set(0, 0, color.White)
		if err := jpeg.Encode(w, img, nil); err != nil {
			t.Fatal(err)
		}
	})
	mux.HandleFunc("/api/v1/capture/ingest", func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&ingested); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := captureapi.NewClient(captureapi.ClientConfig{BaseURL: server.URL, APIToken: "operator", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := capture.NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	target := captureBackfillMissingCandidate{Stream: model.Stream{ID: 339, Slug: "secret-token-should-not-appear", Provider: "direct", SourceURL: server.URL + "/frame.jpg?token=secret", CaptureType: "snapshot_url", ExecutionClass: "image_poll"}, BackfillReason: "explicit_refresh"}
	result := processCaptureBackfillMissingTarget(context.Background(), registry, client, target, 5*time.Second, false)
	if result.Status != "success" {
		t.Fatalf("result=%+v", result)
	}
	if ingested["stream_id"] != float64(339) || ingested["recording_heartbeat"] != false || ingested["source_kind"] != "backfill_missing_frame" {
		t.Fatalf("ingest=%v", ingested)
	}
	frame, err := base64.StdEncoding.DecodeString(ingested["frame_base64"].(string))
	if err != nil || len(frame) == 0 {
		t.Fatalf("frame binding err=%v", err)
	}
	out, _ := json.Marshal(result)
	if strings.Contains(string(out), "token=secret") || strings.Contains(string(out), server.URL) {
		t.Fatalf("result leaked source: %s", out)
	}
}

func TestExplicitFrameRefreshDryRunDoesNotIngestAndFailureIsIsolated(t *testing.T) {
	var ingests int
	mux := http.NewServeMux()
	mux.HandleFunc("/good.jpg", func(w http.ResponseWriter, _ *http.Request) {
		img := image.NewRGBA(image.Rect(0, 0, 1, 1))
		_ = jpeg.Encode(w, img, nil)
	})
	mux.HandleFunc("/api/v1/capture/ingest", func(w http.ResponseWriter, _ *http.Request) {
		ingests++
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, _ := captureapi.NewClient(captureapi.ClientConfig{BaseURL: server.URL, APIToken: "operator", HTTPClient: server.Client()})
	registry, _ := capture.NewDefaultRegistry()
	bad := captureBackfillMissingCandidate{Stream: model.Stream{ID: 1, Provider: "direct", SourceURL: "http://127.0.0.1:1/private?token=secret", CaptureType: "snapshot_url", ExecutionClass: "image_poll"}}
	badResult := processCaptureBackfillMissingTarget(context.Background(), registry, client, bad, 100*time.Millisecond, false)
	if badResult.Status != "error" || strings.Contains(badResult.Reason, "secret") {
		t.Fatalf("bad=%+v", badResult)
	}
	good := captureBackfillMissingCandidate{Stream: model.Stream{ID: 2, Provider: "direct", SourceURL: server.URL + "/good.jpg", CaptureType: "snapshot_url", ExecutionClass: "image_poll"}}
	dry := processCaptureBackfillMissingTarget(context.Background(), registry, client, good, time.Second, true)
	if dry.Status != "dry_run" || ingests != 0 {
		t.Fatalf("dry=%+v ingests=%d", dry, ingests)
	}
	success := processCaptureBackfillMissingTarget(context.Background(), registry, client, good, time.Second, false)
	if success.Status != "success" || ingests != 1 {
		t.Fatalf("success=%+v ingests=%d", success, ingests)
	}
}
