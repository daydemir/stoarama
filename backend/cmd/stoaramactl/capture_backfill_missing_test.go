package main

import (
	"testing"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/model"
)

func TestBackfillMissingEffectiveModeFallsBackToDirectWhenRelayHasNoPullURL(t *testing.T) {
	stream := model.Stream{
		CaptureType:         "youtube_watch",
		ExecutionClass:      "youtube_relay",
		ExecutionConfigJSON: map[string]any{},
	}

	if got := backfillMissingEffectiveMode(stream); got != capture.ModeYouTubeLive {
		t.Fatalf("mode=%q want %q", got, capture.ModeYouTubeLive)
	}
}

func TestBackfillMissingEffectiveModeKeepsRelayWhenPullURLExists(t *testing.T) {
	stream := model.Stream{
		CaptureType:    "youtube_watch",
		ExecutionClass: "youtube_relay",
		ExecutionConfigJSON: map[string]any{
			"relay_pull_url": "https://example.invalid/pull",
		},
	}

	if got := backfillMissingEffectiveMode(stream); got != capture.ModeYouTubeRelay {
		t.Fatalf("mode=%q want %q", got, capture.ModeYouTubeRelay)
	}
}
