package main

import (
	"testing"

	"github.com/daydemir/stoarama/backend/internal/capture"
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
