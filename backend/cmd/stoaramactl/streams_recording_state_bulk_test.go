package main

import (
	"testing"

	"github.com/daydemir/stoarama/backend/internal/model"
)

func TestRecordingStateBulkCandidateFromAPIItem(t *testing.T) {
	candidate, ok := recordingStateBulkCandidateFromAPIItem(map[string]any{
		"stream": map[string]any{
			"id":              float64(123),
			"provider":        "SDOT",
			"name":            "Seattle",
			"recording_state": "on",
		},
	})
	if !ok {
		t.Fatalf("expected candidate")
	}
	if candidate.ID != 123 || candidate.Provider != "SDOT" || candidate.RecordingState != model.RecordingStateOn {
		t.Fatalf("candidate=%+v", candidate)
	}
}

func TestRecordingStateBulkCandidateFromAPIItemRejectsOff(t *testing.T) {
	_, ok := recordingStateBulkCandidateFromAPIItem(map[string]any{
		"stream": map[string]any{
			"id":              float64(123),
			"provider":        "SDOT",
			"recording_state": "off",
		},
	})
	if ok {
		t.Fatalf("off stream should not be candidate")
	}
}
