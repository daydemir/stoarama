package api

import (
	"testing"

	"github.com/daydemir/stoarama/backend/internal/model"
)

func TestResolveInferenceRevision(t *testing.T) {
	tests := []struct {
		name          string
		maxRevision   int
		hasSuccessful bool
		forceRerun    bool
		wantRev       int
		wantErr       bool
	}{
		{
			name:          "first revision",
			maxRevision:   0,
			hasSuccessful: false,
			forceRerun:    false,
			wantRev:       1,
			wantErr:       false,
		},
		{
			name:          "retry after error increments revision",
			maxRevision:   1,
			hasSuccessful: false,
			forceRerun:    false,
			wantRev:       2,
			wantErr:       false,
		},
		{
			name:          "existing success without force rejects",
			maxRevision:   2,
			hasSuccessful: true,
			forceRerun:    false,
			wantRev:       0,
			wantErr:       true,
		},
		{
			name:          "existing success with force increments revision",
			maxRevision:   2,
			hasSuccessful: true,
			forceRerun:    true,
			wantRev:       3,
			wantErr:       false,
		},
		{
			name:          "negative max revision rejects",
			maxRevision:   -1,
			hasSuccessful: false,
			forceRerun:    false,
			wantRev:       0,
			wantErr:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveInferenceRevision(tc.maxRevision, tc.hasSuccessful, tc.forceRerun)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if got != tc.wantRev {
				t.Fatalf("expected revision=%d, got %d", tc.wantRev, got)
			}
		})
	}
}

func TestValidateInferenceCommitSemantics(t *testing.T) {
	tests := []struct {
		name           string
		status         string
		hasDetections  bool
		hasSignals     bool
		hasBoxedIntent bool
		wantErr        bool
	}{
		{
			name:           "success without detections no boxed",
			status:         "success",
			hasDetections:  false,
			hasSignals:     false,
			hasBoxedIntent: false,
			wantErr:        false,
		},
		{
			name:           "success with detections no boxed accepted (backend queues boxing)",
			status:         "success",
			hasDetections:  true,
			hasSignals:     false,
			hasBoxedIntent: false,
			wantErr:        false,
		},
		{
			name:           "success with signals no boxed accepted",
			status:         "success",
			hasDetections:  false,
			hasSignals:     true,
			hasBoxedIntent: false,
			wantErr:        false,
		},
		{
			name:           "success with boxed intent rejected",
			status:         "success",
			hasDetections:  true,
			hasSignals:     false,
			hasBoxedIntent: true,
			wantErr:        true,
		},
		{
			name:           "error cannot include detections",
			status:         "error",
			hasDetections:  true,
			hasSignals:     false,
			hasBoxedIntent: false,
			wantErr:        true,
		},
		{
			name:           "error cannot include signals",
			status:         "error",
			hasDetections:  false,
			hasSignals:     true,
			hasBoxedIntent: false,
			wantErr:        true,
		},
		{
			name:           "error cannot include boxed",
			status:         "error",
			hasDetections:  false,
			hasSignals:     false,
			hasBoxedIntent: true,
			wantErr:        true,
		},
		{
			name:           "error with neither detections nor boxed",
			status:         "error",
			hasDetections:  false,
			hasSignals:     false,
			hasBoxedIntent: false,
			wantErr:        false,
		},
		{
			name:           "invalid status rejected",
			status:         "nope",
			hasDetections:  false,
			hasSignals:     false,
			hasBoxedIntent: false,
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateInferenceCommitSemantics(tc.status, tc.hasDetections, tc.hasSignals, tc.hasBoxedIntent)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}

func TestIsInferenceResultStatus(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{in: "success", want: true},
		{in: "error", want: true},
		{in: "queued_boxed", want: true},
		{in: " queued_boxed ", want: true},
		{in: "unknown", want: false},
	}
	for _, tc := range tests {
		if got := isInferenceResultStatus(tc.in); got != tc.want {
			t.Fatalf("isInferenceResultStatus(%q)=%t want=%t", tc.in, got, tc.want)
		}
	}
}

func TestParseRecordingStateAcceptsOnlyOnOff(t *testing.T) {
	if got, ok := parseRecordingState("off"); !ok || got != model.RecordingStateOff {
		t.Fatalf("expected off to be accepted; got=%q ok=%v", got, ok)
	}
	if got, ok := parseRecordingState("on"); !ok || got != model.RecordingStateOn {
		t.Fatalf("expected on to be accepted; got=%q ok=%v", got, ok)
	}
	if _, ok := parseRecordingState("pending"); ok {
		t.Fatalf("expected pending to be rejected")
	}
	if _, ok := parseRecordingState("failed"); ok {
		t.Fatalf("expected failed to be rejected")
	}
}
