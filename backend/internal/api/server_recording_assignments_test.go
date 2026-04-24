package api

import (
	"reflect"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/daydemir/stoarama/backend/internal/model"
)

func TestRecordingCapacityGroupModes_SharedCaptureModes(t *testing.T) {
	got := recordingCapacityGroupModes(capture.ExecutionClassVideoLive)
	want := []string{
		capture.ExecutionClassVideoLive,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recordingCapacityGroupModes(video_live)=%v want %v", got, want)
	}
}

func TestRecordingCapacityGroup_SharedCaptureModes(t *testing.T) {
	if got := recordingCapacityGroup(capture.ExecutionClassVideoLive); got != "capture_shared" {
		t.Fatalf("recordingCapacityGroup(video_live)=%q want capture_shared", got)
	}
	if got := recordingCapacityGroup(capture.ExecutionClassImagePoll); got != capture.ExecutionClassImagePoll {
		t.Fatalf("recordingCapacityGroup(image_poll)=%q want %q", got, capture.ExecutionClassImagePoll)
	}
}

func TestRecordingCapacityGroup_NonSharedModes(t *testing.T) {
	if got := recordingCapacityGroup(capture.ExecutionClassYouTubeDirect); got != capture.ExecutionClassYouTubeDirect {
		t.Fatalf("recordingCapacityGroup(youtube_direct)=%q want %q", got, capture.ExecutionClassYouTubeDirect)
	}
}

func TestEffectiveGroupMaxActive_UsesMinimumPositive(t *testing.T) {
	got := effectiveGroupMaxActive(6, map[string]int{
		capture.ExecutionClassVideoLive: 6,
	}, recordingCapacityGroupModes(capture.ExecutionClassVideoLive))
	if got != 6 {
		t.Fatalf("effectiveGroupMaxActive(...)=%d want 6", got)
	}
}

func TestEffectiveGroupMaxActive_IgnoresZeroModeCapacity(t *testing.T) {
	got := effectiveGroupMaxActive(6, map[string]int{
		capture.ExecutionClassVideoLive: 6,
		capture.ExecutionClassImagePoll: 0,
	}, recordingCapacityGroupModes(capture.ExecutionClassVideoLive))
	if got != 6 {
		t.Fatalf("effectiveGroupMaxActive(...)=%d want 6", got)
	}
}

func TestRecordingSharedCapacityInvalidReason_MismatchedCapacities(t *testing.T) {
	reason := recordingSharedCapacityInvalidReason(map[string]recordingCapacityClassState{
		capture.ExecutionClassVideoLive: {
			ExecutionClass: capture.ExecutionClassVideoLive,
			Present:        true,
			MaxActive:      6,
		},
		capture.ExecutionClassImagePoll: {
			ExecutionClass: capture.ExecutionClassImagePoll,
			Present:        true,
			MaxActive:      3,
		},
	}, []string{capture.ExecutionClassVideoLive, capture.ExecutionClassImagePoll})
	if reason == "" {
		t.Fatalf("expected invalid reason for mismatched shared capacity")
	}
}

func TestRecordingSharedCapacityInvalidReason_AllowsPartialSpecialization(t *testing.T) {
	reason := recordingSharedCapacityInvalidReason(map[string]recordingCapacityClassState{
		capture.ExecutionClassVideoLive: {
			ExecutionClass: capture.ExecutionClassVideoLive,
			Present:        true,
			MaxActive:      6,
		},
	}, recordingCapacityGroupModes(capture.ExecutionClassVideoLive))
	if reason != "" {
		t.Fatalf("reason=%q want empty", reason)
	}
}

func TestValidateRecordingHeartbeatSharedCapacity_RejectsMismatchedCapacities(t *testing.T) {
	err := validateRecordingHeartbeatSharedCapacity(map[string]int{
		capture.ExecutionClassVideoLive: 6,
		capture.ExecutionClassImagePoll: 3,
	})
	if err == nil {
		t.Fatalf("expected shared capacity validation error")
	}
}

func TestValidateRecordingHeartbeatSharedCapacity_AllowsMatchingCapacities(t *testing.T) {
	err := validateRecordingHeartbeatSharedCapacity(map[string]int{
		capture.ExecutionClassVideoLive: 6,
		capture.ExecutionClassImagePoll: 6,
	})
	if err != nil {
		t.Fatalf("err=%v want nil", err)
	}
}

func TestBuildRecordingAssignmentAuditIssues_FlagsRecordingStateOff(t *testing.T) {
	stream := model.Stream{
		ID:             3520,
		Provider:       "KBS",
		SourceURL:      "https://example.test/live.m3u8",
		SourceFamily:   capture.SourceFamilyVideoManifest,
		CaptureType:    capture.CaptureTypeHLS,
		ExecutionClass: capture.ExecutionClassVideoLive,
		RecordingState: model.RecordingStateOff,
	}
	assignment := recordingAssignmentRow{
		StreamID:       stream.ID,
		ServerID:       "server-a",
		ExecutionClass: capture.ExecutionClassVideoLive,
	}

	issues := buildRecordingAssignmentAuditIssues(stream, assignment, nil)
	if len(issues) == 0 {
		t.Fatalf("expected at least one issue")
	}
	if issues[0].Code != "recording_state_off" {
		t.Fatalf("issues[0].Code=%q want recording_state_off", issues[0].Code)
	}
}

func TestRecordingAllowedExecutionClasses_YouTubeDirectOnly(t *testing.T) {
	stream := model.Stream{
		ID:             3557,
		Provider:       "GIGAEYES",
		SourceURL:      "https://www.youtube.com/watch?v=abc123",
		SourceFamily:   capture.SourceFamilyWatchPage,
		CaptureType:    capture.CaptureTypeYouTubeWatch,
		ExecutionClass: capture.ExecutionClassYouTubeDirect,
		RecordingState: model.RecordingStateOn,
	}
	requested, allowed, err := recordingAllowedExecutionClasses(stream)
	if err != nil {
		t.Fatalf("recordingAllowedExecutionClasses err=%v", err)
	}
	if requested != capture.ExecutionClassYouTubeDirect {
		t.Fatalf("requested=%q want %q", requested, capture.ExecutionClassYouTubeDirect)
	}
	if !reflect.DeepEqual(allowed, []string{capture.ExecutionClassYouTubeDirect}) {
		t.Fatalf("allowed=%v want [%s]", allowed, capture.ExecutionClassYouTubeDirect)
	}
}

func TestRecordingAllowedExecutionClassesWithPreference_YouTubeDirect(t *testing.T) {
	stream := model.Stream{
		ID:             3557,
		Provider:       "GIGAEYES",
		SourceURL:      "https://www.youtube.com/watch?v=abc123",
		SourceFamily:   capture.SourceFamilyWatchPage,
		CaptureType:    capture.CaptureTypeYouTubeWatch,
		ExecutionClass: capture.ExecutionClassYouTubeRelay,
		RecordingState: model.RecordingStateOn,
	}
	requested, allowed, err := recordingAllowedExecutionClassesWithPreference(stream, capture.ExecutionClassYouTubeDirect)
	if err != nil {
		t.Fatalf("recordingAllowedExecutionClassesWithPreference err=%v", err)
	}
	if requested != capture.ExecutionClassYouTubeDirect {
		t.Fatalf("requested=%q want %q", requested, capture.ExecutionClassYouTubeDirect)
	}
	if !reflect.DeepEqual(allowed, []string{capture.ExecutionClassYouTubeDirect}) {
		t.Fatalf("allowed=%v want [%s]", allowed, capture.ExecutionClassYouTubeDirect)
	}
}
