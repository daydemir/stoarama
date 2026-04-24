package main

import "testing"

func TestProposeStreamV2MigrationYouTubeWatch(t *testing.T) {
	item := proposeStreamV2Migration(streamV2MigrationRow{
		ID:             1,
		Provider:       "GIGAEYES",
		SourceURL:      "https://www.youtube.com/watch?v=abc123",
		SourceFamily:   "video_stream",
		CaptureType:    "http_video",
		ExecutionClass: "video_live",
	})

	if item.ProposedCaptureType != "youtube_watch" {
		t.Fatalf("capture_type=%q want youtube_watch", item.ProposedCaptureType)
	}
	if item.ProposedExecutionClass != "youtube_direct" {
		t.Fatalf("execution_class=%q want youtube_direct", item.ProposedExecutionClass)
	}
	if item.ProposedSourceFamily != "watch_page" {
		t.Fatalf("source_family=%q want watch_page", item.ProposedSourceFamily)
	}
	if !item.WouldChange {
		t.Fatalf("would_change=false want true")
	}
	if item.ReviewRequired {
		t.Fatalf("review_required=true want false")
	}
}

func TestProposeStreamV2MigrationPrefersYouTubeIdentityOverResolvedHLS(t *testing.T) {
	item := proposeStreamV2Migration(streamV2MigrationRow{
		ID:                  5,
		Provider:            "GIGAEYES",
		SourceURL:           "https://www.youtube.com/watch?v=abc123",
		SourceFamily:        "video_manifest",
		CaptureType:         "hls",
		ExecutionClass:      "video_live",
		ResolvedCaptureType: "hls",
		ResolvedURL:         "https://manifest.googlevideo.com/api/manifest.m3u8",
	})

	if item.ProposedCaptureType != "youtube_watch" {
		t.Fatalf("capture_type=%q want youtube_watch", item.ProposedCaptureType)
	}
	if item.ProposedExecutionClass != "youtube_direct" {
		t.Fatalf("execution_class=%q want youtube_direct", item.ProposedExecutionClass)
	}
	if item.ProposedSourceFamily != "watch_page" {
		t.Fatalf("source_family=%q want watch_page", item.ProposedSourceFamily)
	}
}

func TestProposeStreamV2MigrationMovesRelayToDirect(t *testing.T) {
	item := proposeStreamV2Migration(streamV2MigrationRow{
		ID:             2,
		Provider:       "YouTube",
		SourceURL:      "https://youtu.be/abc123",
		SourceFamily:   "watch_page",
		CaptureType:    "youtube_watch",
		ExecutionClass: "youtube_relay",
	})

	if item.ProposedExecutionClass != "youtube_direct" {
		t.Fatalf("execution_class=%q want youtube_direct", item.ProposedExecutionClass)
	}
	if item.ReviewRequired {
		t.Fatalf("review_required=true want false")
	}
}

func TestProposeStreamV2MigrationUsesResolvedCaptureType(t *testing.T) {
	item := proposeStreamV2Migration(streamV2MigrationRow{
		ID:                  3,
		Provider:            "TOPIS",
		SourceURL:           "https://example.com/video",
		SourceFamily:        "video_stream",
		CaptureType:         "http_video",
		ExecutionClass:      "video_live",
		ResolvedCaptureType: "hls",
		ResolvedURL:         "https://example.com/playlist.m3u8",
	})

	if item.ProposedCaptureType != "hls" {
		t.Fatalf("capture_type=%q want hls", item.ProposedCaptureType)
	}
	if item.ProposedSourceFamily != "video_manifest" {
		t.Fatalf("source_family=%q want video_manifest", item.ProposedSourceFamily)
	}
}

func TestProposeStreamV2MigrationRequiresReviewForUnknown(t *testing.T) {
	item := proposeStreamV2Migration(streamV2MigrationRow{
		ID:             4,
		Provider:       "Unknown",
		SourceURL:      "",
		SourcePageURL:  "",
		SourceFamily:   "",
		CaptureType:    "",
		ExecutionClass: "",
	})

	if !item.ReviewRequired {
		t.Fatalf("review_required=false want true")
	}
	if item.ProposedCaptureType != "unknown" {
		t.Fatalf("capture_type=%q want unknown", item.ProposedCaptureType)
	}
}
