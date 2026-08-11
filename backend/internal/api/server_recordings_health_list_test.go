package api

import "testing"

func TestAttachRecordingListHealthKeepsCaptureBarsSeparateFromTimeline(t *testing.T) {
	items := []map[string]any{{"id": int64(445)}}
	bins := []recordingHealthBin{{Expected: 60, Captured: 45, Health: "critical"}}
	timeline := recordingTimelineHealth{RecentWindowCount: 2, RecentCoveragePct: float64Ptr(74.71)}

	attachRecordingListHealth(
		items,
		map[int64][]recordingHealthBin{445: bins},
		map[int64]recordingTimelineHealth{445: timeline},
	)

	gotBins, ok := items[0]["capture_health_bins"].([]recordingHealthBin)
	if !ok || len(gotBins) != 1 || gotBins[0].Captured != 45 {
		t.Fatalf("capture_health_bins=%#v", items[0]["capture_health_bins"])
	}
	gotTimeline, ok := items[0]["timeline_health"].(recordingTimelineHealth)
	if !ok || gotTimeline.RecentCoveragePct == nil || *gotTimeline.RecentCoveragePct != 74.71 {
		t.Fatalf("timeline_health=%#v", items[0]["timeline_health"])
	}
}
