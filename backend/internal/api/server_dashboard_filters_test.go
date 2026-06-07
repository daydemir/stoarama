package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardBuildStreamWhereInvalidRecordingState(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams?recording_state=bad", nil)
	_, _, err := dashboardBuildStreamWhereFromRequest(req, dashboardStreamWhereConfig{
		IncludeSearch:         true,
		IncludeSource:         true,
		IncludeYouTubeChannel: true,
	})
	if err == nil {
		t.Fatalf("expected error for invalid recording_state")
	}
}

func TestDashboardBuildStreamWhereRejectsPendingRecordingState(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams?recording_state=pending", nil)
	_, _, err := dashboardBuildStreamWhereFromRequest(req, dashboardStreamWhereConfig{
		IncludeSearch:         true,
		IncludeSource:         true,
		IncludeYouTubeChannel: true,
	})
	if err == nil {
		t.Fatalf("expected error for pending recording_state")
	}
}

func TestDashboardBuildStreamWhereCaptureTypeFilter(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams?capture_type=hls", nil)
	where, args, err := dashboardBuildStreamWhereFromRequest(req, dashboardStreamWhereConfig{
		IncludeCaptureMode: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := len(args), 1; got != want {
		t.Fatalf("args len=%d want=%d", got, want)
	}
	if got, want := args[0], "hls"; got != want {
		t.Fatalf("capture_type arg=%v want=%q", got, want)
	}
	sqlWhere := strings.Join(where, " AND ")
	if !strings.Contains(sqlWhere, "s.capture_type=$1") {
		t.Fatalf("where missing capture_type placeholder: %s", sqlWhere)
	}
}

func TestDashboardBuildStreamWhereInvalidCaptureType(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams?capture_type=bad_mode", nil)
	_, _, err := dashboardBuildStreamWhereFromRequest(req, dashboardStreamWhereConfig{
		IncludeCaptureMode: true,
	})
	if err == nil {
		t.Fatalf("expected error for invalid capture_type")
	}
	if !strings.Contains(err.Error(), "invalid capture_type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDashboardBuildStreamWhereKoreaFamilyFilter(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams?korea_family=topis", nil)
	where, args, err := dashboardBuildStreamWhereFromRequest(req, dashboardStreamWhereConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Fatalf("args len=%d want=0", len(args))
	}
	sqlWhere := strings.Join(where, " AND ")
	if !strings.Contains(sqlWhere, "TOPIS") || !strings.Contains(sqlWhere, "topiscctv") {
		t.Fatalf("where missing topis korea family predicate: %s", sqlWhere)
	}
}

func TestDashboardBuildStreamWhereInvalidKoreaFamily(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams?korea_family=naver", nil)
	_, _, err := dashboardBuildStreamWhereFromRequest(req, dashboardStreamWhereConfig{})
	if err == nil {
		t.Fatalf("expected error for invalid korea_family")
	}
	if !strings.Contains(err.Error(), "invalid korea_family") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDashboardBuildStreamWhereRecordingTabDefault(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/streams?tab=recording", nil)
	where, args, err := dashboardBuildStreamWhereFromRequest(req, dashboardStreamWhereConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Fatalf("args len=%d want=0", len(args))
	}
	sqlWhere := strings.Join(where, " AND ")
	if !strings.Contains(sqlWhere, "s.recording_state='on'") {
		t.Fatalf("where missing recording_state on clause: %s", sqlWhere)
	}
}

func TestDashboardBuildStreamWhereSourceAndYouTubeChannel(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/dashboard/streams?recording_state=on&q=earth&tags=shortlist&tags_not=archive&country=South%20Korea&city=Seoul&source=youtube&youtube_channel=EarthCam%20Live",
		nil,
	)
	where, args, err := dashboardBuildStreamWhereFromRequest(req, dashboardStreamWhereConfig{
		IncludeSearch:         true,
		IncludeSource:         true,
		IncludeYouTubeChannel: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := len(args), 8; got != want {
		t.Fatalf("args len=%d want=%d", got, want)
	}
	if got, want := args[0], "on"; got != want {
		t.Fatalf("recording_state arg=%v want=%q", got, want)
	}
	if got, want := args[1], "%earth%"; got != want {
		t.Fatalf("search arg=%v want=%q", got, want)
	}
	if got, ok := args[2].([]string); !ok || len(got) != 1 || got[0] != "shortlist" {
		t.Fatalf("tags arg=%v want []string{\"shortlist\"}", args[2])
	}
	if got, ok := args[3].([]string); !ok || len(got) != 1 || got[0] != "archive" {
		t.Fatalf("tags_not arg=%v want []string{\"archive\"}", args[3])
	}
	if got, want := args[4], "south korea"; got != want {
		t.Fatalf("country arg=%v want=%q", got, want)
	}
	if got, want := args[5], "seoul"; got != want {
		t.Fatalf("city arg=%v want=%q", got, want)
	}
	if got, want := args[6], "youtube"; got != want {
		t.Fatalf("source arg=%v want=%q", got, want)
	}
	if got, want := args[7], "earthcam live"; got != want {
		t.Fatalf("youtube_channel arg=%v want=%q", got, want)
	}
	sqlWhere := strings.Join(where, " AND ")
	if !strings.Contains(sqlWhere, "s.recording_state=$1") {
		t.Fatalf("where missing recording_state placeholder: %s", sqlWhere)
	}
	if !strings.Contains(sqlWhere, "CAST(s.id AS text) ILIKE $2") {
		t.Fatalf("where missing search placeholder: %s", sqlWhere)
	}
	if !strings.Contains(sqlWhere, "$7") {
		t.Fatalf("where missing source placeholder: %s", sqlWhere)
	}
	if !strings.Contains(sqlWhere, "$8") {
		t.Fatalf("where missing youtube_channel placeholder: %s", sqlWhere)
	}
}

func TestDashboardBuildStreamWhereIgnoresDisabledFilters(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/dashboard/streams?q=earth&source=youtube&youtube_channel=EarthCam%20Live",
		nil,
	)
	where, args, err := dashboardBuildStreamWhereFromRequest(req, dashboardStreamWhereConfig{
		IncludeSearch:         false,
		IncludeSource:         false,
		IncludeYouTubeChannel: false,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Baseline = "1=1" plus the always-on soft-prune predicate. Disabled optional
	// filters (search/source/youtube_channel) add nothing beyond that.
	if got, want := len(where), 2; got != want {
		t.Fatalf("where len=%d want=%d", got, want)
	}
	if got, want := where[0], "1=1"; got != want {
		t.Fatalf("where[0]=%q want=%q", got, want)
	}
	if got, want := where[1], "s.enabled = true AND s.excluded_flag = false"; got != want {
		t.Fatalf("where[1]=%q want=%q", got, want)
	}
	if len(args) != 0 {
		t.Fatalf("args len=%d want=0", len(args))
	}
}
