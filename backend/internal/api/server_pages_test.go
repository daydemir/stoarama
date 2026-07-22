package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoadHTMLPageUsesEmbeddedAssets(t *testing.T) {
	body, err := loadHTMLPage("streams.html")
	if err != nil {
		t.Fatalf("load streams html: %v", err)
	}
	if !strings.Contains(string(body), "Stoarama Streams") {
		t.Fatalf("streams html missing expected title")
	}
}

func TestStreamsPageIgnoresStaleFilterResponses(t *testing.T) {
	body, err := loadHTMLPage("streams.html")
	if err != nil {
		t.Fatalf("load streams html: %v", err)
	}
	page := string(body)
	for _, guard := range []string{
		"streamLoadController.abort();",
		"streamFilterOptionsController.abort();",
		"if (requestToken !== streamLoadToken) return;",
		"if (requestToken !== streamFilterChangeToken) return;",
	} {
		if !strings.Contains(page, guard) {
			t.Fatalf("streams html missing stale response guard %q", guard)
		}
	}
}

func TestRecordingsComposerIsOnlyLoadedByNewRecordingRoute(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	if got := strings.Count(page, "await maybeLandFromCatalogStream();"); got != 1 {
		t.Fatalf("catalog landing calls=%d, want 1 creation-route call", got)
	}
	if strings.Contains(page, "openComposer(false)") {
		t.Fatal("recordings page still has an inline composer entry point")
	}
	if !strings.Contains(page, "function closeComposer() {\n      clearStashedCatalogStreamId();\n      window.location.assign('/recordings');") {
		t.Fatal("composer cancel must clear the stashed catalog stream before returning to the list")
	}
	if !strings.Contains(page, "if (ids.length) {\n          clearStashedCatalogStreamId();\n          state.batchSelected = new Set(ids);") {
		t.Fatal("batch setup must supersede a stashed single-stream intent")
	}
}

func TestHandleDashboardStaticServesDashboardJS(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/static/dashboard.js", nil)
	rec := httptest.NewRecorder()
	(&Server{}).handleDashboardStatic(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/javascript") {
		t.Fatalf("content-type=%q", ct)
	}
	if !strings.Contains(rec.Body.String(), "StoaramaDashboard") {
		t.Fatalf("static body missing dashboard namespace")
	}
}

func TestHandleKoreaAppDefaultsToCaptureTypes(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/korea", nil)
	rec := httptest.NewRecorder()
	(&Server{}).handleKoreaApp(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("status=%d", rec.Code)
	}
	location := rec.Header().Get("Location")
	if !strings.Contains(location, "korea_family=all") || !strings.Contains(location, "capture_types=hls%2Chttp_video") {
		t.Fatalf("location=%q", location)
	}
	if strings.Contains(location, "recordable") {
		t.Fatalf("location should not include legacy recordable param: %q", location)
	}
}
