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

func TestRecordingsComposerUsesCatalogTimezone(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		"Intl.supportedValuesOf('timeZone')",
		"state.catalogTimezoneMissing = timezone === '';",
		"select.add(new Option('Choose timezone', '', true, true), 0);",
		"select.value = timezone;",
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings html missing catalog timezone marker %q", marker)
		}
	}
}

func TestRecordingsComposerAutofillsCatalogNaming(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		"Assigned automatically for this organization",
		"csv.continent",
		"fill(els.namingCountry, stream.location_country || csv.country);",
		"fill(els.namingCity, stream.location_city || csv.city);",
		"els.namingPlazaID.readOnly = false;",
		"els.namingPlazaID.placeholder = '08';",
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings html missing catalog naming marker %q", marker)
		}
	}
}

func TestRecordingsComposerDefaultsToPlazaHourlyDaytimeWindow(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`id="dailyWindowStart" type="time" value="08:00"`,
		`id="dailyWindowEnd" type="time" value="20:00"`,
		`data-naming="plaza_hourly_v1" class="on"`,
		`namingProfile: 'plaza_hourly_v1'`,
		`naming_profile: state.namingProfile`,
		`async function boot()`,
		`setNamingProfile('plaza_hourly_v1');`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings html missing default %q", marker)
		}
	}
	if strings.Contains(page, `id="plazaHourlyNamingFields" class="hidden"`) {
		t.Fatal("default Plaza hourly fields must not start hidden")
	}
	if strings.Contains(page, `setNamingProfile('stoarama_v1');`) {
		t.Fatal("composer boot must not override the Plaza hourly default")
	}
}

func TestRecordingsComposerCannotOverrideRequiredRelay(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`state.relayRequired = out.relay_required === true;`,
		`els.relayRecommendChoice.classList.toggle('hidden', state.relayRequired);`,
		`body.capture_via = state.relayRequired || els.relayRecommendOptIn.checked ? 'relay' : 'cloud';`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings html missing required relay marker %q", marker)
		}
	}
}

func TestRecordingAndStreamPagesShowLocalScheduleTime(t *testing.T) {
	recordings, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	for _, marker := range []string{
		"return `${weekdayLabel(rec.active_weekdays)} · ${window}`;",
		"Ends ${escapeHTML(ends)}",
		"['Local time',",
		"timeZone: String(timezone || 'UTC')",
	} {
		if !strings.Contains(string(recordings), marker) {
			t.Fatalf("recordings html missing local schedule marker %q", marker)
		}
	}
	if strings.Contains(string(recordings), "fmtInstant(") {
		t.Fatal("recordings html still references the removed generic instant formatter")
	}

	streams, err := loadHTMLPage("streams.html")
	if err != nil {
		t.Fatalf("load streams html: %v", err)
	}
	if !strings.Contains(string(streams), "Local time ${esc(local)}") {
		t.Fatal("streams html missing local time indicator")
	}
}

func TestRecordingHealthBinSourceAssignsCapturedPercentageTooltip(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		"const title = `${Math.round(percent * 10) / 10}% of expected clips captured (${captured}/${expected}) · ${start} to ${end}`;",
		`title="${escapeHTML(title)}" aria-label="${escapeHTML(title)}"`,
		`health-strip-bar ${escapeHTML(health)}" title="${escapeHTML(title)}"`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recording health-bin tooltip source missing %q", marker)
		}
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
