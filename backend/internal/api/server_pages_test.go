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

func TestDestructiveWebActionsUseMenusAndConfirmations(t *testing.T) {
	checks := map[string][]string{
		"streams.html": {
			`id="detailStreamActionMenu" class="action-menu"`,
			`Type DELETE to continue.`,
			`Remove tag ${inputTags[0]} from stream ${streamID}?`,
		},
		"recordings.html": {
			`id="bulkCancelMenu" class="action-menu hidden"`,
			`Active capture stops immediately. Existing clips are retained.`,
		},
		"admin.html": {
			`aria-label="Storage destination actions"`,
			`aria-label="Grant actions"`,
			`aria-label="Assignment actions"`,
			`aria-label="Pipeline actions"`,
			`Disable pipeline ${pipelineID}?`,
			`Remove tag ${inputTags.join(', ')} from stream ${streamID}?`,
			`Type DELETE to continue.`,
			`if (!window.confirm(`,
		},
		"org-settings.html": {
			`aria-label="Member actions"`,
			`Remove billing-admin access from ${email}?`,
		},
	}
	for name, markers := range checks {
		body, err := loadHTMLPage(name)
		if err != nil {
			t.Fatalf("load %s: %v", name, err)
		}
		page := string(body)
		for _, marker := range markers {
			if !strings.Contains(page, marker) {
				t.Errorf("%s missing destructive-action safety marker %q", name, marker)
			}
		}
	}
}

func TestOrgSettingsNASStorageUsesPercentageThresholds(t *testing.T) {
	body, err := loadHTMLPage("org-settings.html")
	if err != nil {
		t.Fatalf("load org settings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`storagePercent <= 5`,
		`storagePercent <= 10`,
		`Critical low storage`,
		`storagePercent.toFixed(1)}% free`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("org settings missing NAS capacity marker %q", marker)
		}
	}
}

func TestRecordingRelayRoutingIsSoftAndExplicit(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`Preferred home internet`,
		`Immediate if the preferred connection is unavailable or full; after 12 seconds if it is healthy but does not take the job`,
		`This is a soft preference. Stoarama falls back automatically rather than miss footage.`,
		`/relay-routing`,
		`preferred_relay_group_id: raw ? Number(raw) : null`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings html missing relay-routing marker %q", marker)
		}
	}
}

func TestOrgSettingsRelayGroupsExposeNativeBandwidthRouting(t *testing.T) {
	body, err := loadHTMLPage("org-settings.html")
	if err != nil {
		t.Fatalf("load org settings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`Mbps native load`,
		`Conservative total bandwidth for this internet connection in Mbps`,
		`bandwidth_capacity_mbps`,
		`automatic budget`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("org settings missing bandwidth-routing marker %q", marker)
		}
	}
}

func TestAdminCancellationGuardsPrecedeMutationsAndSuccessUI(t *testing.T) {
	body, err := loadHTMLPage("admin.html")
	if err != nil {
		t.Fatalf("load admin html: %v", err)
	}
	page := string(body)
	assertOrdered := func(name string, markers ...string) {
		t.Helper()
		at := 0
		for _, marker := range markers {
			next := strings.Index(page[at:], marker)
			if next < 0 {
				t.Fatalf("%s missing ordered marker %q", name, marker)
			}
			at += next + len(marker)
		}
	}
	assertOrdered("recording cancellation",
		`async function setStreamRecordingState`,
		`if (String(entered || '').trim() !== confirmToken) {`,
		`return false;`,
		"return fetchJSON(`/api/v1/recording/streams/",
		`const changed = await setStreamRecordingState(id, nextState);`,
		`if (changed === false) return false;`,
		`await Promise.all([`,
	)
	assertOrdered("detail tag cancellation",
		`async function handleTagMutation(action, streamID, rawTagValue)`,
		`if (!window.confirm(`,
		`return false;`,
		`return patchStreamTags(streamID, next);`,
		`detailTagEditor.addEventListener('click'`,
		`const changed = await handleStreamTableAction(action, id, 'detail', btn);`,
		`if (changed === false) { setDetailStatus('No changes.'); return; }`,
		`await loadDetail();`,
		`setDetailStatus('tags updated', 'ok');`,
	)
	assertOrdered("unassign cancellation",
		`data-server-action="unassign"`,
		`if (String(entered || '').trim() !== confirmToken) {`,
		`setServersAssignmentsStatus('No changes.');`,
		`return;`,
		"await fetchJSON(`/api/v1/recording/streams/${streamID}/unassign",
	)
}

func TestStreamsTagCancellationGuardPrecedesDeleteAndReload(t *testing.T) {
	body, err := loadHTMLPage("streams.html")
	if err != nil {
		t.Fatalf("load streams html: %v", err)
	}
	page := string(body)
	guard := strings.Index(page, `if (!window.confirm(`)
	if guard < 0 {
		t.Fatal("streams tag removal confirmation guard is missing")
	}
	cancel := strings.Index(page[guard:], `return false;`)
	remove := strings.Index(page[guard:], `method: 'DELETE'`)
	if cancel < 0 || remove < 0 || cancel >= remove {
		t.Fatal("streams tag cancellation must return before the DELETE request")
	}
	action := strings.Index(page, `const changed = await handleTagMutation(action, id, tagValue);`)
	if action < 0 {
		t.Fatal("streams tag mutation call is missing")
	}
	stop := strings.Index(page[action:], `if (changed === false) return false;`)
	reload := strings.Index(page[action:], `await loadStreams();`)
	if stop < 0 || reload < 0 || stop >= reload {
		t.Fatal("streams tag cancellation must return before list reload and success handling")
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
		`data-health-tooltip="${escapeHTML(title)}" aria-label="${escapeHTML(title)}"`,
		`data-health-tooltip="${escapeHTML(latestTitle)}" tabindex="0"`,
		`aria-describedby="healthTooltip"`,
		`<div id="healthTooltip" class="health-tooltip hidden" role="tooltip"></div>`,
		`healthTooltip.textContent = text;`,
		`const top = Math.max(8, Math.min(window.innerHeight - tooltipRect.height - 8, preferredTop));`,
		`document.addEventListener('pointerover'`,
		`document.addEventListener('focusin'`,
		`window.addEventListener('scroll', hideHealthTooltip, true);`,
		`function renderCards() {
      hideHealthTooltip();`,
		`function renderClipPage() {
      hideHealthTooltip();`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recording health-bin tooltip source missing %q", marker)
		}
	}
}

func TestRecordingsListRendersPersistedTimelineHealth(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`rec.timeline_health && typeof rec.timeline_health === 'object'`,
		`Status / Last 12 hours`,
		`Timeline health`,
		`${captureHealthGraph(healthBins, timezone)}<div class="cell-sub">Last 12 scheduled hours`,
		`recent coverage`,
		`Largest gap`,
		`Whole period`,
		`native layout changed`,
		`continuous timeline · native layout compatible`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings list timeline health missing %q", marker)
		}
	}
}

func TestRecordingDetailUsesPagedHourlyCaptureHealthHeatmap(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`/capture-health${query}`,
		`class="health-heatmap-cell ${health}" data-health-tooltip="${escapeHTML(title)}"`,
		`aria-label="Hourly capture health by local date"`,
		`data-health-page="older"`,
		`data-health-page="newer"`,
		`loadRecordingCaptureHealthPage(button.getAttribute('data-health-page'))`,
		`clipPageState.captureHealth = await fetchRecordingCaptureHealth(recId, '');`,
		`Array.from({ length: 24 }, () => [])`,
		`hours.map((bins, hour) => ({ bin: captureHealthDisplayBin(bins), hour }))`,
		`if (bins.length === 1) return bins[0];`,
		`captured: sum.captured + Number(bin.captured || 0)`,
		`const health = String(bin.health || '');`,
		`repeat(${slots.length},minmax(19px,1fr))`,
		`timeZoneName: 'short'`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recording detail heatmap source missing %q", marker)
		}
	}
}

func TestOrganizationNASConnectionUsesStructuredHealthCard(t *testing.T) {
	body, err := loadHTMLPage("org-settings.html")
	if err != nil {
		t.Fatalf("load org settings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`<dl class="conn-stats">`,
		`<dt>Last batch</dt>`,
		`<dt>Downloaded</dt>`,
		`<dt>Waiting</dt>`,
		`<dt>Oldest waiting</dt>`,
		`<dt>Transfer rate</dt>`,
		`<dt>Last transfer batch</dt>`,
		`Latest download error`,
		`Last connection interruption`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("org settings html missing NAS health marker %q", marker)
		}
	}
}

func TestNASFilesPageUsesPaginatedFolderBrowser(t *testing.T) {
	body, err := loadHTMLPage("nas-files.html")
	if err != nil {
		t.Fatalf("load NAS files html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`/inventory/tree?${params}`,
		`renderBreadcrumbs(path)`,
		`data-path=`,
		`Up one folder`,
		`data.next_cursor`,
		`generation!==requestGeneration`,
		`rootServerOnly=Number(data.server_only||0)`,
		`server-only clips across this NAS`,
		`Download full manifest CSV`,
		`SHA-256`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("NAS files html missing folder-browser marker %q", marker)
		}
	}
	orgBody, err := loadHTMLPage("org-settings.html")
	if err != nil {
		t.Fatalf("load org settings html: %v", err)
	}
	if !strings.Contains(string(orgBody), `href="/nas-files?connection=${id}"`) {
		t.Fatal("org settings does not link to the dedicated NAS files page")
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
