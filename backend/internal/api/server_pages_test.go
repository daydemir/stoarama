package api

import (
	"net/http"
	"net/http/httptest"
	"reflect"
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
		"const title = `${Math.round(percent * 10) / 10}% of expected clips captured (${captured}/${expected}) · ${start} to ${end}${joinedLabel}`;",
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
		`<th>Recording</th><th>Status / Last 12 hours</th><th>Recording quality</th>${joinedSortHeaderHTML()}<th>Schedule</th>`,
		`<td class="recording-cell-wide" data-label="Status / Last 12 hours"><div class="card-status ${st.cls}"><span class="dot"></span>${st.text}</div>${captureHealthHTML}${warning}</td>
		<td class="recording-cell-wide" data-label="Recording quality">${timelineHealthHTML || '<div class="capture-health unavailable">Timeline check pending</div>'}</td>`,
		`const captureHealthHTML = captureHealth === 'unavailable'`,
		"? `${captureHealthGraph(healthBins, timezone)}<div class=\"cell-sub\">Last 12 scheduled hours",
		`recent coverage`,
		`Largest gap`,
		`Whole period`,
		`native layout changed`,
		`continuous timeline · native layout compatible`,
		`dailyGradesHTML(timeline.daily_grades, timezone)`,
		`A–C good · D degraded · E poor · F no usable media · ? not yet measurable`,
		`daily-grade ${grade.toLowerCase()}`,
		`<option value="best14">Completed 14-day score</option>`,
		`best14RatingHTML(best14)`,
		`Insufficient`,
		`state.recordingSort === 'best14'`,
		`Number.isFinite(Number(rating.sort_rank)) ? Number(rating.sort_rank) : 99`,
		`['Capture health', () => captureHealthCardHTML()]`,
		`['Daily recording quality', () => dailyGradesCardHTML(rec)]`,
		`['Schedule', () => scheduleCardHTML(rec)]`,
		`data-health-tooltip="${escapeHTML(title)}" tabindex="0" aria-describedby="healthTooltip"`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings list health-column layout missing %q", marker)
		}
	}
}

func TestRecordingsQualityTiersAreFirstClassVisualStatuses(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`--quality-great-bg: #14532d;`,
		`.best14-rating.great { color: var(--quality-great-fg); background: var(--quality-great-bg); }`,
		`.best14-rating.good { color: var(--quality-good-fg); background: var(--quality-good-bg); }`,
		`.best14-rating.fine { color: var(--quality-fine-fg); background: var(--quality-fine-bg); }`,
		`<div class="quality-section-label">Best 14-day score</div>`,
		`<div class="quality-daily"><div class="quality-section-label">Daily grades</div>`,
		`if (rating === 'GREAT') return 'Great+';`,
		`if (rating === 'VERY_GOOD' || rating === 'GOOD') return 'Good+';`,
		`if (rating === 'FINE') return 'Fine+';`,
		`qualifier === 'UNKNOWN_POTENTIAL'`,
		`const pendingText = 'Awaiting scored days';`,
		`qualifier === 'ENDED' ? 'Ended without a completed tier' : 'Pending'`,
		`Completed scores use 14 consecutive scored recording days; a missing or unmeasured day breaks the run.`,
		`Daily badges grade individual recording days.`,
		`class="best14-rating ${tierClass}" data-health-tooltip=`,
		`tabindex="0" aria-describedby="healthTooltip"`,
		`<span class="best14-rating great"><span class="best14-rating-name">Great+</span></span>`,
		`<span class="best14-rating good"><span class="best14-rating-name">Good+</span></span>`,
		`<span class="best14-rating fine"><span class="best14-rating-name">Fine+</span></span>`,
		`<span class="detail-card-title">Recording quality</span>`,
		`const best14 = timeline && timeline.best_14_rating`,
		`<div><div class="quality-section-label" style="margin-bottom:6px;">Best 14-day score</div>${best14RatingHTML(best14)}</div>`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings aggregate quality presentation missing %q", marker)
		}
	}

	aggregate := strings.Index(page, `<div class="quality-section-label">Best 14-day score</div>`)
	if aggregate < 0 {
		t.Fatal("recordings row is missing the aggregate score label")
	}
	daily := strings.Index(page[aggregate:], `<div class="quality-daily"><div class="quality-section-label">Daily grades</div>`)
	if daily < 0 {
		t.Fatal("recordings row does not place the aggregate score before daily grades")
	}
}

func TestRecordingsListFiltersCompletedAndPotentialBest14ScoresSeparately(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`id="qualityFilter"`,
		`<option value="great_plus">Great+ completed</option>`,
		`<option value="good_plus">Good+ completed</option>`,
		`<option value="fine_plus">Fine+ completed</option>`,
		`<option value="great_potential">Great+ potential</option>`,
		`<option value="good_potential">Good+ potential</option>`,
		`<option value="fine_potential">Fine+ potential</option>`,
		`const BEST14_COMPLETED_TIERS = {`,
		`great_plus: ['GREAT'],`,
		`good_plus: ['GREAT', 'VERY_GOOD', 'GOOD'],`,
		`fine_plus: ['GREAT', 'VERY_GOOD', 'GOOD', 'FINE'],`,
		`if (Number(rating.completed_days || 0) < 14) return '';`,
		`if (String(rating.qualifier || '').trim() !== '') return '';`,
		`if (String(rating.rating || '').toUpperCase() !== 'INSUFFICIENT') return '';`,
		`if (Number(rating.completed_days || 0) >= 14) return '';`,
		`qualifier.endsWith('_POTENTIAL')`,
		`let items = all.filter((rec) => recordingMatchesSearch(rec, state.recordingSearch)`,
		`&& matchesStatusFilter(rec) && matchesQualityFilter(rec));`,
		`state.qualityFilter = els.qualityFilter.value;`,
		`syncQualityFilterQuery();`,
		`if (state.qualityFilter === 'all') syncQualityFilterQuery();`,
		`Array.isArray(rating.filter_keys)`,
		`rating.tier_sort_rank`,
		`Completed tiers require 14 consecutive scored recording days.`,
		`Great+ contains only A, B, and C days.`,
		`Good+ has no F days and at most two E days; D days are allowed.`,
		`Fine+ has no F days.`,
		`A future roll-off potential already has a completed score`,
		`const explicit = String(rating.potential_rating || '').toUpperCase();`,
		`potentialKind === 'FUTURE_ROLL_OFF'`,
		`const BEST14_COMPLETED_SORT_RANK = { GREAT: 0, VERY_GOOD: 1, GOOD: 1, FINE: 2, QUESTIONABLE: 3, BAD: 4 };`,
		`const tierRank = completed && Number.isFinite(apiTierRank) ? apiTierRank : (BEST14_COMPLETED_SORT_RANK[completed] ?? 5);`,
		`return [tierRank, detailRank, -completedDays];`,
		`left[0] - right[0] || left[1] - right[1] || left[2] - right[2] || Number(b.id) - Number(a.id)`,
		`Sorting uses the current completed score: Great+ first, then Good+, Fine+, Questionable, and Bad. Potential does not replace the current score.`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings quality filter missing %q", marker)
		}
	}
}

func TestRecordingsQualityFilterContractUsesCompletedAndPotentialFixtures(t *testing.T) {
	tests := []struct {
		name   string
		rating recordingBest14Rating
		keys   []string
		rank   int
	}{
		{name: "great completed", rating: recordingBest14Rating{Rating: "GREAT", Completed: 14}, keys: []string{"great_plus", "good_plus", "fine_plus"}, rank: 0},
		{name: "very good completed", rating: recordingBest14Rating{Rating: "VERY_GOOD", Completed: 14}, keys: []string{"good_plus", "fine_plus"}, rank: 1},
		{name: "good completed", rating: recordingBest14Rating{Rating: "GOOD", Completed: 14}, keys: []string{"good_plus", "fine_plus"}, rank: 1},
		{name: "fine completed", rating: recordingBest14Rating{Rating: "FINE", Completed: 14}, keys: []string{"fine_plus"}, rank: 2},
		{name: "questionable completed", rating: recordingBest14Rating{Rating: "QUESTIONABLE", Completed: 14}, keys: []string{"questionable"}, rank: 3},
		{name: "bad completed", rating: recordingBest14Rating{Rating: "BAD", Completed: 14}, keys: []string{"bad"}, rank: 4},
		{name: "questionable with great roll-off", rating: recordingBest14Rating{Rating: "QUESTIONABLE", Completed: 14, PotentialRating: "GREAT", PotentialKind: "FUTURE_ROLL_OFF", PotentialDays: 1}, keys: []string{"questionable", "great_potential", "good_potential", "fine_potential"}, rank: 3},
		{name: "great potential", rating: recordingBest14Rating{Rating: "INSUFFICIENT", Qualifier: "GREAT_POTENTIAL", Completed: 8}, keys: []string{"great_potential", "good_potential", "fine_potential"}, rank: 5},
		{name: "good potential", rating: recordingBest14Rating{Rating: "INSUFFICIENT", Qualifier: "GOOD_POTENTIAL", Completed: 8}, keys: []string{"good_potential", "fine_potential"}, rank: 5},
		{name: "fine potential", rating: recordingBest14Rating{Rating: "INSUFFICIENT", Qualifier: "FINE_POTENTIAL", Completed: 8}, keys: []string{"fine_potential"}, rank: 5},
		{name: "short runway", rating: recordingBest14Rating{Rating: "INSUFFICIENT", Qualifier: "SHORT_RUNWAY", Completed: 8}, keys: []string{"insufficient"}, rank: 5},
		{name: "malformed completed tier under 14 days", rating: recordingBest14Rating{Rating: "FINE", Completed: 13}, keys: []string{"insufficient"}, rank: 5},
		{name: "malformed potential at 14 days", rating: recordingBest14Rating{Rating: "INSUFFICIENT", Qualifier: "FINE_POTENTIAL", Completed: 14}, keys: []string{"insufficient"}, rank: 5},
		{name: "malformed completed qualifier", rating: recordingBest14Rating{Rating: "GREAT", Qualifier: "GREAT_POTENTIAL", Completed: 14}, keys: []string{"insufficient"}, rank: 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := best14FilterKeys(tc.rating); !reflect.DeepEqual(got, tc.keys) {
				t.Fatalf("filter keys=%v want=%v", got, tc.keys)
			}
			if got := best14TierSortRank(tc.rating); got != tc.rank {
				t.Fatalf("tier sort rank=%d want=%d", got, tc.rank)
			}
		})
	}
	completed := classifyBest14(dailyGrades("AAABBBCCCAAABB"), "completed", 0)
	if !reflect.DeepEqual(completed.FilterKeys, []string{"great_plus", "good_plus", "fine_plus"}) || completed.TierSortRank != 0 {
		t.Fatalf("classified completed contract=%+v", completed)
	}
	potential := classifyBest14(dailyGrades("AABCDE"), "active", 8)
	if !reflect.DeepEqual(potential.FilterKeys, []string{"good_potential", "fine_potential"}) || potential.TierSortRank != 5 {
		t.Fatalf("classified potential contract=%+v", potential)
	}
}

func TestRecordingsPageShowsProgressiveLoadingAndRecoverableErrors(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`id="pageLoading" class="page-loading" role="status" aria-live="polite"`,
		`id="recordingsLoading" class="recordings-loading hidden" role="status" aria-live="polite"`,
		`id="recordingsLoadError" class="recordings-load-error hidden" role="alert"`,
		`id="recordingsRetryBtn"`,
		`id="cards" class="cards" aria-busy="false"`,
		`function beginRecordingsLoad()`,
		`els.cards.setAttribute('aria-busy', 'true');`,
		`function failRecordingsLoad(error)`,
		`els.recordingsRetryBtn.addEventListener('click', () => refreshRecordings());`,
		`const supportingLoads = [loadBilling(), refreshDestinations(), loadConnections(), loadRelays()];`,
		`await Promise.allSettled([refreshRecordings(), ...supportingLoads]);`,
		`if (!sharedReadOnly) await loadRelays();`,
	} {
		if marker == `if (!sharedReadOnly) await loadRelays();` {
			if strings.Contains(page, marker) {
				t.Fatalf("recordings list still blocks on relay load")
			}
			continue
		}
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings progressive loading missing %q", marker)
		}
	}
}

func TestRecordingsPageExplainsDailyGradesPotentialsAndFilterCounts(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatalf("load recordings html: %v", err)
	}
	page := string(body)
	for _, marker := range []string{
		`<summary>Daily grades and 14-day scores</summary>`,
		`It is not the percentage of clip files that uploaded or dropped.`,
		`At least 99% coverage`,
		`At least 95% coverage`,
		`At least 90% coverage`,
		`At least 80% usable coverage`,
		`less than 80% of the scheduled time`,
		`No usable completed media`,
		`missing calendar day breaks the run`,
		`An under-14 potential has enough scheduled runway`,
		`clean A, B, or C days can move an older low grade`,
		`id="scoreFilterResult"`,
		`function renderScoreFilterResult(count)`,
		`parts.push(String(selected.textContent || '').trim());`,
		`renderScoreFilterResult(items.length);`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings score guide/count missing %q", marker)
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
		`fetchRecordingCaptureHealth(recId, toDate, 'base')`,
		`fetchRecordingCaptureHealth(recId, page.to, 'joined', page.from)`,
		`mergeCaptureHealthJoined(clipPageState.captureHealth, joinedPage)`,
		`Joined coverage is loading.`,
		`Joined coverage is temporarily unavailable. Captured clips remain shown.`,
		`role="status" aria-live="polite"`,
		`function captureHealthJoinedLabel(bin, page)`,
		`class="health-heatmap-cell ${health}" data-health-tooltip="${escapeHTML(title)}"`,
		`aria-label="Hourly capture health by local date"`,
		`data-health-page="older"`,
		`data-health-page="newer"`,
		`loadRecordingCaptureHealthPage(button.getAttribute('data-health-page'))`,
		`void loadRecordingDetailCaptureHealth(recId);`,
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

func TestRecordingDetailCardFailureCannotRemoveJoinedFolderLink(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		`function renderDetailCardSafely(label, render)`,
		`function renderRecordingDetailBody(prefix, cardRenderers)`,
		`renderDetailCardSafely(label, render)`,
		`role="status"`,
		`This detail is temporarily unavailable.`,
		`class="btn-link primary joined-folder-cta"`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recording detail resilience source missing %q", marker)
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

func TestRecordingHeatmapShowsAccessibleJoinedProgress(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{
		"health-joined-marker",
		"const joinedAvailable = sourceMS > 0;",
		"Joined: ${joinedPercent}%",
		"Joined: unavailable",
		"source_duration_ms",
		"joined_ready_ms",
		"<svg viewBox=\"0 0 10 10\"",
	} {
		if !strings.Contains(string(body), marker) {
			t.Fatalf("recordings html missing joined-progress marker %q", marker)
		}
	}
}

func TestRecordingJoinedColumnSortsLazyValuesAndDistinguishesZeroFromUnavailable(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		`<option value="joined_desc">Joined: highest first</option>`,
		`<option value="joined_asc">Joined: lowest first</option>`,
		`const direction = sort === 'joined_asc' ? 1 : -1;`,
		`if (left.available !== right.available) return left.available ? -1 : 1;`,
		`if (left.available && left.percent !== right.percent) return direction * (left.percent - right.percent);`,
		`if (!(sourceMS > 0) || rawPercent === null || rawPercent === undefined`,
		`return { available: false, percent: null, sourceMS: 0, readyMS: 0 };`,
		`available: true,`,
		`>${joined.percent}%</div>`,
		`Joined coverage is temporarily unavailable`,
		`Joined coverage unavailable: no usable recorded footage`,
		`mergeRecordingMetricItems(payload.items);`,
		`state.joinedProgressLoaded = true;`,
		`renderCards();`,
		`data-sortjoined`,
		`aria-sort="${ariaSort}"`,
		`state.recordingSort = next;`,
		`els.recordingSort.value = next;`,
		`Joined, highest first`,
		`Joined, lowest first`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings html missing joined sort/state marker %q", marker)
		}
	}
	refreshStart := strings.Index(page, "async function refreshJoinedProgress(requestToken)")
	if refreshStart < 0 {
		t.Fatal("joined progress refresh function not found")
	}
	refreshEnd := strings.Index(page[refreshStart:], "// ---- Composer open/close ----")
	if refreshEnd < 0 {
		t.Fatal("joined progress refresh function end not found")
	}
	refresh := page[refreshStart : refreshStart+refreshEnd]
	mergeAt := strings.Index(refresh, "mergeRecordingMetricItems(payload.items);")
	loadedAt := strings.Index(refresh, "state.joinedProgressLoaded = true;")
	renderAt := strings.Index(refresh, "renderCards();")
	if mergeAt < 0 || loadedAt < mergeAt || renderAt < loadedAt {
		t.Fatalf("joined lazy values do not rerender in order: merge=%d loaded=%d render=%d", mergeAt, loadedAt, renderAt)
	}
	for _, stale := range []string{">View joined<", ">Browse joined recordings<"} {
		if strings.Contains(page, stale) {
			t.Fatalf("recordings html still contains stale action copy %q", stale)
		}
	}
	for _, label := range []string{">View recording<", ">Browse joined folder<"} {
		if !strings.Contains(page, label) {
			t.Fatalf("recordings html missing exact action copy %q", label)
		}
	}
}

func TestRecordingListHasNoAccountWideFolderNavigator(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		`<label for="recordingSearch">Search recordings</label>`,
		`<input id="recordingSearch" type="search"`,
		`recordingSearch: '',`,
		`recordingSort: recordingSortFromSearch(window.location.search),`,
		`recordingMatchesSearch(rec, state.recordingSearch)`,
		`window.history.replaceState({}, '', recordingsListURL(window.location.href, state.recordingSort));`,
		`data-label="Recording"`,
		`data-label="Status / Last 12 hours"`,
		`data-label="Recording quality"`,
		`data-label="Joined"`,
		`data-label="Schedule"`,
		`data-label="Clip / Capture"`,
		`data-label="Storage"`,
		`data-label="Actions"`,
		`.recordings-table td::before { content: attr(data-label);`,
		`@media (min-width: 1040px)`,
		`@media (max-width: 1039px)`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recording list missing %q", marker)
		}
	}
	for _, removed := range []string{"folderBrowserMobile", "folderNavRail", "folderBreadcrumbs", "recordingMatchesFolder", "buildRecordingFolderTree"} {
		if strings.Contains(page, removed) {
			t.Fatalf("recording list still contains removed folder navigator marker %q", removed)
		}
	}
	if strings.Contains(page, `.recordings-table td:nth-child(5)::before`) || strings.Contains(page, `.recordings-table td:nth-child(9)::before`) {
		t.Fatal("mobile recording labels still depend on fragile nine-column nth-child positions")
	}
	if strings.Contains(page, `role="tree"`) {
		t.Fatal("folder navigator claims ARIA tree semantics without implementing its keyboard contract")
	}
}

func TestRecordingListDefaultsToJoinedDescendingWithoutBlockingBaseline(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		`<option value="joined_desc">Joined: highest first</option>`,
		`return ['newest', 'best14', 'joined_desc', 'joined_asc'].includes(value) ? value : 'joined_desc';`,
		`recordingSort: recordingSortFromSearch(window.location.search),`,
		`els.recordingSort.value = state.recordingSort;`,
		`if (left.available !== right.available) return left.available ? -1 : 1;`,
		`finishRecordingsLoad();
        renderCards();
		void refreshRecordingEnrichment(requestToken);
		void refreshJoinedProgress(requestToken);`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings default joined sort missing %q", marker)
		}
	}
}

func TestRecordingDetailLinksToDedicatedJoinedFolderWithoutEagerJoinedFetch(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		`const folderPath = recordingAPIPath(`,
		`class="btn-link primary joined-folder-cta"`,
		`>Browse joined folder</a>`,
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recording detail missing dedicated joined folder link %q", marker)
		}
	}
	for _, removed := range []string{"renderJoinedList", "joinedFolderView", `/joined?limit=`, `id="clipListBody"`} {
		if strings.Contains(page, removed) {
			t.Fatalf("recording detail still contains embedded joined browser marker %q", removed)
		}
	}
}

func TestRecordingListLoadsMetricsAfterBaseline(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		"const payload = await fetchJSON(recordingAPIPath());",
		"void refreshRecordingEnrichment(requestToken);",
		"void refreshJoinedProgress(requestToken);",
		"recordingAPIPath(`/enrichment?${params.toString()}`)",
		"recordingAPIPath(`/joined-progress${sortQuery}`)",
		"Joined coverage is loading",
		"void loadRecordingDetailCaptureHealth(recId);",
		"renderClipPage();",
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings html missing lazy-metric marker %q", marker)
		}
	}
	if strings.Contains(page, "clipPageState.captureHealth = await fetchRecordingCaptureHealth(recId, '');") {
		t.Fatal("recording detail still waits for capture health before loading joined files")
	}
}

func TestRecordingDetailLoadsTimelineWithoutDuplicateClipMetrics(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	if !strings.Contains(page, `/enrichment?recording_id=${encodeURIComponent(recId)}&timeline_only=1`) {
		t.Fatal("recording detail does not request timeline-only enrichment")
	}
	if !strings.Contains(page, `clipPageState.rec.timeline_health = item.timeline_health || null`) {
		t.Fatal("recording detail does not restrict timeline-only response merging to timeline health")
	}
}

func TestRecordingListLoadsEnrichmentInProgressiveBoundedBatches(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		"const RECORDING_ENRICHMENT_BATCH_SIZE = 1;",
		"const ids = state.recordings.map((rec) => Number(rec.id)).filter((id) => id > 0);",
		"offset < ids.length; offset += RECORDING_ENRICHMENT_BATCH_SIZE",
		"const batch = ids.slice(offset, offset + RECORDING_ENRICHMENT_BATCH_SIZE);",
		"params.set('recording_ids', batch.join(','));",
		"await fetchJSON(recordingAPIPath(`/enrichment?${params.toString()}`))",
		"mergeRecordingMetricItems(payload.items);",
		"renderCards();",
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recordings html missing progressive enrichment marker %q", marker)
		}
	}
	refreshStart := strings.Index(page, "async function refreshRecordingEnrichment(requestToken)")
	if refreshStart < 0 {
		t.Fatal("recording enrichment refresh function not found")
	}
	refreshEnd := strings.Index(page[refreshStart:], "async function refreshJoinedProgress(requestToken)")
	if refreshEnd < 0 {
		t.Fatal("recording enrichment refresh function end not found")
	}
	refresh := page[refreshStart : refreshStart+refreshEnd]
	if got := strings.Count(refresh, "await fetchJSON("); got != 1 {
		t.Fatalf("recording enrichment has %d fetch sites; want one awaited request inside the loop", got)
	}
	if strings.Contains(refresh, "Promise.all") {
		t.Fatal("recording enrichment must not fan out concurrent browser requests")
	}
	fetchAt := strings.Index(refresh, "await fetchJSON(recordingAPIPath(`/enrichment?${params.toString()}`))")
	mergeAt := -1
	renderAt := -1
	catchAt := strings.Index(refresh, "} catch (_)")
	if fetchAt >= 0 {
		if relative := strings.Index(refresh[fetchAt:], "mergeRecordingMetricItems(payload.items);"); relative >= 0 {
			mergeAt = fetchAt + relative
		}
	}
	if mergeAt >= 0 {
		if relative := strings.Index(refresh[mergeAt:], "renderCards();"); relative >= 0 {
			renderAt = mergeAt + relative
		}
	}
	if fetchAt < 0 || mergeAt < fetchAt || renderAt < mergeAt || catchAt < renderAt {
		t.Fatalf("enrichment batch does not await, merge, render incrementally, then stop on failure: fetch=%d merge=%d render=%d catch=%d", fetchAt, mergeAt, renderAt, catchAt)
	}
}

func TestRecordingDetailMetadataLoadIsBoundToActiveRequest(t *testing.T) {
	body, err := loadHTMLPage("recordings.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, marker := range []string{
		"const loadToken = ++clipPageState.detailLoadToken;",
		"if (loadToken !== clipPageState.detailLoadToken) return;",
		"clipPageState.rec = await fetchJSON(recordingAPIPath(`/${encodeURIComponent(recId)}`));",
		"renderClipPage();",
	} {
		if !strings.Contains(page, marker) {
			t.Fatalf("recording detail missing metadata identity fence %q", marker)
		}
	}
	if strings.Contains(page, "clipPageState.joinedPayload") || strings.Contains(page, "/joined?limit=") {
		t.Fatal("recording detail still fetches the embedded joined payload")
	}
	pageHealthStart := strings.Index(page, "async function loadRecordingCaptureHealthPage(direction)")
	if pageHealthStart < 0 {
		t.Fatal("capture-health pagination function not found")
	}
	pageHealthEnd := strings.Index(page[pageHealthStart:], "// The storage card")
	if pageHealthEnd < 0 {
		t.Fatal("capture-health pagination function not found")
	}
	pageHealthSource := page[pageHealthStart : pageHealthStart+pageHealthEnd]
	if strings.Contains(pageHealthSource, "await loadClipPage(") {
		t.Fatal("capture-health pagination refetches recording metadata")
	}
}
