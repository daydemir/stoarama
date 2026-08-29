import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';

const html = fs.readFileSync(new URL('./recordings.html', import.meta.url), 'utf8');

function sourceBetween(startMarker, endMarker) {
  const start = html.indexOf(startMarker);
  assert.notEqual(start, -1, `missing ${startMarker}`);
  const end = html.indexOf(endMarker, start);
  assert.notEqual(end, -1, `missing ${endMarker}`);
  return html.slice(start, end);
}

test('a rejected detail card preserves healthy cards and the joined browser', () => {
  const source = sourceBetween(
    'function renderDetailCardSafely(label, render)',
    'function renderClipPage()',
  );
  const evaluate = new Function('escapeHTML', 'console', `${source}; return { renderRecordingDetailBody };`);
  const quietConsole = { error() {} };
  const escapeHTML = (value) => String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
  const { renderRecordingDetailBody } = evaluate(escapeHTML, quietConsole);
  const output = renderRecordingDetailBody(
    '<header>recording</header>',
    [
      ['Capture health', () => { throw new Error('forced render rejection'); }],
      ['Schedule', () => '<section>schedule remains</section>'],
    ],
    '<section id="clipListBody">joined browser remains</section>',
  );

  assert.match(output, /Capture health/);
  assert.match(output, /temporarily unavailable/);
  assert.match(output, /schedule remains/);
  assert.match(output, /id="clipListBody"/);
  assert.doesNotMatch(output, /forced render rejection/);
});

test('joined augmentation success changes only joined values', () => {
  const source = sourceBetween(
    'function captureHealthPageKey(page)',
    'function captureHealthLocalParts(value, timezone)',
  );
  const evaluate = new Function(`${source}; return { mergeCaptureHealthJoined };`);
  const { mergeCaptureHealthJoined } = evaluate();
  const base = {
    recording_id: 700,
    from: '2026-08-01',
    to: '2026-08-01',
    capture_included: true,
    joined_included: false,
    bins: [{ start: '2026-08-01T08:00:00Z', end: '2026-08-01T09:00:00Z', captured: 60, expected: 60, health: 'healthy' }],
  };
  const joined = {
    recording_id: 700,
    from: '2026-08-01',
    to: '2026-08-01',
    capture_included: false,
    joined_included: true,
    bins: [{ start: '2026-08-01T08:00:00Z', end: '2026-08-01T09:00:00Z', source_duration_ms: 3600000, joined_ready_ms: 3300000 }],
  };
  const merged = mergeCaptureHealthJoined(base, joined);
  assert.equal(merged.bins[0].captured, 60);
  assert.equal(merged.bins[0].expected, 60);
  assert.equal(merged.bins[0].health, 'healthy');
  assert.equal(merged.bins[0].source_duration_ms, 3600000);
  assert.equal(merged.bins[0].joined_ready_ms, 3300000);
  assert.equal(merged.joined_included, true);
});

test('joined augmentation failure retains base captured bins', () => {
  const source = sourceBetween(
    'function captureHealthPageKey(page)',
    'function captureHealthLocalParts(value, timezone)',
  );
  const evaluate = new Function(`${source}; return { markCaptureHealthJoinedUnavailable };`);
  const { markCaptureHealthJoinedUnavailable } = evaluate();
  const base = {
    joined_status: 'loading',
    bins: [{ captured: 60, expected: 60, health: 'healthy', source_duration_ms: 0, joined_ready_ms: 0 }],
  };
  const unavailable = markCaptureHealthJoinedUnavailable(base);
  assert.notEqual(unavailable, base);
  assert.equal(unavailable.joined_status, 'error');
  assert.deepEqual(unavailable.bins, base.bins);
});

test('joined heatmap labels distinguish loading, failure, zero, and unavailable', () => {
  const source = sourceBetween(
    'function captureHealthJoinedLabel(bin, page)',
    'function captureHealthHeatmap(page)',
  );
  const evaluate = new Function(`${source}; return { captureHealthJoinedLabel };`);
  const { captureHealthJoinedLabel } = evaluate();
  const bin = { source_duration_ms: 3600000, joined_ready_ms: 1800000 };
  assert.equal(captureHealthJoinedLabel(bin, { joined_status: 'loading' }), 'Joined: loading');
  assert.equal(captureHealthJoinedLabel(bin, { joined_status: 'error' }), 'Joined: temporarily unavailable');
  assert.equal(captureHealthJoinedLabel(bin, { joined_status: 'ready' }), 'Joined: 50%');
  assert.equal(captureHealthJoinedLabel({ source_duration_ms: 3600000, joined_ready_ms: 0 }, { joined_status: 'ready' }), 'Joined: 0%');
  assert.equal(captureHealthJoinedLabel({ source_duration_ms: 0, joined_ready_ms: 0 }, { joined_status: 'ready' }), 'Joined: unavailable');
});

test('joined file browser collects every paginated result exactly once', async () => {
  const source = sourceBetween(
    'async function fetchAllJoinedPages(fetchPage, initial)',
    '// Render the recording detail view with only immutable, published joined media.',
  );
  const evaluate = new Function('CLIP_PAGE_SIZE', `${source}; return { fetchAllJoinedPages };`);
  const { fetchAllJoinedPages } = evaluate(2);
  const requestedOffsets = [];
  const result = await fetchAllJoinedPages(async (offset) => {
    requestedOffsets.push(offset);
    return { files: [{ artifact_id: offset + 1 }, { artifact_id: offset + 2 }] };
  }, { total: 6, files: [{ artifact_id: 1 }, { artifact_id: 2 }] });

  assert.deepEqual(requestedOffsets, [2, 4]);
  assert.deepEqual(result.files.map((file) => file.artifact_id), [1, 2, 3, 4, 5, 6]);
  assert.equal(new Set(result.files.map((file) => file.artifact_id)).size, 6);
});

test('joined file browser drills through month and weekday to leaf actions', () => {
  const source = sourceBetween(
    'function renderJoinedList(payload)',
    'function clipPagerHTML(page, pageCount)',
  );
  const body = { innerHTML: '' };
  const root = { querySelector: (selector) => selector === '#clipListBody' ? body : null };
  const clipPageState = { total: 3, joinedBrowsePath: [] };
  const escapeHTML = (value) => String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
  const evaluate = new Function(
    'els', 'clipPageState', 'escapeHTML', 'formatDuration', 'formatBytes', 'wireClipPageNav',
    `${source}; return { renderJoinedList, joinedFolderView };`,
  );
  const { renderJoinedList, joinedFolderView } = evaluate(
    { clipPage: root }, clipPageState, escapeHTML, (value) => `${value}ms`, (value) => `${value}B`, () => {},
  );
  const files = [
    { artifact_id: 1, relative_path: 'plaza/August/Thursday/a.mp4', download_path: '/joined/1', size_bytes: 10, scheduled_start_at: '2026-08-06T08:00:00Z', scheduled_end_at: '2026-08-06T09:00:00Z' },
    { artifact_id: 2, relative_path: 'plaza/August/Thursday/b.mp4', download_path: '/joined/2', size_bytes: 20, scheduled_start_at: '2026-08-06T09:00:00Z', scheduled_end_at: '2026-08-06T10:00:00Z' },
    { artifact_id: 3, relative_path: 'plaza/August/Friday/c.mp4', download_path: '/joined/3', size_bytes: 30, scheduled_start_at: '2026-08-07T08:00:00Z', scheduled_end_at: '2026-08-07T09:00:00Z' },
  ];

  assert.deepEqual(joinedFolderView(files, []).folders, [{ name: 'August', count: 3 }]);
  assert.deepEqual(joinedFolderView(files, ['August']).folders, [{ name: 'Thursday', count: 2 }, { name: 'Friday', count: 1 }]);
  assert.deepEqual(joinedFolderView(files, ['August', 'Thursday']).files.map(({ file }) => file.artifact_id), [1, 2]);

  clipPageState.joinedBrowsePath = ['August', 'Thursday'];
  renderJoinedList({ files });
  assert.match(body.innerHTML, /Joined clips[\s\S]*August[\s\S]*Thursday/);
  assert.equal((body.innerHTML.match(/>View<\/a>/g) || []).length, 2);
  assert.equal((body.innerHTML.match(/>Download<\/a>/g) || []).length, 2);
  assert.match(body.innerHTML, /href="\/joined\/1\?disposition=inline"/);
  assert.match(body.innerHTML, /href="\/joined\/2\?disposition=attachment"/);
  assert.doesNotMatch(body.innerHTML, /c\.mp4/);
});

function folderFunctions() {
  const source = sourceBetween(
    'const RECORDING_FOLDER_LEVELS =',
    '// Schedule-row label for relay recordings',
  );
	assert.doesNotMatch(source, /\bfetch\s*\(/, 'folder navigation must stay on the baseline recording payload');
  const escapeHTML = (value) => String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
  const joinedCoverage = (rec) => {
    const sourceMS = Number(rec && rec.source_duration_ms);
    const readyMS = Number(rec && rec.joined_ready_ms);
    const rawPercent = rec && rec.joined_percent;
    if (!(sourceMS > 0) || rawPercent == null || !Number.isFinite(Number(rawPercent))) {
      return { available: false, percent: null };
    }
    return { available: true, percent: Number(rawPercent), sourceMS, readyMS };
  };
  return new Function('escapeHTML', 'joinedCoverage', `${source}; return {
    recordingFolderSegments, buildRecordingFolderTree, folderNodeByKey,
    recordingMatchesFolder, recordingMatchesSearch, recordingSortFromSearch,
    folderSelectionFromSearch, recordingsListURL, sortRecordingItems,
    folderMetricText, folderNavigatorHTML, recordingFolderTreeSignature
  };`)(escapeHTML, joinedCoverage);
}

function folderFixture() {
  return [
    {
      id: 11, name: 'Plac one', status: 'completed', source_duration_ms: 100,
      joined_ready_ms: 50, joined_percent: 50,
      naming: { profile: 'plaza_hourly_v1', folder_name: '11_Europe_Poland_Swidnik_Plac_One', metadata: {
        plaza_id: '11', continent: 'Europe', country: 'Poland', city: 'Swidnik', plaza_name: 'Plac One',
      } },
    },
    {
      id: 12, name: 'Plac two', status: 'completed', source_duration_ms: 900,
      joined_ready_ms: 900, joined_percent: 100,
      naming: { profile: 'plaza_hourly_v1', folder_name: '12_Europe_Poland_Warsaw_Plac_Two', metadata: {
        plaza_id: '12', continent: 'Europe', country: 'Poland', city: 'Warsaw', plaza_name: 'Plac Two',
      } },
    },
  ];
}

test('recording folders build a canonical collision-safe hierarchy with honest fallbacks', () => {
  const { recordingFolderSegments, buildRecordingFolderTree, folderNodeByKey } = folderFunctions();
  const rows = folderFixture();
  rows.push({
    id: 13, name: 'Literal other', naming: { profile: 'plaza_hourly_v1', folder_name: 'literal', metadata: {
      continent: 'Other recordings', country: 'A/B', city: 'A_B', plaza_name: '<Literal>',
    } },
  });
  rows.push({ id: 14, name: 'Legacy recording', naming: { profile: 'stoarama_v1', folder_name: 'recordings', metadata: {} } });
  const tree = buildRecordingFolderTree(rows);
  assert.equal(tree.kind, 'all');
  assert.equal(tree.count, 4);
  assert.deepEqual(recordingFolderSegments(rows[0]).map((part) => part.kind), ['continent', 'country', 'city', 'plaza', 'recording']);
  const literal = recordingFolderSegments(rows[2]);
  const missing = recordingFolderSegments(rows[3]);
  assert.equal(literal[0].label, 'Other recordings');
  assert.equal(missing[0].label, 'Other recordings');
  assert.notEqual(literal[0].keyPart, missing[0].keyPart, 'literal fallback label must not collide with a missing level');
  assert.notEqual(literal[1].keyPart, literal[2].keyPart, 'slash and underscore labels must remain distinct');
	const samePlazaName = structuredClone(rows[0]);
	samePlazaName.id = 15;
	samePlazaName.naming.metadata.plaza_id = '15';
	assert.notEqual(recordingFolderSegments(rows[0])[3].keyPart, recordingFolderSegments(samePlazaName)[3].keyPart, 'plaza IDs must distinguish same-name plazas in one city');
  assert.ok(folderNodeByKey(tree, missing[3].nodeKey));
});

test('folder metrics are duration weighted and distinguish loading from unavailable', () => {
  const { buildRecordingFolderTree, folderMetricText, recordingFolderTreeSignature } = folderFunctions();
  const tree = buildRecordingFolderTree(folderFixture());
  assert.equal(folderMetricText(tree, false, false), '2 recordings · Joined loading');
  assert.equal(folderMetricText(tree, true, false), '2 recordings · 95% joined');
  assert.equal(folderMetricText(buildRecordingFolderTree([{ id: 9, name: 'No footage' }]), true, false), '1 recording · Joined unavailable');
  assert.equal(folderMetricText(tree, false, true), '2 recordings · Joined temporarily unavailable');
	const firstSignature = recordingFolderTreeSignature(tree, 'all', new Set(), true, false);
	const healthOnlyRows = structuredClone(folderFixture());
	healthOnlyRows[0].timeline_health = { grade: 'healthy' };
	assert.equal(recordingFolderTreeSignature(buildRecordingFolderTree(healthOnlyRows), 'all', new Set(), true, false), firstSignature);
	const joinedRows = structuredClone(folderFixture());
	joinedRows[0].joined_ready_ms = 100;
	assert.notEqual(recordingFolderTreeSignature(buildRecordingFolderTree(joinedRows), 'all', new Set(), true, false), firstSignature);
});

test('folder selection, search, deep links, and default joined sort compose deterministically', () => {
  const {
    buildRecordingFolderTree, folderNodeByKey, recordingMatchesFolder,
    recordingMatchesSearch, recordingSortFromSearch, folderSelectionFromSearch,
    recordingsListURL, sortRecordingItems,
  } = folderFunctions();
  const rows = folderFixture();
  rows.push({ id: 99, name: 'Unavailable', source_duration_ms: 0, joined_ready_ms: 0, joined_percent: null });
  const tree = buildRecordingFolderTree(rows);
  const poland = [...tree.byKey.values()].find((node) => node.kind === 'country' && node.label === 'Poland');
  assert.ok(poland);
  assert.equal(rows.filter((row) => recordingMatchesFolder(row, poland)).length, 2);
  assert.equal(recordingMatchesSearch(rows[0], 'swidnik plac'), true);
  assert.equal(recordingMatchesSearch(rows[1], 'swidnik'), false);
  assert.equal(recordingSortFromSearch(''), 'joined_desc');
  assert.equal(recordingSortFromSearch('?sort=not-valid'), 'joined_desc');
  assert.equal(recordingSortFromSearch('?sort=newest'), 'newest');
  assert.deepEqual(sortRecordingItems(rows, recordingSortFromSearch('')).map((row) => row.id), [12, 11, 99]);
  assert.deepEqual(sortRecordingItems(rows, 'joined_asc').map((row) => row.id), [11, 12, 99]);
  assert.equal(folderSelectionFromSearch(`?folder=${encodeURIComponent(poland.key)}`), poland.key);
  assert.equal(folderNodeByKey(tree, folderSelectionFromSearch('?folder=missing')), null);
	const refreshedRows = structuredClone(rows);
	refreshedRows[0].joined_ready_ms = 100;
	refreshedRows[0].joined_percent = 100;
	assert.ok(folderNodeByKey(buildRecordingFolderTree(refreshedRows), poland.key), 'metric rerenders must retain the selected folder key');
  const nextURL = recordingsListURL('https://stoarama.com/recordings?quality=fine_plus', poland.key, 'joined_asc');
  assert.match(nextURL, /^\/recordings\?/);
  assert.match(nextURL, /quality=fine_plus/);
  assert.match(nextURL, /folder=/);
  assert.match(nextURL, /sort=joined_asc/);
});

test('folder navigator uses disclosure buttons and escaped labels without claiming ARIA tree behavior', () => {
  const { buildRecordingFolderTree, folderNavigatorHTML } = folderFunctions();
  const rows = folderFixture();
  rows[0].naming.metadata.plaza_name = '<Plac & One>';
  const tree = buildRecordingFolderTree(rows);
  const europe = [...tree.byKey.values()].find((node) => node.kind === 'continent' && node.label === 'Europe');
  const html = folderNavigatorHTML(tree, {
    selectedKey: europe.key,
    openKeys: new Set([europe.key]),
    joinedLoaded: true,
    joinedError: false,
  }, 'test');
  assert.match(html, /<nav[^>]+aria-label="Recording folders"/);
  assert.match(html, /<button[^>]+data-folder-toggle=/);
  assert.match(html, /aria-expanded="true"/);
  assert.match(html, /aria-controls="folder-test-/);
  assert.match(html, /data-folder-select=/);
	assert.match(html, /style="--folder-indent:\d+px"/);
  assert.match(html, /2 recordings · 95% joined/);
  assert.match(html, /&lt;Plac &amp; One&gt;/);
  assert.doesNotMatch(html, /role="tree"/);
});
