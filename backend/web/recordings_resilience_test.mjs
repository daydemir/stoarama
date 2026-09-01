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

test('a rejected detail card preserves healthy cards and the joined folder link', () => {
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
    '<header><a class="joined-folder-cta">Browse joined folder</a></header>',
    [
      ['Capture health', () => { throw new Error('forced render rejection'); }],
      ['Schedule', () => '<section>schedule remains</section>'],
    ],
  );

  assert.match(output, /Capture health/);
  assert.match(output, /temporarily unavailable/);
  assert.match(output, /schedule remains/);
  assert.match(output, /Browse joined folder/);
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

test('heatmap legend explains capture colors and every joined corner-mark state', () => {
  const source = sourceBetween(
    'function captureHealthLegendHTML()',
    'function captureHealthCardHTML()',
  );
  const evaluate = new Function(`${source}; return { captureHealthLegendHTML };`);
  const output = evaluate().captureHealthLegendHTML();

  assert.match(output, /aria-label="Hourly heatmap legend"/);
  assert.match(output, /Capture/);
  assert.match(output, /98% or better/);
  assert.match(output, /90% to 97\.9%/);
  assert.match(output, /Below 90%/);
  assert.match(output, /Not scheduled/);
  assert.match(output, /Joined mark/);
  assert.match(output, /Check means 100% joined/);
  assert.match(output, /Number is joined percentage/);
  assert.match(output, /No mark means 0%, loading, or unavailable/);
  assert.match(output, /<svg viewBox="0 0 10 10">/);
  assert.doesNotMatch(output, /<(?:a|button|input|select|textarea)\b/i);
  assert.doesNotMatch(output, /\bdata-[a-z-]+=/i);
});
