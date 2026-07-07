(function () {
  const captureTypes = Object.freeze(['youtube_watch', 'hls', 'dash', 'rtsp', 'rtmp', 'http_video', 'still_image', 'webrtc', 'unknown']);
  const captureTypeLabels = Object.freeze({
    youtube_watch: 'YouTube',
    hls: 'HLS',
    dash: 'DASH',
    rtsp: 'RTSP',
    rtmp: 'RTMP',
    http_video: 'Direct Video',
    still_image: 'Still Image',
    webrtc: 'WebRTC',
    unknown: 'Unknown',
  });

  function esc(v) {
    return String(v ?? '').replace(/[&<>"']/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
  }

  async function fetchJSON(path, opts = {}) {
    const method = String((opts && opts.method) || 'GET').toUpperCase();
    const headers = { ...(opts.headers || {}) };
    if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(method) && !headers['Idempotency-Key']) {
      headers['Idempotency-Key'] = `${method}:${path}:${Date.now()}:${Math.random().toString(36).slice(2)}`;
    }
    const res = await fetch(path, { ...opts, credentials: 'same-origin', headers });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(payload.error || `http ${res.status}`);
    return payload;
  }

  function normalizeCaptureType(raw) {
    const value = String(raw || '').trim().toLowerCase();
    return captureTypes.includes(value) ? value : '';
  }

  function captureTypeLabel(raw) {
    const key = normalizeCaptureType(raw);
    return key ? captureTypeLabels[key] : '-';
  }

  function formatBytes(value) {
    const bytes = Number(value);
    if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const exp = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const amount = bytes / (1024 ** exp);
    const digits = amount >= 100 || exp === 0 ? 0 : amount >= 10 ? 1 : 2;
    return `${amount.toFixed(digits)} ${units[exp]}`;
  }

  function sanitizeLimit(raw, fallback, min, max) {
    const n = Number(raw);
    if (!Number.isFinite(n)) return fallback;
    return String(Math.max(min, Math.min(max, Math.floor(n))));
  }

  function normalizeSortDir(raw, fallback = 'desc') {
    const value = String(raw || '').trim().toLowerCase();
    if (value === 'asc' || value === 'desc') return value;
    return fallback === 'asc' ? 'asc' : 'desc';
  }

  function debounce(fn, ms) {
    let timer = null;
    return (...args) => {
      if (timer !== null) clearTimeout(timer);
      timer = window.setTimeout(() => {
        timer = null;
        fn(...args);
      }, ms);
    };
  }

  function splitCSV(raw) {
    return String(raw || '').split(',').map((s) => s.trim()).filter((s) => s.length > 0);
  }

  function normalizeTags(tags) {
    const seen = new Set();
    const out = [];
    for (const tag of (tags || [])) {
      const clean = String(tag || '').trim();
      if (!clean || seen.has(clean)) continue;
      seen.add(clean);
      out.push(clean);
    }
    return out;
  }

  window.StoaramaDashboard = Object.freeze({
    CAPTURE_TYPES: captureTypes,
    esc,
    fetchJSON,
    normalizeCaptureType,
    captureTypeLabel,
    formatBytes,
    sanitizeLimit,
    normalizeSortDir,
    debounce,
    splitCSV,
    normalizeTags,
  });
})();
