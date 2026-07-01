#!/usr/bin/env python3
"""Stoarama NAS pull client.

MIT's on-prem Synology NAS is not inbound-reachable, so Stoarama records public
streams to managed Cloudflare R2 and this client (running ON the NAS) PULLS each
clip down, verifies it byte-for-byte, then asks Stoarama to RELEASE the managed
copy. Confirm-before-release is mandatory: a clip is released only after it has
been downloaded AND its byte count matches the API's recorded size_bytes. Release
detaches the clip from the account (billing stops, it leaves the feed) but the
Stoarama-side R2 object is KEPT; the client's local copy on the NAS is the working
copy.

Dependency-free by design: Python 3 standard library only (urllib, json, os,
time, pathlib). No pip installs, so it runs unchanged in a python:3-slim
container under Synology Container Manager.

The loop drains an account-wide, forward-cursored clips feed:
  GET  {BASE}/account/clips?after_id={cursor}&limit=200   (lists active clips)
  GET  {BASE}{clip.download_path}                         (presigns the R2 GET)
  POST {BASE}/account/recordings/{rid}/clips/{cid}/release (releases after pull)
  POST {BASE}/account/connections/heartbeat              (status each tick)
all with header `Authorization: Bearer {STOARAMA_API_KEY}` (a sir_ account key).

Each tick the client posts a heartbeat {cursor_id, clips_pulled} so the Stoarama
account UI can show this connection as healthy/stale and report progress. The
heartbeat is best-effort: a failure is logged and never breaks the drain loop.

The download endpoint returns JSON {"url": "<presigned>", "size_bytes": ...},
not a 302 redirect, so we parse the body and fetch the `url` field.

Environment:
  STOARAMA_API_BASE         e.g. https://stoarama.com/api/v1   (required)
  STOARAMA_API_KEY          sir_... account API key            (required)
  STOARAMA_OUTPUT_DIR       clip destination dir (default /clips)
  STOARAMA_STATE_FILE       cursor file       (default /state/cursor.json)
  STOARAMA_POLL_INTERVAL_SEC  idle sleep seconds (default 60, i.e. 1 minute)
  STOARAMA_DRY_RUN          "1" = read-only validate (default "0")

DRY-RUN ("1"): download + verify every clip and log "would release", but never
call release and never persist the cursor (it advances in memory only). This is
the safe, repeatable connectivity/integrity check the operator runs first; it
never releases anything and never loses its place. Set to "0" for normal operation.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
import urllib.parse
from pathlib import Path

LIST_PAGE_LIMIT = 200
HTTP_TIMEOUT_SEC = 120
ERROR_BACKOFF_SEC = 30
# A named User-Agent is required: stoarama.com sits behind Cloudflare, which blocks
# the stdlib default "Python-urllib/x" agent (HTTP 403, error code 1010). Every
# request the client makes (API + presigned R2 GET) sets this so the loop works.
USER_AGENT = "stoarama-nas-pull/1.0"


def env_str(name, default):
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def env_int(name, default):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        log("WARN", "invalid int for %s=%r, using default %d" % (name, raw, default))
        return default


def log(level, message):
    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    print("%s %s %s" % (stamp, level, message), flush=True)


class Config:
    def __init__(self):
        self.api_base = env_str("STOARAMA_API_BASE", "").rstrip("/")
        self.api_key = env_str("STOARAMA_API_KEY", "")
        self.output_dir = Path(env_str("STOARAMA_OUTPUT_DIR", "/clips"))
        self.state_file = Path(env_str("STOARAMA_STATE_FILE", "/state/cursor.json"))
        # 60-second default cadence: we hold each clip only until the next pull, so
        # this bounds managed footage to ~1 min per stream. At that hold time managed
        # storage is effectively free, which is what the NAS storage price is based on.
        self.poll_interval_sec = env_int("STOARAMA_POLL_INTERVAL_SEC", 60)
        self.dry_run = env_str("STOARAMA_DRY_RUN", "0") == "1"
        # The list response's download_path is a site-root-absolute path that already
        # includes the /api/v1 prefix, so it is joined onto the ORIGIN (scheme+host),
        # not onto api_base (which already ends in /api/v1). Joining it onto api_base
        # would double the prefix to /api/v1/api/v1/... and 404.
        parts = urllib.parse.urlsplit(self.api_base)
        self.origin = "%s://%s" % (parts.scheme, parts.netloc) if parts.scheme else ""

    def validate(self):
        if not self.api_base:
            raise SystemExit("STOARAMA_API_BASE is required (e.g. https://stoarama.com/api/v1)")
        if not self.api_key:
            raise SystemExit("STOARAMA_API_KEY is required (a sir_ account API key)")


def request_json(cfg, method, path_or_url, base=None, body=None):
    """Call a Stoarama API endpoint with the Bearer key; return parsed JSON.

    path_or_url may be an absolute URL or a path joined onto base (default
    api_base). The download_path from the list response already carries the
    /api/v1 prefix, so it is passed with base=cfg.origin to avoid doubling it.
    `body`, if given, is JSON-encoded and sent with Content-Type application/json
    (used by the heartbeat POST). Raises urllib.error.HTTPError / URLError on
    transport failure so the caller can back off rather than crash.
    """
    if base is None:
        base = cfg.api_base
    url = path_or_url if path_or_url.startswith("http") else base + path_or_url
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, method=method, data=data)
    req.add_header("Authorization", "Bearer " + cfg.api_key)
    req.add_header("User-Agent", USER_AGENT)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as resp:
        body_bytes = resp.read()
    if not body_bytes:
        return {}
    return json.loads(body_bytes.decode("utf-8"))


def send_heartbeat(cfg, cursor_id, clips_pulled):
    """Best-effort POST of the connection heartbeat. Never raises: a heartbeat
    failure must not interrupt the drain loop (logged + swallowed)."""
    try:
        request_json(
            cfg,
            "POST",
            "/account/connections/heartbeat",
            body={"cursor_id": int(cursor_id), "clips_pulled": int(clips_pulled)},
        )
    except urllib.error.HTTPError as exc:
        log("WARN", "heartbeat failed: HTTP %s (continuing)" % exc.code)
    except urllib.error.URLError as exc:
        log("WARN", "heartbeat failed: %s (continuing)" % exc)


def load_cursor(cfg):
    try:
        data = json.loads(cfg.state_file.read_text())
        after_id = int(data.get("after_id", 0))
        if after_id < 0:
            return 0
        return after_id
    except FileNotFoundError:
        return 0
    except (ValueError, OSError) as exc:
        log("WARN", "could not read state file %s (%s); starting from 0" % (cfg.state_file, exc))
        return 0


def persist_cursor(cfg, after_id):
    cfg.state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = cfg.state_file.with_suffix(cfg.state_file.suffix + ".tmp")
    tmp.write_text(json.dumps({"after_id": after_id}))
    os.replace(str(tmp), str(cfg.state_file))


def clip_filename(clip):
    """Derive a stable, filesystem-safe .mp4 name from the clip start instant.

    clip_start_at is RFC3339 (e.g. 2026-06-30T12:00:00Z); we strip separators so
    the name sorts chronologically and is safe on the NAS filesystem.
    """
    start = str(clip.get("clip_start_at", "")).strip()
    safe = "".join(ch if ch.isalnum() else "-" for ch in start).strip("-")
    if not safe:
        safe = "clip-%d" % int(clip["clip_id"])
    return safe + ".mp4"


def download_to_temp(presigned_url, temp_path):
    """Stream the presigned URL to temp_path; return the byte count written."""
    req = urllib.request.Request(presigned_url, method="GET")
    req.add_header("User-Agent", USER_AGENT)
    written = 0
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as resp, open(temp_path, "wb") as out:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            written += len(chunk)
    return written


def process_clip(cfg, clip):
    """Download, verify, atomically place, and (unless dry-run) release one clip.

    Returns True on a clean (download + verify) so the caller may advance the
    cursor; False on any mismatch or error so the cursor stays put and the clip
    is retried next tick. The release step happens only after a verified download.
    """
    clip_id = int(clip["clip_id"])
    recording_id = int(clip["recording_id"])
    expected_bytes = int(clip["size_bytes"])
    download_path = clip["download_path"]

    # 1. Presign the R2 GET via the existing download endpoint (returns JSON).
    #    download_path is site-root-absolute (already includes /api/v1), so it is
    #    joined onto the origin, not api_base, to avoid a doubled /api/v1 prefix.
    try:
        presigned = request_json(cfg, "GET", download_path, base=cfg.origin)
    except urllib.error.HTTPError as exc:
        if exc.code == 410:
            # Clip already released/purged upstream: nothing to pull, safe to advance.
            log("WARN", "clip %d no longer available upstream (410); advancing cursor" % clip_id)
            return True
        log("ERROR", "presign clip %d failed: HTTP %s" % (clip_id, exc.code))
        return False
    presigned_url = presigned.get("url")
    if not presigned_url:
        log("ERROR", "presign clip %d returned no url" % clip_id)
        return False

    rec_dir = cfg.output_dir / str(recording_id)
    rec_dir.mkdir(parents=True, exist_ok=True)
    final_path = rec_dir / clip_filename(clip)
    temp_path = rec_dir / (final_path.name + ".part")

    # 2. Download the bytes to a temp file under OUTPUT_DIR.
    try:
        written = download_to_temp(presigned_url, str(temp_path))
    except (urllib.error.URLError, OSError) as exc:
        log("ERROR", "download clip %d failed: %s" % (clip_id, exc))
        _safe_unlink(temp_path)
        return False

    # 3. Verify byte count == size_bytes (confirm-before-purge). On mismatch:
    #    log, do NOT purge, do NOT advance; retry next tick.
    if written != expected_bytes:
        log(
            "ERROR",
            "clip %d size mismatch: got %d bytes, expected %d; not purging, will retry"
            % (clip_id, written, expected_bytes),
        )
        _safe_unlink(temp_path)
        return False

    # 4. Atomically move temp -> final.
    os.replace(str(temp_path), str(final_path))

    if cfg.dry_run:
        log(
            "INFO",
            "DRY-RUN clip_id=%d recording_id=%d bytes=%d saved=%s would release (no release, cursor not persisted)"
            % (clip_id, recording_id, written, final_path),
        )
        return True

    # 5. Release the managed copy (detach from the account; the upstream R2 object
    #    is KEPT). Already-released is also 200 (idempotent); a 410 here likewise
    #    means the upstream copy is no longer on the account, which is the goal.
    release_path = "/account/recordings/%d/clips/%d/release" % (recording_id, clip_id)
    try:
        request_json(cfg, "POST", release_path)
    except urllib.error.HTTPError as exc:
        if exc.code == 410:
            log("INFO", "clip %d already released upstream (410)" % clip_id)
        else:
            log("ERROR", "release clip %d failed: HTTP %s; keeping local copy, will retry" % (clip_id, exc.code))
            return False
    except urllib.error.URLError as exc:
        log("ERROR", "release clip %d failed: %s; keeping local copy, will retry" % (clip_id, exc))
        return False

    log(
        "INFO",
        "clip_id=%d recording_id=%d bytes=%d saved=%s released"
        % (clip_id, recording_id, written, final_path),
    )
    return True


def _safe_unlink(path):
    try:
        os.unlink(str(path))
    except OSError:
        pass


def run_tick(cfg, cursor, total_pulled):
    """Drain one page of clips from `cursor`. Returns (new_cursor, total_pulled).

    For each clip in order: process it; on success advance the cursor, bump the
    running total, and (unless dry-run) persist the cursor; on failure stop the
    page so the failed clip is the next one retried (strict in-order, drain-once).
    A heartbeat is posted after the page so the account UI tracks progress.
    """
    page = request_json(cfg, "GET", "/account/clips?after_id=%d&limit=%d" % (cursor, LIST_PAGE_LIMIT))
    clips = page.get("clips", [])
    if not clips:
        send_heartbeat(cfg, cursor, total_pulled)
        return cursor, total_pulled

    log("INFO", "fetched %d clip(s) after_id=%d" % (len(clips), cursor))
    for clip in clips:
        if not process_clip(cfg, clip):
            # Stop here; cursor stays at the last successful clip so this one is
            # retried first next tick.
            break
        cursor = int(clip["clip_id"])
        total_pulled += 1
        if not cfg.dry_run:
            persist_cursor(cfg, cursor)
    send_heartbeat(cfg, cursor, total_pulled)
    return cursor, total_pulled


def main():
    cfg = Config()
    cfg.validate()
    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    mode = "DRY-RUN (no releases, cursor not persisted)" if cfg.dry_run else "LIVE"
    cursor = load_cursor(cfg)
    total_pulled = 0
    log("INFO", "stoarama pull starting mode=%s api_base=%s output_dir=%s cursor=%d"
        % (mode, cfg.api_base, cfg.output_dir, cursor))

    while True:
        try:
            new_cursor, total_pulled = run_tick(cfg, cursor, total_pulled)
            if new_cursor == cursor:
                # Empty page (or first clip failed): idle before polling again.
                time.sleep(cfg.poll_interval_sec)
            cursor = new_cursor
        except urllib.error.HTTPError as exc:
            log("ERROR", "list page failed: HTTP %s; backing off %ds" % (exc.code, ERROR_BACKOFF_SEC))
            time.sleep(ERROR_BACKOFF_SEC)
        except urllib.error.URLError as exc:
            log("ERROR", "list page failed: %s; backing off %ds" % (exc, ERROR_BACKOFF_SEC))
            time.sleep(ERROR_BACKOFF_SEC)
        except KeyboardInterrupt:
            log("INFO", "interrupted; exiting")
            return
        except Exception as exc:  # noqa: BLE001 - never crash-loop the daemon
            log("ERROR", "unexpected error: %s; backing off %ds" % (exc, ERROR_BACKOFF_SEC))
            time.sleep(ERROR_BACKOFF_SEC)


if __name__ == "__main__":
    sys.exit(main())
