#!/usr/bin/env python3
"""Stoarama NAS pull client. Python standard library only."""

import argparse
import concurrent.futures
import datetime
import errno
import fcntl
import hashlib
import json
import math
import os
import re
import signal
import socket
import sqlite3
import stat as stat_module
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from enum import Enum
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

CLIENT_VERSION = "development"
LIST_PAGE_LIMIT = 200
DEFAULT_DOWNLOAD_WORKERS = 12
MAX_DOWNLOAD_WORKERS = 32
DOWNLOAD_ATTEMPTS = 3
HTTP_TIMEOUT_SEC = 120
HEARTBEAT_TIMEOUT_SEC = 20
HEARTBEAT_INTERVAL_SEC = 30
STORAGE_TELEMETRY_MAX_AGE_SEC = HEARTBEAT_INTERVAL_SEC * 3
DEFAULT_MIN_FREE_BYTES = 1_250_000_000_000
CAPACITY_RESUME_HYSTERESIS_BYTES = 100_000_000_000
UPDATE_INTERVAL_SEC = 600
INVENTORY_SCAN_INTERVAL_SEC = 24 * 60 * 60
INVENTORY_SYNC_BATCH = 200
INVENTORY_SHUTDOWN_TIMEOUT_SEC = HTTP_TIMEOUT_SEC + 5
ERROR_BACKOFF_SEC = 30
USER_AGENT = "stoarama-nas-pull/%s" % CLIENT_VERSION
JOINED_ROOT = "joined"
JOINED_PROTOCOL_VERSION = 1
JOINED_RANGE_BYTES = 64 * 1024 * 1024
JOINED_MAX_BYTES = 5 * 1024 * 1024 * 1024 - 5 * 1024 * 1024
JOINED_MANIFEST_MAX_BYTES = 16 * 1024 * 1024

INVENTORY_SKIP_REASONS = frozenset((
    "changed_during_hash", "invalid_sidecar", "io_error",
    "permission_denied", "unexpected", "vanished_during_scan",
))
MEDIA_CERTIFICATION_SCHEMA_VERSION = 1
MEDIA_CERTIFICATION_MAX_CLIPS = 8
MEDIA_CERTIFICATION_MAX_BYTES = 512 * 1024 * 1024
MEDIA_CERTIFICATION_TEMP_RESERVE_BYTES = 256 * 1024 * 1024
MEDIA_CERTIFICATION_TOOL_TIMEOUT_SEC = 180
MEDIA_CERTIFICATION_DURATION_TOLERANCE_SEC = 2.0
MEDIA_CERTIFICATION_PROTOCOLS = "file,pipe"
MEDIA_CERTIFICATION_FORMATS = frozenset(("mov", "mp4", "m4a", "3gp", "3g2", "mj2"))
NATIVE_STITCH_MAX_CLIPS = 1024
NATIVE_STITCH_MAX_SOURCE_BYTES = 64 * 1024 * 1024 * 1024
NATIVE_STITCH_MAX_RUN_BYTES = 32 * 1024 * 1024 * 1024
NATIVE_STITCH_TEMP_RESERVE_BYTES = 2 * 1024 * 1024 * 1024
NATIVE_STITCH_ATTEMPT_SEC = 35 * 60
NATIVE_STITCH_DELIVERY_POLL_SEC = 5
NATIVE_STITCH_COMPLETION_MARGIN_SEC = 5 * 60


class ExistingFileMismatch(RuntimeError):
    pass


class FileChangedDuringHash(RuntimeError):
    pass


class RetryExhausted(RuntimeError):
    def __init__(self, cause, retries):
        super().__init__(str(cause))
        self.retries = retries


class InventoryProgressError(RuntimeError):
    pass


class InventoryScanStopped(RuntimeError):
    pass


class SelfUpdateExecError(RuntimeError):
    pass


class MediaCertificationError(RuntimeError):
    pass


class ToolProcessError(MediaCertificationError):
    """Bounded, private subprocess evidence; its raw stderr is never reported."""
    def __init__(self, returncode, stderr):
        super().__init__("media verification tool failed")
        self.returncode = returncode
        self.stderr = stderr


class DeterministicMediaError(MediaCertificationError):
    pass


class JoinedDownloadYield(RuntimeError):
    pass


class RejectJoinedRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, _request, _file_pointer, _code, _message, _headers, _new_url):
        return None


def open_joined_url(request):
    return urllib.request.build_opener(RejectJoinedRedirects()).open(request, timeout=HTTP_TIMEOUT_SEC)


def inventory_skip_reason(exc, sidecar):
    if isinstance(exc, FileChangedDuringHash):
        return "changed_during_hash"
    if isinstance(exc, FileNotFoundError):
        return "vanished_during_scan"
    if isinstance(exc, PermissionError) or (isinstance(exc, OSError) and exc.errno in (errno.EACCES, errno.EPERM)):
        return "permission_denied"
    if sidecar and isinstance(exc, (json.JSONDecodeError, KeyError, TypeError, ValueError)):
        return "invalid_sidecar"
    if isinstance(exc, OSError):
        return "io_error"
    return "unexpected"


class Phase(str, Enum):
    STARTING = "starting"
    IDLE = "idle"
    DRAINING = "draining"
    UPDATING = "updating"
    BLOCKED = "blocked"
    DEGRADED = "degraded"
    CERTIFYING = "certifying"


class PreviousExit(str, Enum):
    UNKNOWN = "unknown"
    CLEAN = "clean"
    SELF_UPDATE = "self_update"
    UNCLEAN_PROCESS = "unclean_process"
    UNCLEAN_REBOOT = "unclean_reboot"


class OutageClass(str, Enum):
    DNS = "dns_failed"
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    HTTP = "http"
    OTHER = "other"


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log(level, message):
    print("%s %s %s" % (utc_now(), level, message), flush=True)


def env_str(name, default):
    value = os.environ.get(name, "").strip()
    return value or default


def env_int(name, default):
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SystemExit("%s must be an integer" % name) from exc


def fsync_dir(path):
    fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path, data, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    fd = os.open(str(temp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(data)
            out.flush()
            os.fsync(out.fileno())
        os.replace(str(temp), str(path))
        fsync_dir(path.parent)
    except BaseException:
        try:
            os.unlink(str(temp))
        except FileNotFoundError:
            pass
        raise


def read_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except (OSError, ValueError) as exc:
        raise RuntimeError("invalid state file %s: %s" % (path, exc)) from exc


class Config:
    def __init__(self):
        self.api_base = env_str("STOARAMA_API_BASE", "").rstrip("/")
        self.api_key = env_str("STOARAMA_API_KEY", "")
        self.output_dir = Path(env_str("STOARAMA_OUTPUT_DIR", "/clips"))
        self.state_dir = Path(env_str("STOARAMA_STATE_DIR", "/state"))
        self.progress_file = self.state_dir / "progress.json"
        self.legacy_progress_file = self.state_dir / "cursor.json"
        self.runtime_file = self.state_dir / "runtime.json"
        self.outage_file = self.state_dir / "outage.json"
        self.capacity_file = self.state_dir / "capacity.json"
        self.inventory_file = self.state_dir / "inventory.sqlite3"
        self.current_file = self.state_dir / "stoarama_pull.py"
        self.candidate_file = self.state_dir / "stoarama_pull.candidate.py"
        self.previous_file = self.state_dir / "stoarama_pull.previous.py"
        self.lock_file = self.state_dir / "client.lock"
        self.poll_interval_sec = env_int("STOARAMA_POLL_INTERVAL_SEC", 60)
        self.download_workers = env_int("STOARAMA_DOWNLOAD_WORKERS", DEFAULT_DOWNLOAD_WORKERS)
        self.inventory_scan_interval_sec = env_int("STOARAMA_INVENTORY_SCAN_INTERVAL_SEC", INVENTORY_SCAN_INTERVAL_SEC)
        # Hash throughput already bounds disk pressure. An additional per-file
        # sleep makes a 100k+ first scan spend hours idle on small clips.
        self.inventory_scan_delay_ms = env_int("STOARAMA_INVENTORY_SCAN_DELAY_MS", 0)
        self.inventory_hash_mbps = env_int("STOARAMA_INVENTORY_HASH_MBPS", 20)
        self.min_free_bytes = env_int("STOARAMA_MIN_FREE_BYTES", DEFAULT_MIN_FREE_BYTES)
        self.update_manifest_url = env_str(
            "STOARAMA_UPDATE_MANIFEST_URL", "https://stoarama.com/nas/download/latest.json"
        )
        self.dry_run = env_str("STOARAMA_DRY_RUN", "0") == "1"
        # Dormant by default. Version 1 is activated only for a specifically
        # approved connection after the backend feed and capacity gates pass.
        self.joined_protocol_version = env_int("STOARAMA_JOINED_PROTOCOL_VERSION", 0)
        # Release/deploy safe by default. Operators enable only after migration,
        # API and a completed clean inventory are independently verified.
        self.native_stitch_enabled = env_str("STOARAMA_NATIVE_STITCH_ENABLED", "false").lower() == "true"
        self.is_candidate = env_str("STOARAMA_CANDIDATE", "0") == "1"
        parsed = urllib.parse.urlsplit(self.api_base)
        self.origin = "%s://%s" % (parsed.scheme, parsed.netloc) if parsed.scheme else ""

    def validate(self):
        if not self.api_base or not self.origin:
            raise SystemExit("STOARAMA_API_BASE must be an absolute URL")
        if not self.api_key:
            raise SystemExit("STOARAMA_API_KEY is required")
        if self.poll_interval_sec < 10 or self.poll_interval_sec > 3600:
            raise SystemExit("STOARAMA_POLL_INTERVAL_SEC must be between 10 and 3600")
        if self.download_workers < 1 or self.download_workers > MAX_DOWNLOAD_WORKERS:
            raise SystemExit("STOARAMA_DOWNLOAD_WORKERS must be between 1 and %d" % MAX_DOWNLOAD_WORKERS)
        if self.inventory_scan_interval_sec < 300 or self.inventory_scan_delay_ms < 0 or self.inventory_hash_mbps < 1 or self.inventory_hash_mbps > 1000:
            raise SystemExit("invalid NAS inventory scan cadence")
        if self.min_free_bytes < 1:
            raise SystemExit("STOARAMA_MIN_FREE_BYTES must be positive")
        if self.joined_protocol_version not in (0, JOINED_PROTOCOL_VERSION):
            raise SystemExit("STOARAMA_JOINED_PROTOCOL_VERSION must be 0 or %d" % JOINED_PROTOCOL_VERSION)


def boot_id():
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError:
        return "unknown"


def request_json(cfg, method, path_or_url, base=None, body=None, timeout=HTTP_TIMEOUT_SEC, authenticate=True):
    base = cfg.api_base if base is None else base
    url = path_or_url if path_or_url.startswith("http") else base + path_or_url
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, method=method, data=data)
    req.add_header("User-Agent", USER_AGENT)
    if authenticate:
        req.add_header("Authorization", "Bearer " + cfg.api_key)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw = response.read()
    return json.loads(raw.decode("utf-8")) if raw else {}


def classify_transport_error(exc):
    reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
    if isinstance(reason, socket.gaierror):
        return OutageClass.DNS
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return OutageClass.TIMEOUT
    if isinstance(reason, (ConnectionError, ConnectionRefusedError, ConnectionResetError)):
        return OutageClass.CONNECTION
    if isinstance(exc, urllib.error.HTTPError):
        return OutageClass.HTTP
    return OutageClass.OTHER


def transient_error(exc):
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code == 429 or 500 <= exc.code < 600
    return classify_transport_error(exc) in (OutageClass.DNS, OutageClass.TIMEOUT, OutageClass.CONNECTION)


def retry_transient(operation, clip_id, phase):
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            return operation(), attempt - 1
        except Exception as exc:
            if not transient_error(exc):
                raise
            if attempt == DOWNLOAD_ATTEMPTS:
                raise RetryExhausted(exc, attempt - 1) from exc
            log(
                "WARN",
                "clip_id=%d %s retry=%d/%d class=%s"
                % (clip_id, phase, attempt, DOWNLOAD_ATTEMPTS - 1, classify_transport_error(exc).value),
            )
            time.sleep(attempt)


def utc_now_precise():
    now_ns = time.time_ns()
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now_ns // 1000000000)) + (".%06dZ" % (now_ns // 1000 % 1000000))


class Inventory:
    """Durable, locally authoritative NAS clip inventory.

    SQLite is intentionally stored on the separate /state mount. Clip writes are
    committed before the server is told it may release a clip. Full scans run in
    the background and use generations so a crash or partial upload can never
    turn an incomplete scan into a mass-missing report.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.lock = threading.RLock()
        # Whole-operation activity fence. Inventory holds this from the first
        # filesystem observation through completion publication; certification
        # only takes it nonblocking at a main-loop delivery-idle boundary.
        self.activity_lock = threading.RLock()
        self.db = sqlite3.connect(str(cfg.inventory_file), timeout=30, check_same_thread=False)
        with self.lock:
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS files (
                    clip_id INTEGER PRIMARY KEY,
                    recording_id INTEGER NOT NULL,
                    relative_path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    state TEXT NOT NULL,
                    verified_at TEXT,
                    file_mtime_ns INTEGER NOT NULL DEFAULT 0,
                    file_ctime_ns INTEGER NOT NULL DEFAULT 0,
                    file_inode INTEGER NOT NULL DEFAULT 0,
                    file_device INTEGER NOT NULL DEFAULT 0,
                    sidecar_relative_path TEXT NOT NULL DEFAULT '',
                    sidecar_size_bytes INTEGER NOT NULL DEFAULT 0,
                    sidecar_sha256 TEXT NOT NULL DEFAULT '',
                    clip_start_us INTEGER NOT NULL DEFAULT 0,
                    clip_end_us INTEGER NOT NULL DEFAULT 0,
                    seen_generation TEXT NOT NULL DEFAULT '',
                    scan_pass TEXT NOT NULL DEFAULT '',
                    client_updated_at TEXT NOT NULL,
                    dirty INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS files_dirty_clip ON files(dirty, clip_id);
                CREATE INDEX IF NOT EXISTS files_generation ON files(seen_generation);
                CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS unmatched_files (
                    relative_path TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    state TEXT NOT NULL,
                    file_mtime_ns INTEGER NOT NULL DEFAULT 0,
                    file_ctime_ns INTEGER NOT NULL DEFAULT 0,
                    file_inode INTEGER NOT NULL DEFAULT 0,
                    file_device INTEGER NOT NULL DEFAULT 0,
                    seen_generation TEXT NOT NULL DEFAULT '',
                    scan_pass TEXT NOT NULL DEFAULT '',
                    client_updated_at TEXT NOT NULL,
                    dirty INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS unmatched_dirty_path ON unmatched_files(dirty,relative_path);
                """
            )
            added_columns = {
                "scan_pass": "TEXT NOT NULL DEFAULT ''",
                "file_ctime_ns": "INTEGER NOT NULL DEFAULT 0",
                "file_inode": "INTEGER NOT NULL DEFAULT 0",
                "file_device": "INTEGER NOT NULL DEFAULT 0",
                "sidecar_relative_path": "TEXT NOT NULL DEFAULT ''",
                "sidecar_size_bytes": "INTEGER NOT NULL DEFAULT 0",
                "sidecar_sha256": "TEXT NOT NULL DEFAULT ''",
                "clip_start_us": "INTEGER NOT NULL DEFAULT 0",
                "clip_end_us": "INTEGER NOT NULL DEFAULT 0",
            }
            for table in ("files", "unmatched_files"):
                columns = {row[1] for row in self.db.execute("PRAGMA table_info(%s)" % table)}
                for column, definition in added_columns.items():
                    if column not in columns:
                        self.db.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, column, definition))
            self.db.commit()

    def close(self):
        with self.lock:
            self.db.close()

    def _upsert(self, clip, state, verified_at, mtime_ns, generation="live", commit=True, scan_pass="", file_identity=(0, 0, 0), sidecar_evidence=("", 0, "")):
        updated_at = utc_now_precise()
        ctime_ns, inode, device = file_identity
        sidecar_path, sidecar_size, sidecar_sha = sidecar_evidence
        with self.lock:
            self.db.execute(
                """INSERT INTO files
                   (clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,file_mtime_ns,file_ctime_ns,file_inode,file_device,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256,clip_start_us,clip_end_us,seen_generation,scan_pass,client_updated_at,dirty)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                   ON CONFLICT(clip_id) DO UPDATE SET
                     recording_id=excluded.recording_id, relative_path=excluded.relative_path,
                     size_bytes=excluded.size_bytes, sha256=excluded.sha256, state=excluded.state,
                     verified_at=excluded.verified_at, file_mtime_ns=excluded.file_mtime_ns,
                     file_ctime_ns=excluded.file_ctime_ns,file_inode=excluded.file_inode,file_device=excluded.file_device,
                     sidecar_relative_path=excluded.sidecar_relative_path,sidecar_size_bytes=excluded.sidecar_size_bytes,sidecar_sha256=excluded.sidecar_sha256,
                     clip_start_us=excluded.clip_start_us,clip_end_us=excluded.clip_end_us,
                     seen_generation=excluded.seen_generation,scan_pass=excluded.scan_pass,
                     client_updated_at=excluded.client_updated_at,dirty=1""",
                (
                    int(clip["clip_id"]), int(clip["recording_id"]), str(clip["relative_path"]),
                    int(clip["size_bytes"]), str(clip["sha256"]).lower(), state,
                    verified_at, int(mtime_ns), int(ctime_ns), int(inode), int(device),
                    str(sidecar_path), int(sidecar_size), str(sidecar_sha).lower(),
                    optional_certification_timestamp_microseconds(clip.get("clip_start_at"), "clip_start_at"),
                    optional_certification_timestamp_microseconds(clip.get("clip_end_at"), "clip_end_at"),
                    generation, scan_pass, updated_at,
                ),
            )
            if commit:
                self.db.commit()

    def _commit_scan_batch(self, progress=None):
        with self.lock:
            if progress is not None:
                self.db.executemany(
                    "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (("scan_rows_visited", str(progress[0])), ("scan_rows_skipped", str(progress[1])),
                     ("scan_skip_reasons", json.dumps(progress[2], sort_keys=True, separators=(",", ":")))),
                )
            self.db.commit()

    def _publish_scan_batch(self, cfg, generation, started_at, progress, stop_event=None):
        try:
            self._commit_scan_batch(progress)
            self.sync_dirty(cfg, generation, started_at, stop_event=stop_event)
        except Exception as exc:
            raise InventoryProgressError("inventory progress persistence failed") from exc

    def record_verified(self, clip):
        path = self.cfg.output_dir / valid_relative_path(clip)
        stat = path.stat()
        if stat.st_size != int(clip["size_bytes"]):
            raise RuntimeError("inventory size changed after verification for clip %d" % int(clip["clip_id"]))
        self._upsert(clip, "present", utc_now_precise(), stat.st_mtime_ns, file_identity=(stat.st_ctime_ns, stat.st_ino, stat.st_dev))

    def _rows(self, where, params=(), limit=INVENTORY_SYNC_BATCH):
        with self.lock:
            return self.db.execute(
                """SELECT clip_id,recording_id,relative_path,size_bytes,sha256,state,
                          verified_at,file_mtime_ns,client_updated_at,file_ctime_ns,file_inode,file_device,
                          sidecar_relative_path,sidecar_size_bytes,sidecar_sha256
                   FROM files WHERE %s ORDER BY clip_id LIMIT ?""" % where,
                tuple(params) + (limit,),
            ).fetchall()

    @staticmethod
    def _reports(rows):
        reports = []
        for row in rows:
            report = {
                "clip_id": row[0], "recording_id": row[1], "relative_path": row[2],
                "size_bytes": row[3], "sha256": row[4], "state": row[5],
                "verified_at": row[6], "file_mtime_ns": row[7], "client_updated_at": row[8],
            }
            if row[9] > 0 and row[10] > 0 and row[11] > 0 and row[12] and row[13] > 0 and len(row[14]) == 64:
                report.update({
                    "file_ctime_ns": row[9], "file_inode": row[10], "file_device": row[11],
                    "sidecar_relative_path": row[12], "sidecar_size_bytes": row[13], "sidecar_sha256": row[14],
                })
            reports.append(report)
        return reports

    def _mark_clean(self, rows):
        with self.lock:
            self.db.executemany(
                "UPDATE files SET dirty=0 WHERE clip_id=? AND client_updated_at=?",
                [(row[0], row[8]) for row in rows],
            )
            self.db.commit()

    def sync_clip_ids(self, cfg, clip_ids, generation="live"):
        if not clip_ids:
            return
        placeholders = ",".join("?" for _ in clip_ids)
        rows = self._rows("dirty=1 AND clip_id IN (%s)" % placeholders, tuple(clip_ids), len(clip_ids))
        if not rows:
            return
        request_json(cfg, "POST", "/account/connections/inventory", body={
            "generation": generation, "complete": False, "files": self._reports(rows),
        })
        self._mark_clean(rows)

    def sync_dirty(self, cfg, generation="live", scan_started_at=None, stop_event=None):
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            rows = self._rows("dirty=1")
            if not rows:
                break
            request_json(cfg, "POST", "/account/connections/inventory", body={
                "generation": generation, "scan_started_at": scan_started_at,
                "complete": False, "files": self._reports(rows),
            })
            self._mark_clean(rows)
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            with self.lock:
                rows = self.db.execute(
                    """SELECT relative_path,size_bytes,sha256,state,file_mtime_ns,client_updated_at
                       FROM unmatched_files WHERE dirty=1 ORDER BY relative_path LIMIT ?""",
                    (INVENTORY_SYNC_BATCH,),
                ).fetchall()
            if not rows:
                return
            reports = [{
                "relative_path": row[0], "size_bytes": row[1], "sha256": row[2],
                "state": row[3], "file_mtime_ns": row[4], "client_updated_at": row[5],
            } for row in rows]
            request_json(cfg, "POST", "/account/connections/inventory", body={
                "generation": generation, "scan_started_at": scan_started_at,
                "complete": False, "files": [], "unmatched_files": reports,
            })
            with self.lock:
                self.db.executemany(
                    "UPDATE unmatched_files SET dirty=0 WHERE relative_path=? AND client_updated_at=?",
                    [(row[0], row[5]) for row in rows],
                )
                self.db.commit()

    def _meta_set(self, values):
        with self.lock:
            self.db.executemany(
                "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                list(values.items()),
            )
            self.db.commit()

    def _incomplete_scan(self):
        with self.lock:
            values = dict(self.db.execute("SELECT key,value FROM meta").fetchall())
        generation = values.get("generation", "")
        started_at = values.get("scan_started_at", "")
        if generation.startswith("scan-") and started_at and not values.get("scan_completed_at"):
            return generation, started_at
        return None

    def _linked_scan_row_is_current(self, clip, generation, scan_pass, path, sidecar_evidence):
        with self.lock:
            row = self.db.execute(
                """SELECT recording_id,relative_path,size_bytes,sha256,state,file_mtime_ns,seen_generation,
                          file_ctime_ns,file_inode,file_device,sidecar_relative_path,sidecar_size_bytes,sidecar_sha256,
                          clip_start_us,clip_end_us
                   FROM files WHERE clip_id=?""",
                (int(clip["clip_id"]),),
            ).fetchone()
            if row is None or row[6] != generation:
                return False
            if row[0] != int(clip["recording_id"]) or row[1] != str(clip["relative_path"]):
                return False
            if row[2] != int(clip["size_bytes"]) or row[3] != str(clip["sha256"]).lower():
                return False
            try:
                stat = path.stat()
            except FileNotFoundError:
                stat = None
            if stat is None:
                current = row[4] == "missing" and row[5] == 0
            else:
                current = (
                    row[4] in ("present", "mismatch") and row[5] == stat.st_mtime_ns
                    and row[7] != 0 and row[7] == stat.st_ctime_ns
                    and row[8] != 0 and row[8] == stat.st_ino
                    and row[9] != 0 and row[9] == stat.st_dev
                    and stat.st_size == int(clip["size_bytes"])
                    and tuple(row[10:13]) == tuple(sidecar_evidence)
                    and row[13] == optional_certification_timestamp_microseconds(clip.get("clip_start_at"), "clip_start_at")
                    and row[14] == optional_certification_timestamp_microseconds(clip.get("clip_end_at"), "clip_end_at")
                )
            if current:
                self.db.execute("UPDATE files SET scan_pass=? WHERE clip_id=?", (scan_pass, int(clip["clip_id"])))
            return current

    def _unmatched_scan_row_is_current(self, relative_path, generation, scan_pass, stat):
        with self.lock:
            row = self.db.execute(
                """SELECT size_bytes,state,file_mtime_ns,seen_generation,file_ctime_ns,file_inode,file_device
                   FROM unmatched_files WHERE relative_path=?""",
                (relative_path,),
            ).fetchone()
            current = (
                row is not None and row[3] == generation and row[1] == "present"
                and row[0] == stat.st_size and row[2] == stat.st_mtime_ns
                and row[4] != 0 and row[4] == stat.st_ctime_ns
                and row[5] != 0 and row[5] == stat.st_ino
                and row[6] != 0 and row[6] == stat.st_dev
            )
            if current:
                self.db.execute("UPDATE unmatched_files SET scan_pass=? WHERE relative_path=?", (scan_pass, relative_path))
            return current

    def _retire_unmatched_linked_path(self, relative_path, generation, scan_pass):
        with self.lock:
            self.db.execute(
                """UPDATE unmatched_files SET state='missing',seen_generation=?,scan_pass=?,client_updated_at=?,dirty=1
                   WHERE relative_path=? AND state<>'missing'""",
                (generation, scan_pass, utc_now_precise(), relative_path),
            )

    def summary(self):
        with self.lock:
            values = dict(self.db.execute("SELECT key,value FROM meta").fetchall())
        generation = values.get("generation", "")
        if not generation:
            return None
        scan_rows_skipped = int(values.get("scan_rows_skipped", 0))
        skip_reasons = None
        if "scan_skip_reasons" in values:
            try:
                parsed_reasons = json.loads(values["scan_skip_reasons"])
            except (TypeError, ValueError):
                parsed_reasons = None
            if isinstance(parsed_reasons, dict) and all(
                key in INVENTORY_SKIP_REASONS and type(value) is int and value > 0
                for key, value in parsed_reasons.items()
            ) and sum(parsed_reasons.values()) == scan_rows_skipped:
                skip_reasons = parsed_reasons
        summary = {
            "generation": generation,
            "scan_started_at": values.get("scan_started_at") or None,
            "scan_completed_at": values.get("scan_completed_at") or None,
            "scan_pass_started_at": values.get("scan_pass_started_at") or None,
            "scan_rows_visited": int(values.get("scan_rows_visited", 0)),
            "scan_rows_skipped": scan_rows_skipped,
            "clips": int(values.get("clips", 0)),
            "bytes": int(values.get("bytes", 0)),
            "mismatches": int(values.get("mismatches", 0)),
            "unmatched": int(values.get("unmatched", 0)),
            "digest": values.get("digest", ""),
        }
        if skip_reasons is not None:
            summary["scan_skip_reasons"] = skip_reasons
        return summary

    def _digest_and_counts(self):
        digest = hashlib.sha256()
        clips = total_bytes = mismatches = unmatched = 0
        with self.lock:
            rows = self.db.execute(
                "SELECT clip_id,relative_path,size_bytes,sha256,state FROM files ORDER BY clip_id"
            )
            for clip_id, path, size_bytes, sha256, state in rows:
                digest.update(("%d\0%s\0%d\0%s\0%s\n" % (clip_id, path, size_bytes, sha256, state)).encode("utf-8"))
                if state == "present":
                    clips += 1
                    total_bytes += size_bytes
                elif state == "mismatch":
                    mismatches += 1
            for path, size_bytes, sha256, state in self.db.execute(
                "SELECT relative_path,size_bytes,sha256,state FROM unmatched_files ORDER BY relative_path"
            ):
                digest.update(("unmatched\0%s\0%d\0%s\0%s\n" % (path, size_bytes, sha256, state)).encode("utf-8"))
                if state == "present":
                    unmatched += 1
        return digest.hexdigest(), clips, total_bytes, mismatches, unmatched

    def full_scan(self, cfg, stop_event):
        with self.activity_lock:
            return self._full_scan_locked(cfg, stop_event)

    def _full_scan_locked(self, cfg, stop_event):
        pass_started_at = utc_now_precise()
        resumed = self._incomplete_scan()
        if resumed is None:
            started_at = utc_now_precise()
            generation = "scan-%s-%s" % (time.strftime("%Y%m%dT%H%M%S", time.gmtime()), os.urandom(4).hex())
            self._meta_set({"generation": generation, "scan_started_at": started_at, "scan_completed_at": "", "digest": ""})
        else:
            generation, started_at = resumed
            log("INFO", "inventory resuming incomplete generation=%s started_at=%s" % (generation, started_at))
        scan_pass = os.urandom(8).hex()
        scanned = 0
        skipped = 0
        skip_reasons = {}
        self._meta_set({"scan_pass_started_at": pass_started_at, "scan_rows_visited": "0", "scan_rows_skipped": "0", "scan_skip_reasons": "{}"})

        known_paths = set()
        for sidecar in walk_raw_files(cfg.output_dir):
            if not sidecar.name.endswith(".stoarama.json"):
                continue
            if stop_event.is_set():
                self._commit_scan_batch((scanned, skipped, skip_reasons))
                return
            try:
                sidecar_bytes, _sidecar_stat = stable_regular_file_bytes(sidecar, 1024 * 1024)
                clip = json.loads(sidecar_bytes.decode("utf-8"))
                relative = valid_relative_path(clip)
                if str(relative) != str(clip.get("relative_path", "")):
                    raise ValueError("sidecar path is not canonical")
                path = cfg.output_dir / relative
                if sidecar != stitch_sidecar_path(path):
                    raise ValueError("sidecar location does not match its clip path")
                expected_size = int(clip["size_bytes"])
                expected_sha = str(clip["sha256"]).lower()
                if len(expected_sha) != 64 or any(ch not in "0123456789abcdef" for ch in expected_sha):
                    raise ValueError("sidecar checksum is invalid")
                sidecar_relative = str(sidecar.relative_to(cfg.output_dir))
                sidecar_evidence = (sidecar_relative, len(sidecar_bytes), hashlib.sha256(sidecar_bytes).hexdigest())
                known_paths.add(str(relative))
                self._retire_unmatched_linked_path(str(relative), generation, scan_pass)
                if self._linked_scan_row_is_current(clip, generation, scan_pass, path, sidecar_evidence):
                    scanned += 1
                    if scanned % INVENTORY_SYNC_BATCH == 0:
                        self._publish_scan_batch(
                            cfg, generation, started_at, (scanned, skipped, skip_reasons), stop_event,
                        )
                    continue
                try:
                    stat = path.stat()
                except FileNotFoundError:
                    stat = None
                if stat is not None:
                    try:
                        actual_size, actual_sha, stat = sha256_file_throttled_stable(path, cfg.inventory_hash_mbps, stop_event)
                    except FileNotFoundError:
                        state, verified_at, mtime_ns = "missing", None, 0
                        identity = (0, 0, 0)
                    except FileChangedDuringHash:
                        self._upsert(
                            clip, "mismatch", None, 0, generation, commit=False,
                            scan_pass=scan_pass, file_identity=(0, 0, 0),
                        )
                        raise
                    else:
                        state = "present" if actual_size == expected_size and actual_sha == expected_sha else "mismatch"
                        verified_at = utc_now_precise() if state == "present" else None
                        mtime_ns = stat.st_mtime_ns
                        identity = (stat.st_ctime_ns, stat.st_ino, stat.st_dev)
                else:
                    state, verified_at, mtime_ns = "missing", None, 0
                    identity = (0, 0, 0)
                self._upsert(clip, state, verified_at, mtime_ns, generation, commit=False, scan_pass=scan_pass, file_identity=identity, sidecar_evidence=sidecar_evidence)
                scanned += 1
                if scanned % INVENTORY_SYNC_BATCH == 0:
                    self._publish_scan_batch(
                        cfg, generation, started_at, (scanned, skipped, skip_reasons), stop_event,
                    )
                if cfg.inventory_scan_delay_ms:
                    stop_event.wait(cfg.inventory_scan_delay_ms / 1000.0)
            except InventoryScanStopped:
                self._commit_scan_batch((scanned, skipped, skip_reasons))
                return
            except sqlite3.Error as exc:
                raise InventoryProgressError("inventory state persistence failed") from exc
            except InventoryProgressError:
                raise
            except Exception as exc:
                skipped += 1
                reason = inventory_skip_reason(exc, sidecar=True)
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                log("WARN", f"inventory skipped reason={reason} count={skip_reasons[reason]}")
        for path in walk_raw_files(cfg.output_dir):
            if stop_event.is_set():
                self._commit_scan_batch((scanned, skipped, skip_reasons))
                return
            try:
                if (
                    not path.is_file()
                    or path.name.endswith(".stoarama.json")
                    or re.fullmatch(r".+\.part-\d+", path.name)
                    or re.fullmatch(r"\..+\.invalid-\d+-\d+", path.name)
                ):
                    continue
                relative = str(path.relative_to(cfg.output_dir))
                if relative in known_paths:
                    continue
                stat = path.stat()
                if self._unmatched_scan_row_is_current(relative, generation, scan_pass, stat):
                    scanned += 1
                    if scanned % INVENTORY_SYNC_BATCH == 0:
                        self._publish_scan_batch(
                            cfg, generation, started_at, (scanned, skipped, skip_reasons), stop_event,
                        )
                    continue
                size_bytes, sha256, stat = sha256_file_throttled_stable(path, cfg.inventory_hash_mbps, stop_event)
                updated_at = utc_now_precise()
                with self.lock:
                    self.db.execute(
                        """INSERT INTO unmatched_files(relative_path,size_bytes,sha256,state,file_mtime_ns,file_ctime_ns,file_inode,file_device,seen_generation,scan_pass,client_updated_at,dirty)
                           VALUES(?,?,?,'present',?,?,?,?,?,?,?,1)
                           ON CONFLICT(relative_path) DO UPDATE SET size_bytes=excluded.size_bytes,sha256=excluded.sha256,
                             state='present',file_mtime_ns=excluded.file_mtime_ns,file_ctime_ns=excluded.file_ctime_ns,
                             file_inode=excluded.file_inode,file_device=excluded.file_device,
                             seen_generation=excluded.seen_generation,scan_pass=excluded.scan_pass,
                             client_updated_at=excluded.client_updated_at,dirty=1""",
                        (relative, size_bytes, sha256, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_ino, stat.st_dev, generation, scan_pass, updated_at),
                    )
                scanned += 1
                if scanned % INVENTORY_SYNC_BATCH == 0:
                    self._publish_scan_batch(
                        cfg, generation, started_at, (scanned, skipped, skip_reasons), stop_event,
                    )
                if cfg.inventory_scan_delay_ms:
                    stop_event.wait(cfg.inventory_scan_delay_ms / 1000.0)
            except InventoryScanStopped:
                self._commit_scan_batch((scanned, skipped, skip_reasons))
                return
            except sqlite3.Error as exc:
                raise InventoryProgressError("inventory state persistence failed") from exc
            except InventoryProgressError:
                raise
            except Exception as exc:
                skipped += 1
                reason = inventory_skip_reason(exc, sidecar=False)
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                log("WARN", f"inventory skipped reason={reason} count={skip_reasons[reason]}")
        # Flush and publish every successfully observed row, but never promote a
        # partial generation. In particular, do not turn unseen prior rows into
        # "missing" when an unreadable/corrupt path was skipped.
        self._commit_scan_batch((scanned, skipped, skip_reasons))
        self.sync_dirty(cfg, generation, started_at, stop_event=stop_event)
        if stop_event.is_set():
            return
        if skipped:
            raise RuntimeError("inventory scan incomplete: %d path(s) could not be verified" % skipped)
        completed_at = utc_now_precise()
        with self.lock:
            self.db.execute(
                """UPDATE files SET state='missing',verified_at=NULL,seen_generation=?,scan_pass=?,dirty=1,client_updated_at=?
                   WHERE state<>'missing' AND scan_pass<>? AND (seen_generation=? OR client_updated_at<=?)""",
                (generation, scan_pass, completed_at, scan_pass, generation, pass_started_at),
            )
            self.db.execute(
                """UPDATE unmatched_files SET state='missing',seen_generation=?,scan_pass=?,dirty=1,client_updated_at=?
                   WHERE state<>'missing' AND scan_pass<>? AND (seen_generation=? OR client_updated_at<=?)""",
                (generation, scan_pass, completed_at, scan_pass, generation, pass_started_at),
            )
            self.db.commit()
        self.sync_dirty(cfg, generation, started_at, stop_event=stop_event)
        if stop_event.is_set():
            return
        digest, clips, total_bytes, mismatches, unmatched = self._digest_and_counts()
        if stop_event.is_set():
            return
        request_json(cfg, "POST", "/account/connections/inventory", body={
            "generation": generation, "scan_started_at": started_at,
            "scan_completed_at": completed_at, "digest": digest, "complete": True, "files": [],
        })
        self._meta_set({
            "generation": generation, "scan_started_at": started_at,
            "scan_completed_at": completed_at, "digest": digest,
            "clips": str(clips), "bytes": str(total_bytes), "mismatches": str(mismatches),
            "unmatched": str(unmatched),
        })
        log("INFO", "inventory scan complete generation=%s clips=%d bytes=%d mismatches=%d unmatched=%d" % (generation, clips, total_bytes, mismatches, unmatched))


def inventory_loop(cfg, inventory, stop_event):
    # Let the main drain establish connectivity first; inventory is deliberately
    # background work and never blocks incoming clip delivery.
    if stop_event.wait(10):
        return
    while not stop_event.is_set():
        try:
            with inventory.activity_lock:
                inventory.sync_dirty(cfg, stop_event=stop_event)
                if stop_event.is_set():
                    return
                inventory.full_scan(cfg, stop_event)
        except Exception as exc:
            log("WARN", "inventory scan/sync failed: %s" % exc)
        stop_event.wait(cfg.inventory_scan_interval_sec)


class Runtime:
    def __init__(self, cfg, inventory=None):
        progress = read_json(cfg.progress_file, {})
        if not progress and cfg.legacy_progress_file.exists():
            progress = read_json(cfg.legacy_progress_file, {})
        self.lock = threading.Lock()
        self.cursor_id = max(0, int(progress.get("after_id", 0)))
        self.clips_pulled = max(0, int(progress.get("clips_pulled", 0)))
        self.bytes_pulled = max(0, int(progress.get("bytes_pulled", 0)))
        self.phase = Phase.STARTING
        self.last_success_at = progress.get("last_success_at")
        self.last_error = ""
        self.last_error_at = None
        self.started_at = utc_now()
        self.boot_id = boot_id()
        self.previous_exit = self._previous_exit(cfg)
        self.heartbeat_succeeded = False
        self.list_succeeded = False
        self.stable_marked = False
        self.inventory = inventory
        self.joined_protocol_version = getattr(cfg, "joined_protocol_version", 0)
        # A new client explicitly clears any stale server-side capacity until
        # the independent probe proves that the configured NAS mount is live.
        self.storage = {"available": False}
        self.storage_observed_monotonic = time.monotonic()
        # Restarts always re-enter the blocked side of hysteresis. A fresh high
        # watermark stat must explicitly reopen admission, so a failed state
        # write can never resurrect an older durable "unblocked" decision.
        self.capacity_blocked = True
        self.capacity_reserved_bytes = 0
        self.batch = {
            "completed_at": None,
            "clips": 0,
            "bytes": 0,
            "duration_ms": 0,
            "workers": cfg.download_workers,
            "retries": 0,
            "failures": 0,
        }

    def _previous_exit(self, cfg):
        prior = read_json(cfg.runtime_file, {})
        status = prior.get("exit")
        if status == PreviousExit.CLEAN.value:
            return PreviousExit.CLEAN
        if status == PreviousExit.SELF_UPDATE.value:
            return PreviousExit.SELF_UPDATE
        if not prior:
            return PreviousExit.UNKNOWN
        if prior.get("boot_id") == self.boot_id:
            return PreviousExit.UNCLEAN_PROCESS
        return PreviousExit.UNCLEAN_REBOOT

    def set_phase(self, phase):
        with self.lock:
            self.phase = phase

    def set_error(self, message):
        with self.lock:
            self.last_error = str(message)[:1000]
            self.last_error_at = utc_now()
            self.phase = Phase.DEGRADED

    def add_successes(self, cfg, cursor_id, successes):
        with self.lock:
            self.cursor_id = max(self.cursor_id, cursor_id)
            self.clips_pulled += len(successes)
            self.bytes_pulled += sum(item[1] for item in successes)
            self.last_success_at = utc_now()
            self.last_error = ""
            self.last_error_at = None
            snapshot = self.progress_payload()
        if not cfg.dry_run:
            atomic_write(cfg.progress_file, json.dumps(snapshot, separators=(",", ":")).encode("utf-8"))

    def progress_payload(self):
        return {
            "after_id": self.cursor_id,
            "clips_pulled": self.clips_pulled,
            "bytes_pulled": self.bytes_pulled,
            "last_success_at": self.last_success_at,
        }

    def set_batch(self, clips, downloaded_bytes, duration_sec, retries, failures):
        duration_ms = max(1, round(duration_sec * 1000))
        with self.lock:
            self.batch = {
                "completed_at": utc_now(),
                "clips": clips,
                "bytes": downloaded_bytes,
                "duration_ms": duration_ms,
                "workers": self.batch["workers"],
                "retries": retries,
                "failures": failures,
            }

    def set_storage(self, storage):
        with self.lock:
            self.storage = storage.copy()
            self.storage_observed_monotonic = time.monotonic()

    def is_capacity_blocked(self):
        with self.lock:
            return self.capacity_blocked

    def reserve_storage(self, cfg, storage, expected_bytes=0):
        """Atomically admit expected bytes and persist hysteresis across restarts."""
        available = bool(storage.get("available"))
        free_bytes = int(storage.get("free_bytes", 0)) if available else 0
        expected_bytes = max(0, int(expected_bytes))
        with self.lock:
            was_blocked = self.capacity_blocked
            resume_at = cfg.min_free_bytes + CAPACITY_RESUME_HYSTERESIS_BYTES
            usable_free = free_bytes - self.capacity_reserved_bytes - expected_bytes
            self.capacity_blocked = (not available) or usable_free < (resume_at if was_blocked else cfg.min_free_bytes)
            changed = self.capacity_blocked != was_blocked
            if not self.capacity_blocked:
                self.capacity_reserved_bytes += expected_bytes
            if changed and self.capacity_blocked:
                try:
                    atomic_write(cfg.capacity_file, b'{"blocked":true}')
                except Exception:
                    self.capacity_blocked = True
                    raise
        return not self.capacity_blocked

    def release_storage_reservation(self, expected_bytes):
        with self.lock:
            self.capacity_reserved_bytes = max(0, self.capacity_reserved_bytes - max(0, int(expected_bytes)))

    def heartbeat_payload(self, outage):
        with self.lock:
            payload = {
                "cursor_id": self.cursor_id,
                "clips_pulled": self.clips_pulled,
                "bytes_pulled": self.bytes_pulled,
                "client_version": CLIENT_VERSION,
                "client_started_at": self.started_at,
                "client_boot_id": self.boot_id,
                "client_phase": self.phase.value,
                "client_previous_exit": self.previous_exit.value,
                "client_last_success_at": self.last_success_at,
                "client_last_error": self.last_error,
                "client_last_error_at": self.last_error_at,
                "last_batch": self.batch.copy(),
                "capacity_blocked": self.capacity_blocked,
                "joined_protocol_version": self.joined_protocol_version,
            }
            storage = self.storage.copy() if self.storage is not None else None
            storage_age = time.monotonic() - self.storage_observed_monotonic
        if outage:
            payload["last_outage"] = outage
        if storage is not None and storage_age <= STORAGE_TELEMETRY_MAX_AGE_SEC:
            payload["storage"] = storage
        elif self.storage is not None:
            payload["storage"] = {"available": False}
        if self.inventory is not None:
            inventory = self.inventory.summary()
            if inventory is not None:
                payload["inventory"] = inventory
        return payload


def mark_runtime(cfg, runtime, exit_status="running"):
    atomic_write(
        cfg.runtime_file,
        json.dumps({"boot_id": runtime.boot_id, "started_at": runtime.started_at, "exit": exit_status}).encode("utf-8"),
    )


def check_storage(cfg):
    for path in (cfg.output_dir, cfg.state_dir):
        if not path.exists() or not path.is_dir():
            raise RuntimeError("required storage directory is missing: %s" % path)
        if not os.path.ismount(str(path)):
            raise RuntimeError("required storage directory is not mounted: %s" % path)
        probe = path / (".stoarama-write-check-%d" % os.getpid())
        atomic_write(probe, b"ok")
        probe.unlink()
        fsync_dir(path)
    if cfg.output_dir.resolve() == cfg.state_dir.resolve():
        raise RuntimeError("clip and state mounts must be different")


def storage_status(cfg):
    """Read capacity from a stable mounted-directory descriptor, never its host fallback."""
    fd = None
    try:
        fd = os.open(cfg.output_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        opened = os.fstat(fd)
        if not os.path.ismount(str(cfg.output_dir)):
            return {"available": False}
        current = os.stat(cfg.output_dir)
        if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
            return {"available": False}
        usage = os.fstatvfs(fd)
        unit = usage.f_frsize or usage.f_bsize
        return {
            "available": True,
            "total_bytes": usage.f_blocks * unit,
            "free_bytes": usage.f_bavail * unit,
        }
    except OSError:
        return {"available": False}
    finally:
        if fd is not None:
            os.close(fd)


def storage_probe_loop(cfg, runtime, stop_event):
    """Keep filesystem syscalls off the liveness-critical heartbeat thread."""
    while not stop_event.is_set():
        try:
            runtime.set_storage(storage_status(cfg))
        except Exception as exc:
            runtime.set_storage({"available": False})
            log("WARN", "storage telemetry probe failed: %s" % exc)
        stop_event.wait(HEARTBEAT_INTERVAL_SEC)


def require_storage_capacity(cfg, runtime, expected_bytes=0):
    """Use a fresh mount-bound stat for every admission decision."""
    try:
        storage = storage_status(cfg)
    except Exception:
        storage = {"available": False}
    runtime.set_storage(storage)
    if not runtime.reserve_storage(cfg, storage, expected_bytes):
        if storage.get("available"):
            detail = "NAS free-space reserve reached"
        else:
            detail = "NAS free-space check unavailable"
        runtime.set_phase(Phase.BLOCKED)
        raise RuntimeError(detail)


def prepare_clip_with_capacity(cfg, runtime, clip):
    expected_bytes = clip.get("size_bytes")
    if isinstance(expected_bytes, bool) or not isinstance(expected_bytes, int) or expected_bytes <= 0:
        raise ValueError(f"clip {clip.get('clip_id', '?')} has invalid positive size_bytes")
    require_storage_capacity(cfg, runtime, expected_bytes)
    try:
        return process_clip(cfg, clip, False)
    finally:
        runtime.release_storage_reservation(expected_bytes)


def set_idle_unless_capacity_blocked(runtime):
    runtime.set_phase(Phase.BLOCKED if runtime.is_capacity_blocked() else Phase.IDLE)


def acquire_lock(cfg):
    cfg.state_dir.mkdir(parents=True, exist_ok=True)
    handle = open(cfg.lock_file, "a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError("another NAS pull client already holds %s" % cfg.lock_file) from exc
    return handle


def valid_relative_path(clip):
    raw = str(clip.get("relative_path", "")).strip().strip("/")
    if not raw:
        raise ValueError("clip %d has no relative_path" % int(clip["clip_id"]))
    parts = raw.split("/")
    if any(part in ("", ".", "..") or "\\" in part for part in parts):
        raise ValueError("clip %d has invalid relative_path" % int(clip["clip_id"]))
    if parts[0] == JOINED_ROOT:
        raise ValueError("clip %d uses reserved relative_path" % int(clip["clip_id"]))
    return Path(*parts)


def walk_raw_files(root):
    root = Path(root)

    def fail_walk(error):
        raise error

    for dirpath, dirnames, filenames in os.walk(root, topdown=True, onerror=fail_walk, followlinks=False):
        directory = Path(dirpath)
        if directory == root:
            dirnames[:] = [name for name in dirnames if name != JOINED_ROOT]
        for filename in filenames:
            yield directory / filename


def sha256_file(path):
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def sha256_file_throttled(path, megabytes_per_sec, stop_event):
    digest = hashlib.sha256()
    size = 0
    target_bytes_per_sec = megabytes_per_sec * 1024 * 1024
    with open(path, "rb") as source:
        while True:
            if stop_event.is_set():
                raise InventoryScanStopped("inventory scan stopped")
            started = time.monotonic()
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            delay = len(chunk) / target_bytes_per_sec - (time.monotonic() - started)
            if delay > 0 and stop_event.wait(delay):
                raise InventoryScanStopped("inventory scan stopped")
    return size, digest.hexdigest()


def file_identity(stat):
    return (stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_ino, stat.st_dev)


def sha256_file_throttled_stable(path, megabytes_per_sec, stop_event):
    before = path.stat()
    size, digest = sha256_file_throttled(path, megabytes_per_sec, stop_event)
    after = path.stat()
    if file_identity(before) != file_identity(after) or size != after.st_size:
        raise FileChangedDuringHash("inventory file changed while it was being hashed")
    return size, digest, after


def stable_regular_file_bytes(path, max_bytes):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = None
    try:
        descriptor = os.open(str(path), flags)
        before = os.fstat(descriptor)
        if not stat_module.S_ISREG(before.st_mode) or before.st_size < 0 or before.st_size > max_bytes:
            raise ValueError("sidecar is not a bounded regular file")
        chunks, total = [], 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise ValueError("sidecar exceeds its size bound")
        after = os.fstat(descriptor)
        if certification_identity(before) != certification_identity(after):
            raise FileChangedDuringHash("sidecar changed while it was read")
        return b"".join(chunks), after
    finally:
        if descriptor is not None:
            os.close(descriptor)


def verified_file(path, expected_bytes, expected_sha):
    if not path.exists():
        return False
    size, digest = sha256_file(path)
    if size != expected_bytes or digest != expected_sha:
        raise ExistingFileMismatch(f"existing file does not match API checksum: {path}")
    return True


def parse_certification_timestamp(value, field):
    raw = str(value or "").strip()
    if not raw:
        raise MediaCertificationError(f"{field} is required")
    try:
        parsed = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MediaCertificationError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise MediaCertificationError(f"{field} must include a timezone")
    return parsed.astimezone(datetime.timezone.utc)


def certification_timestamp_microseconds(value, field):
    parsed = parse_certification_timestamp(value, field)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    delta = parsed - epoch
    return delta.days * 86400000000 + delta.seconds * 1000000 + delta.microseconds


def optional_certification_timestamp_microseconds(value, field):
    return certification_timestamp_microseconds(value, field) if str(value or "").strip() else 0


def certification_identity(file_stat):
    return (
        file_stat.st_mode, file_stat.st_size, file_stat.st_mtime_ns,
        file_stat.st_ctime_ns, file_stat.st_ino, file_stat.st_dev,
    )


def confined_regular_file(root, relative_path):
    raw = str(relative_path or "")
    relative = Path(raw)
    if not raw or relative.is_absolute() or "\\" in raw or any(part in ("", ".", "..") for part in relative.parts):
        raise MediaCertificationError("invalid relative media path")
    resolved_root = root.resolve(strict=True)
    cursor = resolved_root
    for part in relative.parts:
        cursor = cursor / part
        try:
            current_stat = cursor.lstat()
        except OSError as exc:
            raise MediaCertificationError("media path is unavailable") from exc
        if stat_module.S_ISLNK(current_stat.st_mode):
            raise MediaCertificationError("media path contains a symlink")
    resolved = cursor.resolve(strict=True)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise MediaCertificationError("media path escapes the NAS root") from exc
    final_stat = resolved.stat()
    if not stat_module.S_ISREG(final_stat.st_mode):
        raise MediaCertificationError("media path is not a regular file")
    return resolved, final_stat


def hash_certification_file(root, relative_path, expected_size, expected_sha):
    path, path_before = confined_regular_file(root, relative_path)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(str(path), flags)
    except OSError as exc:
        raise MediaCertificationError("media file could not be opened safely") from exc
    digest = hashlib.sha256()
    actual_size = 0
    try:
        opened_before = os.fstat(descriptor)
        if certification_identity(opened_before) != certification_identity(path_before):
            raise MediaCertificationError("media identity changed before hashing")
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            actual_size += len(chunk)
            digest.update(chunk)
        opened_after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    path_after = path.stat()
    if (
        certification_identity(opened_before) != certification_identity(opened_after)
        or certification_identity(opened_after) != certification_identity(path_after)
        or actual_size != path_after.st_size
    ):
        raise MediaCertificationError("media identity changed while hashing")
    actual_sha = digest.hexdigest()
    if actual_size != expected_size or actual_sha != expected_sha:
        raise MediaCertificationError("media bytes do not match the sidecar")
    return path, path_after, actual_sha


def read_certification_sidecar(root, relative_path):
    sidecar_relative = str(relative_path) + ".stoarama.json"
    path, path_before = confined_regular_file(root, sidecar_relative)
    if path_before.st_size > 1024 * 1024:
        raise MediaCertificationError("stitch sidecar exceeds its size bound")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(str(path), flags)
    except OSError as exc:
        raise MediaCertificationError("stitch sidecar could not be opened safely") from exc
    try:
        opened_before = os.fstat(descriptor)
        body = b""
        while len(body) <= 1024 * 1024:
            chunk = os.read(descriptor, min(64 * 1024, 1024 * 1024 + 1 - len(body)))
            if not chunk:
                break
            body += chunk
        opened_after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    path_after = path.stat()
    if (
        len(body) > 1024 * 1024
        or certification_identity(path_before) != certification_identity(opened_before)
        or certification_identity(opened_before) != certification_identity(opened_after)
        or certification_identity(opened_after) != certification_identity(path_after)
    ):
        raise MediaCertificationError("stitch sidecar changed while it was read")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise MediaCertificationError("stitch sidecar is invalid") from exc
    if not isinstance(payload, dict):
        raise MediaCertificationError("stitch sidecar is invalid")
    return payload, len(body), hashlib.sha256(body).hexdigest()


def bounded_tool_output(command, timeout=MEDIA_CERTIFICATION_TOOL_TIMEOUT_SEC):
    with tempfile.TemporaryFile() as stdout_file:
        try:
            completed = subprocess.run(
                command, check=False, stdin=subprocess.DEVNULL,
                stdout=stdout_file, stderr=subprocess.DEVNULL,
                timeout=timeout, env={"PATH": os.environ.get("PATH", "")},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise MediaCertificationError("media verification tool unavailable or timed out") from exc
        if completed.returncode != 0:
            raise MediaCertificationError("media verification tool rejected the file")
        stdout_file.seek(0)
        output = stdout_file.read(1024 * 1024 + 1)
    if len(output) > 1024 * 1024:
        raise MediaCertificationError("media verification output exceeded its bound")
    return output


def media_tool_version(binary):
    raw = bounded_tool_output([binary, "-version"], timeout=30).decode("utf-8", "replace")
    return raw.splitlines()[0][:256] if raw else "unknown"


def probe_native_media(path, ffprobe_bin):
    raw = bounded_tool_output([
        ffprobe_bin, "-v", "error", "-protocol_whitelist", MEDIA_CERTIFICATION_PROTOCOLS,
        "-show_format", "-show_streams", "-show_data_hash", "sha256", "-of", "json", str(path),
    ])
    return parse_native_media_probe(raw)


def probe_native_media_cancellable(path, ffprobe_bin, cancel, pass_fds=()):
    raw = cancellable_tool_output([
        ffprobe_bin, "-v", "error", "-protocol_whitelist", MEDIA_CERTIFICATION_PROTOCOLS,
        "-enable_drefs", "0", "-use_absolute_path", "0",
        "-show_format", "-show_streams", "-show_data_hash", "sha256", "-of", "json", str(path),
    ], cancel, 600, pass_fds=pass_fds)
    return parse_native_media_probe(raw)


def parse_native_media_probe(raw):
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise MediaCertificationError("ffprobe returned invalid metadata") from exc
    streams = payload.get("streams")
    if not isinstance(streams, list):
        raise MediaCertificationError("ffprobe returned no stream list")
    format_info = payload.get("format") if isinstance(payload.get("format"), dict) else {}
    formats = {part.strip() for part in str(format_info.get("format_name", "")).split(",") if part.strip()}
    if not formats or not formats.issubset(MEDIA_CERTIFICATION_FORMATS):
        raise MediaCertificationError("media container is not approved for local certification")
    try:
        duration = float(format_info.get("duration"))
        start_time = float(format_info.get("start_time", "0"))
    except (TypeError, ValueError) as exc:
        raise MediaCertificationError("media timing metadata is invalid") from exc
    if not math.isfinite(duration) or duration <= 0 or not math.isfinite(start_time):
        raise MediaCertificationError("media timing metadata is invalid")
    signature_streams = []
    seen_indexes = set()
    video_count = audio_count = 0
    common_keys = (
        "index", "codec_type", "codec_name", "codec_long_name", "codec_tag_string", "profile", "level",
        "time_base", "start_pts", "duration_ts", "extradata_hash", "disposition",
    )
    video_keys = (
        "pix_fmt", "width", "height", "coded_width", "coded_height", "field_order",
        "sample_aspect_ratio", "display_aspect_ratio", "avg_frame_rate", "r_frame_rate",
        "color_range", "color_space", "color_transfer", "color_primaries", "chroma_location",
    )
    audio_keys = ("sample_fmt", "sample_rate", "channels", "channel_layout", "bits_per_sample")
    for stream in sorted(streams, key=lambda item: item.get("index", -1) if isinstance(item, dict) else -1):
        if not isinstance(stream, dict) or type(stream.get("index")) is not int:
            raise MediaCertificationError("media stream metadata is invalid")
        index = stream["index"]
        stream_type = stream.get("codec_type")
        if index in seen_indexes or stream_type not in ("video", "audio"):
            raise MediaCertificationError("media contains unsupported stream structure")
        if not str(stream.get("codec_name", "")).strip() or not str(stream.get("extradata_hash", "")).strip():
            raise MediaCertificationError("media stream configuration is incomplete")
        seen_indexes.add(index)
        video_count += stream_type == "video"
        audio_count += stream_type == "audio"
        keys = common_keys + (video_keys if stream_type == "video" else audio_keys)
        signature_streams.append({key: stream.get(key) for key in keys})
    if video_count != 1 or audio_count > 1:
        raise MediaCertificationError("media contains unsupported stream structure")
    return {
        "format_name": ",".join(sorted(formats)),
        "duration_seconds": duration,
        "container_start_time_seconds": start_time,
        "signature": {"format_names": sorted(formats), "streams": signature_streams},
        "stable_signature_v1": stable_native_signature_v1(streams, ",".join(sorted(formats))),
    }


def stable_native_signature_v1(probe_streams, format_name):
    """Decoder/layout identity only; never clip-local index/PTS/duration."""
    common = ("codec_type", "codec_name", "codec_tag_string", "profile", "level", "time_base", "extradata_hash")
    video = (
        "pix_fmt", "width", "height", "coded_width", "coded_height", "field_order",
        "sample_aspect_ratio", "display_aspect_ratio", "avg_frame_rate", "r_frame_rate",
        "color_range", "color_space", "color_transfer", "color_primaries", "chroma_location",
    )
    audio = ("sample_fmt", "sample_rate", "channels", "channel_layout", "bits_per_sample")
    canonical = []
    for stream in probe_streams:
        kind = stream.get("codec_type")
        if kind not in ("video", "audio"):
            raise MediaCertificationError("media contains unsupported stream structure")
        keys = common + (video if kind == "video" else audio)
        canonical.append({key: stream.get(key) for key in keys})
    canonical.sort(key=lambda item: item["codec_type"])
    return {"schema_version": 1, "format_name": format_name, "streams": canonical}


def cancellable_tool_output(command, cancel, timeout, stdout_limit=1024 * 1024, stderr_limit=64 * 1024, pass_fds=()):
    """Bounded child group: cancellation cannot strand ffmpeg descendants."""
    if stdout_limit < 0 or stderr_limit < 0:
        raise MediaCertificationError("media verification output bound is invalid")
    for descriptor in pass_fds:
        os.lseek(descriptor, 0, os.SEEK_SET)
    process = subprocess.Popen(
        command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True, env={"PATH": os.environ.get("PATH", "")}, pass_fds=tuple(pass_fds),
    )
    exceeded = threading.Event()
    stdout = bytearray()
    stderr = bytearray()

    def drain(stream, destination, limit):
        try:
            while True:
                chunk = stream.read(64 * 1024)
                if not chunk:
                    return
                remaining = max(0, limit + 1 - len(destination))
                if remaining:
                    destination.extend(chunk[:remaining])
                if len(destination) > limit or len(chunk) > remaining:
                    exceeded.set()
        finally:
            stream.close()

    readers = [
        threading.Thread(target=drain, args=(process.stdout, stdout, stdout_limit), name="native-stitch-stdout", daemon=True),
        threading.Thread(target=drain, args=(process.stderr, stderr, stderr_limit), name="native-stitch-stderr", daemon=True),
    ]
    for reader in readers:
        reader.start()

    def terminate_group():
        if process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)

    started = time.monotonic()
    failure = None
    try:
        while process.poll() is None:
            if exceeded.is_set():
                failure = MediaCertificationError("media verification output exceeded its bound")
                terminate_group()
                break
            if cancel.is_set():
                failure = InventoryScanStopped("native stitch verification yielded to delivery or shutdown")
                terminate_group()
                break
            if time.monotonic() - started > timeout:
                failure = MediaCertificationError("media verification tool timed out")
                terminate_group()
                break
            cancel.wait(.01)
        process.wait()
    finally:
        terminate_group()
        for reader in readers:
            reader.join(timeout=5)
        if any(reader.is_alive() for reader in readers):
            raise MediaCertificationError("media verification output drain did not stop")
    if exceeded.is_set() and failure is None:
        failure = MediaCertificationError("media verification output exceeded its bound")
    if failure is not None:
        raise failure
    if process.returncode:
        raise ToolProcessError(process.returncode, bytes(stderr))
    return bytes(stdout)


_DETERMINISTIC_MEDIA_DIAGNOSTICS = (
    b"invalid data found when processing input",
    b"error while decoding stream",
    b"corrupt decoded frame",
    b"invalid nal unit",
    b"moov atom not found",
    b"error reading header",
    b"invalid atom size",
)
_TRANSIENT_TOOL_DIAGNOSTICS = (
    b"cannot allocate memory", b"out of memory", b"input/output error",
    b"permission denied", b"no such file or directory", b"error while loading shared libraries",
    b"dyld:", b"killed",
)


def deterministic_media_rejection(exc):
    """Classify only affirmative corrupt-byte diagnostics, never infrastructure."""
    if not isinstance(exc, ToolProcessError) or exc.returncode < 0:
        return False
    diagnostic = bytes(exc.stderr).lower()
    if any(marker in diagnostic for marker in _TRANSIENT_TOOL_DIAGNOSTICS):
        return False
    return any(marker in diagnostic for marker in _DETERMINISTIC_MEDIA_DIAGNOSTICS)


def run_media_validation_command(command, cancel, timeout, deterministic_reason, pass_fds=()):
    """Require the same affirmative media rejection twice before terminal failure."""
    first_error = None
    try:
        cancellable_tool_output(command, cancel, timeout, pass_fds=pass_fds)
        return
    except ToolProcessError as first:
        if not deterministic_media_rejection(first):
            raise
        first_error = first
    try:
        cancellable_tool_output(command, cancel, timeout, pass_fds=pass_fds)
    except ToolProcessError as second:
        if deterministic_media_rejection(second):
            raise DeterministicMediaError(deterministic_reason) from second
        raise
    # A first affirmative corrupt-byte rejection followed by success is not a
    # clean decode. The verifier outcome is unstable, so publish no media axis
    # and let the fenced task retry rather than silently treating it as PASS.
    raise MediaCertificationError("media verification result was inconsistent") from first_error


def media_tool_version_cancellable(binary, cancel):
    raw = cancellable_tool_output([binary, "-version"], cancel, 30).decode("utf-8", "replace")
    return raw.splitlines()[0][:256] if raw else "unknown"


def timestamp_frame_duration(frame):
    """Return one unambiguous positive integer duration from FFprobe aliases."""
    values = []
    for field in ("duration", "pkt_duration"):
        if field not in frame:
            continue
        raw = frame[field]
        if raw is None:
            raise MediaCertificationError("timestamp contract frame duration is invalid")
        if isinstance(raw, bool):
            raise MediaCertificationError("timestamp contract frame duration is invalid")
        text = str(raw).strip()
        if not re.fullmatch(r"[+-]?\d+", text):
            raise MediaCertificationError("timestamp contract frame duration is invalid")
        value = int(text)
        if value <= 0:
            raise MediaCertificationError("timestamp contract frame duration is invalid")
        values.append(value)
    if not values:
        raise MediaCertificationError("timestamp contract frame duration is missing")
    if any(value != values[0] for value in values[1:]):
        raise MediaCertificationError("timestamp contract frame duration fields conflict")
    return values[0]


def recompute_timestamp_contract(path, ffprobe_bin, cancel, pass_fds=()):
    raw = cancellable_tool_output([
        ffprobe_bin, "-v", "error", "-protocol_whitelist", MEDIA_CERTIFICATION_PROTOCOLS,
        "-enable_drefs", "0", "-use_absolute_path", "0", "-show_frames", "-show_packets", "-show_streams", "-show_data", "-show_data_hash", "sha256",
        "-show_entries", "stream=index,codec_type,codec_name,codec_tag_string,profile,level,width,height,pix_fmt,time_base,extradata,sample_rate,channels,channel_layout:frame=stream_index,media_type,best_effort_timestamp,pkt_dts,pkt_duration,duration,nb_samples,key_frame,pict_type:packet=stream_index,pts,dts,duration,data_hash",
        "-of", "json", str(path),
    ], cancel, 30, stdout_limit=16 * 1024 * 1024, pass_fds=pass_fds)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise MediaCertificationError("timestamp contract probe returned invalid JSON") from exc
    streams = payload.get("streams")
    frames = payload.get("frames")
    packets = payload.get("packets")
    combined = payload.get("packets_and_frames")
    if isinstance(combined, list):
        frames = [item for item in combined if isinstance(item, dict) and item.get("type") == "frame"]
        packets = [item for item in combined if isinstance(item, dict) and item.get("type") == "packet"]
    if not isinstance(streams, list) or not isinstance(frames, list) or not isinstance(packets, list):
        raise MediaCertificationError("timestamp contract probe is incomplete")
    by_type = {"video": [], "audio": []}
    for stream in streams:
        if not isinstance(stream, dict) or stream.get("codec_type") not in by_type:
            raise MediaCertificationError("timestamp contract has unsupported streams")
        by_type[stream["codec_type"]].append(stream)
    if len(by_type["video"]) != 1 or len(by_type["audio"]) > 1:
        raise MediaCertificationError("timestamp contract has unsupported stream cardinality")
    contract = {"version": 1, "mode": "muxed_source_copy", "audio_selection": "first_optional", "tracks": []}
    timelines = {}
    packet_by_stream_pts = {}
    for packet in packets:
        if not isinstance(packet, dict):
            raise MediaCertificationError("timestamp contract packet evidence is invalid")
        try:
            packet_stream = int(packet["stream_index"])
            packet_pts = int(packet["pts"])
            packet_dts = int(packet["dts"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MediaCertificationError("timestamp contract packet timing is missing") from exc
        packet_hash = str(packet.get("data_hash", ""))
        if packet_hash.startswith("SHA256:"):
            packet_hash = packet_hash[7:].lower()
        if len(packet_hash) != 64 or any(ch not in "0123456789abcdef" for ch in packet_hash):
            raise MediaCertificationError("timestamp contract packet hash is invalid")
        key = (packet_stream, packet_pts)
        if key in packet_by_stream_pts:
            raise MediaCertificationError("timestamp contract packet presentation identity is ambiguous")
        packet_by_stream_pts[key] = (packet_dts, packet_hash)
    for media_type in ("video", "audio"):
        if not by_type[media_type]:
            continue
        stream = by_type[media_type][0]
        try:
            time_num, time_den = [int(part) for part in str(stream.get("time_base", "")).split("/", 1)]
            stream_index = int(stream["index"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MediaCertificationError("timestamp contract time base is invalid") from exc
        if time_num <= 0 or time_den <= 0 or stream_index < 0:
            raise MediaCertificationError("timestamp contract time base is invalid")
        signature_parts = [media_type, stream.get("codec_name", ""), stream.get("codec_tag_string", ""), stream.get("profile", ""), stream.get("pix_fmt", ""), stream.get("level", 0), stream.get("width", 0), stream.get("height", 0), stream.get("channels", 0), stream.get("channel_layout", ""), stream.get("extradata", ""), stream.get("sample_rate", "")]
        signature = "".join("%d:%s|" % (len(str(part).encode("utf-8")), str(part)) for part in signature_parts)
        track = {"stream_index": stream_index, "media_type": media_type, "time_base_num": time_num, "time_base_den": time_den, "first_timestamp": 0, "last_timestamp": 0, "last_duration": 0, "unit_count": 0, "codec_signature_sha256": hashlib.sha256(signature.encode()).hexdigest()}
        if media_type == "audio":
            try: track["sample_rate"] = int(stream.get("sample_rate"))
            except (TypeError, ValueError) as exc: raise MediaCertificationError("timestamp contract audio sample rate is invalid") from exc
            if track["sample_rate"] <= 0: raise MediaCertificationError("timestamp contract audio sample rate is invalid")
        previous_pts = previous_duration = None
        duplicates = nonmonotonic = discontinuities = 0
        presentation_frames = []
        for frame in frames:
            if not isinstance(frame, dict) or frame.get("stream_index") != stream_index or frame.get("media_type") != media_type:
                continue
            try:
                pts = int(frame["best_effort_timestamp"])
                duration = timestamp_frame_duration(frame)
            except (KeyError, TypeError, ValueError) as exc:
                raise MediaCertificationError("timestamp contract frame timing is missing") from exc
            if previous_pts is not None:
                duplicates += pts == previous_pts
                nonmonotonic += pts < previous_pts
                discontinuities += pts != previous_pts + previous_duration
            if track["unit_count"] == 0: track["first_timestamp"] = pts
            track["last_timestamp"] = pts;track["last_duration"] = duration;track["unit_count"] += 1
            previous_pts, previous_duration = pts, duration
            if media_type == "video":
                packet = packet_by_stream_pts.get((stream_index, pts))
                if packet is None:
                    raise MediaCertificationError("video frame lacks exact source packet identity")
                presentation_frames.append({
                    "best_effort_timestamp": pts,
                    "duration_timestamp": duration,
                    "time_base_numerator": time_num,
                    "time_base_denominator": time_den,
                    "packet_dts": packet[0],
                    "key_frame": bool(frame.get("key_frame", 0)),
                    "picture_type": str(frame.get("pict_type", "")),
                    "packet_sha256": packet[1],
                })
            if media_type == "audio":
                try: sample_count = int(frame["nb_samples"])
                except (KeyError, TypeError, ValueError) as exc: raise MediaCertificationError("timestamp contract audio sample count is missing") from exc
                if sample_count <= 0: raise MediaCertificationError("timestamp contract audio sample count is missing")
                track["last_sample_count"] = sample_count
        if track["unit_count"] == 0 or nonmonotonic:
            raise MediaCertificationError("timestamp contract presentation order is invalid")
        contract["tracks"].append(track)
        timelines[media_type] = {"first_timestamp": track["first_timestamp"], "last_timestamp": track["last_timestamp"], "last_duration_timestamp": track["last_duration"], "time_base_numerator": time_num, "time_base_denominator": time_den, "duplicate_timestamp_count": duplicates, "non_monotonic_step_count": nonmonotonic, "discontinuous_step_count": discontinuities}
        if media_type == "video":
            timelines[media_type]["frame_count"] = track["unit_count"]
            timelines["_video_frames"] = presentation_frames
        elif time_num == 1 and time_den == track["sample_rate"]:
            timelines[media_type] = {"sample_rate": track["sample_rate"], "first_sample": track["first_timestamp"], "end_sample": track["last_timestamp"] + track["last_sample_count"], "sample_count": track["last_timestamp"] + track["last_sample_count"] - track["first_timestamp"], "duplicate_timestamp_count": duplicates, "non_monotonic_step_count": nonmonotonic, "discontinuous_step_count": discontinuities}
        else:
            timelines[media_type] = None
    return contract, timelines


def decoded_video_frame_hashes(path, ffmpeg_bin, cancel, pass_fds=()):
    """Decode every video frame and return presentation-order SHA-256 facts.

    The output is bounded and retains source timestamps with -copyts. Hashes
    are corroborating byte facts only; continuity is decided by the attested
    rational timestamps, never by whether two static frames hash alike.
    """
    raw = cancellable_tool_output([
        ffmpeg_bin, "-v", "error", "-copyts", "-protocol_whitelist", MEDIA_CERTIFICATION_PROTOCOLS,
        "-enable_drefs", "0", "-use_absolute_path", "0", "-i", str(path),
        "-map", "0:v:0", "-an", "-fps_mode", "passthrough",
        "-f", "framemd5", "-hash", "sha256", "-",
    ], cancel, 600, stdout_limit=16 * 1024 * 1024, pass_fds=pass_fds)
    frames = []
    time_base = None
    for raw_line in raw.decode("ascii", "strict").splitlines():
        line = raw_line.strip()
        if line.startswith("#tb 0:"):
            try:
                numerator, denominator = [int(value) for value in line.split(":", 1)[1].strip().split("/", 1)]
            except (TypeError, ValueError) as exc:
                raise MediaCertificationError("decoded frame fingerprint time base is invalid") from exc
            if numerator <= 0 or denominator <= 0:
                raise MediaCertificationError("decoded frame fingerprint time base is invalid")
            time_base = (numerator, denominator)
            continue
        if not line or line.startswith("#"):
            continue
        fields = [part.strip() for part in line.split(",")]
        if len(fields) != 6:
            raise MediaCertificationError("decoded frame fingerprint output is invalid")
        try:
            stream, dts, pts, duration, size = [int(value) for value in fields[:5]]
        except ValueError as exc:
            raise MediaCertificationError("decoded frame fingerprint timing is invalid") from exc
        digest = fields[5].lower()
        if stream != 0 or duration <= 0 or size <= 0 or len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise MediaCertificationError("decoded frame fingerprint is invalid")
        frames.append({"dts": dts, "pts": pts, "duration": duration, "decoded_sha256": digest})
        if len(frames) > 100000:
            raise MediaCertificationError("decoded frame count exceeded its bound")
    if not frames:
        raise MediaCertificationError("decoded frame fingerprint output is empty")
    if time_base is None:
        raise MediaCertificationError("decoded frame fingerprint time base is missing")
    return time_base, frames


def native_stitch_video_edge_frames(path, ffmpeg_bin, source_frames, cancel, pass_fds=()):
    _, decoded = decoded_video_frame_hashes(path, ffmpeg_bin, cancel, pass_fds=pass_fds)
    if len(decoded) != len(source_frames):
        raise MediaCertificationError("decoded frame count differs from timestamp contract")
    facts = []
    for index, (source, frame) in enumerate(zip(source_frames, decoded)):
        # ffprobe -show_frames is the decoder's presentation-order timestamp
        # authority frozen by the capture contract. framemd5 may quantize its
        # output time base (notably 30000/1001 with MP4 edit lists), so bind
        # decoded pixels by presentation ordinal and exact total count. Its
        # timestamps are only used to prove framemd5 itself stayed ordered.
        if index > 0 and frame["pts"] <= decoded[index - 1]["pts"]:
            raise MediaCertificationError("decoded fingerprint order is invalid")
        facts.append({**source, "decoded_sha256": frame["decoded_sha256"]})
    return {"first": facts[:8], "last": facts[-8:]}


def strict_decode_media(path, ffmpeg_bin):
    bounded_tool_output([
        ffmpeg_bin, "-v", "error", "-xerror", "-err_detect", "explode",
        "-protocol_whitelist", MEDIA_CERTIFICATION_PROTOCOLS, "-i", str(path),
        "-map", "0:v:0", "-map", "0:a?", "-f", "null", "-",
    ])


def concat_manifest_line(path):
    raw = str(path)
    if "\n" in raw or "\r" in raw:
        raise MediaCertificationError("media path contains a newline")
    # The concat demuxer must pass MOV-specific dref controls to each nested
    # MP4 input. Supplying them on the concat input itself is rejected by
    # FFmpeg 8, while omitting them would permit hostile external references.
    return "file '%s'\noption enable_drefs 0\noption use_absolute_path 0\n" % raw.replace("'", "'\\''")


def validate_native_run(paths, ffmpeg_bin, temp_root):
    if len(paths) < 2:
        return "single_clip"
    manifest = temp_root / "concat.txt"
    output = temp_root / "stitched.mp4"
    manifest.write_text("".join(concat_manifest_line(path) for path in paths), encoding="utf-8")
    bounded_tool_output([
        ffmpeg_bin, "-v", "error", "-xerror", "-err_detect", "explode",
        "-protocol_whitelist", MEDIA_CERTIFICATION_PROTOCOLS,
        "-f", "concat", "-safe", "0", "-i", str(manifest),
        "-map", "0:v:0", "-map", "0:a?", "-c", "copy",
        "-avoid_negative_ts", "make_zero", "-movflags", "+faststart", "-y", str(output),
    ])
    strict_decode_media(output, ffmpeg_bin)
    try:
        output.unlink()
    except FileNotFoundError:
        pass
    return "lossless_concat_and_decode_passed"


def check_native_stitch_delivery(cfg, runtime, cancel):
    if cancel.is_set():
        return True
    page = request_json(cfg, "GET", "/account/clips?after_id=%d&limit=1" % runtime.cursor_id)
    waiting = bool(page.get("clips"))
    if waiting:
        cancel.set()
    return waiting


def hash_certification_file_cancellable(root, relative_path, expected_size, expected_sha, cancel):
    path, path_before = confined_regular_file(root, relative_path)
    parent_before = path.parent.stat()
    descriptor = os.open(str(path), os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0))
    digest = hashlib.sha256(); actual_size = 0
    try:
        opened_before = os.fstat(descriptor)
        if certification_identity(opened_before) != certification_identity(path_before):
            raise MediaCertificationError("media identity changed before hashing")
        while True:
            if cancel.is_set(): raise InventoryScanStopped("certification yielded to delivery")
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk: break
            digest.update(chunk); actual_size += len(chunk)
        opened_after = os.fstat(descriptor)
    except BaseException:
        os.close(descriptor)
        raise
    path_after = path.stat()
    parent_after = path.parent.stat()
    if certification_identity(opened_before) != certification_identity(opened_after) or certification_identity(opened_after) != certification_identity(path_after):
        os.close(descriptor)
        raise MediaCertificationError("media identity changed while hashing")
    if certification_identity(parent_before) != certification_identity(parent_after):
        os.close(descriptor)
        raise MediaCertificationError("media directory changed while hashing")
    if actual_size != expected_size or digest.hexdigest() != expected_sha:
        os.close(descriptor)
        raise MediaCertificationError("media bytes do not match frozen manifest")
    os.lseek(descriptor, 0, os.SEEK_SET)
    return path, path_after, parent_after, descriptor, Path("/dev/fd/%d" % descriptor)


def verify_certification_source_identities(paths, identities, phase):
    """Fail transiently if exact frozen source bytes changed during a verifier phase."""
    for path, identity in zip(paths, identities):
        try:
            current = certification_identity(path.stat())
            current_parent = certification_identity(path.parent.stat())
        except OSError as exc:
            raise MediaCertificationError("media identity changed during %s" % phase) from exc
        if (current, current_parent) != identity:
            raise MediaCertificationError("media identity changed during %s" % phase)


def snapshot_certification_run(paths, identities, clips, destination, cancel):
    """Copy exact frozen bytes into one task-owned run using O(1) source fds."""
    if len(paths) != len(identities) or len(paths) != len(clips):
        raise MediaCertificationError("run snapshot inputs have different lengths")
    snapshots = []
    for ordinal, (path, identity, clip) in enumerate(zip(paths, identities, clips), start=1):
        if cancel.is_set():
            raise InventoryScanStopped("certification yielded while snapshotting a run")
        source = os.open(str(path), os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0))
        snapshot = destination / ("clip-%04d.mp4" % ordinal)
        target = os.open(str(snapshot), os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0), 0o400)
        digest = hashlib.sha256()
        copied = 0
        try:
            if (certification_identity(os.fstat(source)), certification_identity(path.parent.stat())) != identity:
                raise MediaCertificationError("media identity changed before run snapshot")
            expected_size = int(clip["size_bytes"])
            while copied < expected_size:
                if cancel.is_set():
                    raise InventoryScanStopped("certification yielded while snapshotting a run")
                chunk = os.read(source, min(1024 * 1024, expected_size - copied))
                if not chunk:
                    raise MediaCertificationError("run snapshot source ended before frozen size")
                digest.update(chunk)
                copied += len(chunk)
                view = memoryview(chunk)
                while view:
                    written = os.write(target, view)
                    if written <= 0:
                        raise MediaCertificationError("run snapshot write made no progress")
                    view = view[written:]
            if os.read(source, 1):
                raise MediaCertificationError("run snapshot source exceeds frozen size")
            os.fsync(target)
            if (certification_identity(os.fstat(source)), certification_identity(path.parent.stat())) != identity:
                raise MediaCertificationError("media identity changed during run snapshot")
        finally:
            os.close(target)
            os.close(source)
        if copied != expected_size or digest.hexdigest() != clip["sha256"]:
            raise MediaCertificationError("run snapshot differs from frozen bytes")
        snapshots.append(snapshot)
    return snapshots


def strict_decode_media_cancellable(path, ffmpeg_bin, cancel, pass_fds=()):
    command = [ffmpeg_bin, "-v", "error", "-xerror", "-err_detect", "explode", "-protocol_whitelist", MEDIA_CERTIFICATION_PROTOCOLS, "-enable_drefs", "0", "-use_absolute_path", "0", "-i", str(path), "-map", "0:v:0", "-map", "0:a?", "-f", "null", "-"]
    run_media_validation_command(command, cancel, 600, "clip_decode_failed", pass_fds=pass_fds)


def validate_native_run_cancellable(paths, ffmpeg_bin, cancel, temp_root, pass_fds=()):
    if len(paths) == 1: return "single_clip_decode_only"
    manifest = temp_root / "concat.txt"; output = temp_root / "stitched.mp4"
    manifest.write_text("".join(concat_manifest_line(path) for path in paths), encoding="utf-8")
    concat = [ffmpeg_bin,"-v","error","-xerror","-err_detect","explode","-protocol_whitelist",MEDIA_CERTIFICATION_PROTOCOLS,"-f","concat","-safe","0","-i",str(manifest),"-map","0:v:0","-map","0:a?","-c","copy","-y",str(output)]
    run_media_validation_command(concat, cancel, 600, "run_concat_failed", pass_fds=pass_fds)
    try:
        strict_decode_media_cancellable(output,ffmpeg_bin,cancel)
    except DeterministicMediaError as exc:
        raise DeterministicMediaError("run_concat_failed") from exc
    return "lossless_concat_decode_passed"


def native_stitch_timeline(clips, window_start, window_end):
    cursor=window_start; covered=overlaps=0.0; gaps=[]; internal_gaps=[]; overlap_count=0
    leading=max(0.0,(clips[0]["clip_start_at"]-window_start).total_seconds())
    for clip in clips:
        left=max(window_start,clip["clip_start_at"]); right=min(window_end,clip["clip_end_at"])
        if right<=left: continue
        if left>cursor:
            gap=(left-cursor).total_seconds();gaps.append(gap)
            if cursor>window_start:internal_gaps.append(gap)
        elif left<cursor:
            overlap_end=min(right,cursor)
            if overlap_end>left: overlap_count+=1;overlaps+=(overlap_end-left).total_seconds()
        if right>cursor: covered+=(right-max(left,cursor)).total_seconds();cursor=right
    trailing=max(0.0,(window_end-cursor).total_seconds())
    if trailing>0:gaps.append(trailing)
    expected=(window_end-window_start).total_seconds()
    return {"expected_seconds":expected,"covered_seconds":covered,"coverage_pct":covered/expected*100,"leading_gap_seconds":leading,"largest_internal_gap_seconds":max(internal_gaps,default=0),"trailing_gap_seconds":trailing,"largest_gap_seconds":max(gaps,default=0),"gap_count":len(gaps),"gap_over_30s_count":sum(g>30 for g in gaps),"gap_over_5m_count":sum(g>300 for g in gaps),"overlap_count":overlap_count,"overlap_seconds":overlaps}


def native_stitch_full_envelope(timeline):
    return (timeline["leading_gap_seconds"] == 0 and timeline["largest_internal_gap_seconds"] == 0 and
            timeline["trailing_gap_seconds"] == 0 and timeline["gap_count"] == 0 and
            timeline["overlap_count"] == 0 and timeline["overlap_seconds"] == 0 and
            timeline["covered_seconds"] == timeline["expected_seconds"])


def native_stitch_clip_axis_continuity(clips):
    # v1 freezes rational endpoints but not capture-authoritative packet-edge
    # identities. It cannot earn frame-perfect PASS; its hashes remain local
    # verifier observations until a separate capture contract version exists.
    exact_video = False
    exact_audio = all(
        not c["audio_present"] or (
            c.get("timestamp_contract_status") == "per_clip_probe_complete" and c.get("audio_timeline") and
            c["audio_timeline"]["duplicate_timestamp_count"] == 0 and
            c["audio_timeline"]["non_monotonic_step_count"] == 0 and
            c["audio_timeline"]["discontinuous_step_count"] == 0
        ) for c in clips
    )
    return exact_video, exact_audio


def native_stitch_largest_possible_run(raw_clips):
    """Fail-closed temp bound before native signatures are probed."""
    if not isinstance(raw_clips, list) or not raw_clips or len(raw_clips) > NATIVE_STITCH_MAX_CLIPS:
        raise MediaCertificationError("invalid frozen task manifest")
    largest = current = 0
    previous = None
    for frozen in raw_clips:
        try:
            size = int(frozen["size_bytes"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MediaCertificationError("invalid frozen task size") from exc
        if size <= 0:
            raise MediaCertificationError("invalid frozen task size")
        split = previous is not None and (
            frozen.get("capture_generation") != previous.get("capture_generation") or
            frozen.get("capture_attempt_id", "") != previous.get("capture_attempt_id", "") or
            frozen.get("timestamp_contract_version", "") != previous.get("timestamp_contract_version", "") or
            frozen.get("clip_start_at") != previous.get("clip_end_at")
        )
        if split:
            current = 0
        current += size
        largest = max(largest, current)
        previous = frozen
    if largest > NATIVE_STITCH_MAX_RUN_BYTES:
        raise MediaCertificationError("native run byte bound exceeded")
    return largest


def maybe_run_native_stitch(cfg, runtime, inventory, stop_event, ffmpeg_bin="ffmpeg", ffprobe_bin="ffprobe"):
    if not getattr(cfg, "native_stitch_enabled", False): return False
    if not inventory.activity_lock.acquire(blocking=False): return False
    cancel=threading.Event(); watcher_stop=threading.Event(); started=datetime.datetime.now(datetime.timezone.utc)
    absolute_deadline=time.monotonic()+NATIVE_STITCH_ATTEMPT_SEC
    try:
        if check_native_stitch_delivery(cfg,runtime,cancel): return False
        claimed=request_json(cfg,"POST","/account/connections/stitch-certifications/claim",body={})
        task=claimed.get("task")
        if not isinstance(task,dict): return False
        try:
            lease_expires=parse_certification_timestamp(task.get("lease_expires_at"),"lease_expires_at")
            lease_remaining=(lease_expires-datetime.datetime.now(datetime.timezone.utc)).total_seconds()
        except MediaCertificationError:
            lease_remaining=0
        completion_deadline=time.monotonic()+max(0,lease_remaining-5)
        absolute_deadline=min(absolute_deadline,completion_deadline-NATIVE_STITCH_COMPLETION_MARGIN_SEC)
        task["_completion_deadline_monotonic"]=completion_deadline
        if absolute_deadline<=time.monotonic():
            cancel.set()
        runtime.set_phase(Phase.CERTIFYING)
        def watch_delivery():
            while not watcher_stop.wait(NATIVE_STITCH_DELIVERY_POLL_SEC):
                if stop_event.is_set() or time.monotonic()>=absolute_deadline:
                    cancel.set(); return
                try:
                    if check_native_stitch_delivery(cfg,runtime,cancel): return
                except Exception:
                    # Loss of the delivery check is unsafe: certification yields.
                    cancel.set(); return
        watcher=threading.Thread(target=watch_delivery,name="native-stitch-delivery-watch",daemon=True)
        watcher.start()
        return run_native_stitch_task(cfg,runtime,inventory,stop_event,cancel,task,started,absolute_deadline,ffmpeg_bin,ffprobe_bin)
    finally:
        watcher_stop.set()
        watcher=locals().get("watcher")
        if watcher is not None: watcher.join(timeout=1)
        set_idle_unless_capacity_blocked(runtime)
        inventory.activity_lock.release()


def run_native_stitch_task(cfg,runtime,inventory,stop_event,cancel,task,started,absolute_deadline,ffmpeg_bin,ffprobe_bin):
    try:
        return _run_native_stitch_task(cfg,runtime,inventory,stop_event,cancel,task,started,absolute_deadline,ffmpeg_bin,ffprobe_bin)
    except Exception as exc:
        # A claim is never abandoned merely because local setup failed. Return
        # bounded retryable UNKNOWN under the exact token; stale tokens remain
        # fenced by the server.
        completed=datetime.datetime.now(datetime.timezone.utc)
        raw_clips=task.get("clips") if isinstance(task.get("clips"),list) else []
        try:
            window_start=parse_certification_timestamp(task.get("window_start_at"),"window_start_at")
            window_end=parse_certification_timestamp(task.get("window_end_at"),"window_end_at")
            timeline=native_stitch_timeline([
                {**c,"clip_start_at":parse_certification_timestamp(c.get("clip_start_at"),"clip_start_at"),"clip_end_at":parse_certification_timestamp(c.get("clip_end_at"),"clip_end_at")}
                for c in raw_clips
            ],window_start,window_end) if raw_clips else {"expected_seconds":(window_end-window_start).total_seconds(),"covered_seconds":0,"coverage_pct":0,"leading_gap_seconds":(window_end-window_start).total_seconds(),"largest_internal_gap_seconds":0,"trailing_gap_seconds":(window_end-window_start).total_seconds(),"largest_gap_seconds":(window_end-window_start).total_seconds(),"gap_count":1,"gap_over_30s_count":1,"gap_over_5m_count":1,"overlap_count":0,"overlap_seconds":0}
        except Exception:
            raise exc
        reason="attempt_deadline" if time.monotonic()>=absolute_deadline else ("delivery_preempted" if cancel.is_set() else "post_claim_setup_failed")
        report={"schema_version":1,"policy_version":task["policy_version"],"task_id":task["task_id"],"recording_id":task["recording_id"],"recording_job_id":task["recording_job_id"],"window_start_at":task["window_start_at"],"window_end_at":task["window_end_at"],"clip_manifest_sha256":task["clip_manifest_sha256"],"inventory_generation":task["inventory_generation"],"inventory_digest":task["inventory_digest"],"inventory_completed_at":task["inventory_completed_at"],"status":"unknown","nas_byte_decode_status":"unknown","native_run_concat_status":"unknown","within_run_frame_adjacency_status":"unknown","within_run_audio_sample_continuity_status":"unknown","window_continuity_status":"unknown","timeline":timeline,"clips":[],"native_runs":[],"seams":[],"audio_seams":[],"reason_codes":[reason],"client_version":CLIENT_VERSION,"ffmpeg_version":"unavailable","ffprobe_version":"unavailable","started_at":started.isoformat().replace("+00:00","Z"),"completed_at":completed.isoformat().replace("+00:00","Z"),"source_media_modified":False,"reencoded":False,"persistent_output_created":False}
        submit_native_stitch_completion(cfg,task,report)
        return True


def submit_native_stitch_completion(cfg, task, report):
    """Retry the exact immutable report only inside the server lease margin."""
    deadline=float(task.get("_completion_deadline_monotonic",time.monotonic()+HTTP_TIMEOUT_SEC))
    body={"claim_token":task["claim_token"],"report":report}
    last_error=None
    while time.monotonic()<deadline:
        timeout=max(1,min(HTTP_TIMEOUT_SEC,int(deadline-time.monotonic())))
        try:
            return request_json(cfg,"POST","/account/connections/stitch-certifications/complete",body=body,timeout=timeout)
        except Exception as exc:
            if not transient_error(exc):
                raise
            last_error=exc
            if deadline-time.monotonic()<=1:
                break
            time.sleep(min(1,deadline-time.monotonic()))
    raise MediaCertificationError("stitch completion could not be acknowledged inside its lease") from last_error


def _run_native_stitch_task(cfg,runtime,inventory,stop_event,cancel,task,started,absolute_deadline,ffmpeg_bin,ffprobe_bin):
    raw_clips=task.get("clips")
    if not isinstance(raw_clips,list) or not raw_clips or len(raw_clips)>NATIVE_STITCH_MAX_CLIPS: raise MediaCertificationError("invalid frozen task manifest")
    if native_stitch_manifest_hash(raw_clips)!=str(task.get("clip_manifest_sha256") or ""):
        raise MediaCertificationError("frozen task manifest hash differs")
    window_start=parse_certification_timestamp(task.get("window_start_at"),"window_start_at");window_end=parse_certification_timestamp(task.get("window_end_at"),"window_end_at")
    database,summary=open_certification_inventory(cfg)
    try: local=collect_certification_candidates(cfg,database,summary["generation"],int(task["recording_id"]),window_start,window_end,raw_clips)
    finally: database.close()
    frozen_ids=[int(c["clip_id"]) for c in raw_clips]
    if [c["clip_id"] for c in local]!=frozen_ids: raise MediaCertificationError("local inventory does not equal frozen manifest")
    source_bytes=sum(int(c["size_bytes"]) for c in raw_clips)
    if source_bytes>NATIVE_STITCH_MAX_SOURCE_BYTES: raise MediaCertificationError("source byte bound exceeded")
    # Before probing signatures, generation/attempt/timeline boundaries give a
    # safe upper bound on the largest possible run. Signature discovery can
    # only split it further.
    largest_possible_run=native_stitch_largest_possible_run(raw_clips)
    filesystem=os.statvfs(str(cfg.state_dir));free=filesystem.f_bavail*filesystem.f_frsize
    if free<largest_possible_run*2+NATIVE_STITCH_TEMP_RESERVE_BYTES: raise MediaCertificationError("insufficient temporary capacity")
    ffmpeg_version=media_tool_version_cancellable(ffmpeg_bin,cancel);ffprobe_version=media_tool_version_cancellable(ffprobe_bin,cancel)
    clips=[];paths=[];identities=[];video_edges=[]
    reason="completed"
    try:
        for frozen,item in zip(raw_clips,local):
            if stop_event.is_set() or check_native_stitch_delivery(cfg,runtime,cancel): raise InventoryScanStopped("certification yielded to delivery")
            if int(frozen.get("ordinal",0))!=len(clips)+1 or parse_certification_timestamp(frozen.get("clip_start_at"),"clip_start_at")!=item.get("clip_start_at") or parse_certification_timestamp(frozen.get("clip_end_at"),"clip_end_at")!=item.get("clip_end_at"):
                raise MediaCertificationError("sidecar timeline differs from frozen manifest")
            for key in ("clip_id","recording_id","recording_job_id","relative_path","size_bytes","sha256","capture_generation","capture_sequence","capture_attempt_id","timestamp_contract_version","timestamp_contract_status","timestamp_contract_reason","timestamp_contract_sha256"):
                if frozen.get(key)!=item.get(key): raise MediaCertificationError("sidecar differs from frozen manifest")
            path,identity,parent_identity,source_fd,tool_path=hash_certification_file_cancellable(cfg.output_dir,item["relative_path"],item["size_bytes"],item["sha256"],cancel)
            try:
                if time.monotonic()>=absolute_deadline: raise InventoryScanStopped("native stitch attempt deadline")
                recomputed_contract=video_timeline=audio_timeline=None
                if frozen.get("timestamp_contract_status")=="per_clip_probe_complete":
                    recomputed_contract,timelines=recompute_timestamp_contract(tool_path,ffprobe_bin,cancel,pass_fds=(source_fd,))
                    if timestamp_contract_hash(recomputed_contract)!=frozen.get("timestamp_contract_sha256") or recomputed_contract!=item.get("timestamp_contract"):
                        raise MediaCertificationError("exact NAS bytes recompute a different timestamp contract")
                    video_timeline=timelines.get("video");audio_timeline=timelines.get("audio")
                probe=probe_native_media_cancellable(tool_path,ffprobe_bin,cancel,pass_fds=(source_fd,))
                signature=probe["stable_signature_v1"];signature_sha=canonical_report_hash(signature)
                audio_present=any(s.get("codec_type")=="audio" for s in signature.get("streams",[]))
                decode_status="passed"
                try: strict_decode_media_cancellable(tool_path,ffmpeg_bin,cancel,pass_fds=(source_fd,))
                except DeterministicMediaError: decode_status="failed"
                edges=None
                if recomputed_contract is not None:
                    edges=native_stitch_video_edge_frames(tool_path,ffmpeg_bin,timelines.get("_video_frames",[]),cancel,pass_fds=(source_fd,))
            finally:
                os.close(source_fd)
            after_decode=path.stat()
            if certification_identity(after_decode)!=certification_identity(identity) or certification_identity(path.parent.stat())!=certification_identity(parent_identity): raise MediaCertificationError("media identity changed during decode")
            clip_fact={**{k:(v.isoformat().replace("+00:00","Z") if isinstance(v,datetime.datetime) else v) for k,v in frozen.items()},"sidecar_sha256":item["sidecar_sha256"],"file_identity":{"size":after_decode.st_size,"mtime_ns":after_decode.st_mtime_ns,"ctime_ns":after_decode.st_ctime_ns,"inode":after_decode.st_ino,"device":after_decode.st_dev},"native_signature":signature,"native_signature_sha256":signature_sha,"strict_decode":decode_status,"audio_present":audio_present}
            if recomputed_contract is not None: clip_fact["recomputed_timestamp_contract"]=recomputed_contract
            if video_timeline is not None: clip_fact["video_timeline"]=video_timeline
            if audio_timeline is not None: clip_fact["audio_timeline"]=audio_timeline
            clips.append(clip_fact);paths.append(path);identities.append((certification_identity(after_decode),certification_identity(parent_identity)));video_edges.append(edges)
            if decode_status=="failed": raise DeterministicMediaError("clip_decode_failed")
        runs=[];run_start=0
        for index in range(1,len(clips)+1):
            boundary=index==len(clips) or clips[index]["capture_generation"]!=clips[run_start]["capture_generation"] or clips[index].get("capture_attempt_id","")!=clips[run_start].get("capture_attempt_id","") or clips[index].get("timestamp_contract_version","")!=clips[run_start].get("timestamp_contract_version","") or clips[index]["native_signature_sha256"]!=clips[run_start]["native_signature_sha256"] or clips[index]["clip_start_at"]!=clips[index-1]["clip_end_at"]
            if not boundary: continue
            if check_native_stitch_delivery(cfg,runtime,cancel): raise InventoryScanStopped("certification yielded to delivery")
            run_bytes=sum(c["size_bytes"] for c in clips[run_start:index]);
            if run_bytes>NATIVE_STITCH_MAX_RUN_BYTES: raise MediaCertificationError("native run byte bound exceeded")
            with tempfile.TemporaryDirectory(prefix="stoarama-native-stitch-run-",dir=str(cfg.state_dir)) as raw_run:
                run_dir=Path(raw_run)
                snapshot_paths=snapshot_certification_run(paths[run_start:index],identities[run_start:index],clips[run_start:index],run_dir,cancel)
                try: validation=validate_native_run_cancellable(snapshot_paths,ffmpeg_bin,cancel,run_dir)
                except DeterministicMediaError: validation="failed"
            # A deterministic tool diagnosis is attributable only while
            # every source file is still the exact identity hashed above.
            # This check deliberately precedes persistence of a FAILED run
            # fact, including the failure path itself.
            verify_certification_source_identities(
                paths[run_start:index], identities[run_start:index], "run validation")
            if run_start==0: boundary_reason="window_start"
            elif clips[run_start-1]["capture_generation"]!=clips[run_start]["capture_generation"]: boundary_reason="capture_generation_change"
            elif clips[run_start-1].get("capture_attempt_id","")!=clips[run_start].get("capture_attempt_id","") or clips[run_start-1].get("timestamp_contract_version","")!=clips[run_start].get("timestamp_contract_version",""): boundary_reason="capture_attempt_change"
            elif clips[run_start-1]["native_signature_sha256"]!=clips[run_start]["native_signature_sha256"]: boundary_reason="native_signature_change"
            else: boundary_reason="temporal_gap"
            runs.append({"ordinal":len(runs)+1,"first_clip_ordinal":run_start+1,"last_clip_ordinal":index,"clip_count":index-run_start,"source_bytes":run_bytes,"native_signature_sha256":clips[run_start]["native_signature_sha256"],"capture_generation":clips[run_start]["capture_generation"],"capture_attempt_id":clips[run_start].get("capture_attempt_id",""),"timestamp_contract_version":clips[run_start].get("timestamp_contract_version",""),"boundary_reason":boundary_reason,"validation_status":validation});run_start=index
            if validation=="failed": raise DeterministicMediaError("run_concat_failed")
        verify_certification_source_identities(paths, identities, "run validation")
        run_starts={r["first_clip_ordinal"]:r for r in runs};seams=[];audio_seams=[]
        exact_video,exact_audio=native_stitch_clip_axis_continuity(clips)
        for i,(left,right) in enumerate(zip(clips,clips[1:]),start=1):
            boundary=run_starts.get(i+1)
            if boundary:
                seams.append({"ordinal":i,"previous_clip_id":left["clip_id"],"next_clip_id":right["clip_id"],"capture_generation":"","previous_capture_sequence":left["capture_sequence"],"next_capture_sequence":right["capture_sequence"],"native_signature_sha256":"","capture_attempt_id":"","timeline_basis":"unavailable","capture_contract":"","previous_frames":[],"next_frames":[],"confidence":"none","verdict":"not_applicable","reason":boundary["boundary_reason"]})
                audio_seams.append({"ordinal":i,"previous_clip_id":left["clip_id"],"next_clip_id":right["clip_id"],"sample_rate":0,"previous_end_sample":0,"next_start_sample":0,"previous_sample_count":0,"next_sample_count":0,"capture_attempt_id":"","timestamp_contract_version":"","verdict":"not_applicable","reason":boundary["boundary_reason"]})
            else:
                complete=left.get("timestamp_contract_status")=="per_clip_probe_complete" and right.get("timestamp_contract_status")=="per_clip_probe_complete" and left.get("capture_attempt_id") and left.get("capture_attempt_id")==right.get("capture_attempt_id")
                previous_edge=video_edges[i-1];next_edge=video_edges[i]
                adjacent=False
                if adjacent:
                    seams.append({"ordinal":i,"previous_clip_id":left["clip_id"],"next_clip_id":right["clip_id"],"capture_generation":left["capture_generation"],"previous_capture_sequence":left["capture_sequence"],"next_capture_sequence":right["capture_sequence"],"native_signature_sha256":left["native_signature_sha256"],"capture_attempt_id":left["capture_attempt_id"],"timeline_basis":"continuous_source_pts_v1","capture_contract":"continuous-source-pts-v1","previous_frames":previous_edge["last"],"next_frames":next_edge["first"],"confidence":"high","verdict":"exact","reason":"frame_adjacency_proven"})
                else:
                    exact_video=False
                    seams.append({"ordinal":i,"previous_clip_id":left["clip_id"],"next_clip_id":right["clip_id"],"capture_generation":left["capture_generation"],"previous_capture_sequence":left["capture_sequence"],"next_capture_sequence":right["capture_sequence"],"native_signature_sha256":left["native_signature_sha256"],"capture_attempt_id":left.get("capture_attempt_id","") if complete else "","timeline_basis":"unavailable","capture_contract":"","previous_frames":[],"next_frames":[],"confidence":"none","verdict":"ambiguous","reason":"continuous_source_pts_unavailable"})
                if not left["audio_present"] and not right["audio_present"]:
                    audio_seams.append({"ordinal":i,"previous_clip_id":left["clip_id"],"next_clip_id":right["clip_id"],"sample_rate":0,"previous_end_sample":0,"next_start_sample":0,"previous_sample_count":0,"next_sample_count":0,"capture_attempt_id":"","timestamp_contract_version":"","verdict":"not_present","reason":"audio_not_present"})
                elif complete and left.get("audio_timeline") and right.get("audio_timeline") and left["audio_timeline"]["sample_rate"]==right["audio_timeline"]["sample_rate"] and left["audio_timeline"]["end_sample"]==right["audio_timeline"]["first_sample"]:
                    audio_seams.append({"ordinal":i,"previous_clip_id":left["clip_id"],"next_clip_id":right["clip_id"],"sample_rate":left["audio_timeline"]["sample_rate"],"previous_end_sample":left["audio_timeline"]["end_sample"],"next_start_sample":right["audio_timeline"]["first_sample"],"previous_sample_count":left["audio_timeline"]["sample_count"],"next_sample_count":right["audio_timeline"]["sample_count"],"capture_attempt_id":left["capture_attempt_id"],"timestamp_contract_version":"continuous-source-pts-v1","verdict":"exact","reason":"audio_sample_adjacency_proven"})
                else:
                    exact_audio=False
                    audio_seams.append({"ordinal":i,"previous_clip_id":left["clip_id"],"next_clip_id":right["clip_id"],"sample_rate":0,"previous_end_sample":0,"next_start_sample":0,"previous_sample_count":0,"next_sample_count":0,"capture_attempt_id":"","timestamp_contract_version":"","verdict":"ambiguous","reason":"continuous_source_pts_unavailable"})
        # continuous-source-pts-v1 has rational endpoints but no server-frozen
        # packet-edge identity. It can never assert video adjacency, including
        # singleton and all-objective-boundary reports with no intra-run seam.
        exact_video=False
        has_audio=any(c["audio_present"] for c in clips)
        frame_adjacency="passed" if exact_video else "unknown"
        audio_continuity=("passed" if exact_audio else "unknown") if has_audio else "not_present"
        measured_timeline=native_stitch_timeline(local,window_start,window_end)
        full_envelope=native_stitch_full_envelope(measured_timeline)
        seamless=len(runs)==1 and exact_video and (not has_audio or exact_audio) and full_envelope
        status="passed" if seamless else "partial";decode=concat="passed";window_continuity="passed" if seamless else ("partitioned" if len(runs)>1 else "unknown");reason="completed" if seamless else ("partitioned_native_runs" if len(runs)>1 and exact_video and (not has_audio or exact_audio) else "continuous_source_pts_unavailable")
    except DeterministicMediaError as exc:
        status="failed";reason=str(exc);decode="failed" if reason=="clip_decode_failed" else "unknown";concat="failed" if reason=="run_concat_failed" else "unknown";frame_adjacency=audio_continuity=window_continuity="unknown";runs=locals().get("runs",[]);seams=locals().get("seams",[]);audio_seams=locals().get("audio_seams",[])
    except (MediaCertificationError,InventoryScanStopped) as exc:
        status="unknown";reason="attempt_deadline" if time.monotonic()>=absolute_deadline else ("delivery_preempted" if isinstance(exc,InventoryScanStopped) else "verification_transient");decode=concat=frame_adjacency=audio_continuity=window_continuity="unknown";clips=[];runs=[];seams=[];audio_seams=[]
    completed=datetime.datetime.now(datetime.timezone.utc)
    report={"schema_version":1,"policy_version":task["policy_version"],"task_id":task["task_id"],"recording_id":task["recording_id"],"recording_job_id":task["recording_job_id"],"window_start_at":task["window_start_at"],"window_end_at":task["window_end_at"],"clip_manifest_sha256":task["clip_manifest_sha256"],"inventory_generation":summary["generation"],"inventory_digest":summary["digest"],"inventory_completed_at":summary["scan_completed_at"],"status":status,"nas_byte_decode_status":decode,"native_run_concat_status":concat,"within_run_frame_adjacency_status":frame_adjacency,"within_run_audio_sample_continuity_status":audio_continuity,"window_continuity_status":window_continuity,"timeline":native_stitch_timeline(local,window_start,window_end),"clips":clips,"native_runs":runs,"seams":seams,"audio_seams":audio_seams,"reason_codes":[reason],"client_version":CLIENT_VERSION,"ffmpeg_version":ffmpeg_version,"ffprobe_version":ffprobe_version,"started_at":started.isoformat().replace("+00:00","Z"),"completed_at":completed.isoformat().replace("+00:00","Z"),"source_media_modified":False,"reencoded":False,"persistent_output_created":False}
    submit_native_stitch_completion(cfg,task,report)
    return True


def canonical_report_hash(report):
    # Match encoding/json's UTF-8 canonical form. Go deliberately escapes the
    # two JavaScript line separators even with HTML escaping disabled.
    canonical = json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    canonical = canonical.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    encoded = canonical.encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def timestamp_contract_hash(contract):
    """Match Go's typed TimestampContract encoding field-for-field.

    This is intentionally separate from the generic sorted-object hash: the
    server hashes a typed struct in declaration order before freezing a task.
    """
    if not isinstance(contract, dict) or not isinstance(contract.get("tracks"), list):
        raise MediaCertificationError("timestamp contract is invalid")
    ordered_tracks = []
    for track in contract["tracks"]:
        if not isinstance(track, dict):
            raise MediaCertificationError("timestamp contract track is invalid")
        ordered = {
            "stream_index": track.get("stream_index"), "media_type": track.get("media_type"),
            "time_base_num": track.get("time_base_num"), "time_base_den": track.get("time_base_den"),
            "first_timestamp": track.get("first_timestamp"), "last_timestamp": track.get("last_timestamp"),
            "last_duration": track.get("last_duration"), "unit_count": track.get("unit_count"),
        }
        if track.get("sample_rate", 0): ordered["sample_rate"] = track["sample_rate"]
        if track.get("last_sample_count", 0): ordered["last_sample_count"] = track["last_sample_count"]
        ordered["codec_signature_sha256"] = track.get("codec_signature_sha256")
        ordered_tracks.append(ordered)
    ordered_contract = {"version": contract.get("version"), "mode": contract.get("mode"),
                        "audio_selection": contract.get("audio_selection"), "tracks": ordered_tracks}
    raw = json.dumps(ordered_contract, separators=(",", ":"), ensure_ascii=False)
    raw = raw.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def native_stitch_manifest_hash(clips):
    if not isinstance(clips, list):
        raise MediaCertificationError("frozen task manifest is invalid")
    fields = (
        "ordinal", "clip_id", "recording_id", "recording_job_id", "relative_path", "size_bytes", "sha256",
        "clip_start_at", "clip_end_at", "capture_generation", "capture_sequence", "capture_attempt_id",
        "timestamp_contract_version", "timestamp_contract_status", "timestamp_contract_reason", "timestamp_contract_sha256",
    )
    ordered = []
    for clip in clips:
        if not isinstance(clip, dict) or any(field not in clip for field in fields):
            raise MediaCertificationError("frozen task manifest is invalid")
        ordered.append({field: clip[field] for field in fields})
    raw = json.dumps(ordered, separators=(",", ":"), ensure_ascii=False)
    raw = raw.replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def download_verified(url, temp_path, expected_bytes, expected_sha):
    digest = hashlib.sha256()
    written = 0
    req = urllib.request.Request(url, method="GET", headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as response, open(temp_path, "wb") as out:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                digest.update(chunk)
                written += len(chunk)
            out.flush()
            os.fsync(out.fileno())
    except BaseException:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise
    if written != expected_bytes or digest.hexdigest() != expected_sha:
        temp_path.unlink()
        raise RuntimeError("download checksum mismatch")


def valid_joined_item(raw):
    if not isinstance(raw, dict):
        raise ValueError("joined item is invalid")
    fields = {
        "artifact_id", "connection_id", "batch_id", "hour_id", "kind", "content_type", "relative_path", "size_bytes", "sha256",
        "download_path", "ledger_artifact_id", "ledger_relative_path", "ledger_sha256",
        "hour_manifest_id", "hour_manifest_relative_path", "hour_manifest_sha256",
    }
    if set(raw) != fields:
        raise ValueError("joined item has invalid fields")
    artifact_id = raw.get("artifact_id")
    connection_id = raw.get("connection_id")
    size_bytes = raw.get("size_bytes")
    if isinstance(artifact_id, bool) or not isinstance(artifact_id, int) or artifact_id < 1:
        raise ValueError("joined item has invalid id")
    if isinstance(connection_id, bool) or not isinstance(connection_id, int) or connection_id < 1 or connection_id > 2**63 - 1:
        raise ValueError("joined item has invalid connection_id")
    if (
        isinstance(size_bytes, bool) or not isinstance(size_bytes, int)
        or size_bytes < 1 or size_bytes > JOINED_MAX_BYTES
    ):
        raise ValueError("joined item has invalid size_bytes")
    batch_id = raw.get("batch_id")
    if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", batch_id):
        raise ValueError("joined item has invalid batch_id")
    relative_raw = raw.get("relative_path")
    if not isinstance(relative_raw, str):
        raise ValueError("joined item has invalid relative_path")
    relative = Path(relative_raw)
    if (
        not relative_raw or relative.is_absolute() or relative_raw != relative.as_posix()
        or "\\" in relative_raw or any(part in ("", ".", "..") for part in relative.parts)
        or relative.parts[0] == JOINED_ROOT
    ):
        raise ValueError("joined item has invalid relative_path")
    sha256 = valid_sha256(raw.get("sha256"), "joined item")
    kind = raw.get("kind")
    content_type = raw.get("content_type")
    hour_id = raw.get("hour_id")
    if kind not in ("allocation_ledger", "hour_manifest", "media", "batch_index"):
        raise ValueError("joined item has invalid kind")
    if content_type != ("video/mp4" if kind == "media" else "application/json"):
        raise ValueError("joined item has invalid content_type")
    if kind != "media" and size_bytes > JOINED_MANIFEST_MAX_BYTES:
        raise ValueError("joined JSON artifact exceeds size cap")
    if kind == "batch_index":
        if hour_id is not None or relative.as_posix() != "coverage/batch.json":
            raise ValueError("joined batch index has invalid hour identity or path")
    elif kind == "allocation_ledger":
        ledger_match = re.fullmatch(r"coverage/ledgers/([1-9][0-9]*)/([0-9]{4}-[0-9]{2}-[0-9]{2})\.json", relative_raw)
        if hour_id is not None or ledger_match is None:
            raise ValueError("joined allocation ledger has invalid hour identity or path")
        try:
            if datetime.date.fromisoformat(ledger_match.group(2)).isoformat() != ledger_match.group(2):
                raise ValueError
        except ValueError as exc:
            raise ValueError("joined allocation ledger has invalid date") from exc
    elif not valid_joined_hour_id(batch_id, hour_id):
        raise ValueError("joined item has invalid hour identity")
    manifest_path = "coverage/hours/%s.json" % hour_id if hour_id is not None else None
    if kind == "hour_manifest" and relative_raw != manifest_path:
        raise ValueError("joined hour manifest has invalid suffix")
    if kind == "media" and not relative_raw.endswith(".mp4"):
        raise ValueError("joined media has invalid suffix")
    ledger_id = raw.get("ledger_artifact_id")
    ledger_path = raw.get("ledger_relative_path")
    ledger_sha = raw.get("ledger_sha256")
    manifest_id = raw.get("hour_manifest_id")
    manifest_relative_path = raw.get("hour_manifest_relative_path")
    manifest_sha = raw.get("hour_manifest_sha256")
    if kind == "media":
        if isinstance(manifest_id, bool) or not isinstance(manifest_id, int) or manifest_id < 1:
            raise ValueError("joined media has invalid hour_manifest_id")
        manifest_relative_path = valid_joined_relative_path(manifest_relative_path, ".json")
        if manifest_relative_path != manifest_path:
            raise ValueError("joined media has invalid hour manifest path")
        manifest_sha = valid_sha256(manifest_sha, "joined media manifest")
    elif any(value is not None for value in (manifest_id, manifest_relative_path, manifest_sha)):
        raise ValueError("joined non-media item has manifest binding")
    if kind == "hour_manifest":
        if isinstance(ledger_id, bool) or not isinstance(ledger_id, int) or ledger_id < 1:
            raise ValueError("joined hour manifest has invalid ledger_artifact_id")
        ledger_path = valid_joined_relative_path(ledger_path, ".json")
        if not re.fullmatch(r"coverage/ledgers/[1-9][0-9]*/[0-9]{4}-[0-9]{2}-[0-9]{2}\.json", ledger_path):
            raise ValueError("joined hour manifest has invalid ledger path")
        ledger_sha = valid_sha256(ledger_sha, "joined allocation ledger")
    elif any(value is not None for value in (ledger_id, ledger_path, ledger_sha)):
        raise ValueError("joined non-manifest item has ledger binding")
    download_path = raw.get("download_path")
    if not isinstance(download_path, str):
        raise ValueError("joined item has invalid download_path")
    if download_path != "/api/v1/account/joined/%d/download" % artifact_id:
        raise ValueError("joined item has invalid download_path")
    return {
        "id": artifact_id, "connection_id": connection_id, "batch_id": batch_id, "hour_id": hour_id, "kind": kind,
        "content_type": content_type, "relative_path": relative.as_posix(), "size_bytes": size_bytes,
        "sha256": sha256, "download_path": download_path, "ledger_artifact_id": ledger_id,
        "ledger_relative_path": ledger_path, "ledger_sha256": ledger_sha,
        "hour_manifest_id": manifest_id,
        "hour_manifest_relative_path": manifest_relative_path, "hour_manifest_sha256": manifest_sha,
    }


def valid_joined_hour_id(batch_id, value):
    if not isinstance(value, str):
        return False
    match = re.fullmatch(
        re.escape(batch_id) + r"__recording-([1-9][0-9]*)__date-([0-9]{4}-[0-9]{2}-[0-9]{2})__hour-(0[1-9]|1[0-2])__generation-([1-9][0-9]*)",
        value,
    )
    if match is None:
        return False
    try:
        return datetime.date.fromisoformat(match.group(2)).isoformat() == match.group(2)
    except ValueError:
        return False


def valid_sha256(value, label):
    if not isinstance(value, str) or len(value) != 64 or any(ch not in "0123456789abcdef" for ch in value):
        raise ValueError("joined item has invalid sha256")
    return value


def valid_joined_relative_path(value, suffix=None):
    if not isinstance(value, str):
        raise ValueError("joined item has invalid relative_path")
    raw = value
    relative = Path(raw)
    if (
        not raw or relative.is_absolute() or raw != relative.as_posix() or "\\" in raw
        or any(part in ("", ".", "..") for part in relative.parts) or relative.parts[0] == JOINED_ROOT
        or (suffix is not None and not raw.endswith(suffix))
    ):
        raise ValueError("joined item has invalid relative_path")
    return relative.as_posix()


def normalized_etag(value):
    if not isinstance(value, str) or value != value.strip():
        raise ValueError("joined item has invalid etag")
    raw = value
    if raw.startswith("W/"):
        raise ValueError("joined item has weak etag")
    if len(raw) >= 2 and raw[0] == raw[-1] == '"':
        raw = raw[1:-1]
    if not raw or len(raw) > 256 or '"' in raw or any(ord(ch) < 33 or ord(ch) > 126 for ch in raw):
        raise ValueError("joined item has invalid etag")
    return raw


def joined_output_path(cfg, item):
    return cfg.output_dir / JOINED_ROOT / item["batch_id"] / item["relative_path"]


def open_joined_output_dir(cfg, item, create=True):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(str(cfg.output_dir), flags)
    parts = (JOINED_ROOT, item["batch_id"], *Path(item["relative_path"]).parts[:-1])
    try:
        for part in parts:
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise ExistingFileMismatch("joined manifest directory is missing")
                os.mkdir(part, mode=0o755, dir_fd=descriptor)
                os.fsync(descriptor)
                child = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def joined_entry_stat(directory_fd, name):
    try:
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if stat_module.S_ISLNK(current.st_mode) or not stat_module.S_ISREG(current.st_mode):
        raise ExistingFileMismatch("joined entry is not a regular file")
    return current


def hash_joined_entry(cfg, runtime, directory_fd, name, stop_event):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=directory_fd)
    digest = hashlib.sha256()
    size = 0
    try:
        before = os.fstat(descriptor)
        if not stat_module.S_ISREG(before.st_mode):
            raise ExistingFileMismatch("joined entry is not a regular file")
        while True:
            if stop_event.is_set() or poll_raw_pending(cfg, runtime):
                raise JoinedDownloadYield("joined hashing yielded to raw clip delivery")
            chunk = os.read(descriptor, JOINED_RANGE_BYTES)
            if not chunk:
                break
            size += len(chunk)
            digest.update(chunk)
        after = os.fstat(descriptor)
        path_after = joined_entry_stat(directory_fd, name)
        if (
            path_after is None or certification_identity(before) != certification_identity(after)
            or certification_identity(after) != certification_identity(path_after)
        ):
            raise FileChangedDuringHash("joined file changed while hashing")
    finally:
        os.close(descriptor)
    return size, digest.hexdigest(), after


def verify_joined_entry(cfg, runtime, directory_fd, name, expected_bytes, expected_sha, stop_event):
    if joined_entry_stat(directory_fd, name) is None:
        return False
    size, digest, _ = hash_joined_entry(cfg, runtime, directory_fd, name, stop_event)
    if size != expected_bytes or digest != expected_sha:
        raise ExistingFileMismatch("existing joined entry does not match API checksum: %s" % name)
    return True


def joined_transfer_marker_bytes(item, prepared):
    payload = {
        "schema_version": 3,
        **{key: item[key] for key in (
            "id", "connection_id", "batch_id", "hour_id", "kind", "content_type", "relative_path", "size_bytes", "sha256",
            "download_path", "ledger_artifact_id", "ledger_relative_path", "ledger_sha256",
            "hour_manifest_id", "hour_manifest_relative_path", "hour_manifest_sha256",
        )},
        "etag": prepared["etag"],
        "version_id": prepared["version_id"],
        "url_scheme": prepared["url_scheme"],
        "url_authority": prepared["url_authority"],
        "url_path": prepared["url_path"],
    }
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def verify_joined_sidecar(cfg, runtime, directory_fd, name, expected, stop_event):
    if joined_entry_stat(directory_fd, name) is None:
        return False
    size, digest, _ = hash_joined_entry(cfg, runtime, directory_fd, name, stop_event)
    return size == len(expected) and digest == hashlib.sha256(expected).hexdigest()


def valid_joined_timestamp(value, label):
    if not isinstance(value, str) or not value:
        raise ValueError("joined manifest has invalid %s" % label)
    match = re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.([0-9]{1,9}))?(?:Z|[+-][0-9]{2}:[0-9]{2})", value)
    if match is None:
        raise ValueError("joined manifest has invalid %s" % label)
    normalized = value.replace("Z", "+00:00")
    if match.group(1) and len(match.group(1)) > 6:
        normalized = normalized.replace("." + match.group(1), "." + match.group(1)[:6], 1)
    try:
        parsed = datetime.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("joined manifest has invalid %s" % label) from exc
    if parsed.tzinfo is None:
        raise ValueError("joined manifest has invalid %s" % label)
    return parsed


def positive_joined_int(value, label, allow_zero=False):
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > 2**63 - 1:
        raise ValueError("joined manifest has invalid %s" % label)
    return value


def valid_joined_int64(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or not -(2**63) <= value <= 2**63 - 1:
        raise ValueError("joined manifest has invalid %s" % label)
    return value


JOINED_REASON = re.compile(r"[a-z][a-z0-9_]{0,79}\Z")
JOINED_BATCH = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")
QUALIFICATION_FIELDS = ("local_date", "job_id", "window_start", "window_end", "completed_at", "quality_tier")
OBJECT_FIELDS = ("key", "etag", "size_bytes", "sha256")
SEAM_FIELDS = ("verdict", "reason", "signed_gap_nanoseconds")
SOURCE_FIELDS = (
    "clip_id", "recording_id", "recording_job_id", "provider", "endpoint", "region", "bucket",
    "start_utc", "end_utc", "object", "seam_to_previous",
)
CROSS_HOUR_FIELDS = (
    "previous_delivery_hour", "next_delivery_hour", "previous_clip_id", "next_clip_id",
    "previous_presentation_end_utc", "next_presentation_start_utc", "signed_gap_nanoseconds",
    "scheduled_utc", "actual_seam_utc", "boundary_skew_nanoseconds", "allocation_decision", "verdict", "reason",
)
CROSS_DAY_FIELDS = (
    "previous_clip_id", "next_clip_id", "previous_presentation_end_utc", "next_presentation_start_utc",
    "signed_gap_nanoseconds", "scheduled_previous_end_utc", "scheduled_next_start_utc",
    "boundary_skew_nanoseconds", "allocation_decision", "verdict", "reason",
)
MEDIA_TOOL_FIELDS = ("ffmpeg_version", "ffmpeg_sha256", "ffprobe_version", "ffprobe_sha256", "identity_sha256")

JOINED_OBJECT_ORDERS = {}
for _joined_order in (
    ("schema_version", "policy_version", "status", "batch_id", "hour_id", "recording_id", "timezone", "local_date", "delivery_hour", "clock_hour", "scheduled_start_utc", "scheduled_end_utc", "qualification_day", "qualification_sha256", "allocation", "media_tool", "source_claim_sha256", "source_count", "sources", "source_dispositions", "gaps", "scheduled_gap", "quarantine_reason_code", "quarantine_evidence", "media"),
    QUALIFICATION_FIELDS,
    ("artifact_id", "relative_path", "object_key", "size_bytes", "sha256", "ledger_sha256", "hour_source_claim_sha256", "boundaries", "cross_day_boundaries"),
    CROSS_HOUR_FIELDS, CROSS_DAY_FIELDS, MEDIA_TOOL_FIELDS,
    SOURCE_FIELDS,
    ("clip_id", "recording_id", "recording_job_id", "provider", "endpoint", "region", "bucket", "start_utc", "end_utc", "object", "audio_sequence_contract", "seam_to_previous"),
    OBJECT_FIELDS, ("key", "version_id", "etag", "size_bytes", "sha256"), SEAM_FIELDS,
    ("clip_id", "disposition", "media_artifact_id", "media_ordinal", "reason_code"),
    ("artifact_id", "ordinal", "part", "parts", "relative_path", "object_key", "content_id", "size_bytes", "sha256", "actual_start_utc", "actual_end_utc", "utc_offset_seconds", "media_tool_identity", "source_clip_ids", "verification", "maximality_evidence"),
    ("status", "packet_payload_order_status", "decoded_frame_totals_status", "decoded_audio_totals_status", "output_timestamp_status", "strict_decode_status", "source_fingerprint", "output_fingerprint"),
    ("duration_seconds", "tracks"),
    ("duration_seconds", "tracks", "audio_sequence_contracts", "effective_audio_bytes", "effective_audio_sample_frames", "effective_audio_sha256"),
    ("media_type", "packet_count", "packet_chain_sha256", "packet_timing_sha256", "packet_time_bases", "first_packet_pts_seconds", "last_packet_pts_seconds", "first_packet_dts_seconds", "last_packet_dts_seconds", "packet_duration_seconds", "decode_timeline_span_seconds", "decoded_frames", "first_timestamp", "last_timestamp", "timestamp_status"),
    ("media_type", "packet_count", "packet_chain_sha256", "packet_timing_sha256", "packet_time_bases", "first_packet_pts_seconds", "last_packet_pts_seconds", "first_packet_dts_seconds", "last_packet_dts_seconds", "packet_duration_seconds", "decode_timeline_span_seconds", "decoded_frames", "decoded_samples", "first_timestamp", "last_timestamp", "timestamp_status"),
    ("codec_name", "sample_rate", "channels", "channel_layout", "initial_padding", "skip_samples", "discard_padding", "codec_delay", "trailing_padding"),
    ("codec_name", "sample_rate", "channels", "channel_layout", "initial_padding", "skip_samples", "discard_padding", "codec_delay", "trailing_padding", "edit_list_kind", "edit_list_sha256"),
    ("reason_code", "signed_gap_nanoseconds", "no_allocatable_sources"),
    ("schema_version", "batch_id", "generation", "recording_id", "timezone", "local_date", "qualification_day", "qualification_sha256", "source_claim_sha256", "source_clip_count", "source_bytes", "first_clip_id", "last_clip_id", "consecutive_pairs", "sources", "hours", "hour_source_claim_sha256", "cross_hour_boundaries", "cross_day_boundaries", "ledger_sha256"),
    ("previous_clip_id", "next_clip_id", "previous_presentation_end_utc", "next_presentation_start_utc", "signed_gap_nanoseconds"),
    ("delivery_hour", "clock_hour", "source_clip_ids"),
    ("reason_code", "source_clip_ids", "source_claim_sha256", "policy_version", "normalized_failure_facts", "failure_sha256", "evidence_sha256", "isolated_attempt_count", "media_tool_identity"),
    ("candidate_clip_ids", "reason_code", "source_claim_sha256", "policy_version", "evidence_sha256", "normalized_failure_facts", "failure_sha256", "repeat_count", "media_tool_identity"),
    ("schema_version", "policy_version", "allocation_schema_version", "hour_manifest_schema_version", "batch_id", "generation", "frozen_at", "batch_generation_sha256", "frozen_denominator_sha256", "recording_ids", "recording_ids_sha256", "frozen_recordings", "media_tool", "expected_ledger_count", "scheduled_hour_count", "source_clip_count", "source_bytes", "final_media_artifact_count", "allocation_ledgers", "hours"),
    ("recording_id", "priority_ordinal", "eligibility_tier", "eligibility_cutoff", "completed_at", "timezone", "folder_name", "naming_metadata"),
    ("plaza_id", "continent", "country", "city", "plaza_name"),
    ("artifact_id", "recording_id", "local_date", "qualification_sha256", "source_claim_sha256", "relative_path", "object_key", "size_bytes", "sha256", "ledger_sha256", "source_count", "source_bytes", "scheduled_hour_ids"),
    ("hour_manifest_artifact_id", "hour_id", "recording_id", "local_date", "delivery_hour", "status", "relative_path", "object_key", "size_bytes", "sha256", "source_count", "source_bytes", "media_artifact_count"),
    ("previous_clip_id", "next_clip_id", "at_utc", "signed_gap_nanoseconds", "reason"),
    ("category",), ("category", "exit_code", "normalized_fact"), ("source_bytes",), ("output_bytes",),
    ("source_claim_sha256", "reason_code", "failure_sha256", "policy_version", "media_tool_identity", "repeat_count"),
    ("clip_id", "source_claim_sha256"),
    ("projection_version", "ledgers"),
    ("recording_id", "local_date", "source_claim_sha256", "source_count", "source_bytes"),
):
    JOINED_OBJECT_ORDERS.setdefault(frozenset(_joined_order), set()).add(_joined_order)


def exact_joined_fields(value, fields, label):
    expected = set(fields)
    if not isinstance(value, dict) or set(value) != expected:
        raise ValueError("joined %s has invalid fields" % label)
    allowed = JOINED_OBJECT_ORDERS.get(frozenset(expected))
    if allowed is not None and tuple(value) not in allowed:
        raise ValueError("joined %s has noncanonical field order" % label)
    return value


def valid_joined_string(value, label, allow_empty=False, maximum=4096):
    if not isinstance(value, str) or len(value) > maximum or (not allow_empty and not value) or any(ord(ch) < 32 for ch in value):
        raise ValueError("joined %s has invalid string" % label)
    return value


def valid_joined_reason(value, label, allow_empty=False):
    if value == "" and allow_empty:
        return value
    if not isinstance(value, str) or JOINED_REASON.fullmatch(value) is None:
        raise ValueError("joined %s has invalid reason" % label)
    return value


def valid_joined_date(value, label="local_date"):
    try:
        if not isinstance(value, str) or datetime.date.fromisoformat(value).isoformat() != value:
            raise ValueError
    except ValueError as exc:
        raise ValueError("joined %s is invalid" % label) from exc
    return value


def joined_canonical_bytes(value):
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return (
        raw.replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")
        .replace("\u2028", "\\u2028").replace("\u2029", "\\u2029").encode("utf-8")
    )


def joined_canonical_sha(value):
    return hashlib.sha256(joined_canonical_bytes(value)).hexdigest()


def decode_joined_json(content):
    def pairs(values):
        keys = tuple(key for key, _value in values)
        allowed = JOINED_OBJECT_ORDERS.get(frozenset(keys))
        if (allowed is not None and keys not in allowed) or (allowed is None and keys != tuple(sorted(keys))):
            raise ValueError("joined JSON object has noncanonical field order")
        out = {}
        for key, value in values:
            if key in out:
                raise ValueError("joined JSON contains a duplicate field")
            out[key] = value
        return out
    def finite_float(raw):
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError("joined JSON contains a non-finite number")
        return value
    try:
        payload = json.loads(
            content.decode("utf-8"), object_pairs_hook=pairs,
            parse_float=finite_float,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("joined JSON contains a non-finite number")),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("joined JSON artifact is not valid UTF-8 JSON") from exc
    if content != joined_canonical_bytes(payload):
        raise ValueError("joined JSON artifact is not canonical")
    return payload


def joined_timestamp_nanoseconds(value, label):
    parsed = valid_joined_timestamp(value, label)
    match = re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.([0-9]{1,9}))?(?:Z|[+-][0-9]{2}:[0-9]{2})", value)
    if match is None:
        raise ValueError("joined manifest has invalid %s" % label)
    fraction = int((match.group(1) or "").ljust(9, "0"))
    whole = parsed.astimezone(datetime.timezone.utc).replace(microsecond=0) - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (whole.days * 86400 + whole.seconds) * 1_000_000_000 + fraction


def source_claim_projection(source):
    projected = {key: source[key] for key in (
        "clip_id", "recording_id", "recording_job_id", "provider", "endpoint", "region", "bucket",
        "start_utc", "end_utc", "object",
    )}
    projected["seam_to_previous"] = {"verdict": "", "reason": "", "signed_gap_nanoseconds": 0}
    return projected


def source_claim_sha(sources):
    # Go's empty source-only slice is nil and therefore serializes as null.
    return joined_canonical_sha([source_claim_projection(source) for source in sources] if sources else None)


def candidate_source_claim_sha(sources):
    return joined_canonical_sha([
        {"clip_id": source["clip_id"], "source_claim_sha256": source_claim_sha([source])}
        for source in sources
    ])


def valid_qualification_day(day, recording_id, timezone, local_date):
    exact_joined_fields(day, QUALIFICATION_FIELDS, "qualification day")
    if day["local_date"] != valid_joined_date(local_date) or positive_joined_int(day["job_id"], "job_id") < 1:
        raise ValueError("joined qualification day identity conflicts")
    start = valid_joined_timestamp(day["window_start"], "qualification window_start")
    end = valid_joined_timestamp(day["window_end"], "qualification window_end")
    completed = valid_joined_timestamp(day["completed_at"], "qualification completed_at")
    try:
        location = ZoneInfo(valid_joined_string(timezone, "timezone", maximum=255))
    except ZoneInfoNotFoundError as exc:
        raise ValueError("joined timezone is invalid") from exc
    local_start, local_end = start.astimezone(location), end.astimezone(location)
    if (
        end - start != datetime.timedelta(hours=12) or completed < end or day["quality_tier"] != "good+"
        or local_start.date().isoformat() != local_date or local_end.date().isoformat() != local_date
        or (local_start.hour, local_start.minute, local_start.second, local_start.microsecond) != (8, 0, 0, 0)
        or (local_end.hour, local_end.minute, local_end.second, local_end.microsecond) != (20, 0, 0, 0)
    ):
        raise ValueError("joined qualification day is invalid")


def valid_media_tool(tool):
    exact_joined_fields(tool, MEDIA_TOOL_FIELDS, "media tool")
    valid_joined_string(tool["ffmpeg_version"], "ffmpeg_version")
    valid_joined_string(tool["ffprobe_version"], "ffprobe_version")
    valid_sha256(tool["ffmpeg_sha256"], "ffmpeg")
    valid_sha256(tool["ffprobe_sha256"], "ffprobe")
    identity = valid_sha256(tool["identity_sha256"], "media tool")
    unsealed = dict(tool)
    unsealed["identity_sha256"] = ""
    if joined_canonical_sha(unsealed) != identity:
        raise ValueError("joined media tool identity conflicts")


def valid_source(source, recording_id, source_only=False):
    fields = set(SOURCE_FIELDS)
    if not isinstance(source, dict) or set(source) not in (fields, fields | {"audio_sequence_contract"}):
        raise ValueError("joined source has invalid fields")
    clip_id = positive_joined_int(source["clip_id"], "clip_id")
    if source["recording_id"] != recording_id:
        raise ValueError("joined source recording identity conflicts")
    positive_joined_int(source["recording_job_id"], "recording_job_id")
    for key in ("provider", "endpoint", "region", "bucket"):
        valid_joined_string(source[key], "source %s" % key)
    start = valid_joined_timestamp(source["start_utc"], "source start_utc")
    end = valid_joined_timestamp(source["end_utc"], "source end_utc")
    if end <= start:
        raise ValueError("joined source range is invalid")
    object_fields = set(OBJECT_FIELDS)
    if isinstance(source["object"], dict) and "version_id" in source["object"]:
        object_fields.add("version_id")
    exact_joined_fields(source["object"], object_fields, "source object")
    for key in ("key", "etag"):
        valid_joined_string(source["object"][key], "source object %s" % key)
    if "version_id" in source["object"]:
        valid_joined_string(source["object"]["version_id"], "source object version_id")
    positive_joined_int(source["object"]["size_bytes"], "source object size_bytes")
    valid_sha256(source["object"]["sha256"], "source object")
    exact_joined_fields(source["seam_to_previous"], SEAM_FIELDS, "source seam")
    if source_only:
        if "audio_sequence_contract" in source or source["seam_to_previous"] != {"verdict": "", "reason": "", "signed_gap_nanoseconds": 0}:
            raise ValueError("joined source-only claim contains derived evidence")
    else:
        valid_joined_string(source["seam_to_previous"]["verdict"], "source seam verdict", allow_empty=True)
        valid_joined_string(source["seam_to_previous"]["reason"], "source seam reason", allow_empty=True)
        valid_joined_int64(source["seam_to_previous"]["signed_gap_nanoseconds"], "source seam gap")
        if "audio_sequence_contract" in source:
            valid_audio_contract(source["audio_sequence_contract"])
    return clip_id


def valid_derived_source_seams(sources):
    empty = {"verdict": "", "reason": "", "signed_gap_nanoseconds": 0}
    if sources and sources[0]["seam_to_previous"] != empty:
        raise ValueError("joined first source has prior-seam evidence")
    for previous, following in zip(sources, sources[1:]):
        seam = following["seam_to_previous"]
        signed_gap = joined_timestamp_nanoseconds(following["start_utc"], "seam next start") - joined_timestamp_nanoseconds(previous["end_utc"], "seam previous end")
        valid_joined_reason(seam["reason"], "source seam reason")
        valid = (
            (seam["verdict"] == "continuous" and signed_gap == 0)
            or (seam["verdict"] == "gap" and signed_gap > 0)
            or (seam["verdict"] == "overlap" and signed_gap < 0)
            or seam["verdict"] == "incompatible"
        )
        if not valid or seam["signed_gap_nanoseconds"] != signed_gap:
            raise ValueError("joined derived source seam conflicts")


def valid_boundary(boundary, cross_day=False):
    exact_joined_fields(boundary, CROSS_DAY_FIELDS if cross_day else CROSS_HOUR_FIELDS, "boundary")
    if not cross_day:
        previous = positive_joined_int(boundary["previous_delivery_hour"], "previous_delivery_hour")
        following = positive_joined_int(boundary["next_delivery_hour"], "next_delivery_hour")
        if following != previous + 1 or following > 12:
            raise ValueError("joined boundary hour identity conflicts")
        valid_joined_timestamp(boundary["scheduled_utc"], "boundary scheduled_utc")
    else:
        valid_joined_timestamp(boundary["scheduled_previous_end_utc"], "boundary scheduled_previous_end_utc")
        valid_joined_timestamp(boundary["scheduled_next_start_utc"], "boundary scheduled_next_start_utc")
    pairs = (
        ("previous_clip_id", "previous_presentation_end_utc"),
        ("next_clip_id", "next_presentation_start_utc"),
    )
    present = []
    for id_key, time_key in pairs:
        clip_id, timestamp = boundary[id_key], boundary[time_key]
        if (clip_id is None) != (timestamp is None):
            raise ValueError("joined boundary has partial source identity")
        if clip_id is not None:
            positive_joined_int(clip_id, id_key)
            valid_joined_timestamp(timestamp, time_key)
            present.append(True)
        else:
            present.append(False)
    signed_gap = boundary["signed_gap_nanoseconds"]
    if (signed_gap is None) != (not all(present)) or (signed_gap is not None and (isinstance(signed_gap, bool) or not isinstance(signed_gap, int))):
        raise ValueError("joined boundary has invalid signed gap")
    if signed_gap is not None:
        valid_joined_int64(signed_gap, "boundary signed gap")
        want_gap = joined_timestamp_nanoseconds(boundary["next_presentation_start_utc"], "boundary next start") - joined_timestamp_nanoseconds(boundary["previous_presentation_end_utc"], "boundary previous end")
        if signed_gap != want_gap:
            raise ValueError("joined boundary signed gap conflicts")
    actual = None if cross_day else boundary["actual_seam_utc"]
    skew = boundary["boundary_skew_nanoseconds"]
    if actual is not None:
        valid_joined_timestamp(actual, "boundary actual_seam_utc")
    if skew is not None:
        valid_joined_int64(skew, "boundary skew")
    valid_joined_reason(boundary["allocation_decision"], "boundary allocation_decision")
    valid_joined_reason(boundary["verdict"], "boundary verdict")
    valid_joined_reason(boundary["reason"], "boundary reason")


def local_boundary_nanoseconds(local_date, location, hour, day_delta=0):
    day = datetime.date.fromisoformat(local_date) + datetime.timedelta(days=day_delta)
    value = datetime.datetime(day.year, day.month, day.day, hour, tzinfo=location)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    utc = value.astimezone(datetime.timezone.utc)
    delta = utc - epoch
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000


def validate_cross_hour_boundaries(payload, location, local_date):
    sources = payload["sources"]
    minimum = consumed = 0
    for index, boundary in enumerate(payload["cross_hour_boundaries"]):
        consumed += len(payload["hours"][index]["source_clip_ids"])
        scheduled_ns = local_boundary_nanoseconds(local_date, location, 9 + index)
        if joined_timestamp_nanoseconds(boundary["scheduled_utc"], "boundary scheduled_utc") != scheduled_ns:
            raise ValueError("joined allocation ledger boundary schedule conflicts")
        if not sources:
            if boundary != {
                "previous_delivery_hour": index + 1, "next_delivery_hour": index + 2,
                "previous_clip_id": None, "next_clip_id": None,
                "previous_presentation_end_utc": None, "next_presentation_start_utc": None,
                "signed_gap_nanoseconds": None, "scheduled_utc": boundary["scheduled_utc"],
                "actual_seam_utc": None, "boundary_skew_nanoseconds": None,
                "allocation_decision": "no_sources", "verdict": "absent_source",
                "reason": "both_sources_absent",
            }:
                raise ValueError("joined source-free boundary conflicts")
            continue
        candidates = []
        for position in range(minimum, len(sources) + 1):
            if position:
                source = sources[position - 1]
                at_ns = joined_timestamp_nanoseconds(source["end_utc"], "source end")
                candidates.append((abs(at_ns - scheduled_ns), at_ns, position, source["clip_id"], source["end_utc"]))
            if position < len(sources):
                source = sources[position]
                at_ns = joined_timestamp_nanoseconds(source["start_utc"], "source start")
                candidates.append((abs(at_ns - scheduled_ns), at_ns, position, source["clip_id"], source["start_utc"]))
        _, best_ns, best_position, _, best_at = min(candidates)
        if consumed != best_position:
            raise ValueError("joined allocation ledger is not assigned at the closest frozen seam")
        previous = sources[best_position - 1] if best_position else None
        following = sources[best_position] if best_position < len(sources) else None
        if (
            boundary["previous_clip_id"] != (previous["clip_id"] if previous else None)
            or boundary["previous_presentation_end_utc"] != (previous["end_utc"] if previous else None)
            or boundary["next_clip_id"] != (following["clip_id"] if following else None)
            or boundary["next_presentation_start_utc"] != (following["start_utc"] if following else None)
        ):
            raise ValueError("joined allocation ledger boundary source identity conflicts")
        if previous is None or following is None:
            decision, reason = ("no_source_before_boundary", "previous_source_absent") if previous is None else ("no_source_after_boundary", "next_source_absent")
            if (
                boundary["signed_gap_nanoseconds"] is not None or boundary["actual_seam_utc"] is not None
                or boundary["boundary_skew_nanoseconds"] is not None
                or boundary["allocation_decision"] != decision or boundary["verdict"] != "absent_source"
                or boundary["reason"] != reason
            ):
                raise ValueError("joined allocation ledger absent boundary conflicts")
        else:
            gap = joined_timestamp_nanoseconds(following["start_utc"], "source start") - joined_timestamp_nanoseconds(previous["end_utc"], "source end")
            if (
                boundary["signed_gap_nanoseconds"] != gap
                or joined_timestamp_nanoseconds(boundary["actual_seam_utc"], "boundary actual seam") != best_ns
                or boundary["boundary_skew_nanoseconds"] != best_ns - scheduled_ns
                or boundary["allocation_decision"] != "split_before_next_source"
                or boundary["verdict"] != "allocated" or boundary["reason"] != "closest_source_boundary"
            ):
                raise ValueError("joined allocation ledger closest boundary conflicts")
        minimum = best_position


def validate_cross_day_boundaries(payload, location, local_date):
    sources = payload["sources"]
    schedules = (
        (local_boundary_nanoseconds(local_date, location, 20, -1), local_boundary_nanoseconds(local_date, location, 8)),
        (local_boundary_nanoseconds(local_date, location, 20), local_boundary_nanoseconds(local_date, location, 8, 1)),
    )
    for index, (boundary, schedule) in enumerate(zip(payload["cross_day_boundaries"], schedules)):
        previous_scheduled = joined_timestamp_nanoseconds(boundary["scheduled_previous_end_utc"], "scheduled previous end")
        next_scheduled = joined_timestamp_nanoseconds(boundary["scheduled_next_start_utc"], "scheduled next start")
        if (previous_scheduled, next_scheduled) != schedule or previous_scheduled >= next_scheduled:
            raise ValueError("joined cross-day boundary schedule conflicts")
        previous_present = boundary["previous_clip_id"] is not None
        next_present = boundary["next_clip_id"] is not None
        if boundary["verdict"] == "absent_source":
            if previous_present and next_present or boundary["signed_gap_nanoseconds"] is not None or boundary["boundary_skew_nanoseconds"] is not None:
                raise ValueError("joined cross-day absent boundary conflicts")
            reason, decision = ("previous_source_absent", "no_previous_day_source")
            if index == 0 and previous_present:
                reason, decision = "next_source_absent", "empty_day_after_previous_source"
            elif index == 1:
                reason, decision = "next_source_absent", "no_next_day_source"
                if next_present:
                    reason, decision = "previous_source_absent", "empty_day_before_next_source"
            if boundary["reason"] != reason or boundary["allocation_decision"] != decision:
                raise ValueError("joined cross-day absence reason conflicts")
        else:
            if not previous_present or not next_present:
                raise ValueError("joined cross-day boundary lacks adjacent sources")
            gap = joined_timestamp_nanoseconds(boundary["next_presentation_start_utc"], "cross-day next start") - joined_timestamp_nanoseconds(boundary["previous_presentation_end_utc"], "cross-day previous end")
            skew = gap - (next_scheduled - previous_scheduled)
            verdict = "overlap" if gap < 0 else "scheduled_gap"
            if (
                boundary["signed_gap_nanoseconds"] != gap or boundary["boundary_skew_nanoseconds"] != skew
                or boundary["verdict"] != verdict or boundary["allocation_decision"] != "separate_local_days"
                or boundary["reason"] != "scheduled_day_boundary"
            ):
                raise ValueError("joined cross-day boundary classification conflicts")
    if sources and (
        payload["cross_day_boundaries"][0]["next_clip_id"] != sources[0]["clip_id"]
        or payload["cross_day_boundaries"][0]["next_presentation_start_utc"] != sources[0]["start_utc"]
        or payload["cross_day_boundaries"][1]["previous_clip_id"] != sources[-1]["clip_id"]
        or payload["cross_day_boundaries"][1]["previous_presentation_end_utc"] != sources[-1]["end_utc"]
    ):
        raise ValueError("joined cross-day boundary does not bind stream-day edges")


def valid_allocation_ledger(payload):
    fields = {
        "schema_version", "batch_id", "generation", "recording_id", "timezone", "local_date",
        "qualification_day", "qualification_sha256", "source_claim_sha256", "source_clip_count", "source_bytes",
        "first_clip_id", "last_clip_id", "consecutive_pairs", "sources", "hours", "hour_source_claim_sha256",
        "cross_hour_boundaries", "cross_day_boundaries", "ledger_sha256",
    }
    exact_joined_fields(payload, fields, "allocation ledger")
    if payload["schema_version"] != 1 or not isinstance(payload["batch_id"], str) or JOINED_BATCH.fullmatch(payload["batch_id"]) is None:
        raise ValueError("joined allocation ledger identity is invalid")
    positive_joined_int(payload["generation"], "generation")
    recording_id = positive_joined_int(payload["recording_id"], "recording_id")
    local_date = valid_joined_date(payload["local_date"])
    valid_qualification_day(payload["qualification_day"], recording_id, payload["timezone"], local_date)
    location = ZoneInfo(payload["timezone"])
    valid_sha256(payload["qualification_sha256"], "qualification")
    source_count = positive_joined_int(payload["source_clip_count"], "source_clip_count", allow_zero=True)
    source_bytes = positive_joined_int(payload["source_bytes"], "source_bytes", allow_zero=True)
    if not isinstance(payload["sources"], list) or len(payload["sources"]) != source_count:
        raise ValueError("joined allocation ledger source count conflicts")
    source_ids, object_ids, calculated_bytes = [], set(), 0
    for source in payload["sources"]:
        source_ids.append(valid_source(source, recording_id, source_only=True))
        if valid_joined_timestamp(source["start_utc"], "source start").astimezone(location).date().isoformat() != local_date or valid_joined_timestamp(source["end_utc"], "source end").astimezone(location).date().isoformat() != local_date:
            raise ValueError("joined allocation ledger source date conflicts")
        object_identity = tuple(source[key] for key in ("provider", "endpoint", "region", "bucket")) + tuple(source["object"].get(key, "") for key in ("key", "version_id", "etag"))
        if object_identity in object_ids:
            raise ValueError("joined allocation ledger duplicates a source object")
        object_ids.add(object_identity)
        calculated_bytes += source["object"]["size_bytes"]
    for previous, following in zip(payload["sources"], payload["sources"][1:]):
        if (joined_timestamp_nanoseconds(following["start_utc"], "source start") , following["clip_id"]) <= (joined_timestamp_nanoseconds(previous["start_utc"], "source start"), previous["clip_id"]):
            raise ValueError("joined allocation ledger source order conflicts")
    if len(source_ids) != len(set(source_ids)) or calculated_bytes != source_bytes:
        raise ValueError("joined allocation ledger source denominator conflicts")
    if (payload["first_clip_id"], payload["last_clip_id"]) != ((source_ids[0], source_ids[-1]) if source_ids else (None, None)):
        raise ValueError("joined allocation ledger edge identity conflicts")
    pair_fields = {
        "previous_clip_id", "next_clip_id", "previous_presentation_end_utc", "next_presentation_start_utc",
        "signed_gap_nanoseconds",
    }
    if not isinstance(payload["consecutive_pairs"], list) or len(payload["consecutive_pairs"]) != max(0, source_count - 1):
        raise ValueError("joined allocation ledger pair count conflicts")
    for index, pair in enumerate(payload["consecutive_pairs"], 1):
        exact_joined_fields(pair, pair_fields, "allocation pair")
        previous, following = payload["sources"][index - 1], payload["sources"][index]
        previous_end = valid_joined_timestamp(pair["previous_presentation_end_utc"], "pair previous end")
        following_start = valid_joined_timestamp(pair["next_presentation_start_utc"], "pair next start")
        gap = joined_timestamp_nanoseconds(pair["next_presentation_start_utc"], "pair next start") - joined_timestamp_nanoseconds(pair["previous_presentation_end_utc"], "pair previous end")
        valid_joined_int64(pair["signed_gap_nanoseconds"], "pair signed gap")
        if pair["previous_clip_id"] != previous["clip_id"] or pair["next_clip_id"] != following["clip_id"] or pair["previous_presentation_end_utc"] != previous["end_utc"] or pair["next_presentation_start_utc"] != following["start_utc"] or pair["signed_gap_nanoseconds"] != gap:
            raise ValueError("joined allocation ledger pair conflicts")
    if not isinstance(payload["hours"], list) or len(payload["hours"]) != 12 or not isinstance(payload["hour_source_claim_sha256"], list) or len(payload["hour_source_claim_sha256"]) != 12:
        raise ValueError("joined allocation ledger must contain 12 hours")
    flat_ids, cursor = [], 0
    for index, hour in enumerate(payload["hours"], 1):
        exact_joined_fields(hour, {"delivery_hour", "clock_hour", "source_clip_ids"}, "allocation hour")
        if hour["delivery_hour"] != index or hour["clock_hour"] != index + 7 or not isinstance(hour["source_clip_ids"], list):
            raise ValueError("joined allocation ledger hour identity conflicts")
        count = len(hour["source_clip_ids"])
        hour_sources = payload["sources"][cursor:cursor + count]
        if hour["source_clip_ids"] != [source["clip_id"] for source in hour_sources] or source_claim_sha(hour_sources) != payload["hour_source_claim_sha256"][index - 1]:
            raise ValueError("joined allocation ledger hour source claim conflicts")
        flat_ids.extend(hour["source_clip_ids"])
        cursor += count
    if flat_ids != source_ids or source_claim_sha(payload["sources"]) != valid_sha256(payload["source_claim_sha256"], "allocation source claim"):
        raise ValueError("joined allocation ledger ordered source claim conflicts")
    if not isinstance(payload["cross_hour_boundaries"], list) or len(payload["cross_hour_boundaries"]) != 11 or not isinstance(payload["cross_day_boundaries"], list) or len(payload["cross_day_boundaries"]) != 2:
        raise ValueError("joined allocation ledger boundary count conflicts")
    for index, boundary in enumerate(payload["cross_hour_boundaries"], 1):
        valid_boundary(boundary)
        if boundary["previous_delivery_hour"] != index:
            raise ValueError("joined allocation ledger boundary order conflicts")
    for boundary in payload["cross_day_boundaries"]:
        valid_boundary(boundary, cross_day=True)
    validate_cross_hour_boundaries(payload, location, local_date)
    validate_cross_day_boundaries(payload, location, local_date)
    ledger_sha = valid_sha256(payload["ledger_sha256"], "allocation ledger")
    unsealed = dict(payload)
    unsealed["ledger_sha256"] = ""
    if joined_canonical_sha(unsealed) != ledger_sha:
        raise ValueError("joined allocation ledger seal conflicts")
    return payload


def recording_ids_sha(recording_ids):
    return hashlib.sha256("".join("%d\n" % recording_id for recording_id in recording_ids).encode("ascii")).hexdigest()


def joined_naming_token(value):
    out = []
    for character in value.strip():
        if character in "-.":
            out.append(character)
        elif character == "_" or character.isspace():
            out.append("_")
        elif character.isascii() and character.isalnum():
            out.append(character)
    return re.sub(r"_+", "_", "".join(out)).strip("_.")


def joined_folder_name(recording_id, metadata, raw_folder):
    plaza_id = metadata["plaza_id"].strip()
    if not plaza_id.isascii() or not plaza_id.isdigit() or int(plaza_id) <= 0:
        raise ValueError("joined frozen plaza identity is invalid")
    for key in ("continent", "country", "city", "plaza_name"):
        if not metadata[key].strip():
            raise ValueError("joined frozen naming metadata is incomplete")
    if raw_folder.strip():
        parts = raw_folder.strip().strip("/").split("/")
        clean = [joined_naming_token(part) for part in parts]
        if not clean or any(part in ("", ".", "..") for part in clean):
            raise ValueError("joined frozen folder is invalid")
        return "/".join(clean)
    return "_".join((
        "%02d" % int(plaza_id), joined_naming_token(metadata["continent"]),
        joined_naming_token(metadata["country"]), joined_naming_token(metadata["city"]),
        joined_naming_token(metadata["plaza_name"]),
    ))


def joined_delivery_path(frozen, media, delivery_hour):
    location = ZoneInfo(frozen["timezone"])
    start = valid_joined_timestamp(media["actual_start_utc"], "delivery start").astimezone(location)
    end = valid_joined_timestamp(media["actual_end_utc"], "delivery end").astimezone(location)
    if end <= start or start.date() != end.date() or not 1 <= delivery_hour <= 12:
        raise ValueError("joined delivery range is invalid")
    ordinal = positive_joined_int(media["part"], "delivery part")
    parts = positive_joined_int(media["parts"], "delivery parts")
    if ordinal > parts or (parts == 1 and ordinal != 1):
        raise ValueError("joined delivery part is invalid")
    month = start.strftime("%B")
    weekday = start.strftime("%A")
    plaza_id = "%02d" % int(frozen["naming_metadata"]["plaza_id"].strip())
    base = "%s_%s_%04d_%s_W%d_%s_hour_%02d" % (
        plaza_id, joined_naming_token(frozen["naming_metadata"]["plaza_name"]), start.year,
        month, (start.day - 1) // 7 + 1, weekday, delivery_hour,
    )
    if parts > 1:
        base += "_part_%02d" % ordinal
    base += "_%s-%s.mp4" % (start.strftime("%H%M%S"), end.strftime("%H%M%S"))
    return "/".join((frozen["folder_name"], month, weekday, base))


def frozen_denominator_sha(ledgers):
    return joined_canonical_sha({
        "projection_version": 1,
        "ledgers": [{
            "recording_id": ledger["recording_id"], "local_date": ledger["local_date"],
            "source_claim_sha256": ledger["source_claim_sha256"],
            "source_count": ledger["source_count"] if "source_count" in ledger else ledger["source_clip_count"],
            "source_bytes": ledger["source_bytes"],
        } for ledger in ledgers],
    })


def valid_batch_index(payload, item=None):
    fields = {
        "schema_version", "policy_version", "allocation_schema_version", "hour_manifest_schema_version",
        "batch_id", "generation", "frozen_at", "batch_generation_sha256", "frozen_denominator_sha256",
        "recording_ids", "recording_ids_sha256", "frozen_recordings", "media_tool", "expected_ledger_count",
        "scheduled_hour_count", "source_clip_count", "source_bytes", "final_media_artifact_count",
        "allocation_ledgers", "hours",
    }
    exact_joined_fields(payload, fields, "batch index")
    if payload["schema_version"] != 1 or payload["policy_version"] != "joined-delivery-v1" or payload["allocation_schema_version"] != 1 or payload["hour_manifest_schema_version"] != 1:
        raise ValueError("joined batch index schema conflicts")
    batch_id = payload["batch_id"]
    if not isinstance(batch_id, str) or JOINED_BATCH.fullmatch(batch_id) is None:
        raise ValueError("joined batch index identity is invalid")
    generation = positive_joined_int(payload["generation"], "generation")
    valid_joined_timestamp(payload["frozen_at"], "frozen_at")
    valid_sha256(payload["frozen_denominator_sha256"], "frozen denominator")
    recording_ids = payload["recording_ids"]
    if not isinstance(recording_ids, list) or not recording_ids or any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in recording_ids) or len(set(recording_ids)) != len(recording_ids):
        raise ValueError("joined batch recording identities are invalid")
    if recording_ids_sha(recording_ids) != payload["recording_ids_sha256"]:
        raise ValueError("joined batch recording identity hash conflicts")
    valid_media_tool(payload["media_tool"])
    frozen_fields = {"recording_id", "priority_ordinal", "eligibility_tier", "eligibility_cutoff", "completed_at", "timezone", "folder_name", "naming_metadata"}
    naming_fields = {"plaza_id", "continent", "country", "city", "plaza_name"}
    if not isinstance(payload["frozen_recordings"], list) or len(payload["frozen_recordings"]) != len(recording_ids):
        raise ValueError("joined frozen recording count conflicts")
    for ordinal, (recording_id, frozen) in enumerate(zip(recording_ids, payload["frozen_recordings"]), 1):
        exact_joined_fields(frozen, frozen_fields, "frozen recording")
        exact_joined_fields(frozen["naming_metadata"], naming_fields, "frozen recording naming")
        if frozen["recording_id"] != recording_id or frozen["priority_ordinal"] != ordinal or frozen["eligibility_tier"] != "good+":
            raise ValueError("joined frozen recording order conflicts")
        cutoff = valid_joined_timestamp(frozen["eligibility_cutoff"], "eligibility_cutoff")
        completed = valid_joined_timestamp(frozen["completed_at"], "completed_at")
        try:
            ZoneInfo(valid_joined_string(frozen["timezone"], "frozen timezone", maximum=255))
        except ZoneInfoNotFoundError as exc:
            raise ValueError("joined frozen timezone is invalid") from exc
        if completed > cutoff:
            raise ValueError("joined frozen recording completed after cutoff")
        for key, value in frozen["naming_metadata"].items():
            valid_joined_string(value, "frozen naming %s" % key)
        if valid_joined_relative_path(frozen["folder_name"]) != joined_folder_name(recording_id, frozen["naming_metadata"], frozen["folder_name"]):
            raise ValueError("joined frozen recording folder conflicts")
    ledgers, hours = payload["allocation_ledgers"], payload["hours"]
    expected_ledger_count = positive_joined_int(payload["expected_ledger_count"], "expected_ledger_count")
    scheduled_hour_count = positive_joined_int(payload["scheduled_hour_count"], "scheduled_hour_count")
    positive_joined_int(payload["source_clip_count"], "source_clip_count", allow_zero=True)
    positive_joined_int(payload["source_bytes"], "source_bytes", allow_zero=True)
    positive_joined_int(payload["final_media_artifact_count"], "final_media_artifact_count", allow_zero=True)
    if not isinstance(ledgers, list) or expected_ledger_count != len(ledgers) or expected_ledger_count != len(recording_ids) * 14 or not isinstance(hours, list) or scheduled_hour_count != len(hours) or scheduled_hour_count != expected_ledger_count * 12:
        raise ValueError("joined batch denominator shape conflicts")
    ledger_fields = {
        "artifact_id", "recording_id", "local_date", "qualification_sha256", "source_claim_sha256",
        "relative_path", "object_key", "size_bytes", "sha256", "ledger_sha256", "source_count",
        "source_bytes", "scheduled_hour_ids",
    }
    seen_artifacts, expected_hours = set(), {}
    source_count = source_bytes = 0
    previous_date = None
    for index, ledger in enumerate(ledgers):
        exact_joined_fields(ledger, ledger_fields, "batch ledger reference")
        recording_id, day_index = recording_ids[index // 14], index % 14
        local_date = valid_joined_date(ledger["local_date"])
        parsed_date = datetime.date.fromisoformat(local_date)
        if ledger["recording_id"] != recording_id or (day_index and parsed_date != previous_date + datetime.timedelta(days=1)):
            raise ValueError("joined batch ledger order conflicts")
        previous_date = parsed_date
        artifact_id = positive_joined_int(ledger["artifact_id"], "ledger artifact_id")
        if artifact_id in seen_artifacts:
            raise ValueError("joined batch artifact identity is duplicated")
        seen_artifacts.add(artifact_id)
        relative = "coverage/ledgers/%d/%s.json" % (recording_id, local_date)
        if ledger["relative_path"] != relative or ledger["object_key"] != "joined/%s/%s" % (batch_id, relative):
            raise ValueError("joined batch ledger path conflicts")
        if positive_joined_int(ledger["size_bytes"], "ledger size_bytes") > JOINED_MANIFEST_MAX_BYTES:
            raise ValueError("joined batch ledger exceeds JSON size cap")
        for key in ("qualification_sha256", "source_claim_sha256", "sha256", "ledger_sha256"):
            valid_sha256(ledger[key], "batch ledger")
        source_count += positive_joined_int(ledger["source_count"], "ledger source_count", allow_zero=True)
        source_bytes += positive_joined_int(ledger["source_bytes"], "ledger source_bytes", allow_zero=True)
        if not isinstance(ledger["scheduled_hour_ids"], list) or len(ledger["scheduled_hour_ids"]) != 12:
            raise ValueError("joined batch ledger hour identities are invalid")
        for delivery_hour, hour_id in enumerate(ledger["scheduled_hour_ids"], 1):
            expected = "%s__recording-%d__date-%s__hour-%02d__generation-%d" % (batch_id, recording_id, local_date, delivery_hour, generation)
            if hour_id != expected or hour_id in expected_hours:
                raise ValueError("joined batch ledger hour identity conflicts")
            expected_hours[hour_id] = ledger
    hour_fields = {
        "hour_manifest_artifact_id", "hour_id", "recording_id", "local_date", "delivery_hour", "status",
        "relative_path", "object_key", "size_bytes", "sha256", "source_count", "source_bytes",
        "media_artifact_count",
    }
    hour_source_count = hour_source_bytes = media_count = 0
    for index, hour in enumerate(hours):
        exact_joined_fields(hour, hour_fields, "batch hour reference")
        ledger = ledgers[index // 12]
        hour_id = hour["hour_id"]
        artifact_id = positive_joined_int(hour["hour_manifest_artifact_id"], "hour manifest artifact_id")
        if artifact_id in seen_artifacts or hour_id != ledger["scheduled_hour_ids"][index % 12] or expected_hours.get(hour_id) is not ledger or hour["delivery_hour"] != index % 12 + 1 or hour["recording_id"] != ledger["recording_id"] or hour["local_date"] != ledger["local_date"]:
            raise ValueError("joined batch hour order conflicts")
        seen_artifacts.add(artifact_id)
        relative = "coverage/hours/%s.json" % hour_id
        if hour["relative_path"] != relative or hour["object_key"] != "joined/%s/%s" % (batch_id, relative):
            raise ValueError("joined batch hour path conflicts")
        if positive_joined_int(hour["size_bytes"], "hour size_bytes") > JOINED_MANIFEST_MAX_BYTES:
            raise ValueError("joined batch hour exceeds JSON size cap")
        valid_sha256(hour["sha256"], "batch hour")
        count = positive_joined_int(hour["source_count"], "hour source_count", allow_zero=True)
        byte_count = positive_joined_int(hour["source_bytes"], "hour source_bytes", allow_zero=True)
        media = positive_joined_int(hour["media_artifact_count"], "media artifact count", allow_zero=True)
        if hour["status"] == "media":
            if count == 0 or media == 0:
                raise ValueError("joined media hour lacks sources or artifacts")
        elif hour["status"] == "gap_only":
            if count or byte_count or media:
                raise ValueError("joined gap-only hour accounts sources")
        elif hour["status"] == "quarantine_only":
            if count == 0 or media:
                raise ValueError("joined quarantine-only hour conflicts")
        else:
            raise ValueError("joined batch hour status is invalid")
        hour_source_count += count
        hour_source_bytes += byte_count
        media_count += media
    if source_count != payload["source_clip_count"] or source_bytes != payload["source_bytes"] or hour_source_count != source_count or hour_source_bytes != source_bytes or media_count != payload["final_media_artifact_count"]:
        raise ValueError("joined batch aggregate denominator conflicts")
    if frozen_denominator_sha(ledgers) != payload["frozen_denominator_sha256"]:
        raise ValueError("joined frozen denominator hash conflicts")
    evidence_ledgers = [{
        "recording_id": ledger["recording_id"], "local_date": ledger["local_date"],
        "qualification_sha256": ledger["qualification_sha256"], "source_claim_sha256": ledger["source_claim_sha256"],
        "ledger_sha256": ledger["ledger_sha256"], "source_count": ledger["source_count"],
        "source_bytes": ledger["source_bytes"],
    } for ledger in ledgers]
    generation_evidence = {
        "schema_version": payload["schema_version"], "policy_version": payload["policy_version"],
        "batch_id": batch_id, "generation": generation, "frozen_at": payload["frozen_at"],
        "frozen_denominator_sha256": payload["frozen_denominator_sha256"],
        "recording_ids_sha256": payload["recording_ids_sha256"], "frozen_recordings": payload["frozen_recordings"],
        "media_tool_identity": payload["media_tool"]["identity_sha256"],
        "expected_ledger_count": expected_ledger_count, "scheduled_hour_count": scheduled_hour_count,
        "source_clip_count": payload["source_clip_count"], "source_bytes": payload["source_bytes"],
        "ledgers": evidence_ledgers,
    }
    if joined_canonical_sha(generation_evidence) != payload["batch_generation_sha256"]:
        raise ValueError("joined batch generation hash conflicts")
    if item is not None and (item["batch_id"] != batch_id or item["relative_path"] != "coverage/batch.json"):
        raise ExistingFileMismatch("joined batch index identity conflicts")
    return payload


def valid_audio_contract(contract):
    fields = {"codec_name", "sample_rate", "channels", "channel_layout", "initial_padding", "skip_samples", "discard_padding", "codec_delay", "trailing_padding"}
    if not isinstance(contract, dict) or set(contract) not in (fields, fields | {"edit_list_kind", "edit_list_sha256"}):
        raise ValueError("joined audio contract has invalid fields")
    valid_joined_string(contract["codec_name"], "audio codec")
    valid_joined_string(contract["channel_layout"], "audio channel layout")
    positive_joined_int(contract["sample_rate"], "audio sample_rate")
    positive_joined_int(contract["channels"], "audio channels")
    for key in ("initial_padding", "skip_samples", "discard_padding", "codec_delay", "trailing_padding"):
        positive_joined_int(contract[key], key, allow_zero=True)
    if "edit_list_sha256" in contract:
        valid_joined_string(contract["edit_list_kind"], "edit list kind")
        valid_sha256(contract["edit_list_sha256"], "edit list")


def valid_media_fingerprint(fingerprint, output):
    base_fields = {"duration_seconds", "tracks"}
    audio_fields = {"audio_sequence_contracts", "effective_audio_bytes", "effective_audio_sample_frames", "effective_audio_sha256"}
    if not isinstance(fingerprint, dict) or set(fingerprint) not in (base_fields, base_fields | audio_fields):
        raise ValueError("joined media fingerprint has invalid fields")
    if (
        isinstance(fingerprint["duration_seconds"], bool)
        or not isinstance(fingerprint["duration_seconds"], (int, float))
        or not math.isfinite(fingerprint["duration_seconds"])
        or fingerprint["duration_seconds"] <= 0
    ):
        raise ValueError("joined media fingerprint has invalid duration")
    tracks = fingerprint["tracks"]
    if not isinstance(tracks, dict) or set(tracks) not in ({"video"}, {"video", "audio"}):
        raise ValueError("joined media fingerprint has invalid tracks")
    track_base = {
        "media_type", "packet_count", "packet_chain_sha256", "packet_timing_sha256", "packet_time_bases",
        "first_packet_pts_seconds", "last_packet_pts_seconds", "first_packet_dts_seconds", "last_packet_dts_seconds",
        "packet_duration_seconds", "decode_timeline_span_seconds", "decoded_frames", "first_timestamp", "last_timestamp",
        "timestamp_status",
    }
    for media_type, track in tracks.items():
        fields = track_base | ({"decoded_samples"} if media_type == "audio" else set())
        exact_joined_fields(track, fields, "media track fingerprint")
        if track["media_type"] != media_type:
            raise ValueError("joined media track identity conflicts")
        for key in ("packet_count", "decoded_frames"):
            positive_joined_int(track[key], "media track %s" % key)
        if media_type == "audio":
            positive_joined_int(track["decoded_samples"], "decoded_samples")
        for key in ("packet_chain_sha256", "packet_timing_sha256"):
            valid_sha256(track[key], "media track")
        if not isinstance(track["packet_time_bases"], list) or not track["packet_time_bases"] or not all(isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*/[1-9][0-9]*", value) for value in track["packet_time_bases"]):
            raise ValueError("joined media track has invalid time base")
        for key in ("first_packet_pts_seconds", "last_packet_pts_seconds", "first_packet_dts_seconds", "last_packet_dts_seconds", "packet_duration_seconds", "decode_timeline_span_seconds"):
            if not isinstance(track[key], str) or re.fullmatch(r"-?[0-9]+(?:/[1-9][0-9]*)?", track[key]) is None:
                raise ValueError("joined media track has invalid rational")
        for key in ("packet_duration_seconds", "decode_timeline_span_seconds"):
            if int(track[key].split("/", 1)[0]) <= 0:
                raise ValueError("joined media track has non-positive duration")
        for key in ("first_timestamp", "last_timestamp"):
            if isinstance(track[key], bool) or not isinstance(track[key], int):
                raise ValueError("joined media track has invalid timestamp")
        if output and (track["timestamp_status"] != "monotonic" or track["decode_timeline_span_seconds"] != track["packet_duration_seconds"]):
            raise ValueError("joined output timestamps are not monotonic")
    if "audio" in tracks:
        if set(fingerprint) != base_fields | audio_fields or not isinstance(fingerprint["audio_sequence_contracts"], list) or not fingerprint["audio_sequence_contracts"] or (output and len(fingerprint["audio_sequence_contracts"]) != 1):
            raise ValueError("joined audio fingerprint lacks contracts")
        for contract in fingerprint["audio_sequence_contracts"]:
            valid_audio_contract(contract)
        positive_joined_int(fingerprint["effective_audio_bytes"], "effective_audio_bytes")
        positive_joined_int(fingerprint["effective_audio_sample_frames"], "effective_audio_sample_frames")
        valid_sha256(fingerprint["effective_audio_sha256"], "effective audio")
    elif set(fingerprint) != base_fields:
        raise ValueError("joined audio evidence exists without an audio track")


def valid_verification(verification):
    fields = {
        "status", "packet_payload_order_status", "decoded_frame_totals_status", "decoded_audio_totals_status",
        "output_timestamp_status", "strict_decode_status", "source_fingerprint", "output_fingerprint",
    }
    exact_joined_fields(verification, fields, "media verification")
    for key in fields - {"source_fingerprint", "output_fingerprint"}:
        if verification[key] != "passed":
            raise ValueError("joined media verification did not pass")
    valid_media_fingerprint(verification["source_fingerprint"], False)
    valid_media_fingerprint(verification["output_fingerprint"], True)
    expected, actual = verification["source_fingerprint"], verification["output_fingerprint"]
    if set(expected["tracks"]) != set(actual["tracks"]) or abs(actual["duration_seconds"] - expected["duration_seconds"]) > 2:
        raise ValueError("joined media fingerprint stream set conflicts")
    for media_type, want in expected["tracks"].items():
        got = actual["tracks"][media_type]
        comparable = (
            "packet_time_bases", "packet_duration_seconds", "first_packet_pts_seconds", "last_packet_pts_seconds",
            "first_packet_dts_seconds", "last_packet_dts_seconds", "packet_count", "packet_chain_sha256",
            "packet_timing_sha256", "decoded_frames",
        ) + (("decoded_samples",) if media_type == "audio" else ())
        if want["timestamp_status"] != "source_clips_independent" or any(want[key] != got[key] for key in comparable):
            raise ValueError("joined media fingerprint sequence conflicts")
    for key in ("effective_audio_bytes", "effective_audio_sample_frames", "effective_audio_sha256"):
        if expected.get(key) != actual.get(key):
            raise ValueError("joined decoded audio fingerprint conflicts")


def valid_hour_manifest(payload, item=None):
    fields = {
        "schema_version", "policy_version", "status", "batch_id", "hour_id", "recording_id", "timezone",
        "local_date", "delivery_hour", "clock_hour", "scheduled_start_utc", "scheduled_end_utc",
        "qualification_day", "qualification_sha256", "allocation", "media_tool", "source_claim_sha256",
        "source_count", "sources", "source_dispositions", "gaps", "scheduled_gap", "quarantine_reason_code",
        "quarantine_evidence", "media",
    }
    exact_joined_fields(payload, fields, "hour manifest")
    batch_id, hour_id = payload["batch_id"], payload["hour_id"]
    if payload["schema_version"] != 1 or payload["policy_version"] != "joined-delivery-v1" or not isinstance(batch_id, str) or JOINED_BATCH.fullmatch(batch_id) is None or not valid_joined_hour_id(batch_id, hour_id):
        raise ValueError("joined hour manifest identity is invalid")
    recording_id = positive_joined_int(payload["recording_id"], "recording_id")
    local_date = valid_joined_date(payload["local_date"])
    delivery_hour = positive_joined_int(payload["delivery_hour"], "delivery_hour")
    if delivery_hour > 12 or payload["clock_hour"] != delivery_hour + 7:
        raise ValueError("joined hour manifest clock identity conflicts")
    expected_hour = "%s__recording-%d__date-%s__hour-%02d__generation-" % (batch_id, recording_id, local_date, delivery_hour)
    if not hour_id.startswith(expected_hour):
        raise ValueError("joined hour manifest hour identity conflicts")
    start = valid_joined_timestamp(payload["scheduled_start_utc"], "scheduled_start_utc")
    end = valid_joined_timestamp(payload["scheduled_end_utc"], "scheduled_end_utc")
    try:
        location = ZoneInfo(payload["timezone"])
    except (TypeError, ZoneInfoNotFoundError) as exc:
        raise ValueError("joined hour timezone is invalid") from exc
    local_start, local_end = start.astimezone(location), end.astimezone(location)
    if (
        end - start != datetime.timedelta(hours=1) or local_start.date().isoformat() != local_date
        or local_end.date().isoformat() != local_date or local_start.hour != delivery_hour + 7
        or local_start.minute or local_start.second or local_start.microsecond
        or local_end.hour != delivery_hour + 8 or local_end.minute or local_end.second or local_end.microsecond
    ):
        raise ValueError("joined hour manifest schedule conflicts")
    valid_qualification_day(payload["qualification_day"], recording_id, payload["timezone"], local_date)
    valid_sha256(payload["qualification_sha256"], "qualification")
    valid_media_tool(payload["media_tool"])
    allocation_fields = {
        "artifact_id", "relative_path", "object_key", "size_bytes", "sha256", "ledger_sha256",
        "hour_source_claim_sha256", "boundaries", "cross_day_boundaries",
    }
    allocation = exact_joined_fields(payload["allocation"], allocation_fields, "hour allocation")
    positive_joined_int(allocation["artifact_id"], "allocation artifact_id")
    want_ledger_path = "coverage/ledgers/%d/%s.json" % (recording_id, local_date)
    if valid_joined_relative_path(allocation["relative_path"], ".json") != want_ledger_path or allocation["object_key"] != "joined/%s/%s" % (batch_id, want_ledger_path):
        raise ValueError("joined hour allocation path conflicts")
    if positive_joined_int(allocation["size_bytes"], "allocation size_bytes") > JOINED_MANIFEST_MAX_BYTES:
        raise ValueError("joined hour allocation exceeds JSON size cap")
    for key in ("sha256", "ledger_sha256", "hour_source_claim_sha256"):
        valid_sha256(allocation[key], "hour allocation")
    if not isinstance(allocation["boundaries"], list) or not isinstance(allocation["cross_day_boundaries"], list):
        raise ValueError("joined hour allocation boundaries are invalid")
    for boundary in allocation["boundaries"]:
        valid_boundary(boundary)
    for boundary in allocation["cross_day_boundaries"]:
        valid_boundary(boundary, cross_day=True)
    source_count = positive_joined_int(payload["source_count"], "source_count", allow_zero=True)
    if not isinstance(payload["sources"], list) or len(payload["sources"]) != source_count:
        raise ValueError("joined hour manifest source count conflicts")
    source_ids = [valid_source(source, recording_id) for source in payload["sources"]]
    valid_derived_source_seams(payload["sources"])
    if len(source_ids) != len(set(source_ids)) or source_claim_sha(payload["sources"]) != payload["source_claim_sha256"] or payload["source_claim_sha256"] != allocation["hour_source_claim_sha256"]:
        raise ValueError("joined hour manifest source claim conflicts")
    disposition_fields = {"clip_id", "disposition", "media_artifact_id", "media_ordinal", "reason_code"}
    if not isinstance(payload["source_dispositions"], list) or len(payload["source_dispositions"]) != source_count:
        raise ValueError("joined hour manifest dispositions conflict")
    included, quarantined = {}, set()
    for clip_id, disposition in zip(source_ids, payload["source_dispositions"]):
        exact_joined_fields(disposition, disposition_fields, "source disposition")
        if disposition["clip_id"] != clip_id:
            raise ValueError("joined source disposition order conflicts")
        if disposition["disposition"] == "included":
            positive_joined_int(disposition["media_artifact_id"], "disposition media_artifact_id")
            positive_joined_int(disposition["media_ordinal"], "disposition media_ordinal")
            if disposition["reason_code"] != "":
                raise ValueError("joined included source has a quarantine reason")
            included[clip_id] = (disposition["media_artifact_id"], disposition["media_ordinal"])
        elif disposition["disposition"] == "quarantined":
            if disposition["media_artifact_id"] != 0 or disposition["media_ordinal"] != 0:
                raise ValueError("joined quarantined source references media")
            valid_joined_reason(disposition["reason_code"], "source quarantine")
            quarantined.add(clip_id)
        else:
            raise ValueError("joined source disposition is invalid")
    gap_fields = {"previous_clip_id", "next_clip_id", "at_utc", "signed_gap_nanoseconds", "reason"}
    if not isinstance(payload["gaps"], list):
        raise ValueError("joined hour gaps are invalid")
    gap_pairs = set()
    gap_reasons = {}
    last_gap_position = -1
    for gap in payload["gaps"]:
        exact_joined_fields(gap, gap_fields, "hour gap")
        positive_joined_int(gap["previous_clip_id"], "gap previous_clip_id")
        positive_joined_int(gap["next_clip_id"], "gap next_clip_id")
        valid_joined_timestamp(gap["at_utc"], "gap at_utc")
        valid_joined_int64(gap["signed_gap_nanoseconds"], "hour gap duration")
        valid_joined_reason(gap["reason"], "hour gap")
        try:
            position = source_ids.index(gap["next_clip_id"])
        except ValueError as exc:
            raise ValueError("joined hour gap source identity conflicts") from exc
        next_seam = payload["sources"][position]["seam_to_previous"] if position else None
        quarantine_boundary = gap["previous_clip_id"] in quarantined or gap["next_clip_id"] in quarantined
        if (
            position == 0 or source_ids[position - 1] != gap["previous_clip_id"]
            or gap["at_utc"] != payload["sources"][position - 1]["end_utc"]
            or gap["signed_gap_nanoseconds"] != next_seam["signed_gap_nanoseconds"]
            or gap["signed_gap_nanoseconds"] != joined_timestamp_nanoseconds(payload["sources"][position]["start_utc"], "gap next start") - joined_timestamp_nanoseconds(payload["sources"][position - 1]["end_utc"], "gap previous end")
            or (gap["reason"] != next_seam["reason"] and not (quarantine_boundary and gap["reason"] == "source_quarantined"))
        ):
            raise ValueError("joined hour gap evidence conflicts")
        pair = (gap["previous_clip_id"], gap["next_clip_id"])
        if pair in gap_pairs:
            raise ValueError("joined hour gap evidence is duplicated")
        if position <= last_gap_position:
            raise ValueError("joined hour gaps are not in source order")
        last_gap_position = position
        gap_pairs.add(pair)
        gap_reasons[pair] = gap["reason"]
    quarantine_fields = {
        "reason_code", "source_clip_ids", "source_claim_sha256", "policy_version", "normalized_failure_facts",
        "failure_sha256", "evidence_sha256", "isolated_attempt_count", "media_tool_identity",
    }
    if not isinstance(payload["quarantine_evidence"], list):
        raise ValueError("joined quarantine evidence is invalid")
    evidenced = []
    for evidence in payload["quarantine_evidence"]:
        exact_joined_fields(evidence, quarantine_fields, "quarantine evidence")
        valid_joined_reason(evidence["reason_code"], "quarantine evidence")
        ids = evidence["source_clip_ids"]
        if not isinstance(ids, list) or not ids or any(isinstance(value, bool) or not isinstance(value, int) or value < 1 for value in ids):
            raise ValueError("joined quarantine evidence sources are invalid")
        subset = [payload["sources"][source_ids.index(clip_id)] for clip_id in ids if clip_id in source_ids]
        if len(subset) != len(ids) or len(set(ids)) != len(ids) or candidate_source_claim_sha(subset) != evidence["source_claim_sha256"]:
            raise ValueError("joined quarantine source claim conflicts")
        if evidence["policy_version"] != payload["policy_version"] or evidence["media_tool_identity"] != payload["media_tool"]["identity_sha256"] or evidence["isolated_attempt_count"] != 2 or not isinstance(evidence["normalized_failure_facts"], dict) or not evidence["normalized_failure_facts"]:
            raise ValueError("joined quarantine evidence conflicts")
        if joined_canonical_sha(evidence["normalized_failure_facts"]) != evidence["failure_sha256"]:
            raise ValueError("joined quarantine failure hash conflicts")
        proof = {
            "source_claim_sha256": evidence["source_claim_sha256"], "reason_code": evidence["reason_code"],
            "failure_sha256": evidence["failure_sha256"], "policy_version": evidence["policy_version"],
            "media_tool_identity": evidence["media_tool_identity"], "repeat_count": evidence["isolated_attempt_count"],
        }
        if joined_canonical_sha(proof) != evidence["evidence_sha256"]:
            raise ValueError("joined quarantine evidence hash conflicts")
        evidenced.extend(ids)
    if set(evidenced) != quarantined or len(evidenced) != len(set(evidenced)):
        raise ValueError("joined quarantine evidence does not exactly cover quarantined sources")
    media_fields = {
        "artifact_id", "ordinal", "part", "parts", "relative_path", "object_key", "content_id", "size_bytes",
        "sha256", "actual_start_utc", "actual_end_utc", "utc_offset_seconds", "media_tool_identity",
        "source_clip_ids", "verification", "maximality_evidence",
    }
    if not isinstance(payload["media"], list):
        raise ValueError("joined hour media is invalid")
    seen_media, media_sources = set(), []
    for ordinal, media in enumerate(payload["media"], 1):
        exact_joined_fields(media, media_fields, "hour media")
        artifact_id = positive_joined_int(media["artifact_id"], "media artifact_id")
        if artifact_id in seen_media or media["ordinal"] != ordinal or media["part"] != ordinal or media["parts"] != len(payload["media"]):
            raise ValueError("joined hour media order conflicts")
        seen_media.add(artifact_id)
        path = valid_joined_relative_path(media["relative_path"], ".mp4")
        if media["object_key"] != "joined/%s/objects/%s.mp4" % (batch_id, media["content_id"]):
            raise ValueError("joined hour media object identity conflicts")
        size_bytes = positive_joined_int(media["size_bytes"], "media size_bytes")
        if size_bytes > JOINED_MAX_BYTES:
            raise ValueError("joined hour media exceeds size cap")
        sha256 = valid_sha256(media["sha256"], "hour media")
        if media["content_id"] != sha256 or media["media_tool_identity"] != payload["media_tool"]["identity_sha256"]:
            raise ValueError("joined hour media content identity conflicts")
        actual_start = valid_joined_timestamp(media["actual_start_utc"], "actual_start_utc")
        actual_end = valid_joined_timestamp(media["actual_end_utc"], "actual_end_utc")
        offset = actual_start.astimezone(location).utcoffset()
        expected_offset = int(offset.total_seconds()) if offset is not None else None
        if (
            actual_end <= actual_start
            or not -86400 < valid_joined_int64(media["utc_offset_seconds"], "media UTC offset") < 86400
            or media["utc_offset_seconds"] != expected_offset
        ):
            raise ValueError("joined hour media timing conflicts")
        ids = media["source_clip_ids"]
        if not isinstance(ids, list) or not ids or len(ids) != len(set(ids)) or any(included.get(clip_id) != (artifact_id, ordinal) for clip_id in ids):
            raise ValueError("joined hour media source membership conflicts")
        positions = [source_ids.index(clip_id) for clip_id in ids]
        if positions != list(range(positions[0], positions[0] + len(positions))):
            raise ValueError("joined hour media crosses a non-member source")
        run_sources = [payload["sources"][position] for position in positions]
        if media["actual_start_utc"] != run_sources[0]["start_utc"] or media["actual_end_utc"] != run_sources[-1]["end_utc"]:
            raise ValueError("joined hour media range conflicts with its exact sources")
        for previous, following in zip(run_sources, run_sources[1:]):
            if (previous["clip_id"], following["clip_id"]) in gap_pairs:
                raise ValueError("joined hour media crosses a gap or non-continuous seam")
        media_sources.extend(ids)
        valid_verification(media["verification"])
        source_fingerprint = media["verification"]["source_fingerprint"]
        audio_contracts = [source["audio_sequence_contract"] for source in run_sources if "audio_sequence_contract" in source]
        if (
            ("audio" not in source_fingerprint["tracks"] and audio_contracts)
            or ("audio" in source_fingerprint["tracks"] and (
                len(audio_contracts) != len(run_sources)
                or audio_contracts != source_fingerprint["audio_sequence_contracts"]
            ))
        ):
            raise ValueError("joined source audio contracts conflict with verification")
        if not isinstance(media["maximality_evidence"], list):
            raise ValueError("joined media maximality evidence is invalid")
        for evidence in media["maximality_evidence"]:
            exact_joined_fields(evidence, {"candidate_clip_ids", "reason_code", "source_claim_sha256", "policy_version", "evidence_sha256", "normalized_failure_facts", "failure_sha256", "repeat_count", "media_tool_identity"}, "maximality evidence")
            valid_joined_reason(evidence["reason_code"], "maximality evidence")
            ids = evidence["candidate_clip_ids"]
            candidates = [payload["sources"][source_ids.index(clip_id)] for clip_id in ids if clip_id in source_ids]
            if not isinstance(ids, list) or not ids or len(candidates) != len(ids) or len(set(ids)) != len(ids) or candidate_source_claim_sha(candidates) != evidence["source_claim_sha256"] or evidence["policy_version"] != payload["policy_version"] or evidence["media_tool_identity"] != payload["media_tool"]["identity_sha256"] or not isinstance(evidence["normalized_failure_facts"], dict) or not evidence["normalized_failure_facts"]:
                raise ValueError("joined maximality evidence conflicts")
            if joined_canonical_sha(evidence["normalized_failure_facts"]) != evidence["failure_sha256"]:
                raise ValueError("joined maximality failure hash conflicts")
            repeat_count = 1 if evidence["reason_code"] == "output_exceeds_put_cap" else 2
            proof = {
                "source_claim_sha256": evidence["source_claim_sha256"], "reason_code": evidence["reason_code"],
                "failure_sha256": evidence["failure_sha256"], "policy_version": evidence["policy_version"],
                "media_tool_identity": evidence["media_tool_identity"], "repeat_count": evidence["repeat_count"],
            }
            if evidence["repeat_count"] != repeat_count or joined_canonical_sha(proof) != evidence["evidence_sha256"]:
                raise ValueError("joined maximality evidence hash conflicts")
        if media["maximality_evidence"]:
            first_length = len(media["maximality_evidence"][0]["candidate_clip_ids"])
            for evidence_index, evidence in enumerate(media["maximality_evidence"]):
                candidate_ids = evidence["candidate_clip_ids"]
                expected_length = first_length - evidence_index
                if (
                    len(candidate_ids) != expected_length or len(candidate_ids) <= len(media["source_clip_ids"])
                    or positions[0] + len(candidate_ids) > len(source_ids)
                    or candidate_ids != source_ids[positions[0]:positions[0] + len(candidate_ids)]
                ):
                    raise ValueError("joined maximality peel order conflicts")
            adjacent = media["maximality_evidence"][-1]["candidate_clip_ids"]
            if adjacent[:-1] != media["source_clip_ids"] or positions[-1] + 1 >= len(source_ids) or adjacent[-1] != source_ids[positions[-1] + 1]:
                raise ValueError("joined maximality evidence is not the immediate source extension")
    if media_sources != [clip_id for clip_id in source_ids if clip_id in included]:
        raise ValueError("joined media does not exactly cover included sources")
    broken = 0
    sources_by_id = {source["clip_id"]: source for source in payload["sources"]}
    media_by_id = {media["artifact_id"]: media for media in payload["media"]}
    for previous, following in zip(source_ids, source_ids[1:]):
        same_media = previous in included and following in included and included[previous][0] == included[following][0]
        has_gap = (previous, following) in gap_pairs
        if same_media == has_gap:
            raise ValueError("joined hour gaps do not exactly cover media run boundaries")
        if same_media:
            if sources_by_id[following]["seam_to_previous"]["verdict"] != "continuous":
                raise ValueError("joined continuous media run seam conflicts")
            continue
        broken += 1
        seam = sources_by_id[following]["seam_to_previous"]
        signed_gap = seam["signed_gap_nanoseconds"]
        if previous in quarantined or following in quarantined:
            if gap_reasons[(previous, following)] != "source_quarantined":
                raise ValueError("joined quarantine boundary reason conflicts")
        elif signed_gap > 0:
            if seam["verdict"] != "gap":
                raise ValueError("joined signed gap verdict conflicts")
        elif signed_gap < 0:
            if seam["verdict"] != "overlap":
                raise ValueError("joined signed overlap verdict conflicts")
        else:
            previous_media = media_by_id.get(included.get(previous, (0, 0))[0])
            evidence = previous_media["maximality_evidence"][-1] if previous_media and previous_media["maximality_evidence"] else None
            if (
                seam["verdict"] != "incompatible" or evidence is None
                or previous_media["source_clip_ids"][-1] != previous
                or evidence["reason_code"] != seam["reason"]
                or evidence["candidate_clip_ids"][-1] != following
            ):
                raise ValueError("joined zero-duration split lacks typed maximality evidence")
    if broken != len(gap_pairs):
        raise ValueError("joined hour gap count conflicts with media runs")
    status = payload["status"]
    if status == "media":
        if not source_ids or not payload["media"] or payload["scheduled_gap"] is not None or payload["quarantine_reason_code"] != "":
            raise ValueError("joined media hour terminal state conflicts")
    elif status == "gap_only":
        exact_joined_fields(payload["scheduled_gap"], {"reason_code", "signed_gap_nanoseconds", "no_allocatable_sources"}, "scheduled gap")
        if source_ids or payload["media"] or payload["quarantine_evidence"] or payload["quarantine_reason_code"] != "" or payload["scheduled_gap"]["no_allocatable_sources"] is not True:
            raise ValueError("joined gap-only hour terminal state conflicts")
        valid_joined_reason(payload["scheduled_gap"]["reason_code"], "scheduled gap")
        if payload["scheduled_gap"]["signed_gap_nanoseconds"] != 3_600_000_000_000:
            raise ValueError("joined scheduled gap duration conflicts")
    elif status == "quarantine_only":
        valid_joined_reason(payload["quarantine_reason_code"], "quarantine-only hour")
        if not source_ids or payload["media"] or payload["scheduled_gap"] is not None or not payload["quarantine_evidence"] or set(source_ids) != quarantined:
            raise ValueError("joined quarantine-only hour terminal state conflicts")
    else:
        raise ValueError("joined hour manifest has unknown status")
    if item is not None:
        if batch_id != item["batch_id"] or hour_id != item["hour_id"]:
            raise ExistingFileMismatch("joined hour manifest identity conflicts")
        if item["kind"] == "media":
            matches = [media for media in payload["media"] if media["artifact_id"] == item["id"]]
            if len(matches) != 1 or matches[0]["relative_path"] != item["relative_path"] or matches[0]["size_bytes"] != item["size_bytes"] or matches[0]["sha256"] != item["sha256"]:
                raise ExistingFileMismatch("joined media conflicts with sealed hour manifest")
    return payload


def read_joined_content(cfg, runtime, directory_fd, name, limit, stop_event):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=directory_fd)
    content = bytearray()
    digest = hashlib.sha256()
    try:
        before = os.fstat(descriptor)
        if not stat_module.S_ISREG(before.st_mode) or before.st_nlink not in (1, 2):
            raise ExistingFileMismatch("joined hour manifest has an unknown hardlink")
        while True:
            if stop_event.is_set() or poll_raw_pending(cfg, runtime):
                raise JoinedDownloadYield("joined manifest validation yielded to raw clip delivery")
            chunk = os.read(descriptor, min(JOINED_RANGE_BYTES, limit + 1 - len(content)))
            if not chunk:
                break
            content.extend(chunk)
            digest.update(chunk)
            if len(content) > limit:
                raise ValueError("joined JSON artifact is too large")
        after = os.fstat(descriptor)
        path_after = joined_entry_stat(directory_fd, name)
        if path_after is None or certification_identity(before) != certification_identity(after) or certification_identity(after) != certification_identity(path_after):
            raise FileChangedDuringHash("joined hour manifest changed while reading")
    finally:
        os.close(descriptor)
    return bytes(content), digest.hexdigest()


def read_joined_manifest(cfg, runtime, directory_fd, name, expected_sha, item, stop_event):
    content, digest = read_joined_content(
        cfg, runtime, directory_fd, name, JOINED_MANIFEST_MAX_BYTES, stop_event,
    )
    if digest != expected_sha:
        raise ExistingFileMismatch("joined hour manifest checksum conflicts")
    return valid_hour_manifest(decode_joined_json(content), item)


def read_joined_json_path(cfg, runtime, batch_id, relative_path, expected_size, expected_sha, stop_event):
    holder = {"batch_id": batch_id, "relative_path": relative_path}
    directory_fd = open_joined_output_dir(cfg, holder, create=False)
    try:
        content, digest = read_joined_content(
            cfg, runtime, directory_fd, Path(relative_path).name, JOINED_MANIFEST_MAX_BYTES, stop_event,
        )
    finally:
        os.close(directory_fd)
    if len(content) != expected_size or digest != expected_sha:
        raise ExistingFileMismatch("joined JSON reference conflicts: %s" % relative_path)
    return decode_joined_json(content)


def expected_hour_boundaries(ledger, delivery_hour):
    boundaries = []
    cross_day = []
    if delivery_hour > 1:
        boundaries.append(ledger["cross_hour_boundaries"][delivery_hour - 2])
    if delivery_hour < 12:
        boundaries.append(ledger["cross_hour_boundaries"][delivery_hour - 1])
    if delivery_hour == 1:
        cross_day.append(ledger["cross_day_boundaries"][0])
    if delivery_hour == 12:
        cross_day.append(ledger["cross_day_boundaries"][1])
    return boundaries, cross_day


def validate_hour_against_ledger(manifest, ledger, ledger_artifact_id, ledger_relative_path, ledger_size, ledger_sha):
    allocation = manifest["allocation"]
    if (
        ledger["batch_id"] != manifest["batch_id"] or ledger["recording_id"] != manifest["recording_id"]
        or ledger["local_date"] != manifest["local_date"] or ledger["qualification_sha256"] != manifest["qualification_sha256"]
        or ledger["qualification_day"] != manifest["qualification_day"]
        or allocation["artifact_id"] != ledger_artifact_id or allocation["relative_path"] != ledger_relative_path
        or allocation["size_bytes"] != ledger_size or allocation["sha256"] != ledger_sha
        or allocation["ledger_sha256"] != ledger["ledger_sha256"]
    ):
        raise ExistingFileMismatch("joined hour allocation conflicts with installed ledger")
    delivery_hour = manifest["delivery_hour"]
    hour = ledger["hours"][delivery_hour - 1]
    ledger_sources = []
    by_id = {source["clip_id"]: source for source in ledger["sources"]}
    for clip_id in hour["source_clip_ids"]:
        if clip_id not in by_id:
            raise ExistingFileMismatch("joined hour source is absent from installed ledger")
        ledger_sources.append(by_id[clip_id])
    if (
        [source["clip_id"] for source in manifest["sources"]] != hour["source_clip_ids"]
        or [source_claim_projection(source) for source in manifest["sources"]] != [source_claim_projection(source) for source in ledger_sources]
        or manifest["source_claim_sha256"] != ledger["hour_source_claim_sha256"][delivery_hour - 1]
        or allocation["hour_source_claim_sha256"] != manifest["source_claim_sha256"]
    ):
        raise ExistingFileMismatch("joined hour source claim conflicts with installed ledger")
    boundaries, cross_day = expected_hour_boundaries(ledger, delivery_hour)
    if allocation["boundaries"] != boundaries or allocation["cross_day_boundaries"] != cross_day:
        raise ExistingFileMismatch("joined hour boundary evidence conflicts with installed ledger")


def validate_cross_day_ledger_link(previous, following):
    if any(previous[key] != following[key] for key in ("batch_id", "generation", "recording_id", "timezone")):
        raise ExistingFileMismatch("joined consecutive ledger scope conflicts")
    fields = (
        "previous_clip_id", "next_clip_id", "previous_presentation_end_utc", "next_presentation_start_utc",
        "signed_gap_nanoseconds", "scheduled_previous_end_utc", "scheduled_next_start_utc",
        "boundary_skew_nanoseconds", "verdict",
    )
    previous_right = previous["cross_day_boundaries"][1]
    following_left = following["cross_day_boundaries"][0]
    if any(previous_right[field] != following_left[field] for field in fields):
        raise ExistingFileMismatch("joined consecutive ledgers disagree on their shared day boundary")


def validate_hour_ledger_binding(cfg, runtime, item, manifest, stop_event):
    if item["kind"] != "hour_manifest":
        return
    allocation = manifest["allocation"]
    if (
        allocation["artifact_id"] != item["ledger_artifact_id"]
        or allocation["relative_path"] != item["ledger_relative_path"]
        or allocation["sha256"] != item["ledger_sha256"]
    ):
        raise ExistingFileMismatch("joined hour feed lineage conflicts with sealed manifest")
    ledger = read_joined_json_path(
        cfg, runtime, item["batch_id"], item["ledger_relative_path"],
        allocation["size_bytes"], item["ledger_sha256"], stop_event,
    )
    valid_allocation_ledger(ledger)
    validate_hour_against_ledger(
        manifest, ledger, item["ledger_artifact_id"], item["ledger_relative_path"],
        allocation["size_bytes"], item["ledger_sha256"],
    )


def verify_joined_relative_file(cfg, runtime, batch_id, relative_path, size_bytes, sha256, stop_event):
    holder = {"batch_id": batch_id, "relative_path": relative_path}
    directory_fd = open_joined_output_dir(cfg, holder, create=False)
    try:
        if not verify_joined_entry(
            cfg, runtime, directory_fd, Path(relative_path).name, size_bytes, sha256, stop_event,
        ):
            raise ExistingFileMismatch("joined referenced file is missing: %s" % relative_path)
    finally:
        os.close(directory_fd)


def validate_batch_index_proof(cfg, runtime, index, stop_event):
    batch_id = index["batch_id"]
    total_sources = total_bytes = total_media = 0
    seen_media_artifacts = {
        reference["artifact_id"] for reference in index["allocation_ledgers"]
    } | {
        reference["hour_manifest_artifact_id"] for reference in index["hours"]
    }
    seen_media_paths = set()
    denominator_ledgers = []
    previous_ledger = None
    # The compact index is ordered ledger-major and each ledger is immediately
    # followed logically by its exact 12 hour references. Never scan joined/.
    for ledger_position, ledger_ref in enumerate(index["allocation_ledgers"]):
        frozen = index["frozen_recordings"][ledger_position // 14]
        ledger = read_joined_json_path(
            cfg, runtime, batch_id, ledger_ref["relative_path"], ledger_ref["size_bytes"], ledger_ref["sha256"], stop_event,
        )
        valid_allocation_ledger(ledger)
        if (
            ledger["batch_id"] != batch_id or ledger["recording_id"] != ledger_ref["recording_id"]
            or ledger["local_date"] != ledger_ref["local_date"] or ledger["qualification_sha256"] != ledger_ref["qualification_sha256"]
            or ledger["source_claim_sha256"] != ledger_ref["source_claim_sha256"] or ledger["ledger_sha256"] != ledger_ref["ledger_sha256"]
            or ledger["source_clip_count"] != ledger_ref["source_count"] or ledger["source_bytes"] != ledger_ref["source_bytes"]
            or ledger["generation"] != index["generation"] or ledger["timezone"] != frozen["timezone"]
        ):
            raise ExistingFileMismatch("joined batch ledger reference conflicts")
        if previous_ledger is not None and previous_ledger["recording_id"] == ledger["recording_id"]:
            validate_cross_day_ledger_link(previous_ledger, ledger)
        previous_ledger = ledger
        denominator_ledgers.append(ledger)
        day_dispositions = []
        hour_refs = index["hours"][ledger_position * 12:(ledger_position + 1) * 12]
        for delivery_hour, hour_ref in enumerate(hour_refs, 1):
            manifest = read_joined_json_path(
                cfg, runtime, batch_id, hour_ref["relative_path"], hour_ref["size_bytes"], hour_ref["sha256"], stop_event,
            )
            valid_hour_manifest(manifest)
            if (
                manifest["hour_id"] != hour_ref["hour_id"] or manifest["recording_id"] != hour_ref["recording_id"]
                or manifest["local_date"] != hour_ref["local_date"] or manifest["delivery_hour"] != delivery_hour
                or manifest["status"] != hour_ref["status"] or manifest["source_count"] != hour_ref["source_count"]
                or sum(source["object"]["size_bytes"] for source in manifest["sources"]) != hour_ref["source_bytes"]
                or len(manifest["media"]) != hour_ref["media_artifact_count"]
                or manifest["timezone"] != frozen["timezone"] or manifest["media_tool"] != index["media_tool"]
            ):
                raise ExistingFileMismatch("joined batch hour reference conflicts")
            validate_hour_against_ledger(
                manifest, ledger, ledger_ref["artifact_id"], ledger_ref["relative_path"],
                ledger_ref["size_bytes"], ledger_ref["sha256"],
            )
            day_dispositions.extend(disposition["clip_id"] for disposition in manifest["source_dispositions"])
            for media in manifest["media"]:
                if media["relative_path"] != joined_delivery_path(frozen, media, delivery_hour):
                    raise ExistingFileMismatch("joined batch media delivery path conflicts with frozen naming")
                if media["artifact_id"] in seen_media_artifacts or media["relative_path"] in seen_media_paths:
                    raise ExistingFileMismatch("joined batch media reference is duplicated")
                seen_media_artifacts.add(media["artifact_id"])
                seen_media_paths.add(media["relative_path"])
                verify_joined_relative_file(
                    cfg, runtime, batch_id, media["relative_path"], media["size_bytes"], media["sha256"], stop_event,
                )
                total_media += 1
        if day_dispositions != [source["clip_id"] for source in ledger["sources"]]:
            raise ExistingFileMismatch("joined batch day disposition union conflicts with ledger")
        total_sources += ledger["source_clip_count"]
        total_bytes += ledger["source_bytes"]
    if total_sources != index["source_clip_count"] or total_bytes != index["source_bytes"] or total_media != index["final_media_artifact_count"] or frozen_denominator_sha(denominator_ledgers) != index["frozen_denominator_sha256"]:
        raise ExistingFileMismatch("joined batch installed denominator conflicts")


def validate_joined_artifact(cfg, runtime, directory_fd, name, item, stop_event):
    if item["kind"] == "media":
        return
    content, digest = read_joined_content(
        cfg, runtime, directory_fd, name, JOINED_MANIFEST_MAX_BYTES, stop_event,
    )
    if digest != item["sha256"]:
        raise ExistingFileMismatch("joined JSON artifact checksum conflicts")
    payload = decode_joined_json(content)
    if item["kind"] == "allocation_ledger":
        valid_allocation_ledger(payload)
        if payload["batch_id"] != item["batch_id"] or item["relative_path"] != "coverage/ledgers/%d/%s.json" % (payload["recording_id"], payload["local_date"]):
            raise ExistingFileMismatch("joined allocation ledger identity conflicts")
    elif item["kind"] == "hour_manifest":
        valid_hour_manifest(payload, item)
        validate_hour_ledger_binding(cfg, runtime, item, payload, stop_event)
    elif item["kind"] == "batch_index":
        valid_batch_index(payload, item)
        validate_batch_index_proof(cfg, runtime, payload, stop_event)


def validate_media_manifest_binding(cfg, runtime, item, stop_event):
    if item["kind"] != "media":
        return
    manifest_item = {
        "batch_id": item["batch_id"],
        "relative_path": item["hour_manifest_relative_path"],
    }
    directory_fd = open_joined_output_dir(cfg, manifest_item, create=False)
    try:
        read_joined_manifest(
            cfg, runtime, directory_fd, Path(item["hour_manifest_relative_path"]).name,
            item["hour_manifest_sha256"], item, stop_event,
        )
    finally:
        os.close(directory_fd)


def write_joined_stage(directory_fd, name, content):
    flags = (
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(name, flags, 0o600, dir_fd=directory_fd)
    try:
        current = os.fstat(descriptor)
        if not stat_module.S_ISREG(current.st_mode) or current.st_nlink != 1:
            raise ExistingFileMismatch("joined sidecar stage is not exclusively owned")
        offset = 0
        while offset < len(content):
            offset += os.write(descriptor, content[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.fsync(directory_fd)


def linked_joined_stage(directory_fd, marker_name, marker_stat):
    prefix = marker_name + ".stage-"
    matches = []
    for candidate in os.listdir(directory_fd):
        if not candidate.startswith(prefix) or not re.fullmatch(r"[0-9a-f]{32}", candidate[len(prefix):]):
            continue
        try:
            candidate_stat = os.stat(candidate, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            continue
        if stat_module.S_ISREG(candidate_stat.st_mode) and same_joined_inode(marker_stat, candidate_stat):
            matches.append((candidate, candidate_stat))
    if len(matches) != 1:
        raise ExistingFileMismatch("joined marker has an unknown hardlink")
    stage_name, stage_stat = matches[0]
    if marker_stat.st_nlink != 2 or stage_stat.st_nlink != 2:
        raise ExistingFileMismatch("joined sidecar staging link count is invalid")
    return stage_name


def publish_joined_sidecar(cfg, runtime, directory_fd, name, content, stop_event):
    marker_stat = joined_entry_stat(directory_fd, name)
    if marker_stat is not None:
        if not verify_joined_sidecar(cfg, runtime, directory_fd, name, content, stop_event):
            raise ExistingFileMismatch("joined partial ownership marker conflicts")
        marker_stat = joined_entry_stat(directory_fd, name)
        if marker_stat.st_nlink == 2:
            stage_name = linked_joined_stage(directory_fd, name, marker_stat)
            os.unlink(stage_name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        elif marker_stat.st_nlink != 1:
            raise ExistingFileMismatch("joined marker has an unknown hardlink")
        if joined_entry_stat(directory_fd, name).st_nlink != 1:
            raise ExistingFileMismatch("joined marker has an unknown hardlink")
        return
    for _attempt in range(8):
        stage_name = name + ".stage-" + os.urandom(16).hex()
        try:
            write_joined_stage(directory_fd, stage_name, content)
            break
        except FileExistsError:
            continue
    else:
        raise ExistingFileMismatch("could not allocate a private joined sidecar stage")
    try:
        os.link(stage_name, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd, follow_symlinks=False)
    except FileExistsError:
        # Another publisher won. Our private, unlinked stage is left untouched;
        # only an exact same-inode link is ever removed below or on restart.
        raise ExistingFileMismatch("joined marker appeared during publication")
    os.fsync(directory_fd)
    marker_stat = joined_entry_stat(directory_fd, name)
    stage_stat = joined_entry_stat(directory_fd, stage_name)
    if marker_stat.st_nlink != 2 or stage_stat.st_nlink != 2 or not same_joined_inode(marker_stat, stage_stat):
        raise ExistingFileMismatch("joined sidecar staging link count is invalid")
    if not verify_joined_sidecar(cfg, runtime, directory_fd, name, content, stop_event):
        raise ExistingFileMismatch("joined sidecar stage changed during publication")
    os.unlink(stage_name, dir_fd=directory_fd)
    os.fsync(directory_fd)
    if joined_entry_stat(directory_fd, name).st_nlink != 1:
        raise ExistingFileMismatch("joined marker has an unknown hardlink")


def same_joined_inode(left, right):
    return left is not None and right is not None and (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def ensure_owned_joined_partial(cfg, runtime, directory_fd, part_name, marker_name, sidecar, stop_event):
    part_stat = joined_entry_stat(directory_fd, part_name)
    marker_stat = joined_entry_stat(directory_fd, marker_name)
    if marker_stat is None:
        if part_stat is not None:
            raise ExistingFileMismatch("joined partial has no ownership marker")
        publish_joined_sidecar(cfg, runtime, directory_fd, marker_name, sidecar, stop_event)
        marker_stat = joined_entry_stat(directory_fd, marker_name)
    elif not verify_joined_sidecar(cfg, runtime, directory_fd, marker_name, sidecar, stop_event):
        raise ExistingFileMismatch("joined partial ownership marker conflicts")
    marker_stat = joined_entry_stat(directory_fd, marker_name)
    if part_stat is None:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(part_name, flags, 0o600, dir_fd=directory_fd)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.fsync(directory_fd)
        part_stat = joined_entry_stat(directory_fd, part_name)
    if part_stat.st_nlink != 1 or marker_stat.st_nlink != 1:
        raise ExistingFileMismatch("joined partial or marker has an unknown hardlink")
    return part_stat


def truncate_joined_part(directory_fd, name):
    flags = os.O_WRONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(name, flags, dir_fd=directory_fd)
    try:
        current = os.fstat(descriptor)
        if not stat_module.S_ISREG(current.st_mode) or current.st_nlink != 1:
            raise ExistingFileMismatch("joined partial is not exclusively owned")
        os.ftruncate(descriptor, 0)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def poll_raw_pending(cfg, runtime):
    page = request_json(
        cfg, "GET", "/account/clips?after_id=%d&limit=%d" % (runtime.cursor_id, LIST_PAGE_LIMIT)
    )
    clips = page.get("clips", [])
    if not isinstance(clips, list):
        raise RuntimeError("clips response is not a list")
    return bool(clips)


def prepare_joined_download(cfg, item):
    prepared = request_json(cfg, "GET", item["download_path"], base=cfg.origin)
    if set(prepared) != {
        "url", "etag", "if_match", "version_id", "size_bytes", "sha256", "content_type", "expires_in_sec",
    }:
        raise RuntimeError("joined download response has invalid fields")
    url = prepared.get("url")
    if not isinstance(url, str):
        raise RuntimeError("joined download returned invalid URL")
    parsed = urllib.parse.urlsplit(url)
    try:
        parsed.port
    except ValueError as exc:
        raise RuntimeError("joined download returned invalid URL") from exc
    if (
        parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
        or parsed.password is not None or parsed.fragment or not parsed.path.startswith("/")
    ):
        raise RuntimeError("joined download returned invalid URL")
    if prepared.get("size_bytes") != item["size_bytes"] or prepared.get("sha256") != item["sha256"]:
        raise ExistingFileMismatch("joined prepared bytes changed")
    if prepared.get("content_type") != item["content_type"]:
        raise ExistingFileMismatch("joined prepared content type changed")
    expires_in_sec = prepared.get("expires_in_sec")
    if isinstance(expires_in_sec, bool) or not isinstance(expires_in_sec, int) or not 1 <= expires_in_sec <= 3600:
        raise RuntimeError("joined download returned invalid expiry")
    etag = normalized_etag(prepared.get("etag"))
    version_id = prepared.get("version_id")
    if (
        not isinstance(version_id, str) or len(version_id) > 1024 or version_id != version_id.strip()
        or any(ord(ch) < 32 or ord(ch) == 127 for ch in version_id)
    ):
        raise RuntimeError("joined download returned invalid version_id")
    if_match = prepared.get("if_match")
    if not isinstance(if_match, str):
        raise ExistingFileMismatch("joined prepared If-Match changed")
    if if_match != '"%s"' % etag:
        raise ExistingFileMismatch("joined prepared If-Match changed")
    return {
        "url": url, "if_match": if_match, "etag": etag, "version_id": version_id,
        "url_scheme": parsed.scheme, "url_authority": parsed.netloc, "url_path": parsed.path,
    }


def validate_joined_download_renewal(first, current):
    if any(current[key] != first[key] for key in (
        "etag", "version_id", "url_scheme", "url_authority", "url_path",
    )):
        raise ExistingFileMismatch("joined prepared object identity changed")


def append_joined_range(prepared, directory_fd, part_name, item, start, end):
    headers = {
        "User-Agent": USER_AGENT,
        "Range": "bytes=%d-%d" % (start, end),
        "If-Match": prepared["if_match"],
    }
    request = urllib.request.Request(prepared["url"], method="GET", headers=headers)
    try:
        response_context = open_joined_url(request)
    except urllib.error.HTTPError as exc:
        if exc.code == 416:
            truncate_joined_part(directory_fd, part_name)
            raise RuntimeError("joined range was unsatisfiable; partial restarted") from exc
        raise
    with response_context as response:
        status = getattr(response, "status", None) or response.getcode()
        if status == 200:
            truncate_joined_part(directory_fd, part_name)
            raise RuntimeError("joined server ignored range; partial restarted")
        if status != 206:
            raise RuntimeError("joined range returned HTTP %s" % status)
        response_etag = normalized_etag(response.headers.get("ETag"))
        response_version = str(response.headers.get("x-amz-version-id") or "")
        if response_etag != prepared["etag"] or response_version != prepared["version_id"]:
            truncate_joined_part(directory_fd, part_name)
            raise ExistingFileMismatch("joined range object identity drifted; partial restarted")
        expected_range = "bytes %d-%d/%d" % (start, end, item["size_bytes"])
        if response.headers.get("Content-Range") != expected_range:
            raise RuntimeError("joined range returned invalid Content-Range")
        chunk_bytes = end - start + 1
        try:
            content_length = int(response.headers.get("Content-Length", ""))
        except ValueError as exc:
            raise RuntimeError("joined range returned invalid Content-Length") from exc
        if content_length != chunk_bytes:
            raise RuntimeError("joined range returned invalid Content-Length")
        flags = os.O_WRONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(part_name, flags, dir_fd=directory_fd)
        written = 0
        try:
            opened = os.fstat(descriptor)
            if not stat_module.S_ISREG(opened.st_mode) or opened.st_nlink != 1 or opened.st_size != start:
                raise ExistingFileMismatch("joined partial changed before range append")
            os.lseek(descriptor, start, os.SEEK_SET)
            while written < chunk_bytes:
                block = response.read(min(1024 * 1024, chunk_bytes - written))
                if not block:
                    break
                offset = 0
                while offset < len(block):
                    offset += os.write(descriptor, block[offset:])
                written += len(block)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if written != chunk_bytes or response.read(1):
            raise RuntimeError("joined range body length mismatch")


def remove_owned_joined_transfer(cfg, runtime, directory_fd, part_name, marker_name, final_name, marker, stop_event):
    part_stat = joined_entry_stat(directory_fd, part_name)
    marker_stat = joined_entry_stat(directory_fd, marker_name)
    final_stat = joined_entry_stat(directory_fd, final_name)
    if part_stat is not None:
        if part_stat.st_nlink != 2 or final_stat.st_nlink != 2 or not same_joined_inode(part_stat, final_stat):
            raise ExistingFileMismatch("joined partial is not the published final")
        # Only client-owned transfer scratch is disposable. Raw media, finals,
        # unknown partials, and conflicting paths are never unlinked.
        os.unlink(part_name, dir_fd=directory_fd)
    if marker_stat is not None:
        if marker_stat.st_nlink != 1 or not verify_joined_sidecar(
            cfg, runtime, directory_fd, marker_name, marker, stop_event,
        ):
            raise ExistingFileMismatch("joined transfer marker conflicts")
        os.unlink(marker_name, dir_fd=directory_fd)
    os.fsync(directory_fd)
    if joined_entry_stat(directory_fd, final_name).st_nlink != 1:
        raise ExistingFileMismatch("joined published link count is invalid")


def complete_existing_joined(cfg, runtime, directory_fd, item, names, marker, stop_event):
    final_name, part_name, marker_name = names
    final_stat = joined_entry_stat(directory_fd, final_name)
    if final_stat is None:
        return False
    if not verify_joined_entry(
        cfg, runtime, directory_fd, final_name, item["size_bytes"], item["sha256"], stop_event,
    ):
        raise ExistingFileMismatch("joined final disappeared")
    final_stat = joined_entry_stat(directory_fd, final_name)
    part_stat = joined_entry_stat(directory_fd, part_name)
    marker_stat = joined_entry_stat(directory_fd, marker_name)
    if part_stat is None:
        if final_stat.st_nlink != 1:
            raise ExistingFileMismatch("joined final has an unknown hardlink")
    elif final_stat.st_nlink != 2 or part_stat.st_nlink != 2 or not same_joined_inode(final_stat, part_stat):
        raise ExistingFileMismatch("joined partial conflicts with final")
    if marker_stat is not None and not verify_joined_sidecar(
        cfg, runtime, directory_fd, marker_name, marker, stop_event,
    ):
        raise ExistingFileMismatch("joined partial ownership marker conflicts")
    if part_stat is not None and marker_stat is None:
        raise ExistingFileMismatch("joined published partial has no ownership marker")
    validate_joined_artifact(cfg, runtime, directory_fd, final_name, item, stop_event)
    remove_owned_joined_transfer(
        cfg, runtime, directory_fd, part_name, marker_name, final_name, marker, stop_event,
    )
    return True


def download_joined_item(cfg, runtime, item, stop_event):
    ensure_joined_dependency_ack(cfg, runtime, item, stop_event)
    validate_media_manifest_binding(cfg, runtime, item, stop_event)
    directory_fd = open_joined_output_dir(cfg, item)
    final_name = Path(item["relative_path"]).name
    part_name = ".%s.joined-%d.part" % (final_name, item["id"])
    marker_name = part_name + ".stoarama.json"
    names = (final_name, part_name, marker_name)
    try:
        final_stat = joined_entry_stat(directory_fd, final_name)
        part_stat = joined_entry_stat(directory_fd, part_name)
        marker_stat = joined_entry_stat(directory_fd, marker_name)
        if final_stat is not None and part_stat is None and marker_stat is None and complete_existing_joined(
            cfg, runtime, directory_fd, item, names, None, stop_event,
        ):
            return False
        prepared = prepare_joined_download(cfg, item)
        marker = joined_transfer_marker_bytes(item, prepared)
        if complete_existing_joined(cfg, runtime, directory_fd, item, names, marker, stop_event):
            return False
        part_stat = ensure_owned_joined_partial(
            cfg, runtime, directory_fd, part_name, marker_name, marker, stop_event,
        )
        if part_stat.st_nlink != 1:
            raise ExistingFileMismatch("joined partial has an unknown hardlink")
        part_size = part_stat.st_size
        if part_size > item["size_bytes"]:
            truncate_joined_part(directory_fd, part_name)
            part_size = 0
        remaining = item["size_bytes"] - part_size
        require_storage_capacity(cfg, runtime, remaining)
        try:
            while part_size < item["size_bytes"]:
                if stop_event.is_set() or poll_raw_pending(cfg, runtime):
                    raise JoinedDownloadYield("joined download yielded to raw clips")
                try:
                    current_prepared = prepare_joined_download(cfg, item)
                except ExistingFileMismatch:
                    truncate_joined_part(directory_fd, part_name)
                    raise
                validate_joined_download_renewal(prepared, current_prepared)
                end = min(item["size_bytes"], part_size + JOINED_RANGE_BYTES) - 1
                append_joined_range(current_prepared, directory_fd, part_name, item, part_size, end)
                part_size = end + 1
            try:
                verify_joined_entry(
                    cfg, runtime, directory_fd, part_name, item["size_bytes"], item["sha256"], stop_event,
                )
            except ExistingFileMismatch:
                truncate_joined_part(directory_fd, part_name)
                raise RuntimeError("joined download checksum mismatch; partial restarted")
            validate_joined_artifact(cfg, runtime, directory_fd, part_name, item, stop_event)
            os.link(part_name, final_name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd, follow_symlinks=False)
            os.fsync(directory_fd)
            if not complete_existing_joined(cfg, runtime, directory_fd, item, names, marker, stop_event):
                raise RuntimeError("joined publication disappeared")
            return True
        finally:
            runtime.release_storage_reservation(remaining)
    finally:
        os.close(directory_fd)


def joined_ack_receipt_path(cfg, connection_id):
    scope = hashlib.sha256((cfg.origin + "\0" + str(connection_id)).encode("utf-8")).hexdigest()[:16]
    return cfg.state_dir / ("joined-ack-receipts-%s.json" % scope)


def joined_ack_receipts(cfg, connection_id):
    path = joined_ack_receipt_path(cfg, connection_id)
    empty = {"schema_version": 1, "origin": cfg.origin, "connection_id": connection_id, "receipts": {}}
    payload = read_json(path, empty)
    if not isinstance(payload, dict) or set(payload) != set(empty) or payload["schema_version"] != 1 or payload["origin"] != cfg.origin or payload["connection_id"] != connection_id or not isinstance(payload["receipts"], dict):
        raise RuntimeError("joined ACK receipt state conflicts with this connection")
    fields = {"artifact_id", "relative_path", "size_bytes", "sha256"}
    for key, receipt in payload["receipts"].items():
        if (
            not isinstance(key, str) or not key.isdigit() or not isinstance(receipt, dict) or set(receipt) != fields
            or receipt["artifact_id"] != int(key) or isinstance(receipt["artifact_id"], bool)
            or not isinstance(receipt["artifact_id"], int) or receipt["artifact_id"] < 1
            or valid_joined_relative_path(receipt["relative_path"]) != receipt["relative_path"]
            or positive_joined_int(receipt["size_bytes"], "ACK receipt size") > JOINED_MAX_BYTES
            or valid_sha256(receipt["sha256"], "ACK receipt") != receipt["sha256"]
        ):
            raise RuntimeError("joined ACK receipt state is invalid")
    return path, payload


def joined_ack_identity(item, size_bytes=None):
    return {
        "artifact_id": item["id"], "relative_path": item["relative_path"],
        "size_bytes": item["size_bytes"] if size_bytes is None else size_bytes, "sha256": item["sha256"],
    }


def has_joined_ack_receipt(cfg, connection_id, identity):
    _, payload = joined_ack_receipts(cfg, connection_id)
    return payload["receipts"].get(str(identity["artifact_id"])) == identity


def persist_joined_ack_receipt(cfg, connection_id, identity):
    path, payload = joined_ack_receipts(cfg, connection_id)
    key = str(identity["artifact_id"])
    existing = payload["receipts"].get(key)
    if existing is not None and existing != identity:
        raise RuntimeError("joined ACK receipt artifact identity conflicts")
    payload["receipts"][key] = identity
    atomic_write(path, joined_canonical_bytes(payload))


def post_joined_ack(cfg, connection_id, identity):
    request_json(cfg, "POST", "/account/joined/ack", body={
        "artifact_id": identity["artifact_id"], "relative_path": identity["relative_path"],
        "size_bytes": identity["size_bytes"], "sha256": identity["sha256"],
    })
    # The receipt is written only after the exact idempotent ACK succeeds.
    persist_joined_ack_receipt(cfg, connection_id, identity)


def ack_joined_item(cfg, item):
    post_joined_ack(cfg, item["connection_id"], joined_ack_identity(item))


def ensure_joined_dependency_ack(cfg, runtime, item, stop_event):
    if item["kind"] == "hour_manifest":
        dependency = {
            "id": item["ledger_artifact_id"], "relative_path": item["ledger_relative_path"],
            "sha256": item["ledger_sha256"],
        }
    elif item["kind"] == "media":
        dependency = {
            "id": item["hour_manifest_id"], "relative_path": item["hour_manifest_relative_path"],
            "sha256": item["hour_manifest_sha256"],
        }
    else:
        return
    holder = {"batch_id": item["batch_id"], "relative_path": dependency["relative_path"]}
    directory_fd = open_joined_output_dir(cfg, holder, create=False)
    try:
        size_bytes, digest, file_stat = hash_joined_entry(
            cfg, runtime, directory_fd, Path(dependency["relative_path"]).name, stop_event,
        )
    finally:
        os.close(directory_fd)
    if file_stat.st_nlink != 1 or digest != dependency["sha256"] or size_bytes < 1 or size_bytes > JOINED_MANIFEST_MAX_BYTES:
        raise ExistingFileMismatch("joined dependency file identity conflicts")
    identity = joined_ack_identity({**dependency, "size_bytes": size_bytes})
    if not has_joined_ack_receipt(cfg, item["connection_id"], identity):
        post_joined_ack(cfg, item["connection_id"], identity)


def drain_joined(cfg, runtime, stop_event):
    if getattr(cfg, "joined_protocol_version", 0) != JOINED_PROTOCOL_VERSION or cfg.dry_run:
        return False
    page = request_json(cfg, "GET", "/account/joined")
    if set(page) - {"item"}:
        raise RuntimeError("joined response contains unknown fields")
    raw_item = page.get("item")
    if raw_item is None:
        return False
    item = valid_joined_item(raw_item)
    runtime.set_phase(Phase.DRAINING)
    try:
        downloaded = download_joined_item(cfg, runtime, item, stop_event)
    except JoinedDownloadYield:
        log("INFO", "joined artifact_id=%d yielded to raw clip delivery" % item["id"])
        return True
    ack_joined_item(cfg, item)
    log(
        "INFO", "joined artifact_id=%d bytes=%d saved=%s%s"
        % (item["id"], item["size_bytes"], joined_output_path(cfg, item), "" if downloaded else " existing"),
    )
    return True


def release_clip(cfg, recording_id, clip_id):
    path = "/account/recordings/%d/clips/%d/release" % (recording_id, clip_id)
    try:
        request_json(cfg, "POST", path)
    except urllib.error.HTTPError as exc:
        if exc.code not in (404, 410):
            raise


def release_clips(cfg, clips):
    body = {"clips": [
        {"recording_id": int(clip["recording_id"]), "clip_id": int(clip["clip_id"])}
        for clip in clips
    ]}
    try:
        request_json(cfg, "POST", "/account/clips/release", body=body)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
        # Compatibility during a backend-first rolling deploy. The batch route is
        # live before this client is promoted; this fallback can be removed after
        # every installation has crossed that release.
        for clip in clips:
            release_clip(cfg, int(clip["recording_id"]), int(clip["clip_id"]))


def stitch_sidecar_path(clip_path):
    return clip_path.with_name(clip_path.name + ".stoarama.json")


def stitch_provenance(clip):
    """Return the durable metadata needed to order and verify this clip later.

    The pull feed is deliberately ID-cursor ordered for exactly-once delivery,
    while concurrent uploads may commit out of media order. Generation-aware
    clips must therefore be grouped by capture_generation and ordered by
    capture_sequence. Legacy clips have null provenance and fall back to their
    explicit clip timestamps.
    """
    return {
        "schema_version": 2 if clip.get("capture_attempt_id") else 1,
        "clip_id": int(clip["clip_id"]),
        "recording_id": int(clip["recording_id"]),
        "recording_job_id": clip.get("recording_job_id"),
        "capture_generation": clip.get("capture_generation"),
        "capture_sequence": clip.get("capture_sequence"),
        "capture_attempt_id": clip.get("capture_attempt_id"),
        "timestamp_contract_version": clip.get("timestamp_contract_version"),
        "timestamp_contract_status": clip.get("timestamp_contract_status"),
        "timestamp_contract_reason": clip.get("timestamp_contract_reason"),
        "timestamp_contract": clip.get("timestamp_contract"),
        "capture_attempt_id": clip.get("capture_attempt_id"),
        "timestamp_contract_version": clip.get("timestamp_contract_version"),
        "timestamp_contract": clip.get("timestamp_contract"),
        "timestamp_contract_status": clip.get("timestamp_contract_status"),
        "timestamp_contract_reason": clip.get("timestamp_contract_reason"),
        "clip_start_at": clip.get("clip_start_at"),
        "clip_end_at": clip.get("clip_end_at"),
        "size_bytes": int(clip["size_bytes"]),
        "sha256": str(clip.get("sha256", "")).lower(),
        "relative_path": str(clip.get("relative_path", "")),
    }


def write_stitch_sidecar(clip_path, clip):
    payload = json.dumps(stitch_provenance(clip), sort_keys=True, separators=(",", ":"))
    atomic_write(stitch_sidecar_path(clip_path), (payload + "\n").encode("utf-8"))


def process_clip(cfg, clip, release=True):
    clip_id = int(clip["clip_id"])
    recording_id = int(clip["recording_id"])
    expected_bytes = int(clip["size_bytes"])
    expected_sha = str(clip.get("sha256", "")).lower()
    if expected_bytes < 0 or len(expected_sha) != 64 or any(ch not in "0123456789abcdef" for ch in expected_sha):
        raise ValueError("clip %d has invalid integrity metadata" % clip_id)
    final_path = cfg.output_dir / valid_relative_path(clip)
    final_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        exists = verified_file(final_path, expected_bytes, expected_sha)
    except ExistingFileMismatch as exc:
        quarantine = final_path.with_name(f".{final_path.name}.invalid-{clip_id}-{time.time_ns()}")
        if quarantine.exists():
            raise ExistingFileMismatch(quarantine) from exc
        os.replace(str(final_path), str(quarantine))
        fsync_dir(final_path.parent)
        log("WARN", f"clip_id={clip_id} quarantined checksum-mismatched file={quarantine}")
        exists = False
    retries = 0
    downloaded_bytes = 0
    if not exists:
        temp_path = final_path.with_name(final_path.name + ".part-%d" % clip_id)

        def download():
            presigned = request_json(cfg, "GET", str(clip["download_path"]), base=cfg.origin)
            url = str(presigned.get("url", ""))
            if not url:
                raise RuntimeError("clip %d presign returned no URL" % clip_id)
            download_verified(url, temp_path, expected_bytes, expected_sha)

        _, retries = retry_transient(download, clip_id, "download")
        os.replace(str(temp_path), str(final_path))
        fsync_dir(final_path.parent)
        downloaded_bytes = expected_bytes
    # Persist stitch provenance durably before releasing the server-side row.
    # If the process crashes after this write, replay safely verifies the MP4 and
    # rewrites the same deterministic sidecar before retrying release.
    write_stitch_sidecar(final_path, clip)
    if release and not cfg.dry_run:
        _, release_retries = retry_transient(
            lambda: release_clip(cfg, recording_id, clip_id), clip_id, "release"
        )
        retries += release_retries
    suffix = " dry-run" if cfg.dry_run else (" released" if release else " ready")
    log("INFO", "clip_id=%d bytes=%d saved=%s%s" % (clip_id, expected_bytes, final_path, suffix))
    return clip_id, expected_bytes, downloaded_bytes, retries


def drain_page(cfg, runtime, inventory=None):
    require_storage_capacity(cfg, runtime)
    page = request_json(
        cfg, "GET", "/account/clips?after_id=%d&limit=%d" % (runtime.cursor_id, LIST_PAGE_LIMIT)
    )
    clips = page.get("clips", [])
    if not isinstance(clips, list):
        raise RuntimeError("clips response is not a list")
    runtime.list_succeeded = True
    if not clips:
        return False
    runtime.set_phase(Phase.DRAINING)
    started = time.monotonic()
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=cfg.download_workers) as executor:
        futures = [executor.submit(prepare_clip_with_capacity, cfg, runtime, clip) for clip in clips]
        for clip, future in zip(clips, futures):
            try:
                results.append((int(clip["clip_id"]), future.result(), None))
            except Exception as exc:
                results.append((int(clip.get("clip_id", 0)), None, exc))
                log("ERROR", "clip_id=%s failed: %s" % (clip.get("clip_id", "?"), exc))
    cursor = runtime.cursor_id
    successes = []
    prepared = [result for _, result, error in results if error is None]
    downloaded_bytes = sum(result[2] for result in prepared)
    retries = sum(result[3] for result in prepared) + sum(
        error.retries for _, _, error in results if isinstance(error, RetryExhausted)
    )
    clip_by_id = {int(clip["clip_id"]): clip for clip in clips}
    prepared_ids = [clip_id for clip_id, _, error in results if error is None]
    if inventory is not None and not cfg.dry_run:
        # The durable local commit and bounded server sync happen before any
        # release call. If either fails, the page stays server-retained.
        for clip_id in prepared_ids:
            inventory.record_verified(clip_by_id[clip_id])
        inventory.sync_clip_ids(cfg, prepared_ids)
    result_by_id = {clip_id: result for clip_id, result, error in results if error is None}
    releasable = []
    for clip_id, _, error in results:
        if error is not None:
            break
        releasable.append(clip_by_id[clip_id])
    try:
        if releasable and not cfg.dry_run:
            require_storage_capacity(cfg, runtime)
            _, release_retries = retry_transient(
                lambda: release_clips(cfg, releasable), int(releasable[0]["clip_id"]), "release-batch"
            )
            retries += release_retries
    except Exception as exc:
        if isinstance(exc, RetryExhausted):
            retries += exc.retries
        first_id = int(releasable[0]["clip_id"]) if releasable else 0
        log("ERROR", "clip_id=%d release batch failed: %s" % (first_id, exc))
        if releasable:
            for index, (clip_id, _, error) in enumerate(results):
                if clip_id == first_id and error is None:
                    results[index] = (clip_id, None, exc)
                    break
        releasable = []
    for clip in releasable:
        clip_id = int(clip["clip_id"])
        successes.append(result_by_id[clip_id])
        cursor = clip_id
    if successes:
        runtime.add_successes(cfg, cursor, successes)
    failures = [(clip_id, error) for clip_id, _, error in results if error is not None]
    runtime.set_batch(len(prepared), downloaded_bytes, time.monotonic() - started, retries, len(failures))
    if failures:
        first_id, first_error = failures[0]
        runtime.set_error(
            f"{len(failures)} of {len(results)} clips failed; first clip {first_id}: {first_error}"[:1000]
        )
        if runtime.is_capacity_blocked():
            runtime.set_phase(Phase.BLOCKED)
    return bool(successes)


def load_outage(cfg):
    return read_json(cfg.outage_file, None)


def heartbeat_loop(cfg, runtime, stop_event):
    outage = load_outage(cfg)
    while not stop_event.is_set():
        try:
            request_json(
                cfg,
                "POST",
                "/account/connections/heartbeat",
                body=runtime.heartbeat_payload(outage),
                timeout=HEARTBEAT_TIMEOUT_SEC,
            )
            runtime.heartbeat_succeeded = True
            if outage:
                outage["recovered_at"] = utc_now()
                request_json(
                    cfg,
                    "POST",
                    "/account/connections/heartbeat",
                    body=runtime.heartbeat_payload(outage),
                    timeout=HEARTBEAT_TIMEOUT_SEC,
                )
                outage = None
                try:
                    cfg.outage_file.unlink()
                except FileNotFoundError:
                    pass
                log("INFO", "heartbeat recovered")
        except Exception as exc:
            try:
                classification = classify_transport_error(exc).value
                now = utc_now()
                if not outage:
                    outage = {"class": classification, "started_at": now, "failure_count": 0}
                outage["class"] = classification
                outage["failure_count"] = int(outage.get("failure_count", 0)) + 1
                atomic_write(cfg.outage_file, json.dumps(outage).encode("utf-8"))
                log("WARN", "heartbeat failed class=%s count=%d: %s" % (classification, outage["failure_count"], exc))
            except Exception as record_exc:
                # Outage bookkeeping must never kill this thread. If it dies the
                # server's view of the client freezes while the drain loop keeps
                # running, and the NAS is remote and cannot be restarted by hand.
                log("WARN", "heartbeat bookkeeping failed: %s" % record_exc)
        stop_event.wait(HEARTBEAT_INTERVAL_SEC)


def validate_manifest(manifest):
    version = str(manifest.get("version", ""))
    artifact = str(manifest.get("artifact", ""))
    sha256 = str(manifest.get("sha256", "")).lower()
    if not version or len(version) > 64 or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for ch in version):
        raise RuntimeError("invalid update version")
    if not artifact or "/" in artifact or "\\" in artifact or artifact in (".", ".."):
        raise RuntimeError("invalid update artifact")
    if len(sha256) != 64 or any(ch not in "0123456789abcdef" for ch in sha256):
        raise RuntimeError("invalid update sha256")
    return version, artifact, sha256


def stage_update(cfg):
    manifest = request_json(cfg, "GET", cfg.update_manifest_url, authenticate=False, timeout=30)
    version, artifact, expected_sha = validate_manifest(manifest)
    if version == CLIENT_VERSION:
        return None
    artifact_url = cfg.update_manifest_url.rsplit("/", 1)[0] + "/" + artifact
    req = urllib.request.Request(artifact_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as response:
        source = response.read()
    if hashlib.sha256(source).hexdigest() != expected_sha:
        raise RuntimeError("update artifact checksum mismatch")
    compile(source, artifact, "exec")
    atomic_write(cfg.candidate_file, source, mode=0o700)
    log("INFO", "staged NAS pull client version=%s" % version)
    return version


def update_loop(cfg, runtime, stop_event, inventory_stop_event, update_ready):
    while not stop_event.is_set():
        try:
            if stage_update(cfg):
                # Staging is harmless, but process replacement is not. Ask the
                # background inventory scan to stop at a durable checkpoint;
                # the delivery loop remains live while it winds down.
                inventory_stop_event.set()
                update_ready.set()
                return
        except Exception as exc:
            log("WARN", "self-update check failed: %s" % exc)
        stop_event.wait(UPDATE_INTERVAL_SEC)


def promote_candidate(cfg):
    if not cfg.is_candidate:
        return
    if cfg.current_file.exists():
        atomic_write(cfg.previous_file, cfg.current_file.read_bytes(), mode=0o700)
    os.replace(str(cfg.candidate_file), str(cfg.current_file))
    fsync_dir(cfg.state_dir)
    cfg.is_candidate = False
    log("INFO", "promoted candidate version=%s" % CLIENT_VERSION)


def exec_candidate(cfg, runtime):
    mark_runtime(cfg, runtime, PreviousExit.SELF_UPDATE.value)
    env = os.environ.copy()
    env["STOARAMA_CANDIDATE"] = "1"
    try:
        os.execve(sys.executable, [sys.executable, str(cfg.candidate_file), "run"], env)
    except OSError as exc:
        raise SelfUpdateExecError("failed to activate staged NAS client") from exc


def update_can_exec(update_ready, inventory_worker, stop_event=None):
    """Process replacement is safe only between delivery pages and scans."""
    return (
        update_ready.is_set()
        and (stop_event is None or not stop_event.is_set())
        and not inventory_worker.is_alive()
    )


def run(cfg):
    cfg.validate()
    lock_handle = acquire_lock(cfg)
    inventory = Inventory(cfg)
    runtime = Runtime(cfg, inventory)
    mark_runtime(cfg, runtime)
    stop_event = threading.Event()
    inventory_stop_event = threading.Event()
    update_ready = threading.Event()

    def stop(_signum, _frame):
        stop_event.set()
        inventory_stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    heartbeat = threading.Thread(target=heartbeat_loop, args=(cfg, runtime, stop_event), daemon=True)
    heartbeat.start()
    storage_probe = threading.Thread(target=storage_probe_loop, args=(cfg, runtime, stop_event), daemon=True)
    storage_probe.start()
    updater = threading.Thread(
        target=update_loop,
        args=(cfg, runtime, stop_event, inventory_stop_event, update_ready),
        daemon=True,
    )
    updater.start()
    inventory_worker = threading.Thread(
        target=inventory_loop, args=(cfg, inventory, inventory_stop_event), daemon=True,
    )
    inventory_worker.start()
    self_update_failed = False
    try:
        while not stop_event.is_set():
            # No delivery page is active at this boundary. A staged candidate
            # may replace the process even when storage/list work is failing,
            # provided the inventory thread has reached its stop checkpoint.
            if update_can_exec(update_ready, inventory_worker, stop_event):
                runtime.set_phase(Phase.UPDATING)
                exec_candidate(cfg, runtime)
            if not heartbeat.is_alive():
                log("WARN", "heartbeat thread dead; restarting")
                heartbeat = threading.Thread(target=heartbeat_loop, args=(cfg, runtime, stop_event), daemon=True)
                heartbeat.start()
            try:
                check_storage(cfg)
                require_storage_capacity(cfg, runtime)
            except (RuntimeError, OSError) as exc:
                runtime.set_phase(Phase.BLOCKED)
                log("ERROR", "storage blocked: %s" % exc)
                stop_event.wait(cfg.poll_interval_sec)
                continue
            try:
                progress = drain_page(cfg, runtime, inventory)
                if runtime.heartbeat_succeeded and runtime.list_succeeded and not runtime.stable_marked:
                    if cfg.is_candidate:
                        promote_candidate(cfg)
                    # Bootstrap v1 rolls back only runtime states "running" and
                    # "self_update". Once both control-plane probes pass, mark
                    # this release stable so an ordinary container restart does
                    # not resurrect the previous client indefinitely.
                    mark_runtime(cfg, runtime, "healthy")
                    runtime.stable_marked = True
                if update_can_exec(update_ready, inventory_worker, stop_event):
                    runtime.set_phase(Phase.UPDATING)
                    exec_candidate(cfg, runtime)
                set_idle_unless_capacity_blocked(runtime)
                if not progress and not stop_event.is_set():
                    try:
                        joined_progress = drain_joined(cfg, runtime, stop_event)
                    except Exception as exc:
                        runtime.set_error("joined delivery: %s" % exc)
                        log("WARN", "joined delivery deferred: %s" % exc)
                        stop_event.wait(min(cfg.poll_interval_sec, 10))
                        continue
                    if joined_progress:
                        continue
                    try:
                        certified = maybe_run_native_stitch(cfg, runtime, inventory, stop_event)
                    except Exception as exc:
                        certified = False
                        log("WARN", "native stitch certification deferred: %s" % exc)
                    if not certified:
                        stop_event.wait(cfg.poll_interval_sec)
            except Exception as exc:
                if isinstance(exc, SelfUpdateExecError):
                    raise
                runtime.set_error(str(exc))
                log("ERROR", "drain failed: %s" % exc)
                stop_event.wait(ERROR_BACKOFF_SEC)
    except SelfUpdateExecError:
        self_update_failed = True
        raise
    finally:
        stop_event.set()
        inventory_stop_event.set()
        heartbeat.join(timeout=HEARTBEAT_TIMEOUT_SEC + 1)
        storage_probe.join(timeout=1)
        inventory_worker.join(timeout=INVENTORY_SHUTDOWN_TIMEOUT_SEC)
        if not self_update_failed:
            mark_runtime(cfg, runtime, PreviousExit.CLEAN.value)
        if inventory_worker.is_alive():
            log("WARN", "inventory worker still running at shutdown; leaving database open")
        else:
            inventory.close()
        lock_handle.close()
    return 0


def check(cfg):
    cfg.validate()
    check_storage(cfg)
    page = request_json(cfg, "GET", "/account/clips?after_id=0&limit=1")
    if not isinstance(page.get("clips", []), list):
        raise RuntimeError("invalid clips response")
    print("NAS storage mounts and API access are healthy")
    return 0


def validate_certification_storage(cfg):
    try:
        output_root = cfg.output_dir.resolve(strict=True)
        state_root = cfg.state_dir.resolve(strict=True)
        inventory_path = cfg.inventory_file.resolve(strict=True)
        lock_path = cfg.lock_file.resolve(strict=True)
    except OSError as exc:
        raise MediaCertificationError("required NAS certification storage is unavailable") from exc
    if (
        not output_root.is_dir() or not state_root.is_dir()
        or not os.path.ismount(str(output_root)) or not os.path.ismount(str(state_root))
        or output_root == state_root
    ):
        raise MediaCertificationError("NAS certification requires distinct mounted storage roots")
    if inventory_path.parent != state_root or lock_path.parent != state_root:
        raise MediaCertificationError("NAS certification state paths escape the state mount")


def acquire_certification_lock(cfg):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(str(cfg.lock_file), flags)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, BlockingIOError) as exc:
        try:
            os.close(descriptor)
        except UnboundLocalError:
            pass
        raise MediaCertificationError("the NAS pull client must be stopped before certification") from exc
    return descriptor


def open_certification_inventory(cfg):
    if not cfg.inventory_file.is_file():
        raise MediaCertificationError("the NAS inventory database is unavailable")
    uri = "file:%s?mode=ro" % urllib.parse.quote(str(cfg.inventory_file.resolve()), safe="/")
    try:
        database = sqlite3.connect(uri, uri=True)
        database.execute("PRAGMA query_only=ON")
        values = dict(database.execute("SELECT key,value FROM meta").fetchall())
    except sqlite3.Error as exc:
        try:
            database.close()
        except UnboundLocalError:
            pass
        raise MediaCertificationError("the NAS inventory database is unreadable") from exc
    generation = values.get("generation", "")
    started_at = values.get("scan_started_at", "")
    pass_started_at = values.get("scan_pass_started_at", "")
    completed_at = values.get("scan_completed_at", "")
    digest = str(values.get("digest", "")).lower()
    try:
        skipped = int(values.get("scan_rows_skipped", "0"))
    except (TypeError, ValueError) as exc:
        database.close()
        raise MediaCertificationError("the NAS inventory completion proof is invalid") from exc
    if not generation or not started_at or not pass_started_at or not completed_at:
        database.close()
        raise MediaCertificationError("a completed NAS inventory scan is required")
    try:
        started = parse_certification_timestamp(started_at, "inventory scan_started_at")
        pass_started = parse_certification_timestamp(pass_started_at, "inventory scan_pass_started_at")
        completed = parse_certification_timestamp(completed_at, "inventory scan_completed_at")
        certification_sha(digest, "inventory digest")
    except MediaCertificationError as exc:
        database.close()
        raise MediaCertificationError("the NAS inventory completion proof is invalid") from exc
    if started > completed or pass_started > completed:
        database.close()
        raise MediaCertificationError("the NAS inventory completion proof is invalid")
    if skipped != 0:
        database.close()
        raise MediaCertificationError("the completed NAS inventory contains skipped paths")
    return database, {
        "generation": generation, "scan_started_at": started_at,
        "scan_pass_started_at": pass_started_at, "scan_completed_at": completed_at, "digest": digest,
    }


def certification_integer(value, field, minimum=0):
    if type(value) is not int or value < minimum:
        raise MediaCertificationError("stitch sidecar %s is invalid" % field)
    return value


def certification_sha(value, field):
    digest = str(value or "").lower()
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise MediaCertificationError("%s is invalid" % field)
    return digest


def collect_certification_candidates(cfg, database, inventory_generation, recording_id, window_start, window_end, frozen_clips=None):
    frozen_ids = None
    if frozen_clips is not None:
        if not isinstance(frozen_clips, list) or not frozen_clips or len(frozen_clips) > NATIVE_STITCH_MAX_CLIPS:
            raise MediaCertificationError("invalid frozen task manifest")
        frozen_ids = [certification_integer(item.get("clip_id"), "frozen clip_id", 1) for item in frozen_clips]
        if len(set(frozen_ids)) != len(frozen_ids):
            raise MediaCertificationError("duplicate frozen clip id")
    try:
        if frozen_ids is None:
            rows = database.execute(
                """SELECT clip_id,relative_path,size_bytes,sha256,verified_at,seen_generation,state
                   FROM files WHERE recording_id=? ORDER BY clip_id""", (recording_id,),
            ).fetchall()
        else:
            placeholders = ",".join("?" for _ in frozen_ids)
            rows = database.execute(
                """SELECT clip_id,relative_path,size_bytes,sha256,verified_at,seen_generation,state
                   FROM files WHERE recording_id=? AND clip_id IN (%s) ORDER BY clip_id""" % placeholders,
                (recording_id, *frozen_ids),
            ).fetchall()
            extras = database.execute(
                """SELECT count(*) FROM files WHERE recording_id=? AND state='present' AND seen_generation=?
                   AND clip_start_us>0 AND clip_end_us>0 AND clip_end_us>? AND clip_start_us<?
                   AND clip_id NOT IN (%s)""" % placeholders,
                (recording_id, inventory_generation,
                 certification_timestamp_microseconds(window_start.isoformat(), "window_start"),
                 certification_timestamp_microseconds(window_end.isoformat(), "window_end"),
                 *frozen_ids),
            ).fetchone()[0]
            if extras:
                raise MediaCertificationError("local inventory has extra media intersecting frozen window")
    except sqlite3.Error as exc:
        raise MediaCertificationError("the NAS inventory clip proof is unreadable") from exc
    candidates = []
    seen_clip_ids = set()
    seen_sequences = set()
    for clip_id, relative_path, expected_size, expected_sha, verified_at, seen_generation, state in rows:
        sidecar, sidecar_size_bytes, sidecar_sha = read_certification_sidecar(cfg.output_dir, relative_path)
        inventory_clip_id = certification_integer(clip_id, "inventory clip_id", 1)
        inventory_size = certification_integer(expected_size, "inventory size_bytes", 0)
        inventory_sha = certification_sha(expected_sha, "inventory sha256")
        sidecar_clip_id = certification_integer(sidecar.get("clip_id"), "clip_id", 1)
        sidecar_recording_id = certification_integer(sidecar.get("recording_id"), "recording_id", 1)
        sidecar_size = certification_integer(sidecar.get("size_bytes"), "size_bytes", 0)
        sidecar_digest = certification_sha(sidecar.get("sha256"), "stitch sidecar sha256")
        if (
            sidecar.get("schema_version") != 1 or sidecar_clip_id != inventory_clip_id
            or sidecar_recording_id != recording_id
            or str(sidecar.get("relative_path", "")) != relative_path
            or sidecar_size != inventory_size or sidecar_digest != inventory_sha
        ):
            raise MediaCertificationError("stitch sidecar does not match inventory metadata")
        clip_start = parse_certification_timestamp(sidecar.get("clip_start_at"), "clip_start_at")
        clip_end = parse_certification_timestamp(sidecar.get("clip_end_at"), "clip_end_at")
        if clip_end <= clip_start:
            raise MediaCertificationError("clip timeline is invalid")
        if clip_end <= window_start or clip_start >= window_end:
            continue
        if state != "present":
            raise MediaCertificationError("requested window contains media without exact NAS proof")
        if str(seen_generation) != inventory_generation:
            raise MediaCertificationError("clip proof is not from the completed inventory generation")
        verified_timestamp = parse_certification_timestamp(verified_at, "inventory verified_at")
        capture_generation = str(sidecar.get("capture_generation") or "").strip()
        sequence = certification_integer(sidecar.get("capture_sequence"), "capture_sequence", 0)
        job_id = certification_integer(sidecar.get("recording_job_id"), "recording_job_id", 1)
        # Capture provenance is tri-state during rolling deployment: historical
        # NULL, probe UNKNOWN, or COMPLETE with an exact contract.
        capture_attempt_id = str(sidecar.get("capture_attempt_id") or "")
        timestamp_version = str(sidecar.get("timestamp_contract_version") or "")
        timestamp_status = str(sidecar.get("timestamp_contract_status") or "")
        timestamp_reason = str(sidecar.get("timestamp_contract_reason") or "")
        timestamp_contract = sidecar.get("timestamp_contract")
        timestamp_contract_sha = timestamp_contract_hash(timestamp_contract) if isinstance(timestamp_contract, dict) else ""
        complete_timestamp = capture_attempt_id and timestamp_status == "per_clip_probe_complete" and timestamp_version == "continuous-source-pts-v1" and timestamp_reason == "" and isinstance(timestamp_contract, dict)
        unknown_timestamp = capture_attempt_id and timestamp_status == "per_clip_probe_unknown" and not timestamp_version and timestamp_reason in ("missing_terminal_duration","missing_audio_sample_count","invalid_time_base","probe_output_limit","probe_unavailable") and timestamp_contract is None
        legacy_timestamp = not capture_attempt_id and not timestamp_version and not timestamp_status and not timestamp_reason and timestamp_contract is None
        if not (complete_timestamp or unknown_timestamp or legacy_timestamp):
            raise MediaCertificationError("clip timestamp provenance is incoherent")
        if not capture_generation or len(capture_generation) > 256 or any(ord(character) < 32 for character in capture_generation):
            raise MediaCertificationError("clip lacks canonical stitch provenance")
        if inventory_clip_id in seen_clip_ids or (job_id, capture_generation, sequence) in seen_sequences:
            raise MediaCertificationError("duplicate stitch provenance")
        seen_clip_ids.add(inventory_clip_id)
        seen_sequences.add((job_id, capture_generation, sequence))
        candidates.append({
            "clip_id": inventory_clip_id, "recording_id": sidecar_recording_id, "recording_job_id": job_id,
            "relative_path": relative_path, "size_bytes": inventory_size,
            "sha256": inventory_sha,
            "inventory_verified_at": verified_timestamp.isoformat().replace("+00:00", "Z"),
            "sidecar_size_bytes": sidecar_size_bytes, "sidecar_sha256": sidecar_sha,
            "capture_generation": capture_generation, "capture_sequence": sequence,
            "capture_attempt_id": capture_attempt_id,
            "timestamp_contract_version": timestamp_version,
            "timestamp_contract_status": timestamp_status,
            "timestamp_contract_reason": timestamp_reason,
            "timestamp_contract_sha256": timestamp_contract_sha,
            "timestamp_contract": timestamp_contract,
            "clip_start_at": clip_start, "clip_end_at": clip_end,
        })
    candidates.sort(key=lambda item: (
        item["clip_start_at"], item["capture_generation"], item["capture_sequence"], item["clip_id"],
    ))
    if frozen_ids is not None and {item["clip_id"] for item in candidates} != set(frozen_ids):
        raise MediaCertificationError("local inventory does not equal frozen manifest")
    if len({item["recording_job_id"] for item in candidates}) > 1:
        raise MediaCertificationError("requested window contains multiple recording jobs")
    sequence_groups = {}
    for item in candidates:
        key = (item["recording_job_id"], item["capture_generation"])
        sequence_groups.setdefault(key, []).append(item["capture_sequence"])
    for sequences in sequence_groups.values():
        ordered = sorted(sequences)
        if ordered != list(range(ordered[0], ordered[-1] + 1)):
            raise MediaCertificationError("selected capture sequence is incomplete")
    for previous, current in zip(candidates, candidates[1:]):
        if current["clip_start_at"] < previous["clip_end_at"]:
            raise MediaCertificationError("selected clips overlap")
    return candidates


def certify_media_canary(cfg, recording_id, window_start_raw, window_end_raw, limit, ffmpeg_bin, ffprobe_bin):
    if recording_id <= 0:
        raise MediaCertificationError("recording id must be positive")
    if limit < 1 or limit > MEDIA_CERTIFICATION_MAX_CLIPS:
        raise MediaCertificationError("limit must be between 1 and %d" % MEDIA_CERTIFICATION_MAX_CLIPS)
    window_start = parse_certification_timestamp(window_start_raw, "window_start")
    window_end = parse_certification_timestamp(window_end_raw, "window_end")
    if window_end <= window_start:
        raise MediaCertificationError("window_end must be after window_start")
    validate_certification_storage(cfg)
    lock_descriptor = acquire_certification_lock(cfg)
    try:
        return certify_media_canary_locked(
            cfg, recording_id, window_start, window_end, limit, ffmpeg_bin, ffprobe_bin,
        )
    finally:
        fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        os.close(lock_descriptor)


def certify_media_canary_locked(cfg, recording_id, window_start, window_end, limit, ffmpeg_bin, ffprobe_bin):
    inventory, summary = open_certification_inventory(cfg)
    try:
        all_candidates = collect_certification_candidates(
            cfg, inventory, summary["generation"], recording_id, window_start, window_end,
        )
    finally:
        inventory.close()
    if not all_candidates:
        raise MediaCertificationError("no inventory clips intersect the requested window")
    selected = all_candidates[:limit]
    selected_bytes = sum(item["size_bytes"] for item in selected)
    if selected_bytes > MEDIA_CERTIFICATION_MAX_BYTES:
        raise MediaCertificationError("selected media exceeds the canary byte budget")
    filesystem = os.statvfs(str(cfg.state_dir))
    free_bytes = filesystem.f_bavail * filesystem.f_frsize
    if free_bytes < selected_bytes * 2 + MEDIA_CERTIFICATION_TEMP_RESERVE_BYTES:
        raise MediaCertificationError("insufficient temporary capacity for lossless validation")
    tool_versions = {
        "ffmpeg": media_tool_version(ffmpeg_bin),
        "ffprobe": media_tool_version(ffprobe_bin),
    }
    clip_reports = []
    selected_paths = []
    selected_identities = []
    for item in selected:
        path, file_stat, actual_sha = hash_certification_file(
            cfg.output_dir, item["relative_path"], item["size_bytes"], item["sha256"],
        )
        probe = probe_native_media(path, ffprobe_bin)
        expected_duration = (item["clip_end_at"] - item["clip_start_at"]).total_seconds()
        if abs(probe["duration_seconds"] - expected_duration) > MEDIA_CERTIFICATION_DURATION_TOLERANCE_SEC:
            raise MediaCertificationError("container duration does not match the clip timeline")
        strict_decode_media(path, ffmpeg_bin)
        # The decode is a second full read. Recheck the pathname identity so a
        # replacement during verification cannot inherit the earlier hash proof.
        after_decode = path.stat()
        if certification_identity(file_stat) != certification_identity(after_decode):
            raise MediaCertificationError("media identity changed during decode")
        signature_sha = canonical_report_hash(probe["signature"])
        clip_reports.append({
            "clip_id": item["clip_id"], "recording_job_id": item["recording_job_id"],
            "relative_path": item["relative_path"], "size_bytes": item["size_bytes"],
            "sha256": actual_sha, "capture_generation": item["capture_generation"],
            "capture_sequence": item["capture_sequence"],
            "clip_start_at": item["clip_start_at"].isoformat().replace("+00:00", "Z"),
            "clip_end_at": item["clip_end_at"].isoformat().replace("+00:00", "Z"),
            "inventory_verified_at": item["inventory_verified_at"],
            "sidecar_size_bytes": item["sidecar_size_bytes"],
            "sidecar_sha256": item["sidecar_sha256"],
            "file_identity": {
                "size": after_decode.st_size, "mtime_ns": after_decode.st_mtime_ns,
                "ctime_ns": after_decode.st_ctime_ns, "inode": after_decode.st_ino,
                "device": after_decode.st_dev,
            },
            "probe": probe, "native_signature_sha256": signature_sha,
            "strict_decode": "passed",
        })
        selected_paths.append(path)
        selected_identities.append(certification_identity(after_decode))
    runs = []
    run_start = 0
    with tempfile.TemporaryDirectory(prefix="stoarama-certify-", dir=str(cfg.state_dir)) as raw_temp:
        temp_root = Path(raw_temp)
        for index in range(1, len(clip_reports) + 1):
            boundary = index == len(clip_reports)
            if not boundary:
                current_key = (
                    clip_reports[index]["recording_job_id"], clip_reports[index]["capture_generation"],
                    clip_reports[index]["native_signature_sha256"],
                )
                run_key = (
                    clip_reports[run_start]["recording_job_id"], clip_reports[run_start]["capture_generation"],
                    clip_reports[run_start]["native_signature_sha256"],
                )
                boundary = current_key != run_key
            if not boundary:
                continue
            run_paths = selected_paths[run_start:index]
            run_dir = temp_root / ("run-%d" % len(runs))
            run_dir.mkdir()
            concat_state = validate_native_run(run_paths, ffmpeg_bin, run_dir)
            runs.append({
                "first_clip_id": clip_reports[run_start]["clip_id"],
                "last_clip_id": clip_reports[index - 1]["clip_id"],
                "clip_count": index - run_start,
                "native_signature_sha256": clip_reports[run_start]["native_signature_sha256"],
                "lossless_stitch_validation": concat_state,
            })
            run_start = index
    for path, expected_identity in zip(selected_paths, selected_identities):
        if certification_identity(path.stat()) != expected_identity:
            raise MediaCertificationError("media identity changed during stitch validation")
    final_inventory, final_summary = open_certification_inventory(cfg)
    final_inventory.close()
    if final_summary != summary:
        raise MediaCertificationError("NAS inventory completion changed during certification")
    internal_gaps = [
        max(0.0, (current["clip_start_at"] - previous["clip_end_at"]).total_seconds())
        for previous, current in zip(selected, selected[1:])
    ]
    report = {
        "schema_version": MEDIA_CERTIFICATION_SCHEMA_VERSION,
        "certification_scope": "bounded_clip_canary",
        "window_complete": False,
        "recording_id": recording_id,
        "window_start_at": window_start.isoformat().replace("+00:00", "Z"),
        "window_end_at": window_end.isoformat().replace("+00:00", "Z"),
        "inventory_generation": summary.get("generation", ""),
        "inventory_digest": summary.get("digest", ""),
        "inventory_scan_started_at": summary.get("scan_started_at"),
        "inventory_scan_pass_started_at": summary.get("scan_pass_started_at"),
        "inventory_completed_at": summary.get("scan_completed_at"),
        "available_window_clip_count": len(all_candidates),
        "selected_clip_count": len(selected),
        "selected_bytes": selected_bytes,
        "selected_internal_gap_count": sum(1 for gap in internal_gaps if gap > 0),
        "selected_largest_internal_gap_seconds": max(internal_gaps, default=0.0),
        "selected_overlap_count": 0,
        "truncated_by_canary_limit": len(selected) < len(all_candidates),
        "clips": clip_reports,
        "native_runs": runs,
        "tools": tool_versions,
        "source_media_modified": False,
        "reencoded": False,
        "persistent_output_created": False,
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    report["report_sha256"] = canonical_report_hash(report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description="Stoarama NAS pull client")
    parser.add_argument("command", nargs="?", choices=("run", "check", "version", "self-update", "certify"), default="run")
    parser.add_argument("--recording-id", type=int, default=0)
    parser.add_argument("--window-start", default="")
    parser.add_argument("--window-end", default="")
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument("--ffmpeg-bin", default=os.environ.get("FFMPEG_BIN", "ffmpeg"))
    parser.add_argument("--ffprobe-bin", default=os.environ.get("FFPROBE_BIN", "ffprobe"))
    args = parser.parse_args(argv)
    if args.command == "version":
        print(CLIENT_VERSION)
        return 0
    cfg = Config()
    if args.command == "check":
        return check(cfg)
    if args.command == "self-update":
        cfg.validate()
        print(stage_update(cfg) or "already-current")
        return 0
    if args.command == "certify":
        try:
            report = certify_media_canary(
                cfg, args.recording_id, args.window_start, args.window_end,
                args.limit, args.ffmpeg_bin, args.ffprobe_bin,
            )
        except MediaCertificationError as exc:
            print("certification failed: %s" % exc, file=sys.stderr)
            return 1
        except Exception:
            print("certification failed: unexpected local verification failure", file=sys.stderr)
            return 1
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
        return 0
    return run(cfg)


if __name__ == "__main__":
    sys.exit(main())
