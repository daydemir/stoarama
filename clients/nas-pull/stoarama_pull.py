#!/usr/bin/env python3
"""Stoarama NAS pull client. Python standard library only."""

import argparse
import concurrent.futures
import fcntl
import hashlib
import json
import os
import re
import signal
import socket
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from enum import Enum
from pathlib import Path

CLIENT_VERSION = "development"
LIST_PAGE_LIMIT = 200
DEFAULT_DOWNLOAD_WORKERS = 12
MAX_DOWNLOAD_WORKERS = 32
DOWNLOAD_ATTEMPTS = 3
HTTP_TIMEOUT_SEC = 120
HEARTBEAT_TIMEOUT_SEC = 20
HEARTBEAT_INTERVAL_SEC = 30
STORAGE_TELEMETRY_MAX_AGE_SEC = HEARTBEAT_INTERVAL_SEC * 3
UPDATE_INTERVAL_SEC = 600
INVENTORY_SCAN_INTERVAL_SEC = 24 * 60 * 60
INVENTORY_SYNC_BATCH = 200
INVENTORY_SHUTDOWN_TIMEOUT_SEC = HTTP_TIMEOUT_SEC + 5
ERROR_BACKOFF_SEC = 30
USER_AGENT = "stoarama-nas-pull/%s" % CLIENT_VERSION


class ExistingFileMismatch(RuntimeError):
    pass


class FileChangedDuringHash(RuntimeError):
    pass


class RetryExhausted(RuntimeError):
    def __init__(self, cause, retries):
        super().__init__(str(cause))
        self.retries = retries


class Phase(str, Enum):
    STARTING = "starting"
    IDLE = "idle"
    DRAINING = "draining"
    UPDATING = "updating"
    BLOCKED = "blocked"
    DEGRADED = "degraded"


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
        self.update_manifest_url = env_str(
            "STOARAMA_UPDATE_MANIFEST_URL", "https://stoarama.com/nas/download/latest.json"
        )
        self.dry_run = env_str("STOARAMA_DRY_RUN", "0") == "1"
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

    def _upsert(self, clip, state, verified_at, mtime_ns, generation="live", commit=True, scan_pass="", file_identity=(0, 0, 0)):
        updated_at = utc_now_precise()
        ctime_ns, inode, device = file_identity
        with self.lock:
            self.db.execute(
                """INSERT INTO files
                   (clip_id,recording_id,relative_path,size_bytes,sha256,state,verified_at,file_mtime_ns,file_ctime_ns,file_inode,file_device,seen_generation,scan_pass,client_updated_at,dirty)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                   ON CONFLICT(clip_id) DO UPDATE SET
                     recording_id=excluded.recording_id, relative_path=excluded.relative_path,
                     size_bytes=excluded.size_bytes, sha256=excluded.sha256, state=excluded.state,
                     verified_at=excluded.verified_at, file_mtime_ns=excluded.file_mtime_ns,
                     file_ctime_ns=excluded.file_ctime_ns,file_inode=excluded.file_inode,file_device=excluded.file_device,
                     seen_generation=excluded.seen_generation,scan_pass=excluded.scan_pass,
                     client_updated_at=excluded.client_updated_at,dirty=1""",
                (
                    int(clip["clip_id"]), int(clip["recording_id"]), str(clip["relative_path"]),
                    int(clip["size_bytes"]), str(clip["sha256"]).lower(), state,
                    verified_at, int(mtime_ns), int(ctime_ns), int(inode), int(device), generation, scan_pass, updated_at,
                ),
            )
            if commit:
                self.db.commit()

    def _commit_scan_batch(self):
        with self.lock:
            self.db.commit()

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
                          verified_at,file_mtime_ns,client_updated_at
                   FROM files WHERE %s ORDER BY clip_id LIMIT ?""" % where,
                tuple(params) + (limit,),
            ).fetchall()

    @staticmethod
    def _reports(rows):
        return [
            {
                "clip_id": row[0], "recording_id": row[1], "relative_path": row[2],
                "size_bytes": row[3], "sha256": row[4], "state": row[5],
                "verified_at": row[6], "file_mtime_ns": row[7], "client_updated_at": row[8],
            }
            for row in rows
        ]

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

    def sync_dirty(self, cfg, generation="live", scan_started_at=None):
        while True:
            rows = self._rows("dirty=1")
            if not rows:
                break
            request_json(cfg, "POST", "/account/connections/inventory", body={
                "generation": generation, "scan_started_at": scan_started_at,
                "complete": False, "files": self._reports(rows),
            })
            self._mark_clean(rows)
        while True:
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

    def _linked_scan_row_is_current(self, clip, generation, scan_pass, path):
        with self.lock:
            row = self.db.execute(
                """SELECT recording_id,relative_path,size_bytes,sha256,state,file_mtime_ns,seen_generation,
                          file_ctime_ns,file_inode,file_device
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
        return {
            "generation": generation,
            "scan_started_at": values.get("scan_started_at") or None,
            "scan_completed_at": values.get("scan_completed_at") or None,
            "clips": int(values.get("clips", 0)),
            "bytes": int(values.get("bytes", 0)),
            "mismatches": int(values.get("mismatches", 0)),
            "unmatched": int(values.get("unmatched", 0)),
            "digest": values.get("digest", ""),
        }

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
        known_paths = set()
        for sidecar in cfg.output_dir.rglob("*.stoarama.json"):
            if stop_event.is_set():
                self._commit_scan_batch()
                return
            try:
                clip = json.loads(sidecar.read_text(encoding="utf-8"))
                relative = valid_relative_path(clip)
                if str(relative) != str(clip.get("relative_path", "")):
                    raise ValueError("sidecar path is not canonical")
                path = cfg.output_dir / relative
                if sidecar.resolve() != stitch_sidecar_path(path).resolve():
                    raise ValueError("sidecar location does not match its clip path")
                expected_size = int(clip["size_bytes"])
                expected_sha = str(clip["sha256"]).lower()
                if len(expected_sha) != 64 or any(ch not in "0123456789abcdef" for ch in expected_sha):
                    raise ValueError("sidecar checksum is invalid")
                known_paths.add(str(relative))
                self._retire_unmatched_linked_path(str(relative), generation, scan_pass)
                if self._linked_scan_row_is_current(clip, generation, scan_pass, path):
                    scanned += 1
                    if scanned % INVENTORY_SYNC_BATCH == 0:
                        self._commit_scan_batch()
                        self.sync_dirty(cfg, generation, started_at)
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
                self._upsert(clip, state, verified_at, mtime_ns, generation, commit=False, scan_pass=scan_pass, file_identity=identity)
                scanned += 1
                if scanned % INVENTORY_SYNC_BATCH == 0:
                    self._commit_scan_batch()
                    self.sync_dirty(cfg, generation, started_at)
                if cfg.inventory_scan_delay_ms:
                    stop_event.wait(cfg.inventory_scan_delay_ms / 1000.0)
            except Exception as exc:
                skipped += 1
                log("WARN", "inventory skipped sidecar=%s: %s" % (sidecar, exc))
        for path in cfg.output_dir.rglob("*"):
            if stop_event.is_set():
                self._commit_scan_batch()
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
                        self._commit_scan_batch()
                        self.sync_dirty(cfg, generation, started_at)
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
                    self._commit_scan_batch()
                    self.sync_dirty(cfg, generation, started_at)
                if cfg.inventory_scan_delay_ms:
                    stop_event.wait(cfg.inventory_scan_delay_ms / 1000.0)
            except Exception as exc:
                skipped += 1
                log("WARN", "inventory skipped unmatched file=%s: %s" % (path, exc))
        # Flush and publish every successfully observed row, but never promote a
        # partial generation. In particular, do not turn unseen prior rows into
        # "missing" when an unreadable/corrupt path was skipped.
        self._commit_scan_batch()
        self.sync_dirty(cfg, generation, started_at)
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
        self.sync_dirty(cfg, generation, started_at)
        digest, clips, total_bytes, mismatches, unmatched = self._digest_and_counts()
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
            inventory.sync_dirty(cfg)
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
        # A new client explicitly clears any stale server-side capacity until
        # the independent probe proves that the configured NAS mount is live.
        self.storage = {"available": False}
        self.storage_observed_monotonic = time.monotonic()
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
    return Path(*parts)


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
            started = time.monotonic()
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            delay = len(chunk) / target_bytes_per_sec - (time.monotonic() - started)
            if delay > 0 and stop_event.wait(delay):
                raise InterruptedError("inventory scan stopped")
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


def verified_file(path, expected_bytes, expected_sha):
    if not path.exists():
        return False
    size, digest = sha256_file(path)
    if size != expected_bytes or digest != expected_sha:
        raise ExistingFileMismatch(f"existing file does not match API checksum: {path}")
    return True


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
        "schema_version": 1,
        "clip_id": int(clip["clip_id"]),
        "recording_id": int(clip["recording_id"]),
        "recording_job_id": clip.get("recording_job_id"),
        "capture_generation": clip.get("capture_generation"),
        "capture_sequence": clip.get("capture_sequence"),
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
        futures = [executor.submit(process_clip, cfg, clip, False) for clip in clips]
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


def update_loop(cfg, runtime, stop_event, update_ready):
    while not stop_event.is_set():
        try:
            if stage_update(cfg):
                runtime.set_phase(Phase.UPDATING)
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
    os.execve(sys.executable, [sys.executable, str(cfg.candidate_file), "run"], env)


def run(cfg):
    cfg.validate()
    lock_handle = acquire_lock(cfg)
    inventory = Inventory(cfg)
    runtime = Runtime(cfg, inventory)
    mark_runtime(cfg, runtime)
    stop_event = threading.Event()
    update_ready = threading.Event()

    def stop(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    heartbeat = threading.Thread(target=heartbeat_loop, args=(cfg, runtime, stop_event), daemon=True)
    heartbeat.start()
    storage_probe = threading.Thread(target=storage_probe_loop, args=(cfg, runtime, stop_event), daemon=True)
    storage_probe.start()
    updater = threading.Thread(target=update_loop, args=(cfg, runtime, stop_event, update_ready), daemon=True)
    updater.start()
    inventory_worker = threading.Thread(target=inventory_loop, args=(cfg, inventory, stop_event), daemon=True)
    inventory_worker.start()
    try:
        while not stop_event.is_set():
            if not heartbeat.is_alive():
                log("WARN", "heartbeat thread dead; restarting")
                heartbeat = threading.Thread(target=heartbeat_loop, args=(cfg, runtime, stop_event), daemon=True)
                heartbeat.start()
            try:
                check_storage(cfg)
            except (RuntimeError, OSError) as exc:
                runtime.set_phase(Phase.BLOCKED)
                runtime.set_error(str(exc))
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
                if update_ready.is_set():
                    exec_candidate(cfg, runtime)
                runtime.set_phase(Phase.IDLE)
                if not progress:
                    stop_event.wait(cfg.poll_interval_sec)
            except Exception as exc:
                runtime.set_error(str(exc))
                log("ERROR", "drain failed: %s" % exc)
                stop_event.wait(ERROR_BACKOFF_SEC)
    finally:
        stop_event.set()
        heartbeat.join(timeout=HEARTBEAT_TIMEOUT_SEC + 1)
        storage_probe.join(timeout=1)
        inventory_worker.join(timeout=INVENTORY_SHUTDOWN_TIMEOUT_SEC)
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


def main(argv=None):
    parser = argparse.ArgumentParser(description="Stoarama NAS pull client")
    parser.add_argument("command", nargs="?", choices=("run", "check", "version", "self-update"), default="run")
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
    return run(cfg)


if __name__ == "__main__":
    sys.exit(main())
