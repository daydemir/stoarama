#!/usr/bin/env python3
"""Stoarama NAS pull client. Python standard library only."""

import argparse
import concurrent.futures
import fcntl
import hashlib
import json
import os
import shutil
import signal
import socket
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
DOWNLOAD_WORKERS = 4
HTTP_TIMEOUT_SEC = 120
HEARTBEAT_TIMEOUT_SEC = 20
HEARTBEAT_INTERVAL_SEC = 30
UPDATE_INTERVAL_SEC = 600
ERROR_BACKOFF_SEC = 30
USER_AGENT = "stoarama-nas-pull/%s" % CLIENT_VERSION


class ExistingFileMismatch(RuntimeError):
    pass


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
        self.current_file = self.state_dir / "stoarama_pull.py"
        self.candidate_file = self.state_dir / "stoarama_pull.candidate.py"
        self.previous_file = self.state_dir / "stoarama_pull.previous.py"
        self.lock_file = self.state_dir / "client.lock"
        self.poll_interval_sec = env_int("STOARAMA_POLL_INTERVAL_SEC", 60)
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


class Runtime:
    def __init__(self, cfg):
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
            }
        if outage:
            payload["last_outage"] = outage
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
        quarantine = final_path.with_name(f".{final_path.name}.invalid-{clip_id}")
        if quarantine.exists():
            raise RuntimeError(f"clip {clip_id} has both invalid final and quarantine files") from exc
        os.replace(str(final_path), str(quarantine))
        fsync_dir(final_path.parent)
        log("WARN", f"clip_id={clip_id} quarantined checksum-mismatched file={quarantine}")
        exists = False
    if not exists:
        presigned = request_json(cfg, "GET", str(clip["download_path"]), base=cfg.origin)
        url = str(presigned.get("url", ""))
        if not url:
            raise RuntimeError("clip %d presign returned no URL" % clip_id)
        temp_path = final_path.with_name(final_path.name + ".part-%d" % clip_id)
        download_verified(url, temp_path, expected_bytes, expected_sha)
        os.replace(str(temp_path), str(final_path))
        fsync_dir(final_path.parent)
    if release and not cfg.dry_run:
        release_clip(cfg, recording_id, clip_id)
    suffix = " dry-run" if cfg.dry_run else (" released" if release else " ready")
    log("INFO", "clip_id=%d bytes=%d saved=%s%s" % (clip_id, expected_bytes, final_path, suffix))
    return clip_id, expected_bytes


def drain_page(cfg, runtime):
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
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
        futures = [executor.submit(process_clip, cfg, clip, False) for clip in clips]
        for clip, future in zip(clips, futures):
            try:
                results.append((int(clip["clip_id"]), future.result(), None))
            except Exception as exc:
                results.append((int(clip.get("clip_id", 0)), None, exc))
                log("ERROR", "clip_id=%s failed: %s" % (clip.get("clip_id", "?"), exc))
    cursor = runtime.cursor_id
    successes = []
    recording_by_clip = {int(clip["clip_id"]): int(clip["recording_id"]) for clip in clips}
    for index, (clip_id, result, error) in enumerate(results):
        if error is not None:
            break
        try:
            if not cfg.dry_run:
                release_clip(cfg, recording_by_clip[clip_id], clip_id)
        except Exception as exc:
            log("ERROR", "clip_id=%d release failed: %s" % (clip_id, exc))
            results[index] = (clip_id, None, exc)
            break
        successes.append(result)
        cursor = clip_id
    if successes:
        runtime.add_successes(cfg, cursor, successes)
    failures = [(clip_id, error) for clip_id, _, error in results if error is not None]
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
            classification = classify_transport_error(exc).value
            now = utc_now()
            if not outage:
                outage = {"class": classification, "started_at": now, "failure_count": 0}
            outage["class"] = classification
            outage["failure_count"] = int(outage.get("failure_count", 0)) + 1
            atomic_write(cfg.outage_file, json.dumps(outage).encode("utf-8"))
            log("WARN", "heartbeat failed class=%s count=%d: %s" % (classification, outage["failure_count"], exc))
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
    while not stop_event.wait(UPDATE_INTERVAL_SEC):
        try:
            if stage_update(cfg):
                runtime.set_phase(Phase.UPDATING)
                update_ready.set()
                return
        except Exception as exc:
            log("WARN", "self-update check failed: %s" % exc)


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
    runtime = Runtime(cfg)
    mark_runtime(cfg, runtime)
    stop_event = threading.Event()
    update_ready = threading.Event()

    def stop(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    heartbeat = threading.Thread(target=heartbeat_loop, args=(cfg, runtime, stop_event), daemon=True)
    heartbeat.start()
    updater = threading.Thread(target=update_loop, args=(cfg, runtime, stop_event, update_ready), daemon=True)
    updater.start()
    try:
        while not stop_event.is_set():
            try:
                check_storage(cfg)
            except (RuntimeError, OSError) as exc:
                runtime.set_phase(Phase.BLOCKED)
                runtime.set_error(str(exc))
                log("ERROR", "storage blocked: %s" % exc)
                stop_event.wait(cfg.poll_interval_sec)
                continue
            try:
                progress = drain_page(cfg, runtime)
                if cfg.is_candidate and runtime.heartbeat_succeeded and runtime.list_succeeded:
                    promote_candidate(cfg)
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
        mark_runtime(cfg, runtime, PreviousExit.CLEAN.value)
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
