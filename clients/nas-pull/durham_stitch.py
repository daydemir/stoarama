"""Durham's dedicated direct R2 stitch worker."""
import concurrent.futures
import datetime
import hashlib
from http.client import HTTPException, HTTPSConnection
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import sqlite3
import sys
import threading
from types import SimpleNamespace
from urllib.parse import urlsplit

import stoarama_pull as pull


class DurhamError(RuntimeError):
    pass


class DurhamV85r:
    def __init__(self, state):
        if sys.platform != "linux" or platform.machine() != "aarch64":
            raise DurhamError("Durham requires a native Linux ARM64 guest")
        profile_dir = Path(__file__).resolve().parent / "v85r"
        self.pins = json.loads((profile_dir / "durham-arm64.json").read_bytes())
        if self.pins["architecture"] != "linux-arm64":
            raise DurhamError("Durham toolchain architecture differs")
        with (profile_dir / "durham-arm64.tar.gz").open("rb") as source:
            self.bundle = source.read(self.pins["bundle_size_bytes"] + 1)
        if len(self.bundle) != self.pins["bundle_size_bytes"]:
            raise DurhamError("Durham toolchain bundle size differs")
        self.state = state
        self.manifest = self.pins["manifest_sha256"]
        self.tools = {name: "/usr/bin/" + name for name in ("ffmpeg", "ffprobe")}

    @staticmethod
    def verify_executable(path, size, sha, stop):
        if path.is_symlink() or not os.access(path, os.X_OK):
            raise DurhamError("Durham tool must be a pinned executable")
        with path.open("rb") as source:
            header = source.read(20)
        if (header[:6] != b"\x7fELF\x02\x01" or int.from_bytes(header[18:20], "little") != 183
                or pull.stitch_proof_digest(path, stop) != (size, sha)):
            raise DurhamError("Durham native executable identity differs")

    def verify(self, tools, stop):
        if tools != self.tools:
            raise DurhamError("Durham media tool path differs")
        for name, path in tools.items():
            pin = self.pins["tools"][name]
            self.verify_executable(Path(path), pin["size_bytes"], pin["sha256"], stop)
        bundle = pull.stitch_unpack_v85r(self.bundle, self.pins["bundle_sha256"], self.manifest, self.state, stop)
        self.verify_executable(bundle / "remeasure-linux-arm64", self.pins["evaluator_size_bytes"],
                               self.pins["evaluator_sha256"], stop)
        return bundle, dict(
            policy=pull.STITCH_POLICY, architecture="linux-arm64", manifest_sha256=self.manifest,
            evaluator_sha256=self.pins["evaluator_sha256"], october_ffmpeg_recovered=False,
            ffmpeg_version=self.pins["media_tool_version"], ffprobe_version=self.pins["media_tool_version"],
            ffmpeg_sha256=self.pins["tools"]["ffmpeg"]["sha256"],
            ffprobe_sha256=self.pins["tools"]["ffprobe"]["sha256"],
        )


class DurhamHTTP:
    def __init__(self, api, r2_origin, token, stop, v85r):
        self.api = api.rstrip("/")
        self.api_host = self.origin(urlsplit(self.api))
        self.r2_host = self.origin(urlsplit(r2_origin))
        self.token, self.stop = token, stop
        self.v85r = v85r

    @staticmethod
    def origin(url):
        if url.scheme != "https" or not url.hostname or url.username or url.password or url.fragment:
            raise DurhamError("Durham requires a direct HTTPS origin")
        return url.hostname, url.port or 443

    def request(self, url, origin, method, headers, body=None):
        parsed = urlsplit(url)
        if self.origin(parsed) != origin or self.stop.is_set():
            raise DurhamError("Durham request origin or shutdown gate")
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        connection = HTTPSConnection(*origin, timeout=30)
        try:
            connection.request(method, path, headers=headers, body=body)
            response = connection.getresponse()
            if not 200 <= response.status < 300:
                raise DurhamError("Durham HTTP %d" % response.status)
            return connection, response
        except BaseException:
            connection.close()
            raise

    def post(self, path, body):
        connection, response = self.request(self.api + "/account/connections/stitch/" + path,
            self.api_host, "POST", {"Authorization": "Bearer " + self.token,
            "Content-Type": "application/json", "X-Stoarama-Stitch-Source": "r2-v1",
            "X-Stoarama-Stitch-Toolchain": self.v85r.manifest}, pull.cleanup_json_bytes(body))
        with response:
            try:
                raw = response.read(2 * 1024 * 1024 + 1)
                if len(raw) > 2 * 1024 * 1024:
                    raise DurhamError("Durham control response bound")
                return json.loads(raw)
            finally:
                connection.close()

    def transfer(self, cap, method, payload=None):
        if cap["method"] != method:
            raise DurhamError("Durham signed method differs")
        headers = {}
        for name, values in cap["headers"].items():
            if not isinstance(values, list) or len(values) != 1:
                raise DurhamError("Durham signed header differs")
            headers[name] = values[0]
        return self.request(cap["url"], self.r2_host, method, headers, payload)


def task_request(http, task, path, **values):
    return http.post("%d/%s" % (task["id"], path), dict(
        attempt=task["attempt"], lease_token=task["lease_token"], plan_sha256=task["plan_sha256"], **values))


def task_checkpoint(http, task):
    if task_request(http, task, "checkpoint", operation="continue").get("continue") is not True:
        raise DurhamError("Durham stitch attempt no longer active")


def write_json(path, value):
    pull.atomic_write(path, pull.cleanup_json_bytes(value), mode=0o600)
    pull.fsync_dir(path.parent)


def fetch_clip(http, cfg, task, clip):
    relative = str(pull.valid_relative_path({"relative_path": clip["relative_path"], "clip_id": clip["source_id"]}))
    path = cfg.output_dir / relative
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    expected = (clip["size_bytes"], clip["sha256"])
    if not path.exists():
        cap = task_request(http, task, "checkpoint", operation="input", clip_id=clip["source_id"])
        connection, response = http.transfer(cap, "GET")
        temporary = path.with_suffix(".durham.part")
        try:
            digest, size = hashlib.sha256(), 0
            with response, temporary.open("wb") as target:
                while not http.stop.is_set():
                    data = response.read(1024 * 1024)
                    if not data:
                        break
                    size += len(data)
                    if size > expected[0]:
                        raise DurhamError("Durham source exceeds its frozen size")
                    digest.update(data)
                    target.write(data)
                if (size, digest.hexdigest()) != expected or http.stop.is_set():
                    raise DurhamError("Durham source size or SHA-256 differs")
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, path)
            pull.fsync_dir(path.parent)
        finally:
            connection.close()
    if pull.stitch_proof_digest(path, http.stop) != expected:
        raise DurhamError("cached Durham source size or SHA-256 differs")


def upload_output(http, task, cfg, result):
    staged = task_request(http, task, "checkpoint", operation="start_upload", relative_path=result["relative_path"],
                          sha256=result["sha256"], size_bytes=result["size_bytes"])
    if staged["size_bytes"] != result["size_bytes"] or staged["part_bytes"] != 16 * 1024 * 1024:
        raise DurhamError("Durham upload reservation changed")
    if staged["state"] == "staged":
        return
    if staged["state"] == "uploading":
        path = cfg.output_dir / result["relative_path"]
        saved = path.with_suffix(".uploaded.json")
        parts = json.loads(saved.read_bytes()) if saved.exists() else {}
        with path.open("rb") as source:
            number = 0
            while True:
                data = source.read(staged["part_bytes"])
                if not data:
                    break
                number += 1
                sha = hashlib.sha256(data).hexdigest()
                if parts.get(str(number)) == [staged["id"], sha]:
                    continue
                cap = task_request(http, task, "checkpoint", operation="upload_part", object_id=staged["id"],
                                   part_number=number, sha256=sha)
                connection, response = http.transfer(cap, "PUT", data)
                response.close()
                connection.close()
                parts[str(number)] = [staged["id"], sha]
                write_json(saved, parts)
    elif staged["state"] != "completing":
        raise DurhamError("unrecognized Durham upload state")
    if task_request(http, task, "checkpoint", operation="finish_upload", object_id=staged["id"]).get("staged") is not True:
        raise DurhamError("Durham upload completion unconfirmed")


def process_task(http, task, root, state, inventory, tools):
    status = task_request(http, task, "ready")
    if status["state"] in ("staged", "held", "certified"):
        return
    scratch = root / "media"
    scratch.mkdir(mode=0o700, exist_ok=True)
    cfg = SimpleNamespace(output_dir=scratch, state_dir=state, min_free_bytes=1024**3, stitch_enabled=True,
                          durham_v85r=http.v85r, durham_checkpoint=lambda t: task_checkpoint(http, t))
    result_file = root / "result.json"
    if result_file.exists():
        result = json.loads(result_file.read_bytes())
    else:
        clips = task["plan"]["clips"]
        pull.stitch_resources(cfg, sum(clip["size_bytes"] for clip in clips), local_scratch=True)
        for clip in clips:
            fetch_clip(http, cfg, task, clip)
        result = pull.stitch_build(cfg, inventory, task, http.stop, tools, kind=pull.StitchRunKind.DURHAM_R2)
        write_json(result_file, result)
    if (result["outputs"] and not result["left_out"] and not result["repairs"] and not result["error"]
            and all(seam["decision"] != "hold" for seam in result["seams"])):
        for output in result["outputs"]:
            upload_output(http, task, cfg, output)
    if http.post("%d/result" % task["id"], {"attempt": task["attempt"], "result": result}).get("recorded") is not True:
        raise DurhamError("Durham result not durably recorded")


def run_worker(http, slot, state, tools):
    pull.stitch_priority()
    root = state / ("worker-%d" % slot)
    root.mkdir(mode=0o700, exist_ok=True)
    saved = root / "attempt.json"
    work = root / "current"
    with sqlite3.connect(state / "inventory.sqlite3") as database:
        inventory = SimpleNamespace(db=database, lock=threading.Lock())
        wait = 0
        while not http.stop.wait(wait):
            wait = 30
            try:
                if saved.exists():
                    task = json.loads(saved.read_bytes())
                else:
                    response = http.post("lease", {})
                    if response.get("task") is None:
                        continue
                    if response.get("source") != "r2":
                        raise DurhamError("Durham lease is not R2 backed")
                    task = response["task"]
                    work.mkdir(mode=0o700, exist_ok=True)
                    write_json(saved, task)
                expires = pull.parse_certification_timestamp(task["expires_at"], "Durham lease")
                if expires > datetime.datetime.now(datetime.timezone.utc):
                    process_task(http, task, work, state, inventory, tools)
                if work.exists():
                    shutil.rmtree(work)
                saved.unlink()
                pull.fsync_dir(root)
                wait = 0
            except (OSError, RuntimeError, ValueError, HTTPException) as exc:
                # Capability URLs and tokens never appear in the service log.
                print("Durham worker %d holding after %s" % (slot, type(exc).__name__), flush=True)


def main():
    state = Path(os.environ["STOARAMA_STATE_DIR"]).resolve(strict=True)
    info = state.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise DurhamError("Durham state must be private")
    credential_file = Path(os.environ["CREDENTIALS_DIRECTORY"]) / "stoarama-api-key"
    key = credential_file.read_text().strip()
    if not key:
        raise DurhamError("Durham key missing")
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    v85r = DurhamV85r(state)
    v85r.verify(v85r.tools, stop)
    direct = DurhamHTTP(os.environ["STOARAMA_API_BASE"], os.environ["STOARAMA_R2_ORIGIN"], key, stop, v85r)
    with sqlite3.connect(state / "inventory.sqlite3") as db:
        if not db.execute("SELECT 1 FROM v85r_stitch_corpus_meta WHERE sha=?", (pull.STITCH_CORPUS_SHA,)).fetchone():
            raise DurhamError("Durham offline corpus pin differs")
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        workers = [pool.submit(run_worker, direct, n, state, v85r.tools) for n in range(1, 5)]
        done, _ = concurrent.futures.wait(workers, return_when=concurrent.futures.FIRST_COMPLETED)
        stop.set()
        for worker in done:
            worker.result()


if __name__ == "__main__":
    main()
