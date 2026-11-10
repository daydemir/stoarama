#!/usr/bin/env python3
"""Local NAS cleanup benchmark driver (test-only; never used on a real NAS).

  prepare  write disposable raw clips + sidecars under --root and print their
           identities as JSON (the Go harness admits them as plans)
  run      drive the real cleanup coordinator of stoarama_pull.py against a
           local API server until no work is left and print files/second

The Go test TestNASCleanupClientServerThroughput owns the server side (local
PostgreSQL, fake R2, simulated round-trip latency). The fixture root is not
a mount point, so only os.path.ismount is patched; every other filesystem
check, the pinned keep-window guard, the local caps and the ledger are real.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import importlib.util
import json
import os
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

FOLDER = "99991_asia_japan_tokyo_shinjuku9"  # stream 3 in the pinned folder map


def load_client():
    spec = importlib.util.spec_from_file_location("stoarama_pull", Path(__file__).with_name("stoarama_pull.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def identity(info):
    return [info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_ino, info.st_dev]


def prepare(args):
    root = Path(args.root)
    base = datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc)
    files = []
    for k in range(args.count):
        clip = args.first_clip + k
        relative = "%s/day-%03d/clip_%d.mp4" % (FOLDER, k // args.per_dir, clip)
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        body = hashlib.sha256(b"seed%d" % clip).digest() * (args.size // 32 + 1)
        body = body[:args.size]
        path.write_bytes(body)
        start = base + datetime.timedelta(seconds=61 * (clip - args.first_clip))
        end = start + datetime.timedelta(minutes=1)
        stamp = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
        sha = hashlib.sha256(body).hexdigest()
        sidecar = Path(str(path) + ".stoarama.json")
        sidecar.write_text(json.dumps({"schema_version": 1, "clip_id": clip, "recording_id": 2, "relative_path": relative,
                                       "size_bytes": len(body), "sha256": sha, "clip_start_at": stamp(start), "clip_end_at": stamp(end)}))
        files.append({"clip_id": clip, "relative_path": relative, "path": str(path), "size_bytes": len(body), "sha256": sha,
                      "start_at": stamp(start), "end_at": stamp(end), "frozen_identity": identity(path.stat()),
                      "frozen_sidecar_identity": identity(sidecar.stat())})
    json.dump(files, sys.stdout)


def run(args):
    pull = load_client()
    state = Path(args.state)
    state.mkdir(parents=True, exist_ok=True)
    cfg = SimpleNamespace(api_base=args.api, api_key="bench", output_dir=Path(args.root), state_dir=state,
                          inventory_file=state / "inventory.sqlite3", dry_run=False)
    inventory = pull.Inventory(cfg)
    stop = threading.Event()
    root = str(Path(args.root))
    real_ismount = os.path.ismount
    started = time.monotonic()
    cycles = 0
    errors = []
    with mock.patch.object(pull.os.path, "ismount", side_effect=lambda p: str(p) == root or real_ismount(p)), \
            mock.patch.object(pull.os, "unlink", wraps=os.unlink) as unlink, \
            concurrent.futures.ThreadPoolExecutor(max_workers=pull.CLEANUP_V2_WORKERS, thread_name_prefix="nas-cleanup") as executor:
        api = pull.CleanupBatchAPI(cfg) if args.protocol == 2 else None
        try:
            while time.monotonic() - started < args.max_seconds:
                cycles += 1
                if args.simultaneous_tasks:
                    # Sixteen real workers contend for execution leases. The
                    # server must serialize them even when each asks for many.
                    barrier = threading.Barrier(args.simultaneous_tasks)
                    progress_lock = threading.Lock()
                    completed = 0
                    def contend():
                        nonlocal completed
                        barrier.wait(timeout=10)
                        while time.monotonic() - started < args.max_seconds:
                            with progress_lock:
                                if completed >= args.simultaneous_tasks:
                                    return
                            lease = pull.cleanup_lease_v2(cfg, args.simultaneous_tasks, args.simultaneous_tasks, 1 << 30)
                            if not lease["tasks"]:
                                time.sleep(0.1)
                                continue
                            for task in lease["tasks"]:
                                pull.cleanup_process_task(cfg, inventory, stop, task, api)
                                with progress_lock:
                                    completed += 1
                    futures = [executor.submit(contend) for _ in range(args.simultaneous_tasks)]
                    for future in futures:
                        try:
                            future.result()
                        except Exception as exc:
                            errors.append(str(exc))
                    break
                if args.one_attempt:
                    lease = (pull.cleanup_lease_v2(cfg, 1, 1, 1 << 30) if args.protocol == 2
                             else pull.cleanup_lease(cfg))
                    for task in lease["tasks"]:
                        try:
                            pull.cleanup_process_task(cfg, inventory, stop, task, api)
                        except Exception as exc:
                            errors.append(str(exc))
                    break
                if args.protocol == 2:
                    retry = pull.cleanup_batch_v2(cfg, inventory, stop, executor, api)
                else:
                    retry = pull.cleanup_batch(cfg, inventory, stop, executor)
                if retry:
                    break
        finally:
            if api is not None:
                api.close()
    elapsed = time.monotonic() - started
    ledger = state / pull.CLEANUP_LEDGER_NAME
    deleted = 0
    if ledger.exists():
        deleted = sum(1 for line in ledger.read_text().splitlines() if json.loads(line).get("event") == "result" and json.loads(line).get("outcome") == "deleted")
    outcomes = []
    if inventory.db.execute("SELECT 1 FROM sqlite_master WHERE name='cleanup_execution_outcomes'").fetchone():
        outcomes = [json.loads(row[0]) for row in inventory.db.execute("SELECT body FROM cleanup_execution_outcomes ORDER BY id")]
    inventory.close()
    print("BENCH_RESULT " + json.dumps({"protocol": args.protocol, "seconds": round(elapsed, 3), "cycles": cycles,
                                       "ledger_deleted": deleted, "unlink_calls": unlink.call_count,
                                       "errors": errors, "execution_outcomes": outcomes}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--root", required=True)
    p.add_argument("--count", type=int, required=True)
    p.add_argument("--size", type=int, default=128 * 1024)
    p.add_argument("--first-clip", type=int, required=True)
    p.add_argument("--per-dir", type=int, default=500)
    r = sub.add_parser("run")
    r.add_argument("--api", required=True)
    r.add_argument("--root", required=True)
    r.add_argument("--state", required=True)
    r.add_argument("--protocol", type=int, choices=(1, 2), default=2)
    r.add_argument("--max-seconds", type=float, default=1800)
    r.add_argument("--simultaneous-tasks", type=int, default=0, help="contend for execution leases with this many workers (regression harness only)")
    r.add_argument("--one-attempt", action="store_true", help="exercise one lease without replaying an uncertain observation")
    args = parser.parse_args()
    (prepare if args.command == "prepare" else run)(args)


if __name__ == "__main__":
    main()
