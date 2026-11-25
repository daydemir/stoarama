import concurrent.futures
from contextlib import ExitStack
import datetime
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock
import urllib.error

import test_stoarama_pull as support

pull = support.pull


class FastCopyTests(unittest.TestCase):
    def setUp(self):
        self.resources = ExitStack()
        self.addCleanup(self.resources.close)
        tmp = self.resources.enter_context(tempfile.TemporaryDirectory())
        self.cfg = support.NASPullTests().config(Path(tmp))
        self.runtime = pull.Runtime(self.cfg)
        self.inventory = pull.Inventory(self.cfg)
        self.addCleanup(self.inventory.close)
        self.stop = threading.Event()
        self.body = b"whole-object footage"
        self.etag = '"etag-123"'
        self.calls = []
        self.patch("r2_copy_resources", return_value=pull.R2_COPY_FLOOR + 2_000_000_000)
        self.patch("storage_status", return_value={"available": True, "free_bytes": pull.R2_COPY_FLOOR + 2_000_000_000})
        self.patch("CLIENT_VERSION", pull.FAST_COPY_CLIENT_VERSION)
        self.patch("R2_COPY_TELEMETRY", pull.R2CopyTelemetry())

    def patch(self, name, *args, **kwargs):
        return self.resources.enter_context(mock.patch.object(pull, name, *args, **kwargs))

    def task(self, n=1):
        item = dict(kind="raw", source_id=n, stream_id=3, recording_id=2,
                    relative_path="p/clip-%d.mp4" % n, size_bytes=len(self.body),
                    sha256=hashlib.sha256(self.body).hexdigest())
        return dict(id=n, token="%048x" % n, etag=self.etag[1:-1], item=item,
                    policy_sha256=pull.R2_RETIRE_COPY_POLICY,
                    clip=dict(item, clip_id=n, clip_start_at="2026-09-20T00:00:00Z",
                              clip_end_at="2026-09-20T00:01:00Z"),
                    url="https://r2.example/copy/%d?signature=test" % n,
                    headers={"If-Match": [self.etag]})

    def open(self, request, timeout):
        self.assertEqual(request.get_header("If-match"), self.etag)
        response = io.BytesIO(self.body)
        response.status = 200
        response.headers = {"ETag": self.etag, "Content-Length": str(len(self.body))}
        return response

    def path(self, task):
        return self.cfg.output_dir / task["item"]["relative_path"]

    def local(self, task):
        return pull.r2_fast_copy_local(self.cfg, self.runtime, self.stop, time.monotonic() + 60, task, False)

    def run_batch(self, tasks, workers=2):
        reply = {"tasks": tasks, "workers": workers, "lease_expires_at": (datetime.datetime.now(datetime.timezone.utc)
                                                                          + datetime.timedelta(minutes=1)).isoformat()}
        control = SimpleNamespace(request_json=self.control)
        return pull.r2_fast_copy_batch(self.cfg, self.runtime, self.inventory, self.stop, control, reply, False)

    def control(self, operation, path, *, body):
        self.calls.append((operation, body))
        if operation is pull.R2CopyControlOperation.RESULT_BATCH:
            return {"verified": [r["id"] for r in body["results"] if r.get("completion")]}
        return {"ok": True}

    def test_hash_and_size_mismatches_refuse(self):
        for n, field, incorrect in [(1, "sha256", "f" * 64), (2, "size_bytes", len(self.body) - 1)]:
            task = self.task(n)
            task["item"][field] = incorrect
            with mock.patch.object(pull.urllib.request, "build_opener", return_value=SimpleNamespace(open=self.open)):
                with self.assertRaises(pull.R2CopyBlocked) as ex:
                    self.local(task)
            self.assertEqual(ex.exception.reason, pull.R2CopyBlockReason.CHECKSUM)
            self.assertFalse(self.path(task).exists())
            self.assertEqual(self.runtime.capacity_reserved_bytes, 0)

    def test_publish_refuses_a_racing_final(self):
        task = self.task()
        def raced(request, timeout):
            self.path(task).write_bytes(b"existing user's footage")
            return self.open(request, timeout)
        with mock.patch.object(pull.urllib.request, "build_opener", return_value=SimpleNamespace(open=raced)):
            with self.assertRaises(OSError) as exc:
                self.local(task)
        self.assertEqual(exc.exception.errno, errno.EEXIST)
        self.assertEqual(self.path(task).read_bytes(), b"existing user's footage")

    def test_etag_failure_returns_lease_without_replay(self):
        task = self.task()
        failed = mock.Mock(side_effect=urllib.error.HTTPError(task["url"], 412, "changed", {}, None))
        with mock.patch.object(pull.urllib.request, "build_opener", return_value=SimpleNamespace(open=failed)):
            self.assertTrue(self.run_batch([task]))
        failed.assert_called_once()
        self.assertEqual(pull.R2_COPY_TELEMETRY.holds, {"copy GET HTTP 412": 1})
        self.assertFalse(self.path(task).exists())
        self.assertEqual(self.calls, [(pull.R2CopyControlOperation.RESULT_BATCH, {
            "free_bytes": 0, "results": [{"id": task["id"], "token": task["token"]}],
        })])

    def test_bounded_streams_exact_batch_and_telemetry(self):
        active, peak = 0, 0
        lock = threading.Lock()
        ready = threading.Barrier(2)
        tasks = [self.task(n) for n in range(1, 5)]

        def overlapping(request, timeout):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                ready.wait(timeout=2)
                return self.open(request, timeout)
            finally:
                with lock:
                    active -= 1

        before = pull.CLIENT_LOG_RING.seq
        with mock.patch.object(pull.urllib.request, "build_opener", return_value=SimpleNamespace(open=overlapping)):
            with mock.patch.object(pull, "r2_copy_hash", side_effect=AssertionError("fresh transfer re-read")):
                self.assertFalse(self.run_batch(tasks))
        self.assertEqual(peak, 2)
        status = pull.r2_copy_status(self.inventory)
        self.assertEqual(status["workers"], 2)
        self.assertEqual(status["bytes_per_sec"], round(4 * len(self.body) / pull.R2_COPY_RATE_WINDOW_SEC))
        self.assertEqual([entry[0] for entry in self.calls],
                         [pull.R2CopyControlOperation.INVENTORY, pull.R2CopyControlOperation.RESULT_BATCH])
        reports = self.calls[0][1]["files"]
        self.assertEqual(len(reports), 4)
        for task, report in zip(tasks, reports):
            self.assertEqual(self.path(task).read_bytes(), self.body)
            sidecar = Path(str(self.path(task)) + ".stoarama.json")
            self.assertEqual(report["sidecar_sha256"], hashlib.sha256(sidecar.read_bytes()).hexdigest())
            self.assertEqual(report["state"], "present")
        logs, _ = pull.CLIENT_LOG_RING.since(before)
        phases = {word: json.loads(message.split(" ", 4)[4]) for message in
                  [entry["message"] for entry in logs] if message.startswith("R2 fast copy ")
                  for word in [message.split(" ")[3]]}
        self.assertTrue({"get_first_byte_ms", "transfer_ms", "hash_ms", "file_fsync_ms",
                         "directory_fsync_ms", "publish_ms"} <= phases["item"].keys())
        self.assertIn("result_ms", phases["batch"])

    def test_lease_supplies_stream_count(self):
        for workers in (0, 33, "8", None):
            with self.subTest(workers=workers), self.assertRaisesRegex(pull.R2CopyHold, "invalid copy worker count"):
                self.run_batch([self.task()], workers)
        self.assertFalse(self.path(self.task()).exists())
        self.assertEqual(self.calls, [])

    def test_rate_window_forgets_old_transfers(self):
        telemetry = pull.R2CopyTelemetry()
        with mock.patch.object(pull.time, "monotonic", return_value=1000.0):
            telemetry.transferred(600)
        with mock.patch.object(pull.time, "monotonic", return_value=1000.0 + pull.R2_COPY_RATE_WINDOW_SEC - 1):
            telemetry.transferred(300)
            self.assertEqual(telemetry.bytes_per_sec(), round(900 / pull.R2_COPY_RATE_WINDOW_SEC))
        with mock.patch.object(pull.time, "monotonic", return_value=1001.0 + pull.R2_COPY_RATE_WINDOW_SEC):
            self.assertEqual(telemetry.bytes_per_sec(), 1)
        self.assertEqual(telemetry.downloaded, 900)

    def test_worker_configuration_and_lease_telemetry(self):
        with mock.patch.dict(os.environ, {"STOARAMA_R2_FAST_COPY_REREAD": "false"}):
            cfg = pull.Config()
        self.assertFalse(hasattr(cfg, "fast_copy_workers"))
        self.assertFalse(cfg.fast_copy_reread)

        def lease(operation, path, *, body):
            self.assertIs(operation, pull.R2CopyControlOperation.LEASE)
            self.stop.set()
            return {"tasks": []}

        control = mock.MagicMock()
        control.__enter__.return_value = SimpleNamespace(request_json=lease)
        before = pull.CLIENT_LOG_RING.seq
        self.patch("R2CopyControl", return_value=control)
        pull.r2_fast_copy_worker(cfg, self.runtime, self.inventory, self.stop)
        records, _ = pull.CLIENT_LOG_RING.since(before)
        self.assertTrue(any('R2 fast copy lease {"lease_ms":' in row["message"] for row in records))

    def test_backoff_and_group_sizes(self):
        for failures in range(1, 30):
            self.assertTrue(all(2 <= pull.r2_fast_copy_backoff(failures) <= 30 for _ in range(20)))
        self.assertEqual(pull.cleanup_execution_group_size(), 8)
        for version in (pull.KEEP20_CLEANUP_CLIENT_VERSION, pull.LOW_MEMORY_CLEANUP_CLIENT_VERSION):
            with mock.patch.object(pull, "CLIENT_VERSION", version):
                self.assertEqual(pull.cleanup_execution_group_size(), 1)


if __name__ == "__main__":
    unittest.main()
