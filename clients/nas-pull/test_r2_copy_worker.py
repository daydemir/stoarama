import hashlib
import json
import os
import threading
import time
import tempfile
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import test_stoarama_pull as support
pull = support.pull


class R2CopyWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cfg = support.NASPullTests().config(Path(self.tmp.name))
        self.runtime = SimpleNamespace(lock=threading.Lock(), capacity_reserved_bytes=0)
        self.runtime.release_storage_reservation = lambda n: setattr(self.runtime, "capacity_reserved_bytes", self.runtime.capacity_reserved_bytes - n)
        self.inventory = pull.Inventory(self.cfg)
        self.addCleanup(self.inventory.close)
        self.body = b"kept footage" * 400
        self.item = dict(kind="raw", source_id=901, recording_id=2, stream_id=3,
                         relative_path="p/copy.mp4", size_bytes=len(self.body), sha256=hashlib.sha256(self.body).hexdigest())
        self.clip = {**self.item, "clip_id": 901, "clip_start_at": "2026-09-20T00:00:00Z", "clip_end_at": "2026-09-20T00:01:00Z"}
        self.task = dict(id=1, token="a" * 48, item=self.item, clip=self.clip, etag="b" * 32, policy_sha256=pull.R2_RETIRE_COPY_POLICY)
        self.stop = threading.Event()
        self.calls = []
        for patch in (
            mock.patch.object(pull, "NAS_MEDIA_FENCE", pull.NASMediaFence()),
            mock.patch.object(pull, "r2_copy_resources", return_value=pull.R2_COPY_FLOOR + 1_000_000_000),
            mock.patch.object(pull, "storage_status", return_value={"available": True, "free_bytes": pull.R2_COPY_FLOOR + 1_000_000_000}),
            mock.patch.object(pull.R2CopyGuard, "wait", side_effect=lambda n: None),
            mock.patch.object(pull, "request_json", side_effect=self.control),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def control(self, cfg, method, path, **kwargs):
        self.calls.append((path, kwargs))
        if path.endswith("/range"):
            body = kwargs["body"]
            return {"url": "https://r2.test/one-range", "headers": {"Range": "bytes=%d-%d" % (body["offset"], body["offset"] + body["length"] - 1), "If-Match": '\"%s\"' % self.task["etag"]},
                    "etag": self.task["etag"], "offset": body["offset"], "length": body["length"], "max_duration_sec": 10, "range_id": "c" * 48}
        return {"active": True, "verified": True}

    def transfer(self, cfg, task, offset, length, guard, fd, digest):
        data = self.body[offset:offset + length]
        os.write(fd, data)
        digest.update(data)

    def run_copy(self):
        pull.execute_r2_copy(self.cfg, self.runtime, self.inventory, self.stop, self.task)

    def test_resume_after_interruption_publishes_exact_inventory_once_verified(self):
        def interrupted(cfg, task, offset, length, guard, fd, digest):
            os.write(fd, self.body[:100])
            raise OSError("interrupted")
        with mock.patch.object(pull, "r2_copy_range", side_effect=interrupted):
            with self.assertRaises(OSError):
                self.run_copy()
        final = self.cfg.output_dir / self.item["relative_path"]
        self.assertFalse(final.exists())
        self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)
        with mock.patch.object(pull, "r2_copy_range", side_effect=self.transfer) as transfer:
            self.run_copy()
        self.assertEqual(transfer.call_args.args[2], 100)
        self.assertEqual(final.read_bytes(), self.body)
        sidecar = json.loads(Path(str(final) + ".stoarama.json").read_text())
        self.assertEqual(sidecar["sha256"], self.item["sha256"])
        reports = [kw["body"]["files"] for path, kw in self.calls if path.endswith("/inventory")]
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0][0]["clip_id"], 901)
        self.assertEqual(reports[0][0]["sidecar_relative_path"], self.item["relative_path"] + ".stoarama.json")
        self.assertTrue(any(path.endswith("/result") for path, _ in self.calls))
        with mock.patch.object(pull, "r2_copy_range", side_effect=AssertionError("redownloaded")):
            self.run_copy()
        self.assertEqual(final.read_bytes(), self.body)

    def test_existing_conflicts_symlinks_and_bad_checksums_are_preserved(self):
        final = self.cfg.output_dir / self.item["relative_path"]
        final.parent.mkdir()
        final.write_bytes(b"different source data")
        with self.assertRaises(pull.R2CopyHold):
            self.run_copy()
        self.assertEqual(final.read_bytes(), b"different source data")
        self.assertFalse(any(path.endswith("/result") for path, _ in self.calls))
        final.unlink()  # test-owned fixture only
        target = self.cfg.output_dir / "untouched"
        target.write_bytes(b"user data")
        final.symlink_to(target)
        with self.assertRaises(OSError):
            self.run_copy()
        self.assertEqual(target.read_bytes(), b"user data")

    def test_cleanup_and_remote_off_release_fence(self):
        def start_cleanup(cfg, task, offset, length, guard, fd, digest):
            pull.NAS_MEDIA_FENCE.execution_owner = 123
            guard.check()
        with mock.patch.object(pull, "r2_copy_range", side_effect=start_cleanup):
            with self.assertRaises(pull.R2CopyHold):
                self.run_copy()
        self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)
        pull.NAS_MEDIA_FENCE.execution_owner = None
        with mock.patch.object(pull, "request_json", side_effect=urllib.error.HTTPError("https://redacted", 409, "off", {}, None)):
            with self.assertRaises(urllib.error.HTTPError):
                self.run_copy()
        self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)

    def test_stalled_network_times_out_and_releases_writer(self):
        task = self.task
        started = threading.Event()
        done = threading.Event()
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("ETag", '"%s"' % task["etag"])
                self.send_header("Content-Range", "bytes 0-%d/%d" % (task["item"]["size_bytes"] - 1, task["item"]["size_bytes"]))
                self.end_headers()
                started.set()
                done.wait(2)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.cfg.api_base = "http://127.0.0.1:%d" % server.server_port
        try:
            before = time.monotonic()
            local_opener = urllib.request.build_opener()
            def open_local(request, timeout):
                local = urllib.request.Request(self.cfg.api_base + "/range", headers=dict(request.header_items()))
                return local_opener.open(local, timeout=timeout)
            with mock.patch.object(pull.urllib.request, "build_opener", return_value=SimpleNamespace(open=open_local)), mock.patch.object(pull, "R2_COPY_TIMEOUT", .1):
                with self.assertRaises((TimeoutError, OSError)):
                    self.run_copy()
            self.assertTrue(started.is_set())
            self.assertLess(time.monotonic() - before, 1)
            self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
        finally:
            done.set()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_blocked_collated_and_update_wait(self):
        self.task["item"]["kind"] = "collated"
        with self.assertRaises(ValueError):
            self.run_copy()
        ready = threading.Event(); ready.set()
        dead = SimpleNamespace(is_alive=lambda: False)
        alive = SimpleNamespace(is_alive=lambda: True)
        self.assertFalse(pull.update_can_exec(ready, dead, copy_worker=alive))
        self.assertTrue(pull.update_can_exec(ready, dead, copy_worker=dead))


if __name__ == "__main__":
    unittest.main()
