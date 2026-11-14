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
        self.control_patch = SimpleNamespace(original=pull.r2_copy_request_json)
        for patch in (
            mock.patch.object(pull, "NAS_MEDIA_FENCE", pull.NASMediaFence()),
            mock.patch.object(pull, "r2_copy_resources", return_value=pull.R2_COPY_FLOOR + 1_000_000_000),
            mock.patch.object(pull, "storage_status", return_value={"available": True, "free_bytes": pull.R2_COPY_FLOOR + 1_000_000_000}),
            mock.patch.object(pull.R2CopyGuard, "wait", side_effect=lambda n: None),
            mock.patch.object(pull, "r2_copy_request_json", side_effect=self.control),
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
        with mock.patch.object(pull, "r2_copy_request_json", side_effect=urllib.error.HTTPError("https://redacted", 409, "off", {}, None)):
            with self.assertRaises(urllib.error.HTTPError):
                self.run_copy()
        self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)

    def test_total_deadline_bounds_stalled_and_trickling_io_and_reaps_child(self):
        for mode in ("stall", "body", "headers"):
            with self.subTest(mode=mode):
                started, done = threading.Event(), threading.Event()
                class Handler(BaseHTTPRequestHandler):
                    def do_GET(self):
                        started.set()
                        try:
                            if mode == "headers":
                                self.wfile.write(b"HTTP/1.0 200 OK\r\nX-Trickle: ")
                                while not done.wait(.04):
                                    self.wfile.write(b"x"); self.wfile.flush()
                            else:
                                self.send_response(200); self.end_headers()
                                if mode == "stall":
                                    done.wait(2)
                                else:
                                    while not done.wait(.04):
                                        self.wfile.write(b"x" * 128); self.wfile.flush()
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                    def log_message(self, *args):
                        pass
                server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
                children = []
                popen = pull.subprocess.Popen
                def spawn(*args, **kwargs):
                    child = popen(*args, **kwargs); children.append(child); return child
                def transfer(cfg, task, offset, length, guard, fd, digest):
                    pull.r2_copy_http("http://127.0.0.1:%d/range" % server.server_port,
                                      "GET", {}, limit=length, timeout=.4, guard=guard)
                try:
                    before = time.monotonic()
                    with mock.patch.object(pull, "r2_copy_range", side_effect=transfer), mock.patch.object(pull.subprocess, "Popen", side_effect=spawn):
                        with self.assertRaises(pull.R2CopyHold):
                            self.run_copy()
                    self.assertTrue(started.is_set())
                    self.assertLess(time.monotonic() - before, 1.5)
                    self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
                    self.assertTrue(children)
                    self.assertTrue(all(p.poll() is not None for p in children))
                    self.assertEqual(pull.CLIENT_LOG_RING.scoped_secret_counts, {})
                finally:
                    done.set(); server.shutdown(); server.server_close(); thread.join()

    def test_copy_http_normal_response_and_control_deadline(self):
        done = threading.Event()
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(206); self.send_header("ETag", '"etag"'); self.end_headers(); self.wfile.write(b"bounded body")
            def do_POST(self):
                self.send_response(200); self.end_headers()
                try:
                    while not done.wait(.04):
                        self.wfile.write(b" "); self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            url = "http://127.0.0.1:%d" % server.server_port
            status, headers, data = pull.r2_copy_http(url, "GET", {}, timeout=2, limit=100)
            self.assertEqual((status, headers["ETag"], data), (206, '\"etag\"', b"bounded body"))
            # The real API wrapper uses the same hard deadline, even when the
            # server never becomes idle and never completes a JSON body.
            self.cfg.api_base = url
            before = time.monotonic()
            # setUp mocks the API boundary; call its original implementation.
            original = self.control_patch.original
            with self.assertRaises(pull.R2CopyHold):
                original(self.cfg, "POST", "/control", body={}, timeout=.4)
            self.assertLess(time.monotonic() - before, 1.5)
        finally:
            done.set(); server.shutdown(); server.server_close(); thread.join()

    def test_ephemeral_secrets_are_scoped_and_never_accumulate(self):
        ring = pull.ClientLogRing()
        stable = ring.secrets
        for n in range(1000):
            token, url = "opaque-token-%08d" % n, "https://r2.test/%d?X-Amz-Signature=opaque" % n
            with ring.scoped_secrets([token]), ring.scoped_secrets([url]):
                self.assertEqual(len(ring.scoped_secret_counts), 2)
                if n in (0, 999):
                    ring.record("WARN", token)
                    self.assertNotIn(token, ring.since(0)[0][-1]["message"])
            self.assertEqual(ring.scoped_secret_counts, {})
        self.assertEqual(ring.secrets, stable)
        global_stable = pull.CLIENT_LOG_RING.secrets
        for n in range(1000):
            payload = json.dumps({"task": {"token": "opaque-lease-%08d" % n}}).encode()
            with mock.patch.object(pull, "r2_copy_http", return_value=(200, {}, payload)):
                self.control_patch.original(self.cfg, "POST", "/account/connections/r2-copy/lease", body={})
        self.assertEqual(pull.CLIENT_LOG_RING.secrets, global_stable)
        # Nested users of one value cannot remove another active user's guard.
        with ring.scoped_secrets(["nested-private-value"]):
            with ring.scoped_secrets(["nested-private-value"]):
                pass
            self.assertIn("nested-private-value", ring.redaction_secrets())
        self.assertNotIn("nested-private-value", ring.redaction_secrets())

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
