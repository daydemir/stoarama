import json
import re
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import test_stoarama_pull as support

pull = support.pull
Operation = pull.R2CopyControlOperation


class R2CopyControlTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.cfg = support.NASPullTests().config(Path(tmp.name))
        self.stop = threading.Event()
        self.done = threading.Event()
        self.requests = []
        self.ring = pull.ClientLogRing()
        patch = mock.patch.object(pull, "CLIENT_LOG_RING", self.ring)
        patch.start()
        self.addCleanup(patch.stop)
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                owner.requests.append(self.path)
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                try:
                    if self.path == "/stall":
                        owner.done.wait(3)
                        return
                    if self.path == "/headers":
                        self.wfile.write(b"H")
                        self.wfile.flush()
                        owner.done.wait(3)
                        return
                    if self.path == "/redirect":
                        self.send_response(302)
                        self.send_header("Location", "/destination")
                        self.end_headers()
                        return
                    status = 409 if self.path == "/refuse" else 200
                    data = b"[]" if self.path == "/bad-json" else b'{"task":null,"token":"private-response-token"}'
                    if self.path == "/oversize":
                        data = b"x" * (1024 * 1024 + 1)
                    self.send_response(status)
                    if self.path != "/body":
                        self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.flush()
                    if self.path == "/body":
                        while not owner.done.wait(.02):
                            self.wfile.write(b" ")
                            self.wfile.flush()
                    elif status == 200:
                        self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.thread.join)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.done.set)
        self.cfg.api_base = "http://127.0.0.1:%d" % self.server.server_port
        self.control = pull.R2CopyControl(self.cfg, self.stop, 0)
        self.addCleanup(self.control.close)

    def timings(self):
        entries, _ = self.ring.since(0)
        return [entry["message"] for entry in entries if entry["message"].startswith("R2 copy lease-control ")]

    def last_values(self):
        return {key: int(value) for key, value in re.findall(r"(\w+)=(-?\d+)", self.timings()[-1])}

    def test_startup_is_paid_once_outside_request_deadline(self):
        child = pull.R2_COPY_CONTROL_CHILD.replace("emit(1)\nwhile True:", "time.sleep(.7)\nemit(1)\nwhile True:")
        popen = pull.subprocess.Popen
        with mock.patch.object(pull, "R2_COPY_CONTROL_CHILD", child), \
                mock.patch.object(pull.subprocess, "Popen", wraps=popen) as spawn:
            for operation in Operation:
                result = self.control.request_json(operation, "/ok", timeout=.5)
                self.assertIsNone(result["task"])
                pid = self.control.process.pid
            self.assertEqual(spawn.call_count, 1)
            self.assertEqual(self.control.process.pid, pid)
        values = self.last_values()
        self.assertGreaterEqual(values["startup_ready_ms"], 700)
        self.assertLess(values["total_ms"], 500)
        self.assertGreaterEqual(values["sent_to_first_byte_ms"], 0)
        self.assertEqual(values["status"], 200)
        self.control.close()
        self.assertIsNone(self.control.process)

    def test_first_byte_is_not_complete_headers_and_status_survives_body_timeout(self):
        for path, has_first, status in (("/stall", False, -1), ("/headers", True, -1), ("/body", True, 200)):
            with self.subTest(path=path):
                before = time.monotonic()
                with self.assertRaises(pull.R2CopyHold):
                    self.control.request_json(Operation.LEASE, path, timeout=.3)
                self.assertLess(time.monotonic() - before, 1.5)
                values = self.last_values()
                self.assertGreaterEqual(values["request_sent_ms"], 0)
                self.assertEqual(values["sent_to_first_byte_ms"] >= 0, has_first)
                self.assertEqual(values["status"], status)
                self.assertIsNone(self.control.process)
        self.assertEqual(len(self.requests), 3)  # No request replay.

    def test_refusals_reuse_transport_and_redirects_do_not_forward_credentials(self):
        self.control.request_json(Operation.LEASE, "/ok")
        pid = self.control.process.pid
        with self.assertRaises(urllib.error.HTTPError) as refused:
            self.control.request_json(Operation.CHECKPOINT, "/refuse")
        self.assertEqual(refused.exception.code, 409)
        self.control.request_json(Operation.RESULT, "/ok")
        self.assertEqual(self.control.process.pid, pid)
        with self.assertRaises(pull.R2CopyHold):
            self.control.request_json(Operation.LEASE, "/redirect")
        self.assertEqual(self.last_values()["status"], 302)
        self.assertNotIn("/destination", self.requests)
        self.assertIsNone(self.control.process)

    def test_bodyless_python39_http_error_keeps_refusal_and_transport(self):
        original = urllib.error.HTTPError
        class BodylessHTTPError39(original):
            def __init__(self, *args):
                super().__init__(*args)
                if self.fp is not None:
                    super().close()
                self.fp = None
            def close(self):
                # Python3.9 skips addinfourl initialization for fp=None.
                if self.fp is None:
                    raise KeyError("file")
                return super().close()
        self.control.request_json(Operation.LEASE, "/ok")
        pid = self.control.process.pid
        with mock.patch.object(pull.urllib.error, "HTTPError", BodylessHTTPError39):
            with self.assertRaises(BodylessHTTPError39) as refused:
                self.control.request_json(Operation.CHECKPOINT, "/refuse")
        self.assertIsNone(refused.exception.fp)
        self.assertEqual(refused.exception.code, 409)
        self.assertEqual(self.last_values()["status"], 409)
        self.control.request_json(Operation.RESULT, "/ok")
        self.assertEqual(self.control.process.pid, pid)

    def test_actual_log_ring_and_upload_keep_every_operation_and_cached_startup(self):
        for operation in Operation:
            self.control.request_json(operation, "/ok")
            with self.assertRaises(urllib.error.HTTPError):
                self.control.request_json(operation, "/refuse")
        payloads = []
        with mock.patch.object(pull, "request_json", side_effect=lambda *a, **kw: payloads.append(kw["body"])):
            pull.client_log_upload_once(self.cfg, SimpleNamespace(boot_id="fixture"), None, 0)
        lines = [entry["message"] for entry in payloads[0]["lines"]]
        self.assertEqual(len(lines), 11)  # READY plus success/refusal for every operation.
        for operation in Operation:
            for status in (200, 409):
                self.assertTrue(any("operation=%d " % operation.value in line and line.endswith("status=%d" % status) for line in lines))
        self.ring.max_lines = 1
        with self.assertRaises(pull.R2CopyHold):
            self.control.request_json(Operation.LEASE, "/body", timeout=.2)
        self.assertEqual(len(self.timings()), 1)  # READY has been evicted.
        self.assertGreaterEqual(self.last_values()["startup_ready_ms"], 0)
        payloads.clear()
        with mock.patch.object(pull, "request_json", side_effect=lambda *a, **kw: payloads.append(kw["body"])):
            pull.client_log_upload_once(self.cfg, SimpleNamespace(boot_id="fixture"), None, 0)
        serialized = json.dumps(payloads)
        self.assertIn("sent_to_first_byte_ms=", serialized)
        self.assertNotIn(self.cfg.api_key, serialized)
        self.assertNotIn("private-response-token", serialized)
        self.assertNotIn(self.cfg.api_base, serialized)

    def test_cancel_or_expiry_during_popen_assigns_then_reaps_without_sending(self):
        popen = pull.subprocess.Popen
        for cancelled in (False, True):
            children = []
            self.stop.clear()
            def delayed(*args, **kwargs):
                process = popen(*args, **kwargs)
                children.append(process)
                if cancelled:
                    self.stop.set()
                time.sleep(.08)
                return process
            with self.subTest(cancelled=cancelled), mock.patch.object(pull, "R2_COPY_TIMEOUT", .03), \
                    mock.patch.object(pull.subprocess, "Popen", side_effect=delayed):
                with self.assertRaises(pull.R2CopyHold):
                    self.control.request_json(Operation.LEASE, "/ok")
            self.assertTrue(all(process.poll() is not None for process in children))
            self.assertIsNone(self.control.process)
        self.assertEqual(self.requests, [])
        self.stop.clear()

    def test_unreaped_child_retains_ownership_and_drain_gate(self):
        self.control.request_json(Operation.LEASE, "/ok")
        process = self.control.process
        wait = process.wait
        entered, release = threading.Event(), threading.Event()
        calls = []
        def delayed_wait(timeout):
            calls.append(timeout)
            if len(calls) == 1:
                raise subprocess.TimeoutExpired("fixture", timeout)
            entered.set()
            release.wait(2)
            return wait(timeout=timeout)
        with mock.patch.object(process, "wait", side_effect=delayed_wait):
            worker = threading.Thread(target=self.control.close)
            worker.start()
            try:
                self.assertTrue(entered.wait(1))
                self.assertIs(self.control.process, process)
                self.assertTrue(worker.is_alive())
                ready = threading.Event()
                ready.set()
                idle = SimpleNamespace(is_alive=lambda: False)
                self.assertFalse(pull.update_can_exec(ready, idle, copy_worker=worker))
            finally:
                release.set()
                worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertIsNone(self.control.process)

    def test_blocking_dns_and_stop_remain_killable(self):
        child = pull.R2_COPY_CONTROL_CHILD.replace(
            "context=ssl.create_default_context()",
            "context=ssl.create_default_context()\nimport socket\nsocket.getaddrinfo=lambda *a,**k: time.sleep(3)")
        with mock.patch.object(pull, "R2_COPY_CONTROL_CHILD", child), self.assertRaises(pull.R2CopyHold):
            self.control.request_json(Operation.LEASE, "/ok", timeout=.3)
        self.assertEqual(self.last_values()["request_sent_ms"], -1)
        self.assertEqual(self.requests, [])
        self.assertIsNone(self.control.process)
        self.control.start()
        timer = threading.Timer(.05, self.stop.set)
        timer.start()
        try:
            before = time.monotonic()
            with self.assertRaises(pull.R2CopyHold):
                self.control.request_json(Operation.LEASE, "/stall")
            self.assertLess(time.monotonic() - before, 1)
            self.assertIsNone(self.control.process)
        finally:
            timer.join()
            self.stop.clear()

    def test_kill_failure_cannot_release_unreaped_transport_ownership(self):
        self.control.request_json(Operation.LEASE, "/ok")
        process = self.control.process
        kill, wait = process.kill, process.wait
        entered, release = threading.Event(), threading.Event()
        def delayed_wait(timeout):
            entered.set()
            release.wait(2)
            kill()
            return wait(timeout=timeout)
        with mock.patch.object(pull, "log", wraps=pull.log) as logged, \
                mock.patch.object(process, "kill", side_effect=PermissionError("fixture")), \
                mock.patch.object(process, "wait", side_effect=delayed_wait):
            worker = threading.Thread(target=self.control.close)
            worker.start()
            try:
                self.assertTrue(entered.wait(1))
                self.assertIs(self.control.process, process)
                self.assertTrue(worker.is_alive())
            finally:
                release.set()
                worker.join(2)
            logged.assert_any_call("WARN", "R2 copy held: copy control child termination failed")
            self.assertEqual(sum(call.args == ("WARN", "R2 copy held: copy control child termination failed") for call in logged.call_args_list), 1)
        self.assertFalse(worker.is_alive())
        self.assertIsNone(self.control.process)

    def test_bad_framing_bounds_and_json_poison_the_transport(self):
        for path in ("/bad-json", "/oversize"):
            with self.subTest(path=path), self.assertRaises(pull.R2CopyHold):
                self.control.request_json(Operation.LEASE, path)
            self.assertIsNone(self.control.process)
        with self.assertRaises(pull.R2CopyHold):
            self.control.request_json(Operation.LEASE, "/ok", body={"oversize": "x" * 32768})
        self.control.close()
        child = pull.R2_COPY_CONTROL_CHILD.replace("emit(1)\nwhile True:", "emit(1,2)\nwhile True:")
        with mock.patch.object(pull, "R2_COPY_CONTROL_CHILD", child), self.assertRaises(pull.R2CopyHold):
            self.control.request_json(Operation.LEASE, "/ok")
        self.assertIsNone(self.control.process)
        self.assertNotIn("/ok", self.requests)


if __name__ == "__main__":
    unittest.main()
