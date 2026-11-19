import hashlib
import importlib.util
import io
import mmap
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

MODULE_PATH = Path(__file__).with_name("stoarama_pull.py")
SPEC = importlib.util.spec_from_file_location("stoarama_pull_collated", MODULE_PATH)
pull = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pull)

REL = "100000_North_America_US_Key_West_Duval_Street/July/26-Sunday/100000_Duval_Street_2026_July_W4_Sunday_hour_13_part_04_130027-140016.mp4"


class Response:
    def __init__(self, body, start, total, etag="e1", status=206):
        self.status = status
        self.body = io.BytesIO(body[start:])
        self.headers = {"ETag": '"%s"' % etag}
        if status == 206:
            self.headers["Content-Range"] = "bytes %d-%d/%d" % (start, total - 1, total)

    def __enter__(self): return self
    def __exit__(self, *_): return False
    def getcode(self): return self.status
    def read(self, size=-1): return self.body.read(size)


class CollatedDownloadTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        (root / "clips").mkdir()
        self.cfg = SimpleNamespace(
            api_base="https://stoarama.test/api/v1", api_key="sir_test", origin="https://stoarama.test",
            output_dir=root / "clips", state_dir=root, min_free_bytes=100, dry_run=False, poll_interval_sec=10,
        )
        self.body = os.urandom(3 * pull.COLLATED_CHUNK_BYTES + 17)
        self.item = {
            "output_id": 7, "nas_relative_path": REL, "size_bytes": len(self.body),
            "sha256": hashlib.sha256(self.body).hexdigest(), "download_path": "/api/v1/account/collated/7/download",
        }
        self.final = self.cfg.output_dir / "joined" / REL
        self.part = self.final.with_name(".%s.collated-7.part" % self.final.name)
        self.runtime = pull.Runtime.__new__(pull.Runtime)
        self.runtime.lock = threading.Lock()
        self.runtime.capacity_reserved_bytes = 0
        self.runtime.capacity_blocked = False
        self.runtime.collated_after_id = 0
        self.runtime.collated_next_poll = 0.0
        self.requests = []
        self.acks = []
        self.errors = []

    def tearDown(self):
        self.tmp.cleanup()

    def fake_request_json(self, policy=None, items=None):
        def request_json(cfg, method, path, base=None, body=None, **_):
            if path.startswith("/account/collated?"):
                return {"policy": policy or {"enabled": True, "download_bytes_per_sec": 1 << 30, "download_parallel": 2},
                        "items": items if items is not None else [dict(self.item)],
                        "next_after_id": max((i["output_id"] for i in (items if items is not None else [self.item])), default=0)}
            if path.startswith("/api/v1/account/collated/") and path.partition("?")[0].endswith("/download"):
                return {"url": "https://r2.test/obj", "etag": "e1", "if_match": '"e1"',
                        "size_bytes": self.item["size_bytes"], "sha256": self.item["sha256"], "expires_in_sec": 3600}
            if path.partition("?")[0] == "/account/collated/ack":
                self.acks.append(body)
                return {"ok": True, "already_verified": False}
            if path.partition("?")[0] == "/account/collated/error":
                self.errors.append(body)
                return {"ok": True}
            raise AssertionError(path)
        return request_json

    def fake_open(self, request, **_):
        self.requests.append(dict(request.header_items()))
        start = int(request.get_header("Range").split("=")[1].rstrip("-"))
        return Response(self.body, start, len(self.body))

    def drain(self, *, mounted=True, **kwargs):
        self.runtime.collated_after_id = 0
        self.runtime.collated_next_poll = 0.0
        with mock.patch.object(pull, "request_json", side_effect=self.fake_request_json(**kwargs)), \
                mock.patch.object(pull, "open_joined_url", side_effect=self.fake_open), \
                mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**13, "free_bytes": 10**13}), \
                mock.patch.object(pull.os.path, "ismount", return_value=mounted), \
                mock.patch.object(pull.CollatedHandoffMount, "storage", return_value={"available": True, "free_bytes": 10**13}):
            return pull.drain_collated(self.cfg, self.runtime, threading.Event())

    def test_cursor_advances_empty_pages_and_revisits_after_exhaustion(self):
        policy = {"enabled": True, "download_bytes_per_sec": 1 << 20, "download_parallel": 1}
        pages = [{"policy": policy, "items": [], "next_after_id": n} for n in (1000, 2000, 0, 1000)]
        with mock.patch.object(pull, "request_json", side_effect=pages) as request, \
                mock.patch.object(pull.time,"monotonic",side_effect=[0,10,20,30]):
            for expected in (1000, 2000, 0, 1000):
                self.assertFalse(pull.drain_collated(self.cfg, self.runtime, threading.Event()))
                self.assertEqual(self.runtime.collated_after_id, expected)
        self.assertEqual([call.args[2].split("after_id=")[1].split("&")[0] for call in request.call_args_list], ["0", "1000", "2000", "0"])

    def test_joined_loop_paces_advancing_empty_pages(self):
        stop = threading.Event()
        scans = []
        self.runtime.joined_background_enabled = lambda: False
        def drain(cfg, runtime, event):
            scans.append(runtime.collated_after_id)
            runtime.collated_after_id += 1000
            if len(scans) == 3:
                event.set()
            return False
        with mock.patch.object(pull, "drain_collated", side_effect=drain), \
                mock.patch.object(pull.NAS_MEDIA_FENCE, "wait_maintenance", return_value=True), \
                mock.patch.object(stop, "wait", return_value=False) as wait:
            pull.joined_loop(self.cfg, self.runtime, stop)
        self.assertEqual(scans, [0, 1000, 2000])
        self.assertEqual([c.args[0] for c in wait.call_args_list], [10, 10])

    def test_empty_collated_poll_has_own_clock_during_joined_progress(self):
        stop = threading.Event()
        clock = [0.0]
        joined = []
        self.runtime.joined_background_enabled = lambda: True
        policy = {"enabled": True, "download_bytes_per_sec": 1 << 20, "download_parallel": 1}
        def deliver_joined(*args, **kwargs):
            joined.append(clock[0])
            clock[0] += 1
            if len(joined) == 4:
                stop.set()
            return True
        with mock.patch.object(pull.time,"monotonic",side_effect=lambda:clock[0]), \
                mock.patch.object(pull,"request_json",return_value={"policy":policy,"items":[],"next_after_id":1000}) as request, \
                mock.patch.object(pull,"drain_joined",side_effect=deliver_joined), \
                mock.patch.object(pull.NAS_MEDIA_FENCE,"wait_maintenance",return_value=True), \
                mock.patch.object(stop,"wait",return_value=False) as wait:
            pull.joined_loop(self.cfg,self.runtime,stop)
            self.assertEqual(request.call_count,1)
            self.assertEqual(len(joined),4)
            wait.assert_not_called()
            clock[0] = 10
            request.return_value = {"policy":policy,"items":[],"next_after_id":2000}
            self.assertFalse(pull.drain_collated(self.cfg,self.runtime,threading.Event()))
            self.assertEqual(request.call_count,2)
            self.assertEqual(self.runtime.collated_after_id,2000)

    def test_cursor_rejects_nonforward_or_wrong_type(self):
        policy = {"enabled": True, "download_bytes_per_sec": 1 << 20, "download_parallel": 1}
        self.runtime.collated_after_id = 1000
        for cursor in (True, -1, 1000, 999, "2000"):
            self.runtime.collated_next_poll = 0.0
            with mock.patch.object(pull, "request_json", return_value={"policy": policy, "items": [], "next_after_id": cursor}):
                with self.assertRaises(RuntimeError):
                    pull.drain_collated(self.cfg, self.runtime, threading.Event())
            self.assertEqual(self.runtime.collated_after_id, 1000)

    def test_contract_validation(self):
        pull.valid_collated_relative_path(REL)
        pull.valid_collated_relative_path("recordings/339/joined/2026-09-24/339_2026-09-24_hour_16_161446-170012.mp4")
        with self.assertRaises(ValueError):
            pull.valid_collated_relative_path("recordings/339/joined/2026-09-24/338_2026-09-24_hour_16_161446-170012.mp4")
        for bad in ("joined/" + REL, "managed/acct-47/" + REL, REL.replace("26-Sunday", "Sunday"), REL.replace("hour_13", "hour_24"),
                    "../" + REL, "/" + REL, REL.replace("100000_Duval", ".100000_Duval"), REL.replace("part_04", "part_4")):
            with self.assertRaises(ValueError, msg=bad):
                pull.valid_collated_relative_path(bad)
        for bad in ({"enabled": True, "download_bytes_per_sec": 10, "download_parallel": 2},
                    {"enabled": True, "download_bytes_per_sec": 1 << 20, "download_parallel": 33},
                    {"enabled": 1, "download_bytes_per_sec": 1 << 20, "download_parallel": 1}):
            with self.assertRaises(ValueError):
                pull.valid_collated_policy(bad)
        with self.assertRaises(ValueError):
            pull.valid_collated_item({**self.item, "download_path": "/api/v1/account/collated/8/download"})

    def test_downloads_verifies_links_and_acks(self):
        self.assertTrue(self.drain())
        self.assertEqual(self.final.read_bytes(), self.body)
        self.assertFalse(self.part.exists())
        self.assertEqual(self.requests[0]["If-match"], '"e1"')
        self.assertEqual(self.requests[0]["Range"], "bytes=0-")
        self.assertEqual(self.acks, [{"output_id": 7, "nas_relative_path": REL, "size_bytes": len(self.body), "sha256": self.item["sha256"]}])

    def test_resumes_partial_by_offset(self):
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(self.body[:1000])
        self.assertEqual(pull.collated_bytes_needed(self.cfg, self.item), len(self.body) - 1000)
        self.assertTrue(self.drain())
        self.assertEqual(self.requests[0]["Range"], "bytes=1000-")
        self.assertEqual(self.final.read_bytes(), self.body)

    def test_corrupt_partial_restarts_and_never_publishes(self):
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(b"x" * 1000)
        self.assertFalse(self.drain())
        self.assertFalse(self.final.exists())
        self.assertEqual(self.part.stat().st_size, 0)
        self.assertEqual(self.acks, [])
        self.assertIn("checksum mismatch", self.errors[0]["error"])
        self.assertTrue(self.drain())
        self.assertEqual(self.final.read_bytes(), self.body)

    def test_existing_identical_final_is_acked_without_download(self):
        self.final.parent.mkdir(parents=True)
        self.final.write_bytes(self.body)
        self.assertEqual(pull.collated_bytes_needed(self.cfg, self.item), 0)
        self.assertTrue(self.drain())
        self.assertEqual(self.requests, [])
        self.assertEqual(len(self.acks), 1)

    def enable_handoff(self):
        root = self.cfg.output_dir.stat()
        self.item.update(handoff_readback_round=0, handoff_mount=[root.st_dev, root.st_ino],
                         download_path=self.item["download_path"] + "?handoff_readback=1")

    def test_handoff_rejects_replacement_mount_in_new_round(self):
        self.enable_handoff()
        self.assertTrue(self.drain())
        self.cfg.output_dir.rename(self.cfg.output_dir.with_name("replaced"))
        self.cfg.output_dir.mkdir()
        self.item["handoff_readback_round"] = 1
        self.assertFalse(self.drain())
        self.assertEqual(len(self.acks), 1)
        self.assertFalse(self.final.exists())

    def test_handoff_reports_exact_final_file_identity_on_copy_and_existing(self):
        self.enable_handoff()
        self.assertTrue(self.drain())
        self.assertEqual(len(self.requests), 1)
        stat = self.final.stat()
        expected = [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_ino, stat.st_dev]
        self.assertEqual(self.acks[0]["handoff_readback"], expected)

        self.requests.clear()
        self.assertTrue(self.drain())
        self.assertEqual(self.requests, [])
        self.assertEqual(self.acks[1]["handoff_readback"], expected)

    def test_handoff_path_check_requires_each_readback_file_to_still_exist(self):
        self.enable_handoff()
        self.assertTrue(self.drain())
        body = self.acks[0]
        observation = {"id": 8, "manifest_sha256": "b" * 64, "mount": self.item["handoff_mount"],
                       "files": [[7, REL, body["handoff_readback"]]]}
        route = "/account/collated/handoff-check"
        reports = []
        for exists in (True, False):
            if not exists:
                self.final.unlink()

            def request(cfg, method, path, **kwargs):
                self.assertEqual(path, route)
                if method == "GET":
                    return {"check": observation}
                reports.append(kwargs["body"])
                return {"ok": True}

            watcher = (mock.patch.object(pull, "CollatedHandoffPathWatch", wraps=pull.CollatedHandoffPathWatch)
                       if sys.platform.startswith("linux") else mock.patch.object(pull, "CollatedHandoffPathWatch"))
            with watcher, mock.patch.object(pull, "request_json", side_effect=request), \
                    mock.patch.object(pull.os.path, "ismount", return_value=True):
                pull.check_collated_handoff_paths(self.cfg, threading.Event())

        self.assertNotIn("failed_output_id", reports[0])
        self.assertEqual(reports[1]["failed_output_id"], 7)

    @unittest.skipUnless(sys.platform.startswith("linux"), "production NAS path watch uses Linux read leases")
    def test_handoff_blocks_writes_through_a_hard_link_to_an_already_checked_file(self):
        self.enable_handoff()
        self.assertTrue(self.drain())
        alias = Path(self.tmp.name) / "hard-link-outside-share"
        os.link(self.final, alias)
        self.assertTrue(self.drain())
        second = self.final.with_name(self.final.name.replace("_part_04_", "_part_05_"))
        second.write_bytes(self.body)
        info = second.stat()
        second_relative = str(second.relative_to(self.cfg.output_dir / "joined"))
        expected_file = [info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_ino, info.st_dev]
        challenge = {"id": 8, "manifest_sha256": "b" * 64, "mount": self.item["handoff_mount"], "files": [
            [7, REL, self.acks[-1]["handoff_readback"]],
            [8, second_relative, expected_file],
        ]}
        sent = []
        def request(cfg, method, path, **kwargs):
            if method == "GET":
                return {"check": challenge}
            sent.append(kwargs["body"])
            return {"ok": True}

        open_directory = pull.open_collated_dir
        def mutate(cfg, relative, **kwargs):
            if relative == second_relative:
                with self.assertRaises(BlockingIOError):
                    os.open(alias, os.O_RDWR | os.O_NONBLOCK)
            return open_directory(cfg, relative, **kwargs)

        with mock.patch.object(pull, "request_json", side_effect=request), \
                mock.patch.object(pull.os.path, "ismount", return_value=True), \
                mock.patch.object(pull, "open_collated_dir", side_effect=mutate):
            with self.assertRaisesRegex(pull.CollatedDownloadStopped, "changed during path check"):
                pull.check_collated_handoff_paths(self.cfg, threading.Event())
        self.assertEqual(sent, [])
        with alias.open("r+b") as file:
            file.write(b"write after check")

    @unittest.skipUnless(sys.platform.startswith("linux"), "production NAS path watch uses Linux read leases")
    def test_handoff_refuses_a_file_with_a_preexisting_writable_mapping(self):
        self.enable_handoff()
        self.assertTrue(self.drain())
        challenge = {"id": 8, "manifest_sha256": "b" * 64, "mount": self.item["handoff_mount"],
                     "files": [[7, REL, self.acks[0]["handoff_readback"]]]}
        with self.final.open("r+b") as file:
            writable_mapping = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_WRITE)
        with writable_mapping, mock.patch.object(pull, "request_json", return_value={"check": challenge}) as request, \
                mock.patch.object(pull.os.path, "ismount", return_value=True):
            with self.assertRaisesRegex(pull.CollatedDownloadStopped, "file is writable during path check"):
                pull.check_collated_handoff_paths(self.cfg, threading.Event())
        self.assertEqual([call.args[1] for call in request.call_args_list], ["GET"])

    def test_handoff_refuses_unmounted_root(self):
        self.enable_handoff()
        self.assertFalse(self.drain(mounted=False))
        self.assertEqual(self.acks, [])
        self.assertFalse(self.final.exists())

    def test_handoff_rechecks_space_before_writing_a_chunk(self):
        self.enable_handoff()
        original = pull.CollatedHandoffMount.require_space
        free = {"available": True, "free_bytes": pull.R2_COPY_FLOOR}
        def running_out(mount, next_bytes):
            if next_bytes == 4096:
                return
            with mock.patch.object(pull.CollatedHandoffMount, "storage", return_value=free):
                original(mount, next_bytes)
        with mock.patch.object(pull.CollatedHandoffMount, "require_space", running_out):
            self.assertFalse(self.drain())
        self.assertEqual(self.acks, [])
        self.assertFalse(self.final.exists())

    def test_handoff_does_not_report_a_replaced_final(self):
        self.enable_handoff()
        actual = pull.hash_collated_entry

        def replace_after_readback(directory_fd, name, stop_event, *, durable=False, mount=None):
            result = actual(directory_fd, name, stop_event, durable=durable, mount=mount)
            if durable:
                alternate = self.final.with_name("replacement")
                alternate.write_bytes(self.body)
                os.replace(alternate, self.final)
            return result

        with mock.patch.object(pull, "hash_collated_entry", side_effect=replace_after_readback):
            self.assertFalse(self.drain())
        self.assertEqual(self.acks, [])
        self.assertIn("handoff path changed", self.errors[0]["error"])

    def test_existing_different_final_is_never_overwritten(self):
        self.final.parent.mkdir(parents=True)
        self.final.write_bytes(b"someone else's bytes")
        self.assertFalse(self.drain())
        self.assertEqual(self.final.read_bytes(), b"someone else's bytes")
        self.assertEqual(self.acks, [])
        self.assertIn("never overwritten", self.errors[0]["error"])

    def test_disabled_policy_downloads_nothing(self):
        self.assertFalse(self.drain(policy={"enabled": False, "download_bytes_per_sec": 1 << 20, "download_parallel": 1}, items=[]))
        self.assertEqual(self.requests, [])

    def test_free_space_reserve_pauses_without_download(self):
        with mock.patch.object(pull, "request_json", side_effect=self.fake_request_json()), \
                mock.patch.object(pull, "open_joined_url", side_effect=self.fake_open), \
                mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**6, "free_bytes": 10**6}):
            self.assertFalse(pull.drain_collated(self.cfg, self.runtime, threading.Event()))
        self.assertEqual(self.requests, [])

    def test_stop_keeps_partial_for_resume(self):
        stop = threading.Event()
        original = pull.ByteRateLimiter.acquire
        calls = []

        def acquire(limiter, size, stop_event):
            calls.append(size)
            if len(calls) == 2:
                stop.set()
            return original(limiter, size, stop_event)
        with mock.patch.object(pull, "request_json", side_effect=self.fake_request_json()), \
                mock.patch.object(pull, "open_joined_url", side_effect=self.fake_open), \
                mock.patch.object(pull.ByteRateLimiter, "acquire", acquire), \
                mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**13, "free_bytes": 10**13}):
            self.assertFalse(pull.drain_collated(self.cfg, self.runtime, stop))
        self.assertFalse(self.final.exists())
        self.assertGreater(self.part.stat().st_size, 0)
        self.assertEqual(self.acks, [])

    def test_parallel_downloads_share_one_space_reservation(self):
        # Room for exactly one output above the raw headroom: the second parallel
        # item must yield instead of passing the same free-space check.
        free = self.cfg.min_free_bytes + pull.JOINED_RAW_HEADROOM_BYTES + self.item["size_bytes"]
        second = dict(self.item, output_id=8, download_path="/api/v1/account/collated/8/download",
                      nas_relative_path=REL.replace("part_04", "part_05"))
        gate = threading.Event()
        original = pull.download_collated_item

        def slow(cfg, item, limiter, stop_event, **kwargs):
            gate.wait(2)
            return original(cfg, item, limiter, stop_event, **kwargs)
        with mock.patch.object(pull, "request_json", side_effect=self.fake_request_json(items=[dict(self.item), second])), \
                mock.patch.object(pull, "open_joined_url", side_effect=self.fake_open), \
                mock.patch.object(pull, "download_collated_item", side_effect=slow), \
                mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**13, "free_bytes": free}):
            threading.Timer(0.3, gate.set).start()
            self.assertTrue(pull.drain_collated(self.cfg, self.runtime, threading.Event()))
        self.assertEqual(len(self.acks), 1)
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)

    def test_rate_limiter_shares_one_budget(self):
        limiter = pull.ByteRateLimiter(10 * 1024 * 1024)
        started = time.monotonic()
        threads = [threading.Thread(target=limiter.acquire, args=(1024 * 1024, threading.Event())) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertGreaterEqual(time.monotonic() - started, 0.25)


if __name__ == "__main__":
    unittest.main()
