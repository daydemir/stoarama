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
            lock_file=root / "client.lock", runtime_file=root / "runtime.json", validate=lambda: None,
            current_file=root / "stoarama_pull.py", is_candidate=False,
            executing_release_sha256=hashlib.sha256(b"fixture release").hexdigest(),
        )
        self.cfg.current_file.write_bytes(b"fixture release")
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
        self.runtime.handoff_final_check_requested = threading.Event()
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

    def drain(self, *, mounted=True, free=None, **kwargs):
        self.runtime.collated_after_id = 0
        self.runtime.collated_next_poll = 0.0
        storage = {"available": True, "free_bytes": 10**13 if free is None else free}
        with mock.patch.object(pull, "request_json", side_effect=self.fake_request_json(**kwargs)), \
                mock.patch.object(pull, "open_joined_url", side_effect=self.fake_open), \
                mock.patch.object(pull, "storage_status", return_value=dict(storage, total_bytes=10**13)), \
                mock.patch.object(pull.os.path, "ismount", return_value=mounted), \
                mock.patch.object(pull.CollatedHandoffMount, "storage", return_value=storage):
            return pull.drain_collated(self.cfg, self.runtime, threading.Event())

    def handoff_floor(self):
        return max(pull.R2_COPY_FLOOR, self.cfg.min_free_bytes) + pull.R2_COPY_SPACE_GUARD

    def handoff_threshold(self):
        return self.cfg.min_free_bytes + pull.JOINED_RAW_HEADROOM_BYTES

    def assert_ack_matches_final(self):
        stat = self.final.stat()
        self.assertEqual(len(self.acks), 1)
        self.assertEqual(self.acks[0]["handoff_readback"],
                         [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_ino, stat.st_dev])
        self.assertEqual(self.acks[0]["handoff_readback_round"], 0)

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

    def test_collated_queue_is_not_bounded_by_page_waves(self):
        policy = {"enabled": True, "download_bytes_per_sec": 1 << 30, "download_parallel": 4}
        def item(n):
            return {"output_id": n, "nas_relative_path": REL, "size_bytes": 1, "sha256": "a" * 64,
                    "download_path": "/api/v1/account/collated/%d/download" % n}
        pages = [{"policy": policy, "items": [item(n) for n in range(1, 11)], "next_after_id": 10},
                 {"policy": policy, "items": [item(n) for n in range(11, 21)], "next_after_id": 20},
                 {"policy": policy, "items": [], "next_after_id": 0}]
        second_page_started = threading.Event()
        overlapped = []
        def download(cfg, item, *args, **kwargs):
            if item["output_id"] == 1:  # A slow first-page download.
                overlapped.append(second_page_started.wait(5))
            elif item["output_id"] > 10:
                second_page_started.set()
            return True
        acked = []
        self.runtime.reserve_joined_storage = lambda *a, **k: True
        self.runtime.release_storage_reservation = lambda needed: None
        with mock.patch.object(pull, "request_json", side_effect=pages) as request, \
                mock.patch.object(pull, "collated_bytes_needed", return_value=0), \
                mock.patch.object(pull, "storage_status", return_value={}), \
                mock.patch.object(pull, "download_collated_item", side_effect=download), \
                mock.patch.object(pull, "ack_collated_item", side_effect=lambda cfg, item, *a, **k: acked.append(item["output_id"])):
            self.assertTrue(pull.drain_collated(self.cfg, self.runtime, threading.Event()))
        # The next page's downloads ran while the first page's slow one was in flight.
        self.assertEqual(overlapped, [True])
        self.assertEqual(sorted(acked), list(range(1, 21)))
        self.assertEqual(request.call_count, 3)
        self.assertEqual(self.runtime.collated_after_id, 0)

    def test_failing_joined_artifact_backs_off_per_artifact(self):
        clock = [0.0]
        self.runtime.joined_backoff = {}
        self.runtime.joined_protocol_enabled = lambda: True
        self.runtime.set_joined_transfer_error = lambda *a: None
        self.runtime.set_joined_delivery_error = lambda *a: None
        self.runtime.clear_joined_delivery_error = lambda: None
        self.runtime.clear_joined_transfer = lambda: None
        artifact = [13467]
        downloads = []
        def download(cfg, runtime, item, stop_event):
            downloads.append((item["id"], clock[0]))
            if item["id"] == 13467:
                raise RuntimeError("joined JSON artifact is not canonical")
            return True
        with mock.patch.object(pull.time, "monotonic", side_effect=lambda: clock[0]), \
                mock.patch.object(pull, "request_json", side_effect=lambda *a, **k: {"item": {"id": artifact[0]}}), \
                mock.patch.object(pull, "valid_joined_item", side_effect=lambda raw: dict(raw, size_bytes=1)), \
                mock.patch.object(pull, "joined_output_path", return_value="x"), \
                mock.patch.object(pull, "download_joined_item", side_effect=download), \
                mock.patch.object(pull, "ack_joined_item"):
            for clock[0] in (0, 5, 9, 10, 20, 29, 30, 70):
                try:
                    pull.drain_joined(self.cfg, self.runtime, threading.Event(), report_phase=False)
                except RuntimeError:
                    pass
            artifact[0] = 13468  # Another artifact is not held back by 13467's backoff.
            self.assertTrue(pull.drain_joined(self.cfg, self.runtime, threading.Event(), report_phase=False))
        self.assertEqual(downloads, [(13467, 0), (13467, 10), (13467, 30), (13467, 70), (13468, 70)])

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

    @unittest.skipUnless(sys.platform.startswith("linux"), "recovered handoff partials publish through Linux linkat")
    def test_handoff_publishes_a_synced_full_partial_with_zero_payload_reservation(self):
        self.enable_handoff()
        size = len(self.body)
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(self.body)
        reserved = []
        original = pull.Runtime.reserve_joined_storage

        def record(runtime, cfg, storage, expected_bytes, **kwargs):
            reserved.append(expected_bytes)
            return original(runtime, cfg, storage, expected_bytes, **kwargs)

        with mock.patch.object(pull.Runtime, "reserve_joined_storage", record):
            self.assertTrue(self.drain(free=self.handoff_floor() + size - 1))
        self.assertEqual(reserved, [0])
        self.assertEqual(self.requests, [])
        self.assertEqual(self.final.read_bytes(), self.body)
        # The recovered partial keeps its hidden name: it is a hard link to the
        # published final and shares the same data blocks.
        self.assertEqual(self.part.read_bytes(), self.body)
        self.assertEqual(self.part.stat().st_ino, self.final.stat().st_ino)
        self.assertEqual(self.final.stat().st_nlink, 2)
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)
        self.assert_ack_matches_final()

    @unittest.skipUnless(sys.platform.startswith("linux"), "recovered handoff partials publish through Linux linkat")
    def test_handoff_corrupt_full_partial_clears_and_defers_to_next_reservation(self):
        self.enable_handoff()
        size = len(self.body)
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(b"c" * size)
        self.assertFalse(self.drain(free=self.handoff_floor() + size - 1))
        self.assertEqual(self.requests, [])
        self.assertEqual(self.acks, [])
        self.assertFalse(self.final.exists())
        # The corrupt inode is emptied in place, never unlinked by name, so the
        # next reservation starts a fresh full write.
        self.assertEqual(self.part.stat().st_size, 0)
        self.assertIn("full reservation follows", self.errors[0]["error"])
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)

        self.requests.clear()
        self.errors.clear()
        self.assertTrue(self.drain(free=10**13))
        self.assertEqual(self.requests[0]["Range"], "bytes=0-")
        self.assertEqual(self.final.read_bytes(), self.body)
        self.assert_ack_matches_final()

    @unittest.skipUnless(sys.platform.startswith("linux"), "recovered handoff partials publish through Linux linkat")
    def test_handoff_zero_reservation_never_fetches_when_the_candidate_vanishes(self):
        self.enable_handoff()
        size = len(self.body)
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(self.body)
        original = pull.download_collated_item

        def vanish(cfg, item, limiter, stop_event, **kwargs):
            self.assertEqual(kwargs["reserved"], 0)
            self.part.unlink()
            return original(cfg, item, limiter, stop_event, **kwargs)

        with mock.patch.object(pull, "download_collated_item", side_effect=vanish):
            self.assertFalse(self.drain(free=self.handoff_floor() + size - 1))
        self.assertEqual(self.requests, [])
        self.assertEqual(self.acks, [])
        self.assertFalse(self.final.exists())
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)
        self.assertIn("after admission", self.errors[0]["error"])

    @unittest.skipUnless(sys.platform.startswith("linux"), "recovered handoff partials publish through Linux linkat")
    def test_handoff_never_fetches_more_than_the_reserved_partial_remainder(self):
        self.enable_handoff()
        size = len(self.body)
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(self.body[:size - 1000])
        original = pull.download_collated_item

        def grow(cfg, item, limiter, stop_event, **kwargs):
            self.assertEqual(kwargs["reserved"], 1000)
            self.part.write_bytes(self.body)
            return original(cfg, item, limiter, stop_event, **kwargs)

        with mock.patch.object(pull, "download_collated_item", side_effect=grow):
            self.assertFalse(self.drain(free=10**13))
        self.assertEqual(self.requests, [])
        self.assertEqual(self.acks, [])
        self.assertFalse(self.final.exists())
        self.assertEqual(self.part.read_bytes(), self.body)
        self.assertIn("not reserved", self.errors[0]["error"])
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)

        # The grown partial is now the exact approved candidate and publishes
        # under the next (zero-payload) reservation without a GET.
        self.assertTrue(self.drain(free=self.handoff_floor() + size - 1))
        self.assertEqual(self.requests, [])
        self.assertEqual(self.final.read_bytes(), self.body)
        self.assert_ack_matches_final()

    @unittest.skipUnless(sys.platform.startswith("linux"), "recovered handoff partials publish through Linux linkat")
    def test_handoff_links_the_verified_inode_when_the_staged_name_is_swapped(self):
        self.enable_handoff()
        size = len(self.body)
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(self.body)
        replacement = b"r" * size
        actual_link = os.link
        staged = str(self.part)
        aside = self.part.with_name("verified-aside")

        def swap(src, dst, **kwargs):
            # The verified inode keeps a name, so the real linkat can resolve
            # it, while its staged name is handed to a different inode.
            if src.startswith("/proc/self/fd/"):
                os.replace(staged, aside)
                alternate = self.part.with_name("swapped")
                alternate.write_bytes(replacement)
                os.replace(alternate, staged)
            return actual_link(src, dst, **kwargs)

        with mock.patch.object(pull.os, "link", side_effect=swap):
            self.assertTrue(self.drain(free=self.handoff_floor() + size - 1))
        self.assertEqual(self.requests, [])
        # The verified bytes are published, while the substitute keeps its own
        # staged name and inode.
        self.assertEqual(self.final.read_bytes(), self.body)
        self.assertEqual(self.part.read_bytes(), replacement)
        self.assertNotEqual(self.part.stat().st_ino, self.final.stat().st_ino)
        self.assertEqual(self.final.stat().st_ino, aside.stat().st_ino)
        self.assert_ack_matches_final()

        # A second handoff readback of the existing final leaves that .part
        # exactly as it is and reports the same final identity.
        self.assertTrue(self.drain(free=self.handoff_floor() + size - 1))
        self.assertEqual(self.requests, [])
        self.assertEqual(len(self.acks), 2)
        self.assertEqual(self.part.read_bytes(), replacement)
        stat = self.final.stat()
        self.assertEqual(self.acks[1]["handoff_readback"],
                         [stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns, stat.st_ino, stat.st_dev])
        self.assertEqual(self.acks[1]["handoff_readback_round"], 0)

    @unittest.skipUnless(sys.platform.startswith("linux"), "recovered handoff partials publish through Linux linkat")
    def test_handoff_defers_when_the_verified_inode_was_already_unlinked(self):
        self.enable_handoff()
        size = len(self.body)
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(self.body)
        replacement = b"r" * size
        actual_link = os.link
        staged = str(self.part)

        # Replacing the staged name drops the verified inode's only name, so the
        # real linkat has nothing to follow: defer with no final and no fetch.
        def replace_then_link(src, dst, **kwargs):
            if src.startswith("/proc/self/fd/"):
                alternate = self.part.with_name("swapped")
                alternate.write_bytes(replacement)
                os.replace(alternate, staged)
            return actual_link(src, dst, **kwargs)

        with mock.patch.object(pull.os, "link", side_effect=replace_then_link):
            self.assertFalse(self.drain(free=self.handoff_floor() + size - 1))
        self.assertEqual(self.requests, [])
        self.assertEqual(self.acks, [])
        self.assertFalse(self.final.exists())
        self.assertEqual(self.part.read_bytes(), replacement)
        self.assertIn("already unlinked", self.errors[0]["error"])
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)

    @unittest.skipUnless(sys.platform.startswith("linux"), "recovered handoff partials publish through Linux linkat")
    def test_handoff_corrupt_candidate_keeps_its_bytes_when_clearing_is_not_safe(self):
        self.enable_handoff()
        size = len(self.body)
        self.part.parent.mkdir(parents=True)
        self.part.write_bytes(b"c" * size)
        stop_event = threading.Event()
        real_hash = pull.hash_collated_descriptor

        def hash_then_stop(descriptor, directory_fd, name, event, **kwargs):
            result = real_hash(descriptor, directory_fd, name, event, **kwargs)
            stop_event.set()
            return result

        directory_fd = os.open(self.part.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            with mock.patch.object(pull, "hash_collated_descriptor", side_effect=hash_then_stop):
                with self.assertRaises(pull.CollatedDownloadStopped):
                    pull.publish_collated_candidate(
                        self.item, directory_fd, self.final.name, self.part.name, stop_event, mount=None,
                    )
            self.assertFalse(self.final.exists())
            self.assertEqual(self.part.read_bytes(), b"c" * size)

            def changed_mount():
                return SimpleNamespace(
                    check_child=lambda fd: None,
                    check=lambda: (_ for _ in ()).throw(
                        pull.CollatedDownloadStopped("collated handoff root mount changed")),
                )

            with self.assertRaises(pull.CollatedDownloadStopped):
                pull.publish_collated_candidate(
                    self.item, directory_fd, self.final.name, self.part.name, threading.Event(),
                    mount=changed_mount(),
                )
        finally:
            os.close(directory_fd)
        self.assertFalse(self.final.exists())
        self.assertEqual(self.part.read_bytes(), b"c" * size)

    def test_handoff_feed_drains_before_starting_other_delivery(self):
        page = {"policy": {"enabled": True, "download_bytes_per_sec": 1 << 30, "download_parallel": 2,
                           "handoff_check": True}, "items": [dict(self.item)], "next_after_id": 7}
        with mock.patch.object(pull, "request_json", return_value=page), \
                mock.patch.object(pull, "download_collated_item") as download:
            self.assertFalse(pull.drain_collated(self.cfg, self.runtime, threading.Event()))
        self.assertTrue(self.runtime.handoff_final_check_requested.is_set())
        self.assertFalse(pull.collated_handoff_pending(self.cfg).exists())
        download.assert_not_called()

    @unittest.skipUnless(sys.platform.startswith("linux"), "production NAS path watch uses Linux read leases")
    def test_handoff_writer_lock_survives_a_lost_completion_response(self):
        self.enable_handoff()
        self.assertTrue(self.drain())
        challenge = {"id": 8, "manifest_sha256": "b" * 64, "mount": self.item["handoff_mount"],
                     "files": [[7, REL, self.acks[0]["handoff_readback"]]]}

        def lost_reply(cfg, method, path, **kwargs):
            if method == "GET":
                return {"check": challenge}
            with self.assertRaisesRegex(RuntimeError, "another NAS pull client"):
                pull.acquire_lock(self.cfg)
            self.assertTrue(pull.collated_handoff_pending(self.cfg).is_file())
            self.assertEqual(pull.read_json(self.cfg.runtime_file, {})["exit"], "delivery_handoff_check")
            self.assertNotIn("files", pull.read_json(pull.collated_handoff_pending(self.cfg), {}))
            raise TimeoutError("response lost")

        with pull.acquire_lock(self.cfg):
            with self.assertRaisesRegex(RuntimeError, "another NAS pull client"):
                pull.finalize_collated_handoff(self.cfg)
        with mock.patch.object(pull, "request_json", side_effect=lost_reply), \
                mock.patch.object(pull.os.path, "ismount", return_value=True):
            with self.assertRaises(TimeoutError):
                pull.finalize_collated_handoff(self.cfg)

        with self.assertRaisesRegex(RuntimeError, "acceptance is unresolved"):
            pull.acquire_lock(self.cfg)
        replies = [
            {"status": "superseded"}, {"check": dict(challenge, id=9)}, {"ok": 1},
        ]
        with mock.patch.object(pull, "request_json", side_effect=replies) as request, \
                mock.patch.object(pull.os.path, "ismount", return_value=True):
            with self.assertRaisesRegex(RuntimeError, "acceptance is unresolved"):
                pull.finalize_collated_handoff(self.cfg)
        self.assertEqual(request.call_args_list[0].args[1:], ("GET", "/account/collated/handoff-check?check_id=8"))
        self.assertEqual(request.call_args.kwargs["body"]["id"], 9)
        self.assertEqual(pull.read_json(pull.collated_handoff_pending(self.cfg), {})["id"], 9)
        with mock.patch.object(pull, "request_json", side_effect=[{"status": "cancelled"}, {"check": None}]):
            pull.finalize_collated_handoff(self.cfg)
        self.assertFalse(pull.collated_handoff_pending(self.cfg).exists())
        with pull.acquire_lock(self.cfg):
            pass

    def test_handoff_cannot_journal_until_bootstrap_current_slot_matches(self):
        binding = {"id": 8, "manifest_sha256": "b" * 64, "mount": [1, 2]}
        self.cfg.is_candidate = True
        with self.assertRaisesRegex(pull.CollatedDownloadStopped, "current client slot"):
            pull.persist_collated_handoff(self.cfg, binding)
        self.assertFalse(pull.collated_handoff_pending(self.cfg).exists())

        self.cfg.is_candidate = False
        self.cfg.current_file.write_bytes(b"older cached release")
        with self.assertRaisesRegex(pull.CollatedDownloadStopped, "current client slot"):
            pull.persist_collated_handoff(self.cfg, binding)
        self.assertFalse(pull.collated_handoff_pending(self.cfg).exists())

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
