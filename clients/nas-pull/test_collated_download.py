import hashlib
import importlib.util
import io
import os
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
        self.requests = []
        self.acks = []
        self.errors = []

    def tearDown(self):
        self.tmp.cleanup()

    def fake_request_json(self, policy=None, items=None):
        def request_json(cfg, method, path, base=None, body=None, **_):
            if path.startswith("/account/collated?"):
                return {"policy": policy or {"enabled": True, "download_bytes_per_sec": 1 << 30, "download_parallel": 2},
                        "items": items if items is not None else [dict(self.item)]}
            if path == self.item["download_path"]:
                return {"url": "https://r2.test/obj", "etag": "e1", "if_match": '"e1"',
                        "size_bytes": self.item["size_bytes"], "sha256": self.item["sha256"], "expires_in_sec": 3600}
            if path == "/account/collated/ack":
                self.acks.append(body)
                return {"ok": True, "already_verified": False}
            if path == "/account/collated/error":
                self.errors.append(body)
                return {"ok": True}
            raise AssertionError(path)
        return request_json

    def fake_open(self, request, **_):
        self.requests.append(dict(request.header_items()))
        start = int(request.get_header("Range").split("=")[1].rstrip("-"))
        return Response(self.body, start, len(self.body))

    def drain(self, **kwargs):
        with mock.patch.object(pull, "request_json", side_effect=self.fake_request_json(**kwargs)), \
                mock.patch.object(pull, "open_joined_url", side_effect=self.fake_open), \
                mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**13, "free_bytes": 10**13}):
            return pull.drain_collated(self.cfg, threading.Event())

    def test_contract_validation(self):
        pull.valid_collated_relative_path(REL)
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
        self.assertTrue(self.drain())
        self.assertEqual(self.requests, [])
        self.assertEqual(len(self.acks), 1)

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
            self.assertFalse(pull.drain_collated(self.cfg, threading.Event()))
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
            self.assertFalse(pull.drain_collated(self.cfg, stop))
        self.assertFalse(self.final.exists())
        self.assertGreater(self.part.stat().st_size, 0)
        self.assertEqual(self.acks, [])

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
