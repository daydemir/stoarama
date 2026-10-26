import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
from pathlib import Path
from unittest import mock
import test_stoarama_pull as base

pull = base.pull

class InventoryUploadRecoveryTests(unittest.TestCase):
    def fixture(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        cfg = base.NASPullTests().config(Path(tmp.name))
        inventory = pull.Inventory(cfg)
        self.addCleanup(inventory.close)
        return cfg, inventory

    def add_clip(self, cfg, ident=1):
        clip = {"clip_id": ident, "recording_id": 1,
                "relative_path": "recordings/%d.mp4" % ident,
                "size_bytes": 1, "sha256": hashlib.sha256(b"x").hexdigest()}
        path = cfg.output_dir / clip["relative_path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
        pull.write_stitch_sidecar(path, clip)
        return clip

    def test_502_during_walk_retries_page_and_completes_without_rehashing(self):
        cfg, inventory = self.fixture()
        self.add_clip(cfg, 1)
        self.add_clip(cfg, 2)
        pages = []
        def publish(*args, **kwargs):
            pages.append(json.loads(json.dumps(kwargs["body"])))
            if len(pages) == 1:
                raise urllib.error.HTTPError("https://api.invalid", 502, "gateway", {}, None)
            return {}
        stop = threading.Event()
        with mock.patch.object(pull, "INVENTORY_SYNC_BATCH", 1), mock.patch.object(
                pull, "request_json", side_effect=publish), mock.patch.object(
                stop, "wait", return_value=False) as wait, mock.patch.object(
                pull, "sha256_file_throttled_stable", wraps=pull.sha256_file_throttled_stable) as hash_file:
            inventory.full_scan(cfg, stop)
        self.assertEqual(pages[0], pages[1])
        self.assertEqual(hash_file.call_count, 2)
        self.assertTrue(pages[-1]["complete"])
        self.assertIsNotNone(inventory.summary()["scan_completed_at"])
        self.assertEqual(inventory.summary()["scan_rows_skipped"], 0)
        wait.assert_called_once_with(1)

    def test_lost_ack_keeps_dirty_token_until_exact_page_is_acknowledged(self):
        cfg, inventory = self.fixture()
        clip = self.add_clip(cfg)
        inventory._upsert(clip, "present", pull.utc_now_precise(), 1)
        pages = []
        def publish(*args, **kwargs):
            self.assertEqual(inventory.db.execute("SELECT dirty FROM files").fetchone()[0], 1)
            pages.append(json.loads(json.dumps(kwargs["body"])))
            if len(pages) == 1:
                raise urllib.error.URLError("response lost")
            return {}
        stop = threading.Event()
        with mock.patch.object(pull, "request_json", side_effect=publish), mock.patch.object(stop, "wait", return_value=False):
            inventory.sync_dirty(cfg, "scan-test", stop_event=stop)
        self.assertEqual(pages[0], pages[1])
        self.assertEqual(inventory.db.execute("SELECT dirty FROM files").fetchone()[0], 0)

    def test_shutdown_during_retry_preserves_unacknowledged_row(self):
        cfg, inventory = self.fixture()
        clip = self.add_clip(cfg)
        inventory._upsert(clip, "present", pull.utc_now_precise(), 1)
        stop = threading.Event()
        def cancel(delay):
            stop.set()
            return True
        with mock.patch.object(pull, "request_json", side_effect=TimeoutError), mock.patch.object(stop, "wait", side_effect=cancel):
            inventory.sync_dirty(cfg, "scan-test", stop_event=stop)
        self.assertEqual(inventory.db.execute("SELECT dirty FROM files").fetchone()[0], 1)
        self.assertEqual((cfg.output_dir / clip["relative_path"]).read_bytes(), b"x")
        self.assertIsNone(inventory.summary())

    def test_permanent_rejection_is_not_retried_or_marked_clean(self):
        cfg, inventory = self.fixture()
        inventory._upsert(self.add_clip(cfg), "present", pull.utc_now_precise(), 1)
        stop = threading.Event()
        error = urllib.error.HTTPError("https://api.invalid", 403, "denied", {}, None)
        with mock.patch.object(pull, "request_json", side_effect=error) as request, mock.patch.object(stop, "wait") as wait:
            with self.assertRaises(urllib.error.HTTPError):
                inventory.sync_dirty(cfg, "scan-test", stop_event=stop)
        request.assert_called_once()
        wait.assert_not_called()
        self.assertEqual(inventory.db.execute("SELECT dirty FROM files").fetchone()[0], 1)

    def test_unmatched_page_retries_without_losing_its_dirty_row(self):
        cfg, inventory = self.fixture()
        inventory.db.execute("INSERT INTO unmatched_files(relative_path,size_bytes,sha256,state,file_mtime_ns,client_updated_at,dirty) VALUES(?,?,?,?,?,?,1)",
                             ("unknown.mp4", 7, "a" * 64, "present", 1, pull.utc_now_precise()))
        inventory.db.commit()
        pages = []
        def publish(*args, **kwargs):
            pages.append(json.loads(json.dumps(kwargs["body"])))
            if len(pages) == 1:
                raise ConnectionResetError("reset")
            return {}
        stop = threading.Event()
        with mock.patch.object(pull, "request_json", side_effect=publish), mock.patch.object(stop, "wait", return_value=False):
            inventory.sync_dirty(cfg, "scan-test", stop_event=stop)
        self.assertEqual(pages[0], pages[1])
        self.assertEqual(inventory.db.execute("SELECT dirty FROM unmatched_files").fetchone()[0], 0)

    def test_completion_page_retries_same_digest_before_local_completion(self):
        cfg, inventory = self.fixture()
        self.add_clip(cfg)
        completions = []
        def publish(*args, **kwargs):
            if kwargs["body"].get("complete"):
                completions.append(dict(kwargs["body"]))
                self.assertIsNone(inventory.summary()["scan_completed_at"])
                if len(completions) == 1:
                    raise urllib.error.HTTPError("https://api.invalid", 503, "unavailable", {}, None)
            return {}
        stop = threading.Event()
        with mock.patch.object(pull, "request_json", side_effect=publish), mock.patch.object(stop, "wait", return_value=False):
            inventory.full_scan(cfg, stop)
        self.assertEqual(completions[0], completions[1])
        self.assertIsNotNone(inventory.summary()["scan_completed_at"])

if __name__ == "__main__":
    unittest.main()
