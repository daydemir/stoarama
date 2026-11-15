"""Real local locks/SQLite, synthetic media only; never production surfaces."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

import test_stoarama_pull as base

pull = base.pull


def test_backing(root):
    return Path(root).stat().st_dev, pull.InventoryScratchFilesystem.EXT4


class InventoryPathIndexTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cfg = base.NASPullTests().config(Path(self.temp.name))

    def mountinfo(self, filesystem):
        root = self.cfg.state_dir.resolve()
        dev = root.stat().st_dev
        return "1 0 %d:%d / %s rw - %s /dev/test rw\n" % (os.major(dev), os.minor(dev), root, filesystem)

    def test_backing_requires_positive_disk_filesystem_and_capacity(self):
        for fs in ("btrfs", "ext4", "xfs", "tmpfs", "ramfs", "overlay", "unknown"):
            with self.subTest(fs=fs), mock.patch.object(pull.Path, "read_text", return_value=self.mountinfo(fs)):
                if fs in ("btrfs", "ext4", "xfs"):
                    self.assertEqual(pull.inventory_scratch_backing(self.cfg.state_dir)[1].value, fs)
                else:
                    with self.assertRaises(pull.InventoryProgressError): pull.inventory_scratch_backing(self.cfg.state_dir)
        with mock.patch.object(pull.Path, "read_text", return_value=self.mountinfo("ext4")), \
                mock.patch.object(pull.os, "statvfs", return_value=SimpleNamespace(f_bavail=0, f_frsize=4096)):
            with self.assertRaisesRegex(pull.InventoryProgressError, "reserve"):
                pull.inventory_scratch_backing(self.cfg.state_dir)

    def test_index_matches_set_for_duplicates_unicode_and_uncommitted_tail(self):
        paths = ["a.mp4", "a.mp4", "A.mp4", "Česká/clip.mp4", "long/" + "x"*400,
            "surrogate/\udcff.mp4", "surrogate/\ud800.mp4", "nul/\0.mp4"] * 47
        with mock.patch.object(pull, "inventory_scratch_backing", side_effect=test_backing):
            with pull.InventoryPathIndex(self.cfg) as index:
                expected = set()
                for path in paths:
                    expected.add(path);index.add(path)
                for path in expected | {"missing.mp4", "česká/clip.mp4"}:
                    self.assertEqual(path in index, path in expected)
                self.assertEqual(index.db.execute("SELECT count(*) FROM paths").fetchone()[0], len(expected))
                self.assertEqual(index.db.execute("PRAGMA cache_size").fetchone()[0], -2048)
                index.checkpoint()
        self.assertEqual(list(self.cfg.state_dir.glob("inventory-paths-*")), [])

    def test_backing_change_and_database_errors_do_not_complete_a_scan(self):
        body = b"synthetic"
        clip = dict(clip_id=1, recording_id=1, relative_path="recordings/clip.mp4", size_bytes=len(body),
            sha256=hashlib.sha256(body).hexdigest())
        path = self.cfg.output_dir / clip["relative_path"]
        path.parent.mkdir();path.write_bytes(body);pull.write_stitch_sidecar(path, clip)
        original_connect = sqlite3.connect
        for operation in ("insert", "commit", "lookup"):
            with self.subTest(operation=operation):
                inventory = pull.Inventory(self.cfg)
                self.addCleanup(inventory.close)
                pages = []
                def connect(filename, *args, **kwargs):
                    db = original_connect(filename, *args, **kwargs)
                    if Path(filename).name != "paths.sqlite3": return db
                    proxy = mock.Mock(wraps=db)
                    def execute(sql, params=()):
                        if (operation == "insert" and sql.startswith("INSERT")) or (operation == "lookup" and sql.startswith("SELECT 1")):
                            raise sqlite3.OperationalError("synthetic storage failure")
                        return db.execute(sql, params)
                    proxy.execute.side_effect = execute
                    if operation == "commit": proxy.commit.side_effect = sqlite3.OperationalError("synthetic commit failure")
                    return proxy
                with mock.patch.object(pull, "inventory_scratch_backing", side_effect=test_backing), \
                        mock.patch.object(pull.sqlite3, "connect", side_effect=connect), \
                        mock.patch.object(pull, "request_json", side_effect=lambda *a, **k: pages.append(k["body"]) or {}):
                    with self.assertRaises((sqlite3.Error, pull.InventoryProgressError)):
                        inventory.full_scan(self.cfg, threading.Event())
                self.assertFalse(any(page.get("complete") is True for page in pages))
                self.assertIsNone(inventory.summary()["scan_completed_at"])
                self.assertEqual(list(self.cfg.state_dir.glob("inventory-paths-*")), [])

    def test_backing_change_refuses_checkpoint(self):
        with mock.patch.object(pull, "inventory_scratch_backing", side_effect=[test_backing(self.cfg.state_dir), (0, pull.InventoryScratchFilesystem.EXT4)]):
            with pull.InventoryPathIndex(self.cfg) as index:
                index.add("a")
                with self.assertRaisesRegex(pull.InventoryProgressError, "changed"): index.checkpoint()

    def test_accepted_sidecar_path_stays_linked_after_later_hash_failure(self):
        body = b"synthetic"
        clip = dict(clip_id=1, recording_id=1, relative_path="recordings/clip.mp4", size_bytes=len(body),
            sha256=hashlib.sha256(body).hexdigest())
        path = self.cfg.output_dir / clip["relative_path"]
        path.parent.mkdir();path.write_bytes(body);pull.write_stitch_sidecar(path, clip)
        inventory = pull.Inventory(self.cfg)
        self.addCleanup(inventory.close)
        with mock.patch.object(pull,"inventory_scratch_backing",side_effect=test_backing), \
                mock.patch.object(pull,"sha256_file_throttled_stable",side_effect=OSError("synthetic I/O")), \
                mock.patch.object(pull,"request_json",return_value={}):
            with self.assertRaisesRegex(RuntimeError,"inventory scan incomplete"):
                inventory.full_scan(self.cfg,threading.Event())
        self.assertEqual(inventory.db.execute("SELECT count(*) FROM unmatched_files").fetchone()[0],0)
        self.assertIsNone(inventory.summary()["scan_completed_at"])


class ProofInventoryAdmissionTests(unittest.TestCase):
    def cfg(self):
        return SimpleNamespace(stitch_proof_enabled=True)

    def stop(self, lifetime=60):
        task = dict(expires_at=(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=lifetime)).isoformat())
        return pull.StitchStop(self.cfg(), threading.Event(), task, pull.StitchRunKind.PROOF)

    def test_original_deadline_is_not_extended_by_checkpoint_refresh(self):
        stop = self.stop()
        deadline = stop.deadline
        with mock.patch.object(pull, "stitch_resources"), mock.patch.object(pull, "stitch_proof_space"), \
                mock.patch.object(pull, "stitch_proof_checkpoint"):
            stop.task["expires_at"] = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)).isoformat()
            stop.refresh()
        self.assertEqual(stop.deadline, deadline)
        with mock.patch.object(pull.time, "monotonic", return_value=deadline + .01):
            self.assertTrue(stop.is_set())
        self.assertEqual(stop.reason, "proof deadline reached")

    def test_inventory_and_native_sections_cannot_overlap(self):
        lock = threading.RLock()
        scan_entered, release_scan, native_entered, release_native, scan_again = [threading.Event() for _ in range(5)]
        errors = []
        def scan():
            with lock:
                scan_entered.set()
                if not release_scan.wait(3): errors.append("scan barrier timed out")
        def proof():
            try:
                with pull.stitch_proof_activity(SimpleNamespace(activity_lock=lock), self.stop(90)):
                    native_entered.set()
                    if not release_native.wait(3): errors.append("native barrier timed out")
            except Exception as exc: errors.append(type(exc).__name__)
        def later_scan():
            with lock: scan_again.set()
        first, native, later = threading.Thread(target=scan), threading.Thread(target=proof), threading.Thread(target=later_scan)
        first.start()
        try:
            self.assertTrue(scan_entered.wait(1));native.start()
            self.assertFalse(native_entered.is_set())
            release_scan.set();self.assertTrue(native_entered.wait(1))
            later.start();self.assertFalse(scan_again.is_set())
            release_native.set();self.assertTrue(scan_again.wait(1))
        finally:
            release_scan.set();release_native.set()
            for thread in (first,native,later):
                if thread.ident is not None: thread.join(3)
        self.assertEqual(errors, [])

    def test_near_expiry_acquisition_releases_without_entering_native(self):
        stop = self.stop()
        clock = [stop.deadline - .01]
        lock = mock.Mock()
        def acquired(**kwargs): clock[0] = stop.deadline + .01;return True
        lock.acquire.side_effect = acquired
        with mock.patch.object(pull.time, "monotonic", side_effect=lambda:clock[0]):
            with self.assertRaisesRegex(pull.StitchHold, "deadline"):
                with pull.stitch_proof_activity(SimpleNamespace(activity_lock=lock), stop): self.fail("native entered")
        lock.release.assert_called_once()

    def test_cancellation_while_waiting_does_not_release_another_owner(self):
        stop = self.stop()
        lock = mock.Mock()
        def refused(**kwargs): stop.stop.set();return False
        lock.acquire.side_effect = refused
        with self.assertRaises(pull.StitchHold):
            with pull.stitch_proof_activity(SimpleNamespace(activity_lock=lock), stop): self.fail("native entered")
        lock.release.assert_not_called()

    def test_cold_tool_cache_cannot_start_with_expired_original_budget(self):
        stop = self.stop()
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(pull.sys, "platform", "linux"), \
                mock.patch("platform.machine", return_value="x86_64"), \
                mock.patch.object(pull.time, "monotonic", return_value=stop.deadline + .01), \
                mock.patch.object(pull, "request_json") as request:
            with self.assertRaises(pull.BenchmarkError):
                pull.stitch_v85r_tools(SimpleNamespace(state_dir=Path(tmp)), stop.deadline, stop)
        request.assert_not_called()

    def test_cold_capability_response_cannot_refresh_expired_budget(self):
        stop = self.stop()
        clock = [stop.deadline - .01]
        def capability(*args, **kwargs):
            self.assertLessEqual(kwargs["timeout"], .1)
            clock[0] = stop.deadline + 1
            return {}
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(pull.sys, "platform", "linux"), \
                mock.patch("platform.machine", return_value="x86_64"), \
                mock.patch.object(pull.time, "monotonic", side_effect=lambda:clock[0]), \
                mock.patch.object(pull, "request_json", side_effect=capability), \
                mock.patch.object(pull.urllib.request, "urlopen") as download:
            with self.assertRaises(pull.BenchmarkError):
                pull.stitch_v85r_tools(SimpleNamespace(state_dir=Path(tmp)), stop.deadline, stop)
        download.assert_not_called()

    def test_setup_uses_original_deadline_and_releases_lock_after_expiry(self):
        stop = self.stop(90)
        stop.task.update(plan_sha256="a"*64, plan=dict(output_folder="proof"))
        clock = [stop.deadline - 1]
        lock = threading.RLock()
        with tempfile.TemporaryDirectory() as tmp:
            cfg = SimpleNamespace(output_dir=Path(tmp))
            def tools(cfg, deadline, actual_stop):
                self.assertIs(actual_stop, stop)
                self.assertEqual(deadline, stop.deadline)
                self.assertTrue(lock._is_owned())
                clock[0] = stop.deadline + .01
                return {}
            with mock.patch.object(pull.time,"monotonic",side_effect=lambda:clock[0]), \
                    mock.patch.object(pull,"stitch_resources"), \
                    mock.patch.object(pull.NAS_MEDIA_FENCE,"wait_maintenance",return_value=True), \
                    mock.patch.object(pull,"request_json",return_value=stop.task), \
                    mock.patch.object(pull,"stitch_proof_check"), mock.patch.object(pull,"StitchStop",return_value=stop), \
                    mock.patch.object(pull,"stitch_proof_client"), mock.patch.object(pull,"stitch_proof_space"), \
                    mock.patch.object(pull,"StitchProofDirectory"), mock.patch.object(stop,"refresh"), \
                    mock.patch.object(pull,"stitch_corpus"), mock.patch.object(pull,"stitch_v85r_tools",side_effect=tools), \
                    mock.patch.object(pull,"stitch_build") as build, mock.patch.object(pull,"log"):
                with self.assertRaisesRegex(pull.StitchHold,"deadline"):
                    pull.stitch_proof_once(cfg,SimpleNamespace(activity_lock=lock),threading.Event())
            self.assertFalse(lock._is_owned())
            build.assert_not_called()

    def test_preserved_receipt_transport_does_not_hold_scan_lock(self):
        stop = self.stop(90)
        stop.task.update(plan_sha256="a"*64, plan=dict(output_folder="proof",clips=[]))
        output = dict(relative_path="proof/proof.mp4",size_bytes=1,sha256="b"*64)
        receipt = dict(plan_sha256="a"*64,run=dict(id="synthetic"),client_artifact_sha256="c"*64,
            result=dict(outputs=[output]))
        lock = threading.RLock()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp);(root/"proof").mkdir()
            path = root/"proof/receipt.json.gz";path.write_bytes(b"synthetic")
            cfg = SimpleNamespace(output_dir=root,runtime_file=root/"runtime.json")
            def emit(*args): self.assertFalse(lock._is_owned())
            with mock.patch.object(pull,"stitch_resources"), \
                    mock.patch.object(pull.NAS_MEDIA_FENCE,"wait_maintenance",return_value=True), \
                    mock.patch.object(pull,"request_json",return_value=stop.task), \
                    mock.patch.object(pull,"stitch_proof_check"), mock.patch.object(pull,"StitchStop",return_value=stop), \
                    mock.patch.object(pull,"stitch_proof_client"), mock.patch.object(pull,"stitch_proof_space"), \
                    mock.patch.object(pull,"StitchProofDirectory"), \
                    mock.patch.object(pull,"confined_regular_file",return_value=(path,path.stat())), \
                    mock.patch.object(pull,"stitch_proof_unpack",return_value=receipt), \
                    mock.patch.object(pull,"stitch_proof_pipeline"), mock.patch.object(pull,"stitch_hash"), \
                    mock.patch.object(pull,"read_json",return_value=dict(boot_id="synthetic")), \
                    mock.patch.object(pull,"stitch_proof_emit",side_effect=emit):
                self.assertEqual(pull.stitch_proof_once(cfg,SimpleNamespace(activity_lock=lock),threading.Event()),60)


if __name__ == "__main__": unittest.main()
