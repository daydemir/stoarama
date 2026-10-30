import hashlib
import datetime
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from test_stoarama_pull import NASPullTests, pull


class InventoryGenerationCacheTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.cfg = NASPullTests().config(Path(self.scratch.name))
        self.inventory = pull.Inventory(self.cfg)
        self.addCleanup(self.inventory.close)
        self.old_verified = "2026-08-09T00:00:00Z"
        self.clip, self.path, self.sidecar = self.linked(1, "recordings/a.mp4")

    def linked(self, identifier, relative):
        body = b"abc"
        clip = {"clip_id": identifier, "recording_id": 1, "relative_path": relative,
                "size_bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                "clip_start_at": "2026-07-29T08:00:00Z", "clip_end_at": "2026-07-29T08:01:00Z"}
        path = self.cfg.output_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        pull.write_stitch_sidecar(path, clip)
        sidecar_path = pull.stitch_sidecar_path(path)
        sidecar = (str(sidecar_path.relative_to(self.cfg.output_dir)), sidecar_path.stat().st_size,
                   hashlib.sha256(sidecar_path.read_bytes()).hexdigest())
        stat = path.stat()
        self.inventory._upsert(clip, "present", self.old_verified, stat.st_mtime_ns,
                               "scan-old", file_identity=(stat.st_ctime_ns, stat.st_ino, stat.st_dev),
                               sidecar_evidence=sidecar)
        self.inventory.db.execute("UPDATE files SET dirty=0 WHERE clip_id=?", (identifier,))
        self.inventory.db.commit()
        return clip, path, sidecar

    def current(self, clip=None, sidecar=None):
        return self.inventory._linked_scan_row_is_current(
            clip or self.clip, "scan-new", "new-pass", self.path, sidecar or self.sidecar)

    def test_new_generation_publishes_unchanged_rows_and_preserves_actual_hash_time(self):
        self.assertTrue(self.current())
        row = self.inventory.db.execute("SELECT seen_generation,scan_pass,dirty,verified_at FROM files WHERE clip_id=1").fetchone()
        self.assertEqual(row, ("scan-new", "new-pass", 1, self.old_verified))
        calls = []
        with mock.patch.object(pull, "request_json", side_effect=lambda *_a, **kw: calls.append(kw["body"]) or {}):
            self.inventory.sync_dirty(self.cfg, "scan-new", "2026-10-30T00:00:00Z")
        report = [f for body in calls for f in body.get("files", [])][0]
        self.assertEqual(report["verified_at"], self.old_verified)
        self.assertEqual(self.inventory.db.execute("SELECT dirty FROM files WHERE clip_id=1").fetchone()[0], 0)

    def test_new_generation_pass_timestamp_is_not_before_generation_start(self):
        tick = datetime.datetime(2026, 10, 30, 18, tzinfo=datetime.timezone.utc)
        def precise_clock():
            nonlocal tick
            tick += datetime.timedelta(microseconds=1)
            return tick.isoformat()
        with mock.patch.object(pull, "utc_now_precise", side_effect=precise_clock), mock.patch.object(
                pull, "request_json", return_value={}):
            self.inventory.full_scan(self.cfg, threading.Event())
        summary = self.inventory.summary()
        started = pull.parse_certification_timestamp(summary["scan_started_at"], "started")
        passed = pull.parse_certification_timestamp(summary["scan_pass_started_at"], "pass")
        self.assertGreaterEqual(passed, started)

    def test_failed_ack_preserves_retagged_row_for_identical_retry(self):
        self.assertTrue(self.current())
        with mock.patch.object(pull, "request_json", side_effect=RuntimeError("lost acknowledgement")):
            with self.assertRaises(RuntimeError):
                self.inventory.sync_dirty(self.cfg, "scan-new", "2026-10-30T00:00:00Z")
        before = self.inventory._rows("clip_id=1")[0]
        with mock.patch.object(pull, "request_json", return_value={}):
            self.inventory.sync_dirty(self.cfg, "scan-new", "2026-10-30T00:00:00Z")
        self.assertEqual(self.inventory.db.execute("SELECT verified_at FROM files WHERE clip_id=1").fetchone()[0], self.old_verified)
        self.assertEqual(before[8], self.inventory.db.execute("SELECT client_updated_at FROM files WHERE clip_id=1").fetchone()[0])

    def test_incomplete_or_invalid_prior_proof_cannot_cross_generation(self):
        for column, bad in (("verified_at", None), ("verified_at", "invalid"), ("verified_at", "2099-01-01T00:00:00Z"),
                            ("sha256", "x" * 64), ("file_ctime_ns", 0), ("file_inode", 0), ("file_device", 0),
                            ("state", "mismatch"), ("state", "missing")):
            with self.subTest(column=column, bad=bad):
                self.inventory.db.execute("SAVEPOINT proof")
                self.inventory.db.execute("UPDATE files SET %s=? WHERE clip_id=1" % column, (bad,))
                self.assertFalse(self.current())
                self.inventory.db.execute("ROLLBACK TO proof")
                self.inventory.db.execute("RELEASE proof")

    def test_modified_bytes_with_restored_mtime_cannot_reuse_hash(self):
        stat = self.path.stat()
        self.path.write_bytes(b"xyz")
        os.utime(self.path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.assertFalse(self.current())

    def test_unknown_or_reversed_registered_bounds_cannot_cross_generation(self):
        for start, end in ((None, None), ("2026-07-29T08:01:00Z", "2026-07-29T08:00:00Z")):
            with self.subTest(start=start, end=end):
                self.inventory.db.execute("SAVEPOINT bounds")
                self.inventory.db.execute("UPDATE files SET clip_start_us=?,clip_end_us=? WHERE clip_id=1", (
                    pull.optional_certification_timestamp_microseconds(start, "start"),
                    pull.optional_certification_timestamp_microseconds(end, "end")))
                clip = dict(self.clip, clip_start_at=start, clip_end_at=end)
                self.assertFalse(self.current(clip=clip))
                self.inventory.db.execute("ROLLBACK TO bounds")
                self.inventory.db.execute("RELEASE bounds")

    def test_metadata_or_sidecar_change_cannot_reuse_hash(self):
        for key, bad in (("recording_id", 2), ("size_bytes", 4), ("sha256", "d" * 64),
                         ("relative_path", "recordings/elsewhere.mp4"), ("clip_start_at", "2026-07-29T08:00:01Z")):
            with self.subTest(key=key):
                self.assertFalse(self.current(clip=dict(self.clip, **{key: bad})))
        self.assertFalse(self.current(sidecar=(self.sidecar[0], self.sidecar[1], "f" * 64)))

    def test_replaced_or_symlinked_file_cannot_reuse_hash(self):
        other = self.path.with_name("other.mp4")
        other.write_bytes(b"abc")
        self.path.unlink()
        self.path.symlink_to(other)
        self.assertFalse(self.current())
        self.path.unlink()
        other.rename(self.path)
        self.assertFalse(self.current())

    def test_unmatched_requires_exact_regular_identity_and_valid_sha(self):
        relative = "orphan.mp4"
        path = self.cfg.output_dir / relative
        path.write_bytes(b"orphan")
        stat = path.stat()
        self.inventory.db.execute("""INSERT INTO unmatched_files
            (relative_path,size_bytes,sha256,state,file_mtime_ns,file_ctime_ns,file_inode,file_device,seen_generation,client_updated_at,dirty)
            VALUES(?,?,?,'present',?,?,?,?,'scan-old',?,0)""",
            (relative, stat.st_size, hashlib.sha256(b"orphan").hexdigest(), stat.st_mtime_ns, stat.st_ctime_ns, stat.st_ino, stat.st_dev, self.old_verified))
        self.assertTrue(self.inventory._unmatched_scan_row_is_current(relative, "scan-new", "pass", stat))
        self.assertEqual(self.inventory.db.execute("SELECT seen_generation,dirty FROM unmatched_files").fetchone(), ("scan-new", 1))
        self.inventory.db.execute("UPDATE unmatched_files SET seen_generation='scan-old',sha256='invalid'")
        self.assertFalse(self.inventory._unmatched_scan_row_is_current(relative, "scan-new", "pass", stat))

    def test_scope_migration_uploads_cached_file_before_completion_without_payload_read(self):
        self.linked(2, "#recycle/protected.mp4")
        self.linked(3, "joined/protected.mp4")
        calls = []
        with mock.patch.object(pull, "sha256_file_throttled_stable", side_effect=AssertionError("unexpected payload hash")), mock.patch.object(
                pull, "request_json", side_effect=lambda *_a, **kw: calls.append(kw["body"]) or {}):
            self.inventory.full_scan(self.cfg, threading.Event())
        report_index = next(i for i, body in enumerate(calls) if any(f["clip_id"] == 1 for f in body.get("files", [])))
        complete_index = next(i for i, body in enumerate(calls) if body.get("complete"))
        self.assertLess(report_index, complete_index)
        self.assertNotEqual(calls[complete_index]["generation"], "scan-old")
        for identifier in (2, 3):
            self.assertEqual(self.inventory.db.execute("SELECT state,seen_generation FROM files WHERE clip_id=?", (identifier,)).fetchone(), ("present", "scan-old"))
        self.assertTrue((self.cfg.output_dir / "#recycle/protected.mp4").exists())
        self.assertTrue((self.cfg.output_dir / "joined/protected.mp4").exists())


if __name__ == "__main__":
    unittest.main()
