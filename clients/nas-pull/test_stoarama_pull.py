import importlib.util
import hashlib
import json
import os
import socket
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

MODULE_PATH = Path(__file__).with_name("stoarama_pull.py")
SPEC = importlib.util.spec_from_file_location("stoarama_pull", MODULE_PATH)
pull = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pull)


class NASPullTests(unittest.TestCase):
    def config(self, root, dry_run=False):
        state = root / "state"
        clips = root / "clips"
        state.mkdir()
        clips.mkdir()
        return SimpleNamespace(
            api_base="https://stoarama.test/api/v1",
            api_key="sir_test",
            origin="https://stoarama.test",
            output_dir=clips,
            state_dir=state,
            progress_file=state / "progress.json",
            legacy_progress_file=state / "cursor.json",
            runtime_file=state / "runtime.json",
            outage_file=state / "outage.json",
            inventory_file=state / "inventory.sqlite3",
            current_file=state / "stoarama_pull.py",
            candidate_file=state / "stoarama_pull.candidate.py",
            previous_file=state / "stoarama_pull.previous.py",
            lock_file=state / "client.lock",
            update_manifest_url="https://stoarama.test/nas/download/latest.json",
            download_workers=12,
            inventory_scan_interval_sec=86400,
            inventory_scan_delay_ms=0,
            inventory_hash_mbps=1000,
            dry_run=dry_run,
            is_candidate=False,
        )

    def test_relative_path_is_required_and_confined(self):
        self.assertEqual(pull.valid_relative_path({"clip_id": 1, "relative_path": "a/b.mp4"}), Path("a/b.mp4"))
        for value in ("", "../x", "a/../x", "a\\b"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                pull.valid_relative_path({"clip_id": 1, "relative_path": value})

    def test_atomic_write_is_durable_and_replaces(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "state" / "progress.json"
            pull.atomic_write(path, b"one")
            pull.atomic_write(path, b"two")
            self.assertEqual(path.read_bytes(), b"two")
            self.assertFalse(path.with_name("progress.json.tmp").exists())

    def test_inventory_is_durable_and_syncs_checksum_proof(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            clip = {
                "clip_id": 7, "recording_id": 13, "relative_path": "recordings/clip.mp4",
                "size_bytes": 3,
                "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            }
            path = cfg.output_dir / clip["relative_path"]
            path.parent.mkdir()
            path.write_bytes(b"abc")
            inventory = pull.Inventory(cfg)
            inventory.record_verified(clip)
            calls = []
            with mock.patch.object(pull, "request_json", side_effect=lambda *_a, **kw: calls.append(kw["body"]) or {}):
                inventory.sync_clip_ids(cfg, [7])
            self.assertEqual(len(calls), 1)
            report = calls[0]["files"][0]
            self.assertEqual(report["clip_id"], 7)
            self.assertEqual(report["state"], "present")
            self.assertEqual(report["sha256"], clip["sha256"])
            self.assertIsNotNone(report["verified_at"])
            inventory.close()

            reopened = pull.Inventory(cfg)
            self.assertEqual(reopened._rows("clip_id=7")[0][2], clip["relative_path"])
            reopened.close()

    def test_inventory_adds_scan_pass_to_existing_sqlite_schema(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            legacy = sqlite3.connect(str(cfg.inventory_file))
            legacy.executescript(
                """
                CREATE TABLE files (
                    clip_id INTEGER PRIMARY KEY,recording_id INTEGER NOT NULL,relative_path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,sha256 TEXT NOT NULL,state TEXT NOT NULL,verified_at TEXT,
                    file_mtime_ns INTEGER NOT NULL DEFAULT 0,seen_generation TEXT NOT NULL DEFAULT '',
                    client_updated_at TEXT NOT NULL,dirty INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE unmatched_files (
                    relative_path TEXT PRIMARY KEY,size_bytes INTEGER NOT NULL,sha256 TEXT NOT NULL,state TEXT NOT NULL,
                    file_mtime_ns INTEGER NOT NULL DEFAULT 0,seen_generation TEXT NOT NULL DEFAULT '',
                    client_updated_at TEXT NOT NULL,dirty INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
                """
            )
            legacy.commit()
            legacy.close()
            inventory = pull.Inventory(cfg)
            for table in ("files", "unmatched_files"):
                columns = {row[1] for row in inventory.db.execute("PRAGMA table_info(%s)" % table)}
                for column in ("scan_pass", "file_ctime_ns", "file_inode", "file_device"):
                    self.assertIn(column, columns)
            inventory.close()

    def test_full_inventory_scan_reports_complete_digest_and_mismatch(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            clip = {
                "clip_id": 8, "recording_id": 13, "relative_path": "recordings/clip.mp4",
                "size_bytes": 3, "sha256": "a" * 64,
            }
            path = cfg.output_dir / clip["relative_path"]
            path.parent.mkdir()
            path.write_bytes(b"abc")
            pull.write_stitch_sidecar(path, clip)
            (cfg.output_dir / "orphan.mp4").write_bytes(b"orphan")
            (cfg.output_dir / "clip.mp4.part-8").write_bytes(b"partial")
            (cfg.output_dir / ".clip.mp4.invalid-8-123").write_bytes(b"quarantine")
            inventory = pull.Inventory(cfg)
            calls = []
            with mock.patch.object(pull, "request_json", side_effect=lambda *_a, **kw: calls.append(kw["body"]) or {}):
                inventory.full_scan(cfg, threading.Event())
            complete = [body for body in calls if body.get("complete")]
            pages = [body for body in calls if not body.get("complete")]
            self.assertEqual(len(complete), 1)
            self.assertTrue(pages)
            self.assertTrue(all(body.get("scan_started_at") == complete[0]["scan_started_at"] for body in pages))
            self.assertEqual(len(complete[0]["digest"]), 64)
            summary = inventory.summary()
            self.assertEqual(summary["mismatches"], 1)
            self.assertEqual(summary["unmatched"], 1)
            self.assertIsNotNone(summary["scan_pass_started_at"])
            self.assertEqual(summary["scan_rows_visited"], 2)
            self.assertEqual(summary["scan_rows_skipped"], 0)
            inventory.close()

    def test_full_inventory_scan_error_never_publishes_complete_or_marks_unseen_missing(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            inventory = pull.Inventory(cfg)
            prior = {
                "clip_id": 91, "recording_id": 13, "relative_path": "recordings/prior.mp4",
                "size_bytes": 3, "sha256": "b" * 64,
            }
            inventory._upsert(prior, "present", pull.utc_now_precise(), 1, "prior-generation")
            broken = cfg.output_dir / "recordings" / "broken.mp4.stoarama.json"
            broken.parent.mkdir(parents=True)
            broken.write_text("{not-json", encoding="utf-8")
            calls = []
            with mock.patch.object(pull, "request_json", side_effect=lambda *_a, **kw: calls.append(kw["body"]) or {}), self.assertRaisesRegex(
                RuntimeError, "inventory scan incomplete"
            ):
                inventory.full_scan(cfg, threading.Event())
            self.assertFalse(any(body.get("complete") for body in calls))
            self.assertEqual(inventory._rows("clip_id=91")[0][5], "present")
            self.assertIsNone(inventory.summary()["scan_completed_at"])
            inventory.close()

    def test_scan_upserts_use_bounded_durable_commits_at_100k_scale(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            inventory = pull.Inventory(cfg)
            commits = []
            inventory.db.set_trace_callback(lambda statement: commits.append(statement) if statement == "COMMIT" else None)
            for clip_id in range(1, 100001):
                clip = {
                    "clip_id": clip_id, "recording_id": 1,
                    "relative_path": "recordings/%06d.mp4" % clip_id,
                    "size_bytes": 1, "sha256": "c" * 64,
                }
                inventory._upsert(clip, "present", "2026-08-09T00:00:00Z", 1, "large-scan", commit=False)
                if clip_id % pull.INVENTORY_SYNC_BATCH == 0:
                    inventory._commit_scan_batch()
            self.assertEqual(inventory.db.execute("SELECT count(*) FROM files").fetchone()[0], 100000)
            self.assertEqual(len(commits), 100000 // pull.INVENTORY_SYNC_BATCH)
            inventory.close()

    def test_full_scan_commits_each_batch_before_server_sync(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            sha = hashlib.sha256(b"x").hexdigest()
            for clip_id in (1, 2):
                clip = {
                    "clip_id": clip_id, "recording_id": 1,
                    "relative_path": "recordings/%d.mp4" % clip_id,
                    "size_bytes": 1, "sha256": sha,
                }
                path = cfg.output_dir / clip["relative_path"]
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"x")
                pull.write_stitch_sidecar(path, clip)
            inventory = pull.Inventory(cfg)
            durable_counts = []

            def observe_sync(*_args, **kwargs):
                if kwargs["body"].get("files"):
                    other = sqlite3.connect(str(cfg.inventory_file))
                    try:
                        durable_counts.append(other.execute("SELECT count(*) FROM files").fetchone()[0])
                    finally:
                        other.close()
                return {}

            with mock.patch.object(pull, "INVENTORY_SYNC_BATCH", 2), mock.patch.object(pull, "request_json", side_effect=observe_sync):
                inventory.full_scan(cfg, threading.Event())
            self.assertTrue(durable_counts)
            self.assertGreaterEqual(durable_counts[0], 2)
            inventory.close()

    def test_full_scan_resumes_generation_without_rehashing_committed_rows(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            for clip_id, content in ((1, b"a"), (2, b"b")):
                clip = {
                    "clip_id": clip_id, "recording_id": 1,
                    "relative_path": "recordings/%d.mp4" % clip_id,
                    "size_bytes": 1, "sha256": hashlib.sha256(content).hexdigest(),
                }
                path = cfg.output_dir / clip["relative_path"]
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
                pull.write_stitch_sidecar(path, clip)
            inventory = pull.Inventory(cfg)
            stop = threading.Event()
            original_hash = pull.sha256_file_throttled

            def stop_after_first(path, mbps, stop_event):
                result = original_hash(path, mbps, stop_event)
                stop.set()
                return result

            with mock.patch.object(pull, "sha256_file_throttled", side_effect=stop_after_first), mock.patch.object(pull, "request_json", return_value={}):
                inventory.full_scan(cfg, stop)
            first = inventory.db.execute("SELECT relative_path,seen_generation FROM files").fetchone()
            self.assertIsNotNone(first)
            generation = first[1]
            inventory.close()

            resumed = pull.Inventory(cfg)
            hashed = []

            def record_hash(path, mbps, stop_event):
                relative = str(path.relative_to(cfg.output_dir))
                hashed.append(relative)
                if relative == first[0]:
                    raise AssertionError("resumed scan rehashed its committed row")
                return original_hash(path, mbps, stop_event)

            calls = []
            with mock.patch.object(pull, "sha256_file_throttled", side_effect=record_hash), mock.patch.object(
                pull, "request_json", side_effect=lambda *_a, **kw: calls.append(kw["body"]) or {}
            ):
                resumed.full_scan(cfg, threading.Event())
            complete = [body for body in calls if body.get("complete")]
            self.assertEqual(len(complete), 1)
            self.assertEqual(complete[0]["generation"], generation)
            self.assertEqual(len(hashed), 1)
            resumed.close()

    def test_resumed_scan_reconciles_sidecar_removal_and_repair(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            content = b"x"
            clip = {
                "clip_id": 7, "recording_id": 1, "relative_path": "recordings/7.mp4",
                "size_bytes": 1, "sha256": hashlib.sha256(content).hexdigest(),
            }
            path = cfg.output_dir / clip["relative_path"]
            path.parent.mkdir(parents=True)
            path.write_bytes(content)
            sidecar = pull.stitch_sidecar_path(path)
            pull.write_stitch_sidecar(path, clip)
            inventory = pull.Inventory(cfg)
            stop = threading.Event()
            original_hash = pull.sha256_file_throttled

            def interrupt(path_arg, mbps, stop_event):
                result = original_hash(path_arg, mbps, stop_event)
                stop.set()
                return result

            with mock.patch.object(pull, "sha256_file_throttled", side_effect=interrupt), mock.patch.object(pull, "request_json", return_value={}):
                inventory.full_scan(cfg, stop)
            sidecar.unlink()
            with mock.patch.object(pull, "request_json", return_value={}):
                inventory.full_scan(cfg, threading.Event())
            self.assertEqual(inventory.db.execute("SELECT state FROM files WHERE clip_id=7").fetchone()[0], "missing")
            self.assertEqual(inventory.db.execute("SELECT state FROM unmatched_files WHERE relative_path=?", (clip["relative_path"],)).fetchone()[0], "present")

            sidecar.write_text("{broken", encoding="utf-8")
            inventory._meta_set({"scan_completed_at": ""})
            with mock.patch.object(pull, "request_json", return_value={}), self.assertRaisesRegex(RuntimeError, "inventory scan incomplete"):
                inventory.full_scan(cfg, threading.Event())
            pull.write_stitch_sidecar(path, clip)
            with mock.patch.object(pull, "request_json", return_value={}):
                inventory.full_scan(cfg, threading.Event())
            self.assertEqual(inventory.db.execute("SELECT state FROM files WHERE clip_id=7").fetchone()[0], "present")
            self.assertEqual(inventory.db.execute("SELECT state FROM unmatched_files WHERE relative_path=?", (clip["relative_path"],)).fetchone()[0], "missing")
            inventory.close()

    def test_resumed_scan_rehashes_same_size_file_when_mtime_is_restored(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            clip = {
                "clip_id": 1, "recording_id": 1, "relative_path": "recordings/1.mp4",
                "size_bytes": 1, "sha256": hashlib.sha256(b"a").hexdigest(),
            }
            path = cfg.output_dir / clip["relative_path"]
            path.parent.mkdir(parents=True)
            path.write_bytes(b"a")
            pull.write_stitch_sidecar(path, clip)
            inventory = pull.Inventory(cfg)
            stop = threading.Event()
            original_hash = pull.sha256_file_throttled

            def interrupt(path_arg, mbps, stop_event):
                result = original_hash(path_arg, mbps, stop_event)
                stop.set()
                return result

            with mock.patch.object(pull, "sha256_file_throttled", side_effect=interrupt), mock.patch.object(pull, "request_json", return_value={}):
                inventory.full_scan(cfg, stop)
            before = path.stat()
            path.write_bytes(b"z")
            # Linux ctime advances even when mtime is restored; this client runs on Linux/Synology.
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
            hashed = []

            def record_hash(path_arg, mbps, stop_event):
                hashed.append(str(path_arg.relative_to(cfg.output_dir)))
                return original_hash(path_arg, mbps, stop_event)

            with mock.patch.object(pull, "sha256_file_throttled", side_effect=record_hash), mock.patch.object(pull, "request_json", return_value={}):
                inventory.full_scan(cfg, threading.Event())
            self.assertIn(clip["relative_path"], hashed)
            self.assertEqual(inventory.db.execute("SELECT state FROM files WHERE clip_id=1").fetchone()[0], "mismatch")
            inventory.close()

    def test_full_scan_rejects_atomic_replacement_during_hash(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            clip = {
                "clip_id": 1, "recording_id": 1, "relative_path": "recordings/1.mp4",
                "size_bytes": 1, "sha256": hashlib.sha256(b"a").hexdigest(),
            }
            path = cfg.output_dir / clip["relative_path"]
            path.parent.mkdir(parents=True)
            path.write_bytes(b"a")
            pull.write_stitch_sidecar(path, clip)
            inventory = pull.Inventory(cfg)
            prior_stat = path.stat()
            inventory._upsert(
                clip, "present", pull.utc_now_precise(), prior_stat.st_mtime_ns,
                "prior-generation", file_identity=(prior_stat.st_ctime_ns, prior_stat.st_ino, prior_stat.st_dev),
            )
            original_hash = pull.sha256_file_throttled

            def replace_after_hash(path_arg, mbps, stop_event):
                result = original_hash(path_arg, mbps, stop_event)
                replacement = path_arg.with_suffix(".replacement")
                replacement.write_bytes(b"z")
                os.replace(replacement, path_arg)
                return result

            calls = []
            with mock.patch.object(pull, "sha256_file_throttled", side_effect=replace_after_hash), mock.patch.object(
                pull, "request_json", side_effect=lambda *_a, **kw: calls.append(kw["body"]) or {}
            ), self.assertRaisesRegex(RuntimeError, "inventory scan incomplete"):
                inventory.full_scan(cfg, threading.Event())
            self.assertFalse(any(body.get("complete") for body in calls))
            row = inventory.db.execute("SELECT state FROM files WHERE clip_id=1").fetchone()
            self.assertEqual(row[0], "mismatch")
            published = [row for body in calls for row in body.get("files", []) if row.get("clip_id") == 1]
            self.assertTrue(published)
            self.assertEqual(published[-1]["state"], "mismatch")
            inventory.close()

    def test_resumed_pass_marks_post_start_live_row_missing_if_removed(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            original = {
                "clip_id": 1, "recording_id": 1, "relative_path": "recordings/1.mp4",
                "size_bytes": 1, "sha256": hashlib.sha256(b"a").hexdigest(),
            }
            original_path = cfg.output_dir / original["relative_path"]
            original_path.parent.mkdir(parents=True)
            original_path.write_bytes(b"a")
            pull.write_stitch_sidecar(original_path, original)
            inventory = pull.Inventory(cfg)
            stop = threading.Event()
            original_hash = pull.sha256_file_throttled

            def interrupt(path_arg, mbps, stop_event):
                result = original_hash(path_arg, mbps, stop_event)
                stop.set()
                return result

            with mock.patch.object(pull, "sha256_file_throttled", side_effect=interrupt), mock.patch.object(pull, "request_json", return_value={}):
                inventory.full_scan(cfg, stop)
            live = {
                "clip_id": 2, "recording_id": 1, "relative_path": "recordings/2.mp4",
                "size_bytes": 1, "sha256": hashlib.sha256(b"b").hexdigest(),
            }
            live_path = cfg.output_dir / live["relative_path"]
            live_path.write_bytes(b"b")
            pull.write_stitch_sidecar(live_path, live)
            inventory.record_verified(live)
            live_path.unlink()
            pull.stitch_sidecar_path(live_path).unlink()
            with mock.patch.object(pull, "request_json", return_value={}):
                inventory.full_scan(cfg, threading.Event())
            self.assertEqual(inventory.db.execute("SELECT state FROM files WHERE clip_id=2").fetchone()[0], "missing")
            inventory.close()

    def test_large_cached_resume_commits_visit_markers_in_bounded_batches(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            generation = "scan-20260809-cached"
            started_at = "2026-08-09T00:00:00Z"
            inventory = pull.Inventory(cfg)
            inventory._meta_set({"generation": generation, "scan_started_at": started_at, "scan_completed_at": "", "digest": ""})
            sha = hashlib.sha256(b"x").hexdigest()
            for clip_id in range(1, 1001):
                clip = {
                    "clip_id": clip_id, "recording_id": 1,
                    "relative_path": "recordings/%04d.mp4" % clip_id,
                    "size_bytes": 1, "sha256": sha,
                }
                path = cfg.output_dir / clip["relative_path"]
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"x")
                pull.write_stitch_sidecar(path, clip)
                stat = path.stat()
                inventory._upsert(
                    clip, "present", started_at, stat.st_mtime_ns, generation,
                    commit=False, scan_pass="prior", file_identity=(stat.st_ctime_ns, stat.st_ino, stat.st_dev),
                )
                if clip_id % pull.INVENTORY_SYNC_BATCH == 0:
                    inventory._commit_scan_batch()
            commits = []
            inventory.db.set_trace_callback(lambda statement: commits.append(statement) if statement == "COMMIT" else None)
            rehashed = []
            original_hash = pull.sha256_file_throttled

            def record_rehash(path_arg, mbps, stop_event):
                rehashed.append(str(path_arg))
                return original_hash(path_arg, mbps, stop_event)

            with mock.patch.object(pull, "sha256_file_throttled", side_effect=record_rehash), mock.patch.object(
                pull, "request_json", return_value={}
            ):
                inventory.full_scan(cfg, threading.Event())
            self.assertEqual(rehashed, [], "unchanged cached file was rehashed")
            self.assertGreaterEqual(len(commits), 1000 // pull.INVENTORY_SYNC_BATCH)
            self.assertLessEqual(len(commits), 15)
            inventory.close()

    def test_inventory_is_synced_before_release_and_sync_failure_keeps_cursor(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            clip = {"clip_id": 1, "recording_id": 3}
            calls = []

            class Inventory:
                def record_verified(self, item):
                    calls.append(("record", item["clip_id"]))

                def sync_clip_ids(self, _cfg, clip_ids):
                    calls.append(("sync", list(clip_ids)))

            with mock.patch.object(pull, "request_json", return_value={"clips": [clip]}), mock.patch.object(
                pull, "process_clip", return_value=(1, 10, 10, 0)
            ), mock.patch.object(pull, "release_clips", side_effect=lambda *_args: calls.append(("release", 1)) or {}):
                self.assertTrue(pull.drain_page(cfg, runtime, Inventory()))
            self.assertEqual(calls, [("record", 1), ("sync", [1]), ("release", 1)])
            self.assertEqual(runtime.cursor_id, 1)

            runtime = pull.Runtime(cfg)
            runtime.cursor_id = 0
            runtime.clips_pulled = 0
            calls.clear()

            class FailingInventory(Inventory):
                def sync_clip_ids(self, _cfg, clip_ids):
                    calls.append(("sync", list(clip_ids)))
                    raise RuntimeError("inventory sync failed")

            with mock.patch.object(pull, "request_json", return_value={"clips": [clip]}), mock.patch.object(
                pull, "process_clip", return_value=(1, 10, 10, 0)
            ), mock.patch.object(pull, "release_clips") as release, self.assertRaisesRegex(RuntimeError, "inventory sync failed"):
                pull.drain_page(cfg, runtime, FailingInventory())
            release.assert_not_called()
            self.assertEqual(runtime.cursor_id, 0)

    def test_storage_must_be_real_mounts(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            with mock.patch.object(pull.os.path, "ismount", return_value=False):
                with self.assertRaisesRegex(RuntimeError, "not mounted"):
                    pull.check_storage(cfg)

    def test_storage_status_only_reports_real_mount(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            opened = SimpleNamespace(st_dev=1, st_ino=2)
            usage = SimpleNamespace(f_frsize=1, f_bsize=1, f_blocks=1000, f_bavail=250)
            with mock.patch.object(pull.os, "open", return_value=9), mock.patch.object(
                pull.os, "close"
            ), mock.patch.object(pull.os, "fstat", return_value=opened), mock.patch.object(
                pull.os.path, "ismount", return_value=True
            ), mock.patch.object(pull.os, "stat", return_value=opened), mock.patch.object(
                pull.os, "fstatvfs", return_value=usage
            ):
                status = {"available": True, "total_bytes": 1000, "free_bytes": 250}
                self.assertEqual(pull.storage_status(cfg), status)
                runtime = pull.Runtime(cfg)
                runtime.set_storage(status)
                self.assertEqual(runtime.heartbeat_payload(None)["storage"], status)
                outage = {"class": "timeout", "started_at": pull.utc_now(), "failure_count": 1}
                self.assertEqual(runtime.heartbeat_payload(outage)["storage"], status)

            with mock.patch.object(pull.os, "open", return_value=9), mock.patch.object(
                pull.os, "close"
            ), mock.patch.object(pull.os, "fstat", return_value=opened), mock.patch.object(
                pull.os.path, "ismount", return_value=False
            ):
                unavailable = {"available": False}
                self.assertEqual(pull.storage_status(cfg), unavailable)
                runtime.set_storage(unavailable)
                self.assertEqual(runtime.heartbeat_payload(None)["storage"], unavailable)
                runtime.set_storage(status)
                runtime.storage_observed_monotonic -= pull.STORAGE_TELEMETRY_MAX_AGE_SEC + 1
                self.assertEqual(runtime.heartbeat_payload(None)["storage"], unavailable)

    def test_storage_status_rejects_path_identity_change(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            with mock.patch.object(pull.os, "open", return_value=9), mock.patch.object(
                pull.os, "close"
            ), mock.patch.object(pull.os, "fstat", return_value=SimpleNamespace(st_dev=1, st_ino=2)), mock.patch.object(
                pull.os.path, "ismount", return_value=True
            ), mock.patch.object(pull.os, "stat", return_value=SimpleNamespace(st_dev=3, st_ino=4)), mock.patch.object(
                pull.os, "fstatvfs"
            ) as capacity:
                self.assertEqual(pull.storage_status(cfg), {"available": False})
                capacity.assert_not_called()

    def test_download_verifies_size_and_sha(self):
        with tempfile.TemporaryDirectory() as raw:
            target = Path(raw) / "clip.part"
            response = mock.MagicMock()
            response.__enter__.return_value.read.side_effect = [b"abc", b""]
            with mock.patch.object(pull.urllib.request, "urlopen", return_value=response):
                pull.download_verified(
                    "https://example.test/clip",
                    target,
                    3,
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                )
            self.assertEqual(target.read_bytes(), b"abc")

    def test_existing_file_is_verified_before_release(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            final = cfg.output_dir / "recordings" / "clip.mp4"
            final.parent.mkdir()
            final.write_bytes(b"abc")
            clip = {
                "clip_id": 7,
                "recording_id": 13,
                "size_bytes": 3,
                "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "relative_path": "recordings/clip.mp4",
                "download_path": "/unused",
                "recording_job_id": 7001,
                "capture_generation": "sha256:generation",
                "capture_sequence": 9,
                "clip_start_at": "2026-08-03T12:00:00Z",
                "clip_end_at": "2026-08-03T12:01:00Z",
            }
            with mock.patch.object(pull, "release_clip") as release:
                self.assertEqual(pull.process_clip(cfg, clip), (7, 3, 0, 0))
                release.assert_called_once_with(cfg, 13, 7)
            sidecar = json.loads(pull.stitch_sidecar_path(final).read_text())
            self.assertEqual(sidecar["recording_job_id"], 7001)
            self.assertEqual(sidecar["capture_generation"], "sha256:generation")
            self.assertEqual(sidecar["capture_sequence"], 9)
            self.assertEqual(sidecar["sha256"], clip["sha256"])

    def test_legacy_stitch_sidecar_preserves_null_provenance(self):
        clip = {
            "clip_id": 8,
            "recording_id": 13,
            "size_bytes": 3,
            "sha256": "a" * 64,
            "relative_path": "recordings/legacy.mp4",
            "clip_start_at": "2026-08-03T12:00:00Z",
            "clip_end_at": "2026-08-03T12:01:00Z",
        }
        provenance = pull.stitch_provenance(clip)
        self.assertIsNone(provenance["recording_job_id"])
        self.assertIsNone(provenance["capture_generation"])
        self.assertIsNone(provenance["capture_sequence"])

    def test_checksum_mismatch_is_quarantined_and_redownloaded(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            final = cfg.output_dir / "recordings" / "clip.mp4"
            final.parent.mkdir()
            final.write_bytes(b"wrong")
            clip = {
                "clip_id": 7,
                "recording_id": 13,
                "size_bytes": 3,
                "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "relative_path": "recordings/clip.mp4",
                "download_path": "/account/recordings/3/clips/7/download",
            }
            with mock.patch.object(pull, "request_json", return_value={"url": "https://example.test/clip"}), mock.patch.object(
                pull, "download_verified", side_effect=lambda _url, path, *_args: path.write_bytes(b"abc")
            ), mock.patch.object(pull, "release_clip"):
                self.assertEqual(pull.process_clip(cfg, clip), (7, 3, 3, 0))
            self.assertEqual(final.read_bytes(), b"abc")
            quarantines = list(final.parent.glob(".clip.mp4.invalid-7-*"))
            self.assertEqual([path.read_bytes() for path in quarantines], [b"wrong"])
            final.write_bytes(b"wrong again")
            with mock.patch.object(pull, "request_json", return_value={"url": "https://example.test/clip"}), mock.patch.object(
                pull, "download_verified", side_effect=lambda _url, path, *_args: path.write_bytes(b"abc")
            ), mock.patch.object(pull, "release_clip"):
                self.assertEqual(pull.process_clip(cfg, clip), (7, 3, 3, 0))
            self.assertEqual(final.read_bytes(), b"abc")
            quarantines = list(final.parent.glob(".clip.mp4.invalid-7-*"))
            self.assertEqual(sorted(path.read_bytes() for path in quarantines), [b"wrong", b"wrong again"])

    def test_failed_clip_does_not_block_later_downloads_or_advance_past_it(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            clips = [{"clip_id": value, "recording_id": 1} for value in (1, 2, 3)]

            def process(_cfg, clip, release=True):
                if clip["clip_id"] == 2:
                    raise RuntimeError("poison")
                return clip["clip_id"], 10, 10, 0

            with mock.patch.object(pull, "request_json", return_value={"clips": clips}), mock.patch.object(
                pull, "process_clip", side_effect=process
            ):
                self.assertTrue(pull.drain_page(cfg, runtime))
            self.assertEqual(runtime.cursor_id, 1)
            self.assertEqual(runtime.clips_pulled, 1)
            self.assertEqual(runtime.bytes_pulled, 10)
            self.assertEqual(runtime.last_error, "1 of 3 clips failed; first clip 2: poison")
            persisted = json.loads(cfg.progress_file.read_text())
            self.assertEqual(persisted["after_id"], 1)

    def test_release_failure_is_reported_to_heartbeat(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            clip = {"clip_id": 1, "recording_id": 3}
            with mock.patch.object(pull, "request_json", return_value={"clips": [clip]}), mock.patch.object(
                pull, "process_clip", return_value=(1, 10, 10, 0)
            ), mock.patch.object(pull, "release_clips", side_effect=RuntimeError("release denied")):
                self.assertFalse(pull.drain_page(cfg, runtime))
            self.assertEqual(runtime.cursor_id, 0)
            self.assertEqual(runtime.last_error, "1 of 1 clips failed; first clip 1: release denied")

    def test_heartbeat_survives_outage_bookkeeping_failure(self):
        # A throw inside the failure handler used to escape and kill the heartbeat
        # thread, freezing the server's view of a client that kept draining.
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            stop_event = threading.Event()
            beats = []

            def flaky_post(*_args, **_kwargs):
                beats.append(1)
                if len(beats) >= 3:
                    stop_event.set()
                raise urllib.error.URLError(socket.gaierror("name resolution"))

            with mock.patch.object(pull, "request_json", side_effect=flaky_post), mock.patch.object(
                pull, "atomic_write", side_effect=OSError("no space left on device")
            ), mock.patch.object(pull, "HEARTBEAT_INTERVAL_SEC", 0):
                pull.heartbeat_loop(cfg, runtime, stop_event)

            # Loop kept beating through both the transport error and the failed
            # bookkeeping write, instead of dying on the first one.
            self.assertGreaterEqual(len(beats), 3)

    def test_update_check_runs_immediately_on_startup(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            stop_event = threading.Event()
            ready = threading.Event()
            with mock.patch.object(pull, "stage_update", return_value="new-version") as stage:
                pull.update_loop(cfg, runtime, stop_event, ready)
            stage.assert_called_once_with(cfg)
            self.assertTrue(ready.is_set())
            self.assertEqual(runtime.phase, pull.Phase.UPDATING)

    def test_exhausted_retries_are_reported_for_download_and_release(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            clips = [{"clip_id": value, "recording_id": 3} for value in (1, 2)]
            download_error = pull.RetryExhausted(RuntimeError("download failed"), 2)
            release_error = pull.RetryExhausted(RuntimeError("release failed"), 2)

            def process(_cfg, clip, release=True):
                if clip["clip_id"] == 2:
                    raise download_error
                return 1, 10, 10, 0

            with mock.patch.object(pull, "request_json", return_value={"clips": clips}), mock.patch.object(
                pull, "process_clip", side_effect=process
            ), mock.patch.object(pull, "retry_transient", side_effect=release_error):
                self.assertFalse(pull.drain_page(cfg, runtime))
            self.assertEqual(runtime.batch["retries"], 4)
            self.assertEqual(runtime.batch["failures"], 2)

    def test_retry_transient_retries_dns_and_rejects_permanent_errors(self):
        transient = urllib.error.URLError(socket.gaierror(-2, "name resolution failed"))
        operation = mock.Mock(side_effect=[transient, "ok"])
        with mock.patch.object(pull.time, "sleep"):
            self.assertEqual(pull.retry_transient(operation, 7, "download"), ("ok", 1))
        permanent = mock.Mock(side_effect=ValueError("invalid"))
        with self.assertRaisesRegex(ValueError, "invalid"):
            pull.retry_transient(permanent, 7, "download")
        self.assertEqual(permanent.call_count, 1)
        exhausted = mock.Mock(side_effect=transient)
        with mock.patch.object(pull.time, "sleep"), self.assertRaises(pull.RetryExhausted) as caught:
            pull.retry_transient(exhausted, 7, "download")
        self.assertEqual(caught.exception.retries, 2)

    def test_release_page_is_one_batch_request(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            clips = [{"clip_id": 1, "recording_id": 3}, {"clip_id": 2, "recording_id": 3}]
            with mock.patch.object(pull, "request_json", return_value={}) as request:
                pull.release_clips(cfg, clips)
            request.assert_called_once_with(cfg, "POST", "/account/clips/release", body={"clips": clips})

    def test_manifest_validation_and_transport_classification(self):
        self.assertEqual(
            pull.validate_manifest({"version": "v1", "artifact": "client-v1.py", "sha256": "a" * 64}),
            ("v1", "client-v1.py", "a" * 64),
        )
        for manifest in (
            {"version": "../v1", "artifact": "x.py", "sha256": "a" * 64},
            {"version": "v1", "artifact": "../x.py", "sha256": "a" * 64},
            {"version": "v1", "artifact": "x.py", "sha256": "bad"},
        ):
            with self.assertRaises(RuntimeError):
                pull.validate_manifest(manifest)
        error = urllib.error.URLError(socket.gaierror(-2, "name resolution failed"))
        self.assertEqual(pull.classify_transport_error(error), pull.OutageClass.DNS)

    def test_previous_exit_distinguishes_process_from_reboot(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            cfg.runtime_file.write_text(json.dumps({"boot_id": pull.boot_id(), "exit": "running"}))
            self.assertEqual(pull.Runtime(cfg).previous_exit, pull.PreviousExit.UNCLEAN_PROCESS)
            cfg.runtime_file.write_text(json.dumps({"boot_id": "different", "exit": "running"}))
            self.assertEqual(pull.Runtime(cfg).previous_exit, pull.PreviousExit.UNCLEAN_REBOOT)

    def test_legacy_cursor_is_used_when_progress_is_missing(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            cfg.legacy_progress_file.write_text(json.dumps({"after_id": 8814}))
            self.assertEqual(pull.Runtime(cfg).cursor_id, 8814)


if __name__ == "__main__":
    unittest.main()
