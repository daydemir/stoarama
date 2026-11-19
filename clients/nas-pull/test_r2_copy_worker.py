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
        self.control_client = SimpleNamespace(request_json=self.control)
        for patch in (
            mock.patch.object(pull, "R2_COPY_TELEMETRY", pull.R2CopyTelemetry()),
            mock.patch.object(pull, "NAS_MEDIA_FENCE", pull.NASMediaFence()),
            mock.patch.object(pull, "NAS_COPY_PATHS", pull.NASCopyPaths()),
            mock.patch.object(pull, "CLEANUP_DIRECTORIES", pull.CleanupDirectoryLog()),
            mock.patch.object(pull, "r2_copy_resources", return_value=pull.R2_COPY_FLOOR + 1_000_000_000),
            mock.patch.object(pull, "storage_status", return_value={"available": True, "free_bytes": pull.R2_COPY_FLOOR + 1_000_000_000}),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def control(self, operation, path, **kwargs):
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
        pull.execute_r2_copy(self.cfg, self.runtime, self.inventory, self.stop, self.task, self.control_client)

    def test_staged_stitch_certifies_published_bytes_and_reopens_after_lost_result(self):
        output = "fixture_V85r_keep28/2/2026-09-20T00-00-00Z/run-000-%s.mp4" % self.item["sha256"]
        self.item.update(kind="stitch_output", object_key="stitch-staging/generation/output.mp4", relative_path=output)
        self.clip["relative_path"] = output
        published = self.cfg.output_dir / output
        receipts = []
        def control(operation, path, **kwargs):
            if path.endswith("/result"):
                receipt = kwargs["body"]["stitch_receipt"]
                self.assertEqual(published.read_bytes(), self.body)
                self.assertEqual(receipt["identity"], pull.cleanup_identity(published.stat()))
                self.assertEqual(receipt["sha256"], self.item["sha256"])
                self.assertTrue(receipt["fsynced"])
                receipts.append(receipt)
                if len(receipts) == 1:
                    raise pull.R2CopyHold("interrupted result")
            self.assertFalse(path.endswith("/inventory"))
            return self.control(operation, path, **kwargs)
        with mock.patch.object(pull, "r2_copy_range", side_effect=self.transfer), \
                mock.patch.object(self.control_client, "request_json", side_effect=control):
            with self.assertRaises(pull.R2CopyHold):
                self.run_copy()
        with mock.patch.object(pull, "r2_copy_range", side_effect=AssertionError("staged output was downloaded twice")), \
                mock.patch.object(self.control_client, "request_json", side_effect=control):
            self.run_copy()
        self.assertEqual(len(receipts), 2)
        self.assertEqual(receipts[0], receipts[1])
        self.assertFalse(Path(str(published) + ".stoarama.json").exists())

    def test_verify_only_missing_parent_and_media_never_create_or_download(self):
        self.task["work_mode"] = "verify_existing"
        parent = self.cfg.output_dir / "p"
        for existing_parent in (False, True):
            with self.subTest(existing_parent=existing_parent):
                if existing_parent:
                    parent.mkdir()
                with mock.patch.object(pull, "r2_copy_range", side_effect=AssertionError("download")):
                    self.run_copy()
                self.assertEqual(parent.exists(), existing_parent)
                self.assertFalse((parent / "copy.mp4").exists())
                self.assertFalse(list(parent.glob("*.part")))
                self.assertTrue(any(path.endswith("/block") and kw["body"]["reason"] == "verify_missing" for path, kw in self.calls))
                with self.inventory.lock:
                    self.inventory.db.execute("DELETE FROM r2_copy_blocks")
                    self.inventory.db.commit()
                self.calls.clear()

    def test_two_refreshes_keep_original_download_telemetry(self):
        with mock.patch.object(pull, "r2_copy_range", side_effect=self.transfer):
            self.run_copy()
        before = pull.r2_copy_status(self.inventory)
        self.task["work_mode"] = "verify_existing"
        for _ in range(2):
            with mock.patch.object(pull, "r2_copy_range", side_effect=AssertionError("download")):
                self.run_copy()
        after = pull.r2_copy_status(self.inventory)
        self.assertEqual(after["committed_bytes"], before["committed_bytes"])
        self.assertEqual(after["verified_in_place"], before["verified_in_place"])
        self.assertEqual(after["refresh_verified_in_place"], dict(objects=2, bytes=2 * len(self.body)))

    def test_verify_only_preserves_media_partial_and_resource_holds(self):
        self.task["work_mode"] = "verify_existing"
        final = self.cfg.output_dir / self.item["relative_path"]
        final.parent.mkdir()
        final.write_bytes(self.body)
        partial = final.parent / ".copy.mp4.r2-copy-901.part"
        partial.write_bytes(b"preserve partial")
        with mock.patch.object(pull, "r2_copy_resources", side_effect=pull.R2CopyHold("copy memory headroom")):
            with self.assertRaises(pull.R2CopyHold):
                self.run_copy()
        self.assertEqual(partial.read_bytes(), b"preserve partial")
        self.assertEqual(final.read_bytes(), self.body)
        self.assertFalse(self.calls)
        with mock.patch.object(pull, "r2_copy_range", side_effect=AssertionError("download")):
            self.run_copy()
        self.assertEqual(partial.read_bytes(), b"preserve partial")
        self.assertEqual(final.read_bytes(), self.body)

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

    def test_verified_in_place_and_downloaded_result_counters(self):
        final = self.cfg.output_dir / self.item["relative_path"]
        final.parent.mkdir()
        final.write_bytes(self.body)
        self.run_copy()
        result = next(kw["body"] for path, kw in self.calls if path.endswith("/result"))
        self.assertEqual((result["completion"], result["downloaded_bytes"], result["committed_bytes"]), ("verified_in_place", 0, 0))
        self.assertEqual(pull.r2_copy_status(self.inventory)["verified_in_place"], dict(objects=1, bytes=len(self.body)))
        final.unlink()
        self.calls.clear()
        with mock.patch.object(pull, "r2_copy_range", side_effect=self.transfer):
            self.run_copy()
        result = next(kw["body"] for path, kw in self.calls if path.endswith("/result"))
        self.assertEqual((result["completion"], result["downloaded_bytes"], result["committed_bytes"]), ("downloaded", len(self.body), len(self.body)))

    def test_worker_immediately_moves_past_acknowledged_block_and_logs_http_holds(self):
        waits = []
        class Stop:
            def wait(self, seconds):
                waits.append(seconds)
                return len(waits) > 2
        control = mock.MagicMock()
        control.__enter__.return_value = control
        control.request_json.side_effect = [{"blocked": True, "id": 1, "reason": "r2_identity"},
                                           urllib.error.HTTPError("/lease", 409, "hold", {}, None)]
        with mock.patch.object(pull, "R2CopyControl", return_value=control), mock.patch.object(pull, "log") as logged:
            pull.r2_copy_worker(self.cfg, self.runtime, self.inventory, Stop(), 0)
        self.assertEqual(waits, [0, 0, 60])
        logged.assert_any_call("WARN", "R2 copy control HTTP 409; holding")

    def test_only_reviewed_release_advertises_staged_copy_without_dsm_setting(self):
        for version in ("r2-copy-v6", pull.CLEANUP_CERTIFIED_STITCH_VERSION):
            with self.subTest(version=version):
                control = mock.MagicMock()
                control.__enter__.return_value = control
                control.request_json.return_value = {"task": None}
                stop = mock.Mock()
                stop.wait.side_effect = [False, True]
                with mock.patch.object(pull, "R2CopyControl", return_value=control), \
                        mock.patch.object(pull, "r2_copy_resources", return_value=10), \
                        mock.patch.object(pull, "CLIENT_VERSION", version):
                    pull.r2_copy_worker(self.cfg, self.runtime, self.inventory, stop, 0)
                request = control.request_json.call_args.kwargs["body"]
                self.assertEqual(request.get("accept_stitch_output", False),
                                 version == pull.CLEANUP_CERTIFIED_STITCH_VERSION)

    def test_stop_during_range_releases_reservation_and_preserves_partial(self):
        def stop(cfg, task, offset, length, guard, fd, digest):
            os.write(fd, self.body[:100])
            self.stop.set()
            guard.check()
        with mock.patch.object(pull, "r2_copy_range", side_effect=stop):
            with self.assertRaises(pull.R2CopyHold):
                self.run_copy()
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)
        self.assertFalse(pull.NAS_COPY_PATHS.active)
        self.assertFalse((self.cfg.output_dir/self.item["relative_path"]).exists())
        self.assertEqual(next((self.cfg.output_dir/"p").glob(".*.part")).read_bytes(), self.body[:100])

    def test_complete_interrupted_partial_is_media_synced_before_resume_publication(self):
        def interrupted(cfg, task, offset, length, guard, fd, digest):
            os.write(fd, self.body)
            self.stop.set()
            raise pull.R2CopyHold("cancelled after complete write")
        with mock.patch.object(pull, "r2_copy_range", side_effect=interrupted):
            with self.assertRaises(pull.R2CopyHold):
                self.run_copy()
        self.stop.clear()
        events = []
        fsync = pull.os.fsync
        def sync(fd):
            if os.fstat(fd).st_size == len(self.body):
                events.append("media_sync")
            return fsync(fd)
        def control(operation, path, **kwargs):
            if path.endswith("/inventory"):
                self.assertIn("media_sync", events)
            return self.control(operation, path, **kwargs)
        with mock.patch.object(pull, "r2_copy_range", side_effect=AssertionError("redownloaded")), \
                mock.patch.object(pull.os, "fsync", side_effect=sync), \
                mock.patch.object(self.control_client, "request_json", side_effect=control):
            self.run_copy()
        self.assertTrue(events)
        self.assertTrue(any(path.endswith("/result") for path, _ in self.calls))

    def test_normal_delivery_publication_waits_for_copy_and_retains_exact_custody(self):
        started, done = threading.Event(), threading.Event()
        workers, errors, reports = [], [], []
        def delivery():
            started.set()
            try:
                self.inventory.record_verified(self.clip)
                self.inventory.sync_clip_ids(self.cfg, [self.clip["clip_id"]])
            except BaseException as exc:
                errors.append(exc)
            finally:
                done.set()
        def control(operation, path, **kwargs):
            if path.endswith("/inventory"):
                workers.append(threading.Thread(target=delivery))
                workers[-1].start()
                self.assertTrue(started.wait(1))
                self.assertFalse(done.wait(.05))
                row = kwargs["body"]["files"][0]
                self.assertEqual(row["sidecar_relative_path"], self.item["relative_path"] + ".stoarama.json")
                self.assertTrue(row["file_inode"])
            return self.control(operation, path, **kwargs)
        try:
            with mock.patch.object(pull, "r2_copy_range", side_effect=self.transfer), \
                    mock.patch.object(self.control_client, "request_json", side_effect=control), \
                    mock.patch.object(pull, "request_json", side_effect=lambda *a, **kw: reports.extend(kw["body"]["files"]) or {}):
                self.run_copy()
                self.assertTrue(done.wait(2))
                for worker in workers:
                    worker.join(2)
        finally:
            for worker in workers:
                worker.join(2)
        self.assertFalse(errors)
        self.assertEqual(len(reports), 1)
        self.assertTrue(reports[0]["sidecar_sha256"])
        self.assertTrue(reports[0]["file_inode"])
        # A later ordinary delivery must not erase or merely refresh old proof.
        self.inventory.record_verified(self.clip)
        row = self.inventory._reports(self.inventory._rows("clip_id=?", (self.clip["clip_id"],), 1))[0]
        self.assertEqual(row["sidecar_sha256"], reports[0]["sidecar_sha256"])
        sidecar = self.cfg.output_dir/(self.item["relative_path"] + ".stoarama.json")
        sidecar.write_text("{}")
        with self.assertRaises(RuntimeError):
            self.inventory.record_verified(self.clip)

    def test_existing_conflicts_symlinks_and_bad_checksums_are_preserved(self):
        final = self.cfg.output_dir / self.item["relative_path"]
        final.parent.mkdir()
        final.write_bytes(b"different source data")
        self.run_copy()
        self.assertEqual(self.calls[-1][1]["body"]["reason"], "final_conflict")
        self.assertEqual(pull.r2_copy_status(self.inventory)["mismatch_latched"], 1)
        self.assertEqual(final.read_bytes(), b"different source data")
        self.assertFalse(any(path.endswith("/result") for path, _ in self.calls))
        final.unlink()  # test-owned fixture only
        target = self.cfg.output_dir / "untouched"
        target.write_bytes(b"user data")
        final.symlink_to(target)
        self.task["id"] = 2
        self.run_copy()
        self.assertEqual(self.calls[-1][1]["body"]["reason"], "path_conflict")
        self.assertEqual(target.read_bytes(), b"user data")

    def test_sidecar_partial_checksum_conflicts_are_terminal_and_preserved(self):
        final = self.cfg.output_dir / self.item["relative_path"]
        final.parent.mkdir()
        final.write_bytes(self.body)
        sidecar = Path(str(final) + ".stoarama.json")
        sidecar.write_bytes(b"old sidecar")
        self.run_copy()
        self.assertEqual(self.calls[-1][1]["body"]["reason"], "sidecar_conflict")
        self.assertEqual(sidecar.read_bytes(), b"old sidecar")
        final.unlink()
        sidecar.unlink()
        self.task["id"] = 2
        partial = final.parent / (".%s.r2-copy-%d.part" % (final.name, self.item["source_id"]))
        partial.write_bytes(self.body + b"oversize")
        self.run_copy()
        self.assertEqual(self.calls[-1][1]["body"]["reason"], "partial_conflict")
        partial.write_bytes(b"x" * len(self.body))
        self.task["id"] = 3
        self.run_copy()
        self.assertEqual(self.calls[-1][1]["body"]["reason"], "checksum_mismatch")
        self.assertEqual(partial.read_bytes(), b"x" * len(self.body))

    def test_mismatch_latch_survives_failed_report_restart_and_reassigned_token(self):
        final = self.cfg.output_dir / self.item["relative_path"]
        final.parent.mkdir()
        final.write_bytes(b"conflict")
        original = self.control_client.request_json
        def fail(operation, path, **kw):
            if path.endswith("/block"):
                raise pull.R2CopyHold("copy HTTP total deadline")
            return original(operation, path, **kw)
        with mock.patch.object(self.control_client, "request_json", side_effect=fail), self.assertRaises(pull.R2CopyHold):
            self.run_copy()
        self.inventory.close()
        self.inventory = pull.Inventory(self.cfg)
        self.addCleanup(self.inventory.close)
        self.assertEqual(pull.r2_copy_status(self.inventory)["mismatch_latched"], 1)
        self.task["token"] = "b" * 48
        with mock.patch.object(pull, "_execute_r2_copy", side_effect=AssertionError("lost latch")):
            self.run_copy()
        self.assertEqual(self.calls[-1][1]["body"]["token"], "b" * 48)

    def test_actual_download_bytes_survive_failed_completion_and_partial_resume(self):
        original = self.control_client.request_json
        def control(operation, path, **kw):
            if path.endswith("/checkpoint") and kw["body"].get("range_id"):
                raise pull.R2CopyHold("copy HTTP total deadline")
            return original(operation, path, **kw)
        headers = {"Content-Range": "bytes 0-%d/%d" % (len(self.body)-1, len(self.body)), "ETag": self.task["etag"]}
        with mock.patch.object(pull, "r2_copy_http", return_value=(206, headers, self.body)), \
                mock.patch.object(self.control_client, "request_json", side_effect=control):
            with self.assertRaises(pull.R2CopyHold):
                self.run_copy()
        partial = next((self.cfg.output_dir / "p").glob(".*.part"))
        self.assertEqual(partial.read_bytes(), self.body)
        self.assertEqual(pull.r2_copy_status(self.inventory)["downloaded_bytes"], len(self.body))
        with mock.patch.object(pull, "r2_copy_http", side_effect=AssertionError("partial was downloaded twice")):
            self.run_copy()
        status = pull.r2_copy_status(self.inventory)
        self.assertEqual(status["downloaded_bytes"], len(self.body))
        self.assertEqual(status["committed_bytes"], len(self.body))

    def test_resource_hold_does_not_block_or_latch(self):
        with mock.patch.object(pull, "r2_copy_resources", side_effect=pull.R2CopyHold("copy memory headroom")):
            with self.assertRaises(pull.R2CopyHold):
                self.run_copy()
        self.assertFalse(any(path.endswith("/block") for path, _ in self.calls))
        self.assertEqual(self.inventory.db.execute("SELECT count(*) FROM r2_copy_blocks").fetchone()[0], 0)

    def test_enabled_cleanup_allows_copy_but_remote_off_stops_it(self):
        pull.NAS_MEDIA_FENCE.execution_owner = 123
        pull.NAS_MEDIA_FENCE.cleaners = 1
        with mock.patch.object(pull, "r2_copy_range", side_effect=self.transfer):
            self.run_copy()
        self.assertTrue(any(path.endswith("/result") for path, _ in self.calls))
        self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
        self.assertEqual(self.runtime.capacity_reserved_bytes, 0)
        with mock.patch.object(self.control_client, "request_json", side_effect=urllib.error.HTTPError("https://redacted", 409, "off", {}, None)):
            with self.assertRaises(urllib.error.HTTPError):
                self.run_copy()
        self.assertFalse(pull.NAS_COPY_PATHS.active)

    def test_cleanup_registration_between_publication_steps_recovers_private_links(self):
        final = self.cfg.output_dir / self.item["relative_path"]
        link = pull.os.link
        namespace = pull.r2_copy_namespace
        for sidecar in (False, True):
            # Fresh target per case; kept fixture files are never overwritten.
            self.item["relative_path"] = "p/copy-%s.mp4" % sidecar
            self.clip["relative_path"] = self.item["relative_path"]
            final = self.cfg.output_dir / self.item["relative_path"]
            registered, pending = [], []
            def namespace_then_register(parent, guard, mutate):
                result = namespace(parent, guard, mutate)
                for key in pending:
                    pull.CLEANUP_DIRECTORIES.enter(key)
                    registered.append(key)
                pending.clear()
                return result
            def register_after_link(src, dst, **kwargs):
                result = link(src, dst, **kwargs)
                if dst.endswith(".stoarama.json") == sidecar:
                    info = os.fstat(kwargs["dst_dir_fd"])
                    key = (info.st_dev, info.st_ino)
                    pending.append(key)
                return result
            with self.subTest(sidecar=sidecar), mock.patch.object(pull, "r2_copy_range", side_effect=self.transfer), \
                    mock.patch.object(pull.os, "link", side_effect=register_after_link), \
                    mock.patch.object(pull, "r2_copy_namespace", side_effect=namespace_then_register):
                with self.assertRaises(pull.R2CopyHold):
                    self.run_copy()
            self.assertTrue(final.exists())
            self.assertEqual(self.runtime.capacity_reserved_bytes, 0)
            for key in registered:
                pull.CLEANUP_DIRECTORIES.leave(key)
            with mock.patch.object(pull, "r2_copy_range", side_effect=AssertionError("redownloaded")):
                self.run_copy()
            self.assertEqual(final.read_bytes(), self.body)
            self.assertEqual(final.stat().st_nlink, 1)
            self.assertFalse(list(final.parent.glob(".*.part")))

    def test_cleanup_namespace_registration_refuses_only_that_directory(self):
        parent = self.cfg.output_dir / "p"
        parent.mkdir()
        info = parent.stat()
        key = (info.st_dev, info.st_ino)
        pull.CLEANUP_DIRECTORIES.enter(key)
        try:
            with self.assertRaises(pull.R2CopyHold):
                self.run_copy()
            self.assertFalse(list(parent.iterdir()))
        finally:
            pull.CLEANUP_DIRECTORIES.leave(key)

    def test_delivery_and_stale_copy_cannot_replace_owned_path(self):
        with pull.NAS_COPY_PATHS.own(self.item["relative_path"]):
            with self.assertRaises(pull.R2CopyHold):
                self.run_copy()
            entered = threading.Event()
            def delivery():
                with pull.NAS_COPY_PATHS.own(self.item["relative_path"], wait=True):
                    entered.set()
            worker = threading.Thread(target=delivery)
            worker.start()
            self.assertFalse(entered.wait(.05))
            # A waiting delivery has not taken the cleanup writer fence.
            self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
        worker.join(1)
        self.assertTrue(entered.is_set())

    def test_cleanup_priority_blocks_disk_but_not_control_and_cancels(self):
        guard = pull.R2CopyGuard(self.cfg, self.runtime, self.stop, self.task, self.control_client)
        ready = threading.Event()
        @pull.NAS_COPY_PATHS.cleanup_priority
        def cleanup():
            ready.set()
            self.assertEqual(guard.check(), pull.R2_COPY_FLOOR + 1_000_000_000)
            self.stop.wait(1)
        worker = threading.Thread(target=cleanup)
        worker.start()
        self.assertTrue(ready.wait(1))
        timer = threading.Timer(.1, self.stop.set)
        timer.start()
        with self.assertRaises(pull.R2CopyHold):
            guard.disk()
        worker.join(1)
        timer.join(1)
        self.assertEqual(pull.NAS_COPY_PATHS.priority, 0)

    def test_copy_pool_drains_every_worker_before_supervisor_finishes(self):
        started = threading.Barrier(pull.R2_COPY_WORKERS + 1)
        drain = threading.Event()
        def worker(*args):
            started.wait(2)
            self.stop.wait(2)
            drain.wait(2)
        with mock.patch.object(pull, "r2_copy_worker", side_effect=worker):
            supervisor = threading.Thread(target=pull.r2_copy_loop, args=(self.cfg, self.runtime, self.inventory, self.stop))
            supervisor.start()
            started.wait(2)
            self.stop.set()
            supervisor.join(.05)
            self.assertTrue(supervisor.is_alive())
            drain.set()
            supervisor.join(2)
            self.assertFalse(supervisor.is_alive())

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
            with pull.R2CopyControl(self.cfg, self.stop, 0) as control:
                with self.assertRaises(pull.R2CopyHold):
                    control.request_json(pull.R2CopyControlOperation.LEASE, "/control", body={}, timeout=.4)
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
