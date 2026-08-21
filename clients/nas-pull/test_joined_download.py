import errno
import hashlib
import importlib.util
import io
import os
import tempfile
import threading
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

MODULE_PATH = Path(__file__).with_name("stoarama_pull.py")
SPEC = importlib.util.spec_from_file_location("stoarama_pull_joined", MODULE_PATH)
pull = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pull)


class RangeResponse:
    def __init__(self, body, start, end, total, etag="etag-1", version=""):
        self.status = 206
        self.body = io.BytesIO(body)
        self.headers = {"ETag": '"%s"' % etag, "Content-Range": "bytes %d-%d/%d" % (start, end, total), "Content-Length": str(len(body))}
        if version:
            self.headers["x-amz-version-id"] = version

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def getcode(self): return self.status
    def read(self, size=-1): return self.body.read(size)


class JoinedDownloadTests(unittest.TestCase):
    def config(self, root):
        state, clips = root / "state", root / "clips"
        state.mkdir(); clips.mkdir()
        return SimpleNamespace(
            api_base="https://stoarama.test/api/v1", api_key="sir_test", origin="https://stoarama.test",
            output_dir=clips, state_dir=state, progress_file=state / "progress.json",
            legacy_progress_file=state / "cursor.json", runtime_file=state / "runtime.json",
            outage_file=state / "outage.json", capacity_file=state / "capacity.json",
            inventory_file=state / "inventory.sqlite3", download_workers=12, min_free_bytes=100, dry_run=False,
        )

    def item(self, content=b"abcdef", **changes):
        item = {
            "id": 41, "campaign_id": "2c1b87b2-9579-4cf7-9920-21198fd9571c",
            "campaign_expected_outputs": 10, "campaign_manifest_sha256": "a" * 64, "recording_id": 77,
            "relative_path": "MIT_North-America_US_Cambridge_Kendall/May/Monday/stream_hour_01_080000-090000.mp4",
            "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
            "etag": '"etag-1"', "version_id": "", "coverage_object_key": "joined/campaign/coverage.json",
            "coverage_size_bytes": 123, "coverage_sha256": "b" * 64, "coverage_etag": '"coverage-etag"',
            "coverage_version_id": "", "coverage_start_at": "2026-08-21T08:00:00Z",
            "coverage_end_at": "2026-08-21T09:00:00Z", "download_path": "/api/v1/account/joined/41/download",
        }
        item.update(changes)
        return item

    def prepared(self, item):
        return {"url": "https://r2.test/exact-object", "etag": item["etag"], "if_match": item["etag"],
                "version_id": item["version_id"], "size_bytes": item["size_bytes"], "sha256": item["sha256"], "expires_in_sec": 300}

    def storage(self): return {"available": True, "total_bytes": 10**13, "free_bytes": 10**13}

    def names(self, cfg, item):
        directory_fd = pull.open_joined_output_dir(cfg, item)
        final = Path(item["relative_path"]).name
        part = ".%s.joined-%d.part" % (final, item["id"])
        return directory_fd, final, final + ".stoarama.json", part, part + ".stoarama.json"

    def write_entry(self, directory_fd, name, content):
        descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=directory_fd)
        try:
            os.write(descriptor, content); os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.fsync(directory_fd)

    def test_protocol_cap_and_path_validation(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); self.assertEqual(pull.Runtime(cfg).heartbeat_payload(None)["joined_protocol_version"], 1)
        good = self.item(); self.assertEqual(pull.valid_joined_item(good)["etag"], "etag-1")
        for change in ({"id": 0}, {"campaign_id": "../escape"}, {"relative_path": "../escape.mp4"},
                       {"relative_path": "/absolute.mp4"}, {"relative_path": "joined/nested.mp4"}, {"sha256": "bad"},
                       {"etag": "W/\"weak\""}, {"download_path": "/api/v1/account/joined/42/download"},
                       {"size_bytes": pull.JOINED_MAX_BYTES + 1}):
            with self.subTest(change=change), self.assertRaises(ValueError): pull.valid_joined_item({**good, **change})
        with tempfile.TemporaryDirectory() as raw, tempfile.TemporaryDirectory() as outside:
            cfg = self.config(Path(raw)); cfg.output_dir.joinpath("joined").symlink_to(outside, target_is_directory=True)
            with self.assertRaises(OSError): pull.open_joined_output_dir(cfg, pull.valid_joined_item(good))
            self.assertEqual(list(Path(outside).iterdir()), [])

    def test_bounded_ranges_publish_sidecar_ack_and_cleanup_owned_scratch(self):
        content, raw_item, requests, acks = b"abcdef", self.item(b"abcdef"), [], []
        def api(_cfg, _method, path, body=None, **_kwargs):
            if path == "/account/joined": return {"item": raw_item}
            if path.startswith("/account/clips?"): return {"clips": []}
            if path == raw_item["download_path"]: return self.prepared(raw_item)
            if path == "/account/joined/ack": acks.append(body); return {"ok": True}
            raise AssertionError(path)
        def open_range(request, **_kwargs):
            headers = dict(request.header_items()); requests.append(headers)
            start, end = map(int, headers["Range"].removeprefix("bytes=").split("-"))
            return RangeResponse(content[start:end + 1], start, end, len(content))
        with tempfile.TemporaryDirectory() as raw:
            cfg, runtime = self.config(Path(raw)), None
            runtime = pull.Runtime(cfg)
            with mock.patch.object(pull, "JOINED_RANGE_BYTES", 3), mock.patch.object(pull, "storage_status", return_value=self.storage()), \
                 mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull.urllib.request, "urlopen", side_effect=open_range):
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            item = pull.valid_joined_item(raw_item); final = pull.joined_output_path(cfg, item)
            self.assertEqual(final.read_bytes(), content)
            self.assertEqual(final.with_name(final.name + ".stoarama.json").read_bytes(), pull.joined_sidecar_bytes(item))
            self.assertEqual([r["Range"] for r in requests], ["bytes=0-2", "bytes=3-5"])
            self.assertEqual([r["If-match"] for r in requests], ['"etag-1"', '"etag-1"'])
            self.assertEqual(len(acks), 1); self.assertEqual(list(final.parent.glob(".*.part*")), [])

    def test_hash_is_bounded_cancellable_and_yields_to_raw(self):
        content, item = b"abcdef", pull.valid_joined_item(self.item(b"abcdef"))
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _, part, marker = self.names(cfg, item)
            try:
                sidecar = pull.joined_sidecar_bytes(item)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False):
                    pull.ensure_owned_joined_partial(cfg, runtime, directory_fd, part, marker, sidecar, threading.Event())
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, content); os.close(descriptor)
                with mock.patch.object(pull, "JOINED_RANGE_BYTES", 3), mock.patch.object(pull, "poll_raw_pending", side_effect=[False, True]) as poll, \
                     self.assertRaises(pull.JoinedDownloadYield):
                    pull.hash_joined_entry(cfg, runtime, directory_fd, part, threading.Event())
                self.assertEqual(poll.call_count, 2); self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 6)
            finally: os.close(directory_fd)

    def test_unknown_partial_and_hardlink_are_never_modified(self):
        item = pull.valid_joined_item(self.item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _, part, marker = self.names(cfg, item)
            try:
                self.write_entry(directory_fd, part, b"unknown")
                with self.assertRaisesRegex(pull.ExistingFileMismatch, "ownership marker"):
                    pull.download_joined_item(cfg, runtime, item, threading.Event())
                self.write_entry(directory_fd, marker, pull.joined_sidecar_bytes(item))
                os.link(part, "external-hardlink", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "unknown hardlink"):
                    pull.download_joined_item(cfg, runtime, item, threading.Event())
                with self.assertRaisesRegex(pull.ExistingFileMismatch, "exclusively owned"):
                    pull.truncate_joined_part(directory_fd, part)
                self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 7)
            finally: os.close(directory_fd)

    def test_crash_before_link_orphan_is_ignored_and_never_modified(self):
        item = pull.valid_joined_item(self.item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _, _part, marker = self.names(cfg, item)
            try:
                expected = pull.joined_sidecar_bytes(item)
                orphan = marker + ".stage-" + "a" * 32
                self.write_entry(directory_fd, orphan, expected)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), mock.patch.object(
                    pull.os, "urandom", return_value=b"\xbb" * 16,
                ):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                    self.assertTrue(pull.verify_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event()))
                descriptor = os.open(orphan, os.O_RDONLY, dir_fd=directory_fd)
                try: self.assertEqual(os.read(descriptor, len(expected) + 1), expected)
                finally: os.close(descriptor)
                self.assertEqual(pull.joined_entry_stat(directory_fd, marker).st_nlink, 1)
            finally: os.close(directory_fd)

    def test_enospc_stage_does_not_poison_restart(self):
        item = pull.valid_joined_item(self.item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _, _part, marker = self.names(cfg, item)
            try:
                expected = pull.joined_sidecar_bytes(item)
                orphan = marker + ".stage-" + "a" * 32
                real_write, calls = os.write, 0
                def fail_write(descriptor, content):
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        return real_write(descriptor, content[:7])
                    raise OSError(errno.ENOSPC, "no space left")
                with mock.patch.object(pull.os, "urandom", return_value=b"\xaa" * 16), mock.patch.object(
                    pull.os, "write", side_effect=fail_write,
                ), self.assertRaisesRegex(OSError, "no space left"):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                descriptor = os.open(orphan, os.O_RDONLY, dir_fd=directory_fd)
                try: self.assertEqual(os.read(descriptor, 8), expected[:7])
                finally: os.close(descriptor)
                self.assertIsNone(pull.joined_entry_stat(directory_fd, marker))
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), mock.patch.object(
                    pull.os, "urandom", return_value=b"\xbb" * 16,
                ):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                descriptor = os.open(orphan, os.O_RDONLY, dir_fd=directory_fd)
                try: self.assertEqual(os.read(descriptor, 8), expected[:7])
                finally: os.close(descriptor)
                self.assertEqual(pull.joined_entry_stat(directory_fd, marker).st_nlink, 1)
            finally: os.close(directory_fd)

    def test_restart_after_link_removes_only_proven_stage_link(self):
        item = pull.valid_joined_item(self.item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _, _part, marker = self.names(cfg, item)
            try:
                expected = pull.joined_sidecar_bytes(item)
                linked_stage = marker + ".stage-" + "a" * 32
                unknown_stage = marker + ".stage-" + "b" * 32
                self.write_entry(directory_fd, linked_stage, expected)
                self.write_entry(directory_fd, unknown_stage, b"unknown")
                os.link(linked_stage, marker, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                os.fsync(directory_fd)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertIsNone(pull.joined_entry_stat(directory_fd, linked_stage))
                descriptor = os.open(unknown_stage, os.O_RDONLY, dir_fd=directory_fd)
                try: self.assertEqual(os.read(descriptor, 8), b"unknown")
                finally: os.close(descriptor)
                self.assertEqual(pull.joined_entry_stat(directory_fd, marker).st_nlink, 1)
            finally: os.close(directory_fd)

    def test_conflicting_marker_and_unknown_stage_are_never_modified(self):
        item = pull.valid_joined_item(self.item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _, _part, marker = self.names(cfg, item)
            try:
                unknown_stage = marker + ".stage-" + "a" * 32
                self.write_entry(directory_fd, marker, b"conflicting-marker")
                self.write_entry(directory_fd, unknown_stage, b"unknown-stage")
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(
                    pull.ExistingFileMismatch, "ownership marker conflicts"
                ):
                    pull.publish_joined_sidecar(
                        cfg, runtime, directory_fd, marker, pull.joined_sidecar_bytes(item), threading.Event(),
                    )
                for name, expected in ((marker, b"conflicting-marker"), (unknown_stage, b"unknown-stage")):
                    descriptor = os.open(name, os.O_RDONLY, dir_fd=directory_fd)
                    try: self.assertEqual(os.read(descriptor, len(expected) + 1), expected)
                    finally: os.close(descriptor)
            finally: os.close(directory_fd)

    def test_crash_after_final_link_is_completed_without_overwrite(self):
        content, raw_item = b"abcdef", self.item(b"abcdef"); item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, final, sidecar, part, marker = self.names(cfg, item)
            try:
                with mock.patch.object(pull, "poll_raw_pending", return_value=False):
                    pull.ensure_owned_joined_partial(cfg, runtime, directory_fd, part, marker, pull.joined_sidecar_bytes(item), threading.Event())
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, content); os.fsync(descriptor); os.close(descriptor)
                os.link(part, final, src_dir_fd=directory_fd, dst_dir_fd=directory_fd); os.fsync(directory_fd)
            finally: os.close(directory_fd)
            acks = []
            def api(_cfg, _method, path, body=None, **_kwargs):
                if path == "/account/joined": return {"item": raw_item}
                if path.startswith("/account/clips?"): return {"clips": []}
                if path == "/account/joined/ack": acks.append(body); return {"ok": True}
                raise AssertionError(path)
            with mock.patch.object(pull, "request_json", side_effect=api): self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            final_path = pull.joined_output_path(cfg, item)
            self.assertEqual(final_path.read_bytes(), content); self.assertEqual(final_path.with_name(sidecar).read_bytes(), pull.joined_sidecar_bytes(item))
            self.assertEqual(len(acks), 1); self.assertEqual(list(final_path.parent.glob(".*.part*")), [])

    def test_http_restart_rules_truncate_only_owned_partial(self):
        item = pull.valid_joined_item(self.item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _, part, marker = self.names(cfg, item)
            try:
                with mock.patch.object(pull, "poll_raw_pending", return_value=False):
                    pull.ensure_owned_joined_partial(cfg, runtime, directory_fd, part, marker, pull.joined_sidecar_bytes(item), threading.Event())
                for response, error_text in ((RangeResponse(b"abcdef", 0, 5, 6), "ignored range"),):
                    response.status = 200; descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, b"abc"); os.close(descriptor)
                    with mock.patch.object(pull.urllib.request, "urlopen", return_value=response), self.assertRaisesRegex(RuntimeError, error_text):
                        pull.append_joined_range("https://r2.test/x", '"etag-1"', directory_fd, part, item, 0, 2)
                    self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 0)
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, b"abc"); os.close(descriptor)
                changed = RangeResponse(b"def", 3, 5, 6, etag="etag-2")
                with mock.patch.object(pull.urllib.request, "urlopen", return_value=changed), self.assertRaisesRegex(pull.ExistingFileMismatch, "identity drifted"):
                    pull.append_joined_range("https://r2.test/x", '"etag-1"', directory_fd, part, item, 3, 5)
                self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 0)
            finally: os.close(directory_fd)

    def test_ack_failure_keeps_verified_final_and_sidecar_for_replay(self):
        content, raw_item, fail_ack = b"abc", self.item(b"abc"), True
        def api(_cfg, _method, path, **_kwargs):
            nonlocal fail_ack
            if path == "/account/joined": return {"item": raw_item}
            if path.startswith("/account/clips?"): return {"clips": []}
            if path == raw_item["download_path"]: return self.prepared(raw_item)
            if path == "/account/joined/ack":
                if fail_ack: fail_ack = False; raise urllib.error.URLError("ack unavailable")
                return {"ok": True}
            raise AssertionError(path)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg)
            with mock.patch.object(pull, "storage_status", return_value=self.storage()), mock.patch.object(pull, "request_json", side_effect=api), \
                 mock.patch.object(pull.urllib.request, "urlopen", return_value=RangeResponse(content, 0, 2, 3)), \
                 self.assertRaisesRegex(urllib.error.URLError, "ack unavailable"):
                pull.drain_joined(cfg, runtime, threading.Event())
            item = pull.valid_joined_item(raw_item); final = pull.joined_output_path(cfg, item); sidecar = final.with_name(final.name + ".stoarama.json")
            before = final.read_bytes(), sidecar.read_bytes()
            with mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull.urllib.request, "urlopen") as download:
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            download.assert_not_called(); self.assertEqual((final.read_bytes(), sidecar.read_bytes()), before)

    def test_existing_final_without_sidecar_is_read_only_conflict(self):
        content, raw_item = b"abcdef", self.item(); item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); final = pull.joined_output_path(cfg, item); final.parent.mkdir(parents=True); final.write_bytes(content)
            def api(_cfg, _method, path, **_kwargs):
                return {"item": raw_item} if path == "/account/joined" else {"clips": []}
            with mock.patch.object(pull, "request_json", side_effect=api), self.assertRaisesRegex(pull.ExistingFileMismatch, "provenance sidecar"):
                pull.drain_joined(cfg, runtime, threading.Event())
            self.assertEqual(final.read_bytes(), content); self.assertFalse(final.with_name(final.name + ".stoarama.json").exists())

    def test_unknown_third_final_link_is_rejected(self):
        content, item = b"abcdef", pull.valid_joined_item(self.item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, final, sidecar, part, marker = self.names(cfg, item)
            try:
                self.write_entry(directory_fd, part, content)
                self.write_entry(directory_fd, marker, pull.joined_sidecar_bytes(item))
                os.link(part, final, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                os.link(marker, sidecar, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                os.link(final, "unknown-third-link", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                names = final, sidecar, part, marker
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(
                    pull.ExistingFileMismatch, "partial conflicts"
                ):
                    pull.complete_existing_joined(
                        cfg, runtime, directory_fd, item, names, pull.joined_sidecar_bytes(item), threading.Event(),
                    )
                self.assertEqual(os.stat(final, dir_fd=directory_fd).st_nlink, 3)
            finally: os.close(directory_fd)

    def test_unknown_joined_feed_fields_fail_closed(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            with mock.patch.object(pull, "request_json", return_value={"item": None, "surprise": True}), self.assertRaisesRegex(RuntimeError, "unknown fields"):
                pull.drain_joined(cfg, pull.Runtime(cfg), threading.Event())


if __name__ == "__main__": unittest.main()
