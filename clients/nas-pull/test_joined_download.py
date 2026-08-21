import errno
import hashlib
import importlib.util
import io
import json
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
        self.headers = {
            "ETag": '"%s"' % etag,
            "Content-Range": "bytes %d-%d/%d" % (start, end, total),
            "Content-Length": str(len(body)),
        }
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

    def media_item(self, content=b"abcdef", **changes):
        item = {
            "id": 41, "batch_id": "batch-2026-08", "hour_id": 501, "kind": "media",
            "content_type": "video/mp4",
            "relative_path": "MIT_North-America_US_Cambridge_Kendall/May/Monday/stream_hour_01_080000-090000.mp4",
            "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
            "download_path": "/api/v1/account/joined/41/download", "hour_manifest_id": 40,
            "hour_manifest_relative_path": "MIT_North-America_US_Cambridge_Kendall/May/Monday/stream_hour_01.hour.json",
            "hour_manifest_sha256": None,
        }
        item.update(changes)
        if item["hour_manifest_sha256"] is None:
            item["hour_manifest_sha256"] = hashlib.sha256(self.manifest_bytes(item)).hexdigest()
        return item

    def manifest_payload(self, media=None, gap=False):
        media_entries = [] if media is None else [{
            "artifact_id": media["id"], "part_ordinal": 1, "relative_path": media["relative_path"],
            "size_bytes": media["size_bytes"], "sha256": media["sha256"], "content_id": media["sha256"],
            "coverage_start_at": "2026-05-04T08:00:00Z", "coverage_end_at": "2026-05-04T09:00:00Z",
        }]
        return {
            "schema_version": 1, "batch_id": "batch-2026-08", "hour_id": 501, "recording_id": 77,
            "local_date": "2026-05-04", "delivery_hour": 1, "local_timezone": "America/New_York",
            "hour_start_at": "2026-05-04T08:00:00-04:00", "hour_end_at": "2026-05-04T09:00:00-04:00",
            "source_clip_count": 0 if gap else 60, "source_bytes": 0 if gap else 123456,
            "source_manifest_sha256": "a" * 64, "gap_only": gap, "media": media_entries,
        }

    def manifest_bytes(self, media=None, gap=False):
        return (json.dumps(self.manifest_payload(media, gap), sort_keys=True, separators=(",", ":")) + "\n").encode()

    def manifest_item(self, gap=True):
        content = self.manifest_bytes(None, gap)
        return {
            "id": 40, "batch_id": "batch-2026-08", "hour_id": 501, "kind": "hour_manifest",
            "content_type": "application/json",
            "relative_path": "MIT_North-America_US_Cambridge_Kendall/May/Monday/stream_hour_01.hour.json",
            "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
            "download_path": "/api/v1/account/joined/40/download", "hour_manifest_id": None,
            "hour_manifest_relative_path": None, "hour_manifest_sha256": None,
        }, content

    def install_manifest(self, cfg, item):
        path = cfg.output_dir / "joined" / item["batch_id"] / item["hour_manifest_relative_path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.manifest_bytes(item))
        return path

    def prepared(self, item):
        return {
            "url": "https://r2.test/exact-object", "etag": '"etag-1"', "if_match": '"etag-1"',
            "version_id": "", "size_bytes": item["size_bytes"], "sha256": item["sha256"],
            "content_type": item["content_type"],
        }

    def marker(self, item):
        return pull.joined_transfer_marker_bytes(item, {
            "etag": "etag-1", "version_id": "", "url": "https://r2.test/x", "if_match": '"etag-1"',
        })

    def storage(self): return {"available": True, "total_bytes": 10**13, "free_bytes": 10**13}

    def names(self, cfg, item):
        directory_fd = pull.open_joined_output_dir(cfg, item)
        final = Path(item["relative_path"]).name
        part = ".%s.joined-%d.part" % (final, item["id"])
        return directory_fd, final, part, part + ".stoarama.json"

    def write_entry(self, directory_fd, name, content):
        descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=directory_fd)
        try:
            os.write(descriptor, content); os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.fsync(directory_fd)

    def test_protocol_artifact_shape_and_kind_validation(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); self.assertEqual(pull.Runtime(cfg).heartbeat_payload(None)["joined_protocol_version"], 1)
        good = self.media_item(); self.assertEqual(pull.valid_joined_item(good)["kind"], "media")
        for change in (
            {"id": 0}, {"batch_id": "../escape"}, {"relative_path": "../escape.mp4"},
            {"relative_path": "/absolute.mp4"}, {"relative_path": "joined/nested.mp4"}, {"sha256": "bad"},
            {"download_path": "/api/v1/account/joined/42/download"}, {"size_bytes": pull.JOINED_MAX_BYTES + 1},
            {"content_type": "application/json"}, {"hour_manifest_id": None}, {"surprise": True},
        ):
            with self.subTest(change=change), self.assertRaises(ValueError):
                pull.valid_joined_item({**good, **change})
        batch = {**good, "kind": "batch_index", "content_type": "application/json", "hour_id": None,
                 "relative_path": "batch.json", "hour_manifest_id": None, "hour_manifest_relative_path": None,
                 "hour_manifest_sha256": None}
        self.assertEqual(pull.valid_joined_item(batch)["kind"], "batch_index")

    def test_gap_only_hour_manifest_downloads_and_exact_acks(self):
        raw_item, content = self.manifest_item(gap=True); requests, acks = [], []
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
            cfg, runtime = self.config(Path(raw)), None; runtime = pull.Runtime(cfg)
            with mock.patch.object(pull, "JOINED_RANGE_BYTES", 64), mock.patch.object(pull, "storage_status", return_value=self.storage()), \
                 mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull.urllib.request, "urlopen", side_effect=open_range):
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            item = pull.valid_joined_item(raw_item); final = pull.joined_output_path(cfg, item)
            self.assertEqual(final.read_bytes(), content); self.assertFalse(list(final.parent.glob("*.mp4")))
            self.assertEqual(acks, [{"artifact_id": 40, "relative_path": raw_item["relative_path"],
                                     "size_bytes": len(content), "sha256": raw_item["sha256"]}])
            self.assertTrue(requests); self.assertEqual(list(final.parent.glob(".*.part*")), [])

    def test_media_requires_and_matches_local_sealed_hour_manifest(self):
        content, raw_item, requests, acks = b"abcdef", self.media_item(), [], []
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
            cfg, runtime = self.config(Path(raw)), None; runtime = pull.Runtime(cfg); self.install_manifest(cfg, raw_item)
            with mock.patch.object(pull, "JOINED_RANGE_BYTES", 3), mock.patch.object(pull, "storage_status", return_value=self.storage()), \
                 mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull.urllib.request, "urlopen", side_effect=open_range):
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            final = pull.joined_output_path(cfg, pull.valid_joined_item(raw_item))
            self.assertEqual(final.read_bytes(), content); self.assertFalse(final.with_name(final.name + ".stoarama.json").exists())
            self.assertEqual([r["Range"] for r in requests], ["bytes=0-2", "bytes=3-5"])
            self.assertEqual([r["If-match"] for r in requests], ['"etag-1"', '"etag-1"'])
            self.assertEqual(acks[0]["artifact_id"], 41); self.assertEqual(list(final.parent.glob(".*.part*")), [])

    def test_media_manifest_mismatch_or_missing_manifest_fails_before_download(self):
        item = pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg, runtime = self.config(Path(raw)), None; runtime = pull.Runtime(cfg)
            with self.assertRaises(pull.ExistingFileMismatch): pull.download_joined_item(cfg, runtime, item, threading.Event())
            path = self.install_manifest(cfg, item); path.write_bytes(self.manifest_bytes({**item, "size_bytes": 7}))
            altered = {**item, "hour_manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "sealed hour manifest"):
                pull.download_joined_item(cfg, runtime, altered, threading.Event())

    def test_hash_is_bounded_cancellable_and_yields_to_raw(self):
        content, item = b"abcdef", pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, part, marker = self.names(cfg, item)
            try:
                with mock.patch.object(pull, "poll_raw_pending", return_value=False):
                    pull.ensure_owned_joined_partial(cfg, runtime, directory_fd, part, marker, self.marker(item), threading.Event())
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, content); os.close(descriptor)
                with mock.patch.object(pull, "JOINED_RANGE_BYTES", 3), mock.patch.object(pull, "poll_raw_pending", side_effect=[False, True]) as poll, self.assertRaises(pull.JoinedDownloadYield):
                    pull.hash_joined_entry(cfg, runtime, directory_fd, part, threading.Event())
                self.assertEqual(poll.call_count, 2); self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 6)
            finally: os.close(directory_fd)

    def test_unknown_partial_and_hardlink_are_never_modified(self):
        item = pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); self.install_manifest(cfg, item)
            directory_fd, _, part, marker = self.names(cfg, item)
            prepared = {"etag": "etag-1", "version_id": "", "url": "x", "if_match": '"etag-1"'}
            try:
                self.write_entry(directory_fd, part, b"unknown")
                with mock.patch.object(pull, "prepare_joined_download", return_value=prepared), mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "ownership marker"):
                    pull.download_joined_item(cfg, runtime, item, threading.Event())
                self.write_entry(directory_fd, marker, self.marker(item)); os.link(part, "external-hardlink", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                with mock.patch.object(pull, "prepare_joined_download", return_value=prepared), mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "unknown hardlink"):
                    pull.download_joined_item(cfg, runtime, item, threading.Event())
                with self.assertRaisesRegex(pull.ExistingFileMismatch, "exclusively owned"): pull.truncate_joined_part(directory_fd, part)
                self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 7)
            finally: os.close(directory_fd)

    def test_private_stage_crash_enospc_restart_and_conflict_safety(self):
        item = pull.valid_joined_item(self.media_item()); expected = self.marker(item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _part, marker = self.names(cfg, item)
            try:
                orphan = marker + ".stage-" + "a" * 32; self.write_entry(directory_fd, orphan, expected)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), mock.patch.object(pull.os, "urandom", return_value=b"\xbb" * 16):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertEqual(os.stat(marker, dir_fd=directory_fd).st_nlink, 1)
                descriptor = os.open(orphan, os.O_RDONLY, dir_fd=directory_fd)
                try: self.assertEqual(os.read(descriptor, len(expected) + 1), expected)
                finally: os.close(descriptor)
            finally: os.close(directory_fd)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _part, marker = self.names(cfg, item)
            try:
                real_write, calls = os.write, 0
                def fail_write(descriptor, content):
                    nonlocal calls
                    calls += 1
                    if calls == 1: return real_write(descriptor, content[:7])
                    raise OSError(errno.ENOSPC, "no space left")
                with mock.patch.object(pull.os, "urandom", return_value=b"\xaa" * 16), mock.patch.object(pull.os, "write", side_effect=fail_write), self.assertRaisesRegex(OSError, "no space left"):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                short = marker + ".stage-" + "a" * 32
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), mock.patch.object(pull.os, "urandom", return_value=b"\xbb" * 16):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertEqual(os.stat(short, dir_fd=directory_fd).st_size, 7)
            finally: os.close(directory_fd)

    def test_restart_after_stage_link_removes_only_proven_link(self):
        item = pull.valid_joined_item(self.media_item()); expected = self.marker(item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _part, marker = self.names(cfg, item)
            try:
                linked = marker + ".stage-" + "a" * 32; unknown = marker + ".stage-" + "b" * 32
                self.write_entry(directory_fd, linked, expected); self.write_entry(directory_fd, unknown, b"unknown")
                os.link(linked, marker, src_dir_fd=directory_fd, dst_dir_fd=directory_fd); os.fsync(directory_fd)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False): pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertIsNone(pull.joined_entry_stat(directory_fd, linked)); self.assertEqual(os.stat(unknown, dir_fd=directory_fd).st_size, 7)
                self.assertEqual(os.stat(marker, dir_fd=directory_fd).st_nlink, 1)
            finally: os.close(directory_fd)

    def test_conflicting_marker_is_never_modified(self):
        item = pull.valid_joined_item(self.media_item()); expected = self.marker(item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, _part, marker = self.names(cfg, item)
            try:
                self.write_entry(directory_fd, marker, b"conflicting-marker")
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "marker conflicts"):
                    pull.publish_joined_sidecar(cfg, runtime, directory_fd, marker, expected, threading.Event())
                self.assertEqual(os.stat(marker, dir_fd=directory_fd).st_size, len(b"conflicting-marker"))
            finally: os.close(directory_fd)

    def test_crash_after_final_link_completes_without_final_sidecar(self):
        content, raw_item = b"abcdef", self.media_item(); item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); self.install_manifest(cfg, item)
            directory_fd, final, part, marker = self.names(cfg, item)
            try:
                with mock.patch.object(pull, "poll_raw_pending", return_value=False): pull.ensure_owned_joined_partial(cfg, runtime, directory_fd, part, marker, self.marker(item), threading.Event())
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, content); os.fsync(descriptor); os.close(descriptor)
                os.link(part, final, src_dir_fd=directory_fd, dst_dir_fd=directory_fd); os.fsync(directory_fd)
            finally: os.close(directory_fd)
            acks = []
            def api(_cfg, _method, path, body=None, **_kwargs):
                if path == "/account/joined": return {"item": raw_item}
                if path.startswith("/account/clips?"): return {"clips": []}
                if path == raw_item["download_path"]: return self.prepared(raw_item)
                if path == "/account/joined/ack": acks.append(body); return {"ok": True}
                raise AssertionError(path)
            with mock.patch.object(pull, "request_json", side_effect=api): self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            final_path = pull.joined_output_path(cfg, item)
            self.assertEqual(final_path.read_bytes(), content); self.assertFalse(final_path.with_name(final_path.name + ".stoarama.json").exists())
            self.assertEqual(len(acks), 1); self.assertEqual(list(final_path.parent.glob(".*.part*")), [])

    def test_http_restart_rules_truncate_only_owned_partial(self):
        item = pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, _, part, marker = self.names(cfg, item)
            prepared = {"url": "https://r2.test/x", "if_match": '"etag-1"', "etag": "etag-1", "version_id": ""}
            try:
                with mock.patch.object(pull, "poll_raw_pending", return_value=False): pull.ensure_owned_joined_partial(cfg, runtime, directory_fd, part, marker, self.marker(item), threading.Event())
                response = RangeResponse(b"abcdef", 0, 5, 6); response.status = 200
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, b"abc"); os.close(descriptor)
                with mock.patch.object(pull.urllib.request, "urlopen", return_value=response), self.assertRaisesRegex(RuntimeError, "ignored range"):
                    pull.append_joined_range(prepared, directory_fd, part, item, 0, 2)
                self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 0)
                descriptor = os.open(part, os.O_WRONLY, dir_fd=directory_fd); os.write(descriptor, b"abc"); os.close(descriptor)
                changed = RangeResponse(b"def", 3, 5, 6, etag="etag-2")
                with mock.patch.object(pull.urllib.request, "urlopen", return_value=changed), self.assertRaisesRegex(pull.ExistingFileMismatch, "identity drifted"):
                    pull.append_joined_range(prepared, directory_fd, part, item, 3, 5)
                self.assertEqual(os.stat(part, dir_fd=directory_fd).st_size, 0)
            finally: os.close(directory_fd)

    def test_ack_failure_replays_exact_final_without_download_or_sidecar(self):
        content, raw_item, fail_ack = b"abc", self.media_item(b"abc"), True
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
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); self.install_manifest(cfg, raw_item)
            with mock.patch.object(pull, "storage_status", return_value=self.storage()), mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull.urllib.request, "urlopen", return_value=RangeResponse(content, 0, 2, 3)), self.assertRaisesRegex(urllib.error.URLError, "ack unavailable"):
                pull.drain_joined(cfg, runtime, threading.Event())
            final = pull.joined_output_path(cfg, pull.valid_joined_item(raw_item)); before = final.read_bytes()
            with mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull.urllib.request, "urlopen") as download:
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            download.assert_not_called(); self.assertEqual(final.read_bytes(), before); self.assertFalse(final.with_name(final.name + ".stoarama.json").exists())

    def test_exact_preexisting_final_is_acked_without_download(self):
        content, raw_item = b"abcdef", self.media_item(); item = pull.valid_joined_item(raw_item)
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); self.install_manifest(cfg, item)
            final = pull.joined_output_path(cfg, item); final.parent.mkdir(parents=True, exist_ok=True); final.write_bytes(content)
            def api(_cfg, _method, path, **_kwargs):
                if path == "/account/joined": return {"item": raw_item}
                if path.startswith("/account/clips?"): return {"clips": []}
                if path == "/account/joined/ack": return {"ok": True}
                raise AssertionError(path)
            with mock.patch.object(pull, "request_json", side_effect=api), mock.patch.object(pull.urllib.request, "urlopen") as download:
                self.assertTrue(pull.drain_joined(cfg, runtime, threading.Event()))
            download.assert_not_called(); self.assertEqual(final.read_bytes(), content)

    def test_unknown_third_final_link_is_rejected(self):
        content, item = b"abcdef", pull.valid_joined_item(self.media_item())
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw)); runtime = pull.Runtime(cfg); directory_fd, final, part, marker = self.names(cfg, item)
            try:
                self.write_entry(directory_fd, part, content); self.write_entry(directory_fd, marker, self.marker(item))
                os.link(part, final, src_dir_fd=directory_fd, dst_dir_fd=directory_fd); os.link(final, "unknown-third-link", src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
                with mock.patch.object(pull, "poll_raw_pending", return_value=False), self.assertRaisesRegex(pull.ExistingFileMismatch, "partial conflicts"):
                    pull.complete_existing_joined(cfg, runtime, directory_fd, item, (final, part, marker), self.marker(item), threading.Event())
                self.assertEqual(os.stat(final, dir_fd=directory_fd).st_nlink, 3)
            finally: os.close(directory_fd)

    def test_unknown_joined_feed_fields_fail_closed(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            with mock.patch.object(pull, "request_json", return_value={"item": None, "surprise": True}), self.assertRaisesRegex(RuntimeError, "unknown fields"):
                pull.drain_joined(cfg, pull.Runtime(cfg), threading.Event())


if __name__ == "__main__": unittest.main()
