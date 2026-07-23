import importlib.util
import json
import os
import socket
import tempfile
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
            current_file=state / "stoarama_pull.py",
            candidate_file=state / "stoarama_pull.candidate.py",
            previous_file=state / "stoarama_pull.previous.py",
            lock_file=state / "client.lock",
            update_manifest_url="https://stoarama.test/nas/download/latest.json",
            download_workers=12,
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

    def test_storage_must_be_real_mounts(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            with mock.patch.object(pull.os.path, "ismount", return_value=False):
                with self.assertRaisesRegex(RuntimeError, "not mounted"):
                    pull.check_storage(cfg)

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
                "recording_id": 3,
                "size_bytes": 3,
                "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "relative_path": "recordings/clip.mp4",
                "download_path": "/unused",
            }
            with mock.patch.object(pull, "release_clip") as release:
                self.assertEqual(pull.process_clip(cfg, clip), (7, 3, 0, 0))
                release.assert_called_once_with(cfg, 3, 7)

    def test_checksum_mismatch_is_quarantined_and_redownloaded(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            final = cfg.output_dir / "recordings" / "clip.mp4"
            final.parent.mkdir()
            final.write_bytes(b"wrong")
            clip = {
                "clip_id": 7,
                "recording_id": 3,
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
            ), mock.patch.object(pull, "release_clip", side_effect=RuntimeError("release denied")):
                self.assertFalse(pull.drain_page(cfg, runtime))
            self.assertEqual(runtime.cursor_id, 0)
            self.assertEqual(runtime.last_error, "1 of 1 clips failed; first clip 1: release denied")

    def test_exhausted_retries_are_reported_for_download_and_release(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            clips = [{"clip_id": value, "recording_id": 3} for value in (1, 2)]
            download_error = pull.RetryExhausted(RuntimeError("download failed"), 2)
            release_error = pull.RetryExhausted(RuntimeError("release failed"), 2)
            with mock.patch.object(pull, "request_json", return_value={"clips": clips}), mock.patch.object(
                pull, "process_clip", side_effect=[(1, 10, 10, 0), download_error]
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
