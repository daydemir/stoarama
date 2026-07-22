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
                self.assertEqual(pull.process_clip(cfg, clip), (7, 3))
                release.assert_called_once_with(cfg, 3, 7)

    def test_failed_clip_does_not_block_later_downloads_or_advance_past_it(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            clips = [{"clip_id": value, "recording_id": 1} for value in (1, 2, 3)]

            def process(_cfg, clip, release=True):
                if clip["clip_id"] == 2:
                    raise RuntimeError("poison")
                return clip["clip_id"], 10

            with mock.patch.object(pull, "request_json", return_value={"clips": clips}), mock.patch.object(
                pull, "process_clip", side_effect=process
            ):
                self.assertTrue(pull.drain_page(cfg, runtime))
            self.assertEqual(runtime.cursor_id, 1)
            self.assertEqual(runtime.clips_pulled, 1)
            self.assertEqual(runtime.bytes_pulled, 10)
            persisted = json.loads(cfg.progress_file.read_text())
            self.assertEqual(persisted["after_id"], 1)

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
