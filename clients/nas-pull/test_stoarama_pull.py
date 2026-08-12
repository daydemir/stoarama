import importlib.util
import hashlib
import io
import json
import fcntl
import os
import socket
import sqlite3
import tempfile
import threading
import datetime
import signal
import shutil
import subprocess
import time
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
        (state / "client.lock").touch()
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
            capacity_file=state / "capacity.json",
            inventory_file=state / "inventory.sqlite3",
            current_file=state / "stoarama_pull.py",
            candidate_file=state / "stoarama_pull.candidate.py",
            previous_file=state / "stoarama_pull.previous.py",
            lock_file=state / "client.lock",
            update_manifest_url="https://stoarama.test/nas/download/latest.json",
            download_workers=12,
            poll_interval_sec=10,
            inventory_scan_interval_sec=86400,
            inventory_scan_delay_ms=0,
            inventory_hash_mbps=1000,
            min_free_bytes=100,
            dry_run=dry_run,
            is_candidate=False,
        )

    def certify_media_canary(self, *args, **kwargs):
        with mock.patch.object(pull.os.path, "ismount", return_value=True):
            return pull.certify_media_canary(*args, **kwargs)

    def run_mocked_v1_stitch(self, cfg, generations, validate_run=None, validate_clip=None):
        """Run the complete worker path for current v1 timestamp provenance."""
        start = datetime.datetime(2026, 8, 10, 8, tzinfo=datetime.timezone.utc)
        attempt = "123e4567-e89b-12d3-a456-426614174099"
        contract = {
            "version": 1, "mode": "muxed_source_copy", "audio_selection": "first_optional",
            "tracks": [{"stream_index": 0, "media_type": "video", "time_base_num": 1,
                        "time_base_den": 30, "first_timestamp": 0, "last_timestamp": 29,
                        "last_duration": 1, "unit_count": 30,
                        "codec_signature_sha256": "c" * 64}],
        }
        timeline = {"frame_count": 30, "first_timestamp": 0, "last_timestamp": 29,
                    "last_duration_timestamp": 1, "time_base_numerator": 1,
                    "time_base_denominator": 30, "duplicate_timestamp_count": 0,
                    "non_monotonic_step_count": 0, "discontinuous_step_count": 0}
        frozen, local, paths = [], [], []
        for index, generation in enumerate(generations):
            content = ("v1-clip-%d" % index).encode()
            path = cfg.output_dir / ("recordings/v1-%d.mp4" % index)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            clip_start = start + datetime.timedelta(seconds=index)
            clip_end = clip_start + datetime.timedelta(seconds=1)
            item = {
                "ordinal": index + 1, "clip_id": index + 1, "recording_id": 7,
                "recording_job_id": 9, "relative_path": "recordings/v1-%d.mp4" % index,
                "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
                "clip_start_at": clip_start.isoformat().replace("+00:00", "Z"),
                "clip_end_at": clip_end.isoformat().replace("+00:00", "Z"),
                "capture_generation": generation, "capture_sequence": index + 1,
                "capture_attempt_id": attempt, "timestamp_contract_version": "continuous-source-pts-v1",
                "timestamp_contract_status": "per_clip_probe_complete", "timestamp_contract_reason": "",
                "timestamp_contract_sha256": pull.timestamp_contract_hash(contract),
            }
            frozen.append(item)
            local.append({**item, "clip_start_at": clip_start, "clip_end_at": clip_end,
                          "sidecar_sha256": "b" * 64, "timestamp_contract": contract})
            paths.append(path)
        task = {
            "task_id": 90, "claim_token": "claim", "recording_id": 7, "recording_job_id": 9,
            "policy_version": "native-window-v1", "window_start_at": frozen[0]["clip_start_at"],
            "window_end_at": frozen[-1]["clip_end_at"],
            "clip_manifest_sha256": pull.native_stitch_manifest_hash(frozen), "clips": frozen,
            "inventory_generation": "generation", "inventory_digest": "d" * 64,
            "inventory_completed_at": "2026-08-10T21:00:00Z",
        }
        completed = []
        def request(_cfg, _method, endpoint, body=None, **_kwargs):
            if endpoint.endswith("/complete"):
                completed.append(body["report"])
            return {"ok": True}
        probe = {"stable_signature_v1": {"schema_version": 1, "format_name": "mov,mp4",
                                         "streams": [{"codec_type": "video", "codec_name": "h264"}]}}
        if validate_run is None:
            validate_run = lambda *_args: "lossless_concat_decode_passed"
        if validate_clip is None:
            validate_clip = lambda *_args: None
        summary = {"generation": "generation", "digest": "d" * 64,
                   "scan_completed_at": "2026-08-10T21:00:00Z"}
        with mock.patch.object(pull, "open_certification_inventory", return_value=(SimpleNamespace(close=lambda: None), summary)), \
             mock.patch.object(pull, "collect_certification_candidates", return_value=local), \
             mock.patch.object(pull, "check_native_stitch_delivery", return_value=False), \
             mock.patch.object(pull, "recompute_timestamp_contract", return_value=(contract, {"video": timeline, "_video_frames": []})), \
             mock.patch.object(pull, "native_stitch_video_edge_frames", return_value={"first": [], "last": []}), \
             mock.patch.object(pull, "probe_native_media_cancellable", return_value=probe), \
             mock.patch.object(pull, "strict_decode_media_cancellable", side_effect=validate_clip), \
             mock.patch.object(pull, "validate_native_run_cancellable", side_effect=validate_run), \
             mock.patch.object(pull, "media_tool_version_cancellable", return_value="tool test"), \
             mock.patch.object(pull, "request_json", side_effect=request):
            self.assertTrue(pull._run_native_stitch_task(
                cfg, None, None, threading.Event(), threading.Event(), task,
                datetime.datetime.now(datetime.timezone.utc), time.monotonic() + 60, "ffmpeg", "ffprobe"))
        return completed[-1], paths

    def media_tools(self, root):
        log_path = root / "ffmpeg.log"
        ffprobe = root / "ffprobe"
        ffmpeg = root / "ffmpeg"
        ffprobe.write_text(
            """#!/usr/bin/env python3
import json,sys
if '-version' in sys.argv:
    print('ffprobe test-1')
else:
    print(json.dumps({'format':{'format_name':'mov,mp4','duration':'60.0','start_time':'0.0'},'streams':[{'index':0,'codec_type':'video','codec_name':'h264','codec_tag_string':'avc1','profile':'High','level':40,'pix_fmt':'yuv420p','width':1920,'height':1080,'sample_aspect_ratio':'1:1','time_base':'1/90000','avg_frame_rate':'30/1','r_frame_rate':'30/1','extradata_hash':'SHA256:video','disposition':{'default':1}},{'index':1,'codec_type':'audio','codec_name':'aac','codec_tag_string':'mp4a','sample_rate':'48000','channels':2,'channel_layout':'stereo','time_base':'1/48000','extradata_hash':'SHA256:audio','disposition':{'default':1}}]}))
""",
            encoding="utf-8",
        )
        ffmpeg.write_text(
            """#!/usr/bin/env python3
import os,sys
if '-version' in sys.argv:
    print('ffmpeg test-1')
    raise SystemExit(0)
with open(%r,'a',encoding='utf-8') as out:
    out.write('\\0'.join(sys.argv[1:])+'\\n')
if '-c' in sys.argv and sys.argv[sys.argv.index('-c')+1] == 'copy':
    with open(sys.argv[-1],'wb') as out:
        out.write(b'stitched')
""" % str(log_path),
            encoding="utf-8",
        )
        ffprobe.chmod(0o755)
        ffmpeg.chmod(0o755)
        return ffmpeg, ffprobe, log_path

    def seed_certification_window(self, cfg, count=2):
        inventory = pull.Inventory(cfg)
        inventory._meta_set({
            "generation": "completed-generation",
            "scan_started_at": "2026-08-10T00:00:00Z",
            "scan_completed_at": "2026-08-10T01:00:00Z",
            "scan_pass_started_at": "2026-08-10T00:00:00Z",
            "scan_rows_visited": str(count), "scan_rows_skipped": "0",
            "scan_skip_reasons": "{}", "digest": "d" * 64,
        })
        paths = []
        for index in range(count):
            content = ("native-%d" % index).encode("utf-8")
            clip = {
                "clip_id": index + 1, "recording_id": 77, "recording_job_id": 900,
                "relative_path": "recordings/%d.mp4" % (index + 1),
                "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
                "capture_generation": "gen-1", "capture_sequence": index,
                "clip_start_at": "2026-08-10T%02d:00:00Z" % index,
                "clip_end_at": "2026-08-10T%02d:01:00Z" % index,
            }
            path = cfg.output_dir / clip["relative_path"]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            pull.write_stitch_sidecar(path, clip)
            file_stat = path.stat()
            inventory._upsert(
                clip, "present", "2026-08-10T01:00:00Z", file_stat.st_mtime_ns,
                "completed-generation", file_identity=(file_stat.st_ctime_ns, file_stat.st_ino, file_stat.st_dev),
            )
            paths.append(path)
        inventory.close()
        return paths

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
            self.assertEqual(summary["scan_skip_reasons"], {})
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
            log_messages = []
            with mock.patch.object(pull, "request_json", side_effect=lambda *_a, **kw: calls.append(kw["body"]) or {}), self.assertRaisesRegex(
                RuntimeError, "inventory scan incomplete"
            ), mock.patch.object(pull, "log", side_effect=lambda level, message: log_messages.append((level, message))):
                inventory.full_scan(cfg, threading.Event())
            self.assertFalse(any(body.get("complete") for body in calls))
            self.assertEqual(inventory._rows("clip_id=91")[0][5], "present")
            summary = inventory.summary()
            self.assertIsNone(summary["scan_completed_at"])
            self.assertEqual(summary["scan_rows_skipped"], 1)
            self.assertEqual(summary["scan_skip_reasons"], {"invalid_sidecar": 1})
            self.assertIn(("WARN", "inventory skipped reason=invalid_sidecar count=1"), log_messages)
            self.assertFalse(any(str(broken) in message or "not-json" in message for _, message in log_messages))
            inventory.close()

    def test_inventory_skip_reasons_are_bounded_and_stable(self):
        cases = (
            (pull.FileChangedDuringHash("changed"), False, "changed_during_hash"),
            (FileNotFoundError("gone"), False, "vanished_during_scan"),
            (PermissionError("denied"), False, "permission_denied"),
            (OSError("read"), False, "io_error"),
            (ValueError("bad sidecar"), True, "invalid_sidecar"),
            (RuntimeError("unknown"), False, "unexpected"),
        )
        for exc, sidecar, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(pull.inventory_skip_reason(exc, sidecar), expected)
                self.assertIn(expected, pull.INVENTORY_SKIP_REASONS)
        self.assertEqual(pull.INVENTORY_SKIP_REASONS, frozenset(expected for _, _, expected in cases))

    def test_legacy_or_corrupt_skip_reason_meta_is_omitted(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            inventory = pull.Inventory(cfg)
            inventory._meta_set({
                "generation": "legacy-scan", "scan_rows_skipped": "13",
                "scan_pass_started_at": "2026-08-09T00:00:00Z",
            })
            self.assertNotIn("scan_skip_reasons", inventory.summary())
            for malformed in ('{"io_error":"x"}', '{"io_error":true}', '{"io_error":12}', '[]', '{not-json'):
                with self.subTest(malformed=malformed):
                    inventory._meta_set({"scan_skip_reasons": malformed})
                    self.assertNotIn("scan_skip_reasons", inventory.summary())
            inventory.close()

    def test_media_certification_canary_hashes_decodes_and_losslessly_stitches(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            paths = self.seed_certification_window(cfg)
            before = [path.read_bytes() for path in paths]
            ffmpeg, ffprobe, log_path = self.media_tools(root)
            report = self.certify_media_canary(
                cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                2, str(ffmpeg), str(ffprobe),
            )
            self.assertEqual(report["certification_scope"], "bounded_clip_canary")
            self.assertFalse(report["window_complete"])
            self.assertFalse(report["reencoded"])
            self.assertFalse(report["source_media_modified"])
            self.assertEqual(report["selected_clip_count"], 2)
            self.assertEqual(report["native_runs"][0]["lossless_stitch_validation"], "lossless_concat_and_decode_passed")
            report_hash = report.pop("report_sha256")
            self.assertEqual(report_hash, pull.canonical_report_hash(report))
            self.assertEqual([path.read_bytes() for path in paths], before)
            calls = log_path.read_text(encoding="utf-8").splitlines()
            self.assertTrue(any("\0-c\0copy\0" in ("\0" + call + "\0") for call in calls))
            self.assertTrue(all("-protocol_whitelist\0file,pipe" in call for call in calls))
            self.assertFalse(any(protocol in call for call in calls for protocol in ("http:", "https:", "tcp:", "udp:")))
            self.assertFalse(any("libx264" in call or "libx265" in call or "-vf" in call for call in calls))
            self.assertFalse(any(cfg.state_dir.glob("stoarama-certify-*")))

    def test_media_certification_refuses_incomplete_inventory_and_symlinks(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            paths = self.seed_certification_window(cfg, count=1)
            inventory = pull.Inventory(cfg)
            inventory._meta_set({"scan_completed_at": ""})
            inventory.close()
            ffmpeg, ffprobe, _ = self.media_tools(root)
            with self.assertRaisesRegex(pull.MediaCertificationError, "completed NAS inventory"):
                self.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                    1, str(ffmpeg), str(ffprobe),
                )
            inventory = pull.Inventory(cfg)
            inventory._meta_set({"scan_completed_at": "2026-08-10T01:00:00Z"})
            inventory.close()
            real = paths[0]
            moved = real.with_suffix(".real")
            real.rename(moved)
            real.symlink_to(moved.name)
            with self.assertRaisesRegex(pull.MediaCertificationError, "symlink"):
                self.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                    1, str(ffmpeg), str(ffprobe),
                )

    def test_media_certification_partitions_native_layouts_without_cross_run_concat(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            self.seed_certification_window(cfg)
            ffmpeg, ffprobe, log_path = self.media_tools(root)
            original_probe = pull.probe_native_media
            probe_count = 0

            def different_audio(path, binary):
                nonlocal probe_count
                result = original_probe(path, binary)
                probe_count += 1
                if probe_count == 2:
                    result["signature"]["streams"][1]["extradata_hash"] = "SHA256:different-audio"
                return result

            with mock.patch.object(pull, "probe_native_media", side_effect=different_audio):
                report = self.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                    2, str(ffmpeg), str(ffprobe),
                )
            self.assertEqual(len(report["native_runs"]), 2)
            self.assertTrue(all(run["lossless_stitch_validation"] == "single_clip" for run in report["native_runs"]))
            calls = log_path.read_text(encoding="utf-8").splitlines()
            self.assertFalse(any("\0-c\0copy\0" in ("\0" + call + "\0") for call in calls))

    def test_media_certification_detects_change_during_decode(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            paths = self.seed_certification_window(cfg, count=1)
            ffmpeg, ffprobe, _ = self.media_tools(root)

            def mutate_after_decode(path, _binary):
                path.write_bytes(b"replacement")

            with mock.patch.object(pull, "strict_decode_media", side_effect=mutate_after_decode), self.assertRaisesRegex(
                pull.MediaCertificationError, "identity changed during decode"
            ):
                self.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                    1, str(ffmpeg), str(ffprobe),
                )
            self.assertEqual(paths[0].read_bytes(), b"replacement")

    def test_media_certification_cli_failure_is_bounded(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            stderr = io.StringIO()
            with mock.patch.object(pull, "Config", return_value=cfg), mock.patch("sys.stderr", stderr):
                status = pull.main([
                    "certify", "--recording-id", "77", "--window-start", "bad-secret-path",
                    "--window-end", "2026-08-10T02:00:00Z",
                ])
            self.assertEqual(status, 1)
            self.assertEqual(stderr.getvalue(), "certification failed: window_start must be an ISO-8601 timestamp\n")

    def test_media_tool_subprocess_is_noninteractive(self):
        completed = SimpleNamespace(returncode=0)
        with mock.patch.object(pull.subprocess, "run", return_value=completed) as run:
            self.assertEqual(pull.bounded_tool_output(["ffprobe", "-version"]), b"")
        self.assertIs(run.call_args.kwargs["stdin"], pull.subprocess.DEVNULL)

    def test_media_certification_closes_inventory_on_malformed_digest(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            self.seed_certification_window(cfg, count=1)
            writable = sqlite3.connect(str(cfg.inventory_file))
            writable.execute("UPDATE meta SET value='malformed' WHERE key='digest'")
            writable.commit()
            writable.close()
            real_connect = pull.sqlite3.connect
            opened = []

            def track_connect(*args, **kwargs):
                connection = real_connect(*args, **kwargs)
                opened.append(connection)
                return connection

            with mock.patch.object(pull.sqlite3, "connect", side_effect=track_connect), self.assertRaisesRegex(
                pull.MediaCertificationError, "completion proof is invalid"
            ):
                pull.open_certification_inventory(cfg)
            self.assertEqual(len(opened), 1)
            with self.assertRaises(sqlite3.ProgrammingError):
                opened[0].execute("SELECT 1")

    def test_media_certification_cli_bounds_malformed_sidecar_failure(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            paths = self.seed_certification_window(cfg, count=1)
            sidecar_path = pull.stitch_sidecar_path(paths[0])
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            sidecar["clip_id"] = "not-an-integer"
            sidecar_path.write_text(json.dumps(sidecar) + "\n", encoding="utf-8")
            ffmpeg, ffprobe, _ = self.media_tools(root)
            stderr = io.StringIO()
            with mock.patch.object(pull, "Config", return_value=cfg), mock.patch.object(
                pull.os.path, "ismount", return_value=True
            ), mock.patch("sys.stderr", stderr):
                status = pull.main([
                    "certify", "--recording-id", "77", "--window-start", "2026-08-10T00:00:00Z",
                    "--window-end", "2026-08-10T02:00:00Z", "--ffmpeg-bin", str(ffmpeg),
                    "--ffprobe-bin", str(ffprobe),
                ])
            self.assertEqual(status, 1)
            self.assertIn("stitch sidecar clip_id is invalid", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())
            self.assertNotIn(str(cfg.output_dir), stderr.getvalue())

    def test_media_certification_inventory_is_opened_read_only_and_generation_bound(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            paths = self.seed_certification_window(cfg, count=1)
            database, summary = pull.open_certification_inventory(cfg)
            try:
                self.assertEqual(summary["generation"], "completed-generation")
                with self.assertRaisesRegex(sqlite3.OperationalError, "readonly"):
                    database.execute("UPDATE meta SET value='changed' WHERE key='generation'")
            finally:
                database.close()

            writable = sqlite3.connect(str(cfg.inventory_file))
            writable.execute("UPDATE files SET seen_generation='older-generation'")
            writable.commit()
            writable.close()
            ffmpeg, ffprobe, _ = self.media_tools(root)
            with self.assertRaisesRegex(pull.MediaCertificationError, "completed inventory generation"):
                self.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                    1, str(ffmpeg), str(ffprobe),
                )

    def test_media_certification_refuses_mixed_recording_jobs(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            paths = self.seed_certification_window(cfg)
            sidecar_path = pull.stitch_sidecar_path(paths[1])
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            sidecar["recording_job_id"] = 901
            sidecar_path.write_text(json.dumps(sidecar, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
            ffmpeg, ffprobe, _ = self.media_tools(root)
            with self.assertRaisesRegex(pull.MediaCertificationError, "multiple recording jobs"):
                self.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                    2, str(ffmpeg), str(ffprobe),
                )

    def test_media_certification_refuses_known_nonpresent_window_media(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            self.seed_certification_window(cfg, count=1)
            inventory = pull.Inventory(cfg)
            inventory.db.execute("UPDATE files SET state='mismatch' WHERE clip_id=1")
            inventory.db.commit()
            inventory.close()
            ffmpeg, ffprobe, _ = self.media_tools(root)
            with self.assertRaisesRegex(pull.MediaCertificationError, "without exact NAS proof"):
                self.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                    1, str(ffmpeg), str(ffprobe),
                )

    def test_media_certification_ignores_newer_out_of_window_inventory_rows(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            self.seed_certification_window(cfg, count=1)
            content = b"later"
            clip = {
                "clip_id": 99, "recording_id": 77, "recording_job_id": 901,
                "relative_path": "recordings/99.mp4", "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(), "capture_generation": "gen-live",
                "capture_sequence": 0, "clip_start_at": "2026-08-11T00:00:00Z",
                "clip_end_at": "2026-08-11T00:01:00Z",
            }
            path = cfg.output_dir / clip["relative_path"]
            path.write_bytes(content)
            pull.write_stitch_sidecar(path, clip)
            file_stat = path.stat()
            inventory = pull.Inventory(cfg)
            inventory._upsert(
                clip, "present", "2026-08-11T00:02:00Z", file_stat.st_mtime_ns, "live",
                file_identity=(file_stat.st_ctime_ns, file_stat.st_ino, file_stat.st_dev),
            )
            inventory.close()
            ffmpeg, ffprobe, _ = self.media_tools(root)
            report = self.certify_media_canary(
                cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                1, str(ffmpeg), str(ffprobe),
            )
            self.assertEqual(report["selected_clip_count"], 1)

    def test_media_certification_capture_generation_is_a_hard_run_boundary(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            paths = self.seed_certification_window(cfg)
            sidecar_path = pull.stitch_sidecar_path(paths[1])
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            sidecar["capture_generation"] = "gen-2"
            sidecar["capture_sequence"] = 0
            sidecar_path.write_text(json.dumps(sidecar, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
            ffmpeg, ffprobe, log_path = self.media_tools(root)
            report = self.certify_media_canary(
                cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                2, str(ffmpeg), str(ffprobe),
            )
            self.assertEqual(len(report["native_runs"]), 2)
            self.assertFalse(any("\0-c\0copy\0" in ("\0" + call + "\0") for call in log_path.read_text().splitlines()))

    def test_media_certification_checks_generation_sequences_globally(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            paths = self.seed_certification_window(cfg, count=3)
            changes = ((paths[0], "gen-a", 0), (paths[1], "gen-b", 0), (paths[2], "gen-a", 2))
            for path, generation, sequence in changes:
                sidecar_path = pull.stitch_sidecar_path(path)
                sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
                sidecar["capture_generation"] = generation
                sidecar["capture_sequence"] = sequence
                sidecar_path.write_text(json.dumps(sidecar, sort_keys=True, separators=(",", ":")) + "\n")
            ffmpeg, ffprobe, _ = self.media_tools(root)
            with self.assertRaisesRegex(pull.MediaCertificationError, "sequence is incomplete"):
                self.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T03:00:00Z",
                    3, str(ffmpeg), str(ffprobe),
                )

    def test_media_certification_rejects_duration_mismatch_and_invalid_timing(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            paths = self.seed_certification_window(cfg, count=1)
            ffmpeg, ffprobe, _ = self.media_tools(root)
            with mock.patch.object(pull, "probe_native_media", return_value={
                "duration_seconds": 5.0, "signature": {"streams": []},
                "format_name": "mov,mp4", "container_start_time_seconds": 0.0,
            }), self.assertRaisesRegex(pull.MediaCertificationError, "duration does not match"):
                self.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                    1, str(ffmpeg), str(ffprobe),
                )
            invalid = json.dumps({
                "format": {"format_name": "mov,mp4", "duration": "NaN", "start_time": "0"},
                "streams": [],
            }).encode()
            with mock.patch.object(pull, "bounded_tool_output", return_value=invalid), self.assertRaisesRegex(
                pull.MediaCertificationError, "timing metadata is invalid"
            ):
                pull.probe_native_media(paths[0], str(ffprobe))

    def test_media_certification_fails_closed_when_client_is_running_or_mounts_are_unverified(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cfg = self.config(root)
            self.seed_certification_window(cfg, count=1)
            ffmpeg, ffprobe, _ = self.media_tools(root)
            with self.assertRaisesRegex(pull.MediaCertificationError, "distinct mounted storage roots"):
                pull.certify_media_canary(
                    cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                    1, str(ffmpeg), str(ffprobe),
                )
            holder = open(cfg.lock_file, "r", encoding="utf-8")
            fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                with self.assertRaisesRegex(pull.MediaCertificationError, "must be stopped"):
                    self.certify_media_canary(
                        cfg, 77, "2026-08-10T00:00:00Z", "2026-08-10T02:00:00Z",
                        1, str(ffmpeg), str(ffprobe),
                    )
            finally:
                holder.close()

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

    def test_full_scan_sync_failure_is_not_counted_as_a_skipped_path(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            clip = {
                "clip_id": 1, "recording_id": 1, "relative_path": "recordings/1.mp4",
                "size_bytes": 1, "sha256": hashlib.sha256(b"x").hexdigest(),
            }
            path = cfg.output_dir / clip["relative_path"]
            path.parent.mkdir(parents=True)
            path.write_bytes(b"x")
            pull.write_stitch_sidecar(path, clip)
            inventory = pull.Inventory(cfg)
            with mock.patch.object(pull, "INVENTORY_SYNC_BATCH", 1), mock.patch.object(
                inventory, "sync_dirty", side_effect=RuntimeError("server unavailable")
            ), self.assertRaisesRegex(pull.InventoryProgressError, "progress persistence failed"):
                inventory.full_scan(cfg, threading.Event())
            summary = inventory.summary()
            self.assertEqual(summary["scan_rows_skipped"], 0)
            self.assertEqual(summary["scan_skip_reasons"], {})
            inventory.close()

    def test_full_scan_sqlite_failure_is_not_counted_as_a_skipped_path(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            clip = {
                "clip_id": 1, "recording_id": 1, "relative_path": "recordings/1.mp4",
                "size_bytes": 1, "sha256": hashlib.sha256(b"x").hexdigest(),
            }
            path = cfg.output_dir / clip["relative_path"]
            path.parent.mkdir(parents=True)
            path.write_bytes(b"x")
            pull.write_stitch_sidecar(path, clip)
            inventory = pull.Inventory(cfg)
            with mock.patch.object(
                inventory, "_linked_scan_row_is_current", side_effect=sqlite3.OperationalError("database full")
            ), self.assertRaisesRegex(pull.InventoryProgressError, "state persistence failed"):
                inventory.full_scan(cfg, threading.Event())
            summary = inventory.summary()
            self.assertEqual(summary["scan_rows_skipped"], 0)
            self.assertEqual(summary["scan_skip_reasons"], {})
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
                sidecar = pull.stitch_sidecar_path(path)
                sidecar_bytes = sidecar.read_bytes()
                stat = path.stat()
                inventory._upsert(
                    clip, "present", started_at, stat.st_mtime_ns, generation,
                    commit=False, scan_pass="prior", file_identity=(stat.st_ctime_ns, stat.st_ino, stat.st_dev),
                    sidecar_evidence=(str(sidecar.relative_to(cfg.output_dir)), len(sidecar_bytes), hashlib.sha256(sidecar_bytes).hexdigest()),
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
            clip = {"clip_id": 1, "recording_id": 3, "size_bytes": 10}
            calls = []

            class Inventory:
                def record_verified(self, item):
                    calls.append(("record", item["clip_id"]))

                def sync_clip_ids(self, _cfg, clip_ids):
                    calls.append(("sync", list(clip_ids)))

            with mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**12, "free_bytes": 10**12}), mock.patch.object(pull, "request_json", return_value={"clips": [clip]}), mock.patch.object(
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

            with mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**12, "free_bytes": 10**12}), mock.patch.object(pull, "request_json", return_value={"clips": [clip]}), mock.patch.object(
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

    def test_capacity_guard_fails_closed_and_uses_resume_hysteresis(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            cfg.min_free_bytes = 1_000
            runtime = pull.Runtime(cfg)

            with mock.patch.object(pull, "storage_status", return_value={
                "available": True, "total_bytes": 10**12, "free_bytes": cfg.min_free_bytes - 1,
            }):
                with self.assertRaisesRegex(RuntimeError, "reserve reached"):
                    pull.require_storage_capacity(cfg, runtime)
            self.assertTrue(runtime.heartbeat_payload(None)["capacity_blocked"])
            self.assertEqual(runtime.phase, pull.Phase.BLOCKED)

            with mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**12, "free_bytes": 1_000 + pull.CAPACITY_RESUME_HYSTERESIS_BYTES}):
                pull.require_storage_capacity(cfg, runtime)
            self.assertFalse(runtime.heartbeat_payload(None)["capacity_blocked"])

            with mock.patch.object(pull, "storage_status", side_effect=OSError("stat failed")):
                with self.assertRaisesRegex(RuntimeError, "check unavailable"):
                    pull.require_storage_capacity(cfg, runtime)
            self.assertTrue(runtime.heartbeat_payload(None)["capacity_blocked"])

    def test_capacity_guard_prevents_list_download_release_and_cursor_advance(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            cfg.min_free_bytes = 1_000
            runtime = pull.Runtime(cfg)
            runtime.cursor_id = 41
            blocked = {"available": True, "total_bytes": 10_000, "free_bytes": 999}
            with mock.patch.object(pull, "storage_status", return_value=blocked), mock.patch.object(
                pull, "request_json"
            ) as request, mock.patch.object(pull, "process_clip") as process, mock.patch.object(
                pull, "release_clips"
            ) as release, self.assertRaisesRegex(RuntimeError, "reserve reached"):
                pull.drain_page(cfg, runtime)
            request.assert_not_called()
            process.assert_not_called()
            release.assert_not_called()
            self.assertEqual(runtime.cursor_id, 41)
            self.assertEqual(runtime.phase, pull.Phase.BLOCKED)

    def test_capacity_hysteresis_survives_restart(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            cfg.min_free_bytes = 1_000
            runtime = pull.Runtime(cfg)
            high = {
                "available": True, "total_bytes": 10**12,
                "free_bytes": cfg.min_free_bytes + pull.CAPACITY_RESUME_HYSTERESIS_BYTES,
            }
            self.assertTrue(runtime.reserve_storage(cfg, high))
            low = {"available": True, "total_bytes": 10**12, "free_bytes": cfg.min_free_bytes - 1}
            self.assertFalse(runtime.reserve_storage(cfg, low))
            restarted = pull.Runtime(cfg)
            self.assertTrue(restarted.capacity_blocked)
            self.assertFalse(restarted.reserve_storage(cfg, {
                "available": True, "total_bytes": 10**12,
                "free_bytes": cfg.min_free_bytes + pull.CAPACITY_RESUME_HYSTERESIS_BYTES - 1,
            }))
            self.assertTrue(restarted.reserve_storage(cfg, {
                "available": True, "total_bytes": 10**12,
                "free_bytes": cfg.min_free_bytes + pull.CAPACITY_RESUME_HYSTERESIS_BYTES + 200,
            }))
            # Restart conservatively re-enters blocked hysteresis even after a
            # clean unblock; only a fresh high-watermark stat reopens it.
            self.assertTrue(pull.Runtime(cfg).capacity_blocked)

    def test_capacity_state_write_failure_remains_blocked(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            high = {
                "available": True, "total_bytes": 10**13,
                "free_bytes": cfg.min_free_bytes + pull.CAPACITY_RESUME_HYSTERESIS_BYTES + 200,
            }
            self.assertTrue(runtime.reserve_storage(cfg, high))
            low = {"available": True, "total_bytes": 10**13, "free_bytes": cfg.min_free_bytes - 1}
            with mock.patch.object(pull, "atomic_write", side_effect=OSError("state full")), self.assertRaisesRegex(OSError, "state full"):
                runtime.reserve_storage(cfg, low)
            self.assertTrue(runtime.capacity_blocked)
            self.assertEqual(runtime.capacity_reserved_bytes, 0)
            self.assertTrue(pull.Runtime(cfg).capacity_blocked)

    def test_invalid_clip_size_never_downloads_releases_or_advances_cursor(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            runtime.cursor_id = 41
            storage = {
                "available": True, "total_bytes": 10**13,
                "free_bytes": cfg.min_free_bytes + pull.CAPACITY_RESUME_HYSTERESIS_BYTES,
            }
            for invalid in (None, 0, -1, "100", True):
                clip = {"clip_id": 42, "recording_id": 1, "size_bytes": invalid}
                with self.subTest(size=invalid), mock.patch.object(pull, "storage_status", return_value=storage), mock.patch.object(
                    pull, "request_json", return_value={"clips": [clip]}
                ), mock.patch.object(pull, "process_clip") as process, mock.patch.object(
                    pull, "release_clips"
                ) as release:
                    self.assertFalse(pull.drain_page(cfg, runtime))
                    process.assert_not_called()
                    release.assert_not_called()
                    self.assertEqual(runtime.cursor_id, 41)

    def test_concurrent_capacity_reservations_cannot_cross_floor(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            cfg.min_free_bytes = 1_000
            runtime = pull.Runtime(cfg)
            storage = {"available": True, "total_bytes": 10_000, "free_bytes": 1_300}
            # Establish the unblocked side of hysteresis before workers race.
            self.assertTrue(runtime.reserve_storage(cfg, {
                "available": True, "total_bytes": 10**12,
                "free_bytes": cfg.min_free_bytes + pull.CAPACITY_RESUME_HYSTERESIS_BYTES,
            }))
            barrier = threading.Barrier(3)
            results = []

            def reserve():
                barrier.wait()
                results.append(runtime.reserve_storage(cfg, storage, 200))
                barrier.wait()

            workers = [threading.Thread(target=reserve) for _ in range(2)]
            for worker in workers:
                worker.start()
            barrier.wait()
            barrier.wait()
            for worker in workers:
                worker.join()
            self.assertEqual(sorted(results), [False, True])
            self.assertEqual(runtime.capacity_reserved_bytes, 200)
            runtime.release_storage_reservation(200)
            self.assertEqual(runtime.capacity_reserved_bytes, 0)

    def test_idle_transition_preserves_capacity_blocked_phase(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            pull.set_idle_unless_capacity_blocked(runtime)
            self.assertEqual(runtime.phase, pull.Phase.BLOCKED)
            runtime.capacity_blocked = False
            pull.set_idle_unless_capacity_blocked(runtime)
            self.assertEqual(runtime.phase, pull.Phase.IDLE)
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

    def test_unknown_timestamp_contract_survives_sidecar_exactly(self):
        clip = {
            "clip_id": 9, "recording_id": 13, "size_bytes": 3,
            "sha256": "a" * 64, "relative_path": "recordings/unknown.mp4",
            "clip_start_at": "2026-08-03T12:00:00Z", "clip_end_at": "2026-08-03T12:01:00Z",
            "capture_attempt_id": "00000000-0000-4000-8000-000000000001",
            "timestamp_contract_status": "per_clip_probe_unknown",
            "timestamp_contract_reason": "missing_terminal_duration",
        }
        provenance = pull.stitch_provenance(clip)
        self.assertEqual(provenance["schema_version"], 2)
        self.assertEqual(provenance["capture_attempt_id"], clip["capture_attempt_id"])
        self.assertEqual(provenance["timestamp_contract_status"], "per_clip_probe_unknown")
        self.assertEqual(provenance["timestamp_contract_reason"], "missing_terminal_duration")
        self.assertIsNone(provenance["timestamp_contract"])

    def test_complete_timestamp_contract_survives_sidecar_exactly(self):
        contract = {
            "version": 1, "mode": "muxed_source_copy", "audio_selection": "first_optional",
            "tracks": [{"stream_index": 0, "media_type": "video",
                        "time_base_num": 1, "time_base_den": 1000,
                        "first_timestamp": 0, "last_timestamp": 1000,
                        "last_duration": 40, "unit_count": 26,
                        "codec_signature_sha256": "b" * 64}],
        }
        clip = {
            "clip_id": 10, "recording_id": 13, "size_bytes": 3,
            "sha256": "a" * 64, "relative_path": "recordings/complete.mp4",
            "clip_start_at": "2026-08-03T12:00:00Z", "clip_end_at": "2026-08-03T12:01:00Z",
            "capture_attempt_id": "00000000-0000-4000-8000-000000000002",
            "timestamp_contract_version": "continuous-source-pts-v1",
            "timestamp_contract_status": "per_clip_probe_complete",
            "timestamp_contract": contract,
        }
        provenance = pull.stitch_provenance(clip)
        self.assertEqual(provenance["schema_version"], 2)
        self.assertEqual(provenance["timestamp_contract_version"], "continuous-source-pts-v1")
        self.assertEqual(provenance["timestamp_contract_status"], "per_clip_probe_complete")
        self.assertEqual(provenance["timestamp_contract"], contract)
        self.assertIsNone(provenance["timestamp_contract_reason"])

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
            clips = [{"clip_id": value, "recording_id": 1, "size_bytes": 10} for value in (1, 2, 3)]

            def process(_cfg, clip, release=True):
                if clip["clip_id"] == 2:
                    raise RuntimeError("poison")
                return clip["clip_id"], 10, 10, 0

            with mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**12, "free_bytes": 10**12}), mock.patch.object(pull, "request_json", return_value={"clips": clips}), mock.patch.object(
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
            clip = {"clip_id": 1, "recording_id": 3, "size_bytes": 10}
            with mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**12, "free_bytes": 10**12}), mock.patch.object(pull, "request_json", return_value={"clips": [clip]}), mock.patch.object(
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
            inventory_stop_event = threading.Event()
            ready = threading.Event()
            with mock.patch.object(pull, "stage_update", return_value="new-version") as stage:
                pull.update_loop(cfg, runtime, stop_event, inventory_stop_event, ready)
            stage.assert_called_once_with(cfg)
            self.assertTrue(inventory_stop_event.is_set())
            self.assertTrue(ready.is_set())
            self.assertEqual(runtime.phase, pull.Phase.STARTING)

    def test_update_exec_waits_for_inventory_without_blocking_delivery_thread(self):
        ready = threading.Event()
        ready.set()
        inventory_release = threading.Event()

        def inventory_work():
            inventory_release.wait()

        inventory_worker = threading.Thread(target=inventory_work)
        inventory_worker.start()
        try:
            self.assertFalse(pull.update_can_exec(ready, inventory_worker))
            # The check is nonblocking: the caller can keep draining clips while
            # a slow NAS read reaches its cooperative cancellation point.
            self.assertTrue(inventory_worker.is_alive())
            inventory_release.set()
            inventory_worker.join(timeout=1)
            self.assertTrue(pull.update_can_exec(ready, inventory_worker))
            stopping = threading.Event()
            stopping.set()
            self.assertFalse(pull.update_can_exec(ready, inventory_worker, stopping))
        finally:
            inventory_release.set()
            inventory_worker.join(timeout=1)

    def test_update_exec_failure_is_terminal_and_bounded(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            with mock.patch.object(pull, "mark_runtime") as mark, mock.patch.object(
                pull.os, "execve", side_effect=OSError("secret-bearing operating-system detail")
            ), self.assertRaisesRegex(pull.SelfUpdateExecError, "failed to activate staged NAS client") as caught:
                pull.exec_candidate(cfg, runtime)
            mark.assert_called_once_with(cfg, runtime, pull.PreviousExit.SELF_UPDATE.value)
            self.assertNotIn("secret-bearing", str(caught.exception))

    def test_inventory_dirty_sync_stops_between_batches_and_preserves_unsent_rows(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            inventory = pull.Inventory(cfg)
            with inventory.lock:
                for clip_id in range(1, pull.INVENTORY_SYNC_BATCH + 2):
                    inventory._upsert({
                        "clip_id": clip_id, "recording_id": 1,
                        "relative_path": "recordings/%d.mp4" % clip_id,
                        "size_bytes": 1, "sha256": "a" * 64,
                    }, "present", pull.utc_now_precise(), 1, commit=False)
                inventory.db.commit()
            stop_event = threading.Event()
            calls = []

            def publish(*_args, **kwargs):
                calls.append(kwargs["body"])
                stop_event.set()
                return {}

            with mock.patch.object(pull, "request_json", side_effect=publish):
                inventory.sync_dirty(cfg, stop_event=stop_event)
            self.assertEqual(len(calls), 1)
            remaining = inventory.db.execute("SELECT COUNT(*) FROM files WHERE dirty=1").fetchone()[0]
            self.assertEqual(remaining, 1)
            inventory.close()

    def test_run_activates_quiescent_update_before_storage_work(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            cfg.validate = mock.Mock()
            fake_inventory = mock.Mock()
            fake_lock = mock.Mock()

            class FakeThread:
                def __init__(self, target, args=(), **_kwargs):
                    self.target = target
                    self.args = args
                    self.alive = target is not pull.inventory_loop

                def start(self):
                    if self.target is pull.update_loop:
                        self.target(*self.args)

                def is_alive(self):
                    return self.alive

                def join(self, timeout=None):
                    del timeout

            with mock.patch.object(pull, "acquire_lock", return_value=fake_lock), mock.patch.object(
                pull, "Inventory", return_value=fake_inventory
            ), mock.patch.object(pull.threading, "Thread", FakeThread), mock.patch.object(
                pull.signal, "signal"
            ), mock.patch.object(pull, "mark_runtime") as mark, mock.patch.object(
                pull, "stage_update", return_value="new-version"
            ), mock.patch.object(
                pull, "check_storage", side_effect=AssertionError("storage work ran before update gate")
            ), mock.patch.object(
                pull.os, "execve", side_effect=OSError("activation failed")
            ) as activate, self.assertRaisesRegex(pull.SelfUpdateExecError, "failed to activate"):
                pull.run(cfg)
            activate.assert_called_once()
            self.assertEqual(mark.call_args_list[-1].args[2], pull.PreviousExit.SELF_UPDATE.value)

    def test_signal_during_delivery_never_activates_staged_candidate(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            cfg.validate = mock.Mock()
            fake_inventory = mock.Mock()
            fake_lock = mock.Mock()
            handlers = []
            inventory_threads = []

            class FakeThread:
                def __init__(self, target, args=(), **_kwargs):
                    self.target = target
                    self.args = args
                    self.alive = True
                    if target is pull.inventory_loop:
                        inventory_threads.append(self)

                def start(self):
                    if self.target is pull.update_loop:
                        self.target(*self.args)

                def is_alive(self):
                    return self.alive

                def join(self, timeout=None):
                    del timeout
                    self.alive = False

            def remember_handler(_signal_number, handler):
                handlers.append(handler)

            def stop_during_drain(*_args, **_kwargs):
                handlers[0](None, None)
                inventory_threads[0].alive = False
                return False

            with mock.patch.object(pull, "acquire_lock", return_value=fake_lock), mock.patch.object(
                pull, "Inventory", return_value=fake_inventory
            ), mock.patch.object(pull.threading, "Thread", FakeThread), mock.patch.object(
                pull.signal, "signal", side_effect=remember_handler
            ), mock.patch.object(pull, "mark_runtime"), mock.patch.object(
                pull, "stage_update", return_value="new-version"
            ), mock.patch.object(pull, "check_storage"), mock.patch.object(
                pull, "require_storage_capacity"
            ), mock.patch.object(
                pull, "drain_page", side_effect=stop_during_drain
            ), mock.patch.object(pull, "exec_candidate") as activate:
                self.assertEqual(pull.run(cfg), 0)
            activate.assert_not_called()

    def test_inventory_hash_cancellation_commits_progress_without_completing_scan(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            cfg.inventory_hash_mbps = 1
            content = b"x" * (2 * 1024 * 1024)
            clip = {
                "clip_id": 1, "recording_id": 1, "relative_path": "recordings/1.mp4",
                "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
            }
            path = cfg.output_dir / clip["relative_path"]
            path.parent.mkdir(parents=True)
            path.write_bytes(content)
            pull.write_stitch_sidecar(path, clip)
            inventory = pull.Inventory(cfg)
            stop_event = threading.Event()
            original_wait = stop_event.wait
            reads = 0

            def cancel_after_first_chunk(delay):
                nonlocal reads
                reads += 1
                if reads == 1:
                    stop_event.set()
                    return True
                return original_wait(delay)

            with mock.patch.object(stop_event, "wait", side_effect=cancel_after_first_chunk), mock.patch.object(
                pull, "request_json", return_value={}
            ) as request:
                inventory.full_scan(cfg, stop_event)
            summary = inventory.summary()
            self.assertIsNone(summary["scan_completed_at"])
            self.assertEqual(summary["scan_rows_skipped"], 0)
            self.assertFalse(any(call.kwargs.get("body", {}).get("complete") for call in request.mock_calls))
            inventory.close()

    def test_inventory_final_sync_cancellation_never_promotes_missing_or_complete(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            inventory = pull.Inventory(cfg)
            with inventory.lock:
                inventory.db.execute(
                    """INSERT INTO unmatched_files
                       (relative_path,size_bytes,sha256,state,seen_generation,scan_pass,client_updated_at,dirty)
                       VALUES ('old.mp4',1,?,'present','live','','2000-01-01T00:00:00Z',0)""",
                    ("a" * 64,),
                )
                inventory.db.commit()
            stop_event = threading.Event()
            original_sync = inventory.sync_dirty
            sync_calls = 0

            def cancel_final_sync(*args, **kwargs):
                nonlocal sync_calls
                sync_calls += 1
                stop_event.set()
                return original_sync(*args, **kwargs)

            with mock.patch.object(inventory, "sync_dirty", side_effect=cancel_final_sync), mock.patch.object(
                pull, "request_json", return_value={}
            ) as request:
                inventory.full_scan(cfg, stop_event)
            self.assertEqual(sync_calls, 1)
            state = inventory.db.execute(
                "SELECT state FROM unmatched_files WHERE relative_path='old.mp4'"
            ).fetchone()[0]
            self.assertEqual(state, "present")
            self.assertIsNone(inventory.summary()["scan_completed_at"])
            self.assertFalse(any(call.kwargs.get("body", {}).get("complete") for call in request.mock_calls))
            inventory.close()

    def test_exhausted_retries_are_reported_for_download_and_release(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            runtime = pull.Runtime(cfg)
            clips = [{"clip_id": value, "recording_id": 3, "size_bytes": 10} for value in (1, 2)]
            download_error = pull.RetryExhausted(RuntimeError("download failed"), 2)
            release_error = pull.RetryExhausted(RuntimeError("release failed"), 2)

            def process(_cfg, clip, release=True):
                if clip["clip_id"] == 2:
                    raise download_error
                return 1, 10, 10, 0

            with mock.patch.object(pull, "storage_status", return_value={"available": True, "total_bytes": 10**12, "free_bytes": 10**12}), mock.patch.object(pull, "request_json", return_value={"clips": clips}), mock.patch.object(
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

    def test_full_scan_reports_all_or_none_cleanup_evidence(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            content = b"native clip"
            clip = {
                "schema_version": 1, "clip_id": 71, "recording_id": 9,
                "relative_path": "recording/clip.mp4", "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "clip_start_at": "2026-08-12T00:00:00Z", "clip_end_at": "2026-08-12T00:01:00Z",
                "capture_generation": "gen", "capture_sequence": 1, "recording_job_id": 3,
            }
            media = cfg.output_dir / clip["relative_path"]
            media.parent.mkdir()
            media.write_bytes(content)
            pull.write_stitch_sidecar(media, clip)
            inventory = pull.Inventory(cfg)
            reports = []
            with mock.patch.object(pull, "request_json", side_effect=lambda _c, _m, _p, body=None, **_k: reports.extend(body.get("files", [])) or {}):
                inventory.full_scan(cfg, threading.Event())
            inventory.close()
            report = next(row for row in reports if row["clip_id"] == 71)
            self.assertGreater(report["file_ctime_ns"], 0)
            self.assertGreater(report["file_inode"], 0)
            self.assertGreater(report["file_device"], 0)
            self.assertEqual(report["sidecar_relative_path"], "recording/clip.mp4.stoarama.json")
            sidecar_bytes = pull.stitch_sidecar_path(media).read_bytes()
            self.assertEqual(report["sidecar_size_bytes"], len(sidecar_bytes))
            self.assertEqual(report["sidecar_sha256"], hashlib.sha256(sidecar_bytes).hexdigest())

    def test_incremental_delivery_omits_incomplete_cleanup_evidence(self):
        row = (1, 2, "r/c.mp4", 3, "a" * 64, "present", None, 4, "now", 5, 6, 7, "", 0, "")
        report = pull.Inventory._reports([row])[0]
        self.assertNotIn("file_ctime_ns", report)
        self.assertNotIn("sidecar_relative_path", report)

    def test_full_scan_rejects_sidecar_symlink(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            media = cfg.output_dir / "r/clip.mp4"
            media.parent.mkdir()
            media.write_bytes(b"x")
            target = cfg.output_dir / "target.json"
            target.write_text("{}")
            pull.stitch_sidecar_path(media).symlink_to(target)
            inventory = pull.Inventory(cfg)
            with mock.patch.object(pull, "request_json", return_value={}), self.assertRaisesRegex(RuntimeError, "inventory scan incomplete"):
                inventory.full_scan(cfg, threading.Event())
            summary = inventory.summary()
            inventory.close()
            self.assertEqual(summary["scan_rows_skipped"], 1)
            self.assertEqual(summary["scan_skip_reasons"], {"io_error": 1})

    def test_native_stitch_canonical_hash_utf8_matches_go(self):
        value = {"text": "şehir <&>\u2028\u2029", "nested": {"z": 1, "a": "雪"}}
        self.assertEqual(
            pull.canonical_report_hash(value),
            "52aa500473f858f6ebeac4cf64d06940d591e1c9a866a6c6e81ae045d1655036",
        )

    def test_timestamp_contract_hash_matches_go_typed_struct(self):
        contract = {"version": 1, "mode": "muxed_source_copy", "audio_selection": "first_optional", "tracks": [{
            "stream_index": 0, "media_type": "video", "time_base_num": 1, "time_base_den": 30,
            "first_timestamp": 0, "last_timestamp": 1, "last_duration": 1, "unit_count": 2,
            "codec_signature_sha256": "a" * 64,
        }]}
        self.assertEqual(pull.timestamp_contract_hash(contract), "4ce417b7312b190cee462327346cc95c1133383257004211603869df6bd755fc")

    def test_native_stitch_disabled_never_claims(self):
        cfg = SimpleNamespace(native_stitch_enabled=False)
        with mock.patch.object(pull, "request_json") as request:
            self.assertFalse(pull.maybe_run_native_stitch(cfg, None, None, threading.Event()))
        request.assert_not_called()

    def test_native_stitch_inventory_activity_fence_prevents_claim(self):
        lock = threading.RLock()
        held = threading.Event()
        release = threading.Event()
        def scan_owner():
            with lock:
                held.set()
                release.wait(2)
        owner = threading.Thread(target=scan_owner)
        owner.start()
        self.assertTrue(held.wait(1))
        cfg = SimpleNamespace(native_stitch_enabled=True)
        inventory = SimpleNamespace(activity_lock=lock)
        with mock.patch.object(pull, "request_json") as request:
            self.assertFalse(pull.maybe_run_native_stitch(cfg, None, inventory, threading.Event()))
        request.assert_not_called()
        release.set()
        owner.join(1)

    def test_native_stitch_activity_fence_keeps_inventory_scan_out_until_release(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            inventory = pull.Inventory(cfg)
            entered = threading.Event()
            finished = threading.Event()
            inventory.activity_lock.acquire()
            owned = True
            try:
                with mock.patch.object(inventory, "_full_scan_locked", side_effect=lambda *_: entered.set()):
                    thread = threading.Thread(target=lambda: (inventory.full_scan(cfg, threading.Event()), finished.set()))
                    thread.start()
                    self.assertFalse(entered.wait(.1), "inventory entered while certification owned the activity fence")
                    inventory.activity_lock.release()
                    owned = False
                    self.assertTrue(entered.wait(1))
                    self.assertTrue(finished.wait(1))
                    thread.join(1)
            finally:
                # RLock has no ownership query; release only on the assertion
                # path that failed before the ordinary release above.
                if owned:
                    inventory.activity_lock.release()
                inventory.close()

    def test_native_stitch_delivery_watcher_preempts_active_verification(self):
        cfg = SimpleNamespace(native_stitch_enabled=True)
        inventory = SimpleNamespace(activity_lock=threading.RLock())
        phases = []
        runtime = SimpleNamespace(
            cursor_id=0,
            set_phase=lambda phase: phases.append(phase),
            is_capacity_blocked=lambda: False,
        )
        delivery_checks = 0
        claimed = {"task_id": 9, "claim_token": "claim", "lease_expires_at":
                   (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=45)).isoformat().replace("+00:00", "Z")}
        worker_observed_cancel = threading.Event()

        def request(_cfg, method, path, body=None, **_kwargs):
            nonlocal delivery_checks
            if method == "GET":
                delivery_checks += 1
                return {"clips": [] if delivery_checks == 1 else [{"clip_id": 1}]}
            return {"task": claimed}

        def active_worker(_cfg, _runtime, _inventory, _stop, cancel, *_args):
            if cancel.wait(1):
                worker_observed_cancel.set()
            return True

        with mock.patch.object(pull, "NATIVE_STITCH_DELIVERY_POLL_SEC", .01), \
             mock.patch.object(pull, "request_json", side_effect=request), \
             mock.patch.object(pull, "run_native_stitch_task", side_effect=active_worker):
            self.assertTrue(pull.maybe_run_native_stitch(cfg, runtime, inventory, threading.Event()))
        self.assertTrue(worker_observed_cancel.is_set())
        self.assertGreaterEqual(delivery_checks, 2)
        self.assertEqual(phases[-1], pull.Phase.IDLE)

    def test_native_stitch_post_claim_setup_failure_completes_unknown(self):
        utc = datetime.timezone.utc
        started = datetime.datetime.now(utc)
        task = {
            "task_id": 3, "claim_token": "claim", "recording_id": 7,
            "recording_job_id": 9, "policy_version": "native-window-v1",
            "window_start_at": "2026-08-10T08:00:00Z",
            "window_end_at": "2026-08-10T20:00:00Z",
            "clip_manifest_sha256": "a" * 64, "clips": [],
            "inventory_generation": "generation", "inventory_digest": "d" * 64,
            "inventory_completed_at": "2026-08-10T21:00:00Z",
        }
        calls = []
        def request(_cfg, method, path, body=None, **_kwargs):
            calls.append((method, path, body))
            return {"ok": True}
        with mock.patch.object(pull, "request_json", side_effect=request):
            self.assertTrue(pull.run_native_stitch_task(SimpleNamespace(), None, None, threading.Event(), threading.Event(), task, started, time.monotonic() + 60, "ffmpeg", "ffprobe"))
        self.assertEqual(calls[-1][1], "/account/connections/stitch-certifications/complete")
        self.assertEqual(calls[-1][2]["report"]["status"], "unknown")
        self.assertEqual(calls[-1][2]["report"]["reason_codes"], ["post_claim_setup_failed"])

    def test_cancellable_tool_kills_process_group(self):
        with tempfile.TemporaryDirectory() as raw:
            script = Path(raw) / "hang.py"
            child_pid = Path(raw) / "child.pid"
            script.write_text(
                "import os,subprocess,sys,time\n"
                "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])\n"
                "open(sys.argv[1],'w').write(str(p.pid))\n"
                "time.sleep(60)\n",
                encoding="utf-8",
            )
            cancel = threading.Event()
            timer = threading.Timer(.2, cancel.set)
            timer.start()
            with self.assertRaises(pull.InventoryScanStopped):
                pull.cancellable_tool_output([os.sys.executable, str(script), str(child_pid)], cancel, 30)
            timer.cancel()
            deadline = time.time() + 2
            while not child_pid.exists() and time.time() < deadline:
                time.sleep(.01)
            self.assertTrue(child_pid.exists())
            pid = int(child_pid.read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)

    def test_cancellable_tool_live_caps_stdout_stderr_and_kills_descendants(self):
        for channel in ("stdout", "stderr"):
            with self.subTest(channel=channel), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                script = root / "overflow.py"
                child_pid = root / "child.pid"
                stream = "sys.stdout.buffer" if channel == "stdout" else "sys.stderr.buffer"
                script.write_text(
                    "import subprocess,sys\n"
                    "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])\n"
                    "open(sys.argv[1],'w').write(str(p.pid))\n"
                    "chunk=b'x'*65536\n"
                    "while True:\n"
                    "  %s.write(chunk); %s.flush()\n" % (stream, stream),
                    encoding="utf-8",
                )
                started = time.monotonic()
                with self.assertRaisesRegex(pull.MediaCertificationError, "output exceeded"):
                    pull.cancellable_tool_output(
                        [os.sys.executable, str(script), str(child_pid)], threading.Event(), 30,
                        stdout_limit=1024, stderr_limit=1024)
                self.assertLess(time.monotonic() - started, 3, "live cap waited for the child timeout")
                deadline = time.time() + 2
                while not child_pid.exists() and time.time() < deadline:
                    time.sleep(.01)
                self.assertTrue(child_pid.exists())
                pid = int(child_pid.read_text())
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)

    def test_native_stitch_tool_failures_are_deterministic_only_for_repeatable_corruption(self):
        cancel = threading.Event()
        infrastructure = (
            pull.ToolProcessError(-signal.SIGKILL, b"Killed"),
            pull.ToolProcessError(1, b"Cannot allocate memory"),
            pull.ToolProcessError(1, b"Input/output error"),
            pull.ToolProcessError(127, b"error while loading shared libraries"),
        )
        for failure in infrastructure:
            with self.subTest(returncode=failure.returncode, stderr=failure.stderr), \
                 mock.patch.object(pull, "cancellable_tool_output", side_effect=failure), \
                 self.assertRaises(pull.ToolProcessError):
                pull.strict_decode_media_cancellable(Path("exact.mp4"), "ffmpeg", cancel)
        corrupt = pull.ToolProcessError(1, b"Invalid data found when processing input")
        with mock.patch.object(pull, "cancellable_tool_output", side_effect=[corrupt, corrupt]) as tool, \
             self.assertRaisesRegex(pull.DeterministicMediaError, "clip_decode_failed"):
            pull.strict_decode_media_cancellable(Path("corrupt.mp4"), "ffmpeg", cancel)
        self.assertEqual(tool.call_count, 2)
        with mock.patch.object(pull, "cancellable_tool_output", side_effect=[corrupt, pull.ToolProcessError(-signal.SIGKILL, b"Killed")]), \
             self.assertRaises(pull.ToolProcessError):
            pull.strict_decode_media_cancellable(Path("unstable.mp4"), "ffmpeg", cancel)
        with mock.patch.object(pull, "cancellable_tool_output", side_effect=[corrupt, b""]), \
             self.assertRaisesRegex(pull.MediaCertificationError, "inconsistent"):
            pull.strict_decode_media_cancellable(Path("fail-then-pass.mp4"), "ffmpeg", cancel)
        with tempfile.TemporaryDirectory() as raw, \
             mock.patch.object(pull, "cancellable_tool_output", side_effect=[corrupt, b""]), \
             self.assertRaisesRegex(pull.MediaCertificationError, "inconsistent"):
            pull.validate_native_run_cancellable(
                [Path("left.mp4"), Path("right.mp4")], "ffmpeg", cancel, Path(raw))

    def test_native_stitch_completion_retries_exact_report_inside_lease(self):
        calls = []
        def request(_cfg, _method, _path, body=None, **_kwargs):
            calls.append(body)
            if len(calls) == 1:
                raise urllib.error.URLError(TimeoutError("lost response"))
            return {"ok": True, "replayed": True}
        task = {"claim_token": "claim", "_completion_deadline_monotonic": time.monotonic() + 5}
        with mock.patch.object(pull, "request_json", side_effect=request), mock.patch.object(pull.time, "sleep"):
            result = pull.submit_native_stitch_completion(SimpleNamespace(), task, {"status": "unknown"})
        self.assertTrue(result["replayed"])
        self.assertEqual(calls[0], calls[1], "completion retry changed immutable evidence")

    def test_native_stitch_hash_yields_and_rejects_atomic_replacement(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            relative = "recordings/clip.mp4"
            path = root / relative
            path.parent.mkdir()
            body = b"a" * (2 * 1024 * 1024)
            path.write_bytes(body)
            expected = hashlib.sha256(body).hexdigest()
            cancel = threading.Event()
            original_read = pull.os.read
            reads = 0
            def cancel_after_first(fd, size):
                nonlocal reads
                chunk = original_read(fd, size)
                reads += 1
                if reads == 1:
                    cancel.set()
                return chunk
            with mock.patch.object(pull.os, "read", side_effect=cancel_after_first), self.assertRaises(pull.InventoryScanStopped):
                pull.hash_certification_file_cancellable(root, relative, len(body), expected, cancel)

            cancel.clear()
            replaced = False
            def replace_after_first(fd, size):
                nonlocal replaced
                chunk = original_read(fd, size)
                if not replaced:
                    replacement = root / "replacement.mp4"
                    replacement.write_bytes(body)
                    os.replace(replacement, path)
                    replaced = True
                return chunk
            with mock.patch.object(pull.os, "read", side_effect=replace_after_first), self.assertRaisesRegex(pull.MediaCertificationError, "identity changed"):
                pull.hash_certification_file_cancellable(root, relative, len(body), expected, cancel)

    def test_native_stitch_capacity_is_largest_run_not_total_and_hard_bounded(self):
        clip = lambda size, generation="g", attempt="a", start="s", end="e": {
            "size_bytes": size, "capture_generation": generation,
            "capture_attempt_id": attempt, "timestamp_contract_version": "v",
            "clip_start_at": start, "clip_end_at": end,
        }
        half = pull.NATIVE_STITCH_MAX_RUN_BYTES // 2
        clips = [clip(half, "g1", "a1", "0", "1"), clip(half, "g1", "a1", "1", "2")]
        self.assertEqual(pull.native_stitch_largest_possible_run(clips), pull.NATIVE_STITCH_MAX_RUN_BYTES)
        split = [clip(half, "g1", "a1", "0", "1"), clip(half, "g2", "a2", "1", "2")]
        self.assertEqual(pull.native_stitch_largest_possible_run(split), half)
        with self.assertRaisesRegex(pull.MediaCertificationError, "run byte bound"):
            pull.native_stitch_largest_possible_run([clip(pull.NATIVE_STITCH_MAX_RUN_BYTES + 1)])
        with self.assertRaisesRegex(pull.MediaCertificationError, "manifest"):
            pull.native_stitch_largest_possible_run([clip(1)] * (pull.NATIVE_STITCH_MAX_CLIPS + 1))

    def test_native_stitch_collects_only_frozen_window_rows(self):
        database = sqlite3.connect(":memory:")
        database.execute("""CREATE TABLE files(clip_id integer,recording_id integer,relative_path text,size_bytes integer,
            sha256 text,verified_at text,seen_generation text,state text,clip_start_us integer,clip_end_us integer)""")
        start = datetime.datetime(2026, 8, 10, 8, tzinfo=datetime.timezone.utc)
        end = start + datetime.timedelta(hours=12)
        micros = lambda value: pull.certification_timestamp_microseconds(value, "test")
        rows = [
            (7, 47, "recordings/current.mp4", 4, "a" * 64, "2026-08-10T21:00:00Z", "generation", "present", micros("2026-08-10T08:00:00Z"), micros("2026-08-10T20:00:00Z")),
            (8, 47, "recordings/old-corrupt.mp4", 4, "b" * 64, "2026-08-01T21:00:00Z", "generation", "present", micros("2026-08-01T08:00:00Z"), micros("2026-08-01T20:00:00Z")),
        ]
        database.executemany("INSERT INTO files VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
        frozen = [{"clip_id": 7}]
        sidecar = {"schema_version": 1, "clip_id": 7, "recording_id": 47, "recording_job_id": 9,
                   "relative_path": "recordings/current.mp4", "size_bytes": 4, "sha256": "a" * 64,
                   "clip_start_at": "2026-08-10T08:00:00Z", "clip_end_at": "2026-08-10T20:00:00Z",
                   "capture_generation": "generation", "capture_sequence": 1}
        with mock.patch.object(pull, "read_certification_sidecar", return_value=(sidecar, 10, "c" * 64)) as read:
            candidates = pull.collect_certification_candidates(SimpleNamespace(output_dir=Path("/unused")), database,
                "generation", 47, start, end, frozen)
        self.assertEqual([item["clip_id"] for item in candidates], [7])
        self.assertEqual(read.call_count, 1, "out-of-window history was parsed")
        database.execute("INSERT INTO files VALUES(?,?,?,?,?,?,?,?,?,?)", (9, 47, "recordings/extra.mp4", 4,
            "d" * 64, "2026-08-10T21:00:00Z", "generation", "present", micros("2026-08-10T09:00:00Z"), micros("2026-08-10T10:00:00Z")))
        with self.assertRaisesRegex(pull.MediaCertificationError, "extra media"):
            pull.collect_certification_candidates(SimpleNamespace(output_dir=Path("/unused")), database,
                "generation", 47, start, end, frozen)
        database.close()

    def test_historical_native_stitch_finishes_terminal_partial_with_all_pair_facts(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            start = datetime.datetime(2026, 8, 10, 8, tzinfo=datetime.timezone.utc)
            clips = []
            local = []
            for index in range(2):
                content = ("clip-%d" % index).encode()
                path = cfg.output_dir / ("recordings/%d.mp4" % (index + 1))
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
                frozen = {
                    "ordinal": index + 1, "clip_id": index + 1, "recording_id": 7,
                    "recording_job_id": 9, "relative_path": "recordings/%d.mp4" % (index + 1),
                    "size_bytes": len(content), "sha256": hashlib.sha256(content).hexdigest(),
                    "clip_start_at": (start + datetime.timedelta(seconds=index)).isoformat().replace("+00:00", "Z"),
                    "clip_end_at": (start + datetime.timedelta(seconds=index + 1)).isoformat().replace("+00:00", "Z"),
                    "capture_generation": "generation", "capture_sequence": index + 1,
                    "capture_attempt_id": "", "timestamp_contract_version": "",
                    "timestamp_contract_status": "", "timestamp_contract_reason": "",
                    "timestamp_contract_sha256": "",
                }
                clips.append(frozen)
                local.append({**frozen,
                    "clip_start_at": start + datetime.timedelta(seconds=index),
                    "clip_end_at": start + datetime.timedelta(seconds=index + 1),
                    "sidecar_sha256": "b" * 64,
                })
            task = {
                "task_id": 4, "claim_token": "claim", "recording_id": 7,
                "recording_job_id": 9, "policy_version": "native-window-v1",
                "window_start_at": start.isoformat().replace("+00:00", "Z"),
                "window_end_at": (start + datetime.timedelta(seconds=2)).isoformat().replace("+00:00", "Z"),
                "clip_manifest_sha256": pull.native_stitch_manifest_hash(clips), "clips": clips,
                "inventory_generation": "generation", "inventory_digest": "d" * 64,
                "inventory_completed_at": "2026-08-10T21:00:00Z",
            }
            summary = {"generation": "generation", "digest": "d" * 64, "scan_completed_at": "2026-08-10T21:00:00Z"}
            database = SimpleNamespace(close=lambda: None)
            completed = []
            requests = []
            def request(_cfg, method, path, body=None, **_kwargs):
                requests.append((method, path))
                if path.endswith("/complete"):
                    completed.append(body["report"])
                return {"ok": True}
            probe = {"stable_signature_v1": {"schema_version": 1, "format_name": "mov,mp4", "streams": [{"codec_type": "video", "codec_name": "h264"}]}}
            with mock.patch.object(pull, "open_certification_inventory", return_value=(database, summary)), \
                 mock.patch.object(pull, "collect_certification_candidates", return_value=local), \
                 mock.patch.object(pull, "check_native_stitch_delivery", return_value=False), \
                 mock.patch.object(pull, "probe_native_media_cancellable", return_value=probe), \
                 mock.patch.object(pull, "strict_decode_media_cancellable"), \
                 mock.patch.object(pull, "validate_native_run_cancellable", return_value="lossless_concat_decode_passed"), \
                 mock.patch.object(pull, "media_tool_version_cancellable", return_value="tool test"), \
                 mock.patch.object(pull, "request_json", side_effect=request):
                self.assertTrue(pull._run_native_stitch_task(cfg, None, None, threading.Event(), threading.Event(), task, datetime.datetime.now(datetime.timezone.utc), time.monotonic() + 60, "ffmpeg", "ffprobe"))
            report = completed[-1]
            self.assertEqual(report["status"], "partial")
            self.assertEqual(report["nas_byte_decode_status"], "passed")
            self.assertEqual(report["native_run_concat_status"], "passed")
            self.assertEqual(report["within_run_frame_adjacency_status"], "unknown")
            self.assertEqual(report["within_run_audio_sample_continuity_status"], "not_present")
            self.assertEqual(report["window_continuity_status"], "unknown")
            self.assertEqual(len(report["seams"]), 1)
            self.assertEqual(report["seams"][0]["verdict"], "ambiguous")
            self.assertEqual(len(report["audio_seams"]), 1)
            self.assertEqual(report["audio_seams"][0]["verdict"], "not_present")
            self.assertEqual(requests, [("POST", "/account/connections/stitch-certifications/complete")])
            for index in range(2):
                self.assertEqual((cfg.output_dir / ("recordings/%d.mp4" % (index + 1))).read_bytes(), ("clip-%d" % index).encode())

    def test_native_stitch_deterministic_clip_failure_persists_exact_failed_fact(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            start = datetime.datetime(2026, 8, 10, 8, tzinfo=datetime.timezone.utc)
            content = b"bad-clip"
            path = cfg.output_dir / "recordings/1.mp4"
            path.parent.mkdir(parents=True)
            path.write_bytes(content)
            frozen = {
                "ordinal": 1, "clip_id": 1, "recording_id": 7, "recording_job_id": 9,
                "relative_path": "recordings/1.mp4", "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "clip_start_at": start.isoformat().replace("+00:00", "Z"),
                "clip_end_at": (start + datetime.timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
                "capture_generation": "generation", "capture_sequence": 1,
                "capture_attempt_id": "", "timestamp_contract_version": "",
                "timestamp_contract_status": "", "timestamp_contract_reason": "",
                "timestamp_contract_sha256": "",
            }
            local = [{**frozen, "clip_start_at": start,
                      "clip_end_at": start + datetime.timedelta(seconds=1),
                      "sidecar_sha256": "b" * 64}]
            task = {
                "task_id": 5, "claim_token": "claim", "recording_id": 7, "recording_job_id": 9,
                "policy_version": "native-window-v1", "window_start_at": frozen["clip_start_at"],
                "window_end_at": frozen["clip_end_at"], "clip_manifest_sha256": pull.native_stitch_manifest_hash([frozen]),
                "clips": [frozen], "inventory_generation": "generation", "inventory_digest": "d" * 64,
                "inventory_completed_at": "2026-08-10T21:00:00Z",
            }
            completed = []
            def request(_cfg, _method, endpoint, body=None, **_kwargs):
                if endpoint.endswith("/complete"):
                    completed.append(body["report"])
                return {"ok": True}
            probe = {"stable_signature_v1": {"schema_version": 1, "format_name": "mov,mp4", "streams": [{"codec_type": "video", "codec_name": "h264"}]}}
            with mock.patch.object(pull, "open_certification_inventory", return_value=(SimpleNamespace(close=lambda: None), {"generation": "generation", "digest": "d" * 64, "scan_completed_at": "2026-08-10T21:00:00Z"})), \
                 mock.patch.object(pull, "collect_certification_candidates", return_value=local), \
                 mock.patch.object(pull, "check_native_stitch_delivery", return_value=False), \
                 mock.patch.object(pull, "probe_native_media_cancellable", return_value=probe), \
                 mock.patch.object(pull, "strict_decode_media_cancellable", side_effect=pull.DeterministicMediaError("clip_decode_failed")), \
                 mock.patch.object(pull, "media_tool_version_cancellable", return_value="tool test"), \
                 mock.patch.object(pull, "request_json", side_effect=request):
                self.assertTrue(pull._run_native_stitch_task(cfg, None, None, threading.Event(), threading.Event(), task, datetime.datetime.now(datetime.timezone.utc), time.monotonic() + 60, "ffmpeg", "ffprobe"))
            report = completed[-1]
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["reason_codes"], ["clip_decode_failed"])
            self.assertEqual(report["nas_byte_decode_status"], "failed")
            self.assertEqual(len(report["clips"]), 1)
            self.assertEqual(report["clips"][0]["strict_decode"], "failed")

    def test_native_stitch_signature_ignores_stream_order_and_index(self):
        video = {"index": 0, "codec_type": "video", "codec_name": "h264", "time_base": "1/90000", "extradata_hash": "SHA256:v", "pix_fmt": "yuv420p", "width": 1920, "height": 1080}
        audio = {"index": 1, "codec_type": "audio", "codec_name": "aac", "time_base": "1/48000", "extradata_hash": "SHA256:a", "sample_fmt": "fltp", "sample_rate": "48000", "channels": 2}
        first = pull.stable_native_signature_v1([video, audio], "mov,mp4")
        video["index"], audio["index"] = 9, 3
        second = pull.stable_native_signature_v1([audio, video], "mov,mp4")
        self.assertEqual(pull.canonical_report_hash(first), pull.canonical_report_hash(second))

    def test_timestamp_contract_is_recomputed_from_exact_bytes_with_rational_evidence(self):
        payload = {
            "streams": [
                {"index": 0, "codec_type": "video", "codec_name": "h264", "codec_tag_string": "avc1", "profile": "High", "level": 40, "width": 1920, "height": 1080, "pix_fmt": "yuv420p", "time_base": "1/30", "extradata": "abc"},
                {"index": 1, "codec_type": "audio", "codec_name": "aac", "codec_tag_string": "mp4a", "profile": "LC", "time_base": "1/48000", "extradata": "def", "sample_rate": "48000", "channels": 2, "channel_layout": "stereo"},
            ],
            "frames": [
                {"stream_index": 0, "media_type": "video", "best_effort_timestamp": 0, "pkt_dts": 0, "pkt_duration": 1, "key_frame": 1, "pict_type": "I"},
                {"stream_index": 0, "media_type": "video", "best_effort_timestamp": 1, "pkt_dts": 1, "pkt_duration": 1, "key_frame": 0, "pict_type": "P"},
                {"stream_index": 0, "media_type": "video", "best_effort_timestamp": 3, "pkt_dts": 3, "pkt_duration": 1, "key_frame": 0, "pict_type": "P"},
                {"stream_index": 1, "media_type": "audio", "best_effort_timestamp": 0, "pkt_dts": 0, "pkt_duration": 1024, "nb_samples": 1024},
                {"stream_index": 1, "media_type": "audio", "best_effort_timestamp": 1024, "pkt_dts": 1024, "pkt_duration": 1024, "nb_samples": 1024},
            ],
            "packets": [
                {"stream_index": 0, "pts": 0, "dts": -2, "duration": 1, "data_hash": "SHA256:" + "a" * 64},
                {"stream_index": 0, "pts": 1, "dts": -1, "duration": 1, "data_hash": "SHA256:" + "b" * 64},
                {"stream_index": 0, "pts": 3, "dts": 0, "duration": 1, "data_hash": "SHA256:" + "c" * 64},
            ],
        }
        raw = json.dumps(payload, separators=(",", ":")).encode()
        with mock.patch.object(pull, "cancellable_tool_output", return_value=raw) as tool:
            contract, timelines = pull.recompute_timestamp_contract(Path("safe.mp4"), "ffprobe", threading.Event())
        self.assertEqual(contract["version"], 1)
        self.assertEqual(contract["mode"], "muxed_source_copy")
        self.assertEqual(contract["audio_selection"], "first_optional")
        self.assertEqual(contract["tracks"][0]["unit_count"], 3)
        self.assertEqual(contract["tracks"][1]["last_sample_count"], 1024)
        self.assertEqual(timelines["video"]["discontinuous_step_count"], 1)
        self.assertEqual(timelines["_video_frames"][0]["packet_dts"], -2)
        self.assertEqual(timelines["_video_frames"][2]["packet_sha256"], "c" * 64)
        self.assertEqual(timelines["audio"]["first_sample"], 0)
        self.assertEqual(timelines["audio"]["end_sample"], 2048)
        self.assertEqual(tool.call_args.kwargs["stdout_limit"], 16 * 1024 * 1024)

    def test_timestamp_contract_duration_alias_matches_capture_and_fails_closed(self):
        self.assertEqual(pull.timestamp_frame_duration({"duration": 1001}), 1001)
        self.assertEqual(pull.timestamp_frame_duration({"pkt_duration": 1001}), 1001)
        self.assertEqual(pull.timestamp_frame_duration({"duration": "1001", "pkt_duration": 1001}), 1001)
        for frame in (
            {},
            {"duration": None, "pkt_duration": None},
            {"duration": 1001, "pkt_duration": None},
            {"duration": "N/A", "pkt_duration": 1001},
            {"duration": 0},
            {"duration": 1.5},
            {"duration": 1001, "pkt_duration": 1000},
        ):
            with self.subTest(frame=frame), self.assertRaises(pull.MediaCertificationError):
                pull.timestamp_frame_duration(frame)

        fixture_path = MODULE_PATH.parents[2] / "backend" / "internal" / "capture" / "testdata" / "ffprobe-8.1.2-bframes-aac-native-copy.json"
        payload = json.loads(fixture_path.read_text())
        self.assertTrue(all("duration" in frame and "pkt_duration" not in frame for frame in payload["frames"]))
        payload["packets"] = [
            {"stream_index": 0, "pts": frame["best_effort_timestamp"], "dts": frame["pkt_dts"],
             "duration": frame["duration"], "data_hash": "SHA256:" + chr(ord("a") + index) * 64}
            for index, frame in enumerate(payload["frames"])
            if frame["media_type"] == "video"
        ]
        with mock.patch.object(pull, "cancellable_tool_output", return_value=json.dumps(payload).encode()):
            contract, timelines = pull.recompute_timestamp_contract(Path("captured.mp4"), "ffprobe", threading.Event())
        self.assertEqual(contract["tracks"][0]["last_duration"], 1001)
        self.assertEqual(contract["tracks"][1]["last_duration"], 1024)
        self.assertEqual(timelines["video"]["frame_count"], 3)

    def test_native_stitch_decoded_frame_edges_bind_presentation_order_and_hashes(self):
        rows = (
            "#tb 0: 1/30\n"
            "#stream#, dts, pts, duration, size, hash\n"
            "0, -2, 0, 1, 6144, " + "a" * 64 + "\n"
            "0, -1, 1, 1, 6144, " + "b" * 64 + "\n"
            "0, 0, 2, 1, 6144, " + "c" * 64 + "\n"
        ).encode()
        source = [{"best_effort_timestamp": i, "duration_timestamp": 1,
                   "time_base_numerator": 1, "time_base_denominator": 30,
                   "packet_dts": i - 2, "key_frame": i == 0,
                   "picture_type": "I" if i == 0 else "P", "packet_sha256": "d" * 64}
                  for i in range(3)]
        with mock.patch.object(pull, "cancellable_tool_output", return_value=rows):
            edge = pull.native_stitch_video_edge_frames(Path("safe.mp4"), "ffmpeg", source, threading.Event())
        self.assertEqual(edge["first"][0]["best_effort_timestamp"], 0)
        self.assertEqual(edge["last"][-1]["decoded_sha256"], "c" * 64)
        self.assertEqual(len(edge["first"]), 3)

    def test_native_stitch_real_reset0_video_fixture_reaches_full_pass(self):
        ffmpeg = shutil.which("ffmpeg")
        ffprobe = shutil.which("ffprobe")
        if not ffmpeg or not ffprobe:
            self.skipTest("ffmpeg/ffprobe required for native seam fixture")
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            media_dir = cfg.output_dir / "recordings"
            media_dir.mkdir(parents=True)
            subprocess.run([
                ffmpeg, "-v", "error", "-f", "lavfi", "-i", "testsrc=size=64x64:rate=5",
                "-t", "4", "-c:v", "libx264", "-g", "10", "-keyint_min", "10", "-sc_threshold", "0",
                "-pix_fmt", "yuv420p", "-f", "segment", "-segment_time", "2", "-reset_timestamps", "0",
                str(media_dir / "%02d.mp4"),
            ], check=True)
            paths = sorted(media_dir.glob("*.mp4"))
            self.assertEqual(len(paths), 2)
            start = datetime.datetime(2026, 8, 10, 8, tzinfo=datetime.timezone.utc)
            attempt = "123e4567-e89b-12d3-a456-426614174000"
            frozen = []
            local = []
            for index, path in enumerate(paths):
                body = path.read_bytes()
                contract, _ = pull.recompute_timestamp_contract(path, ffprobe, threading.Event())
                item = {
                    "ordinal": index + 1, "clip_id": index + 1, "recording_id": 7, "recording_job_id": 9,
                    "relative_path": "recordings/%s" % path.name, "size_bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "clip_start_at": (start + datetime.timedelta(seconds=index * 2)).isoformat().replace("+00:00", "Z"),
                    "clip_end_at": (start + datetime.timedelta(seconds=(index + 1) * 2)).isoformat().replace("+00:00", "Z"),
                    "capture_generation": "generation", "capture_sequence": index + 1,
                    "capture_attempt_id": attempt, "timestamp_contract_version": "continuous-source-pts-v1",
                    "timestamp_contract_status": "per_clip_probe_complete", "timestamp_contract_reason": "",
                    "timestamp_contract_sha256": pull.timestamp_contract_hash(contract),
                }
                frozen.append(item)
                local.append({**item, "clip_start_at": start + datetime.timedelta(seconds=index * 2),
                              "clip_end_at": start + datetime.timedelta(seconds=(index + 1) * 2),
                              "sidecar_sha256": "b" * 64, "timestamp_contract": contract})
            task = {
                "task_id": 6, "claim_token": "claim", "recording_id": 7, "recording_job_id": 9,
                "policy_version": "native-window-v1", "window_start_at": frozen[0]["clip_start_at"],
                "window_end_at": frozen[-1]["clip_end_at"], "clip_manifest_sha256": pull.native_stitch_manifest_hash(frozen),
                "clips": frozen, "inventory_generation": "generation", "inventory_digest": "d" * 64,
                "inventory_completed_at": "2026-08-10T21:00:00Z",
            }
            completed = []
            def request(_cfg, _method, endpoint, body=None, **_kwargs):
                if endpoint.endswith("/complete"):
                    completed.append(body["report"])
                return {"ok": True}
            summary = {"generation": "generation", "digest": "d" * 64, "scan_completed_at": "2026-08-10T21:00:00Z"}
            with mock.patch.object(pull, "open_certification_inventory", return_value=(SimpleNamespace(close=lambda: None), summary)), \
                 mock.patch.object(pull, "collect_certification_candidates", return_value=local), \
                 mock.patch.object(pull, "check_native_stitch_delivery", return_value=False), \
                 mock.patch.object(pull, "request_json", side_effect=request):
                self.assertTrue(pull._run_native_stitch_task(cfg, None, None, threading.Event(), threading.Event(), task,
                    datetime.datetime.now(datetime.timezone.utc), time.monotonic() + 120, ffmpeg, ffprobe))
            report = completed[-1]
            self.assertEqual(report["status"], "partial", report)
            self.assertEqual(report["within_run_frame_adjacency_status"], "unknown")
            self.assertEqual(report["within_run_audio_sample_continuity_status"], "not_present")
            self.assertEqual(report["window_continuity_status"], "unknown")
            self.assertEqual(report["seams"][0]["verdict"], "ambiguous")
            self.assertEqual(report["seams"][0]["reason"], "continuous_source_pts_unavailable")

    def test_native_stitch_real_reset0_bframes_aac_keeps_audio_axis_fail_closed(self):
        ffmpeg = shutil.which("ffmpeg")
        ffprobe = shutil.which("ffprobe")
        if not ffmpeg or not ffprobe:
            self.skipTest("ffmpeg/ffprobe required for native seam fixture")
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            media_dir = cfg.output_dir / "recordings"
            media_dir.mkdir(parents=True)
            subprocess.run([
                ffmpeg, "-v", "error",
                "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=30000/1001",
                "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000",
                "-t", "4.004", "-map", "0:v:0", "-map", "1:a:0",
                "-c:v", "libx264", "-bf", "3", "-g", "60", "-keyint_min", "60", "-sc_threshold", "0",
                "-pix_fmt", "yuv420p", "-c:a", "aac", "-f", "segment", "-segment_time", "2.002",
                "-reset_timestamps", "0", str(media_dir / "%02d.mp4"),
            ], check=True)
            paths = sorted(media_dir.glob("*.mp4"))
            self.assertEqual(len(paths), 2)
            start = datetime.datetime(2026, 8, 10, 8, tzinfo=datetime.timezone.utc)
            attempt = "123e4567-e89b-12d3-a456-426614174001"
            frozen, local = [], []
            for index, path in enumerate(paths):
                body = path.read_bytes()
                contract, _ = pull.recompute_timestamp_contract(path, ffprobe, threading.Event())
                clip_start = start + datetime.timedelta(seconds=index * 2.002)
                clip_end = start + datetime.timedelta(seconds=(index + 1) * 2.002)
                item = {
                    "ordinal": index + 1, "clip_id": index + 11, "recording_id": 7, "recording_job_id": 10,
                    "relative_path": "recordings/%s" % path.name, "size_bytes": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "clip_start_at": clip_start.isoformat().replace("+00:00", "Z"),
                    "clip_end_at": clip_end.isoformat().replace("+00:00", "Z"),
                    "capture_generation": "generation-aac", "capture_sequence": index + 1,
                    "capture_attempt_id": attempt, "timestamp_contract_version": "continuous-source-pts-v1",
                    "timestamp_contract_status": "per_clip_probe_complete", "timestamp_contract_reason": "",
                    "timestamp_contract_sha256": pull.timestamp_contract_hash(contract),
                }
                frozen.append(item)
                local.append({**item, "clip_start_at": clip_start, "clip_end_at": clip_end,
                              "sidecar_sha256": "b" * 64, "timestamp_contract": contract})
            task = {
                "task_id": 7, "claim_token": "claim", "recording_id": 7, "recording_job_id": 10,
                "policy_version": "native-window-v1", "window_start_at": frozen[0]["clip_start_at"],
                "window_end_at": frozen[-1]["clip_end_at"], "clip_manifest_sha256": pull.native_stitch_manifest_hash(frozen),
                "clips": frozen, "inventory_generation": "generation", "inventory_digest": "d" * 64,
                "inventory_completed_at": "2026-08-10T21:00:00Z",
            }
            completed = []
            def request(_cfg, _method, endpoint, body=None, **_kwargs):
                if endpoint.endswith("/complete"):
                    completed.append(body["report"])
                return {"ok": True}
            summary = {"generation": "generation", "digest": "d" * 64, "scan_completed_at": "2026-08-10T21:00:00Z"}
            with mock.patch.object(pull, "open_certification_inventory", return_value=(SimpleNamespace(close=lambda: None), summary)), \
                 mock.patch.object(pull, "collect_certification_candidates", return_value=local), \
                 mock.patch.object(pull, "check_native_stitch_delivery", return_value=False), \
                 mock.patch.object(pull, "request_json", side_effect=request):
                self.assertTrue(pull._run_native_stitch_task(cfg, None, None, threading.Event(), threading.Event(), task,
                    datetime.datetime.now(datetime.timezone.utc), time.monotonic() + 120, ffmpeg, ffprobe))
            report = completed[-1]
            self.assertEqual(report["status"], "partial", report)
            self.assertEqual(report["nas_byte_decode_status"], "passed")
            self.assertEqual(report["native_run_concat_status"], "passed")
            self.assertEqual(report["within_run_frame_adjacency_status"], "unknown")
            self.assertEqual(report["within_run_audio_sample_continuity_status"], "unknown")
            self.assertEqual(report["window_continuity_status"], "unknown")
            self.assertEqual(report["audio_seams"][0]["verdict"], "ambiguous")

    def test_native_stitch_timeline_tracks_internal_without_edge_gaps(self):
        utc = datetime.timezone.utc
        start = datetime.datetime(2026, 8, 12, tzinfo=utc)
        clips = [
            {"clip_start_at": start, "clip_end_at": start + datetime.timedelta(seconds=10)},
            {"clip_start_at": start + datetime.timedelta(seconds=20), "clip_end_at": start + datetime.timedelta(seconds=30)},
        ]
        got = pull.native_stitch_timeline(clips, start, start + datetime.timedelta(seconds=30))
        self.assertEqual(got["leading_gap_seconds"], 0)
        self.assertEqual(got["trailing_gap_seconds"], 0)
        self.assertEqual(got["largest_internal_gap_seconds"], 10)
        self.assertEqual(got["gap_count"], 1)

    def test_native_stitch_whole_window_rejects_single_run_edge_loss(self):
        full = {"expected_seconds": 43200, "covered_seconds": 43200, "leading_gap_seconds": 0,
                "largest_internal_gap_seconds": 0, "trailing_gap_seconds": 0, "gap_count": 0,
                "overlap_count": 0, "overlap_seconds": 0}
        self.assertTrue(pull.native_stitch_full_envelope(full))
        lost = dict(full, covered_seconds=42300, leading_gap_seconds=900, gap_count=1)
        self.assertFalse(pull.native_stitch_full_envelope(lost))

    def test_native_stitch_single_clip_cannot_bypass_timestamp_axis_gates(self):
        historical = [{"timestamp_contract_status": "", "audio_present": False}]
        self.assertEqual(pull.native_stitch_clip_axis_continuity(historical), (False, True))
        incomplete_audio = [{
            "timestamp_contract_status": "per_clip_probe_complete", "audio_present": True,
            "video_timeline": {"duplicate_timestamp_count": 0, "non_monotonic_step_count": 0, "discontinuous_step_count": 0},
            "audio_timeline": None,
        }]
        self.assertEqual(pull.native_stitch_clip_axis_continuity(incomplete_audio), (False, False))

    def test_native_stitch_v1_singleton_finishes_terminal_partial(self):
        with tempfile.TemporaryDirectory() as raw:
            report, _ = self.run_mocked_v1_stitch(self.config(Path(raw)), ["generation-1"])
        self.assertEqual(report["status"], "partial", report)
        self.assertEqual(report["nas_byte_decode_status"], "passed")
        self.assertEqual(report["native_run_concat_status"], "passed")
        self.assertEqual(report["within_run_frame_adjacency_status"], "unknown")
        self.assertEqual(report["within_run_audio_sample_continuity_status"], "not_present")
        self.assertEqual(report["window_continuity_status"], "unknown")
        self.assertEqual(report["reason_codes"], ["continuous_source_pts_unavailable"])
        self.assertEqual(report["seams"], [])

    def test_native_stitch_v1_all_objective_boundaries_finish_terminal_partial(self):
        with tempfile.TemporaryDirectory() as raw:
            report, _ = self.run_mocked_v1_stitch(
                self.config(Path(raw)), ["generation-1", "generation-2"])
        self.assertEqual(report["status"], "partial", report)
        self.assertEqual(report["native_run_concat_status"], "passed")
        self.assertEqual(report["within_run_frame_adjacency_status"], "unknown")
        self.assertEqual(report["window_continuity_status"], "partitioned")
        self.assertEqual(len(report["native_runs"]), 2)
        self.assertEqual(len(report["seams"]), 1)
        self.assertEqual(report["seams"][0]["verdict"], "not_applicable")
        self.assertEqual(report["seams"][0]["reason"], "capture_generation_change")

    def test_native_stitch_source_replacement_during_failed_concat_is_unknown(self):
        with tempfile.TemporaryDirectory() as raw:
            cfg = self.config(Path(raw))
            def replace_then_fail(paths, *_args):
                replacement = paths[0].with_suffix(".replacement")
                replacement.write_bytes(b"changed source identity")
                os.replace(replacement, paths[0])
                raise pull.DeterministicMediaError("run_concat_failed")
            report, _ = self.run_mocked_v1_stitch(
                cfg, ["generation-1", "generation-1"], validate_run=replace_then_fail)
        self.assertEqual(report["status"], "unknown", report)
        self.assertEqual(report["reason_codes"], ["verification_transient"])
        self.assertEqual(report["nas_byte_decode_status"], "unknown")
        self.assertEqual(report["native_run_concat_status"], "unknown")
        self.assertEqual(report["clips"], [])
        self.assertEqual(report["native_runs"], [])

    def test_native_stitch_inconsistent_clip_decode_is_unknown_without_axes(self):
        with tempfile.TemporaryDirectory() as raw:
            report, _ = self.run_mocked_v1_stitch(
                self.config(Path(raw)), ["generation-1"],
                validate_clip=lambda *_args: (_ for _ in ()).throw(
                    pull.MediaCertificationError("media verification result was inconsistent")))
        self.assertEqual(report["status"], "unknown", report)
        self.assertEqual(report["nas_byte_decode_status"], "unknown")
        self.assertEqual(report["native_run_concat_status"], "unknown")
        self.assertEqual(report["clips"], [])
        self.assertEqual(report["native_runs"], [])

    def test_native_stitch_inconsistent_concat_is_unknown_without_axes(self):
        with tempfile.TemporaryDirectory() as raw:
            report, _ = self.run_mocked_v1_stitch(
                self.config(Path(raw)), ["generation-1", "generation-1"],
                validate_run=lambda *_args: (_ for _ in ()).throw(
                    pull.MediaCertificationError("media verification result was inconsistent")))
        self.assertEqual(report["status"], "unknown", report)
        self.assertEqual(report["nas_byte_decode_status"], "unknown")
        self.assertEqual(report["native_run_concat_status"], "unknown")
        self.assertEqual(report["clips"], [])
        self.assertEqual(report["native_runs"], [])


if __name__ == "__main__":
    unittest.main()
