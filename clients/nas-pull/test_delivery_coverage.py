import hashlib
import json
import os
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from unittest import mock

import delivery_coverage as verifier

FFMPEG = os.environ.get("FFMPEG_BIN") or shutil.which("ffmpeg")
FFPROBE = os.environ.get("FFPROBE_BIN") or shutil.which("ffprobe")
VERIFIER = Path(__file__).with_name("delivery_coverage.py")


def sha(data):
    return hashlib.sha256(data).hexdigest()


@unittest.skipUnless(FFMPEG and FFPROBE, "media tools are unavailable")
class CoverageFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        scratch = tempfile.TemporaryDirectory()
        cls.addClassCleanup(scratch.cleanup)
        cls.root = Path(scratch.name)
        for name, frequency in (("one", 450), ("two", 990)):
            cls.build("-f", "lavfi", "-i", "testsrc2=s=64x64:r=25:d=1.28",
                      "-f", "lavfi", "-i", f"sine=f={frequency}:r=48000:d=1.28",
                      "-map", "0:v", "-map", "1:a", "-c:v", "libx264", "-g", "32",
                      "-c:a", "aac", "-aac_pns", "0", f"{name}.mp4")
        for result, names in (("delivery", ("one", "two")), ("reordered", ("two", "one"))):
            (cls.root / "concat.txt").write_text(
                "".join(f"file '{name}.mp4'\nduration 1.301333\n" for name in names), encoding="ascii",
            )
            cls.build("-f", "concat", "-safe", "0", "-auto_convert", "0", "-i", "concat.txt",
                      "-copyts", "-map", "0:v:0", "-map", "0:a:0", "-c", "copy",
                      "-movflags", "+faststart", f"{result}.mp4")

    @classmethod
    def build(cls, *args):
        subprocess.run([FFMPEG, "-nostdin", "-v", "error", "-y", *args], cwd=cls.root,
                       check=True, capture_output=True, timeout=30)

    def manifest(self, delivered, *sources, members=("one", "two")):
        def identity(name):
            path = self.root / (name + ".mp4")
            return {"object_key": f"{name}.mp4", "size_bytes": path.stat().st_size, "sha256": sha(path.read_bytes())}

        output = dict(identity(delivered), part=1, parts=1, source_clip_ids=[110, 120],
                      nas_relative_path="delivery/part.mp4")
        clips = [
            {"disposition": "included", "part": 1,
             "clip": dict(identity(name), clip_id=clip_id, recording_id=9, bucket="raw", etag="fixture-etag",
                          start_utc=f"2026-11-19T00:00:0{second}Z", end_utc=f"2026-11-19T00:00:0{second + 2}Z")}
            for name, clip_id, second in zip(members, (110, 120), (0, 2))
        ]
        return dict(schema_version=1, policy_version="collation-v2", generation=2, status="collated",
                    recording_id=9, batch_id="fixture", hour_id="fixture-hour",
                    outputs=[output], clips=clips)

    def verify(self, delivered, *sources, manifest_digest=None, members=("one", "two"), manifest_path=None):
        manifest = json.dumps(self.manifest(delivered, *sources, members=members)).encode()
        path = manifest_path or self.root / "manifest.json"
        if manifest_path is None:
            path.write_bytes(manifest)
        return subprocess.run([sys.executable, str(VERIFIER), "--manifest", str(path), "--manifest-sha256",
                               manifest_digest or sha(manifest), "--part", "1", "--ffmpeg", FFMPEG, "--ffprobe", FFPROBE,
                               f"{delivered}.mp4",
                               *(f"{source}.mp4" for source in sources)], cwd=self.root,
                              capture_output=True, text=True, timeout=30)


class DeliveryCoverageTest(CoverageFixture):
    def test_proves_visible_video_and_integer_aac_through_seam(self):
        result = self.verify("delivery", "one", "two")
        self.assertEqual(result.returncode, 0, result.stderr)
        evidence = json.loads(result.stdout)
        first, second = evidence["sources"]
        self.assertEqual(first["verdict"], "proven")
        self.assertEqual(second["verdict"], "proven")
        self.assertEqual(first["source"]["clip_id"], 110)
        self.assertEqual(second["source"]["clip_id"], 120)
        self.assertEqual(first["tracks"]["audio"]["rule"], "standalone_exact")
        self.assertEqual(second["tracks"]["audio"]["rule"], "seam_primed")
        self.assertEqual(second["tracks"]["audio"]["seam_primer_packets"], 1)
        for row in (first, second):
            self.assertEqual(row["tracks"]["video"]["frame_count"], 32)
            self.assertEqual(row["tracks"]["audio"]["sample_count"], 61440)

    def test_rejects_reordered_delivery_even_with_exact_object_hash(self):
        result = self.verify("reordered", "one", "two")
        self.assertEqual(result.returncode, 1)
        self.assertIn("output packet bytes or flags differ", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_rejects_missing_source(self):
        result = self.verify("delivery", "one")
        self.assertEqual(result.returncode, 1)
        self.assertIn("every source in manifest order", result.stderr)
        self.assertEqual(result.stdout, "")

    def mutated_video(self, source, target, change):
        data = bytearray((self.root / f"{source}.mp4").read_bytes())
        header = data.index(b"tkhd") + 4
        edit = data.index(b"elst") + 4
        self.assertEqual(data[edit], 0)
        self.assertEqual(data[header], 0)
        change(data, header, edit)
        (self.root / f"{target}.mp4").write_bytes(data)
        return target

    def test_rejects_output_dwell_and_disabled_video_track(self):
        cases = (
            ("dwell", lambda data, header, edit: struct.pack_into(">i", data, edit + 16, 0), "non-unit edit rate"),
            ("disabled", lambda data, header, edit: data.__setitem__(header + 3, 0), "track is disabled"),
        )
        for name, change, reason in cases:
            with self.subTest(name=name):
                output = self.mutated_video("delivery", name, change)
                result = self.verify(output, "one", "two")
                self.assertEqual(result.returncode, 1)
                self.assertIn(reason, result.stderr)
                self.assertEqual(result.stdout, "")

    def test_rejects_truncated_movie_clock_and_muted_audio_track(self):
        def truncate_movie(data, header, edit):
            movie = data.index(b"mvhd") + 4
            self.assertEqual(data[movie], 0)
            struct.pack_into(">I", data, movie + 16, 1)

        def mute_audio(data, header, edit):
            audio = data.index(b"tkhd", header) + 4
            self.assertEqual(data[audio], 0)
            struct.pack_into(">h", data, audio + 36, 0)

        for name, change, reason in (("short-movie", truncate_movie, "movie duration"),
                                     ("muted", mute_audio, "playback rate or volume")):
            with self.subTest(name=name):
                output = self.mutated_video("delivery", name, change)
                result = self.verify(output, "one", "two")
                self.assertEqual(result.returncode, 1)
                self.assertIn(reason, result.stderr)
                self.assertEqual(result.stdout, "")

    def test_shorter_output_edit_leaves_only_earlier_source_proven(self):
        def shorten(data, header, edit):
            for offset in (header + 20, edit + 8):
                current = struct.unpack_from(">I", data, offset)[0]
                struct.pack_into(">I", data, offset, current - 39)

        output = self.mutated_video("delivery", "shorter", shorten)
        result = self.verify(output, "one", "two")
        self.assertEqual(result.returncode, 0, result.stderr)
        first, second = json.loads(result.stdout)["sources"]
        self.assertEqual(first["verdict"], "proven")
        self.assertEqual(second["verdict"], "not_proven")
        self.assertNotIn("tracks", second)

    def test_malformed_second_source_handler_preserves_first_source_proof(self):
        def alter_handler(data, header, edit):
            handler = data.index(b"hdlr", header) + 4
            data[handler + 8] = 255

        second = self.mutated_video("two", "handler", alter_handler)
        result = self.verify("delivery", "one", second, members=("one", second))
        self.assertEqual(result.returncode, 0, result.stderr)
        first, second = json.loads(result.stdout)["sources"]
        self.assertEqual(first["verdict"], "proven")
        self.assertEqual(second["verdict"], "not_proven")
        self.assertIn("track and probe layout differ", second["reason"])

    def test_rejects_unbounded_manifest_without_waiting_for_fifo_writer(self):
        fifo = self.root / "manifest.fifo"
        os.mkfifo(fifo)
        large = self.root / "large.manifest"
        with large.open("wb") as file:
            file.truncate(verifier.MANIFEST_LIMIT + 1)
        for path in (fifo, large):
            with self.subTest(path=path):
                result = self.verify("delivery", "one", "two", manifest_path=path)
                self.assertEqual(result.returncode, 1)
                self.assertIn("bounded regular file", result.stderr)
                self.assertEqual(result.stdout, "")

    def test_wrong_decode_schedule_or_final_duration_fails_packet_identity(self):
        with ExitStack() as stack:
            item = verifier.Input(stack, self.root / "one.mp4")
            track = verifier.read_tracks(FFPROBE, item)["video"]
            variants = (
                (replace(track.packets[1], dts=track.packets[1].dts + 1), 1, "PTS/DTS mapping"),
                (replace(track.packets[-1], duration=track.packets[-1].duration + 1), -1, "outside a seam extension"),
            )
            for packet, index, reason in variants:
                with self.subTest(index=index):
                    altered = list(track.packets)
                    altered[index] = packet
                    with self.assertRaisesRegex(verifier.CoverageError, reason):
                        verifier.packet_mapping(track, replace(track, packets=altered), 0)

    def test_aac_seam_and_later_sample_mismatches_are_rejected(self):
        with ExitStack() as stack:
            raw = verifier.Input(stack, self.root / "two.mp4")
            delivered = verifier.Input(stack, self.root / "delivery.mp4")
            previous = verifier.Input(stack, self.root / "one.mp4")
            def load(input_file):
                tracks = verifier.read_tracks(FFPROBE, input_file)
                return tracks["audio"], verifier.media_edits(input_file, tracks)["audio"]

            raw_track, raw_edit = load(raw)
            delivered_track, delivered_edit = load(delivered)
            previous_track, _ = load(previous)
            raw_pcm = verifier.audio_pcm(stack, FFMPEG, FFPROBE, raw, raw_track, raw_edit)
            delivered_pcm = verifier.audio_pcm(stack, FFMPEG, FFPROBE, delivered, delivered_track, delivered_edit)
            start = len(previous_track.packets)
            fd, blocks, frame_size = delivered_pcm
            for index, reason in ((start, "primed seam reference"), (start + 3, "visible audio samples differ")):
                with self.subTest(index=index):
                    offset = blocks[index].offset * frame_size
                    byte = os.pread(fd, 1, offset)
                    os.pwrite(fd, bytes((byte[0] ^ 1,)), offset)
                    with self.assertRaisesRegex(verifier.CoverageError, reason):
                        verifier.prove_audio(FFMPEG, raw, delivered, raw_track, delivered_track,
                                             raw_pcm, delivered_pcm, start)
                    os.pwrite(fd, byte, offset)

    def test_rejects_unpinned_manifest(self):
        result = self.verify("delivery", "one", "two", manifest_digest="0" * 64)
        self.assertEqual(result.returncode, 1)
        self.assertIn("manifest differs from the pinned SHA-256", result.stderr)
        self.assertEqual(result.stdout, "")


@unittest.skipUnless(FFMPEG and FFPROBE, "media tools are unavailable")
class ReusableVerifierTest(CoverageFixture):
    """prove_ordered is the reusable slice behind prove_part and the CLI."""

    def ordered(self, delivered, *sources, **options):
        manifest = self.manifest(delivered, *sources)
        target, candidates = verifier.manifest_part(manifest, 1)
        paths = [self.root / f"{delivered}.mp4", *(self.root / f"{source}.mp4" for source in sources)]
        return target, verifier.prove_ordered(target, candidates, paths, FFMPEG, FFPROBE, **options)

    def test_ordered_paths_match_the_cli_verdict_rows(self):
        result = self.verify("delivery", "one", "two")
        self.assertEqual(result.returncode, 0, result.stderr)
        _, rows = self.ordered("delivery", "one", "two")
        self.assertEqual(rows, json.loads(result.stdout)["sources"])

    def test_borrowed_descriptors_match_paths_and_keep_ownership(self):
        descriptors = []
        try:
            for name in ("delivery", "one", "two"):
                descriptors.append(os.open(self.root / f"{name}.mp4", os.O_RDONLY | os.O_NOFOLLOW))
            _, borrowed = self.ordered("delivery", "one", "two")
            # Same inputs as integer descriptors: identical verdict rows.
            manifest = self.manifest("delivery", "one", "two")
            target, candidates = verifier.manifest_part(manifest, 1)
            _, from_fds = (target, verifier.prove_ordered(target, candidates, descriptors, FFMPEG, FFPROBE))
            self.assertEqual(from_fds, borrowed)
            for descriptor, name in zip(descriptors, ("delivery", "one", "two")):
                self.assertEqual(os.fstat(descriptor).st_size, (self.root / f"{name}.mp4").stat().st_size)
                # Ownership retained: the original descriptor is still open,
                # still at its original offset, and still readable.
                self.assertEqual(os.lseek(descriptor, 0, os.SEEK_CUR), 0)
                self.assertTrue(os.pread(descriptor, 1, 0))
        finally:
            for descriptor in descriptors:
                os.close(descriptor)

    def test_pre_cancelled_attempt_stops_before_any_source_verdict(self):
        cancel = threading.Event()
        cancel.set()
        with self.assertRaisesRegex(verifier.ProofStopped, "cancelled"):
            self.ordered("delivery", "one", "two", cancel=cancel)

    def test_expired_deadline_stops_the_whole_attempt(self):
        with self.assertRaisesRegex(verifier.ProofStopped, "deadline"):
            self.ordered("delivery", "one", "two", deadline=time.monotonic() - 1)

    def test_cancellation_escapes_the_per_source_verdict_catch(self):
        original = verifier.prove_source
        for label, stop_after, expected in (("first", 1, 1), ("final", 2, 2)):
            with self.subTest(cancelled_after=label):
                cancel = threading.Event()
                results = []

                def prove_then_cancel(*args, **kwargs):
                    results.append(original(*args, **kwargs))
                    if len(results) == stop_after:
                        cancel.set()
                    return results[-1]

                with mock.patch.object(verifier, "prove_source", side_effect=prove_then_cancel):
                    with self.assertRaisesRegex(verifier.ProofStopped, "cancelled"):
                        self.ordered("delivery", "one", "two", cancel=cancel)
                # The abort is never a verdict row, even when the cancel lands
                # after the final source computed and its readback finished.
                self.assertEqual(len(results), expected)

    def test_deadline_aborts_mid_decode_instead_of_a_verdict(self):
        calls = {"count": 0}
        cancel = threading.Event()

        def budget_then_expire(run):
            calls["count"] += 1
            if calls["count"] > 2:
                run.deadline = time.monotonic() - 1

        original = verifier.Run.budget

        def budget(run):
            remaining = original(run)
            budget_then_expire(run)
            return remaining

        with mock.patch.object(verifier.Run, "budget", autospec=True, side_effect=budget):
            with self.assertRaisesRegex(verifier.ProofStopped, "deadline"):
                self.ordered("delivery", "one", "two", cancel=cancel, deadline=time.monotonic() + 600)

    def test_cancel_while_parsing_the_output_edit_is_ProofStopped(self):
        cancel = threading.Event()
        calls = []

        def stop_while_reading_output(source, stop, **kwargs):
            calls.append(stop)
            cancel.set()
            # The shared MP4 reader labels its hold with stop.reason; that is
            # the same string Run.check() turns into ProofStopped.
            raise verifier.StitchHold(stop.reason)

        with mock.patch.object(verifier, "stitch_editlist_metadata", side_effect=stop_while_reading_output):
            with self.assertRaisesRegex(verifier.ProofStopped, "^proof cancelled$"):
                self.ordered("delivery", "one", "two", cancel=cancel)
        # The output edit is parsed before any source verdict exists.
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].reason, "proof cancelled")

    def test_uncancelled_edit_hold_keeps_its_ordinary_meaning(self):
        original = verifier.stitch_editlist_metadata
        calls = []

        def hold_on_the_first_source(source, stop, **kwargs):
            calls.append(source)
            if len(calls) == 2:  # the output edit parses first, then source one
                raise verifier.StitchHold("invalid or excessive MP4 boxes")
            return original(source, stop, **kwargs)

        with mock.patch.object(verifier, "stitch_editlist_metadata", side_effect=hold_on_the_first_source):
            _, rows = self.ordered("delivery", "one", "two")
        # No stop was requested, so the hold stays a per-source verdict.
        self.assertEqual([row["verdict"] for row in rows], ["not_proven", "proven"])
        self.assertEqual(rows[0]["reason"], "invalid or excessive MP4 boxes")

    def test_aac_pcm_stderr_sink_keeps_the_ashowinfo_parser_working(self):
        with ExitStack() as stack:
            item = verifier.Input(stack, self.root / "one.mp4")
            track = verifier.read_tracks(FFPROBE, item)["audio"]
            edit = verifier.media_edits(item, verifier.read_tracks(FFPROBE, item))["audio"]
            fd, blocks, frame_size = verifier.audio_pcm(stack, FFMPEG, FFPROBE, item, track, edit)
            self.assertEqual(os.fstat(fd).st_size, sum(block.decoded_samples for block in blocks.values()) * frame_size)
            self.assertTrue(blocks)


if __name__ == "__main__":
    unittest.main()
