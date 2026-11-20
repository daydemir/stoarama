"""Offline Durham AAC concat tests; synthetic 48 kHz fixtures only."""
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack
from fractions import Fraction
from pathlib import Path
from unittest import mock

import delivery_coverage as coverage
import durham_aac as aac
import stoarama_pull as pull

FFMPEG = os.environ.get("FFMPEG_BIN") or shutil.which("ffmpeg")
FFPROBE = os.environ.get("FFPROBE_BIN") or shutil.which("ffprobe")


def identity(path):
    data = Path(path).read_bytes()
    return {"size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def av_clip(seconds=1.28, tone=450):
    """One 48 kHz H.264 + AAC-LC clip: 32 frames and 61,440 samples."""
    return ("-f", "lavfi", "-i", f"testsrc2=s=64x64:r=25:d={seconds}",
            "-f", "lavfi", "-i", f"sine=f={tone}:r=48000:d={seconds}")


@unittest.skipUnless(FFMPEG and FFPROBE, "media tools are unavailable")
class AacConcatTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        scratch = tempfile.TemporaryDirectory()
        cls.addClassCleanup(scratch.cleanup)
        cls.root = Path(scratch.name)
        # The native 7.1.5 case needed an explicit clock, so pin 48000 here.
        common = ("-c:v", "libx264", "-g", "32", "-c:a", "aac", "-aac_pns", "0",
                  "-movie_timescale", "48000")
        cls.build("one.mp4", *av_clip(), "-map", "0:v", "-map", "1:a", *common)
        cls.build("two.mp4", *av_clip(tone=990), "-map", "0:v", "-map", "1:a", *common)
        cls.build("silent.mp4", *av_clip(), "-map", "0:v", "-c:v", "libx264", "-g", "32", "-an",
                  "-movie_timescale", "48000")
        cls.build("audio.mp4", *av_clip(), "-map", "1:a", "-c:a", "aac", "-aac_pns", "0", "-vn",
                  "-movie_timescale", "48000")
        cls.build("other.mp4", *av_clip(tone=990), "-map", "0:v", "-map", "1:a", "-vf", "scale=32:32",
                  *common)

    @classmethod
    def build(cls, name, *args):
        subprocess.run([FFMPEG, "-nostdin", "-v", "error", "-y", *args, str(cls.root / name)],
                       check=True, capture_output=True, timeout=60)

    @property
    def tools(self):
        return {"ffmpeg": FFMPEG, "ffprobe": FFPROBE}

    def sources(self, *names):
        return [{"path": self.root / name, **identity(self.root / name)} for name in names]

    def test_two_clips_publish_exact_identity_and_vetted_facts(self):
        output = self.root / "joined.mp4"
        result = aac.concat_aac(self.sources("one.mp4", "two.mp4"), output, self.tools)
        self.assertEqual(identity(output), result["output"])
        self.assertEqual(result["timeline"]["video_track_timescale"], 12800)
        self.assertEqual(result["timeline"]["movie_timescale"], 192000)
        self.assertEqual(result["timeline"]["concat_microseconds"], [1301334, 1301333])
        first, second = result["inputs"]
        for row in (first, second):
            self.assertEqual(row["span"], "488/375")
            video, audio = row["tracks"]["video"], row["tracks"]["audio"]
            self.assertEqual(video["frame_count"], 32)
            self.assertEqual((video["packet_count"], audio["packet_count"]), (32, 61))
            self.assertEqual(audio["sample_count"], 61440)
            self.assertRegex(video["frame_chain_sha256"], "^[0-9a-f]{64}$")
            self.assertRegex(audio["pcm_chain_sha256"], "^[0-9a-f]{64}$")
        self.assertEqual(first["tracks"]["video"]["packet_start"], 0)
        self.assertEqual(first["tracks"]["audio"]["packet_start"], 0)
        self.assertEqual(second["tracks"]["video"]["packet_start"], 32)
        self.assertEqual(second["tracks"]["audio"]["packet_start"], 61)
        self.assertEqual(first["tracks"]["audio"]["rule"], "standalone_exact")
        self.assertEqual(second["tracks"]["audio"]["rule"], "seam_primed")
        self.assertEqual(first["tracks"]["audio"]["seam_primer_packets"], 0)
        self.assertEqual(second["tracks"]["audio"]["seam_primer_packets"], 1)
        # The two tones are distinguishable, so an audio track swapped for the
        # other input cannot satisfy the byte-exact PCM chains.
        self.assertNotEqual(first["tracks"]["audio"]["pcm_chain_sha256"],
                            second["tracks"]["audio"]["pcm_chain_sha256"])
        # No encoded packet changed: the proven chains are the input chains.
        for source, row in zip(self.sources("one.mp4", "two.mp4"), result["inputs"]):
            with ExitStack() as stack:
                item = coverage.Input(stack, Path(source["path"]), source)
                tracks = coverage.read_tracks(FFPROBE, item)
                for kind, track in tracks.items():
                    self.assertEqual(row["tracks"][kind]["packet_chain_sha256"],
                                     coverage.digest_rows(
                                         (packet.pts, packet.dts, packet.duration, packet.content())
                                         for packet in track.packets))
        probe = subprocess.run([FFPROBE, "-v", "error", "-show_entries", "stream=codec_name",
                                "-of", "json", str(output)], capture_output=True, text=True, timeout=30)
        self.assertEqual([stream["codec_name"] for stream in json.loads(probe.stdout)["streams"]],
                         ["h264", "aac"])
        self.assertEqual([path.name for path in self.root.iterdir() if path.name.startswith(".durham-aac-")], [])

    def test_descriptor_inputs_match_paths(self):
        output = self.root / "fds.mp4"
        opened = [os.open(self.root / name, os.O_RDONLY | os.O_NOFOLLOW) for name in ("one.mp4", "two.mp4")]
        try:
            result = aac.concat_aac([{"descriptor": fd, **identity(self.root / name)}
                                     for fd, name in zip(opened, ("one.mp4", "two.mp4"))],
                                    output, self.tools)
            offsets = [os.lseek(fd, 0, os.SEEK_CUR) for fd in opened]
        finally:
            for fd in opened:
                os.close(fd)
        self.assertEqual(offsets, [0, 0])
        self.assertEqual(result["inputs"][1]["tracks"]["audio"]["rule"], "seam_primed")
        self.assertEqual(identity(output), result["output"])

    def test_pinned_directory_publish_uses_the_retained_parent_descriptor(self):
        # The child cannot see the parent's /proc/self/fd entry; only the Linux
        # /proc/<parent>/fd form is traversable from a spawned tool. A pinned
        # descriptor names the directory inode, not the name it was opened
        # under, so renaming it and putting a fresh empty directory at the old
        # name separates the two: a path-based publish would fill the fresh
        # directory while the retained inode stays empty.
        if not sys.platform.startswith("linux"):
            self.skipTest("pinned parent directory descriptors are Linux-only")
        pinned, retained = self.root / "pinned", self.root / "retained"
        pinned.mkdir()
        directory = os.open(pinned, os.O_RDONLY | os.O_DIRECTORY)
        try:
            pinned.rename(retained)
            pinned.mkdir()
            # The inputs stay outside the pinned directory.
            result = aac.concat_aac(self.sources("one.mp4", "two.mp4"), "published.mp4",
                                    self.tools, directory=directory)
            self.assertEqual(list(pinned.iterdir()), [])
        finally:
            os.close(directory)
        output = retained / "published.mp4"
        self.assertEqual(identity(output), result["output"])
        self.assertEqual([path.name for path in retained.iterdir()
                          if path.name.startswith(".durham-aac-")], [])

    def test_bad_input_identity_leaves_no_output(self):
        output = self.root / "bad-identity.mp4"
        sources = self.sources("one.mp4", "two.mp4")
        sources[1]["sha256"] = "0" * 64
        with self.assertRaises(aac.DurhamAacError):
            aac.concat_aac(sources, output, self.tools)
        self.assertFalse(output.exists())

    def test_unsupported_track_layout_leaves_no_output(self):
        for member in ("silent.mp4", "audio.mp4"):
            with self.subTest(member=member):
                output = self.root / f"bad-{member}"
                with self.assertRaises(aac.DurhamAacError):
                    aac.concat_aac(self.sources("one.mp4", member), output, self.tools)
                self.assertFalse(output.exists())

    def test_changed_video_configuration_across_the_run_is_rejected(self):
        output = self.root / "mixed.mp4"
        with self.assertRaises(aac.DurhamAacError):
            aac.concat_aac(self.sources("one.mp4", "other.mp4"), output, self.tools)
        self.assertFalse(output.exists())

    def test_cancelled_or_expired_run_leaves_no_output(self):
        cancel = threading.Event()
        cancel.set()
        for name, kwargs in (("cancel", {"cancel": cancel}),
                             ("deadline", {"deadline": time.monotonic() - 1})):
            with self.subTest(name=name):
                output = self.root / f"stopped-{name}.mp4"
                with self.assertRaises(coverage.ProofStopped):
                    aac.concat_aac(self.sources("one.mp4", "two.mp4"), output, self.tools, **kwargs)
                self.assertFalse(output.exists())

    def test_publish_never_replaces_an_existing_requested_output(self):
        output = self.root / "existing.mp4"
        output.write_bytes(b"keep")
        before = identity(output)
        with self.assertRaises(aac.DurhamAacError):
            aac.concat_aac(self.sources("one.mp4", "two.mp4"), output, self.tools)
        self.assertEqual(identity(output), before)

    def test_failure_after_publish_removes_the_requested_output(self):
        output = self.root / "unproven.mp4"
        real = pull.stitch_proof_digest

        def readback(path, stop):
            # The staged readback is real; the post-publish readback is wrong.
            return real(path, stop) if Path(path).name != output.name else (1, "0" * 64)

        with mock.patch.object(aac.pull, "stitch_proof_digest", side_effect=readback):
            with self.assertRaises(aac.DurhamAacError):
                aac.concat_aac(self.sources("one.mp4", "two.mp4"), output, self.tools)
        self.assertFalse(output.exists())
        self.assertEqual([path.name for path in self.root.iterdir() if path.name.startswith(".durham-aac-")], [])

    def test_directory_sync_failure_after_link_leaves_no_requested_output(self):
        output = self.root / "unsynced.mp4"
        real = pull.fsync_dir

        def failing(path):
            # Only the publish-time parent sync fails, after the link succeeded.
            if Path(path) == self.root and output.exists():
                raise OSError("simulated directory fsync failure")
            real(path)

        with mock.patch.object(aac.pull, "fsync_dir", side_effect=failing):
            with self.assertRaises(OSError):
                aac.concat_aac(self.sources("one.mp4", "two.mp4"), output, self.tools)
        self.assertFalse(output.exists())
        self.assertEqual([path.name for path in self.root.iterdir() if path.name.startswith(".durham-aac-")], [])

    def test_rollback_keeps_a_replacement_at_the_requested_path(self):
        output = self.root / "replaced.mp4"
        keeper = self.root / "keeper.mp4"
        keeper.write_bytes(b"caller file")
        expected = identity(keeper)

        def replace(path):
            # Another writer wins the name after our link, before our failure.
            if Path(path) == self.root:
                os.replace(keeper, output)
            else:
                pull.fsync_dir(path)

        with mock.patch.object(aac.pull, "fsync_dir", side_effect=replace):
            with self.assertRaises(aac.DurhamAacError):
                aac.concat_aac(self.sources("one.mp4", "two.mp4"), output, self.tools)
        self.assertEqual(identity(output), expected)

    def test_shorter_mux_shift_is_rejected_even_when_each_input_is_affine(self):
        output = self.root / "short-shift.mp4"
        real = coverage.prove_ordered
        slack = Fraction(1, 12800) + Fraction(1, 500000)

        def shortened(*args, **kwargs):
            rows = copy.deepcopy(real(*args, **kwargs))
            # Input 2 arrives just over one original video tick plus 1 us
            # early. Each input still maps constantly on its own, so only the
            # absolute cumulative offset can catch the shorter mux shift.
            for kind in ("video", "audio"):
                shift = Fraction(rows[1]["tracks"][kind]["visible_time_shift"])
                rows[1]["tracks"][kind]["visible_time_shift"] = str(shift - slack)
            return rows

        with mock.patch.object(aac.coverage, "prove_ordered", side_effect=shortened):
            with self.assertRaisesRegex(aac.DurhamAacError, "placement"):
                aac.concat_aac(self.sources("one.mp4", "two.mp4"), output, self.tools)
        self.assertFalse(output.exists())
        self.assertEqual([path.name for path in self.root.iterdir() if path.name.startswith(".durham-aac-")], [])

    def test_timing_certifies_cumulative_offsets_within_one_tick_and_1_us(self):
        tracks = [{"video": mock.Mock(time_base=Fraction(1, 12800)),
                   "audio": mock.Mock(time_base=Fraction(1, 48000))}
                  for _ in range(2)]
        ticks = {"video": Fraction(1, 12800), "audio": Fraction(1, 48000)}
        waits = aac.durations([Fraction(488, 375)] * 2)
        assigned = Fraction(waits[0], aac.MICROSECONDS)

        def rows(offsets):
            return [{"tracks": {kind: {"visible_time_shift": str(Fraction(7) + (offsets[kind] if index else 0))}
                                for kind in ("video", "audio")}} for index in (0, 1)]

        aac.timing(rows({"video": assigned, "audio": assigned}), tracks, waits)
        for kind in ("video", "audio"):
            slack = ticks[kind] + Fraction(1, aac.MICROSECONDS)
            with self.subTest(kind=kind, accepted=slack):
                offsets = {other: assigned for other in ticks}
                offsets[kind] = assigned + slack
                aac.timing(rows(offsets), tracks, waits)
            # A shorter mux shift: each input is still affine on its own, so
            # only the absolute cumulative offset can reject it.
            for delta in (-slack - Fraction(1, 500000), slack + Fraction(1, 500000)):
                with self.subTest(kind=kind, rejected=delta):
                    offsets = {other: assigned for other in ticks}
                    offsets[kind] = assigned + delta
                    with self.assertRaisesRegex(aac.DurhamAacError, "placement"):
                        aac.timing(rows(offsets), tracks, waits)

    def test_proof_requires_exact_video_and_both_aac_rules(self):
        rows = [{"ordinal": 1, "verdict": "proven", "tracks": {
            "video": {"frame_count": 32, "packet_count": 32, "extra_output_frames": 0},
            "audio": {"rule": "standalone_exact", "seam_primer_packets": 0}}}]
        aac.verify(rows, 1)
        broken = (
            ("frame", lambda row: row["tracks"]["video"].update(frame_count=31), "frame count"),
            ("extra", lambda row: row["tracks"]["video"].update(extra_output_frames=1), "frame count"),
            ("rule", lambda row: row["tracks"]["audio"].update(rule="seam_primed"), "standalone"),
        )
        for name, change, reason in broken:
            with self.subTest(name=name):
                with self.assertRaisesRegex(aac.DurhamAacError, reason):
                    candidate = json.loads(json.dumps(rows))
                    change(candidate[0])
                    aac.verify(candidate, 1)
        second = json.loads(json.dumps(rows))
        second.append({"ordinal": 2, "verdict": "proven", "tracks": {
            "video": {"frame_count": 1, "packet_count": 1, "extra_output_frames": 0},
            "audio": {"rule": "standalone_exact", "seam_primer_packets": 0}}})
        with self.assertRaisesRegex(aac.DurhamAacError, "seam primed"):
            aac.verify(second, 2)
        for candidate in ([], [dict(rows[0], verdict="not_proven")]):
            with self.assertRaises(aac.DurhamAacError):
                aac.verify(candidate, 1)

    def test_span_and_clock_arithmetic_is_rational(self):
        self.assertEqual(aac.durations([Fraction(488, 375), Fraction(488, 375)]), [1301334, 1301333])
        with ExitStack() as stack:
            track = coverage.read_tracks(FFPROBE, coverage.Input(stack, self.root / "one.mp4"))["audio"]
        self.assertEqual(aac.span(track), Fraction(488, 375))
        self.assertEqual(aac.durations([Fraction(1, 3)]), [333334])
        with self.assertRaises(aac.DurhamAacError):
            aac.durations([Fraction(0)])
        self.assertEqual(aac.clocks({"video": mock.Mock(time_base=Fraction(1, 12800)),
                                     "audio": mock.Mock(time_base=Fraction(1, 48000))}), (12800, 192000))
        with self.assertRaises(aac.DurhamAacError):
            aac.clocks({"video": mock.Mock(time_base=Fraction(1, 2 ** 31)),
                        "audio": mock.Mock(time_base=Fraction(1, 48000))})

    def test_original_video_only_builder_is_untouched(self):
        source = Path(pull.__file__).read_text(encoding="utf-8")
        self.assertIn('"-map", "0:v:0", "-an", "-c:v", "copy"', source)


if __name__ == "__main__":
    unittest.main()
