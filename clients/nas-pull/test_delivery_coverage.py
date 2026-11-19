import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

FFMPEG = shutil.which("ffmpeg")
FFPROBE = shutil.which("ffprobe")
VERIFIER = Path(__file__).with_name("delivery_coverage.py")


def sha(data):
    return hashlib.sha256(data).hexdigest()


@unittest.skipUnless(FFMPEG and FFPROBE, "media tools are unavailable")
class DeliveryCoverageTest(unittest.TestCase):
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

    def verify(self, delivered, *sources, manifest_digest=None):
        def identity(name):
            path = self.root / (name + ".mp4")
            return {"object_key": f"{name}.mp4", "size_bytes": path.stat().st_size, "sha256": sha(path.read_bytes())}

        output = dict(identity(delivered), part=1, parts=1, source_clip_ids=[110, 120],
                      nas_relative_path="delivery/part.mp4")
        clips = [
            {"disposition": "included", "part": 1,
             "clip": dict(identity(name), clip_id=clip_id, recording_id=9, bucket="raw", etag="fixture-etag",
                          start_utc=f"2026-11-19T00:00:0{second}Z", end_utc=f"2026-11-19T00:00:0{second + 2}Z")}
            for name, clip_id, second in (("one", 110, 0), ("two", 120, 2))
        ]
        manifest = json.dumps(dict(schema_version=1, policy_version="collation-v2", generation=2, status="collated",
                                   recording_id=9, batch_id="fixture", hour_id="fixture-hour",
                                   outputs=[output], clips=clips)).encode()
        path = self.root / "manifest.json"
        path.write_bytes(manifest)
        return subprocess.run([sys.executable, str(VERIFIER), "--manifest", str(path), "--manifest-sha256",
                               manifest_digest or sha(manifest), "--part", "1", f"{delivered}.mp4",
                               *(f"{source}.mp4" for source in sources)], cwd=self.root,
                              capture_output=True, text=True, timeout=30)

    def test_proves_visible_video_and_integer_aac_through_seam(self):
        result = self.verify("delivery", "one", "two")
        self.assertEqual(result.returncode, 0, result.stderr)
        evidence = json.loads(result.stdout)
        first, second = evidence["sources"]
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

    def test_rejects_unpinned_manifest(self):
        result = self.verify("delivery", "one", "two", manifest_digest="0" * 64)
        self.assertEqual(result.returncode, 1)
        self.assertIn("manifest differs from the pinned SHA-256", result.stderr)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
