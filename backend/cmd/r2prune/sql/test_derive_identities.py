"""Synthetic inputs only; no catalog, NAS or media access."""
import datetime
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("derive_identities.py")
REPAIR = "experimental/stitching-confident-repairs/20261009/335/"


def identity(kind, key, stream, start, end, size, folder=""):
    return "\t".join([kind, key, str(stream), str(start), str(end), str(size), "f", "0", folder, "1"]) + "\n"


class DeriveIdentitiesTests(unittest.TestCase):
    def run_script(self, objects, identities, derived):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / "snapshot.tsv").write_text("".join(f"{k}\t{v}\te\t0\n" for k, v in objects.items()))
        (root / "identities.tsv").write_text("".join(identities))
        (root / "derived-db.tsv").write_text("".join(derived))
        (root / "best14-sources.tsv").write_text("")
        run = subprocess.run([sys.executable, str(SCRIPT), "snapshot.tsv", "identities.tsv", "derived-db.tsv", "best14-sources.tsv"],
                             cwd=root, capture_output=True, text=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        return (root / "derived-identities.tsv").read_text().splitlines()

    def recording(self, recording_id, stream, timezone="UTC"):
        return f"rec\t{recording_id}\t\t{stream}\t0\t0\t0\tfalse\tfalse\t0\t\t{timezone}\n"

    def test_repair_hour_becomes_the_collated_hour_in_the_recording_timezone(self):
        key = REPAIR + "a/335_Plac_2026_July_W4_Thursday_hour_05_parts_01_03_v799.mp4"
        rows = self.run_script({key: 10}, [], [self.recording("335", 5)])
        start = int(datetime.datetime(2026, 7, 23, 12, tzinfo=datetime.timezone.utc).timestamp())
        self.assertEqual(rows, ["\t".join(["col", key, "5", str(start), str(start + 3600), "10", "f", "0", "", "repair_hour_from_name"])])

    def test_unambiguous_hour_is_required(self):
        for name in ["335_Plac_2026_July_W4_Thursday_hour_30_parts_01_03_v799.mp4",
                     "335_Plac_2026_July_W4_Thursday_hour_05_parts_01.mp4",
                     "335_Plac_2026_July_W4_Thursday_parts_01_03_v799.mp4",
                     "335_Plac_2026_Someday_W4_Thursday_hour_05_parts_01_03_v799.mp4"]:
            key = REPAIR + "a/" + name
            self.assertEqual(self.run_script({key: 10}, [], [self.recording("335", 5)]), [], name)

    def test_recording_without_a_stream_or_timezone_stays_uncatalogued(self):
        key = REPAIR + "a/335_Plac_2026_July_W4_Thursday_hour_05_parts_01_03_v799.mp4"
        self.assertEqual(self.run_script({key: 10}, [], []), [])
        self.assertEqual(self.run_script({key: 10}, [], [self.recording("335", 5, timezone="")]), [])
        self.assertEqual(self.run_script({key: 10}, [], [self.recording("999", 5)]), [])

    def test_repaired_interval_comes_from_the_named_hour_not_the_listing(self):
        later = REPAIR + "a/335_Plac_2026_July_W4_Thursday_hour_05_parts_01_03_v799.mp4"
        earlier = REPAIR + "a/335_Plac_2026_July_W1_Thursday_hour_05_parts_01_03_v799.mp4"
        rows = self.run_script({later: 10, earlier: 11}, [], [self.recording("335", 5)])
        starts = [r.split("\t")[3] for r in rows]
        self.assertEqual(starts, [str(int(datetime.datetime(2026, 7, 23, 12, tzinfo=datetime.timezone.utc).timestamp())),
                                  str(int(datetime.datetime(2026, 7, 2, 12, tzinfo=datetime.timezone.utc).timestamp()))])

    def test_page_duplicate_still_wins_over_the_hour_identity(self):
        sha = "c" * 64
        page_key = "experimental/stitching-confident-repairs/20261009/model79-review/" + sha + ".mp4"
        key = "experimental/stitching-confident-repairs/20261009/model79-union/" + sha + ".mp4"
        page = f"page\t{page_key}\tv\t335\t20\t0\t0\tfalse\tfalse\t0\t{sha}|335_Plac.mp4\n"
        rows = self.run_script({key: 20, page_key: 20}, [], [self.recording("335", 5), page])
        self.assertEqual(rows, [f"dup\t{key}\t5\t0\t0\t20\tf\t0\t\tpage_duplicate:{page_key}"])


if __name__ == "__main__":
    unittest.main()
