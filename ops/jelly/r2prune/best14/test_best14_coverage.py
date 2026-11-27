# /usr/bin/python3 -I ops/jelly/r2prune/best14/test_best14_coverage.py -q
# Offline: one synthetic served day, every exclusion the rule names, and the covered rows.
import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('best14_coverage', HERE / 'best14_coverage.py')
bc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bc)

GEN = 'g' * 32
LEDGER_COLS = ['recording_id', 'stream_id', 'local_date', 'delivery_hour', 'local_hour', 'captured', 'status', 'hour_id', 'batch_id',
               'parts', 'served', 'complete', 'in_r2', 'source', 'alternates', 'covered_by', 'sources', 'file_ids', 'object_keys',
               'delivery_paths', 'best14_target', 'generation', 'published']
T0 = 1785000000


def tsv(path, rows):
    Path(path).write_text(''.join('\t'.join(map(str, r)) + '\n' for r in rows))


class Coverage(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = Path(self.tmp.name)
        (self.d / 'data').mkdir()
        # Recording 7, 2026-08-01: hours 1..8 served; hour 9 served by an older generation.
        self.hours = {h: f'b__recording-7__date-2026-08-01__hour-{h:02d}__generation-1' for h in range(1, 10)}
        sources = {3: 'original,clock_assembly'}
        ledger = [LEDGER_COLS] + [[7, 70, '2026-08-01', h, h + 7, 'y', 'collated', self.hours[h], 'b', 1, 1, 'y', 'y', '', 0, '',
                                   sources.get(h, 'original'), str(h), f'joined/b/objects/o{h}.mp4', '', 'complete',
                                   GEN if h < 9 else 'f' * 32, 'y'] for h in range(1, 10)]
        tsv(self.d / 'ledger.tsv', ledger)
        # Each hour h: one output o<h> holding clips 10h+1 and 10h+2; clip spans [T, T+60) back to back.
        outputs, clips, catalog, holds, listing = [], [], [], [], []
        for h in range(1, 10):
            t = T0 + h * 3600
            key = f'joined/b/objects/o{h}.mp4'
            outputs.append([self.hours[h], 'collated', 7, '2026-08-01', h, 100 + h, key, 1000 + h, 1, t, t + 120,
                            f'{10 * h + 1},{10 * h + 2}', 'true', 'true', 'true'])
            holds.append([key, f'{1000 + h}:oe{h}', 'true', 'false', 'false', 'false', 'none'])
            listing.append([key, 1000 + h, f'"oe{h}"', 1])
            for i in (1, 2):
                cid, raw = 10 * h + i, f'managed/acct-47/f/{h}-{i}.mp4'
                start = t + (i - 1) * 60
                clips.append([self.hours[h], cid, 1, 'included', raw, 50 + cid, 's' * 64, f'e{cid}', start, start + 60, '60.0'])
                catalog.append([cid, 70, raw, 50 + cid, f'e{cid}', 's' * 64, 'true', start, start + 60])
                listing.append([raw, 50 + cid, f'"e{cid}"', 1])
        self.outputs, self.clips, self.catalog, self.holds, self.listing = outputs, clips, catalog, holds, listing
        (self.d / 'sole.tsv').write_text('# KE sole copies\n# never remove\nid\tobject_key\tsize_bytes\tsha256\n')
        tsv(self.d / 'flags.tsv', [['recording_id', 'local_date', 'prev_hour'], [7, '2026-08-01', 6]])
        tsv(self.d / 'exceptions.tsv', [['stream', 'prev_clip', 'next_clip']])

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, *extra):
        for name, rows in (('outputs', self.outputs), ('manifest-clips', self.clips), ('catalog-clips', self.catalog),
                           ('output-holds', self.holds)):
            tsv(self.d / 'data' / f'{name}.tsv', rows)
        tsv(self.d / 'data' / 'snapshot.tsv', self.listing)
        with contextlib.redirect_stdout(io.StringIO()):
            bc.main([str(self.d / 'out'), '--data', str(self.d / 'data'), '--ledger', str(self.d / 'ledger.tsv'), '--generation', GEN,
                     '--ke-sole-copy', str(self.d / 'sole.tsv'), '--seam-flags', str(self.d / 'flags.tsv'),
                     '--seam-exceptions', str(self.d / 'exceptions.tsv'), *extra])
        covered = [l.split('\t') for l in (self.d / 'out' / 'coverage.tsv').read_text().splitlines()]
        excluded = {l.split('\t')[0]: l.split('\t')[3] for l in (self.d / 'out' / 'excluded.tsv').read_text().splitlines()[1:]}
        return covered, excluded

    def test_covered_and_every_named_exclusion(self):
        covered, excluded = self.build()
        # hours 1, 2, 5, 8 covered; 3 clock, 4 after clock, 6 and 7 seam-flagged, 9 another generation
        self.assertEqual(covered, [[f'managed/acct-47/f/{h}-{i}.mp4', str(50 + 10 * h + i), f'joined/b/objects/o{h}.mp4', str(1000 + h)]
                                   for h in (1, 2, 5, 8) for i in (1, 2)])
        want = {3: 'clock_assembly_hour', 4: 'after_clock_assembly_hour', 6: 'seam_flagged', 7: 'seam_flagged', 9: 'hour_not_serving'}
        self.assertEqual(excluded, {f'managed/acct-47/f/{h}-{i}.mp4': w for h, w in want.items() for i in (1, 2)})
        summary = json.loads((self.d / 'out' / 'summary.json').read_text())
        self.assertEqual((summary['covered_raws'], summary['covering_outputs']), (8, 4))

    def test_each_cover_condition_excludes(self):
        cases = {
            'catalog_differs_from_stitched_bytes': lambda: self.catalog[0].__setitem__(5, 't' * 64),
            'catalog_purged': lambda: self.catalog[0].__setitem__(6, 'false'),
            'raw_listing_differs': lambda: self.listing[1].__setitem__(2, '"x"'),
            'hour_not_collated': lambda: self.outputs[0].__setitem__(1, 'gap_only'),
            'clip_not_in_output_sources': lambda: self.outputs[0].__setitem__(11, '12'),
            'output_not_verified': lambda: self.outputs[0].__setitem__(12, 'false'),
            'no_content_seconds': lambda: self.clips[0].__setitem__(10, ''),
            'clip_span_outside_output': lambda: self.catalog[0].__setitem__(7, T0 + 3600 - 1),
            'output_not_live_at_catalog_size': lambda: self.listing[0].__setitem__(1, 999),
            'output_retirement_named': lambda: self.holds[0].__setitem__(3, 'true'),
            'output_restore_unverified': lambda: self.holds[0].__setitem__(6, 'unverified'),
            'output_not_keep_held': lambda: self.holds[0].__setitem__(2, 'false'),  # a delivery hold alone is not enough
            'output_ke_sole_copy': lambda: (self.d / 'sole.tsv').write_text('id\tobject_key\tsize_bytes\tsha256\n1\tjoined/b/objects/o1.mp4\t1\tx\n'),
            'disposition_quarantined': lambda: self.clips[0].__setitem__(3, 'quarantined'),
            '2_outputs_for_part': lambda: self.outputs.append(list(self.outputs[0][:6]) + ['joined/b/objects/dup.mp4'] + self.outputs[0][7:]),
        }
        for why, break_it in cases.items():
            with self.subTest(why):
                self.tearDown()
                self.setUp()
                break_it()
                _, excluded = self.build()
                self.assertEqual(excluded.get('managed/acct-47/f/1-1.mp4'), why)

    def test_an_exclusion_wins_over_another_hours_coverage(self):
        # clip 31 (clock-assembly hour 3) and clip 71 (seam-flagged hour 7) are also in hour 1's manifest and output
        for cid, raw in ((31, 'managed/acct-47/f/3-1.mp4'), (71, 'managed/acct-47/f/7-1.mp4')):
            self.clips.append([self.hours[1], cid, 1, 'included', raw, 50 + cid, 's' * 64, f'e{cid}', T0 + 3600, T0 + 3660, '60.0'])
            self.outputs[0][11] += f',{cid}'
        for c in self.catalog:
            if c[0] in (31, 71):
                c[7:9] = [T0 + 3600, T0 + 3660]
        covered, excluded = self.build()
        self.assertNotIn('managed/acct-47/f/3-1.mp4', [c[0] for c in covered])
        self.assertEqual(excluded['managed/acct-47/f/3-1.mp4'], 'clock_assembly_hour')
        self.assertEqual(excluded['managed/acct-47/f/7-1.mp4'], 'seam_flagged')

    def test_owner_keep_holds_without_a_delivery_hold(self):
        self.holds[0][1:3] = ['', 'true']
        covered, _ = self.build()
        self.assertIn('managed/acct-47/f/1-1.mp4', [c[0] for c in covered])

    def test_operator_hour_exception_and_within(self):
        (self.d / 'within.txt').write_text('managed/acct-47/f/1-1.mp4\nmanaged/acct-47/f/2-1.mp4\n')
        covered, excluded = self.build('--exclude-hour', '7:2026-08-01:2', '--within', str(self.d / 'within.txt'))
        self.assertEqual([c[0] for c in covered], ['managed/acct-47/f/1-1.mp4'])
        self.assertEqual(excluded['managed/acct-47/f/2-1.mp4'], 'excluded_by_operator')
        self.assertEqual(excluded['managed/acct-47/f/1-2.mp4'], 'outside_approved_list')

    def test_seam_exception_clips_exclude_their_hours(self):
        tsv(self.d / 'exceptions.tsv', [['stream', 'prev_clip', 'next_clip'], [7, 22, 51]])
        covered, excluded = self.build()
        self.assertEqual({c[0].split('/')[-1][0] for c in covered}, {'1', '8'})
        self.assertEqual(excluded['managed/acct-47/f/2-1.mp4'], 'seam_exception')


if __name__ == '__main__':
    unittest.main()
