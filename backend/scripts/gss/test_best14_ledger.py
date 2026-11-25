import csv
import dataclasses
import datetime
import gzip
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import best14_ledger as L  # noqa: E402

RID, STREAM = 401, '14478'
DAYS = [(datetime.date(2026, 8, 19) + datetime.timedelta(days=i)).isoformat() for i in range(14)]
CODES = 'RRRRRRRRRR..'  # hours 11 and 12 have no capture
ETAG = '0' * 32


def best14():
    return {RID: (STREAM, [(d, CODES) for d in DAYS])}


def key(batch, oid):
    return f'joined/{batch}/objects/{oid:064x}.mp4'


def path(date, dh, part=None):
    return f'{RID}_Cusco/{date}/{RID}_Cusco_hour_{dh:02d}' + (f'_part_{part:02d}' if part else '') + '.mp4'


class World:
    """One recording, 14 days, two-part hours 1-10, registered in goodplus, listed by the live
    generation and fully in R2."""

    def __init__(self):
        self.records, self.listing, self.files, self.assemblies = {}, {}, {}, []
        row = 0
        for date in DAYS:
            for dh in range(1, 11):
                row += 1
                self.add(row, date, dh, 'goodplus-20260821-generation-1', parts=2)
            for dh in (11, 12):
                row += 1
                self.records[(RID, date, dh)] = [L.Record(row, f'h-{row}', 'goodplus-20260821-generation-1', 'gap_only', '2026-09-01', ())]
        self.next_row = row + 1

    def add(self, row, date, dh, batch, parts, source=L.Source.ORIGINAL, registered='2026-09-01', in_registry=True):
        ps = tuple(L.Part(row * 100 + p, p, parts, key(batch, row * 100 + p), 1000 + p, f'{row * 100 + p:064x}',
                          f'2026-08-19 0{p}:00:00+00', f'2026-08-19 0{p}:30:00+00', f'nas/{row}/{p}.mp4', source)
                   for p in range(1, parts + 1))
        rec = L.Record(row, f'h-{row}', batch, 'collated', registered, ps)
        self.records.setdefault((RID, date, dh), []).append(rec)
        for p in ps:
            self.listing[p.key] = (p.size_bytes, ETAG)
            if in_registry:
                self.files[str(p.output_id)] = {'path': path(date, dh, p.part if parts > 1 else None)}
        return rec

    def inputs(self):
        return L.Inputs(best14(), self.records, list(self.assemblies), self.listing, {'files': self.files}, {})

    def select(self):
        return L.select(self.inputs())

    def hour(self, hours, date, dh):
        return next(h for h in hours if (h.local_date, h.delivery_hour) == (date, dh))

    def assembly(self, rec_or_ids, inputs=None, sha='ab', source=L.Source.CLOCK_ASSEMBLY, live=True, path=None):
        ids = tuple(rec_or_ids.parts[i - 1].output_id for i in inputs) if inputs else tuple(rec_or_ids)
        a = L.Assembly('sha256:' + sha * 32, f'joined/clock-prestitch-20261125/objects/{sha * 32}.mp4', 777, ETAG, sha * 32,
                       ids, source, path)
        if live:
            self.listing[a.key] = (a.size_bytes, ETAG)
            self.assemblies.append(a)
        return a


def summary(hours):
    return L.summary_line(hours, L.stream_rows(hours, L.day_rows(hours)), 0)


class SelectionTests(unittest.TestCase):
    def test_complete_world(self):
        hours = World().select()
        self.assertEqual(len(hours), 14 * 12)
        days = L.day_rows(hours)
        streams = L.stream_rows(hours, days)
        self.assertEqual({d['day_stitched'] for d in days}, {'R'})
        self.assertEqual((streams[0]['days_complete'], streams[0]['available'], streams[0]['captured_hours']), (14, 'y', 140))
        self.assertTrue(summary(hours).startswith('streams_available=1/1 days_complete=14/14 hours_complete=140/140 original=280 '))

    def test_missing_key_and_unregistered_hour(self):
        w = World()
        first = w.records[(RID, DAYS[0], 1)][0]
        del w.listing[first.parts[1].key]
        del w.records[(RID, DAYS[1], 3)]  # captured per hours_08_19, never stitched
        hours = w.select()
        h = w.hour(hours, DAYS[0], 1)
        self.assertEqual((h.complete, h.in_r2, h.ok), (True, False, False))
        h = w.hour(hours, DAYS[1], 3)
        self.assertEqual((h.captured, h.record, h.served), (True, None, ()))
        days = L.day_rows(hours)
        self.assertEqual([d['day_stitched'] for d in days[:3]], ['S', 'S', 'R'])
        s = L.stream_rows(hours, days)[0]
        self.assertEqual((s['days_complete'], s['available'], s['hours_complete'], s['hours_with_record']), (12, 'n', 138, 139))

    def test_incomplete_record(self):
        w = World()
        rec = w.records[(RID, DAYS[0], 1)][0]
        w.records[(RID, DAYS[0], 1)] = [dataclasses.replace(rec, parts=rec.parts[:1])]
        h = w.hour(w.select(), DAYS[0], 1)
        self.assertEqual((h.complete, h.in_r2), (False, True))

    def test_size_mismatch_fails(self):
        w = World()
        k = w.records[(RID, DAYS[0], 1)][0].parts[0].key
        w.listing[k] = (w.listing[k][0] + 1, ETAG)
        with self.assertRaisesRegex(L.LedgerError, 'R2 holds'):
            w.select()

    def test_sources(self):
        self.assertIs(L.part_source('goodplus-20260821-generation-1', 'joined/g/objects/a.mp4', False, False), L.Source.ORIGINAL)
        self.assertIs(L.part_source('nightly-collation-v2', 'joined/n/objects/a.mp4', False, True), L.Source.RESTORE)
        self.assertIs(L.part_source('nightly-collation-v2', 'joined/n/objects/a.mp4', True, True), L.Source.NAS_UPLOAD)
        self.assertIs(L.part_source('recovery-best14-20261125', 'joined/r/objects/a.mp4', False, True), L.Source.NEW_STITCH)
        with self.assertRaises(L.LedgerError):
            L.part_source('nightly-collation-v2', 'joined/clock-prestitch-20261125/objects/a.mp4', False, False)

    def test_new_stitch_fills_an_unstitched_hour(self):
        w = World()
        del w.records[(RID, DAYS[2], 4)]
        w.add(w.next_row, DAYS[2], 4, 'recovery-best14-20261125', 3, L.Source.NEW_STITCH, in_registry=False)
        h = w.hour(w.select(), DAYS[2], 4)
        self.assertEqual((h.ok, {s.source for s in h.served}), (True, {L.Source.NEW_STITCH}))
        self.assertEqual([s.path for s in h.served], [f'nas/{w.next_row}/{p}.mp4' for p in (1, 2, 3)])
        self.assertEqual({s.etag for s in h.served}, {ETAG})

    def test_one_record_serves_an_hour(self):
        w = World()
        old = w.records[(RID, DAYS[0], 1)][0]
        for p in old.parts:
            del w.listing[p.key]  # deleted from R2: only the re-stitch is complete there
        new = w.add(w.next_row, DAYS[0], 1, 'recovery-best14-20261125', 1, L.Source.NEW_STITCH, '2026-11-25', in_registry=False)
        h = w.hour(w.select(), DAYS[0], 1)
        self.assertEqual((h.record, h.alternates), (new, 0))
        for p in old.parts:
            w.listing[p.key] = (p.size_bytes, ETAG)  # the NAS upload lands too: the original lineage serves
        h = w.hour(w.select(), DAYS[0], 1)
        self.assertEqual((h.record, h.alternates), (old, 1))
        self.assertEqual({s.file_id for s in h.served}, {str(p.output_id) for p in old.parts})


class AssemblyTests(unittest.TestCase):
    def setUp(self):
        self.w = World()
        del self.w.records[(RID, DAYS[3], 5)]
        self.rec = self.w.add(self.w.next_row, DAYS[3], 5, 'nightly-collation-v2', 4)

    def test_assembly_replaces_its_parts(self):
        a = self.w.assembly(self.rec, [2, 3])
        h = self.w.hour(self.w.select(), DAYS[3], 5)
        ids = [p.output_id for p in self.rec.parts]
        self.assertEqual([s.file_id for s in h.served], [str(ids[0]), a.file_id, str(ids[3])])
        self.assertEqual((h.served[1].path, h.served[1].source, h.ok), (path(DAYS[3], 5, 2), L.Source.CLOCK_ASSEMBLY, True))

    def test_live_generation_assembly_keeps_its_path_and_source(self):
        a = self.w.assembly(self.rec, [1, 2, 3, 4], source=L.Source.V85_ASSEMBLY, path='live/name.mp4')
        h = self.w.hour(self.w.select(), DAYS[3], 5)
        self.assertEqual([(s.file_id, s.path, s.source) for s in h.served], [(a.file_id, 'live/name.mp4', L.Source.V85_ASSEMBLY)])
        self.assertIn('v85_assembly=1', summary(self.w.select()))

    def test_whole_hour_assembly_takes_the_hour_name(self):
        self.w.assembly(self.rec, [1, 2, 3, 4])
        h = self.w.hour(self.w.select(), DAYS[3], 5)
        self.assertEqual([s.path for s in h.served], [path(DAYS[3], 5)])

    def test_assembly_serves_while_a_replaced_part_is_gone(self):
        self.w.assembly(self.rec, [1, 2])
        del self.w.listing[self.rec.parts[0].key]
        self.assertTrue(self.w.hour(self.w.select(), DAYS[3], 5).ok)

    def test_assembly_across_an_hour_boundary(self):
        nxt = self.w.records[(RID, DAYS[3], 6)][0]
        a = self.w.assembly([self.rec.parts[2].output_id, self.rec.parts[3].output_id, nxt.parts[0].output_id],
                            source=L.Source.V85_ASSEMBLY, path='cross.mp4')
        hours = self.w.select()
        h5, h6 = self.w.hour(hours, DAYS[3], 5), self.w.hour(hours, DAYS[3], 6)
        self.assertEqual([s.file_id for s in h5.served][-1], a.file_id)
        self.assertEqual(([s.file_id for s in h6.served], h6.covered_by), ([str(nxt.parts[1].output_id)], (a.file_id,)))
        self.assertTrue(h5.ok and h6.ok)

    def test_parts_taken_twice_fail(self):
        self.w.assembly(self.rec, [1, 2], 'aa')
        self.w.assembly(self.rec, [2, 3], 'bb')
        with self.assertRaisesRegex(L.LedgerError, 'input of two assemblies'):
            self.w.select()

    def test_non_consecutive_parts_fail(self):
        self.w.assembly(self.rec, [1, 3])
        with self.assertRaisesRegex(L.LedgerError, 'not consecutive'):
            self.w.select()

    def test_run_must_cross_at_the_hour_edge(self):
        nxt = self.w.records[(RID, DAYS[3], 6)][0]
        self.w.assembly([self.rec.parts[1].output_id, nxt.parts[0].output_id])
        with self.assertRaisesRegex(L.LedgerError, 'not consecutive'):
            self.w.select()

    def test_inputs_outside_the_window_fail(self):
        self.w.records[(RID, '2026-01-01', 1)] = [L.Record(9999, 'h-out', 'g', 'collated', 'r', (dataclasses.replace(self.rec.parts[0], output_id=1),))]
        self.w.assembly([self.rec.parts[0].output_id, 1])
        with self.assertRaisesRegex(L.LedgerError, 'reach outside the best-14 hours'):
            self.w.select()

    def test_assembly_into_an_alternate_record_fails(self):
        alt = self.w.add(self.w.next_row + 1, DAYS[3], 6, 'recovery-best14-20261125', 2, L.Source.NEW_STITCH, in_registry=False)
        for p in alt.parts:
            del self.w.listing[p.key]  # the original record serves hour 6
        self.w.assembly([self.rec.parts[3].output_id, alt.parts[0].output_id], path='a.mp4')
        with self.assertRaisesRegex(L.LedgerError, 'record that serves no hour'):
            self.w.select()

    def test_clock_join_assembly_across_hours_fails(self):
        nxt = self.w.records[(RID, DAYS[3], 6)][0]
        self.w.assembly([self.rec.parts[3].output_id, nxt.parts[0].output_id])
        with self.assertRaisesRegex(L.LedgerError, 'crosses an hour'):
            self.w.select()

    def test_registry_assemblies_need_a_matching_hold(self):
        a = self.w.assembly(self.rec, [1, 2], live=False)
        registry = {'files': {a.file_id: {'key': a.key, 'size_bytes': a.size_bytes, 'etag': f'"{ETAG}"', 'sha256': a.sha256, 'path': 'p.mp4'}}}
        store = type('S', (), {'load': lambda self, p: {'repairs': [{'sha256': a.sha256, 'kind': 'model79', 'input_ids': list(a.input_ids)}]}})()
        listing = {a.key: (a.size_bytes, ETAG)}
        self.assertEqual(L.registry_assemblies(registry, store, listing, {}), ([], 1))
        live, unheld = L.registry_assemblies(registry, store, listing, {a.key: (ETAG, a.size_bytes)})
        self.assertEqual(([(x.source, x.input_ids, x.path) for x in live], unheld), ([(L.Source.V79_ASSEMBLY, a.input_ids, 'p.mp4')], 0))
        with self.assertRaisesRegex(L.LedgerError, 'R2 holds'):
            L.registry_assemblies(registry, store, {a.key: (a.size_bytes, '1' * 32)}, {})

    def test_reads_clock_join_proofs(self):
        a = self.w.assembly(self.rec, [2, 3], live=False)
        with tempfile.TemporaryDirectory() as d:
            proofs = os.path.join(d, 'joined', 'clock-prestitch-20261125', 'proofs')
            os.makedirs(proofs)
            with open(os.path.join(proofs, a.sha256 + '.json'), 'w') as f:
                json.dump({'hour_id': self.rec.hour_id, 'inputs': [{'output_id': i} for i in a.input_ids],
                           'output': {'key': a.key, 'sha256': a.sha256, 'size_bytes': a.size_bytes}}, f)
            self.assertEqual(L.read_proofs(d, {}), [])
            self.assertEqual(L.read_proofs(d, {a.key: (a.size_bytes, ETAG)}), [a])


class RuleTests(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.hours = self.w.select()

    def test_rules_pass(self):
        L.check_rules(self.hours)

    def test_record_serving_two_hours_fails(self):
        a, b = self.hours[0], self.hours[1]
        hours = [a, dataclasses.replace(b, record=a.record, served=a.served)] + self.hours[2:]
        with self.assertRaisesRegex(L.LedgerError, 'serves two hours'):
            L.check_rules(hours)

    def test_file_from_another_set_fails(self):
        a, b = self.hours[0], self.hours[1]
        hours = [dataclasses.replace(a, served=a.served + b.served[:1])] + self.hours[1:]
        with self.assertRaisesRegex(L.LedgerError, 'not from the serving record'):
            L.check_rules(hours)

    def test_assembly_duplicating_a_part_fails(self):
        h = self.hours[0]
        asm = L.Served('sha256:' + 'cd' * 32, 'joined/clock-prestitch-x/objects/cd.mp4', 1, ETAG, 'cd' * 32, 'p.mp4',
                       L.Source.CLOCK_ASSEMBLY, tuple(s.file_id for s in h.served))
        hours = [dataclasses.replace(h, served=(asm,) + h.served)] + self.hours[1:]
        with self.assertRaisesRegex(L.LedgerError, 'duplicates its parts'):
            L.check_rules(hours)

    def test_part_served_by_no_file_fails(self):
        h = self.hours[0]
        hours = [dataclasses.replace(h, served=h.served[:1])] + self.hours[1:]
        with self.assertRaisesRegex(L.LedgerError, 'served by no file'):
            L.check_rules(hours)

    def test_duplicate_delivery_path_fails(self):
        a = self.w.records[(RID, DAYS[0], 1)][0].parts[0]
        b = self.w.records[(RID, DAYS[0], 2)][0].parts[0]
        self.w.files[str(b.output_id)] = self.w.files[str(a.output_id)]
        with self.assertRaisesRegex(L.LedgerError, 'delivery paths are served twice'):
            self.w.select()


def write_registry(d, objects, files):
    """A live generation registry with the given asset objects, mirrored under d/assets."""
    assets = {}
    for p, o in objects.items():
        body = json.dumps(o).encode()
        sha = hashlib.sha256(body).hexdigest()
        k = f'datasets/x/assets/{sha}/{p.rsplit("/", 1)[1]}'
        os.makedirs(os.path.join(d, 'assets', os.path.dirname(k)), exist_ok=True)
        with open(os.path.join(d, 'assets', k), 'wb') as f:
            f.write(body)
        assets[p] = {'key': k, 'sha256': sha, 'size_bytes': len(body)}
    with open(os.path.join(d, 'registry.json.gz'), 'wb') as f:
        f.write(gzip.compress(json.dumps({'recording_ids': [RID], 'assets': assets, 'files': files}).encode()))


class CliTests(unittest.TestCase):
    def test_extract_and_build(self):
        w = World()
        with tempfile.TemporaryDirectory() as d:
            p = lambda n: os.path.join(d, n)
            with open(p('best14.tsv'), 'w') as f:
                f.write('\t'.join(L.BEST14_COLUMNS + ('day_stitched',)) + '\n')
                f.writelines(f'{RID}\t{STREAM}\t{day}\t{CODES}\tR\n' for day in DAYS)
            with open(p('extract.tsv'), 'w', newline='') as f:
                out = csv.writer(f, delimiter='\t', lineterminator='\n')
                out.writerow(L.EXTRACT_COLUMNS)
                for (rid, date, dh), recs in w.records.items():
                    for r in recs:
                        head = [rid, date, dh, r.row_id, r.hour_id, r.batch_id, r.status, r.registered_at, 5, 5]
                        if not r.parts:
                            out.writerow(head + [''] * 9 + ['f', 'f'])
                        for q in r.parts:
                            out.writerow(head + [q.output_id, q.part, q.parts, q.key, q.nas_path, q.size_bytes, q.sha256,
                                                 q.start_at, q.end_at, 'f', 'f'])
            with open(p('listing.txt'), 'w') as f:
                f.writelines(f'{k}\t{n}\t"{e}"\n' for k, (n, e) in w.listing.items())
            with open(p('holds.tsv'), 'w') as f:
                f.write('object_key\tetag\tsize_bytes\n')
            write_registry(d, {'/public/growth.json': {'repairs': []},
                               f'/public/streams/{RID}.json': {'days': [{'date': '2026-07-01'}]}}, w.files)
            tool = [sys.executable, '-I', L.__file__]
            inputs = ['--best14', p('best14.tsv'), '--registry', p('registry.json.gz'), '--assets', p('assets')]
            sql = subprocess.run(tool + ['extract-sql'] + inputs, check=True, capture_output=True, text=True).stdout
            self.assertIn(f"({RID},date '{DAYS[0]}')", sql)
            self.assertIn(f"({RID},date '2026-07-01')", sql)  # the live generation's own days
            self.assertIn('delivery_keep_holds', subprocess.run(tool + ['holds-sql'], check=True, capture_output=True, text=True).stdout)
            build = tool + ['build'] + inputs + ['--extract', p('extract.tsv'), '--holds', p('holds.tsv'), '--listing', p('listing.txt')]
            r = subprocess.run(build + ['--out-dir', p('out')], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertTrue(r.stdout.startswith('streams_available=1/1 days_complete=14/14 hours_complete=140/140'))
            rows = L.read_tsv(p('out/best14-ledger.tsv'), L.LEDGER_COLUMNS)
            self.assertEqual(len(rows), 14 * 12)
            self.assertEqual(rows[0]['delivery_paths'].split(','), [path(DAYS[0], 1, 1), path(DAYS[0], 1, 2)])
            with open(p('listing.txt'), 'a') as f:
                f.write('raw/x.mp4 1\n')
            r = subprocess.run(build + ['--out-dir', p('out2')], capture_output=True, text=True)
            self.assertNotEqual(r.returncode, 0)
            self.assertIn('not a key/size/ETag listing line', r.stderr)


if __name__ == '__main__':
    unittest.main()
