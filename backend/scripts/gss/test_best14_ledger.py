import csv
import dataclasses
import datetime
import gzip
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import best14_ledger as L  # noqa: E402
import build_generation  # noqa: E402

RID, STREAM = 401, '14478'
DAYS = [(datetime.date(2026, 8, 19) + datetime.timedelta(days=i)).isoformat() for i in range(14)]
CODES = 'RRRRRRRRRR..'  # hours 11 and 12 have no capture


def best14():
    return {RID: (STREAM, [(d, CODES) for d in DAYS])}


def key(batch, oid):
    return f'joined/{batch}/objects/{oid:064x}.mp4'


def path(date, dh, part=None):
    return f'{RID}_Cusco/{date}/{RID}_Cusco_hour_{dh:02d}' + (f'_part_{part:02d}' if part else '') + '.mp4'


class World:
    """One recording, 14 days, two-part hours 1-10, registered in goodplus and fully in R2."""

    def __init__(self):
        self.records, self.listing, self.registry = {}, {}, {}
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
        ps = tuple(L.Part(row * 100 + p, p, parts, key(batch, row * 100 + p), 1000 + p, f'nas/{row}/{p}.mp4', source)
                   for p in range(1, parts + 1))
        rec = L.Record(row, f'h-{row}', batch, 'collated', registered, ps)
        self.records.setdefault((RID, date, dh), []).append(rec)
        for p in ps:
            self.listing[p.key] = p.size_bytes
            if in_registry:
                self.registry[str(p.output_id)] = path(date, dh, p.part if parts > 1 else None)
        return rec

    def select(self, assemblies=()):
        return L.select(best14(), self.records, list(assemblies), self.listing, self.registry)

    def hour(self, hours, date, dh):
        return next(h for h in hours if (h.local_date, h.delivery_hour) == (date, dh))


def assembly(rec, inputs, sha='ab'):
    return L.Assembly(sha * 32, f'joined/clock-prestitch-20261125/objects/{sha * 32}.mp4', 777, rec.hour_id,
                   tuple(rec.parts[i - 1].output_id for i in inputs))


class SelectionTests(unittest.TestCase):
    def test_complete_world(self):
        hours = World().select()
        self.assertEqual(len(hours), 14 * 12)
        days = L.day_rows(hours)
        streams = L.stream_rows(hours, days)
        self.assertEqual({d['day_stitched'] for d in days}, {'R'})
        self.assertEqual(streams[0]['days_complete'], 14)
        self.assertEqual(streams[0]['available'], 'y')
        self.assertEqual(streams[0]['captured_hours'], 140)
        self.assertEqual(len(L.builder_files(hours)), 14 * 10 * 2)
        self.assertTrue(L.summary_line(hours, streams).startswith('streams_available=1/1 days_complete=14/14 hours_complete=140/140 original=280 '))

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
        w.listing[w.records[(RID, DAYS[0], 1)][0].parts[0].key] += 1
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
        hours = w.select()
        h = w.hour(hours, DAYS[2], 4)
        self.assertEqual((h.ok, h.in_registry, {s.source for s in h.served}), (True, False, {L.Source.NEW_STITCH}))
        self.assertIn('unpublishable_hours=1', L.summary_line(hours, L.stream_rows(hours, L.day_rows(hours))))
        self.assertFalse({s.file_id for s in h.served} & set(L.builder_files(hours)))

    def test_one_record_serves_an_hour(self):
        w = World()
        old = w.records[(RID, DAYS[0], 1)][0]
        for p in old.parts:
            del w.listing[p.key]  # deleted from R2: only the re-stitch is complete there
        new = w.add(w.next_row, DAYS[0], 1, 'recovery-best14-20261125', 1, L.Source.NEW_STITCH, '2026-11-25', in_registry=False)
        h = w.hour(w.select(), DAYS[0], 1)
        self.assertEqual((h.record, h.alternates), (new, 0))
        for p in old.parts:
            w.listing[p.key] = p.size_bytes  # the NAS upload lands too: the original lineage serves
        h = w.hour(w.select(), DAYS[0], 1)
        self.assertEqual((h.record, h.alternates), (old, 1))
        self.assertEqual({s.file_id for s in h.served}, {str(p.output_id) for p in old.parts})


class AssemblyTests(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.rec = self.w.add(self.w.next_row, DAYS[3], 5, 'nightly-collation-v2', 4)
        del self.w.records[(RID, DAYS[3], 5)][0]

    def live(self, a):
        self.w.listing[a.key] = a.size_bytes
        return a

    def test_assembly_replaces_its_parts(self):
        a = self.live(assembly(self.rec, [2, 3]))
        h = self.w.hour(self.w.select([a]), DAYS[3], 5)
        ids = [p.output_id for p in self.rec.parts]
        self.assertEqual([s.file_id for s in h.served], ['sha256:' + a.sha256, str(ids[0]), str(ids[3])])
        self.assertEqual(h.served[0].path, path(DAYS[3], 5, 2))
        self.assertEqual(h.served[0].source, L.Source.CLOCK_ASSEMBLY)
        self.assertTrue(h.ok)
        files = L.builder_files(self.w.select([a]))
        self.assertTrue({str(i) for i in ids} <= set(files))  # the builder's --clock-assemblies replaces the inputs
        self.assertNotIn('sha256:' + a.sha256, files)

    def test_whole_hour_assembly_takes_the_hour_name(self):
        a = self.live(assembly(self.rec, [1, 2, 3, 4]))
        h = self.w.hour(self.w.select([a]), DAYS[3], 5)
        self.assertEqual([s.path for s in h.served], [path(DAYS[3], 5)])

    def test_assembly_outside_r2_does_not_serve(self):
        a = assembly(self.rec, [1, 2])
        h = self.w.hour(self.w.select([a]), DAYS[3], 5)
        self.assertEqual(len(h.served), 4)

    def test_assembly_serves_while_a_replaced_part_is_gone(self):
        a = self.live(assembly(self.rec, [1, 2]))
        del self.w.listing[self.rec.parts[0].key]
        self.assertTrue(self.w.hour(self.w.select([a]), DAYS[3], 5).ok)

    def test_parts_taken_twice_fail(self):
        a = self.live(assembly(self.rec, [1, 2], 'aa'))
        b = self.live(assembly(self.rec, [2, 3], 'bb'))
        with self.assertRaisesRegex(L.LedgerError, 'inputs of two assemblies'):
            self.w.select([a, b])

    def test_non_consecutive_parts_fail(self):
        a = self.live(assembly(self.rec, [1, 3]))
        with self.assertRaisesRegex(L.LedgerError, 'not consecutive'):
            self.w.select([a])

    def test_parts_of_another_record_fail(self):
        other = self.w.records[(RID, DAYS[0], 1)][0]
        a = self.live(dataclasses.replace(assembly(self.rec, [1, 2]), input_ids=(self.rec.parts[0].output_id, other.parts[0].output_id)))
        with self.assertRaisesRegex(L.LedgerError, 'not parts of hour'):
            self.w.select([a])

    def test_reads_clock_join_proofs(self):
        a = assembly(self.rec, [2, 3])
        with tempfile.TemporaryDirectory() as d:
            proofs = os.path.join(d, 'joined', 'clock-prestitch-20261125', 'proofs')
            os.makedirs(proofs)
            with open(os.path.join(proofs, a.sha256 + '.json'), 'w') as f:
                json.dump({'hour_id': a.hour_id, 'inputs': [{'output_id': i} for i in a.input_ids],
                           'output': {'key': a.key, 'sha256': a.sha256, 'size_bytes': a.size_bytes}}, f)
            self.assertEqual(L.read_proofs(d), [a])

    def test_unknown_hour_fails(self):
        a = dataclasses.replace(assembly(self.rec, [1, 2]), hour_id='h-unknown')
        with self.assertRaisesRegex(L.LedgerError, 'outside the best-14 extract'):
            self.w.select([a])


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
        asm = L.Served('sha256:' + 'cd' * 32, 'joined/clock-prestitch-x/objects/cd.mp4', 1, 'p.mp4', L.Source.CLOCK_ASSEMBLY,
                       tuple(s.file_id for s in h.served))
        hours = [dataclasses.replace(h, served=(asm,) + h.served)] + self.hours[1:]
        with self.assertRaisesRegex(L.LedgerError, 'duplicates parts'):
            L.check_rules(hours)

    def test_duplicate_delivery_path_fails(self):
        a = self.w.records[(RID, DAYS[0], 1)][0].parts[0]
        b = self.w.records[(RID, DAYS[0], 2)][0].parts[0]
        self.w.registry[str(b.output_id)] = self.w.registry[str(a.output_id)]
        with self.assertRaisesRegex(L.LedgerError, 'delivery paths are served twice'):
            self.w.select()


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
                        head = [rid, date, dh, r.row_id, r.hour_id, r.batch_id, r.status, r.registered_at]
                        if not r.parts:
                            out.writerow(head + [''] * 6 + ['f', 'f'])
                        for q in r.parts:
                            out.writerow(head + [q.output_id, q.part, q.parts, q.key, q.nas_path, q.size_bytes, 'f', 'f'])
            with open(p('listing.txt'), 'w') as f:
                f.writelines(f'2026-11-25 10:00:00 {n:>10} {k}\n' for k, n in w.listing.items())
            with open(p('registry.json.gz'), 'wb') as f:
                f.write(gzip.compress(json.dumps({'files': {k: {'path': v} for k, v in w.registry.items()}}).encode()))
            tool = [sys.executable, '-I', L.__file__]
            sql = subprocess.run(tool + ['extract-sql', '--best14', p('best14.tsv')], check=True, capture_output=True, text=True).stdout
            self.assertIn(f"({RID},date '{DAYS[0]}')", sql)
            r = subprocess.run(tool + ['build', '--best14', p('best14.tsv'), '--extract', p('extract.tsv'), '--listing', p('listing.txt'),
                                       '--registry', p('registry.json.gz'), '--out-dir', p('out')], check=True, capture_output=True, text=True)
            self.assertTrue(r.stdout.startswith('streams_available=1/1 days_complete=14/14 hours_complete=140/140'))
            rows = L.read_tsv(p('out/best14-ledger.tsv'), L.LEDGER_COLUMNS)
            self.assertEqual(len(rows), 14 * 12)
            self.assertEqual(rows[0]['delivery_paths'].split(','), [path(DAYS[0], 1, 1), path(DAYS[0], 1, 2)])
            # The builder takes the ledger's days and files as its --best14 and --files.
            self.assertEqual(build_generation.read_best14(p('out/best14-ledger-days.tsv')), {RID: {(day, 'R') for day in DAYS}})
            self.assertEqual(len(build_generation.read_lines(p('out/best14-ledger-files.txt'))), 280)
            with open(p('listing.txt'), 'a') as f:
                f.write('2026-11-25 10:00:00 1 raw/x.mp4\n')
            r = subprocess.run(tool + ['build', '--best14', p('best14.tsv'), '--extract', p('extract.tsv'), '--listing', p('listing.txt'),
                                           '--registry', p('registry.json.gz'), '--out-dir', p('out2')], capture_output=True, text=True)
            self.assertNotEqual(r.returncode, 0)
            self.assertIn('not a recursive listing of joined/', r.stderr)


if __name__ == '__main__':
    unittest.main()
