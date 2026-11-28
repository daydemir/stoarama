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
from unittest import mock
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import best14_ledger as L  # noqa: E402
import delivery_naming as N  # noqa: E402

RID, STREAM = 401, '14478'
DAYS = [(datetime.date(2026, 8, 19) + datetime.timedelta(days=i)).isoformat() for i in range(14)]
CODES = 'RRRRRRRRRR..'  # hours 11 and 12 have no capture
ETAG = '0' * 32


def best14():
    return {RID: (STREAM, [(d, CODES) for d in DAYS])}


def key(batch, oid):
    return f'joined/{batch}/objects/{oid:064x}.mp4'


NAMES = {RID: (f'{RID}_South_America_Peru_Cusco_Plaza_Mayor', f'{RID}_Plaza_Mayor')}


def path(date, dh, part=None, count=2):
    return N.delivery_path(NAMES, RID, date, dh, part or 1, count if part else 1)


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
                self.files[str(p.output_id)] = {'path': path(date, dh, p.part if parts > 1 else None, parts)}
        return rec

    def inputs(self):
        return L.Inputs(best14(), self.records, list(self.assemblies), self.listing, {'files': self.files}, {}, 0, NAMES)

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


PUB = L.Publication('ab' * 16, False, {RID: 'best14'}, {RID: 'available'})


def summary(hours):
    return L.summary_line(hours, L.stream_rows(hours, L.day_rows(hours)), 0, PUB)


class SelectionTests(unittest.TestCase):
    def test_complete_world(self):
        hours = World().select()
        self.assertEqual(len(hours), 14 * 12)
        days = L.day_rows(hours)
        streams = L.stream_rows(hours, days)
        self.assertEqual({d['day_stitched'] for d in days}, {'R'})
        self.assertEqual((streams[0]['days_complete'], streams[0]['available'], streams[0]['captured_hours']), (14, 'y', 140))
        self.assertIn(f'generation={"ab" * 16} published=n page_available=1 windows_best14=1 streams_available=1/1 '
                      'days_complete=14/14 hours_complete=140/140 original=280 ', summary(hours))

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

    def test_closure_blocked_part_is_not_servable_unless_held(self):
        w = World()
        rec = w.records[(RID, DAYS[0], 1)][0]
        blocked = dataclasses.replace(rec.parts[1], closed=True)
        w.records[(RID, DAYS[0], 1)] = [dataclasses.replace(rec, parts=(rec.parts[0], blocked))]
        hours = w.select()
        h = w.hour(hours, DAYS[0], 1)
        self.assertEqual((h.complete, h.in_r2, [s.live for s in h.served]), (True, False, [True, False]))
        self.assertIn('closed_parts=1', summary(hours))
        self.assertEqual(L.day_rows(hours)[0]['day_stitched'], 'S')
        inputs = dataclasses.replace(w.inputs(), holds={blocked.key: (ETAG, blocked.size_bytes)})
        self.assertTrue(w.hour(L.select(inputs), DAYS[0], 1).ok)  # already held: the guard does not apply

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
        self.assertEqual([s.path for s in h.served], [path(DAYS[2], 4, p, 3) for p in (1, 2, 3)])
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

    def test_live_generation_assembly_takes_its_delivery_name(self):
        a = self.w.assembly(self.rec, [1, 2, 3, 4], source=L.Source.V85_ASSEMBLY, path='live/name_v85.mp4')
        h = self.w.hour(self.w.select(), DAYS[3], 5)
        self.assertEqual([(s.file_id, s.path, s.source) for s in h.served], [(a.file_id, path(DAYS[3], 5), L.Source.V85_ASSEMBLY)])
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

    def test_vetoed_assembly_serves_its_parts_under_their_part_names(self):
        a = self.w.assembly(self.rec, [1, 2, 3, 4], source=L.Source.V85_ASSEMBLY, path='live/name_v85.mp4')
        with mock.patch.object(L, 'VETOED', frozenset({a.file_id})):
            hours = self.w.select()
        h = self.w.hour(hours, DAYS[3], 5)
        self.assertEqual([(s.file_id, s.path) for s in h.served],
                         [(str(p.output_id), path(DAYS[3], 5, p.part, 4)) for p in self.rec.parts])
        self.assertTrue(h.ok)
        self.assertIn('v85_assembly=0', summary(hours))

    def test_vetoed_assembly_across_an_hour_boundary_serves_both_hours_as_parts(self):
        nxt = self.w.records[(RID, DAYS[3], 6)][0]
        a = self.w.assembly([self.rec.parts[3].output_id, nxt.parts[0].output_id], source=L.Source.V79_ASSEMBLY, path='cross.mp4')
        with mock.patch.object(L, 'VETOED', frozenset({a.file_id})):
            hours = self.w.select()
        h5, h6 = self.w.hour(hours, DAYS[3], 5), self.w.hour(hours, DAYS[3], 6)
        self.assertEqual([s.file_id for s in h5.served], [str(p.output_id) for p in self.rec.parts])
        self.assertEqual(([s.file_id for s in h6.served], h6.covered_by), ([str(p.output_id) for p in nxt.parts], ()))
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

    def test_clock_join_replaces_live_assemblies_inside_it(self):
        v79 = self.w.assembly(self.rec, [1, 2], 'aa', L.Source.V79_ASSEMBLY, path='v79.mp4')
        v85 = self.w.assembly(self.rec, [3, 4], 'bb', L.Source.V85_ASSEMBLY, path='v85.mp4')
        a = self.w.assembly(self.rec, [1, 2, 3, 4], 'cc')
        hours = self.w.select()
        h = self.w.hour(hours, DAYS[3], 5)
        self.assertEqual([(s.file_id, s.path, s.source) for s in h.served], [(a.file_id, path(DAYS[3], 5), L.Source.CLOCK_ASSEMBLY)])
        served = {s.file_id for x in hours for s in x.served}
        self.assertFalse({v79.file_id, v85.file_id} & served)
        self.assertIn(v79.key, self.w.listing)  # the replaced assembly stays in R2 for rollback

    def test_clock_join_partly_over_a_live_assembly_fails(self):
        self.w.assembly(self.rec, [1, 2, 3], 'aa', L.Source.V79_ASSEMBLY, path='v79.mp4')
        self.w.assembly(self.rec, [3, 4], 'cc')
        with self.assertRaisesRegex(L.LedgerError, 'only partly inside'):
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
        with tempfile.TemporaryDirectory() as d, mock.patch("continuity_join.validate_clock_proof"):
            proofs = os.path.join(d, 'joined', 'clock-prestitch-20261125', 'proofs')
            os.makedirs(proofs)
            p = Path(proofs) / (a.sha256 + '.json')
            record = {'hour_id': self.rec.hour_id, 'inputs': [{'output_id': i} for i in a.input_ids],
                      'output': {'key': a.key, 'sha256': a.sha256, 'size_bytes': a.size_bytes}}
            p.write_text(json.dumps(record), encoding='utf-8')
            proof_key = a.key.replace('/objects/', '/proofs/').replace('.mp4', '.json')
            proof_bytes = p.read_bytes()
            listing = {a.key: (a.size_bytes, ETAG), proof_key: (len(proof_bytes), ETAG)}
            uploads = {a.key: (a.size_bytes, ETAG, a.sha256),
                       proof_key: (len(proof_bytes), ETAG, hashlib.sha256(proof_bytes).hexdigest())}
            self.assertEqual(L.read_proofs(d, listing, uploads, {}, None), [a])
            for bad in ({}, {a.key: (a.size_bytes, ETAG, 'f' * 64)}, {a.key: (a.size_bytes, '1' * 32, a.sha256)}):
                with self.assertRaisesRegex(L.LedgerError, 'no unique server-checked upload binds'):
                    L.read_proofs(d, listing, bad, {}, None)
            with self.assertRaises(L.LedgerError):
                L.read_proofs(d, {a.key: listing[a.key]}, uploads, {}, None)
            with mock.patch.object(L, 'VETOED_PAIRS', frozenset(zip(a.input_ids, a.input_ids[1:]))):
                with self.assertRaisesRegex(L.LedgerError, 'vetoed original-output boundary'):
                    L.read_proofs(d, listing, uploads, {}, None)
            p.write_bytes(proof_bytes + b' ')
            with self.assertRaisesRegex(L.LedgerError, 'exact proof bytes'):
                L.read_proofs(d, listing, uploads, {}, None)
            p.write_bytes(proof_bytes)
            with self.assertRaisesRegex(L.LedgerError, 'complete upload log'):
                L.read_proofs(d, listing, dict(uploads, extra=(1, ETAG, '0' * 64)), {}, None)

    def test_an_unheld_live_assembly_cannot_return_through_a_proof(self):
        a = self.w.assembly(self.rec, [2, 3], live=False)
        registry = {'files': {a.file_id: {'key': a.key}}}
        with self.assertRaisesRegex(L.LedgerError, 'not in R2 with a matching keep hold'):
            L.with_proofs([], [a], registry)
        held = dataclasses.replace(a, path='p.mp4', source=L.Source.CLOCK_ASSEMBLY)
        self.assertEqual(L.with_proofs([held], [a], registry), [held])  # the live entry serves
        with self.assertRaisesRegex(L.LedgerError, 'differs from its live assembly'):
            L.with_proofs([held], [dataclasses.replace(a, etag='1' * 32)], registry)
        with self.assertRaisesRegex(L.LedgerError, 'differs from its live assembly'):
            L.with_proofs([held], [dataclasses.replace(a, input_ids=a.input_ids[:1])], registry)

    def test_conflicting_input_lists_for_one_file_fail(self):
        self.w.assembly(self.rec, [1, 2], 'aa', L.Source.V79_ASSEMBLY, path='v79.mp4')
        self.w.assembly(self.rec, [1, 2, 3], 'aa')  # the same sha256 with other inputs
        with self.assertRaisesRegex(L.LedgerError, 'conflicting input lists'):
            self.w.select()

    def test_clock_join_replaces_only_v79_and_v85(self):
        self.w.assembly(self.rec, [1, 2], 'aa', L.Source.STRICT_ASSEMBLY, path='strict.mp4')
        self.w.assembly(self.rec, [1, 2, 3], 'cc')
        with self.assertRaisesRegex(L.LedgerError, 'only V79/V85 can be replaced'):
            self.w.select()

    def test_a_vetoed_clock_proof_leaves_the_live_assembly_serving(self):
        v79 = self.w.assembly(self.rec, [1, 2], 'aa', L.Source.V79_ASSEMBLY, path='v79.mp4')
        a = self.w.assembly(self.rec, [1, 2, 3], 'cc')
        with mock.patch.object(L, 'VETOED', frozenset({a.file_id})):
            h = self.w.hour(self.w.select(), DAYS[3], 5)
        self.assertEqual([s.file_id for s in h.served][:1], [v79.file_id])
        self.assertNotIn(a.file_id, [s.file_id for s in h.served])


class RuleTests(unittest.TestCase):
    def setUp(self):
        self.w = World()
        self.hours = self.w.select()

    def test_rules_pass(self):
        L.check_rules(self.hours, NAMES)

    def test_record_serving_two_hours_fails(self):
        a, b = self.hours[0], self.hours[1]
        hours = [a, dataclasses.replace(b, record=a.record, served=a.served)] + self.hours[2:]
        with self.assertRaisesRegex(L.LedgerError, 'serves two hours'):
            L.check_rules(hours, NAMES)

    def test_file_from_another_set_fails(self):
        a, b = self.hours[0], self.hours[1]
        hours = [dataclasses.replace(a, served=a.served + b.served[:1])] + self.hours[1:]
        with self.assertRaisesRegex(L.LedgerError, 'not from the serving record'):
            L.check_rules(hours, NAMES)

    def test_assembly_duplicating_a_part_fails(self):
        h = self.hours[0]
        asm = L.Served('sha256:' + 'cd' * 32, 'joined/clock-prestitch-x/objects/cd.mp4', 1, ETAG, 'cd' * 32, 'p.mp4',
                       L.Source.CLOCK_ASSEMBLY, tuple(s.file_id for s in h.served))
        hours = [dataclasses.replace(h, served=(asm,) + h.served)] + self.hours[1:]
        with self.assertRaisesRegex(L.LedgerError, 'duplicates its parts'):
            L.check_rules(hours, NAMES)

    def test_part_served_by_no_file_fails(self):
        h = self.hours[0]
        hours = [dataclasses.replace(h, served=h.served[:1])] + self.hours[1:]
        with self.assertRaisesRegex(L.LedgerError, 'served by no file'):
            L.check_rules(hours, NAMES)

    def test_served_vetoed_assembly_fails(self):
        h = self.hours[0]
        asm = L.Served('sha256:' + 'cd' * 32, 'experimental/x/cd.mp4', 1, ETAG, 'cd' * 32, path(DAYS[0], 1),
                       L.Source.V79_ASSEMBLY, tuple(s.file_id for s in h.served))
        hours = [dataclasses.replace(h, served=(asm,))] + self.hours[1:]
        L.check_rules(hours, NAMES)
        with mock.patch.object(L, 'VETOED', frozenset({asm.file_id})), self.assertRaisesRegex(L.LedgerError, 'vetoed assemblies are served'):
            L.check_rules(hours, NAMES)

    def test_paths_breaking_the_naming_fail(self):
        h = self.hours[0]
        hours = [dataclasses.replace(h, served=(dataclasses.replace(h.served[0], path='401_x/nas/part.mp4'),) + h.served[1:])] + self.hours[1:]
        with self.assertRaisesRegex(L.LedgerError, 'break the delivery naming'):
            L.check_rules(hours, NAMES)


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
        f.write(gzip.compress(json.dumps({'generation': 'cd' * 16, 'recording_ids': [RID], 'assets': assets, 'files': files}).encode()))


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
                            out.writerow(head + [''] * 9 + ['f'] * (2 + len(L.CLOSURE_COLUMNS)))
                        for q in r.parts:
                            out.writerow(head + [q.output_id, q.part, q.parts, q.key, q.nas_path, q.size_bytes, q.sha256,
                                                 q.start_at, q.end_at, 'f', 'f'] + ['f'] * len(L.CLOSURE_COLUMNS))
            with open(p('listing.txt'), 'w') as f:
                f.writelines(f'{k}\t{n}\t"{e}"\n' for k, (n, e) in w.listing.items())
            with open(p('holds.tsv'), 'w') as f:
                f.write('object_key\tetag\tsize_bytes\n')
            catalog = {'naming': N.NAMING_SPEC, 'streams': [{'id': RID, 'name': 'Cusco - Plaza Mayor', 'folder': NAMES[RID][0],
                       'metadata': {'continent': 'South America', 'country': 'Peru', 'city': 'Cusco', 'plaza_name': 'Plaza Mayor'}}]}
            write_registry(d, {'/public/growth.json': {'repairs': []}, '/public/catalog.json': catalog,
                               f'/public/streams/{RID}.json': {'days': [{'date': '2026-07-01'}]},
                               '/public/metadata.json': {'streams': [{'id': RID, 'start': DAYS[0], 'best14_start': DAYS[0],
                                                                      'availability': 'available'}]}}, w.files)
            tool = [sys.executable, '-I', L.__file__]
            inputs = ['--best14', p('best14.tsv'), '--registry', p('registry.json.gz'), '--assets', p('assets')]
            sql = subprocess.run(tool + ['extract-sql'] + inputs, check=True, capture_output=True, text=True).stdout
            self.assertIn(f"({RID},date '{DAYS[0]}')", sql)
            self.assertIn(f"({RID},date '2026-07-01')", sql)  # the live generation's own days
            self.assertIn('delivery_keep_holds', subprocess.run(tool + ['holds-sql'], check=True, capture_output=True, text=True).stdout)
            build = tool + ['build'] + inputs + ['--extract', p('extract.tsv'), '--holds', p('holds.tsv'), '--listing', p('listing.txt')]
            r = subprocess.run(build + ['--out-dir', p('out')], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, r.stderr)
            self.assertTrue(r.stdout.startswith('generation=' + 'cd' * 16 + ' published=y page_available=1 windows_best14=1 '
                                                'streams_available=1/1 days_complete=14/14 hours_complete=140/140'), r.stdout)
            rows = L.read_tsv(p('out/best14-ledger.tsv'), L.LEDGER_COLUMNS)
            self.assertEqual(len(rows), 14 * 12)
            self.assertEqual(rows[0]['delivery_paths'].split(','), [path(DAYS[0], 1, 1), path(DAYS[0], 1, 2)])
            with open(p('listing.txt'), 'a') as f:
                f.write('raw/x.mp4 1\n')
            r = subprocess.run(build + ['--out-dir', p('out2')], capture_output=True, text=True)
            self.assertNotEqual(r.returncode, 0)
            self.assertIn('not a key/size/ETag listing line', r.stderr)


class FakeLive:
    def __init__(self, generation, streams, files):
        self.objects = {'/public/growth.json': {'generation': generation}, '/public/metadata.json': {'streams': streams},
                        f'/api/coverage-delivery/manifest?stream={RID}&scope=available':
                            {'outputs': [{'id': int(i), 'available': True} for i in files]}}

    def get(self, path, generation=None):
        return self.objects[path]


class LedgerFileTests(unittest.TestCase):
    def test_served_is_what_the_generation_publishes(self):
        w = World()
        hours = w.select()
        page = [h for h in hours if h.local_date != DAYS[0]]  # the page shows 13 of the best-14 days
        published = {s.file_id for h in page for s in h.served} - {hours[13].served[0].file_id}
        with tempfile.TemporaryDirectory() as d:
            L.write(d, page, hours, 0, PUB, published)
            rows = {(r['local_date'], int(r['delivery_hour'])): r for r in L.read_tsv(os.path.join(d, 'best14-ledger.tsv'), L.LEDGER_COLUMNS)}
            self.assertEqual(len(rows), 14 * 12)
            self.assertEqual((rows[(DAYS[0], 1)]['served'], rows[(DAYS[0], 1)]['best14_target']), ('0', 'complete'))
            self.assertEqual((rows[(DAYS[1], 2)]['served'], rows[(DAYS[1], 2)]['complete']), ('1', 'y'))
            self.assertEqual(rows[(DAYS[0], 12)]['best14_target'], 'no_capture')
            st = L.read_tsv(os.path.join(d, 'best14-ledger-streams.tsv'), L.STREAM_COLUMNS)[0]
            self.assertEqual((st['served_files'], st['best14_days_complete'], st['best14_available']), (str(len(published)), '14', 'y'))


class LiveCheckTests(unittest.TestCase):
    def test_live_check_marks_a_matching_ledger_published(self):
        hours = World().select()
        files = {s.file_id for h in hours for s in h.served}
        with tempfile.TemporaryDirectory() as d:
            L.write(d, hours, hours, 0, PUB, files)
            live = [{'id': RID, 'start': DAYS[0], 'best14_start': DAYS[0], 'window': 'best14', 'availability': 'available'}]
            self.assertEqual(L.live_check(d, FakeLive('ef' * 16, live, files)), [f"ledger generation ['{'ab' * 16}'] is not the live generation {'ef' * 16}"])
            rolled = [dict(live[0], window='original', availability='partial')]
            self.assertEqual(len(L.live_check(d, FakeLive('ab' * 16, rolled, files))), 1)
            self.assertEqual(len(L.live_check(d, FakeLive('ab' * 16, live, set(list(files)[1:])))), 1)
            vetoed = sorted(files)[0]  # as if the ledger and the live page both served a vetoed assembly
            with mock.patch.object(L, 'VETOED', frozenset({vetoed})):
                self.assertEqual(L.live_check(d, FakeLive('ab' * 16, live, files)), [f'recording {RID}: serves vetoed assembly {vetoed}'])
            self.assertIn('published=n', Path(d, L.SUMMARY_FILE).read_text())
            self.assertEqual(L.live_check(d, FakeLive('ab' * 16, live, files)), [])
            self.assertIn(f'generation={"ab" * 16} published=y', Path(d, L.SUMMARY_FILE).read_text())
            self.assertEqual({r['published'] for r in L.read_tsv(os.path.join(d, 'best14-ledger.tsv'), L.LEDGER_COLUMNS)}, {'y'})
            self.assertEqual({r['published'] for r in L.read_tsv(os.path.join(d, 'best14-ledger-streams.tsv'), L.STREAM_COLUMNS)}, {'y'})

DATABASE_URL = os.environ.get('STOARAMA_TEST_DATABASE_URL')
CLOSURE_SCHEMA = """
CREATE TABLE recording_collation_hours(id bigint, hour_id text, batch_id text, status text, registered_at timestamptz,
  recording_id bigint, local_date date, delivery_hour smallint, source_clip_count int, included_clip_count int);
CREATE TABLE recording_collation_outputs(id bigint, collation_hour_id bigint, part int, parts int, object_key text,
  nas_relative_path text, size_bytes bigint, sha256 text, start_at timestamptz, end_at timestamptz, source_clip_ids bigint[]);
CREATE TABLE nas_restore_requests(output_id bigint, target text, state text, completed_at timestamptz, verified_etag text,
  object_key text, sha256 text, size_bytes bigint);
CREATE TABLE r2_retirement_writer_operations(collation_recovery_hour_id text, completed_at timestamptz, object_key text);
CREATE TABLE r2_retirement_reservations(kind text, source_id bigint);
CREATE TABLE r2_nonraw_intents(kind text, source_id bigint);
CREATE TABLE r2_object_retirements(kind text, source_id bigint, retired_at timestamptz, bucket text, object_key text,
  sha256 text, size_bytes bigint);
CREATE TABLE r2_inverse_intents(source_id bigint);
INSERT INTO recording_collation_hours VALUES (1, 'h', 'nightly-collation-v2', 'collated', '2026-09-01', 401, '2026-08-19', 1, 5, 5);
INSERT INTO recording_collation_outputs SELECT i, 1, i, 9, 'joined/n/objects/' || i || '.mp4', 'p/' || i || '.mp4', 100 + i,
  repeat(i::text, 64 / length(i::text)), '2026-08-19 06:00+00', '2026-08-19 06:30+00', ARRAY[i * 10] FROM generate_series(1, 9) i;
INSERT INTO r2_object_retirements SELECT 'collated', id, '2026-11-01', 'stoarama', object_key, sha256, size_bytes
  FROM recording_collation_outputs WHERE id BETWEEN 1 AND 6;
-- 2: restored after the retirement, same bytes: no longer blocks (0269).
-- 3: restored before the retirement; 4: different sha256; 5: not verified; 6: empty verified_etag.
INSERT INTO nas_restore_requests SELECT id, 'output', s, c::timestamptz, e, object_key, sha, size_bytes FROM (VALUES
  (2, 'verified', '2026-11-25', 'etag', NULL), (3, 'verified', '2026-10-01', 'etag', NULL), (4, 'verified', '2026-11-25', 'etag', 'f'),
  (5, 'leased', '2026-11-25', 'etag', NULL), (6, 'verified', '2026-11-25', '', NULL)) v(i, s, c, e, bad)
  JOIN recording_collation_outputs o ON o.id = v.i CROSS JOIN LATERAL (SELECT coalesce(repeat(v.bad, 64), o.sha256) sha) x;
INSERT INTO r2_retirement_reservations VALUES ('collated', 7), ('raw', 80);
INSERT INTO r2_nonraw_intents VALUES ('collated', 9);
"""


@unittest.skipUnless(DATABASE_URL, 'set STOARAMA_TEST_DATABASE_URL to run the extract SQL against PostgreSQL')
class ExtractSqlTests(unittest.TestCase):
    def test_closure_clauses(self):
        schema = 'gss_ledger_%d' % os.getpid()
        sql = (f'CREATE SCHEMA {schema}; SET search_path={schema};' + CLOSURE_SCHEMA + L.extract_sql({(RID, DAYS[0])})
               + f'DROP SCHEMA {schema} CASCADE;')
        r = subprocess.run(['psql', DATABASE_URL, '-XqA', '-F', '\t', '-P', 'footer=off', '-v', 'ON_ERROR_STOP=1', '-f', '-'],
                           input=sql, capture_output=True, text=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        rows = {int(x['output_id']): x for x in csv.DictReader(r.stdout.splitlines(), delimiter='\t')}
        closed = {i: [c for c in L.CLOSURE_COLUMNS if rows[i][c] == 't'] for i in rows}
        self.assertEqual(closed, {1: ['retired_collated'], 2: [], 3: ['retired_collated'], 4: ['retired_collated'],
                                  5: ['retired_collated'], 6: ['retired_collated'], 7: ['reserved_collated'],
                                  8: ['reserved_raw_source'], 9: ['nonraw_intent_collated']})



class VetoTests(unittest.TestCase):
    def test_committed_veto_list(self):
        self.assertEqual(L.VETOED, {'sha256:68e17a34141c2ff8f950d1f61011c375d79fb8d7647f933b6e452ccb14ebe412',
                                    'sha256:9f0189a8e885353ff0a72f5b9c298a641bb8f9a3b6c70dddcec842b55b97682d',
                                    'sha256:676b0f19d65bcc33818bda41ca2a43ddffad62689aaafdb9fa83d7f2d361a6a4'})

    def test_rows_need_a_sha_evidence_and_reason(self):
        good = ['sha256:' + 'ab' * 32, '1', 'v79', '2026-08-10', '1', '1>2', '1.0', 'evidence/x.png', 'clock skips']
        cases = {'ok': [good], 'bad sha': [['ab' * 32] + good[1:]], 'no reason': [good[:-1] + ['']],
                 'no evidence': [good[:7] + ['', good[8]]], 'duplicate': [good, good]}
        with tempfile.TemporaryDirectory() as d:
            for name, rows in cases.items():
                p = os.path.join(d, name + '.tsv')
                Path(p).write_text('\n'.join('\t'.join(r) for r in [L.VETO_COLUMNS] + rows) + '\n', encoding='utf-8')
                if name == 'ok':
                    self.assertEqual(L.read_veto(p), ({good[0]}, {(1, 2)}))
                else:
                    with self.assertRaises(L.LedgerError, msg=name):
                        L.read_veto(p)


if __name__ == '__main__':
    unittest.main()
