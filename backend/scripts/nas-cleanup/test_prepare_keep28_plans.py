import copy
import unittest
import tempfile
import json
from pathlib import Path
import pathlib
import urllib.error
import io
import sys
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'ops/jelly/finish'))
from cli_exec_guard import install_guard, guard_test_module
install_guard()

import prepare_keep28_plans as k


def setUpModule():
    guard_test_module()


def row(sid, kind, n, size=1, purged=False):
    return {'stream_id': sid, 'kind': kind, 'source_id': n, 'path': f'p/{n}.mp4', 'size_bytes': size, 'r2_purged': purged}


class Keep28PlanTests(unittest.TestCase):
    def admission(self, master=True):
        rows = [row(3, 'raw', 1), row(3, 'raw', 2)]
        post = dict(id=1442, plan_sha256='a'*64)
        detail = dict(post, label='k28-test', mode='dry_run', policy_current=True,
                      policy_sha256=k.POLICY_SHA, master_deletion_enabled=master,
                      allow_nas_only=False, approval_reason='', approved_until=None,
                      items=[dict(kind='raw', source_id=n) for n in (1, 2)],
                      results=[dict(ordinal=n, delete_ever_issued=False, state='leased') for n in (1, 2)])
        return post, detail, rows

    def test_admission_is_unarmed_independently_of_master_and_records_master(self):
        for master in (True, False):
            post, detail, rows = self.admission(master)
            receipts = []
            with patch.object(k, 'service', side_effect=[post, detail]) as service, patch('builtins.print'):
                k.submit('fixture', 'k28-test', rows, receipts, [], atomic=True)
            self.assertIs(receipts[0]['master_deletion_enabled'], master)
            self.assertEqual([c.args[1] for c in service.call_args_list], ['POST', 'GET'])

    def test_admitted_plan_contract_is_explicit_and_fail_closed(self):
        mutations = [
            lambda d: d.pop('master_deletion_enabled'),
            lambda d: d.update(master_deletion_enabled=1),
            lambda d: d.update(mode='enabled'),
            lambda d: d.update(id=99),
            lambda d: d.update(label='foreign'),
            lambda d: d.update(plan_sha256='b'*64),
            lambda d: d.update(policy_sha256='b'*64),
            lambda d: d.update(policy_current=False),
            lambda d: d.update(allow_nas_only=True),
            lambda d: d.update(approval_reason='previous approval'),
            lambda d: d['items'].append(d['items'][0]),
            lambda d: d['results'].pop(),
            lambda d: d['results'][0].update(ordinal=2),
            lambda d: d['results'][0].update(delete_ever_issued=True),
            lambda d: d['results'][0].update(delete_ever_issued=0),
            lambda d: d['results'][0].update(state='delete_issued'),
            lambda d: d['results'][0].update(state='deleted'),
        ]
        for mutation in mutations:
            post, detail, rows = self.admission()
            mutation(detail)
            with self.subTest(mutation=mutation), self.assertRaisesRegex(RuntimeError, 'reconcile its label'):
                k.validate_admission(post, detail, 'k28-test', rows)

    def test_summary_only_counts_recoverable_supply_without_admission_or_plan_files(self):
        rows = [row(3, 'raw', 1, size=10), row(3, 'collated', 2, size=100), row(3, 'raw', 3, size=1000, purged=True)]
        with tempfile.TemporaryDirectory() as out, \
             patch('sys.argv', ['prepare', '--stream', '3', '--summary-only', '--out', out]), \
             patch.object(k, 'candidates', return_value=rows), \
             patch.object(k, 'submit', side_effect=AssertionError('summary cannot admit')), patch('builtins.print'):
            k.main()
            summary = json.loads((Path(out) / 'summary.json').read_text())
            self.assertEqual((summary['recoverable_files'], summary['recoverable_bytes']), (2, 110))
            self.assertEqual((summary['candidate_files'], summary['candidate_bytes']), (3, 1110))
            self.assertEqual(summary['plans'], [])
            self.assertEqual([p.name for p in Path(out).iterdir()], ['summary.json'])

    def test_atomic_admission_never_splits_an_uncertain_proposal(self):
        with patch.object(k, 'service', side_effect=urllib.error.HTTPError('/plans', 409, 'conflict', {}, io.BytesIO(b'rejected'))) as call:
            with self.assertRaisesRegex(RuntimeError, 'atomic admission rejected'):
                k.submit('https://example.invalid', 'lookahead', [row(3, 'raw', 1), row(3, 'raw', 2)], [], [], atomic=True)
            self.assertEqual(call.call_count, 1)
    def test_exit_codes_mean_nothing_was_admitted_and_other_failures_stay_ambiguous(self):
        with patch('traceback.print_exc'):
            with patch.object(k, 'main', side_effect=k.ReadFailed('timeout')):
                self.assertEqual(k.entrypoint(), k.EXIT_READ_FAILED)
            with patch.object(k, 'service', side_effect=urllib.error.HTTPError('/plans', 409, 'conflict', {}, io.BytesIO(b'rejected'))), \
                 patch.object(k, 'main', side_effect=lambda: k.submit('https://example.invalid', 'x', [row(3, 'raw', 1)], [], [], atomic=True)):
                with self.assertRaisesRegex(RuntimeError, 'atomic admission rejected'):
                    k.entrypoint()
            with patch.object(k, 'service', side_effect=urllib.error.HTTPError('/plans', 502, 'bad gateway', {}, None)), \
                 patch.object(k, 'main', side_effect=lambda: k.submit('https://example.invalid', 'x', [row(3, 'raw', 1)], [], [], atomic=True)):
                with self.assertRaisesRegex(RuntimeError, 'admission not confirmed'):
                    k.entrypoint()

    def test_only_atomic_post_409_emits_durable_single_stream_rejection(self):
        for code in (409, 502):
            with tempfile.TemporaryDirectory() as out, \
                 patch('sys.argv', ['prepare', '--stream', '3', '--max-plans', '1', '--atomic-admission', '--submit', '--out', out]), \
                 patch.object(k, 'candidates', return_value=[row(3, 'raw', 1)]), \
                 patch.object(k, 'service', side_effect=urllib.error.HTTPError('/plans', code, 'conflict', {}, io.BytesIO(b'{"error":"protected identity"}'))) as post:
                if code == 409:
                    self.assertEqual(k.entrypoint(), k.EXIT_ADMISSION_REJECTED)
                    evidence = json.loads((Path(out) / 'admission-rejected.json').read_text())
                    self.assertEqual((evidence['http_status'], evidence['stream_id']), (409, 3))
                    self.assertIn('protected identity', evidence['reason'])
                    self.assertFalse((Path(out) / 'summary.json').exists())
                else:
                    with self.assertRaisesRegex(RuntimeError, 'admission not confirmed'):
                        k.entrypoint()
                    self.assertFalse((Path(out) / 'admission-rejected.json').exists())
                self.assertEqual(post.call_count, 1)

    def test_detail_get_conflict_does_not_mean_admission_was_rejected(self):
        with tempfile.TemporaryDirectory() as out, \
             patch('sys.argv', ['prepare', '--stream', '3', '--atomic-admission', '--submit', '--out', out]), \
             patch.object(k, 'candidates', return_value=[row(3, 'raw', 1)]), \
             patch.object(k, 'service', side_effect=[{'id': 1}, urllib.error.HTTPError('/plans/1', 409, 'conflict', {}, None)]):
            with self.assertRaises(urllib.error.HTTPError):
                k.entrypoint()
            self.assertFalse((Path(out) / 'admission-rejected.json').exists())

    def test_only_transient_sqlstates_are_read_failures(self):
        transient = ['ERROR:  57014: canceling statement due to statement timeout',
                     'FATAL:  57P01: terminating connection due to administrator command\nserver closed the connection unexpectedly',
                     'psql: error: connection to server at "db" failed: timeout expired',
                     'ERROR:  40001: could not serialize access']
        fatal = ['PANIC:  XX001: invalid page\nserver closed the connection unexpectedly',
                 'PANIC:  could not write to file\nserver closed the connection unexpectedly',
                 'ERROR:  57014: canceling statement\nERROR:  XX001: invalid page',
                 'ERROR:  42601: syntax error', 'ERROR:  XX000: internal error', 'something else']
        for text in transient:
            self.assertIsNotNone(k.transient_sql_failure(text), text)
        for text in fatal:
            self.assertIsNone(k.transient_sql_failure(text), text)

    def test_catalogue_timeout_is_a_read_failure(self):
        with patch('subprocess.run', side_effect=k.subprocess.TimeoutExpired('psql', 330)), patch.object(k, 'pg_env', return_value={}):
            with self.assertRaises(k.ReadFailed):
                k.query('SELECT 1', 'psql')
        failed = k.subprocess.CompletedProcess([], 1, b'', b'ERROR:  57014: canceling statement due to statement timeout')
        with patch('subprocess.run', return_value=failed), patch.object(k, 'pg_env', return_value={}):
            with self.assertRaises(k.ReadFailed):
                k.query('SELECT 1', 'psql')
        corrupt = k.subprocess.CompletedProcess([], 1, b'', b'ERROR:  XX001: invalid page in block 7 of relation base/1/2')
        with patch('subprocess.run', return_value=corrupt), patch.object(k, 'pg_env', return_value={}):
            with self.assertRaisesRegex(RuntimeError, 'query failed') as caught:
                k.query('SELECT 1', 'psql')
        self.assertNotIsInstance(caught.exception, k.ReadFailed)

    def test_preparer_requires_current_delivery_hold_check(self):
        with patch.object(k, 'query', return_value=[]) as query:
            k.stream_candidates(k.load_policy(k.POLICY_PATH)[3], 'stoarama', 'psql')
        self.assertIn("NOT delivery_nas_held(x.kind,x.source_id,x.path,x.sha256,x.size_bytes)", query.call_args.args[0])

    def test_policy_is_pinned_and_complete(self):
        policy = k.load_policy(k.POLICY_PATH)
        self.assertEqual(len(policy), 109)
        self.assertEqual(k.POLICY_SHA, 'bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393')
        self.assertEqual(sum(1 for v in policy.values() if v['keep_everything']), 23)
        # Stream 3 is best14 2026-09-19 in Asia/Tokyo; the active window is
        # [best-3d, best+17d) = [2026-09-15T15:00Z, 2026-10-05T15:00Z).
        self.assertEqual(policy[3]['keep_start_utc'], '2026-09-15T15:00:00+00:00')
        self.assertEqual(policy[3]['keep_end_utc'], '2026-10-05T15:00:00+00:00')

    def test_keep_everything_streams_are_listed_against_their_window(self):
        # Their NAS copies outside the window leave against a live R2 copy, so
        # the preparer lists them like any other stream.
        policy = k.load_policy(k.POLICY_PATH)
        self.assertTrue(policy[78]['keep_everything'])
        with patch.object(k, 'stream_candidates', return_value=[]) as listed, \
                patch.object(k, 'admissible_candidates', return_value=[]):
            k.candidates(policy, {78}, 'stoarama', 'psql')
        listed.assert_called_once_with(policy[78], 'stoarama', 'psql')

    def test_policy_loader_rejects_drift_from_the_legacy_selection(self):
        raw = k.POLICY_PATH.read_bytes()
        rows = json.loads(raw)
        for name, mutate in (
            ('shorter window', lambda r: r[0].update(end_at='2026-10-04T15:00:00+00:00')),
            ('not a local midnight', lambda r: r[0].update(start_at='2026-09-15T00:00:00+00:00')),
            ('timezone drift', lambda r: r[0].update(timezone='Asia/Seoul')),
            ('widened past legacy', lambda r: r[0].update(start_at='2026-09-11T15:00:00+00:00', end_at='2026-10-09T15:00:00+00:00')),
        ):
            tampered = copy.deepcopy(rows)
            mutate(tampered)
            tmp = tempfile.TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            path = pathlib.Path(tmp.name) / 'p.json'
            path.write_bytes(json.dumps(tampered).encode())
            with self.assertRaises(SystemExit):
                k.load_policy(path)

    def test_batch_digest_matches_the_active_20_day_vector(self):
        plans = [{'id': 1388, 'plan_sha256': 'a4a8bc7137d732ec9bf68df6e823b7ce4527e7cb4a13afdd05b87bc544601ccc'}, {'id': 1389, 'plan_sha256': 'b' * 64}]
        self.assertEqual(k.batch_digest(k.POLICY_SHA, plans), 'a98f3220d7f8cf754d824b2e01bd9d96406fe470f568798519a2b47c68e2f1df')

    def test_batches_never_mix_streams_kinds_or_recovery_and_cap_items(self):
        rows = [row(3, 'raw', n) for n in range(1, 1202)] + [row(3, 'collated', 5000), row(3, 'raw', 6000, purged=True), row(4, 'raw', 7000)]
        plans = k.batches(rows, 'k28', 500, 0)
        self.assertEqual([l for l, _ in plans], ['k28-s3-col-0001', 'k28-s3-raw-0001', 'k28-s3-raw-0002', 'k28-s3-raw-0003', 'k28-s4-raw-0001', 'k28-s3-raw-nasonly-0001'])
        self.assertTrue(all(1 <= len(items) <= 500 for _, items in plans))
        for _, items in plans:
            self.assertEqual(len({(r['stream_id'], r['kind'], r['r2_purged']) for r in items}), 1)
        self.assertEqual(sum(len(i) for _, i in plans), len(rows))

    def test_default_plans_fill_up_to_20000_items_and_2_tb(self):
        # The item cap (20000) is what the plan-status payload can serve; the
        # byte cap is the server's 2 TB per-plan bound.
        self.assertEqual((k.MAX_PLAN_ITEMS, k.MAX_PLAN_BYTES), (20000, 2_000_000_000_000))
        plans = k.batches([row(3, 'raw', n, size=1) for n in range(1, 20002)], 'k28', k.MAX_PLAN_ITEMS, k.MAX_PLAN_BYTES)
        self.assertEqual([len(i) for _, i in plans], [20000, 1])
        # At 17 MB per clip 20000 items is only ~340 GB, so items bind first;
        # the 2 TB byte cap only binds for very large files (>=100 MB each).
        rows = [row(3, 'raw', n, size=17_000_000) for n in range(1, 20002)]
        plans = k.batches(rows, 'k28', k.MAX_PLAN_ITEMS, k.MAX_PLAN_BYTES)
        self.assertEqual([len(i) for _, i in plans], [20000, 1])
        plans = k.batches([row(3, 'raw', n, size=500_000_000) for n in range(1, 4003)], 'k28', k.MAX_PLAN_ITEMS, k.MAX_PLAN_BYTES)
        self.assertEqual([len(i) for _, i in plans], [4000, 2])
        self.assertTrue(all(sum(r['size_bytes'] for r in i) <= k.MAX_PLAN_BYTES for _, i in plans))
        self.assertTrue(all(len(i) <= k.MAX_PLAN_ITEMS for _, i in plans))

    def test_batch_digest_keeps_the_legacy_vector(self):
        # The digest algorithm is unchanged; the historic keep28 vector stays
        # exact as a cross-repo regression pin.
        plans = [{'id': 1388, 'plan_sha256': 'a4a8bc7137d732ec9bf68df6e823b7ce4527e7cb4a13afdd05b87bc544601ccc'}, {'id': 1389, 'plan_sha256': 'b' * 64}]
        self.assertEqual(k.batch_digest(k.LEGACY_POLICY_SHA, plans), 'f2e8d37dca0fcb75e902fc2a1e4271022dc8cc23b5f6f1d4cbca53b9be8e3328')

    def test_batch_manifests_group_by_stream_and_skip_nas_only(self):
        receipts = [{'id': n, 'label': 'k28-s%d-raw-%04d' % (s, n), 'stream_id': s, 'plan_sha256': '%064x' % n, 'items': 10, 'bytes': 100} for n, s in ((1, 3), (2, 3), (3, 4))]
        receipts.append({'id': 4, 'label': 'k28-s3-raw-nasonly-0001', 'stream_id': 3, 'plan_sha256': 'c' * 64, 'items': 1, 'bytes': 1})
        batches = k.batch_manifests(receipts, 'k28', 'stream')
        self.assertEqual([(b['name'], [p['id'] for p in b['plans']]) for b in batches], [('k28-s3', [1, 2]), ('k28-s4', [3])])
        self.assertEqual(batches[0]['batch_sha256'], k.batch_digest(k.POLICY_SHA, batches[0]['plans']))
        self.assertEqual(len(k.batch_manifests(receipts, 'k28', 'all')[0]['plans']), 3)

    def test_runner_timestamp_prefix_fits_immutable_labels(self):
        prefix = 'k28auto-20261111-120000'
        with tempfile.TemporaryDirectory() as out, \
             patch('sys.argv', ['prepare', '--label-prefix', prefix, '--out', out]), \
             patch.object(k, 'candidates', return_value=[row(3, 'raw', 1)]), \
             patch('builtins.print'):
            k.main()
            label = k.batches([row(3, 'raw', 1)], prefix, 10000, k.MAX_PLAN_BYTES)[0][0]
            self.assertTrue(k.LABEL.fullmatch(label))

    def test_byte_cap_for_pilot(self):
        plans = k.batches([row(3, 'raw', n, size=2_000_000_000) for n in range(1, 6)], 'k28', 500, 5_000_000_000)
        self.assertEqual([len(i) for _, i in plans], [2, 2, 1])

    def test_largest_files_fill_first_plan_independent_of_input_order(self):
        rows = [row(3, 'raw', 1, size=1), row(3, 'raw', 2, size=80),
                row(3, 'raw', 3, size=20), row(3, 'raw', 4, size=1)]
        for inputs in (rows, list(reversed(rows))):
            plans = k.batches(inputs, 'k28', 100, 100)
            self.assertEqual([[i['source_id'] for i in items] for _, items in plans], [[2, 3], [1, 4]])

    def test_plan_priority_uses_bytes_per_operation_and_recovery_present(self):
        rows = [row(3, 'raw', 1, size=10), row(3, 'collated', 2, size=100),
                row(4, 'raw', 3, size=1000, purged=True)]
        plans = k.batches(rows, 'k28', 100, 1000)
        self.assertEqual([p[0] for p in plans], ['k28-s3-col-0001', 'k28-s3-raw-0001', 'k28-s4-raw-nasonly-0001'])

    def test_stream_candidates_fences_hold_checks_after_indexed_stream_scope(self):
        with patch.object(k, 'query', return_value=[]) as query:
            k.stream_candidates(k.load_policy(k.POLICY_PATH)[3], 'stoarama', 'psql')
        sql = query.call_args.args[0]
        self.assertIn('raw AS MATERIALIZED', sql)
        self.assertIn('col AS MATERIALIZED', sql)
        self.assertIn('c.recording_id=r.id OFFSET 0', sql)
        self.assertIn('f.connection_id=13 AND f.clip_id=c.id OFFSET 0', sql)


def plaza(date, hhmmss='103000', folder='99991_asia_japan_tokyo_shinjuku9'):
    month, weekday = k.MONTHS[date.month - 1], k.WEEKDAYS[date.weekday()]
    return '%s/%s/%02d-%s/p_x_%04d_%s_W%d_%s_hour_%s.mp4' % (
        folder, month, date.day, weekday, date.year, month, (date.day - 1) // 7 + 1, weekday, hhmmss)


def clip(date, size=10, held=False, hhmmss='103000', folder='99991_asia_japan_tokyo_shinjuku9', cid=0, catalogued=None, raw_key=None,
         zone='Asia/Tokyo', seen='2026-11-01T00:00:00+00:00'):
    start = k.datetime.datetime.combine(date, k.datetime.time(10, 30), k.zoneinfo.ZoneInfo(zone))
    end = start + k.datetime.timedelta(minutes=1)
    return [plaza(date, hhmmss, folder), start.isoformat(), end.isoformat(), held, size, cid,
            size if catalogued is None else catalogued, raw_key, seen]


SEOUL = '04_Asia_South_Korea_Seoul_Daechi_Station_Intersection'  # Stream 78, Keep Everything: Seoul [2026-08-24, 2026-09-13).


def ke_clip(date, cid, size=10, hhmmss='103000', **kw):
    return clip(date, size, hhmmss=hhmmss, folder=SEOUL, cid=cid, zone='Asia/Seoul', **kw)


class DayPlanTests(unittest.TestCase):
    def setUp(self):
        self.policy = k.load_policy(k.POLICY_PATH)  # Stream 3 keeps Tokyo [2026-09-16, 2026-10-06).
        self.folders = k.load_folders()
        self.d = k.datetime.date

    def facts(self, *clips, claimed=(), held_dirs=(), outputs=()):
        return dict(clips=list(clips), outputs=[list(x) for x in outputs], claimed=[list(x) for x in claimed],
                    held_dirs=list(held_dirs), keep20_active=True)

    def units(self, w, facts, listed=None):
        return k.day_units(w, self.folders, facts, listed)[0]

    def test_units_are_outside_unheld_non_keep_everything_directory_days(self):
        out = self.d(2026, 9, 12)
        facts = self.facts(clip(out, 10), clip(out, 20, hhmmss='113000'), clip(out, -1, hhmmss='123000'),
                           clip(self.d(2026, 9, 15)), clip(self.d(2026, 9, 20)), clip(self.d(2026, 10, 6)),
                           clip(self.d(2026, 10, 7), 5), clip(self.d(2026, 10, 10), held=True))
        units = self.units(self.policy[3], facts)
        self.assertEqual([(u['local_date'], u['files'], u['bytes']) for u in units], [('2026-09-12', 2, 30), ('2026-10-07', 1, 5)])
        self.assertEqual(units[0]['relative_dir'], '99991_asia_japan_tokyo_shinjuku9/September/12-Saturday')
        self.assertNotIn('names', units[0])
        for state in ('pending', 'permitted', 'deleted'):
            self.assertEqual(self.units(self.policy[3], self.facts(clip(out), claimed=[(units[0]['relative_dir'], '2026-09-12', state, None, '2026-11-02T00:00:00Z')])), [])
        self.assertEqual(self.units(self.policy[3], self.facts(clip(out), held_dirs=[units[0]['relative_dir']])), [])
        self.assertEqual(self.units(self.policy[3], dict(self.facts(clip(out)), keep20_active=False)), [])

    def test_foreign_folders_yield_nothing(self):
        facts = self.facts(clip(self.d(2026, 10, 10), folder=SEOUL))
        self.assertEqual(self.units(self.policy[3], facts), [])

    def test_keep_everything_units_name_only_media_with_a_listed_r2_counterpart(self):
        ke = self.policy[78]
        self.assertTrue(ke['keep_everything'])
        out = self.d(2026, 9, 20)
        facts = self.facts(
            ke_clip(out, 1, raw_key='managed/a/1.mp4'),                         # raw listed, same size: named
            ke_clip(out, 2, hhmmss='113000'),                                   # no counterpart: stays
            ke_clip(out, 3, hhmmss='123000', raw_key='managed/a/3.mp4'),        # raw listed with another size: stays
            ke_clip(out, 4, hhmmss='133000', raw_key='managed/a/4.mp4'),        # raw not listed: stays
            ke_clip(out, 5, hhmmss='143000'),                                   # stitched output listed: named
            ke_clip(out, 6, hhmmss='153000', raw_key='managed/a/6.mp4', catalogued=11),  # NAS size differs from the clip
            ke_clip(out, 7, hhmmss='163000', size=-1, raw_key='managed/a/7.mp4'),        # not on the NAS
            outputs=[('joined/goodplus-x/objects/' + 'a' * 64 + '.mp4', 900, [5, 8]),
                     ('joined/goodplus-x/objects/' + 'b' * 64 + '.mp4', 800, [3])])
        listing = {'managed/a/1.mp4': 10, 'managed/a/3.mp4': 99, 'managed/a/6.mp4': 11,
                   'joined/goodplus-x/objects/' + 'a' * 64 + '.mp4': 900, 'joined/goodplus-x/objects/' + 'b' * 64 + '.mp4': 1}
        asked = []
        units, excluded = k.day_units(ke, self.folders, facts, lambda keys: asked.append(keys) or {x: listing[x] for x in keys if x in listing})
        [unit] = units
        self.assertEqual([n['name'] for n in unit['names']], [plaza(out, h, SEOUL).rsplit('/', 1)[1] for h in ('103000', '143000')])
        self.assertEqual((unit['files'], unit['bytes'], unit['names'][0]['size_bytes']), (2, 20, 10))
        self.assertEqual(excluded, {'files': 4, 'bytes': 40})
        self.assertEqual(len(asked), 1)
        self.assertNotIn('managed/a/7.mp4', asked[0])
        # A deleted KE unit's name stays out until the NAS reports the file again after the deletion
        # (a kept size mismatch); its other names are planned again.
        lister = lambda keys: {x: listing[x] for x in keys if x in listing}
        deleted = lambda at: dict(facts, claimed=[[unit['relative_dir'], '2026-09-20', 'deleted', unit['names'][:1], at]])
        again = self.units(ke, deleted('2026-11-02T00:00:00Z'), lister)
        self.assertEqual([n['name'] for n in again[0]['names']], [unit['names'][1]['name']])
        self.assertEqual(self.units(ke, deleted('2026-10-31T00:00:00Z'), lister), units)
        self.assertEqual(self.units(ke, dict(facts, claimed=[[unit['relative_dir'], '2026-09-20', 'pending', unit['names'][:1],
                                                             '2026-11-02T00:00:00Z']]), lister), [])

    def test_keep_everything_window_and_boundary_days_are_never_planned(self):
        ke, everything = self.policy[78], (lambda keys: {x: 10 for x in keys})
        days = {self.d(2026, 8, 22): True, self.d(2026, 8, 23): False, self.d(2026, 8, 24): False, self.d(2026, 9, 1): False,
                self.d(2026, 9, 12): False, self.d(2026, 9, 13): False, self.d(2026, 9, 14): True}
        facts = self.facts(*[ke_clip(d, n, raw_key='managed/a/%d.mp4' % n) for n, d in enumerate(days)])
        self.assertEqual(sorted(u['local_date'] for u in self.units(ke, facts, everything)),
                         sorted(d.isoformat() for d, ok in days.items() if ok))

    def test_keep_everything_plans_split_under_the_name_cap(self):
        unit = {'relative_dir': SEOUL + '/September/20-Sunday', 'local_date': '2026-09-20', 'stream_id': 78, 'files': 2, 'bytes': 20,
                'names': [{'name': 'a.mp4', 'size_bytes': 10}, {'name': 'b.mp4', 'size_bytes': 10}]}
        with patch.object(k, 'MAX_DAY_NAMES', 3):
            plans = k.day_plans([unit, dict(unit, local_date='2026-09-21'), dict(unit, local_date='2026-09-22')], 'kday', 1000)
        self.assertEqual([len(p['units']) for p in plans], [1, 1, 1])

    def test_undated_catalogue_name_refuses_its_directory(self):
        out = self.d(2026, 9, 12)
        odd = clip(out)
        odd[0] = odd[0].rsplit('/', 1)[0] + '/renamed.mp4'
        self.assertEqual(self.units(self.policy[3], self.facts(clip(out), odd)), [])

    def test_plans_carry_checkable_totals_and_the_server_digest(self):
        unit = {'relative_dir': 'a/September/12-Saturday', 'local_date': '2026-09-12', 'stream_id': 3, 'files': 2, 'bytes': 300}
        # Golden digest shared with backend cleanupDayDigest.
        self.assertEqual(k.day_digest([unit]), '9116c84bd7061b32fa4560b4d0eeafafb47928ed9482772d4ae6d6e2cca5feb3')
        self.assertEqual(k.day_digest([dict(relative_dir='a/September/12-Saturday', local_date='2026-09-12', stream_id=78, files=2, bytes=300, names=[dict(name='a.mp4', size_bytes=100), dict(name='b.mp4', size_bytes=200)])]), 'e49626b3909930c3726bae74feccaae438249f9a49dc43c9ada3a2877e8e98f9')
        units = [dict(unit, local_date='2026-09-%02d' % n, files=n, bytes=10 * n) for n in range(1, 6)]
        plans = k.day_plans(units, 'kday', 2)
        self.assertEqual([p['label'] for p in plans], ['kday-s3-day-0001', 'kday-s3-day-0002', 'kday-s3-day-0003'])
        self.assertEqual([(p['files'], p['bytes']) for p in plans], [(3, 30), (7, 70), (5, 50)])
        self.assertTrue(all(p['plan_sha256'] == k.day_digest(p['units']) and p['policy_sha256'] == k.POLICY_SHA for p in plans))
        detail = dict(id=9, label=plans[0]['label'], plan_sha256=plans[0]['plan_sha256'], mode='dry_run', policy_sha256=k.POLICY_SHA,
                      files=3, bytes=30, units=[dict(u, state='pending', receipts=[]) for u in plans[0]['units']])
        k.validate_day_admission(plans[0], dict(id=9, plan_sha256=plans[0]['plan_sha256']), detail)
        with self.assertRaises(RuntimeError):
            k.validate_day_admission(plans[0], dict(id=9, plan_sha256=plans[0]['plan_sha256']), dict(detail, files=4))

    def test_day_dates_match_server_and_client(self):
        tokyo = k.zoneinfo.ZoneInfo('Asia/Tokyo')
        self.assertEqual(k.day_file_date('recordings/366/continuous', '1789227000.mp4', tokyo), self.d(2026, 9, 13))
        self.assertEqual(k.day_file_date('recordings/366/77', '1789227000000.mp4', tokyo), self.d(2026, 9, 13))
        self.assertIsNone(k.day_file_date('recordings/366/77', '1789227000.mp4', tokyo))
        self.assertEqual(k.pg_timestamp('2026-09-12T01:30:00.5+00:00').microsecond, 500000)



class DayAdmissionReconcileTests(unittest.TestCase):
    """A 5xx or transport failure can follow a committed admission; read the label back first."""
    def setUp(self):
        unit = {'relative_dir': 'recordings/386/continuous', 'local_date': '2026-09-22', 'stream_id': 293, 'files': 2, 'bytes': 300}
        self.plan = k.day_plans([unit], 'kday-x', 1)[0]

    def err(self, code, body=b'{"error":"refused"}'):
        return urllib.error.HTTPError('/day-plans', code, 'x', {}, io.BytesIO(body))

    def admit(self, posts, reads):
        with patch.object(k, 'service', side_effect=posts) as post, patch.object(k, 'query', side_effect=reads) as read, \
             patch.object(k.time, 'sleep') as sleep:
            try:
                return k.day_admit('https://example.invalid', self.plan, 'psql'), post.call_count, read.call_count, sleep.call_count
            except RuntimeError as exc:
                return exc, post.call_count, read.call_count, sleep.call_count

    def test_committed_admission_behind_a_5xx_or_transport_failure_is_adopted(self):
        for failure in (self.err(502), self.err(504), urllib.error.URLError('reset'), TimeoutError(), ConnectionResetError()):
            with self.subTest(failure=failure):
                self.assertEqual(self.admit([failure], [[[11, self.plan['plan_sha256']]]]), ((11, None), 1, 1, 0))

    def test_uncommitted_admission_is_retried_with_backoff(self):
        self.assertEqual(self.admit([self.err(502), self.err(503), {'id': 12}], [[], []]), ((12, None), 3, 2, 2))

    def test_conflict_after_our_own_in_flight_commit_is_adopted_not_a_rejection(self):
        self.assertEqual(self.admit([self.err(502), self.err(409)], [[], [[11, self.plan['plan_sha256']]]]), ((11, None), 2, 2, 1))

    def test_real_refusals_and_identity_mismatches_fail_safe(self):
        (pid, refusal), posts, reads, sleeps = self.admit([self.err(409)], [[]])
        self.assertEqual((pid, refusal.code, posts, reads, sleeps), (None, 409, 1, 1, 0))
        cases = {'refused for': ([self.err(400)], []), 'another digest': ([self.err(502)], [[[11, 'f' * 64]]]),
                 'could not be read back': ([self.err(409)], [k.ReadFailed('timeout')])}
        for message, (posts, reads) in cases.items():
            with self.subTest(message):
                exc, _, _, sleeps = self.admit(posts, reads)
                self.assertIn(message, str(exc))
                self.assertEqual(sleeps, 0)
        n = len(k.ADMISSION_RETRY_DELAYS)
        exc, posts, reads, sleeps = self.admit([self.err(502)] * n, [[]] * (n - 1) + [k.ReadFailed('timeout')])
        self.assertIn('not confirmed', str(exc))
        self.assertEqual((posts, reads, sleeps), (n, n, n - 1))

    def test_day_main_adopts_and_records_the_receipt(self):
        detail = dict(id=11, label=self.plan['label'], plan_sha256=self.plan['plan_sha256'], mode='dry_run', policy_sha256=k.POLICY_SHA,
                      files=2, bytes=300, units=[dict(u, state='pending', receipts=[]) for u in self.plan['units']])
        a = SimpleNamespace(max_units=1000, out=None, stream=[293], psql='psql', summary_only=False, max_plans=1,
                            label_prefix='kday-x', submit=True, api='https://example.invalid', r2prune=None, bucket='stoarama')
        with tempfile.TemporaryDirectory() as out, patch.object(k, 'load_folders', return_value={}), \
             patch.object(k, 'day_stream_rows', return_value={}), \
             patch.object(k, 'day_units', return_value=(self.plan['units'], {'files': 0, 'bytes': 0})), \
             patch.object(k, 'service', side_effect=[self.err(502), detail]), \
             patch.object(k, 'query', return_value=[[11, self.plan['plan_sha256']]]), patch('builtins.print'):
            a.out = out
            self.assertEqual(k.day_main(a, {293: {'keep_everything': False}}), 0)
            summary = json.loads((Path(out) / 'summary.json').read_text())
        self.assertEqual([x['id'] for x in summary['receipts']], [11])

    def test_day_main_lists_keep_everything_last_and_needs_a_lister(self):
        a = SimpleNamespace(max_units=1000, out=None, stream=[], psql='psql', summary_only=True, max_plans=0,
                            label_prefix='kday-x', submit=False, r2prune=None, bucket='stoarama')
        policy = {5: {'keep_everything': True}, 9: {'keep_everything': False}, 7: {'keep_everything': False}}
        seen = []
        with tempfile.TemporaryDirectory() as out, patch.object(k, 'load_folders', return_value={}), \
             patch.object(k, 'day_stream_rows', return_value={}), patch('builtins.print'), \
             patch.object(k, 'day_units', side_effect=lambda w, f, facts, listed: seen.append(listed) or ([], {'files': 0, 'bytes': 0})):
            a.out = out
            with self.assertRaises(SystemExit):
                k.day_main(a, policy)
            self.assertEqual(seen, [None, None])  # 7 and 9 ran before Keep Everything stream 5 refused.
            a.r2prune = '/bin/false'
            self.assertEqual(k.day_main(a, policy), 0)
            self.assertTrue(callable(seen[-1]))

    def test_r2_listing_lists_only_the_wanted_roots(self):
        with tempfile.TemporaryDirectory() as out:
            path = Path(out) / 'listing.tsv'
            def run(argv, **kw):
                self.assertEqual(argv[argv.index('--out') + 1], str(path))
                self.assertEqual([argv[i + 1] for i, x in enumerate(argv) if x == '--prefix'], ['joined/g/objects/', 'managed/a/b/'])
                path.write_text('managed/a/b/c/1.mp4\t10\t"e"\t1\nmanaged/a/b/c/2.mp4\t5\t"e"\t1\njoined/g/objects/x.mp4\t9\t"e"\t1\n')
            with patch.object(k.subprocess, 'run', side_effect=run):
                sizes = k.r2_listing('r2prune', path)({'managed/a/b/c/1.mp4', 'joined/g/objects/x.mp4'})
        self.assertEqual(sizes, {'managed/a/b/c/1.mp4': 10, 'joined/g/objects/x.mp4': 9})


if __name__ == '__main__':
    unittest.main()
