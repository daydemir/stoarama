import copy
import unittest
import tempfile
import json
from pathlib import Path
import pathlib
import urllib.error
import io
import sys
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
            path = pathlib.Path(self.enterContext(tempfile.TemporaryDirectory())) / 'p.json'
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

    def test_default_plans_fill_up_to_10000_items_and_100_gb(self):
        rows = [row(3, 'raw', n, size=17_000_000) for n in range(1, 12001)]
        plans = k.batches(rows, 'k28', k.MAX_PLAN_ITEMS, k.MAX_PLAN_BYTES)
        self.assertEqual([len(i) for _, i in plans], [5882, 5882, 236])
        self.assertTrue(all(sum(r['size_bytes'] for r in i) <= k.MAX_PLAN_BYTES for _, i in plans))
        plans = k.batches([row(3, 'raw', n, size=1) for n in range(1, 10002)], 'k28', k.MAX_PLAN_ITEMS, k.MAX_PLAN_BYTES)
        self.assertEqual([len(i) for _, i in plans], [10000, 1])

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


if __name__ == '__main__':
    unittest.main()
