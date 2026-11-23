import copy
import errno
import io
from datetime import timedelta
import importlib.util
import json
import sys
import os
import shutil
import subprocess
from types import SimpleNamespace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'finish'))
from cli_exec_guard import install_guard, guard_test_module
install_guard()

spec = importlib.util.spec_from_file_location('runner', Path(__file__).with_name('runner.py'))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
_admission_spec = importlib.util.spec_from_file_location('finish_admission_fixture', Path(__file__).resolve().parents[1] / 'finish/admission.py')
FINISH_ADMISSION = importlib.util.module_from_spec(_admission_spec)
_admission_spec.loader.exec_module(FINISH_ADMISSION)
NOW = r.timestamp('2026-11-11T12:00:00+00:00')
PREPARER = r.Runner(Path(__file__).resolve().parents[3], Path('.')).preparer()
# The active 20-day window shape: 20 local days, not the legacy 28.
WINDOWS = {3: (r.timestamp('2026-09-01T00:00:00+00:00'), r.timestamp('2026-09-21T00:00:00+00:00'))}

_bound_cli_fixture = patch.object(r, 'bound_launcher', return_value=Path('/fixture/stoaramactl.bin'))

def setUpModule():
    guard_test_module()
    _bound_cli_fixture.start()

def tearDownModule():
    _bound_cli_fixture.stop()


def fixture():
    item = dict(kind='raw', source_id=1, stream_id=3, size_bytes=100,
                sha256='a'*64, start_at='2026-08-01T00:00:00+00:00',
                end_at='2026-08-01T01:00:00+00:00', object_key='clips/1',
                bucket='stoarama', relative_path='stream/1.mp4', source_ids=[1])
    row = dict(ordinal=1, state='would_delete', recovery='r2_md5_etag_matched',
               delete_ever_issued=False, updated_at=NOW.isoformat())
    p = dict(id=42, items=[item], results=[row], mode='dry_run', policy_current=True,
             policy_sha256=r.POLICY_SHA, plan_sha256='b'*64, allow_nas_only=False,
             master_deletion_enabled=True, approval_reason='', approved_until=(NOW+timedelta(hours=24)).isoformat())
    c = copy.deepcopy(item)
    c['sources'] = [dict(source_id=1, stream_id=3, bucket='stoarama', start_at=item['start_at'], end_at=item['end_at'])]
    return p, dict(items=[c], protected_overlap=0,delivery_keep_overlap=0)


def refused(row):
    row.update(state='keep', delete_ever_issued=True, outcome={
        'outcome': 'unverified', 'unlink_attempted': False,
        'refusal_protocol': 1, 'refusal_reason': 'local_guard'})


def plan_with_refusals(plan, total, ordinals):
    item, row = plan['items'][0], plan['results'][0]
    plan['items'] = [dict(item, source_id=n) for n in range(1, total+1)]
    plan['results'] = [dict(copy.deepcopy(row), ordinal=n, state='deleted', delete_ever_issued=True, outcome={})
                       for n in range(1, total+1)]
    for n in ordinals: refused(plan['results'][n-1])


class Keep20PolicyTests(unittest.TestCase):
    def test_policy_path_and_digest_are_the_active_20_day_pin(self):
        self.assertEqual(r.POLICY, 'backend/internal/api/nas_cleanup_keep20_policy.json')
        self.assertEqual(r.POLICY_SHA, 'bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393')
        self.assertIn('window 20', r.REASON)

    def test_load_policy_excludes_keep_everything_streams(self):
        repo = Path(__file__).resolve().parents[3]
        raw = (repo / r.POLICY).read_bytes()
        job = r.Runner(Path('.'), Path(tempfile.mkdtemp()))
        self.addCleanup(shutil.rmtree, job.root)
        with patch.object(r, 'command', return_value=raw):
            job.load_policy()
        self.assertEqual(len(job.windows) + len(job.keep_everything), 109)
        self.assertEqual(len(job.keep_everything), 23)
        self.assertIn(78, job.keep_everything)
        self.assertNotIn(78, job.windows)
        self.assertNotIn(3, job.keep_everything)
        self.assertEqual(job.windows[3][0].isoformat(), '2026-09-15T15:00:00+00:00')
        self.assertEqual(job.windows[3][1].isoformat(), '2026-10-05T15:00:00+00:00')


class DecisionTests(unittest.TestCase):
    def test_17234_catalog_only_ranking_cache_is_rebuilt_for_reviewed_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            job = r.Runner(Path('.'), Path(tmp))
            job.windows = {17234: WINDOWS[3]}
            bucket = os.environ.get('R2_BUCKET', 'stoarama')
            legacy = r.hashlib.sha256((r.POLICY_SHA + '\n' + bucket).encode()).hexdigest()
            path = job.root / 'stream-summaries' / legacy / '17234' / 'summary.json'
            path.parent.mkdir(parents=True)
            summary = dict(policy_sha256=r.POLICY_SHA, bucket=bucket, streams=[17234],
                           observed_at_epoch=r.time.time(), recoverable_files=50, recoverable_bytes=29585000000)
            path.write_text(json.dumps(summary))

            def prepare(argv, **kwargs):
                self.assertIn('--summary-only', argv)
                new_path = Path(argv[argv.index('--out')+1]) / 'summary.json'
                new_path.parent.mkdir(parents=True, exist_ok=True)
                new_path.write_text(json.dumps(dict(summary, recoverable_files=0, recoverable_bytes=0)))

            with patch.object(r, 'command', side_effect=prepare) as call:
                self.assertEqual(job.stream_order(), [17234])
                self.assertEqual(call.call_count, 1)
            self.assertEqual(json.loads(path.read_text())['recoverable_files'], 50)

    def test_stream_order_uses_cached_bytes_per_file_not_stream_id_or_total(self):
        with tempfile.TemporaryDirectory() as tmp:
            job = r.Runner(Path('.'), Path(tmp))
            job.windows = {5: WINDOWS[3], 3: WINDOWS[3], 4: WINDOWS[3], 6: WINDOWS[3]}
            job.query = lambda *_: self.fail('stream ordering must not query the catalog')
            for sid, files, size in [(3, 100, 1000), (4, 2, 100), (5, 1, 50), (6, 0, 0)]:
                path = job.stream_summary_path(sid)
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps(dict(policy_sha256=r.POLICY_SHA, bucket=os.environ.get('R2_BUCKET', 'stoarama'),
                    streams=[sid], observed_at_epoch=r.time.time(), recoverable_files=files, recoverable_bytes=size)))
            with patch.object(r, 'command', side_effect=AssertionError('cache should be reused')):
                self.assertEqual(job.stream_order(), [4, 5, 3, 6])

    def test_stream_order_refreshes_expired_summaries_one_stream_at_a_time(self):
        with tempfile.TemporaryDirectory() as tmp:
            job = r.Runner(Path('.'), Path(tmp))
            job.windows = WINDOWS
            path = job.stream_summary_path(3)
            path.parent.mkdir(parents=True)
            summary = dict(policy_sha256=r.POLICY_SHA, bucket=os.environ.get('R2_BUCKET', 'stoarama'),
                           streams=[3], observed_at_epoch=0, recoverable_files=1, recoverable_bytes=10)
            path.write_text(json.dumps(summary))
            def prepare(argv, **kwargs):
                self.assertIn('--summary-only', argv)
                self.assertEqual(argv[argv.index('--stream')+1], '3')
                self.assertNotIn('--submit', argv)
                summary['observed_at_epoch'] = r.time.time()
                path.write_text(json.dumps(summary))
            with patch.object(r, 'command', side_effect=prepare) as call:
                self.assertEqual(job.stream_order(), [3])
                self.assertEqual(call.call_count, 1)
            summary['recoverable_files'] = True
            path.write_text(json.dumps(summary))
            with self.assertRaises(r.Halt):
                job.stream_order()

    def test_budget_boundaries_and_invalid_values(self):
        self.assertTrue(r.budget_permits(r.DAY_CAP - r.PLAN_CAP, r.PLAN_CAP))
        for recent, proposed in [(r.DAY_CAP-99, 100), (-1, 100), (0, 0), (0, r.PLAN_CAP+1)]:
            self.assertFalse(r.budget_permits(recent, proposed))

    def test_boundary_is_outside_but_overlap_is_kept(self):
        self.assertTrue(r.outside(3, '2026-08-31T23:00:00Z', '2026-09-01T00:00:00Z', WINDOWS))
        self.assertTrue(r.outside(3, '2026-09-21T00:00:00Z', '2026-09-21T01:00:00Z', WINDOWS))
        self.assertFalse(r.outside(3, '2026-08-31T23:00:00Z', '2026-09-01T00:00:01Z', WINDOWS))

    def test_verifier_passes_both_recovery_proofs(self):
        for recovery in r.RECOVERY:
            p, cat = fixture()
            p['results'][0]['recovery'] = recovery
            self.assertEqual(r.verify(p, cat, WINDOWS, NOW), 100)

    def test_verifier_rejects_incomplete_or_uncertain_evidence(self):
        mutations = [
            lambda p,c: p.update(items=[]),
            lambda p,c: p['results'].append(copy.deepcopy(p['results'][0])),
            lambda p,c: p['results'][0].update(ordinal=2),
            lambda p,c: p['results'][0].update(delete_ever_issued=True),
            lambda p,c: p['results'][0].update(recovery='r2_etag_bound'),
            lambda p,c: p['results'][0].update(updated_at=(NOW-timedelta(hours=20)).isoformat()),
            lambda p,c: p['results'][0].update(updated_at=(NOW+timedelta(seconds=1)).isoformat()),
            lambda p,c: p.update(policy_current=False),
            lambda p,c: p.update(policy_sha256='c'*64),
            lambda p,c: p.update(allow_nas_only=True),
            lambda p,c: c.update(protected_overlap=1),
            lambda p,c: c.update(delivery_keep_overlap=1),
            lambda p,c: c.update(items=[]),
            lambda p,c: c['items'][0].update(sha256='c'*64),
            lambda p,c: c['items'][0].update(sources=[]),
            lambda p,c: c['items'][0]['sources'][0].update(stream_id=4),
            lambda p,c: c['items'][0]['sources'][0].update(end_at='2026-09-02T00:00:00Z'),
        ]
        for mutate in mutations:
            p, cat = fixture()
            mutate(p, cat)
            with self.subTest(mutation=mutate), self.assertRaises(r.Halt):
                r.verify(p, cat, WINDOWS, NOW)

    def test_raw_and_collated_catalog_bounds_are_recomputed(self):
        for kind in ('raw', 'collated'):
            p, cat = fixture()
            p['items'][0]['kind'] = cat['items'][0]['kind'] = kind
            p['items'][0]['end_at'] = cat['items'][0]['end_at'] = '2026-09-02T00:00:00Z'
            with self.assertRaises(r.Halt):
                r.verify(p, cat, WINDOWS, NOW)

    def test_every_nonpermitting_dry_state_stops(self):
        for state in ('keep', 'conflict', 'missing', 'unverified', 'deleted', 'delete_issued', 'media_deleted_sidecar_failed', 'unknown'):
            p, _ = fixture()
            p['results'][0]['state'] = state
            with self.subTest(state=state), self.assertRaises(r.Halt):
                r.dry_state(p)

    def test_pending_dry_run_waits(self):
        p, _ = fixture()
        p['results'][0]['state'] = 'leased'
        self.assertFalse(r.dry_state(p))

    def test_unverified_recovery_waits_only_while_dry_run_in_progress(self):
        for state in ('pending', 'leased', 'would_delete', 'conflict'):
            p, _ = fixture()
            p['results'][0]['recovery'] = 'r2_present_unverified'
            p['items'].append(copy.deepcopy(p['items'][0]))
            p['results'].append(dict(p['results'][0], ordinal=2, state=state,
                                     recovery='r2_sha_verified'))
            with self.subTest(state=state):
                if state in ('pending', 'leased'):
                    self.assertFalse(r.dry_state(p))
                else:
                    with self.assertRaises(r.Halt):
                        r.dry_state(p)

    def test_other_unverified_recovery_still_fails_during_progress(self):
        for recovery in ('unchecked', 'nas_only_at_check', 'conflict', 'unknown'):
            p, _ = fixture()
            p['results'][0]['recovery'] = recovery
            p['items'].append(copy.deepcopy(p['items'][0]))
            p['results'].append(dict(p['results'][0], ordinal=2, state='pending'))
            with self.subTest(recovery=recovery), self.assertRaises(r.Halt):
                r.dry_state(p)

    def test_wait_for_recovery_is_bounded_by_dry_run_deadline(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.p['results'][0]['recovery'] = 'r2_present_unverified'
            job.p['items'].append(copy.deepcopy(job.p['items'][0]))
            job.p['results'].append(dict(job.p['results'][0], ordinal=2, state='pending'))
            with patch.object(r.time, 'monotonic', side_effect=[0, 1, 20*3600]), \
                 patch.object(r.time, 'sleep') as sleep, \
                 self.assertRaisesRegex(r.Halt, 'dry run timed out'):
                r.Runner.await_dry(job, 42)
            sleep.assert_called_once_with(60)

    def test_only_explicit_expiry_reasons_allow_replanning(self):
        p, _ = fixture()
        p['mode'] = 'paused'
        for reason in ('auto-paused: approval expired; prior approval', 'auto-paused: execution lease expired without a report'):
            p['approval_reason'] = reason
            self.assertEqual(r.execution_state(p), 'expired')
        for reason in ('', 'manual pause', 'auto-paused: item observed conflict', 'lease expired maybe'):
            p['approval_reason'] = reason
            self.assertEqual(r.execution_state(p), 'anomaly')

    def test_deleted_requires_ever_issued_and_valid_mode(self):
        p, _ = fixture()
        p['mode'] = 'enabled'
        p['results'][0].update(state='deleted', delete_ever_issued=True)
        self.assertEqual(r.execution_state(p), 'done')
        p['mode'] = 'retired'
        with self.assertRaises(r.Halt):
            r.execution_state(p)

    def test_certified_refusal_runs_with_remainder_and_completes_with_deletions(self):
        p, _ = fixture()
        p.update(mode='enabled')
        refused(p['results'][0])
        p['results'].append(dict(ordinal=2, state='pending', delete_ever_issued=False))
        p['items'].append(dict(p['items'][0], source_id=2))
        self.assertEqual(r.execution_state(p), 'running')
        p['results'][1].update(state='deleted', delete_ever_issued=True)
        self.assertEqual(r.execution_state(p), 'done')
        p['mode'] = 'paused'
        for reason in ('completed: all items deleted or kept',
                       'batch-completed: batch 12 plan finished cleanly'):
            p['approval_reason'] = reason
            self.assertEqual(r.execution_state(p), 'done')
        p['approval_reason'] = 'auto-paused: contradictory receipt'
        self.assertEqual(r.execution_state(p), 'anomaly')

    def test_keep_requires_certified_non_unlink_evidence(self):
        for change in ({'refusal_protocol': 0}, {'unlink_attempted': True},
                       {'outcome': 'deleted'}, {'refusal_reason': 'observation_replayed'}):
            p, _ = fixture()
            p['mode'] = 'enabled'
            refused(p['results'][0])
            p['results'][0]['outcome'].update(change)
            with self.subTest(change=change), self.assertRaises(r.Halt):
                r.execution_state(p)


class FakeRunner(r.Runner):
    """Exercise orchestration without any network, DB or subprocess access."""
    def __init__(self, root, dry_run=False):
        self.root = Path(root)
        self.status = {'current_plan': None, 'last_error': None, 'owner_id': '00000000-0000-0000-0000-000000000001'}
        self.dry_run = dry_run
        self.stopping = False
        self.lock = None
        self.ownership_unreadable = False
        self.windows = WINDOWS
        self.events = []
        self.p, self.cat = fixture()
        self.finish_state = self.root / 'finish'
        self.finish_state.mkdir()
        self.picks = [42, None]
        self.owner_validated = True

    def finish_admission(self): return FINISH_ADMISSION
    def _finish_health(self, admission): return {'active': {}}
    def finish_health(self): pass
    def validate_owner(self): self.owner_validated = True
    def load_policy(self): pass
    def initialize(self): pass
    def check_delivery(self): pass
    def active(self):
        return {'plans': [self.p['id']] if self.p['mode'] == 'enabled' else [], 'batches': 0}
    def idle(self): self.events.append('idle')
    def progress(self): pass
    def recent_bytes(self): return 0
    def catalog(self, plan): return self.cat
    def plan(self, pid): return self.p
    def pick(self, lookahead=False): return self.picks.pop(0) if self.picks else None
    def stream_order(self): return sorted(self.windows)
    def await_dry(self, pid): return self.p
    def master(self, enabled, emergency=False):
        self.events.append('ON' if enabled else 'OFF')
    def api(self, method, path, body=None):
        self.events.append(path)
        if path.endswith('/control'):
            self.assert_enable = body
            self.p.update(mode=body['mode'], approval_reason=body['reason'])
        if path.endswith('/close'):
            self.p['mode'] = 'retired'
        return {'id': self.p['id'], 'plan_sha256': self.p['plan_sha256'], 'mode': 'retired'}
    def monitor(self, pid):
        self.events.append('monitor')
        self.p['mode'] = 'paused'
        self.p['approval_reason'] = 'completed: all items deleted'
        self.p['results'][0].update(state='deleted', delete_ever_issued=True)
        self.finish(self.p)
    def drain(self, pid): pass
    def recovery(self, plan): self.events.append('recovery')
    def notify(self, subject): self.events.append('notify')


class LoopTests(unittest.TestCase):
    def test_lookahead_is_admitted_off_and_never_enabled_before_current_close(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            successor = copy.deepcopy(job.p)
            successor['id'] = 43
            successor['plan_sha256'] = 'c' * 64
            job.picks = [42, 43]
            def pick(lookahead=False):
                if lookahead:
                    self.assertEqual(job.p['mode'], 'dry_run')
                    self.assertEqual([e for e in job.events if e in {'ON', 'OFF'}][-1], 'OFF')
                    self.assertTrue(job.status['lookahead_pending'])
                return job.picks.pop(0) if job.picks else None
            job.pick = pick
            original_monitor = job.monitor
            def monitor(pid):
                if pid == 42:
                    self.assertEqual(job.status['next_plan'], 43)
                    original_monitor(pid)
                    self.assertIsNone(job.status['current_plan'])
                    self.assertIn('plans/42/close', job.events)
                    job.p = successor
                else:
                    self.assertEqual(pid, 43)
                    original_monitor(pid)
            job.monitor = monitor
            job.run()
            self.assertLess(job.events.index('plans/42/close'), job.events.index('plans/43/control'))
            self.assertEqual(job.events.count('OFF'), 3)
            self.assertIsNone(job.status['next_plan'])

    def test_interrupted_lookahead_requires_owner_reconciliation_after_off(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.status['lookahead_pending'] = True
            job.pick = lambda **kw: self.fail('admitted another lookahead after ambiguous admission')
            with self.assertRaisesRegex(r.Halt, 'lookahead admission interrupted'):
                job.run()
            self.assertEqual(job.events, ['OFF'])

    def test_failed_successor_check_stops_monitoring_current_execution(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.p.update(mode='enabled', master_deletion_enabled=True)
            bad = copy.deepcopy(job.p)
            bad.update(id=43, mode='dry_run')
            bad['results'][0]['state'] = 'conflict'
            job.status.update(current_plan=42, next_plan=43, live_commit='a'*40)
            job.plan = lambda pid: job.p if pid == 42 else bad
            job.live = lambda: {'commit': {'id': 'a'*40}}
            with self.assertRaisesRegex(r.Halt, 'non-permitting item'):
                r.Runner.monitor(job, 42)

    def test_empty_lookahead_keeps_current_reservations_and_uses_atomic_admission(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.repo = Path(root)
            job.status['current_plan'] = 42
            queries, commands = [], []
            job.query = lambda sql: queries.append(sql) or []
            def command(args, **kwargs):
                commands.append(args)
                out = Path(args[args.index('--out') + 1])
                out.mkdir(parents=True)
                (out / 'summary.json').write_text(json.dumps(dict(receipts=[], rejected_at_admission=[])))
            with patch.object(r, 'command', side_effect=command):
                self.assertIsNone(r.Runner.pick(job, lookahead=True))
            self.assertEqual(len(queries), 1, 'lookahead attempted completion verification with current work reserved')
            self.assertIn('id<>42', queries[0])
            self.assertTrue(all('--atomic-admission' in args for args in commands))

    def test_pick_skips_nasonly_plans(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.repo = Path(root)
            job.windows = {3: None, 4: None}
            job.status['current_plan'] = None
            queries = []
            job.query = lambda sql: queries.append(sql) or []
            labels = iter(['k28auto-x-s3-raw-nasonly-0001', 'k28auto-x-s4-raw-0001'])
            ids = iter([1405, 1406])
            def command(args, **kwargs):
                out = Path(args[args.index('--out') + 1])
                out.mkdir(parents=True)
                (out / 'summary.json').write_text(json.dumps(dict(
                    receipts=[dict(id=next(ids), label=next(labels))], rejected_at_admission=[])))
            with patch.object(r, 'command', side_effect=command):
                self.assertEqual(r.Runner.pick(job, lookahead=True), 1406)
            self.assertIn("NOT LIKE '%-nasonly-%'", queries[0])

    def isolation_job(self, root, windows):
        job = FakeRunner(root)
        job.repo = Path(__file__).resolve().parents[3]
        job.windows = dict.fromkeys(windows)
        job.query = lambda *a, **k: []
        return job

    def preparer_exits(self, outcomes):
        """Fake preparer: an int is its exit code; a str is the admitted label."""
        def command(args, **kwargs):
            outcome = outcomes.pop(args[args.index('--stream') + 1])
            if not isinstance(outcome, str):
                raise r.CommandFailed(outcome)
            out = Path(args[args.index('--out') + 1])
            out.mkdir(parents=True)
            (out / 'summary.json').write_text(json.dumps(dict(
                receipts=[dict(id=1406, label=outcome)], rejected_at_admission=[])))
        return command

    def test_admission_rejection_is_still_a_stop(self):
        with tempfile.TemporaryDirectory() as root:
            job = self.isolation_job(root, [3, 4])
            outcomes = {'3': 1, '4': 'k28auto-x-s4-raw-0001'}
            with patch.object(r, 'command', side_effect=self.preparer_exits(outcomes)):
                with self.assertRaisesRegex(r.Halt, 'local command failed'):
                    r.Runner.pick(job)
            self.assertIn('4', outcomes, 'continued to another stream after a failed admission check')
            self.assertNotIn('deferred_streams', job.status)

    def test_non_dictionary_deferrals_are_unreadable_ownership_state(self):
        with tempfile.TemporaryDirectory() as root:
            raw=json.dumps({'current_plan': None, 'last_error': None, 'deferred_streams': [3]})
            path=Path(root)/'status.json';path.write_text(raw)
            job=r.Runner(Path('.'),Path(root))
            with self.assertRaisesRegex(r.Halt,'ownership unreadable'):job.initialize()
            with self.assertRaisesRegex(r.Halt,'must be preserved'):job.save('failed')
            self.assertEqual(path.read_text(),raw);job.lock.close()

    def test_atomic_409_excludes_stream_persistently_and_admits_other_stream(self):
        with tempfile.TemporaryDirectory() as root:
            job = self.isolation_job(root, [3, 4])
            good = self.preparer_exits({'4': 'k28auto-x-s4-raw-0001'})
            calls = []
            def prepare(args, **kwargs):
                sid = args[args.index('--stream') + 1]
                calls.append(sid)
                if sid == '4':
                    return good(args, **kwargs)
                out = Path(args[args.index('--out') + 1]); out.mkdir(parents=True)
                label = args[args.index('--label-prefix') + 1] + '-s3-raw-0001'
                (out / 'admission-rejected.json').write_text(json.dumps(dict(http_status=409, stream_id=3,
                    label=label, reason='protected identity', rejected_at_epoch=1)))
                raise r.CommandFailed(5)
            with patch.object(r, 'command', side_effect=prepare):
                self.assertEqual(r.Runner.pick(job, lookahead=True), 1406)
            self.assertEqual(calls, ['3', '4'])
            self.assertEqual(job.status['excluded_streams']['3']['reason'], 'protected identity')
            self.assertFalse(job.stream_ready(3))
            restored = r.Runner(job.repo, Path(root))
            restored.initialize()
            try:
                self.assertFalse(restored.stream_ready(3))
                restored.windows = job.windows
                restored.query = lambda sql, *a: 0 if 'count(*)' in sql else []
                restored.stream_order = lambda: [3]
                restored.confirm_empty = lambda: self.fail('excluded supply claimed complete')
                self.assertIsNone(restored.pick())
                self.assertEqual(restored.status['phase'], 'excluded_streams_remaining')
            finally:
                restored.lock.close()

    def test_excluded_unenabled_current_or_successor_never_enables_but_issued_stops(self):
        for saved in ('current_plan', 'next_plan'):
            with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
                job = FakeRunner(root)
                job.status.update(excluded_streams={'3': dict(http_status=409, stream_id=3,
                    reason='protected', label='rejected')}, **{saved: 42})
                job.picks = [None]
                job.run()
                self.assertNotIn('ON', job.events)
                self.assertEqual(job.status['parked_unenabled_plan'], 42)
                self.assertEqual(job.status['phase'], 'excluded_streams_remaining')
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.status['excluded_streams'] = {'3': {}}
            job.p['results'][0]['delete_ever_issued'] = True
            with self.assertRaisesRegex(r.Halt, 'issued deletions'):
                job.excluded_plan(job.p)
            self.assertNotIn('parked_unenabled_plan', job.status)

    def test_lookahead_exclusion_prevents_current_enable_and_unrelated_pending_still_stops(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            def pick(lookahead=False):
                if lookahead:
                    job.status['excluded_streams'] = {'3': {}}
                    return None
                return None if job.status.get('excluded_streams') else 42
            job.pick = pick
            job.run()
            self.assertNotIn('ON', job.events)
        with tempfile.TemporaryDirectory() as root:
            job = self.isolation_job(root, [])
            job.status['excluded_streams'] = {'3': {}}
            job.query = lambda sql: 1 if 'count(*)' in sql else []
            with self.assertRaisesRegex(r.Halt, 'reserved or uncertain'):
                r.Runner.pick(job)

    def test_excluded_plan_waits_for_complete_dry_run_before_parking(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            job.status.update(current_plan=42, excluded_streams={'3': {}})
            job.p['results'][0]['state'] = 'leased'
            job.picks = [None]
            observed = []
            def finish_dry(pid):
                self.assertNotIn('parked_unenabled_plan', job.status)
                observed.append(pid)
                job.p['results'][0]['state'] = 'would_delete'
                return job.p
            job.await_dry = finish_dry
            job.run()
            self.assertEqual(observed, [42])
            self.assertNotIn('ON', job.events)
            self.assertEqual(job.status['parked_unenabled_plan'], 42)

    def test_rejection_without_evidence_or_outside_admission_still_stops(self):
        with tempfile.TemporaryDirectory() as root:
            job = self.isolation_job(root, [3])
            with patch.object(r, 'command', side_effect=r.CommandFailed(5)):
                with self.assertRaises(FileNotFoundError):
                    r.Runner.pick(job)
                with self.assertRaisesRegex(r.Halt, 'outside atomic admission'):
                    job.prepare_stream(3, ['--summary-only', '--out', root])
            self.assertNotIn('excluded_streams', job.status)

    def test_ambiguous_submission_timeout_or_unknown_failure_still_stops(self):
        for code in (None, 1, 3, 4):
            with tempfile.TemporaryDirectory() as root:
                job = self.isolation_job(root, [3, 4])
                with patch.object(r, 'command', side_effect=self.preparer_exits({'3': code})):
                    with self.assertRaisesRegex(r.Halt, 'local command failed'):
                        r.Runner.pick(job)
                self.assertNotIn('deferred_streams', job.status)

    def test_legacy_deferral_stops_before_any_work(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.status['deferred_streams'] = {'3': {'retry_after': 0, 'failures': 1}}
            job.pick = lambda **kw: self.fail('picked work after a prior failed check')
            with self.assertRaisesRegex(r.Halt, 'prior deferred failure'):
                job.run()
            self.assertNotIn('ON', job.events)

    def test_ranking_read_failure_stops_before_cached_other_stream(self):
        with tempfile.TemporaryDirectory() as root:
            job = r.Runner(Path(__file__).resolve().parents[3], Path(root))
            job.windows = {3: WINDOWS[3], 4: WINDOWS[3]}
            path = job.stream_summary_path(4)
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(dict(policy_sha256=r.POLICY_SHA, bucket=os.environ.get('R2_BUCKET', 'stoarama'),
                streams=[4], observed_at_epoch=r.time.time(), recoverable_files=1, recoverable_bytes=10)))
            with patch.object(r, 'command', side_effect=r.CommandFailed(4)):
                with self.assertRaisesRegex(r.Halt, 'local command failed'):
                    job.stream_order()
            self.assertNotIn('deferred_streams', job.status)

    def test_lookahead_read_failure_stops_before_enable(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            job.status['current_plan'] = 42
            job.pick = lambda **kw: (_ for _ in ()).throw(r.CommandFailed(4))
            with self.assertRaisesRegex(r.Halt, 'local command failed'):
                job.run()
            self.assertNotIn('ON', job.events)
            self.assertTrue(job.status['lookahead_pending'])
            self.assertEqual(job.status['current_plan'], 42)

    def test_command_failure_logs_privately_and_halts(self):
        with tempfile.TemporaryDirectory() as root:
            log = Path(root) / 'command-failures.log'
            with patch.object(r, 'COMMAND_FAILURE_LOG', log):
                with self.assertRaisesRegex(r.Halt, 'local command failed'):
                    r.command([sys.executable, '-c', 'import sys; sys.stderr.write("boom-detail"); sys.exit(3)'])
            row = json.loads(log.read_text().splitlines()[0])
            self.assertEqual(row['returncode'], 3)
            self.assertIn('boom-detail', row['stderr_tail'])
            self.assertEqual(oct(log.stat().st_mode & 0o777), '0o600')

    def test_preparer_gets_long_timeout(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.repo = Path(root)
            job.windows = {3: None}
            job.status['current_plan'] = None
            job.query = lambda sql: []
            seen = []
            def command(args, **kwargs):
                seen.append(kwargs.get('timeout'))
                out = Path(args[args.index('--out') + 1])
                out.mkdir(parents=True)
                (out / 'summary.json').write_text(json.dumps(dict(receipts=[], rejected_at_admission=[])))
            with patch.object(r, 'command', side_effect=command):
                r.Runner.pick(job, lookahead=True)
            self.assertEqual(seen, [r.PREPARE_TIMEOUT_SEC])

    def test_batch_order_and_approval_digest(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            job.run()
            self.assertEqual([e for e in job.events if e != 'idle'], ['OFF', 'ON', 'plans/42/control', 'monitor', 'OFF', 'recovery', 'plans/42/close'])
            self.assertEqual(job.assert_enable['plan_sha256'], 'b'*64)
            self.assertEqual(job.assert_enable['approval_hours'], 24)
            self.assertTrue(job.assert_enable['allow_long_approval'])
            self.assertEqual(job.status['phase'], 'complete')

    def test_dry_run_never_enables(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root, True)
            job.run()
            self.assertEqual(job.events, ['OFF', 'idle'])
            self.assertEqual(job.status['phase'], 'dry_run_pass')

    def test_verifier_failure_never_enables(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            job.cat['protected_overlap'] = 1
            with self.assertRaises(r.Halt): job.run()
            self.assertNotIn('ON', job.events)

    def test_stop_before_batch_keeps_master_off(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            (job.root / 'STOP').touch()
            job.run()
            self.assertNotIn('ON', job.events)
            self.assertEqual(job.status['phase'], 'stopped')

    def test_stop_during_on_redeploy_turns_off_without_enable(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            def master(enabled, emergency=False):
                job.events.append('ON' if enabled else 'OFF')
                if enabled: job.stopping = True
            job.master = master
            job.run()
            self.assertEqual([e for e in job.events if e != 'idle'], ['OFF', 'ON', 'OFF'])

    def test_restart_recovers_current_execution_before_next_batch(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.status.update(current_plan=42, started_plan_sha256=job.p['plan_sha256'])
            job.p.update(mode='enabled', master_deletion_enabled=True)
            job.picks = [None]
            job.stopping = True
            with patch.object(r, 'utcnow', return_value=NOW):
                job.run()
            self.assertEqual(job.events[:3], ['monitor', 'OFF', 'recovery'])

    def test_expiry_closes_only_after_off_and_recovery(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.status['current_plan'] = 42
            job.p['mode'] = 'paused'
            job.p['approval_reason'] = 'auto-paused: approval expired; prior approval'
            job.finish(job.p)
            self.assertEqual(job.events, ['OFF', 'recovery', 'plans/42/close'])

    def test_finish_closes_certified_refusals_and_replans_only_never_issued(self):
        for expiry in (False, True):
            with self.subTest(expiry=expiry), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.status['current_plan'] = 42
                plan_with_refusals(job.p, 100, [1])
                job.p.update(mode='paused', approval_reason='completed: all items deleted or kept')
                if expiry:
                    job.p['approval_reason'] = 'auto-paused: approval expired; prior approval'
                    job.p['items'].append(dict(job.p['items'][0], source_id=2))
                    job.p['results'].append(dict(ordinal=101, state='pending', delete_ever_issued=False))
                before = copy.deepcopy(job.p['results'])
                job.finish(job.p)
                self.assertEqual(job.events, ['OFF', 'recovery', 'plans/42/close'])
                self.assertEqual(job.p['results'], before)
                self.assertIsNone(job.status['current_plan'])
                self.assertEqual(job.status['phase'], 'batch_complete')
                self.assertEqual(job.picks, [42, None])

    def test_finish_rereads_close_before_releasing_ownership(self):
        for change in ('contradiction', 'not_retired', 'identity', 'contradicted_deleted'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.status.update(current_plan=42, started_plan_sha256='b'*64)
                refused(job.p['results'][0])
                job.p.update(mode='paused', approval_reason='completed: all items deleted or kept')
                api = job.api
                def race(method, path, body=None):
                    self.assertNotIn('operator_override', body or {})
                    result = api(method, path, body)
                    job.p = copy.deepcopy(job.p)
                    if change in ('contradiction', 'contradicted_deleted'):
                        job.p['results'][0].update(state='unverified' if change == 'contradiction' else 'deleted', outcome={
                            'certified_refusal': job.p['results'][0]['outcome'],
                            'contradictory_receipt': {'outcome': 'deleted'}})
                    elif change == 'not_retired':
                        job.p['mode'] = 'paused'
                    else:
                        job.p['plan_sha256'] = 'c'*64
                    return result
                job.api = race
                job.run = lambda: job.finish(copy.deepcopy(job.p))
                with patch.object(r, 'Runner', return_value=job), \
                        patch.object(r.sys, 'argv', ['runner']), patch.object(r.signal, 'signal'):
                    self.assertEqual(r.main(), 1)
                self.assertEqual(job.status['current_plan'], 42)
                self.assertEqual(job.status['started_plan_sha256'], 'b'*64)
                self.assertEqual(job.status['phase'], 'failed')
                self.assertTrue(job.status['last_error'])
                self.assertTrue((job.root / 'STOP').exists())
                self.assertNotIn('ON', job.events)
                self.assertIn('notify', job.events)
                self.assertEqual(job.picks, [42, None])

    def test_refusals_are_noted_once_and_visible_in_progress(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            plan_with_refusals(job.p, 100, [1])
            job.record_refusals(job.p)
            job.record_refusals(job.p)
            summary = job.status['certified_refusals']
            self.assertEqual(summary['files'], 1)
            self.assertEqual(summary['plan_files'], 100)
            notes = (Path(root)/'notes.jsonl').read_text().splitlines()
            self.assertEqual(len(notes), 1)
            self.assertEqual(json.loads(json.loads(notes[0])['text']), summary)

    def test_refusal_percentage_and_streak_stop_before_close(self):
        for total, ordinals in [(100, [1, 3]), (1000, list(range(1, 7))), (1, [1])]:
            with self.subTest(total=total, ordinals=ordinals), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.status['current_plan'] = 42
                plan_with_refusals(job.p, total, ordinals)
                job.p.update(mode='enabled', master_deletion_enabled=True)
                monitor = r.Runner.monitor
                job.run = lambda: monitor(job, 42)
                with patch.object(r, 'Runner', return_value=job), patch.object(r.sys, 'argv', ['runner']), \
                        patch.object(r.signal, 'signal'):
                    self.assertEqual(r.main(), 1)
                self.assertEqual(job.status['phase'], 'failed')
                self.assertIn('owner review', job.status['last_error'])
                self.assertTrue((Path(root)/'STOP').exists())
                self.assertEqual(job.events[0], 'OFF')
                self.assertIn('notify', job.events)
                self.assertNotIn('plans/42/close', job.events)
                self.assertTrue((Path(root)/'notes.jsonl').exists())

    def test_five_consecutive_refusals_at_one_percent_are_within_bound(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            plan_with_refusals(job.p, 500, list(range(1, 6)))
            job.record_refusals(job.p)
            self.assertEqual(job.status['certified_refusals']['consecutive'], 5)

    def test_refusal_bounds_cannot_reset_on_restart_or_block_plan_finish_bypass(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            plan_with_refusals(job.p, 100, [1, 2])
            job.p.update(mode='paused', approval_reason='completed: all items deleted or kept')
            # New process/status still derives the full count from durable receipts.
            self.assertNotIn('certified_refusals', job.status)
            with self.assertRaisesRegex(r.Halt, 'owner review'):
                job.finish(job.p)
            self.assertEqual(job.status['certified_refusals']['files'], 2)
            self.assertNotIn('plans/42/close', job.events)

    def test_close_409_latches_owner_stop_with_current_plan(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.status.update(current_plan=42, started_plan_sha256='b'*64)
            plan_with_refusals(job.p, 100, [1])
            job.p.update(mode='paused', approval_reason='completed: all items deleted or kept')
            job.api = lambda method, path, body: r.request('https://invalid/' + path, 'fixture', method, body)
            job.run = lambda: job.finish(copy.deepcopy(job.p))
            error = r.urllib.error.HTTPError('https://invalid', 409, 'private-body', {}, io.BytesIO(b'private-body'))
            opener = SimpleNamespace(open=unittest.mock.Mock(side_effect=error))
            with patch.object(r.urllib.request, 'build_opener', return_value=opener), \
                    patch.object(r, 'Runner', return_value=job), patch.object(r.sys, 'argv', ['runner']), \
                    patch.object(r.signal, 'signal'):
                self.assertEqual(r.main(), 1)
            self.assertEqual(job.status['current_plan'], 42)
            self.assertEqual(job.status['started_plan_sha256'], 'b'*64)
            self.assertIn('owner', job.status['last_error'])
            self.assertNotIn('private-body', json.dumps(job.status))
            self.assertTrue((job.root / 'STOP').exists())
            self.assertEqual(job.events[0], 'OFF')
            self.assertNotIn('ON', job.events)
            self.assertEqual(opener.open.call_count, 1)

    def test_finish_never_closes_uncertain_receipts_even_after_expiry(self):
        for state in ('unverified', 'media_deleted_sidecar_failed', 'conflict', 'missing', 'keep'):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.status['current_plan'] = 42
                job.p.update(mode='paused', approval_reason='auto-paused: approval expired; prior approval')
                job.p['results'][0].update(state=state, delete_ever_issued=True)
                with self.assertRaises(r.Halt):
                    job.finish(job.p)
                self.assertEqual(job.events, ['OFF', 'recovery'])
                self.assertEqual(job.status['current_plan'], 42)

    def test_finish_pauses_terminal_enabled_plan_before_close(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.p['mode'] = 'enabled'
            plan_with_refusals(job.p, 100, [1])
            job.finish(job.p)
            self.assertEqual(job.events, ['OFF', 'recovery', 'plans/42/control', 'plans/42/close'])

    def test_anomaly_never_closes_or_replans(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.p['mode'] = 'paused'
            with self.assertRaises(r.Halt): job.finish(job.p)
            self.assertEqual(job.events, ['OFF', 'recovery'])

    def test_failure_latches_stop_and_alerts_owner(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.fail('protected identity overlap')
            self.assertTrue((job.root / 'STOP').exists())
            self.assertEqual(job.status['last_error'], 'protected identity overlap')
            self.assertIn('notify', job.events)
            with self.assertRaises(r.Halt): job.run()

    def test_budget_wait_does_not_enable(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            job.recent_bytes = lambda: r.DAY_CAP
            with patch.object(r.time, 'sleep', side_effect=lambda _: setattr(job, 'stopping', True)):
                job.run()
            self.assertNotIn('ON', job.events)

    def test_failed_recovery_does_not_close(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            def bad(_): raise r.Halt('R2 recovery verification failed')
            job.recovery = bad
            with self.assertRaises(r.Halt): job.finish(job.p)
            self.assertNotIn('plans/42/close', job.events)

    def test_stop_during_execution_drains_without_pause(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.stopping = True
            job.status.update(current_plan=42, live_commit='d'*40)
            job.p['mode'] = 'enabled'
            job.p['master_deletion_enabled'] = True
            job.active = lambda: {'plans': [42], 'batches': 0}
            job.live = lambda: {'commit': {'id': 'd'*40}}
            def complete(_):
                job.p['mode'] = 'paused'
                job.p['approval_reason'] = 'completed: all items deleted'
                job.p['results'][0].update(state='deleted', delete_ever_issued=True)
            with patch.object(r.time, 'sleep', side_effect=complete):
                r.Runner.monitor(job, 42)
            self.assertEqual(job.events, ['OFF', 'recovery', 'plans/42/close'])

    def test_monitor_rechecks_auto_pause_between_report_and_active_query(self):
        for reason in ('auto-paused: approval expired', 'auto-paused: uncertain receipt'):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.p.update(mode='enabled', master_deletion_enabled=True)
                job.status.update(current_plan=42, live_commit='d'*40)
                def pause():
                    job.p.update(mode='paused', approval_reason=reason)
                    return {'plans': [], 'batches': 0}
                job.active = pause
                with patch.object(job, 'finish') as finish:
                    r.Runner.monitor(job, 42)
                finish.assert_called_once_with(job.p)
                self.assertEqual(job.p['mode'], 'paused')

    def test_monitor_empty_active_requires_confirmed_stop_and_rejects_other_execution(self):
        for active in ({'plans': [], 'batches': 0}, {'plans': [99], 'batches': 0},
                       {'plans': [], 'batches': 1}):
            with self.subTest(active=active), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.p.update(mode='enabled', master_deletion_enabled=True)
                job.active = lambda: active
                with patch.object(job, 'finish') as finish, self.assertRaises(r.Halt):
                    r.Runner.monitor(job, 42)
                finish.assert_not_called()

    def test_deleted_anomalous_pause_still_stops(self):
        p, _ = fixture()
        p['mode'] = 'paused'
        p['approval_reason'] = 'auto-paused: conflicting duplicate receipt'
        p['results'][0].update(state='deleted', delete_ever_issued=True)
        self.assertEqual(r.execution_state(p), 'anomaly')

    def test_delivery_failure_blocks_enable_and_is_persisted(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            def bad(): raise r.Halt('owner email configuration missing')
            job.check_delivery = bad
            with self.assertRaises(r.Halt): job.run()
            self.assertNotIn('ON', job.events)
            def failed_email(_): raise r.Halt('HTTP request failed')
            job.notify = failed_email
            job.fail('owner email configuration missing')
            self.assertTrue(job.status['owner_notification_failed'])

    def test_http_errors_do_not_retain_response_or_credentials(self):
        with patch.object(r.urllib.request, 'build_opener', side_effect=ValueError('fake-sensitive-data')):
            with self.assertRaises(r.Halt) as error:
                r.request('https://invalid.example', 'fake-test-token')
        self.assertEqual(str(error.exception), 'HTTP request failed (GET)')


class SafetyRegressionTests(unittest.TestCase):
    def test_real_lock_contention_leaves_existing_runner_alone(self):
        with tempfile.TemporaryDirectory() as root:
            owner = r.Runner(Path(root), Path(root))
            owner.initialize()
            try:
                with patch.object(r.Runner, 'fail') as fail, \
                        patch.object(r.Runner, 'run') as run, \
                        patch.object(r.sys, 'argv', ['runner', '--repo', root, '--state', root]):
                    self.assertEqual(r.main(), 0)
                fail.assert_not_called()
                run.assert_not_called()
                self.assertFalse((Path(root) / 'STOP').exists())
            finally:
                owner.lock.close()

    def test_legacy_deferral_startup_refusal_preserves_full_ownership(self):
        with tempfile.TemporaryDirectory() as root:
            original=dict(current_plan=42,last_error=None,deleted_files=29797,deleted_bytes=2026235087172,
                          owner_id='00000000-0000-0000-0000-000000000001',
                          deferred_streams={'3':{'retry_after':0,'failures':1}})
            path=Path(root)/'status.json';path.write_text(json.dumps(original))
            job=FakeRunner(root);job.repo=Path(root)
            initialize=r.Runner.initialize;job.initialize=lambda:initialize(job)
            with patch.object(r,'Runner',return_value=job),patch.object(r.sys,'argv',['runner']), \
                 patch.object(r.signal,'signal'),patch.object(r.sys,'stderr',new=io.StringIO()):
                self.assertEqual(r.main(),1)
            stored=json.loads(path.read_text())
            for key in ('current_plan','deleted_files','deleted_bytes','deferred_streams'):self.assertEqual(stored[key],original[key])
            self.assertIn('prior deferred failure',stored['last_error']);self.assertNotIn('ON',job.events)
            self.assertTrue((Path(root)/'STOP').exists())

    def test_main_handles_other_startup_errors_through_fail_safe(self):
        for stage in ('initialize', 'active', 'load_policy', 'check_delivery'):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                error = OSError('fake-sensitive-error') if stage == 'initialize' else r.Halt('startup check failed')
                with patch.object(job, stage, side_effect=error), \
                        patch.object(r, 'Runner', return_value=job), \
                        patch.object(r.sys, 'argv', ['runner']), \
                        patch.object(r.signal, 'signal'), \
                        patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                    self.assertEqual(r.main(), 1)
                self.assertEqual(job.events[-2:], ['OFF', 'notify'])
                self.assertNotIn('ON', job.events)
                self.assertTrue((job.root / 'STOP').exists())
                self.assertNotIn('fake-sensitive', stderr.getvalue() + json.dumps(job.status))

    def test_manual_pause_during_recovery_preserves_ownership_and_stops(self):
        for deleted in (False, True):
            with self.subTest(deleted=deleted), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.status.update(current_plan=42, started_plan_sha256=job.p['plan_sha256'])
                job.p.update(mode='paused', approval_reason='auto-paused: approval expired; prior approval')
                if deleted:
                    job.p['results'][0].update(state='deleted', delete_ever_issued=True)
                def manual_pause(_):
                    job.events.append('recovery')
                    # A new server report, not a mutation of the already fetched snapshot.
                    job.p = dict(copy.deepcopy(job.p), approval_reason='manual pause')
                job.recovery = manual_pause
                job.run = lambda: job.finish(copy.deepcopy(job.p))
                with patch.object(r, 'Runner', return_value=job), \
                        patch.object(r.sys, 'argv', ['runner']), \
                        patch.object(r.signal, 'signal'), \
                        patch.object(r.sys, 'stderr', new=io.StringIO()):
                    self.assertEqual(r.main(), 1)
                self.assertEqual(job.events[:3], ['OFF', 'recovery', 'OFF'])
                self.assertIn('notify', job.events)
                self.assertNotIn('plans/42/close', job.events)
                self.assertEqual(job.status['current_plan'], 42)
                self.assertEqual(job.status['started_plan_sha256'], 'b'*64)
                self.assertEqual(job.picks, [42, None])
                self.assertTrue((job.root / 'STOP').exists())

    def test_source_lookup_must_resolve_manifest_and_catalog_union_once(self):
        for manifest, catalog, resolved in [
                ([1], [1, 2], [1]),  # JOIN silently dropped the catalog's missing clip.
                ([1, 2], [1, 2], [1]),
                ([1], [1], [1, 1]),
                ([1, 2], [1], [1]),
                ([1], [1], [1, 2])]:
            with self.subTest(manifest=manifest, catalog=catalog, resolved=resolved):
                p, cat = fixture()
                p['items'][0]['source_ids'] = manifest
                cat['items'][0]['source_ids'] = catalog
                clip = cat['items'][0]['sources'][0]
                cat['items'][0]['sources'] = [dict(clip, source_id=sid) for sid in resolved]
                with self.assertRaisesRegex(r.Halt, 'source catalog'):
                    r.verify(p, cat, WINDOWS, NOW)

    def test_concurrent_manual_pause_after_drain_never_closes_or_replans(self):
        for deleted in (False, True):
            with self.subTest(deleted=deleted), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.status['current_plan'] = 42
                job.p.update(mode='paused', approval_reason='auto-paused: approval expired; prior approval')
                def manual_pause(_):
                    job.p['approval_reason'] = 'manual pause'
                    if deleted:
                        job.p['results'][0].update(state='deleted', delete_ever_issued=True)
                job.drain = manual_pause
                with self.assertRaisesRegex(r.Halt, 'non-expiry'):
                    job.finish(copy.deepcopy(job.p))
                job.fail('plan paused for a non-expiry reason')
                self.assertEqual(job.events[:2], ['OFF', 'recovery'])
                self.assertIn('notify', job.events)
                self.assertNotIn('plans/42/close', job.events)
                self.assertEqual(job.picks, [42, None])
                self.assertTrue((job.root / 'STOP').exists())

    def test_disk_full_cannot_prevent_off_notification_or_independent_stop_write(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            def full(*args, **kwargs):
                job.events.append('save')
                raise OSError(errno.ENOSPC, 'test disk full')
            job.save = full
            with patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                job.fail('failure')
            self.assertEqual(job.events, ['save', 'OFF', 'notify', 'save'])
            self.assertTrue((job.root / 'STOP').exists())
            self.assertEqual(job.status['last_error'], 'failure')
            self.assertIn('failure state write failed', stderr.getvalue())

    def test_stop_write_failure_is_guarded_after_off_alert_and_state(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            with patch.object(r.os, 'link', side_effect=OSError(errno.ENOSPC, 'test disk full')), \
                    patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                job.fail('failure')
            self.assertEqual(job.events, ['OFF', 'notify'])
            self.assertEqual(json.loads((job.root / 'status.json').read_text())['last_error'], 'failure')
            self.assertIn('STOP write failed', stderr.getvalue())

    def test_healthy_saved_completed_enabled_plan_resumes_before_off(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.status.update(current_plan=42, started_plan_sha256=job.p['plan_sha256'])
            job.p.update(mode='enabled', master_deletion_enabled=True)
            job.p['results'][0].update(state='deleted', delete_ever_issued=True)
            job.stopping = True
            with patch.object(r, 'utcnow', return_value=NOW):
                job.run()
            self.assertEqual(job.events[:3], ['monitor', 'OFF', 'recovery'])

    def test_no_enabled_plan_off_barrier_precedes_policy_email_and_latched_error(self):
        for latched in (False, True):
            with self.subTest(latched=latched), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.status['last_error'] = 'prior failure' if latched else None
                def policy():
                    job.events.append('policy')
                    raise r.Halt('policy mismatch')
                job.load_policy = policy
                job.check_delivery = lambda: self.fail('email checked before policy')
                with self.assertRaises(r.Halt):
                    job.run()
                self.assertEqual(job.events, [] if latched else ['OFF', 'policy'])

    def test_main_shuts_down_unknown_or_unhealthy_enabled_plan_then_alerts_and_stops(self):
        mutations = [
            lambda j: j.status.update(current_plan=None),
            lambda j: j.status.update(current_plan=99),
            lambda j: j.status.pop('started_plan_sha256'),
            lambda j: j.status.update(started_plan_sha256='c'*64),
            lambda j: j.p.update(approved_until=(NOW-timedelta(seconds=1)).isoformat()),
            lambda j: j.p.update(approved_until=None),
            lambda j: j.p['results'][0].update(state='conflict'),
            lambda j: j.p.update(master_deletion_enabled=False),
            lambda j: j.status.update(last_error='prior failure'),
            lambda j: setattr(j, 'dry_run', True),
            lambda j: setattr(j, 'active', lambda: {'plans': [42], 'batches': 1}),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                job.status.update(current_plan=42, started_plan_sha256=job.p['plan_sha256'])
                job.p.update(mode='enabled', master_deletion_enabled=True)
                mutate(job)
                with patch.object(r, 'Runner', return_value=job), \
                        patch.object(r, 'utcnow', return_value=NOW), \
                        patch.object(r.sys, 'argv', ['runner']), \
                        patch.object(r.signal, 'signal'), \
                        patch.object(r.sys, 'stderr', new=io.StringIO()):
                    self.assertEqual(r.main(), 1)
                self.assertEqual(job.events[:2], ['notify'] if job.status.get('last_error') == 'prior failure' else ['OFF', 'notify'])
                self.assertNotIn('monitor', job.events)
                self.assertEqual((job.root / 'STOP').exists(), job.status.get('last_error') != 'prior failure')


class StreamNameTests(unittest.TestCase):
    def test_name_lookup_failure_keeps_progress_rows(self):
        job = r.Runner(Path('.'), Path('.'))
        rows = [dict(stream_id=3, files=1, bytes=2)]
        def fail(sql):
            raise r.Halt('read-only catalog query failed')
        job.query = fail
        job.add_stream_names(rows)
        self.assertEqual(rows, [dict(stream_id=3, files=1, bytes=2)])

    def test_name_lookup_adds_names(self):
        job = r.Runner(Path('.'), Path('.'))
        rows = [dict(stream_id=3, files=1, bytes=2), dict(stream_id=4, files=1, bytes=2)]
        job.query = lambda sql: {'3': 'Market square'}
        job.add_stream_names(rows)
        self.assertEqual(rows[0]['stream_name'], 'Market square')
        self.assertNotIn('stream_name', rows[1])


class LocalDeliveryTests(unittest.TestCase):
    def test_unset_and_empty_email_write_readable_notes_without_network(self):
        for env in ({}, {'CLEANUP_OWNER_EMAIL': ''}):
            with self.subTest(env=env), tempfile.TemporaryDirectory() as root, \
                    patch.dict(r.os.environ, env, clear=True), \
                    patch.object(r, 'request', side_effect=AssertionError('network forbidden')) as request:
                job = r.Runner(Path('.'), Path(root))
                job.check_delivery()
                job.notify('daily progress today')
                with patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                    job.alert('review required')
                notes = Path(root) / 'notes.jsonl'
                rows = [json.loads(line) for line in notes.read_text().splitlines()]
                self.assertEqual(len(rows), 3)
                self.assertIn('Deleted so far:', rows[1]['text'])
                self.assertIn('ACTION NEEDED: review required', rows[2]['subject'])
                self.assertIn('review required', stderr.getvalue())
                self.assertEqual(notes.stat().st_mode & 0o777, 0o600)
                request.assert_not_called()

    def test_startup_local_delivery_gates_enable_even_after_same_day_note(self):
        for broken in (False, True):
            with self.subTest(broken=broken), tempfile.TemporaryDirectory() as root, \
                    patch.dict(r.os.environ, {}, clear=True), \
                    patch.object(r, 'request', side_effect=AssertionError('network forbidden')), \
                    patch.object(r, 'utcnow', return_value=NOW), \
                    patch.object(r.sys, 'stderr', new=io.StringIO()):
                job = FakeRunner(root)
                job.status['last_note_day'] = NOW.date().isoformat()
                job.check_delivery = lambda: r.Runner.check_delivery(job)
                if broken:
                    (Path(root) / 'notes.jsonl').mkdir()
                    with self.assertRaisesRegex(r.Halt, 'owner note write failed'):
                        job.run()
                    self.assertNotIn('ON', job.events)
                    self.assertTrue(job.status['owner_notification_failed'])
                else:
                    job.run()
                    self.assertIn('ON', job.events)
                    self.assertTrue((Path(root) / 'notes.jsonl').is_file())

    def test_alert_survives_note_fsync_failure_and_still_emails(self):
        with tempfile.TemporaryDirectory() as root, \
                patch.dict(r.os.environ, {'CLEANUP_OWNER_EMAIL': 'owner@example.invalid',
                    'EMAIL_FROM': 'runner@example.invalid', 'EMAIL_RESEND_API_KEY': 'fake'}), \
                patch.object(r.os, 'fsync', side_effect=OSError('private failure')), \
                patch.object(r, 'request', return_value={'id': 'accepted'}) as request, \
                patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
            job = r.Runner(Path('.'), Path(root))
            job.alert('review required')
            self.assertTrue(job.status['owner_notification_failed'])
            self.assertIn('owner note write failed', stderr.getvalue())
            self.assertNotIn('private failure', stderr.getvalue())
            request.assert_called_once()


class EmailTests(unittest.TestCase):
    def status(self):
        return dict(phase='monitoring', current_plan=42, deleted_files=5841,
                    deleted_bytes=161500000000, deleted_24h_files=100,
                    deleted_24h_bytes=2000000000,
                    per_stream=[dict(stream_id=3, stream_name='Camera <north> & yard',
                                     files=5841, bytes=161500000000)])

    def send(self, status, reason, fail=False):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        job = r.Runner(Path('.'), Path(root.name))
        job.status = status
        with patch.dict(r.os.environ, {'EMAIL_FROM': 'runner@example.invalid',
                        'CLEANUP_OWNER_EMAIL': 'owner@example.invalid',
                        'EMAIL_RESEND_API_KEY': 'fake-test-key'}), \
                patch.object(r, 'utcnow', return_value=NOW), \
                patch.object(r, 'request', return_value={'id': 'test'}) as request:
            if fail:
                with patch.object(r, 'render_email', side_effect=ValueError('private exception')):
                    job.notify(reason)
            else:
                job.notify(reason)
        self.assertEqual(request.call_count, 1)
        return request.call_args.args[3]

    def test_daily(self):
        payload = self.send(self.status(), 'daily progress 2026-11-11')
        self.assertEqual(payload['subject'], 'Stoarama NAS cleanup — Nov 11: 5,841 files (161.5 GB) deleted so far')
        for body in (payload['text'], payload['html']):
            for value in ('deleting batch 42', 'Current plan: 42', '5,841', '161.5 GB', '100 files (2.0 GB)', 'R2 copies are kept'):
                self.assertIn(value, body)
        self.assertNotIn('<table', payload['text'])
        self.assertIn('<table', payload['html'])
        self.assertEqual(r.human_size(2000000000000), '2.0 TB')

    def test_alert(self):
        status = dict(self.status(), phase='master_false', last_error='R2 recovery verification failed')
        payload = self.send(status, 'STOPPED: owner review required')
        self.assertEqual(payload['subject'], 'ACTION NEEDED: STOPPED: owner review required')
        for body in (payload['text'], payload['html']):
            self.assertIn('What to do: reply or ask Claude', body)
            self.assertIn('Deletion is off.', body)
            self.assertIn(status['last_error'], body)
        payload = self.send(self.status(), 'MASTER OFF NOT CONFIRMED; retrying shutdown')
        self.assertIn('check shutdown confirmation', payload['text'])
        self.assertNotIn('Deletion is off.', payload['text'])

    def test_missing_fields_and_phases(self):
        payload = self.send({}, 'daily progress 2026-11-11')
        self.assertIn('Current plan: none', payload['text'])
        self.assertIn('No deleted files reported yet', payload['html'])
        self.assertIn('0 files (0.0 GB)', payload['subject'])
        for phase, words in [('dry_run', 'checking the next batch (no deletion)'),
                             ('master_false', 'idle, deletion switched off')]:
            self.assertIn(words, self.send({'phase': phase}, 'daily progress')['text'])

    def test_escaping_and_selected_fields_only(self):
        status = dict(self.status(), sql='SELECT private', token='private-token',
                      response='private-http-body', last_error='fixed <reason> & "detail"')
        payload = self.send(status, 'STOPPED: <fixed> & "reason"')
        self.assertIn('Camera &lt;north&gt; &amp; yard', payload['html'])
        self.assertIn('fixed &lt;reason&gt; &amp; &quot;detail&quot;', payload['html'])
        self.assertNotIn('<fixed>', payload['html'])
        for secret in ('SELECT private', 'private-token', 'private-http-body'):
            self.assertNotIn(secret, json.dumps(payload))
        status['per_stream'][0]['stream_name'] = 'https://example.invalid/?token=private'
        self.assertNotIn('https:', self.send(status, 'daily progress')['text'])

    def test_render_failure_sends_old_minimal_text(self):
        status = self.status()
        payload = self.send(status, 'STOPPED: owner review required', fail=True)
        self.assertEqual(json.loads(payload['text']), {k: status.get(k) for k in
                         ('phase', 'deleted_files', 'deleted_bytes', 'current_plan', 'last_error')})
        self.assertNotIn('html', payload)
        self.assertNotIn('private exception', json.dumps(payload))
        # Malformed optional data also falls back and sends once.
        payload = self.send(dict(status, per_stream=[{}]), 'daily progress')
        self.assertNotIn('html', payload)



class ReadFailureTests(unittest.TestCase):
    def job(self, root):
        job = r.Runner(Path('.'), Path(root))
        job.status['owner_id'] = '00000000-0000-0000-0000-000000000001'
        job.prepare = SimpleNamespace(pg_env=lambda: {'PGOPTIONS': '-c default_transaction_read_only=on'},
                                      transient_sql_failure=PREPARER.transient_sql_failure)
        return job

    def test_database_failures_and_nested_reads_stop_once(self):
        for code in ('57014','08006','53300','57P03','40001','40P01','55P03','42601',None):
            with self.subTest(code=code),tempfile.TemporaryDirectory() as root:
                job=self.job(root)
                bad=subprocess.TimeoutExpired(['psql'],330) if code is None else SimpleNamespace(returncode=1,stdout=b'',stderr=('ERROR:  '+code+' fake-sensitive').encode())
                with patch.object(r.subprocess,'run',side_effect=[bad,SimpleNamespace(returncode=0,stdout=b'7')]) as run, \
                     patch.object(r.time,'sleep') as sleep,patch.object(r.sys,'stderr',new=io.StringIO()) as stderr:
                    with self.assertRaises(r.Halt):job.read('outer',lambda:job.query('SELECT 7','progress query'))
                self.assertEqual(run.call_count,1);sleep.assert_not_called()
                self.assertNotIn('fake-sensitive',stderr.getvalue())

    def test_http_failures_never_replay_reads_or_writes(self):
        for method in ('GET','POST','PUT'):
            for code in (401,403,408,429,503):
                with self.subTest(method=method,code=code),tempfile.TemporaryDirectory() as root:
                    job=self.job(root)
                    error=r.urllib.error.HTTPError('https://invalid',code,'fake-sensitive',{},io.BytesIO(b'secret'))
                    opener=SimpleNamespace(open=unittest.mock.Mock(side_effect=[error,io.BytesIO(b'{}')]))
                    with patch.dict(os.environ,SERVICE_TOKEN='fixture'),patch.object(r.urllib.request,'build_opener',return_value=opener), \
                         patch.object(r.time,'sleep') as sleep,patch.object(r.sys,'stderr',new=io.StringIO()) as stderr:
                        with self.assertRaises(r.Halt):job.api(method,'plans/42')
                    self.assertEqual(opener.open.call_count,1);sleep.assert_not_called();self.assertNotIn('fake-sensitive',stderr.getvalue())

    def test_monitor_read_failure_reaches_failure_path_before_progress(self):
        with tempfile.TemporaryDirectory() as root:
            job=self.job(root);job.finish_health=lambda:None;job.owner_validated=True;job.status.update(current_plan=42,live_commit='d'*40)
            with patch.dict(os.environ,SERVICE_TOKEN='fixture'),patch.object(r,'request',side_effect=r.ReadFailure('HTTP transport failed (GET)')) as request, \
                 patch.object(job,'master') as master,patch.object(job,'alert'),patch.object(job,'drain'),patch.object(job,'recovery'), \
                 patch.object(job,'finish') as finish,patch.object(job,'progress') as progress,patch.object(r.time,'sleep') as sleep, \
                 patch.object(r.sys,'stderr',new=io.StringIO()):
                try:job.monitor(42)
                except r.Halt as exc:job.fail(str(exc))
            master.assert_called_once_with(False,emergency=True);finish.assert_not_called();progress.assert_not_called();sleep.assert_not_called()
            self.assertEqual(request.call_count,2)  # Failed monitor, then one read during fail-safe recovery.
            self.assertTrue((Path(root)/'STOP').exists());self.assertIsNotNone(job.status['last_error'])


class RecoveryRetryTests(unittest.TestCase):
    def summary(self, **changes):
        p,_=fixture()
        p['results'][0].update(delete_ever_issued=True)
        summary=dict(plan_id=42,plan_sha256=p['plan_sha256'],items=1,delete_ever_issued=1,
                     verified=1,problems=0,by_status={'ok':1},problem_items=[],checked_at=NOW.isoformat())
        summary.update(changes)
        return p,summary

    def test_recovery_read_failures_stop_without_retrying(self):
        for failure in ('timeout','get','head'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as root:
                p,summary=self.summary()
                good=SimpleNamespace(returncode=0,stdout=json.dumps(summary).encode())
                bad=SimpleNamespace(returncode=2,stdout=json.dumps(dict(summary,verified=0,problems=1,
                    by_status={'head_error':1},problem_items=[{'status':'head_error'}])).encode())
                first={'timeout':subprocess.TimeoutExpired(['ctl'],1800),
                       'get':SimpleNamespace(returncode=1,stdout=b'',stderr=b'fake-sensitive'), 'head':bad}[failure]
                job=r.Runner(Path('.'),Path(root))
                with patch.object(r.subprocess,'run',side_effect=[first,good]) as run, \
                        patch.object(r.time,'sleep') as sleep, patch.object(r.sys,'stderr',new=io.StringIO()):
                    with self.assertRaises(r.Halt):job.recovery(p)
                sleep.assert_not_called()
                self.assertEqual(run.call_args.kwargs['timeout'],1800)
                self.assertEqual(run.call_count,1);self.assertNotIn('recovery_issued',job.status)
                self.assertNotIn('fake-sensitive',(Path(root)/'runner.jsonl').read_text())

    def test_missing_mismatched_and_wrong_identity_recovery_never_retry_or_pass(self):
        for status in ('missing','size_mismatch','bucket_mismatch','incomplete_identity','wrong_plan','issued_race'):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as root:
                p,summary=self.summary()
                summary.update(verified=0,problems=1,by_status={status:1},problem_items=[{'status':status}])
                if status=='wrong_plan': summary.update(plan_id=99,by_status={'head_error':1},problem_items=[{'status':'head_error'}])
                if status=='issued_race': summary.update(delete_ever_issued=2)
                job=r.Runner(Path('.'),Path(root))
                with patch.object(r.subprocess,'run',return_value=SimpleNamespace(returncode=2,stdout=json.dumps(summary).encode())) as run, \
                        patch.object(r.time,'sleep') as sleep:
                    with self.assertRaises(r.Halt): job.recovery(p)
                sleep.assert_not_called()
                self.assertEqual(run.call_count,1)
                self.assertNotIn('recovery_issued',job.status)

    def test_exhausted_recovery_read_keeps_original_latch_off_and_stop(self):
        with tempfile.TemporaryDirectory() as root:
            job=FakeRunner(root)
            job.status['current_plan']=42
            job.recovery=lambda plan: r.Runner.recovery(job,plan)
            failed=SimpleNamespace(returncode=1,stdout=b'',stderr=b'fake-sensitive')
            with patch.object(r.subprocess,'run',return_value=failed) as run, \
                    patch.object(r.time,'sleep') as sleep,patch.object(r.sys,'stderr',new=io.StringIO()):
                job.fail('original failure')
            self.assertEqual(run.call_count,1)
            sleep.assert_not_called()
            self.assertEqual(job.status['last_error'],'original failure')
            self.assertEqual(job.status['recovery_error'],'recovery verification: recovery CLI read failed')
            self.assertTrue(job.status['shutdown_or_recovery_failed'])
            self.assertEqual(job.events[0],'OFF')
            self.assertTrue((Path(root)/'STOP').exists())
            self.assertEqual(job.status['phase'],'failed')
            self.assertNotIn('fake-sensitive',(Path(root)/'runner.jsonl').read_text())

    def test_failure_drains_before_fresh_report_and_recovery(self):
        with tempfile.TemporaryDirectory() as root:
            job=FakeRunner(root)
            job.status['current_plan']=42
            job.drain=lambda pid: job.events.append('drain')
            job.plan=lambda pid: job.events.append('plan') or job.p
            with patch.object(r.sys,'stderr',new=io.StringIO()): job.fail('original failure')
            self.assertEqual(job.events[:5],['OFF','notify','drain','plan','recovery'])
            self.assertEqual(job.status['last_error'],'original failure')
            self.assertTrue((Path(root)/'STOP').exists())


@unittest.skipUnless(os.environ.get('STOARAMA_TEST_DATABASE_URL'), 'local scratch Postgres required')
class SummaryQueryTests(unittest.TestCase):
    def test_picker_leaves_certified_stitch_plans_to_their_controller(self):
        psql = shutil.which('psql') or '/opt/homebrew/opt/postgresql@17/bin/psql'
        setup = """BEGIN;
        CREATE TEMP TABLE nas_cleanup_plans(id bigint,account_id bigint,connection_id bigint,manifest jsonb,
          mode text,report_sha256 text,label text);
        CREATE TEMP TABLE nas_cleanup_items(plan_id bigint,state text,delete_ever_issued boolean,updated_at timestamptz);
        INSERT INTO nas_cleanup_plans VALUES
          (1,47,13,'[{"permit_reason":"durham_stitch_certified"}]','dry_run','%s','certified'),
          (2,47,13,'[{"kind":"raw"}]','dry_run','%s','ordinary');
        INSERT INTO nas_cleanup_items VALUES (1,'would_delete',false,now()),(2,'would_delete',false,now());
        """ % (r.POLICY_SHA, r.POLICY_SHA)
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            def query(sql):
                output = subprocess.run(
                    [psql, '-XqAt', '-v', 'ON_ERROR_STOP=1', os.environ['STOARAMA_TEST_DATABASE_URL']],
                    input=(setup + sql + ';ROLLBACK;').encode(), capture_output=True, timeout=15, check=True,
                )
                return json.loads(output.stdout)
            job.query = query
            self.assertEqual(r.Runner.pick(job, lookahead=True), 2)

    def test_actual_sql_keeps_receipt_scope_budget_and_legacy_manifest_facts(self):
        psql=shutil.which('psql') or '/opt/homebrew/opt/postgresql@17/bin/psql'
        setup="""BEGIN;
        CREATE TEMP TABLE nas_cleanup_plans(id bigint,account_id bigint,connection_id bigint,manifest jsonb);
        CREATE TEMP TABLE nas_cleanup_items(plan_id bigint,ordinal integer,state text,delete_ever_issued boolean,updated_at timestamptz);
        INSERT INTO nas_cleanup_plans VALUES
          (1,47,13,'[{"stream_id":3,"size_bytes":100},{"stream_id":3,"size_bytes":200}]'),
          (2,47,13,'[{"stream_id":4,"size_bytes":300}]'),(3,99,14,'[{"stream_id":5,"size_bytes":400}]');
        INSERT INTO nas_cleanup_items VALUES
          (1,1,'deleted',true,now()),(1,2,'delete_issued',true,now()),
          (2,1,'deleted',true,now()-interval '25 hours'),(3,1,'deleted',true,now());
        """
        def query(sql,*args):
            proc=subprocess.run([psql,'-XqAt','-v','ON_ERROR_STOP=1',os.environ['STOARAMA_TEST_DATABASE_URL']],
                input=(setup+sql+';ROLLBACK;').encode(),capture_output=True,timeout=15,check=True)
            return json.loads(proc.stdout)
        with tempfile.TemporaryDirectory() as root:
            job=r.Runner(Path('.'),Path(root))
            job.query=query
            job.add_stream_names=lambda rows: None
            job.status['last_note_day']=r.utcnow().date().isoformat()
            job.progress()
            self.assertEqual(job.status['deleted_files'],2)
            self.assertEqual(job.status['deleted_bytes'],400)
            self.assertEqual(job.status['deleted_24h_files'],1)
            self.assertEqual(job.status['deleted_24h_bytes'],100)
            self.assertEqual(job.recent_bytes(),700)  # Includes uncertain issued receipts and other accounts, as before.

if __name__ == '__main__': unittest.main()
