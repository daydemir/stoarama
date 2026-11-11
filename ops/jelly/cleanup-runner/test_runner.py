import copy
import errno
import io
from datetime import timedelta
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('runner', Path(__file__).with_name('runner.py'))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
NOW = r.timestamp('2026-11-11T12:00:00+00:00')
WINDOWS = {3: (r.timestamp('2026-09-01T00:00:00+00:00'), r.timestamp('2026-09-29T00:00:00+00:00'))}


def fixture():
    item = dict(kind='raw', source_id=1, stream_id=3, size_bytes=100,
                sha256='a'*64, start_at='2026-08-01T00:00:00+00:00',
                end_at='2026-08-01T01:00:00+00:00', object_key='clips/1',
                bucket='stoarama', relative_path='stream/1.mp4', source_ids=[1])
    row = dict(ordinal=1, state='would_delete', recovery='r2_md5_etag_matched',
               delete_ever_issued=False, updated_at=NOW.isoformat())
    p = dict(id=42, items=[item], results=[row], mode='dry_run', policy_current=True,
             policy_sha256=r.POLICY_SHA, plan_sha256='b'*64, allow_nas_only=False,
             master_deletion_enabled=False, approval_reason='', approved_until=(NOW+timedelta(hours=24)).isoformat())
    c = copy.deepcopy(item)
    c['sources'] = [dict(source_id=1, stream_id=3, bucket='stoarama', start_at=item['start_at'], end_at=item['end_at'])]
    return p, dict(items=[c], protected_overlap=0)


class DecisionTests(unittest.TestCase):
    def test_budget_boundaries_and_invalid_values(self):
        self.assertTrue(r.budget_permits(r.DAY_CAP - r.PLAN_CAP, r.PLAN_CAP))
        for recent, proposed in [(r.DAY_CAP-99, 100), (-1, 100), (0, 0), (0, r.PLAN_CAP+1)]:
            self.assertFalse(r.budget_permits(recent, proposed))

    def test_boundary_is_outside_but_overlap_is_kept(self):
        self.assertTrue(r.outside(3, '2026-08-31T23:00:00Z', '2026-09-01T00:00:00Z', WINDOWS))
        self.assertTrue(r.outside(3, '2026-09-29T00:00:00Z', '2026-09-29T01:00:00Z', WINDOWS))
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


class FakeRunner(r.Runner):
    """Exercise orchestration without any network, DB or subprocess access."""
    def __init__(self, root, dry_run=False):
        self.root = Path(root)
        self.status = {'current_plan': None, 'last_error': None}
        self.dry_run = dry_run
        self.stopping = False
        self.lock = None
        self.windows = WINDOWS
        self.events = []
        self.p, self.cat = fixture()
        self.picks = [42, None]

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
    def pick(self): return self.picks.pop(0)
    def await_dry(self, pid): return self.p
    def master(self, enabled, emergency=False):
        self.events.append('ON' if enabled else 'OFF')
    def api(self, method, path, body=None):
        self.events.append(path)
        if path.endswith('/control'):
            self.assert_enable = body
            self.p['mode'] = 'enabled'
        return {'mode': 'retired'}
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
    def test_batch_order_and_approval_digest(self):
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            job.run()
            self.assertEqual([e for e in job.events if e != 'idle'], ['OFF', 'ON', 'plans/42/control', 'monitor', 'OFF', 'recovery'])
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
            job.active = lambda: {'plans': [42], 'batches': 0}
            job.live = lambda: {'commit': {'id': 'd'*40}}
            def complete(_):
                job.p['mode'] = 'paused'
                job.p['approval_reason'] = 'completed: all items deleted'
                job.p['results'][0].update(state='deleted', delete_ever_issued=True)
            with patch.object(r.time, 'sleep', side_effect=complete):
                r.Runner.monitor(job, 42)
            self.assertEqual(job.events, ['OFF', 'recovery'])

    def test_deleted_anomalous_pause_still_stops(self):
        p, _ = fixture()
        p['mode'] = 'paused'
        p['approval_reason'] = 'auto-paused: conflicting duplicate receipt'
        p['results'][0].update(state='deleted', delete_ever_issued=True)
        self.assertEqual(r.execution_state(p), 'anomaly')

    def test_live_rejects_deployment_in_progress(self):
        job = r.Runner.__new__(r.Runner)
        job.render = lambda *args: [{'deploy': {'status': 'build_in_progress'}}]
        with self.assertRaises(r.Halt): job.live()

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
            self.assertEqual(job.events, ['OFF', 'notify', 'save'])
            self.assertTrue((job.root / 'STOP').exists())
            self.assertEqual(job.status['last_error'], 'failure')
            self.assertIn('failure state write failed', stderr.getvalue())

    def test_stop_write_failure_is_guarded_after_off_alert_and_state(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            with patch.object(Path, 'touch', side_effect=OSError(errno.ENOSPC, 'test disk full')), \
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
                self.assertEqual(job.events, ['OFF'] if latched else ['OFF', 'policy'])

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
                self.assertEqual(job.events[:2], ['OFF', 'notify'])
                self.assertNotIn('monitor', job.events)
                self.assertTrue((job.root / 'STOP').exists())


class RenderOffTests(unittest.TestCase):
    def make_render(self, job, failures=(), enabled=False):
        # Model Render's env snapshots: PUT changes configuration, never the live process.
        self.calls = []
        self.config = 'false' if enabled else 'true'
        self.deploys = {
            'old': dict(id='old', status='live', commit={'id': 'd'*40}, env=self.config),
            'on': dict(id='on', status='canceled' if enabled else 'build_in_progress', commit={'id': 'd'*40}, env='true')}
        self.failures = list(failures)
        self.created = []
        def render(method, path, body=None):
            self.calls.append((method, path, body))
            if method == 'PUT':
                self.assertEqual(path, '/env-vars/NAS_CLEANUP_DELETION_ENABLED')
                self.assertEqual(body, {'value': 'true' if enabled else 'false'})
                if self.failures and self.failures[0] == 'put':
                    self.failures.pop(0)
                    raise r.Halt('HTTP request failed (PUT)')
                self.config = body['value']
                return {}
            if path == '/deploys?limit=100':
                return [{'deploy': copy.deepcopy(d)} for d in self.deploys.values()]
            if path.endswith('/cancel'):
                self.deploys[path.split('/')[2]]['status'] = 'canceled'
                return {}
            if method == 'POST' and path == '/deploys':
                self.assertEqual(body, {'commitId': 'd'*40, 'clearCache': 'do_not_clear'})
                pid = 'off-' + str(len(self.created))
                self.created.append(pid)
                self.deploys[pid] = dict(id=pid, status='created', commit={'id': body['commitId']}, env=self.config)
                return {'id': pid}
            if path.startswith('/deploys/'):
                d = self.deploys[path.split('/')[2]]
                if self.failures and self.failures[0] == 'deploy':
                    self.failures.pop(0)
                    d['status'] = 'build_failed'
                else:
                    for previous in self.deploys.values():
                        if previous['status'] == 'live': previous['status'] = 'deactivated'
                    d['status'] = 'live'
                    if self.failures and self.failures[0] == 'runtime':
                        self.failures.pop(0)
                        d['env'] = 'true'
                return copy.deepcopy(d)
            if path == '/env-vars/NAS_CLEANUP_DELETION_ENABLED':
                return {'value': self.config}
            raise AssertionError('unexpected Render route')
        job.render = render
        job.status['current_plan'] = 42
        def plan(pid):
            self.calls.append(('runtime', pid, None))
            return dict(job.p, master_deletion_enabled=next(
                d['env'] == 'true' for d in self.deploys.values() if d['status'] == 'live'))
        job.plan = plan
        job.live = lambda: r.Runner.live(job)

    def test_startup_unreadable_ownership_confirms_off_then_alerts_and_stops(self):
        for failure in ('json', 'read', 'blocking_read', 'shape'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                self.make_render(job)
                state = Path(root) / 'status.json'
                state.write_text('fake-sensitive-corrupt-json' if failure == 'json' else '[]')
                read_text = Path.read_text
                def read(path, *args, **kwargs):
                    if path == state and failure in {'read', 'blocking_read'}:
                        error = PermissionError if failure == 'read' else BlockingIOError
                        raise error('fake-sensitive-read-error')
                    return read_text(path, *args, **kwargs)
                def notify(subject):
                    self.assertEqual(self.deploys['off-0']['status'], 'live')
                    self.assertEqual(self.deploys['off-0']['env'], 'false')
                    self.assertIn(('runtime', 42, None), self.calls)
                    job.events.append(subject)
                with patch.object(Path, 'read_text', new=read), \
                        patch.object(r.Runner, 'render', side_effect=job.render), \
                        patch.object(r.Runner, 'plan', side_effect=job.plan), \
                        patch.object(r.Runner, 'query', return_value=42), \
                        patch.object(r.Runner, 'notify', side_effect=notify), \
                        patch.object(r, 'request', side_effect=AssertionError('network forbidden')), \
                        patch.object(r.time, 'sleep', side_effect=AssertionError('unexpected retry')), \
                        patch.object(r.sys, 'argv', ['runner', '--repo', root, '--state', root]), \
                        patch.object(r.signal, 'signal'), \
                        patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                    self.assertEqual(r.main(), 1)
                self.assertEqual(job.events, ['STOPPED: owner review required'])
                self.assertTrue((Path(root) / 'STOP').exists())
                status = json.loads(state.read_text())
                self.assertIn('unknown enabled plan', status['last_error'])
                self.assertNotIn('fake-sensitive', stderr.getvalue() + json.dumps(status))

    def test_off_cancels_inflight_on_and_verifies_new_live_env_without_disk(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            self.make_render(job)
            with patch.object(job, 'save', side_effect=OSError(errno.ENOSPC, 'disk full')), \
                    patch.object(r.time, 'sleep', side_effect=AssertionError('unexpected retry')):
                r.Runner.master(job, False, emergency=True)
            self.assertEqual(self.calls[:4], [
                ('PUT', '/env-vars/NAS_CLEANUP_DELETION_ENABLED', {'value': 'false'}),
                ('GET', '/deploys?limit=100', None),
                ('POST', '/deploys/on/cancel', None),
                ('POST', '/deploys', {'commitId': 'd'*40, 'clearCache': 'do_not_clear'})])
            self.assertEqual(self.deploys['on']['status'], 'canceled')
            self.assertEqual(self.deploys['old']['status'], 'deactivated')
            self.assertEqual(self.deploys['off-0']['status'], 'live')
            self.assertEqual(self.deploys['off-0']['env'], 'false')
            self.assertIn(('runtime', 42, None), self.calls)

    def test_off_retries_put_failed_deploy_and_wrong_runtime_with_backoff_and_alerts(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            self.make_render(job, ['put', 'put', 'deploy', 'runtime'])
            with patch.object(r.time, 'sleep', side_effect=[None]*4) as sleep, \
                    patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                r.Runner.master(job, False, emergency=True)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [15, 30, 60, 120])
            self.assertEqual(job.events.count('notify'), 4)
            self.assertEqual(stderr.getvalue().count('MASTER OFF NOT CONFIRMED'), 4)
            self.assertEqual(len(self.created), 3)
            self.assertEqual(self.deploys[self.created[-1]]['env'], 'false')
            self.assertEqual(self.deploys[self.created[-1]]['status'], 'live')

    def test_off_does_not_silently_exit_when_email_and_render_stay_unavailable(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.render = lambda *args: (_ for _ in ()).throw(r.Halt('HTTP request failed'))
            job.notify = lambda *args: (_ for _ in ()).throw(r.Halt('email failed'))
            class EndTest(BaseException): pass
            with patch.object(r.time, 'sleep', side_effect=[None]*7 + [EndTest]) as sleep, \
                    patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                with self.assertRaises(EndTest):
                    r.Runner.master(job, False, emergency=True)
            self.assertEqual([call.args[0] for call in sleep.call_args_list], [15, 30, 60, 120, 240, 300, 300, 300])
            self.assertEqual(stderr.getvalue().count('MASTER OFF NOT CONFIRMED'), 8)
            self.assertEqual(stderr.getvalue().count('owner email failed'), 8)

    def test_off_polls_new_deploy_and_retries_timeout(self):
        for timeout in (False, True):
            with self.subTest(timeout=timeout), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                self.make_render(job)
                render = job.render
                queued = [True]
                def delayed(method, path, body=None):
                    if method == 'GET' and path == '/deploys/off-0' and queued:
                        queued.pop()
                        return dict(self.deploys['off-0'], status='queued')
                    return render(method, path, body)
                job.render = delayed
                clock = [0, 1801, 2000] if timeout else [0, 1]
                with patch.object(r.time, 'monotonic', side_effect=clock), \
                        patch.object(r.time, 'sleep', side_effect=[None]) as sleep, \
                        patch.object(r.sys, 'stderr', new=io.StringIO()):
                    r.Runner.master(job, False, emergency=True)
                sleep.assert_called_once_with(15)
                self.assertEqual(len(self.created), 1)
                self.assertEqual(job.events.count('notify'), int(timeout))

    def inject_reads(self, job, target, errors, enabled=False):
        render, plan = job.render, job.plan
        remaining = list(errors)
        def fail():
            if remaining:
                raise remaining.pop(0)
        def flaky_render(method, path, body=None):
            if method == 'GET':
                runtime_read = any(c[0] == 'runtime' for c in self.calls)
                matches = (
                    target == 'list' and path == '/deploys?limit=100' and not self.created
                    or target == 'status' and path.startswith('/deploys/')
                    or target == 'live' and path == '/deploys?limit=100' and self.created and not runtime_read
                    or target == 'final_live' and path == '/deploys?limit=100' and runtime_read
                    or target == 'env' and path == '/env-vars/NAS_CLEANUP_DELETION_ENABLED')
                if matches:
                    fail()
            return render(method, path, body)
        def flaky_plan(pid):
            if target == 'plan':
                fail()
            return plan(pid)
        def flaky_query(sql):
            if target == 'query':
                fail()
            return {'plans': [], 'batches': 0} if enabled else 42
        job.render, job.plan, job.query = flaky_render, flaky_plan, flaky_query
        if enabled:
            job.idle = lambda: r.Runner.idle(job)
            job.active = lambda: r.Runner.active(job)
        elif target == 'query':
            job.status['current_plan'] = None
        return remaining

    def transient_errors(self):
        errors = [r.urllib.error.HTTPError('https://example.invalid', code, 'fake-sensitive-body', {}, io.BytesIO())
                  for code in (429, 503)]
        for error in errors:
            self.addCleanup(error.close)
        return errors + [TimeoutError('fake-sensitive-timeout')]

    def test_off_transient_reads_during_redeploy_wait_converge(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            self.make_render(job)
            render = job.render
            replies = [dict(id='off-0', status='build_in_progress')]
            replies.extend(self.transient_errors())
            def delayed(method, path, body=None):
                if method == 'GET' and path == '/deploys/off-0' and replies:
                    reply = replies.pop(0)
                    if isinstance(reply, Exception):
                        raise reply
                    return reply
                return render(method, path, body)
            job.render = delayed
            with patch.object(r.time, 'sleep') as sleep, \
                    patch.object(r.sys, 'stderr', new=io.StringIO()):
                r.Runner.master(job, False)
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [15, 5, 10, 20])
            self.assertEqual(self.created, ['off-0'])
            self.assertEqual(job.status['phase'], 'master_false')
            self.assertEqual(job.events, [])

    def test_off_retries_each_read_without_recreating_or_canceling_its_false_deploy(self):
        for target in ('list', 'status', 'live', 'env', 'plan', 'query', 'final_live'):
            for error in self.transient_errors():
                with self.subTest(target=target, error=getattr(error, 'code', type(error).__name__)), tempfile.TemporaryDirectory() as root:
                    job = FakeRunner(root)
                    self.make_render(job)
                    remaining = self.inject_reads(job, target, [error, error])
                    with patch.object(r.time, 'sleep') as sleep, \
                            patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                        r.Runner.master(job, False)
                    self.assertEqual(remaining, [])
                    self.assertEqual([c.args[0] for c in sleep.call_args_list], [5, 10])
                    self.assertEqual(self.created, ['off-0'])
                    self.assertEqual([c[1] for c in self.calls if c[0] == 'POST' and c[1].endswith('/cancel')], ['/deploys/on/cancel'])
                    self.assertEqual(self.deploys['off-0']['env'], 'false')
                    self.assertEqual(self.deploys['off-0']['status'], 'live')
                    self.assertEqual(job.status['phase'], 'master_false')
                    self.assertEqual(job.events, [])
                    log = (Path(root) / 'runner.jsonl').read_text()
                    self.assertEqual(len(log.splitlines()), 2)
                    self.assertIn('STEP FAILED: OFF', stderr.getvalue())
                    self.assertNotIn('fake-sensitive', log + stderr.getvalue())

    def test_off_resumes_same_deploy_after_exhausted_reads_at_every_verification_step(self):
        for target in ('status', 'live', 'env', 'plan', 'query', 'final_live'):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as root:
                job = FakeRunner(root)
                self.make_render(job)
                remaining = self.inject_reads(job, target, [TimeoutError('private')] * 7)
                with patch.object(r.time, 'sleep') as sleep, \
                        patch.object(r.sys, 'stderr', new=io.StringIO()):
                    r.Runner.master(job, False)
                self.assertEqual(remaining, [])
                self.assertEqual([c.args[0] for c in sleep.call_args_list], [5, 10, 20, 30, 30, 25, 15])
                self.assertEqual(job.events, ['notify'])
                self.assertEqual(self.created, ['off-0'])
                self.assertEqual([c[1] for c in self.calls if c[0] == 'POST' and c[1].endswith('/cancel')], ['/deploys/on/cancel'])
                self.assertEqual(sum(c[0] == 'PUT' for c in self.calls), 2)
                self.assertEqual(job.status['phase'], 'master_false')

    def test_persistent_off_status_failure_keeps_alerting_and_never_cancels_own_deploy(self):
        class EndTest(BaseException): pass
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            self.make_render(job)
            self.inject_reads(job, 'status', [TimeoutError('fake-sensitive')] * 14)
            def sleep(delay):
                if job.events.count('notify') == 2:
                    raise EndTest
            with patch.object(r.time, 'sleep', side_effect=sleep), \
                    patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                with self.assertRaises(EndTest):
                    r.Runner.master(job, False)
            self.assertEqual(job.events, ['notify', 'notify'])
            self.assertEqual(stderr.getvalue().count('MASTER OFF NOT CONFIRMED'), 2)
            self.assertEqual(self.created, ['off-0'])
            self.assertEqual(self.deploys['off-0']['status'], 'created')
            self.assertNotEqual(job.status.get('phase'), 'master_false')
            self.assertEqual([c[1] for c in self.calls if c[0] == 'POST' and c[1].endswith('/cancel')], ['/deploys/on/cancel'])
            rows = [json.loads(line) for line in (Path(root) / 'runner.jsonl').read_text().splitlines()]
            self.assertEqual(len(rows), 14)
            self.assertTrue(all(row['step'] == 'OFF deploy status GET' for row in rows))
            self.assertNotIn('fake-sensitive', stderr.getvalue() + json.dumps(rows))

    def test_on_retries_reads_without_replaying_mutations(self):
        for target in ('query', 'list', 'status', 'live', 'env', 'plan'):
            for error in self.transient_errors():
                with self.subTest(target=target, error=getattr(error, 'code', type(error).__name__)), tempfile.TemporaryDirectory() as root:
                    job = FakeRunner(root)
                    self.make_render(job, enabled=True)
                    remaining = self.inject_reads(job, target, [error, error], enabled=True)
                    with patch.object(r.time, 'sleep') as sleep, \
                            patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                        r.Runner.master(job, True)
                    self.assertEqual(remaining, [])
                    self.assertEqual([c.args[0] for c in sleep.call_args_list], [5, 10])
                    self.assertEqual(len(self.created), 1)
                    self.assertEqual(sum(c[0] == 'PUT' for c in self.calls), 1)
                    self.assertFalse(any(c[0] == 'POST' and c[1].endswith('/cancel') for c in self.calls))
                    self.assertEqual(job.status['phase'], 'master_true')
                    self.assertEqual(self.deploys[self.created[0]]['env'], 'true')
                    self.assertIn('STEP FAILED: ON', stderr.getvalue())

    def test_persistent_on_read_failure_propagates_without_recreating_deploy(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            self.make_render(job, enabled=True)
            self.inject_reads(job, 'status', [TimeoutError('fake-sensitive')] * 7, enabled=True)
            with patch.object(r.time, 'sleep') as sleep, \
                    patch.object(r.sys, 'stderr', new=io.StringIO()) as stderr:
                with self.assertRaises(TimeoutError):
                    r.Runner.master(job, True)
            self.assertEqual(sum(c.args[0] for c in sleep.call_args_list), 120)
            self.assertEqual(len(self.created), 1)
            self.assertEqual(stderr.getvalue().count('STEP FAILED: ON deploy status GET'), 7)
            self.assertNotIn('fake-sensitive', stderr.getvalue() + (Path(root) / 'runner.jsonl').read_text())

    def test_off_email_and_retry_survive_broken_stderr(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            self.make_render(job, ['put'])
            with patch.object(r.sys, 'stderr') as stderr, \
                    patch.object(r.time, 'sleep', side_effect=[None]):
                stderr.write.side_effect = OSError(errno.ENOSPC, 'test log disk full')
                r.Runner.master(job, False, emergency=True)
            self.assertEqual(job.events, ['notify'])
            self.assertEqual(self.deploys['off-0']['status'], 'live')

    def test_startup_without_saved_plan_checks_live_runtime_using_existing_report(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            self.make_render(job)
            job.status['current_plan'] = None
            job.query = lambda sql: 42
            with patch.object(r.time, 'sleep', side_effect=AssertionError('unexpected retry')):
                r.Runner.master(job, False, emergency=True)
            self.assertIn(('runtime', 42, None), self.calls)


class EmailTests(unittest.TestCase):
    def status(self):
        return dict(phase='monitoring', current_plan=42, deleted_files=5841,
                    deleted_bytes=161500000000, deleted_24h_files=100,
                    deleted_24h_bytes=2000000000,
                    per_stream=[dict(stream_id=3, stream_name='Camera <north> & yard',
                                     files=5841, bytes=161500000000)])

    def send(self, status, reason, fail=False):
        job = r.Runner(Path('.'), Path('.'))
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


if __name__ == '__main__': unittest.main()
