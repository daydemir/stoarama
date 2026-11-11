import copy
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
             master_deletion_enabled=False, approval_reason='')
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
        self.windows = WINDOWS
        self.events = []
        self.p, self.cat = fixture()
        self.picks = [42, None]

    def load_policy(self): pass
    def check_delivery(self): pass
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
        self.finish(self.p, 'done')
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
            self.assertEqual(job.events, ['idle', 'OFF'])
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
            job.status['current_plan'] = 42
            job.p['mode'] = 'enabled'
            job.picks = [None]
            job.stopping = True
            job.run()
            self.assertEqual(job.events[:3], ['monitor', 'OFF', 'recovery'])

    def test_expiry_closes_only_after_off_and_recovery(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.status['current_plan'] = 42
            job.p['mode'] = 'paused'
            job.p['approval_reason'] = 'auto-paused: approval expired; prior approval'
            job.finish(job.p, 'expired')
            self.assertEqual(job.events, ['OFF', 'recovery', 'plans/42/close'])

    def test_anomaly_never_closes_or_replans(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.p['mode'] = 'paused'
            with self.assertRaises(r.Halt): job.finish(job.p, 'anomaly')
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
            with self.assertRaises(r.Halt): job.finish(job.p, 'expired')
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

    def test_render_redeploy_is_single_key_and_current_live_sha(self):
        with tempfile.TemporaryDirectory() as root:
            job = FakeRunner(root)
            job.live = lambda: {'id': 'dep-current', 'commit': {'id': 'd'*40}}
            calls = []
            def render(method, path, body=None):
                calls.append((method, path, body))
                if path == '/deploys' and method == 'POST': return {'id': 'dep-current'}
                if path == '/deploys/dep-current': return {'status': 'live', 'commit': {'id': 'd'*40}}
                if path.startswith('/env-vars/'): return {'value': 'false'}
                self.fail('unexpected Render route')
            job.render = render
            r.Runner.master(job, False)
            self.assertEqual(calls, [
                ('PUT', '/env-vars/NAS_CLEANUP_DELETION_ENABLED', {'value': 'false'}),
                ('POST', '/deploys', {'commitId': 'd'*40, 'clearCache': 'do_not_clear'}),
                ('GET', '/deploys/dep-current', None),
                ('GET', '/env-vars/NAS_CLEANUP_DELETION_ENABLED', None)])

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


if __name__ == '__main__': unittest.main()
