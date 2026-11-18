import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import admission
import reconcile
import refresh_recovery as recovery
import tick


class AdmissionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.report = {'plan_id': 1440, 'plan_sha256': 'a' * 64, 'delete_ever_issued': 2,
                       'verified': 2, 'problems': 0, 'checked_at': '2026-11-17T07:00:00Z'}

    def invoke(self, module, run):
        argv = ['wrapper', '--cli', 'cli', '--state', str(self.state), '--notify-agent', 'controller']
        argv += ['--safety-collector', 'collector'] if module is tick else ['--plan-source', 'source']
        with patch.object(sys, 'argv', argv), patch.object(subprocess, 'run', side_effect=run) as mocked, \
                patch.object(module, 'notify'):
            result = module.main()
        return result, mocked.call_count

    def ok(self, command, **kwargs):
        if command[0] == 'source':
            return subprocess.CompletedProcess(command, 0, stdout=b'{"plan_id":1440}')
        if 'verify-recovery' in command:
            kwargs['stdout'].write(json.dumps(self.report).encode())
        elif 'tick' in command:
            Path(command[command.index('--status') + 1]).write_text('{"schema":"stoarama.finish-status.v1","updated_at":"2026-11-17T07:00:00Z","imported_items":3}')
        else:
            kwargs['stdout'].write(b'{}')
        return subprocess.CompletedProcess(command, 0)

    def assert_blocked(self):
        before = {str(p.relative_to(self.state)): p.read_bytes() for p in self.state.rglob('*') if p.is_file()}
        for module in (tick, recovery):
            self.assertEqual(self.invoke(module, self.ok), (1, 0))
        after = {str(p.relative_to(self.state)): p.read_bytes() for p in self.state.rglob('*') if p.is_file()}
        # Creating the other lane's flock is harmless; journal/evidence/status never change.
        self.assertEqual(before, {k: v for k, v in after.items() if k in before})

    def test_partial_import_then_next_tick_never_retries(self):
        def partial(command, **kwargs):
            if 'tick' in command:
                Path(command[command.index('--status') + 1]).write_text('{"stop_reason":"manifest_import_failed","imported_items":2}')
                return subprocess.CompletedProcess(command, 1)
            return self.ok(command, **kwargs)
        self.assertEqual(self.invoke(tick, partial)[0], 1)
        evidence = next(self.state.glob('tick-*/status.json'))
        self.assertEqual(json.loads(evidence.read_text())['imported_items'], 2)
        self.assertEqual(json.loads((self.state / 'status.json').read_text())['stop_reason'], 'manifest_import_failed')
        self.assert_blocked()

    def test_dependency_timeout_and_missing_status_stay_latched(self):
        for mode in ('timeout', 'missing', 'cli-timeout', 'bad-shape'):
            with self.subTest(mode=mode):
                # Each case uses an isolated state, every subprocess is mocked.
                with tempfile.TemporaryDirectory() as temp:
                    self.state = Path(temp)
                    def failure(command, **kwargs):
                        if mode == 'timeout' or (mode == 'cli-timeout' and 'tick' in command):
                            raise subprocess.TimeoutExpired(command, 90)
                        if 'tick' in command:
                            if mode == 'bad-shape':
                                Path(command[command.index('--status') + 1]).write_text('[]')
                            return subprocess.CompletedProcess(command, 0)
                        return self.ok(command, **kwargs)
                    self.assertEqual(self.invoke(tick, failure)[0], 1)
                    self.assert_blocked()

    def test_recovery_problems_cannot_be_replaced_by_zero(self):
        self.report['problems'] = 1
        self.report['verified'] = 1
        self.assertEqual(self.invoke(recovery, self.ok)[0], 1)
        self.report['problems'] = 0
        self.assert_blocked()
        self.assertEqual(json.loads((self.state / 'recovery.json').read_text())['problems'], 1)
        self.assertFalse((self.state / 'recovery-last-good.json').exists())

    def test_reconcile_binds_actual_artifacts_and_resumes_recovery_first(self):
        self.invoke(tick, lambda *a, **kw: (_ for _ in ()).throw(subprocess.TimeoutExpired('collector', 90)))
        digest = admission.binding(self.state)
        journal = admission.load(self.state)
        authority, evidence = self.state / 'authority.json', self.state / 'evidence.json'
        authority.write_text(json.dumps({'controller': '886efa32', 'authority': 'new root GO', 'latch_sha256': digest}))
        evidence.write_text(json.dumps({'latch_sha256': digest, 'queue_reconciled': True,
                                       'reconciled_attempts': list(journal['active'].values())}))
        argv = ['reconcile', '--state', str(self.state), '--expected-latch-sha256', 'f' * 64,
                '--authority', str(authority), '--evidence', str(evidence)]
        with patch.object(sys, 'argv', argv), patch.object(subprocess, 'run') as run:
            self.assertEqual(reconcile.main(), 1)
            self.assertTrue(admission.load(self.state)['failed'])
            argv[4] = digest
            self.assertEqual(reconcile.main(), 0)
            run.assert_not_called()
        receipt = json.loads(next((self.state / 'reconciliations').iterdir()).read_text())
        self.assertEqual(receipt['controller'], '886efa32')
        self.assertEqual(receipt['evidence_sha256'], hashlib.sha256(evidence.read_bytes()).hexdigest())
        self.assertEqual(self.invoke(tick, self.ok), (1, 0))
        self.assertEqual(self.invoke(recovery, self.ok)[0], 0)
        self.assertEqual(self.invoke(tick, self.ok)[0], 0)
        self.assertTrue(next(self.state.glob('tick-*/failure.json')).exists())

    def test_interruption_and_failed_latch_write_leave_active_stop(self):
        class Interrupted(BaseException):
            pass
        def interrupted(command, **kwargs):
            if 'tick' in command:
                Path(command[command.index('--status') + 1]).write_text('{"imported_items":2}')
                raise Interrupted()
            return self.ok(command, **kwargs)
        with self.assertRaises(Interrupted):
            self.invoke(tick, interrupted)
        self.assertEqual(self.invoke(tick, self.ok), (1, 0))
        self.assert_blocked()
        self.assertTrue(admission.load(self.state)['failed'])

    def test_failure_write_failure_still_cannot_retry(self):
        original = admission.publish
        def full(path, record):
            if record.get('failed'):
                raise OSError('disk full')
            original(path, record)
        with patch.object(admission, 'publish', side_effect=full):
            self.assertEqual(self.invoke(tick, lambda *a, **kw: (_ for _ in ()).throw(OSError()))[0], 1)
        self.assertEqual(self.invoke(tick, self.ok), (1, 0))
        self.assert_blocked()

    def test_cross_lane_failure_prevents_inflight_success_publication(self):
        # Model either order with both real lane locks held, as in the wrappers.
        import fcntl
        for failed_lane in ('tick', 'recovery'):
            with self.subTest(failed_lane=failed_lane), tempfile.TemporaryDirectory() as temp:
                state = Path(temp)
                with (state / 'lock').open('a') as tick_lock, (state / 'recovery.lock').open('a') as recovery_lock:
                    for lock in (tick_lock, recovery_lock):
                        fcntl.flock(lock, fcntl.LOCK_EX)
                    attempts = {lane: admission.Attempt(state, lane) for lane in admission.LANES}
                    other = 'recovery' if failed_lane == 'tick' else 'tick'
                    self.assertFalse(attempts[failed_lane].finish('dependency_failed', {}))
                    self.assertFalse(attempts[other].finish('', self.report))
                    self.assertIn('stop_reason', json.loads((state / 'status.json').read_text()))
                    self.assertNotIn('problems', json.loads((state / 'recovery.json').read_text()))

    def test_first_install_preserves_legacy_failure(self):
        for filename, record in (('status.json', {'stop_reason': 'manifest_import_failed'}),
                                 ('recovery.json', {'problems': 1})):
            with self.subTest(filename=filename), tempfile.TemporaryDirectory() as temp:
                self.state = Path(temp)
                original = json.dumps(record)
                (self.state / filename).write_text(original)
                self.assertEqual(self.invoke(tick, self.ok), (1, 0))
                self.assertEqual((self.state / filename).read_text(), original)
                self.assertEqual(json.loads(next(self.state.glob('legacy-*/*')).read_text()), record)
