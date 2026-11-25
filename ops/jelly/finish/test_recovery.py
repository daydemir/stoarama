from cli_binding_fixture import environment as binding_environment
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from cli_exec_guard import guard_test_module


def setUpModule():
    guard_test_module()


class RecoveryRefreshTest(unittest.TestCase):
    def test_preserves_canonical_timestamp_and_publishes_failure(self):
        script = Path(__file__).with_name('refresh_recovery.py')
        for success in (True, False):
            with self.subTest(success=success), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                source = root / 'source'
                source.write_text('#!/bin/sh\necho \'{"plan_id":1440}\'\n')
                record = {'plan_id': 1440, 'plan_sha256': 'a' * 64, 'delete_ever_issued': 2,
                          'verified': 2, 'problems': 0, 'checked_at': '2026-11-17T07:00:00.123456789Z',
                          'problem_items': [{'error': 'SECRET'}]}
                cli = root / 'cli'
                cli.write_text('#!/bin/sh\n' + ("echo '" + json.dumps(record) + "'\n" if success else 'echo SECRET >&2\nexit 1\n'))
                darma = root / 'darma'
                darma.write_text('#!/bin/sh\necho "$*" > "' + str(root / 'notification') + '"\n')
                for executable in (source, cli, darma):
                    executable.chmod(0o700)
                result = subprocess.run([sys.executable, str(script), '--cli', str(cli), '--state', str(root / 'state'),
                                         '--plan-source', str(source), '--notify-agent', 'deniz'],
                                        env={**os.environ, **binding_environment(root, cli), 'PATH': str(root) + ':' + os.environ['PATH']}, capture_output=True)
                self.assertEqual(result.returncode, 0 if success else 1)
                saved = (root / 'state/recovery.json').read_text()
                self.assertNotIn('SECRET', saved)
                self.assertNotIn(b'SECRET', result.stdout + result.stderr)
                self.assertEqual((root / 'notification').exists(), not success)
                if success:
                    self.assertEqual(json.loads(saved)['checked_at'], record['checked_at'])
                else:
                    self.assertEqual(json.loads(saved), {'refresh_error': 'recovery_refresh_failed'})

class RecoveryInvalidationTest(unittest.TestCase):
    def test_old_success_is_invalidated_for_malformed_sources_and_full_disk(self):
        import refresh_recovery as refresh
        from unittest.mock import patch
        for failure in ('null-source', 'list-source', 'null-report', 'list-report', 'publish'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp:
                state=Path(temp)
                canonical=state/'recovery.json'
                canonical.write_text('{"checked_at":"old success"}')
                def run(command, **kwargs):
                    if command[0]=='source':
                        raw=b'[]' if failure=='list-source' else (b'{"plan_id":1440}' if failure.endswith('report') else b'null')
                        return subprocess.CompletedProcess(command,0,stdout=raw)
                    kwargs['stdout'].write(b'null' if failure=='null-report' else b'[]')
                    return subprocess.CompletedProcess(command,0)
                with patch('sys.argv',['refresh','--cli','cli','--state',temp,'--plan-source','source','--notify-agent','deniz']), \
                     patch.object(refresh.subprocess,'run',side_effect=run), patch.object(refresh,'notify') as notify:
                    if failure=='publish':
                        with patch.object(refresh,'publish',side_effect=OSError('full disk')):
                            self.assertEqual(refresh.main(),1)
                        self.assertFalse(canonical.exists())
                    else:
                        self.assertEqual(refresh.main(),1)
                        self.assertIn('refresh_error',json.loads(canonical.read_text()))
                    notify.assert_called_once()


REPORT = {'plan_id': 1440, 'plan_sha256': 'a' * 64, 'delete_ever_issued': 2,
          'verified': 2, 'problems': 0, 'checked_at': '2026-11-17T07:00:00Z'}


def transient(kind):
    return lambda command, **kwargs: subprocess.CompletedProcess(command, 75, stdout=json.dumps({'transient': kind}).encode())


def plan_ok(command, **kwargs):
    return subprocess.CompletedProcess(command, 0, stdout=b'{"plan_id":1440}')


def report(record, returncode=0):
    def verify(command, **kwargs):
        kwargs['stdout'].write(record if isinstance(record, bytes) else json.dumps(record).encode())
        return subprocess.CompletedProcess(command, returncode)
    return verify


class TransientRecoveryTest(unittest.TestCase):
    """Transient plan-source failures retry, then hold without latching; findings still latch."""
    def setUp(self):
        import refresh_recovery
        self.refresh = refresh_recovery
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.old = json.dumps(dict(REPORT, checked_at='2026-11-17T01:00:00Z'))  # an old success
        (self.state / 'recovery.json').write_text(self.old)

    def invoke(self, *sources, verify=report(REPORT)):
        from unittest.mock import patch
        calls = []
        def run(command, **kwargs):
            if command[0] == 'source':
                calls.append(command)
                return sources[min(len(calls), len(sources)) - 1](command, **kwargs)
            return verify(command, **kwargs)
        with patch('sys.argv', ['refresh', '--cli', 'cli', '--state', str(self.state), '--plan-source', 'source',
                                '--notify-agent', 'deniz']), \
                patch.object(self.refresh.subprocess, 'run', side_effect=run), \
                patch.object(self.refresh, 'bound_launcher', side_effect=Path), \
                patch.object(self.refresh.time, 'sleep') as sleep, patch.object(self.refresh, 'notify') as notify:
            code = self.refresh.main()
        return code, len(calls), [c.args[0] for c in sleep.call_args_list], [c.args[1] for c in notify.call_args_list]

    def admission(self):
        return json.loads((self.state / 'admission.json').read_text())

    def recovery(self):
        return json.loads((self.state / 'recovery.json').read_text())

    def test_each_transient_class_retries_then_holds_without_latching(self):
        from admission import Transient
        for kind in Transient:
            with self.subTest(kind=kind):
                self.setUp()
                code, tries, sleeps, notes = self.invoke(transient(kind.value))
                self.assertEqual((code, tries, sleeps), (75, 3, [120, 360]))
                self.assertEqual(self.admission(), {'failed': False, 'active': {}})
                self.assertEqual((self.state / 'recovery.json').read_text(), self.old,
                                 'the old report keeps its own checked_at and 7 h limit; never restamped')
                self.assertEqual(notes, [], 'no data, no finding: the eventual stale stop alerts')
                self.assertEqual([p for p in self.state.iterdir() if p.is_dir()], [])
                self.assertFalse((self.state / 'recovery-last-good.json').exists())
                self.assertEqual(self.invoke(transient(kind.value))[::3], (75, []))
                self.assertFalse(self.admission()['failed'])

    def test_plan_source_and_verifier_timeouts_hold(self):
        def hang(command, **kwargs):
            raise subprocess.TimeoutExpired(command, 30)
        self.assertEqual(self.invoke(hang)[:3], (75, 3, [120, 360]))
        self.assertEqual((self.state / 'recovery.json').read_text(), self.old)
        self.setUp()
        self.assertEqual(self.invoke(plan_ok, verify=hang)[:3], (75, 1, []))
        self.assertFalse(self.admission()['failed'])
        self.assertEqual((self.state / 'recovery.json').read_text(), self.old)

    def test_transient_then_success_refreshes_and_reconciled_hold_stays_required(self):
        (self.state / 'admission.json').write_text(json.dumps({'failed': False, 'active': {}, 'recovery_required': True}))
        self.assertEqual(self.invoke(transient('dns'))[0], 75)
        self.assertTrue(self.admission()['recovery_required'], 'a hold never satisfies a human reconcile')
        self.assertEqual(self.invoke(transient('dns'), transient('network'), plan_ok)[:3], (0, 3, [120, 360]))
        self.assertEqual(self.recovery(), REPORT)
        self.assertEqual(self.admission(), {'failed': False, 'active': {}, 'recovery_required': False})

    def test_substantive_and_unclassified_failures_latch_without_retry(self):
        def failed(command, **kwargs):
            return subprocess.CompletedProcess(command, 1, stdout=b'')
        def denied(command, **kwargs):
            raise PermissionError()
        def removed(kind):  # data-derived classes removed by the lean fallback
            return lambda command, **kwargs: subprocess.CompletedProcess(command, 75, stdout=json.dumps({'transient': kind}).encode())
        def raw75(stdout):
            return lambda command, **kwargs: subprocess.CompletedProcess(command, 75, stdout=stdout)
        cases = {
            'plan source finding (exit 1)': ((failed,), report(REPORT), 'refresh_error'),
            'unclassified exception': ((denied,), report(REPORT), 'refresh_error'),
            'unknown transient class': ((raw75(b'{"transient":"bogus"}'),), report(REPORT), 'refresh_error'),
            'nas_report_lagging class': ((removed('nas_report_lagging'),), report(REPORT), 'refresh_error'),
            'master_off class': ((removed('master_off'),), report(REPORT), 'refresh_error'),
            'recovery_held class': ((removed('recovery_held'),), report(REPORT), 'refresh_error'),
            'runner_transition class': ((removed('runner_transition'),), report(REPORT), 'refresh_error'),
            'malformed transient record': ((raw75(b'{"transient":"dns","plan_id":1}'),), report(REPORT), 'refresh_error'),
            'empty transient record': ((raw75(b''),), report(REPORT), 'refresh_error'),
            'problems over 0': ((plan_ok,), report(dict(REPORT, problems=1, verified=1), 2), 'problems'),
            'verified differs from issued': ((plan_ok,), report(dict(REPORT, verified=1)), 'refresh_error'),
            'verifier failed (no report)': ((plan_ok,), report(b'', 1), 'refresh_error'),
            'malformed report': ((plan_ok,), report(b'[]'), 'refresh_error'),
            'oversize report': ((plan_ok,), report(b' ' * (2 * 1024 * 1024 + 1)), 'refresh_error'),
            'invalid digest': ((plan_ok,), report(dict(REPORT, plan_sha256='x')), 'refresh_error'),
            'wrong plan': ((plan_ok,), report(dict(REPORT, plan_id=1441)), 'refresh_error'),
        }
        for name, (sources, verify, key) in cases.items():
            with self.subTest(name):
                self.setUp()
                code, tries, sleeps, notes = self.invoke(*sources, verify=verify)
                self.assertEqual((code, tries, sleeps), (1, 1, []))
                self.assertTrue(self.admission()['failed'])
                self.assertIn(key, self.recovery())
                self.assertEqual(len(notes), 1)
                self.assertNotIn('transient', notes[0])


class CollectorEntryTest(unittest.TestCase):
    def test_only_a_transient_failure_exits_75_with_its_class(self):
        import collector
        import contextlib
        import io
        from unittest.mock import patch
        from admission import Transient, TransientFailure
        for error, code, out in ((TransientFailure(Transient.DNS), 75, '{"transient": "dns"}\n'),
                                 (ValueError('evidence unavailable'), 1, ''), (KeyError('SECRET'), 1, '')):
            stdout, stderr = io.StringIO(), io.StringIO()
            with self.subTest(error=error), patch.object(collector, 'main', side_effect=error), \
                    contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                self.assertEqual(collector.entry(), code)
            self.assertEqual(stdout.getvalue(), out)
            self.assertNotIn('SECRET', stdout.getvalue() + stderr.getvalue())


class WireTimestampTest(unittest.TestCase):
    def test_all_go_fraction_widths_on_supported_python(self):
        import refresh_recovery as refresh
        refresh.validate_checked_at('2026-11-17T07:00:00Z')
        for width in range(1, 10):
            with self.subTest(width=width):
                refresh.validate_checked_at('2026-11-17T07:00:00.' + '123456789'[:width] + 'Z')
                refresh.validate_checked_at('2026-11-17T07:00:00.' + '123456789'[:width] + '+00:00')
        for value in (None, '2026-11-17T07:00:00', '2026-11-17T07:00:00.1234567890Z'):
            with self.assertRaises(ValueError):
                refresh.validate_checked_at(value)


if __name__ == '__main__':
    unittest.main()
