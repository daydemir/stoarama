import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).parent
DEADMAN = HERE / 'deadman.py'
NOW = '2026-11-17T12:00:00Z'
STATUS_PATH = '/remote/finish/status.json'


def status(updated_at=NOW, **extra):
    record = {'schema': 'stoarama.finish-status.v1', 'updated_at': updated_at, 'queue_depth': 10}
    record.update(extra)
    return json.dumps(record)


class DeadmanTest(unittest.TestCase):
    def setUp(self):
        self.stack = tempfile.TemporaryDirectory()
        self.root = Path(self.stack.name)
        self.state = self.root / 'state'
        self.state.mkdir()
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.fixture = self.root / 'fixture.json'
        self.ssh_code = self.root / 'ssh-code'
        self.ssh_sleep = self.root / 'ssh-sleep'
        self.notifications = self.root / 'notifications'
        self.darma_fail = self.root / 'darma-fail'
        ssh = self.bin / 'ssh'
        ssh.write_text('''#!/bin/sh
if [ -s "{sleep}" ]; then sleep "$(cat "{sleep}")"; fi
if [ -s "{code}" ]; then printf '%s\\n' 'SECRET-HOST-DETAIL' >&2; exit "$(cat "{code}")"; fi
cat "{fixture}"
'''.format(sleep=self.ssh_sleep, code=self.ssh_code, fixture=self.fixture))
        darma = self.bin / 'darma'
        darma.write_text('''#!/bin/sh
if [ -e "{fail}" ]; then printf '%s\\n' 'SECRET-DARMA-DETAIL' >&2; exit 1; fi
printf '%s\\n' "$*" >> "{log}"
'''.format(fail=self.darma_fail, log=self.notifications))
        ssh.chmod(0o700)
        darma.chmod(0o700)

    def tearDown(self):
        self.stack.cleanup()

    def run_deadman(self, now=NOW, extra=(), fixture=None, ssh_code=None, ssh_sleep=None, darma_fail=False):
        if fixture is not None:
            self.fixture.write_text(fixture)
        for path, value in ((self.ssh_code, ssh_code), (self.ssh_sleep, ssh_sleep)):
            path.unlink(missing_ok=True)
            if value is not None:
                path.write_text(str(value))
        self.darma_fail.unlink(missing_ok=True)
        if darma_fail:
            self.darma_fail.write_text('')
        command = [sys.executable, str(DEADMAN), '--ssh-target', 'jelly', '--status-path', STATUS_PATH,
                   '--state', str(self.state), '--notify-agent', 'deniz']
        if now is not None:
            command += ['--now', now]
        command += list(extra)
        result = subprocess.run(command, capture_output=True, text=True,
                                env={**os.environ, 'PATH': str(self.bin) + ':' + os.environ['PATH']})
        return result

    def sent(self):
        return self.notifications.read_text().splitlines() if self.notifications.exists() else []

    def test_ok_is_silent(self):
        result = self.run_deadman(fixture=status())
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), 'deadman ok')
        self.assertEqual(self.sent(), [])
        self.assertEqual(json.loads((self.state / 'deadman.json').read_text())['condition'], 'ok')

    def test_stale_alerts_once_then_reminds(self):
        stale = status('2026-11-17T11:00:00Z')
        first = self.run_deadman(fixture=stale)
        self.assertEqual(first.returncode, 1)
        self.assertEqual(first.stdout.strip(), 'deadman stale')
        self.assertEqual(len(self.sent()), 1)
        self.assertIn('Stoarama finish dead-man: stale since 2026-11-17T12:00:00Z', self.sent()[0])
        self.assertIn('deniz', self.sent()[0])
        quiet = self.run_deadman(now='2026-11-17T12:05:00Z', fixture=stale)
        self.assertEqual(quiet.returncode, 1)
        self.assertEqual(len(self.sent()), 1)
        reminded = self.run_deadman(now='2026-11-17T18:00:00Z', fixture=stale)
        self.assertEqual(reminded.returncode, 1)
        self.assertEqual(len(self.sent()), 2)
        self.assertIn('since 2026-11-17T12:00:00Z', self.sent()[1])

    def test_recovery_message_once(self):
        stale = status('2026-11-17T11:00:00Z')
        self.run_deadman(fixture=stale)
        recovered = self.run_deadman(now='2026-11-17T12:10:00Z', fixture=status('2026-11-17T12:09:00Z'))
        self.assertEqual(recovered.returncode, 0)
        self.assertEqual(len(self.sent()), 2)
        self.assertIn('recovered (tick fresh) after stale.', self.sent()[1])
        again = self.run_deadman(now='2026-11-17T12:20:00Z', fixture=status('2026-11-17T12:19:00Z'))
        self.assertEqual(again.returncode, 0)
        self.assertEqual(len(self.sent()), 2)

    def test_stopped_known_and_unknown_reason(self):
        for reason, detail in (('copy_mismatch_latched', 'copy_mismatch_latched'),
                               ('something_new', 'unknown:something_new')):
            with self.subTest(reason=reason):
                self.tearDown()
                self.setUp()
                result = self.run_deadman(fixture=status(stop_reason=reason))
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout.strip(), 'deadman stopped ' + detail)
                self.assertIn('dead-man: stopped (' + detail + ')', self.sent()[0])

    def test_stopped_wins_over_stale_age(self):
        result = self.run_deadman(fixture=status('2020-01-01T00:00:00Z', stop_reason='database_unavailable'))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout.strip(), 'deadman stopped database_unavailable')

    def test_malformed_shapes(self):
        cases = {
            'not json': 'not json at all SECRET-PAYLOAD',
            'array': '[1, 2]',
            'schema': json.dumps({'schema': 'other', 'updated_at': NOW}),
            'timestamp': status(updated_at='2026-11-17 12:00:00'),
            'stop type': status(stop_reason=7),
        }
        for name, fixture in cases.items():
            with self.subTest(case=name):
                self.tearDown()
                self.setUp()
                result = self.run_deadman(fixture=fixture)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stdout.startswith('deadman malformed '))
                self.assertNotIn('SECRET-PAYLOAD', result.stdout + result.stderr)
                self.assertEqual(len(self.sent()), 1)

    def test_future_timestamp(self):
        result = self.run_deadman(fixture=status('2026-11-17T12:05:00Z'))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout.strip(), 'deadman future')

    def test_missing_status_file(self):
        result = self.run_deadman(ssh_code=1)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout.strip(), 'deadman missing')
        self.assertNotIn('SECRET-HOST-DETAIL', result.stdout + result.stderr)

    def test_unreachable_alerts_only_on_second_run(self):
        first = self.run_deadman(ssh_code=255)
        self.assertEqual(first.returncode, 1)
        self.assertEqual(first.stdout.strip(), 'deadman unreachable')
        self.assertEqual(self.sent(), [])
        second = self.run_deadman(now='2026-11-17T12:05:00Z', ssh_code=255)
        self.assertEqual(second.returncode, 1)
        self.assertEqual(len(self.sent()), 1)
        self.assertIn('dead-man: unreachable since 2026-11-17T12:00:00Z', self.sent()[0])
        third = self.run_deadman(now='2026-11-17T12:10:00Z', ssh_code=255)
        self.assertEqual(len(self.sent()), 1)
        okay = self.run_deadman(now='2026-11-17T12:15:00Z', fixture=status('2026-11-17T12:14:00Z'))
        self.assertEqual(okay.returncode, 0)
        self.assertIn('recovered (tick fresh) after unreachable.', self.sent()[-1])

    def test_unreachable_after_alerted_stale_alerts_on_second_run(self):
        # A previous condition's alert timestamp must not carry into the new one.
        self.run_deadman(fixture=status('2026-11-17T11:00:00Z'))
        self.assertEqual(len(self.sent()), 1)
        first_unreachable = self.run_deadman(now='2026-11-17T12:01:00Z', ssh_code=255)
        self.assertEqual(first_unreachable.returncode, 1)
        self.assertEqual(len(self.sent()), 1)
        second_unreachable = self.run_deadman(now='2026-11-17T12:02:00Z', ssh_code=255)
        self.assertEqual(second_unreachable.returncode, 1)
        self.assertEqual(len(self.sent()), 2)
        self.assertIn('dead-man: unreachable', self.sent()[1])

    def test_condition_change_alerts_immediately(self):
        self.run_deadman(fixture=status('2026-11-17T11:00:00Z'))
        self.assertEqual(len(self.sent()), 1)
        stopped = self.run_deadman(now='2026-11-17T12:01:00Z',
                                  fixture=status('2026-11-17T12:01:00Z', stop_reason='database_unavailable'))
        self.assertEqual(stopped.returncode, 1)
        self.assertEqual(stopped.stdout.strip(), 'deadman stopped database_unavailable')
        self.assertEqual(len(self.sent()), 2)
        self.assertIn('dead-man: stopped (database_unavailable)', self.sent()[1])

    def test_ssh_timeout_is_unreachable(self):
        for run in (1, 2):
            with self.subTest(run=run):
                result = self.run_deadman(fixture=status(), ssh_sleep=5, extra=['--ssh-timeout', '1'])
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout.strip(), 'deadman unreachable')
                self.assertEqual(len(self.sent()), 1 if run == 2 else 0)

    def test_timestamp_precision(self):
        for updated_at in ('2026-11-17T11:59:00.123456789Z', '2026-11-17T11:59:00Z', '2026-11-17T11:59:00.5Z'):
            with self.subTest(updated_at=updated_at):
                self.tearDown()
                self.setUp()
                result = self.run_deadman(fixture=status(updated_at=updated_at))
                self.assertEqual(result.returncode, 0)
                self.assertEqual(result.stdout.strip(), 'deadman ok')

    def test_notification_failure_exits_2_and_retries(self):
        stale = status('2026-11-17T11:00:00Z')
        failed = self.run_deadman(fixture=stale, darma_fail=True)
        self.assertEqual(failed.returncode, 2)
        self.assertEqual(failed.stdout.strip(), 'deadman notification_failed')
        self.assertNotIn('SECRET-DARMA-DETAIL', failed.stdout + failed.stderr)
        recorded = json.loads((self.state / 'deadman.json').read_text())
        self.assertIsNone(recorded['last_alert_at'])
        retried = self.run_deadman(now='2026-11-17T12:05:00Z', fixture=stale)
        self.assertEqual(retried.returncode, 1)
        self.assertEqual(len(self.sent()), 1)

    def test_relative_status_path_rejected(self):
        result = subprocess.run([sys.executable, str(DEADMAN), '--ssh-target', 'jelly',
                                 '--status-path', 'status.json', '--state', str(self.state),
                                 '--notify-agent', 'deniz'],
                                capture_output=True, text=True,
                                env={**os.environ, 'PATH': str(self.bin) + ':' + os.environ['PATH']})
        self.assertEqual(result.returncode, 2)
        self.assertIn('must be absolute', result.stderr)
        self.assertEqual(self.sent(), [])


if __name__ == '__main__':
    unittest.main()
