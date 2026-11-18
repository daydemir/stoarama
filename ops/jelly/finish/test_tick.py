from cli_binding_fixture import environment as binding_environment
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class WrapperTest(unittest.TestCase):
    def test_success_and_dependency_stop_notify_without_leaking(self):
        wrapper = Path(__file__).with_name('tick.py')
        for success in (True, False):
            with self.subTest(success=success), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                collector = root / 'collector'
                collector.write_text('#!/bin/sh\n' + ('echo "{}"\n' if success else 'echo SECRET >&2\nexit 1\n'))
                cli = root / 'cli'
                cli.write_text('''#!/usr/bin/env python3
import json,sys
from pathlib import Path
if sys.argv[1:]==['nas-r2-copy','status','--json']:
    print('{}')
else:
    assert sys.argv[sys.argv.index('--refresh-picks')+1].endswith('picks.json')
    assert sys.argv[sys.argv.index('--expected-refresh-picks-sha256')+1]=='a'*64
    Path(sys.argv[sys.argv.index('--status')+1]).write_text('{"schema":"stoarama.finish-status.v1","updated_at":"2026-11-17T07:00:00Z","queue_depth":10000}')
''')
                darma = root / 'darma'
                darma.write_text('#!/bin/sh\necho "$*" > "' + str(root / 'notification') + '"\n')
                for executable in (collector, cli, darma):
                    executable.chmod(0o700)
                result = subprocess.run([sys.executable, str(wrapper), '--cli', str(cli), '--state', str(root / 'state'),
                                         '--safety-collector', str(collector), '--notify-agent', 'deniz',
                                         '--refresh-picks', str(root/'picks.json'), '--expected-refresh-picks-sha256', 'a'*64],
                                        env={**os.environ, **binding_environment(root, cli), 'PATH': str(root) + ':' + os.environ['PATH']}, capture_output=True)
                self.assertEqual(result.returncode, 0 if success else 1)
                self.assertNotIn(b'SECRET', result.stdout + result.stderr)
                self.assertEqual((root / 'notification').exists(), not success)
                self.assertTrue((root / 'state/status.json').exists())

    def test_state_failure_and_stale_status_alert_current_failure(self):
        wrapper = Path(__file__).with_name('tick.py')
        for failure in ('state', 'lock', 'status'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                state = root / 'state'
                if failure == 'state':
                    state.write_text('not a directory')
                else:
                    state.mkdir()
                    (state / 'status.json').write_text('{"stop_reason":"copy_mismatch_latched"}')
                    if failure == 'lock':
                        (state / 'lock').mkdir()
                collector = root / 'collector'
                collector.write_text('#!/bin/sh\necho "{}"\n')
                cli = root / 'cli'
                cli.write_text('#!/bin/sh\nif [ "$1" = nas-r2-copy ]; then echo "{}"; else exit 1; fi\n')
                darma = root / 'darma'
                darma.write_text('#!/bin/sh\necho "$*" > "' + str(root / 'notification') + '"\n')
                for executable in (collector, cli, darma):
                    executable.chmod(0o700)
                result = subprocess.run([sys.executable, str(wrapper), '--cli', str(cli), '--state', str(state),
                                         '--safety-collector', str(collector), '--notify-agent', 'deniz'],
                                        env={**os.environ, **binding_environment(root, cli), 'PATH': str(root) + ':' + os.environ['PATH']}, capture_output=True)
                self.assertEqual(result.returncode, 1)
                if failure == 'status':
                    self.assertFalse((root / 'notification').exists())
                    self.assertEqual(json.loads((state / 'status.json').read_text())['stop_reason'], 'copy_mismatch_latched')
                    continue
                notification = (root / 'notification').read_text()
                self.assertNotIn('copy_mismatch_latched', notification)
                self.assertIn('current_status_unavailable' if failure == 'status' else 'dependency_failed', notification)


class StderrTailTest(unittest.TestCase):
    def setUp(self):
        sys.path.insert(0, str(Path(__file__).parent))
        import tick
        self.tick = tick

    def test_failed_attempt_retains_bounded_redacted_tail(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            code = "import sys;sys.stderr.write('x'*100000+'\\npostgres://user:SECRET@host/db\\n'+'cause=statement_timeout tier=1 sqlstate=57014\\n');sys.exit(1)"
            rc = self.tick.run_cli([sys.executable, '-c', code], path)
            self.assertEqual(rc, 1)
            tail = json.loads((path / 'cli-stderr.json').read_text())
            self.assertTrue(tail['truncated'])
            self.assertLessEqual(len(tail['tail'].encode()), self.tick.STDERR_TAIL_BYTES)
            self.assertNotIn('SECRET', tail['tail'])
            self.assertIn('sqlstate=57014', tail['tail'])
            self.assertEqual((path / 'cli-stderr.json').stat().st_mode & 0o777, 0o600)

    def test_timeout_still_retains_tail(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            with self.assertRaises(subprocess.TimeoutExpired):
                self.tick.run_cli([sys.executable, '-c', "import sys,time;print('before_timeout',file=sys.stderr,flush=True);time.sleep(10)"], path, timeout=0.2)
            self.assertIn('before_timeout', json.loads((path / 'cli-stderr.json').read_text())['tail'])


if __name__ == '__main__':
    unittest.main()
