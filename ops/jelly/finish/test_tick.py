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
    Path(sys.argv[sys.argv.index('--status')+1]).write_text('{"queue_depth":10000}')
''')
                darma = root / 'darma'
                darma.write_text('#!/bin/sh\necho "$*" > "' + str(root / 'notification') + '"\n')
                for executable in (collector, cli, darma):
                    executable.chmod(0o700)
                result = subprocess.run([sys.executable, str(wrapper), '--cli', str(cli), '--state', str(root / 'state'),
                                         '--safety-collector', str(collector), '--notify-agent', 'deniz',
                                         '--refresh-picks', str(root/'picks.json'), '--expected-refresh-picks-sha256', 'a'*64],
                                        env={**os.environ, 'PATH': str(root) + ':' + os.environ['PATH']}, capture_output=True)
                self.assertEqual(result.returncode, 0 if success else 1)
                self.assertNotIn(b'SECRET', result.stdout + result.stderr)
                self.assertEqual((root / 'notification').exists(), not success)
                self.assertEqual((root / 'state/status.json').exists(), success)

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
                                        env={**os.environ, 'PATH': str(root) + ':' + os.environ['PATH']}, capture_output=True)
                self.assertEqual(result.returncode, 1)
                notification = (root / 'notification').read_text()
                self.assertNotIn('copy_mismatch_latched', notification)
                self.assertIn('current_status_unavailable' if failure == 'status' else 'dependency_failed', notification)


if __name__ == '__main__':
    unittest.main()
