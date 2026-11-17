import importlib.util
import json
import os
from pathlib import Path
import subprocess
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
    Path(sys.argv[sys.argv.index('--status')+1]).write_text('{"queue_depth":10000}')
''')
                darma = root / 'darma'
                darma.write_text('#!/bin/sh\necho "$*" > "' + str(root / 'notification') + '"\n')
                for executable in (collector, cli, darma):
                    executable.chmod(0o700)
                result = subprocess.run(['python3', str(wrapper), '--cli', str(cli), '--state', str(root / 'state'),
                                         '--safety-collector', str(collector), '--notify-agent', 'deniz'],
                                        env={**os.environ, 'PATH': str(root) + ':' + os.environ['PATH']}, capture_output=True)
                self.assertEqual(result.returncode, 0 if success else 1)
                self.assertNotIn(b'SECRET', result.stdout + result.stderr)
                self.assertEqual((root / 'notification').exists(), not success)
                self.assertEqual((root / 'state/status.json').exists(), success)


if __name__ == '__main__':
    unittest.main()
