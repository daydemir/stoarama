import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class RecoveryRefreshTest(unittest.TestCase):
    def test_preserves_canonical_timestamp_and_publishes_failure(self):
        script = Path(__file__).with_name('refresh_recovery.py')
        for success in (True, False):
            with self.subTest(success=success), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                source = root / 'source'
                source.write_text('#!/bin/sh\necho \'{"plan_id":1440}\'\n')
                record = {'plan_id': 1440, 'plan_sha256': 'a' * 64, 'delete_ever_issued': 2,
                          'verified': 2, 'problems': 0, 'checked_at': '2026-11-17T07:00:00Z',
                          'problem_items': [{'error': 'SECRET'}]}
                cli = root / 'cli'
                cli.write_text('#!/bin/sh\n' + ("echo '" + json.dumps(record) + "'\n" if success else 'echo SECRET >&2\nexit 1\n'))
                darma = root / 'darma'
                darma.write_text('#!/bin/sh\necho "$*" > "' + str(root / 'notification') + '"\n')
                for executable in (source, cli, darma):
                    executable.chmod(0o700)
                result = subprocess.run(['python3', str(script), '--cli', str(cli), '--state', str(root / 'state'),
                                         '--plan-source', str(source), '--notify-agent', 'deniz'],
                                        env={**os.environ, 'PATH': str(root) + ':' + os.environ['PATH']}, capture_output=True)
                self.assertEqual(result.returncode, 0 if success else 1)
                saved = (root / 'state/recovery.json').read_text()
                self.assertNotIn('SECRET', saved)
                self.assertNotIn(b'SECRET', result.stdout + result.stderr)
                self.assertEqual((root / 'notification').exists(), not success)
                if success:
                    self.assertEqual(json.loads(saved)['checked_at'], record['checked_at'])
                else:
                    self.assertEqual(json.loads(saved), {'refresh_error': 'recovery_refresh_failed'})


if __name__ == '__main__':
    unittest.main()
