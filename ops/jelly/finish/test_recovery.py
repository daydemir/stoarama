import json
import os
from pathlib import Path
import subprocess
import sys
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
