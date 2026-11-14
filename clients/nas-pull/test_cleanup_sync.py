"""Count real C/Python fsyncs, including SQLite FULL-WAL commits, per group."""
import json
import os
import platform
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class CleanupSyncTests(unittest.TestCase):
    def test_eight_file_group_sync_budget(self):
        source = Path(__file__).parent
        mac = platform.system() == 'Darwin'
        with tempfile.TemporaryDirectory() as scratch:
            library = str(Path(scratch)/('count_fsync.dylib' if mac else 'count_fsync.so'))
            subprocess.run(['cc', *(['-dynamiclib'] if mac else ['-shared', '-fPIC']),
                '-o', library, str(source/'test-support'/'count_fsync.c'), *([] if mac else ['-ldl'])], check=True)
            result = subprocess.run([sys.executable, str(source/'bench_cleanup_group.py'), '--runs', '2'],
                env=dict(os.environ, **{('DYLD_INSERT_LIBRARIES' if mac else 'LD_PRELOAD'): library}),
                capture_output=True, text=True, check=True)
            measured = json.loads(result.stdout)
            self.assertEqual(measured['client'], 'cleanup-batch-v6')
            for sample in measured['samples']:
                self.assertEqual(sample['sqlite_commits'], 21)
                # 21 FULL-WAL syncs, 16 ledger syncs, 8 media-directory syncs.
                self.assertEqual(sample['fsyncs'], 45)
