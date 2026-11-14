"""Exact rolling-cap migration, clock reversals, transactions and bounded work."""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from test_cleanup import pull


class CleanupCapTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cfg = SimpleNamespace(inventory_file=Path(self.temp.name) / 'inventory.sqlite')
        self.inventory = pull.Inventory(self.cfg)
        self.addCleanup(lambda: self.inventory.close())

    def insert(self, at, size=10, item=1):
        row = self.inventory.db.execute(
            "INSERT INTO cleanup_local_deletions(item_id,plan_sha256,relative_path,size_bytes,at_epoch) VALUES(?,'sha','path',?,?)",
            (item, size, at)).lastrowid
        self.inventory.db.commit()
        return row

    def assert_totals(self, now):
        expected = self.inventory.db.execute(
            'SELECT count(*),coalesce(sum(size_bytes),0) FROM cleanup_local_deletions WHERE at_epoch>?',
            (now - 86400,)).fetchone()
        with patch.object(pull.time, 'time', return_value=now):
            files, size = pull.cleanup_local_headroom(self.inventory)
        self.assertEqual((pull.CLEANUP_MAX_FILES_24H-files, pull.CLEANUP_MAX_BYTES_24H-size), expected)

    def test_exact_boundary_clock_reversal_future_rows_and_releases(self):
        for at in [0, 9999, 10000, 10001, 15000, 1000000]:
            self.insert(at)
        for cutoff in [10000, 10001, 20000, -1, 15000, 1000000, 999999, 10000]:
            self.assert_totals(cutoff + 86400)
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 6)
        row = self.insert(10002, size=100)
        self.assert_totals(96400)
        pull.cleanup_release_caps(self.inventory, [row])
        self.assert_totals(96400)
        self.inventory.db.execute('UPDATE cleanup_local_deletions SET at_epoch=20000,size_bytes=1000 WHERE at_epoch=0')
        self.inventory.db.commit()
        self.assert_totals(96400)
        self.assert_totals(110000)

    def test_reservations_and_totals_rollback_together(self):
        self.insert(10001)
        self.assert_totals(96400)
        before = self.inventory.db.execute('SELECT * FROM cleanup_cap_totals').fetchall()
        self.inventory.db.execute("CREATE TRIGGER reject_cap BEFORE INSERT ON cleanup_local_deletions WHEN NEW.item_id=2 BEGIN SELECT RAISE(ABORT,'injected'); END")
        tasks = [dict(item_id=n, plan_sha256='sha', item=dict(relative_path='path', size_bytes=20)) for n in [1, 2]]
        with patch.object(pull.time, 'time', return_value=106400), self.assertRaises(sqlite3.IntegrityError):
            pull.cleanup_reserve_group_caps(self.inventory, tasks)
        self.assertEqual(self.inventory.db.execute('SELECT * FROM cleanup_cap_totals').fetchall(), before)
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 1)
        self.assert_totals(96400)

    def test_restart_and_legacy_migration_preserve_history_and_exact_totals(self):
        self.insert(10000)
        self.insert(20000, size=20)
        self.inventory.close()
        db = sqlite3.connect(str(self.cfg.inventory_file))
        for name in ['cleanup_cap_insert', 'cleanup_cap_delete', 'cleanup_cap_update']:
            db.execute('DROP TRIGGER ' + name)
        db.execute('DROP TABLE cleanup_cap_totals')
        db.commit()
        db.close()
        with patch.object(pull.time, 'time', return_value=100000):
            self.inventory = pull.Inventory(self.cfg)
        self.assert_totals(100000)
        self.inventory.close()
        self.inventory = pull.Inventory(self.cfg)
        self.assert_totals(96400)
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 2)

    def test_cap_reads_do_not_rescan_large_history_or_active_window(self):
        # Most rows are still active: an index-only SUM of the whole window
        # would also fail this bounded-work check.
        self.inventory.db.executemany(
            "INSERT INTO cleanup_local_deletions(item_id,plan_sha256,relative_path,size_bytes,at_epoch) VALUES(?,'sha','path',10,?)",
            ((n, 10001 if n % 2 else 0) for n in range(100000)))
        self.inventory.db.commit()
        self.assert_totals(96400)
        steps = []
        self.inventory.db.set_progress_handler(lambda: steps.append(1) or 0, 100)
        try:
            with patch.object(pull.time, 'time', return_value=96400.5):
                pull.cleanup_local_headroom(self.inventory)
        finally:
            self.inventory.db.set_progress_handler(None, 0)
        self.assertLess(len(steps), 20)

    def test_interrupted_migration_rolls_back_totals_and_can_restart(self):
        self.insert(20000, size=100)
        for name in ['cleanup_cap_insert', 'cleanup_cap_delete', 'cleanup_cap_update']:
            self.inventory.db.execute('DROP TRIGGER ' + name)
        self.inventory.db.execute('DROP TABLE cleanup_cap_totals')
        self.inventory.db.commit()
        self.inventory.db.set_authorizer(lambda action, *args:
            sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_CREATE_TRIGGER else sqlite3.SQLITE_OK)
        try:
            with self.assertRaises(sqlite3.DatabaseError):
                pull.cleanup_initialize(self.inventory)
        finally:
            self.inventory.db.set_authorizer(None)
        self.assertIsNone(self.inventory.db.execute("SELECT 1 FROM sqlite_master WHERE name='cleanup_cap_totals'").fetchone())
        with patch.object(pull.time, 'time', return_value=100000):
            pull.cleanup_initialize(self.inventory)
        self.assert_totals(100000)
