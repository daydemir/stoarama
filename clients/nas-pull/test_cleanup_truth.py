"""Per-item durable truth after one permitted group stops; disposable files only."""
import datetime
import json
import os
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import test_cleanup_bulk as fixtures
pull = fixtures.pull


class CleanupTruthTests(unittest.TestCase):
    setUp = fixtures.CleanupBulkTests.setUp
    group = fixtures.CleanupBulkTests.group
    fixture_pins = fixtures.CleanupBulkTests.fixture_pins
    api = fixtures.CleanupBulkTests.api
    run_group = fixtures.CleanupBulkTests.run_group
    phases = fixtures.CleanupBulkTests.phases
    version = 'cleanup-batch-v8'

    def results(self):
        self.assertTrue(all(phase == 'result' for _, phase, _ in pull.cleanup_journal(self.inventory)))
        return [json.loads(body) for _, _, body in pull.cleanup_journal(self.inventory)]

    def test_completed_collated_item_and_untouched_siblings_replay_exact_truth(self):
        tasks = self.group(8)
        item = tasks[0]['item']
        old = self.cfg.output_dir / item['relative_path']
        relative = 'joined/03_Plac/July/24-Friday/03_Plac_2026_July_W4_Friday_hour_08_part_01_080013-080114.mp4'
        target = self.cfg.output_dir / relative
        target.parent.mkdir(parents=True)
        old.rename(target)
        Path(str(old)+'.stoarama.json').unlink()
        item.update(kind='collated', relative_path=relative, sidecar_required=False,
                    start_at='2026-07-24T06:00:13Z', end_at='2026-07-24T06:01:14.373Z',
                    frozen_identity=pull.cleanup_identity(target.stat()))
        self.pins = self.fixture_pins('2027-01-01T00:00:00+00:00', '2027-01-29T00:00:00+00:00',
                                      folders={'stream': 33, '03_Plac': 33})
        self.pins[0]['windows'][33] = self.pins[0]['windows'][33][:2]+('Europe/Warsaw',)
        for task in tasks:
            with pull.CleanupFile(self.cfg, task['item'], threading.Event(), time.monotonic()+60) as current:
                task['approved_proof'] = current.proof()
        clock, unlink = time.monotonic, pull.os.unlink
        offset = [0]
        def remove(name, **kwargs):
            result = unlink(name, **kwargs)
            if name == target.name:
                offset[0] = 16
            return result
        with mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, 'unlink', side_effect=remove):
            with self.assertRaises(pull.CleanupPermitRefused):
                self.run_group(tasks)
        rows = self.results()
        self.assertEqual(rows[0]['outcome'], 'deleted')
        self.assertIs(rows[0]['unlink_attempted'], True)
        self.assertTrue(all(r['unlink_attempted'] is False and r['refusal_protocol'] == 1 for r in rows[1:]))
        self.assertTrue(all('media_unlinked' not in r and 'sidecar_unlinked' not in r for r in rows))
        ledger = [json.loads(line) for line in (self.cfg.state_dir/pull.CLEANUP_LEDGER_NAME).read_text().splitlines()]
        results = [row for row in ledger if row['event'] == 'result']
        self.assertEqual(len(results), 8)
        self.assertTrue(results[0]['media_unlinked'])
        self.assertFalse(results[0]['sidecar_unlinked'])
        self.assertTrue(all(row['unlink_attempted'] is False for row in results[1:]))
        received = []
        def replay(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith('/result'))
            received.append(body)
            return fixtures.fixtures.fixture_ack(body, int(path.split('/')[-2]), ok=True)
        with mock.patch.object(pull, 'restore_api', side_effect=replay), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('replay cannot unlink')):
            result = pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=5)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(len(received), 8)
        self.assertFalse(self.phases())

    def test_sidecar_deadline_keeps_only_that_item_partial(self):
        tasks = self.group(3)
        first = Path(tasks[0]['item']['relative_path']).name
        clock, unlink = time.monotonic, pull.os.unlink
        offset = [0]
        def remove(name, **kwargs):
            result = unlink(name, **kwargs)
            if name == first:
                offset[0] = 16
            return result
        with mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, 'unlink', side_effect=remove):
            with self.assertRaises(pull.CleanupPermitRefused):
                self.run_group(tasks)
        rows = self.results()
        self.assertEqual(rows[0]['outcome'], 'media_deleted_sidecar_failed')
        self.assertTrue(rows[0]['unlink_attempted'])
        self.assertTrue(Path(str(self.cfg.output_dir/tasks[0]['item']['relative_path'])+'.stoarama.json').exists())
        self.assertTrue(all(r['unlink_attempted'] is False and r['refusal_protocol'] == 1 for r in rows[1:]))


    def expire_after_first_item(self, tasks, index=0):
        first_sidecar = Path(tasks[index]['item']['relative_path']).name + '.stoarama.json'
        clock, unlink = time.monotonic, pull.os.unlink
        offset = [0]
        def remove(name, **kwargs):
            result = unlink(name, **kwargs)
            if name == first_sidecar:
                offset[0] = 16
            return result
        return mock.patch.object(pull.os, 'unlink', side_effect=remove), mock.patch.object(
            pull.time, 'monotonic', side_effect=lambda: clock()+offset[0])

    def test_seven_completed_items_survive_last_member_deadline(self):
        tasks = self.group(8)
        remove, clock = self.expire_after_first_item(tasks, index=6)
        with remove, clock:
            with self.assertRaises(pull.CleanupPermitRefused):
                self.run_group(tasks)
        rows = self.results()
        self.assertTrue(all(row['outcome']=='deleted' and row['unlink_attempted'] is True for row in rows[:7]))
        self.assertEqual(rows[-1]['outcome'], 'unverified')
        self.assertIs(rows[-1]['unlink_attempted'], False)
        self.assertEqual(rows[-1]['refusal_protocol'], 1)
        self.assertTrue((self.cfg.output_dir/tasks[-1]['item']['relative_path']).exists())

    def test_unknown_unlink_result_affects_only_its_own_member(self):
        tasks = self.group(3)
        with mock.patch.object(pull.os, 'unlink', side_effect=OSError('syscall result unknown')):
            with self.assertRaises(OSError):
                self.run_group(tasks)
        rows = self.results()
        self.assertTrue(rows[0]['unlink_attempted'])
        self.assertEqual(rows[0]['outcome'], 'unverified')
        self.assertNotIn('refusal_protocol', rows[0])
        self.assertTrue(all(row['unlink_attempted'] is False and row['refusal_protocol']==1 for row in rows[1:]))

    def test_failed_parent_sync_does_not_invalidate_untouched_refusals(self):
        import stat
        tasks = self.group(3)
        sync = pull.os.fsync
        def fail(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode) and not (self.cfg.output_dir/tasks[0]['item']['relative_path']).exists():
                raise OSError('parent durability unknown')
            return sync(fd)
        remove, clock = self.expire_after_first_item(tasks)
        with remove, clock, mock.patch.object(pull.os, 'fsync', side_effect=fail):
            with self.assertRaises(pull.CleanupPermitRefused):
                self.run_group(tasks)
        rows = self.results()
        self.assertEqual(rows[0]['outcome'], 'unverified')
        self.assertTrue(rows[0]['unlink_attempted'])
        self.assertNotIn('refusal_protocol', rows[0])
        self.assertTrue(all(row['unlink_attempted'] is False and row['refusal_protocol'] == 1 for row in rows[1:]))

    def test_mixed_result_checkpoint_failure_keeps_conservative_intents(self):
        for checkpoint in ('ledger', 'sqlite'):
            with self.subTest(checkpoint=checkpoint):
                case = type(self)('test_sidecar_deadline_keeps_only_that_item_partial')
                case.setUp()
                try:
                    tasks = case.group(3)
                    ledger, journal = pull.cleanup_ledger_append_many, pull.cleanup_journal_group
                    def append(cfg, records):
                        if checkpoint == 'ledger' and records[0]['event'] == 'result':
                            raise OSError('result ledger failed')
                        return ledger(cfg, records)
                    def write(inventory, entries):
                        if checkpoint == 'sqlite' and entries[0][2] == 'result':
                            raise OSError('result transaction failed')
                        return journal(inventory, entries)
                    remove, clock = case.expire_after_first_item(tasks)
                    with remove, clock, mock.patch.object(pull, 'cleanup_ledger_append_many', side_effect=append), \
                            mock.patch.object(pull, 'cleanup_journal_group', side_effect=write):
                        with case.assertRaises(OSError):
                            case.run_group(tasks)
                    case.assertEqual(case.phases(), ['intent']*3)
                    intents = [json.loads(row[2]) for row in pull.cleanup_journal(case.inventory)]
                    case.assertTrue(all(row['unlink_attempted'] is True and 'refusal_protocol' not in row for row in intents))
                    case.assertFalse((case.cfg.output_dir/tasks[0]['item']['relative_path']).exists())
                    case.assertTrue(all((case.cfg.output_dir/task['item']['relative_path']).exists() for task in tasks[1:]))
                finally:
                    case.doCleanups()


class ClientMemoryTests(unittest.TestCase):
    def test_numeric_rss_and_peak_only(self):
        self.assertEqual(pull.process_memory_bytes('Name: private\nVmRSS:\t123 kB\nVmHWM: 456 kB\nVmSwap: 789 kB\n'),
                         {'client_rss_bytes': 123*1024, 'client_peak_rss_bytes': 456*1024})
        self.assertEqual(pull.process_memory_bytes(None), {})
        for line in ('VmRSS: -1 kB', 'VmRSS: x kB', 'VmRSS: 1 MB', 'VmRSS: '+str(1<<63)+' kB'):
            with self.subTest(line=line):
                self.assertEqual(pull.process_memory_bytes(line), {})

    def test_host_memory_refreshes_without_cached_or_stale_rss(self):
        facts = pull.HostFacts(Path('/unused-test-state'))
        values = iter(('VmRSS: 100 kB\nVmHWM: 200 kB', 'VmRSS: 150 kB\nVmHWM: 300 kB', None))
        def read(path, *args):
            if path == '/proc/self/status':
                self.assertEqual(args, (8192,))
                return next(values)
            self.assertEqual(path, '/proc/meminfo')
            return 'MemTotal: 4000 kB\nMemAvailable: 1000 kB'
        with mock.patch.object(facts, '_gather_static', return_value={}), \
                mock.patch.object(pull, '_read_text', side_effect=read):
            first, second, third = facts(), facts(), facts()
        self.assertEqual(first['client_rss_bytes'], 100*1024)
        self.assertEqual(second['client_rss_bytes'], 150*1024)
        self.assertEqual(second['client_peak_rss_bytes'], 300*1024)
        self.assertNotIn('client_rss_bytes', third)
        self.assertNotIn('client_peak_rss_bytes', third)

    @unittest.skipUnless(Path('/proc/self/status').is_file(), 'Linux procfs only')
    def test_actual_linux_self_memory_is_numeric(self):
        facts = pull.process_memory_bytes(Path('/proc/self/status').read_text())
        self.assertGreater(facts['client_rss_bytes'], 0)
        self.assertGreaterEqual(facts['client_peak_rss_bytes'], facts['client_rss_bytes'])
