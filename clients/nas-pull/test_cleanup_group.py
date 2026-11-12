"""Crash windows of bounded execution groups, against real disposable files/SQLite."""
import copy
import datetime
import hashlib
import json
import threading
import time
import unittest
from unittest import mock

import test_cleanup as fixtures
pull = fixtures.pull
fixture_ack = fixtures.fixture_ack


class CleanupGroupTests(unittest.TestCase):
    setUp = fixtures.CleanupTests.setUp
    fixture_pins = fixtures.CleanupTests.fixture_pins

    def group(self):
        tasks = []
        for n in range(3):
            item = copy.deepcopy(self.item)
            item.update(source_id=11+n, relative_path='stream/day/%d.mp4' % n)
            path = self.cfg.output_dir / item['relative_path']
            path.write_bytes(b'fixture media')
            sidecar = type(path)(str(path)+'.stoarama.json')
            binding = dict(self.binding, clip_id=item['source_id'], relative_path=item['relative_path'])
            sidecar.write_text(json.dumps(dict(schema_version=1, **binding)))
            item.update(frozen_identity=pull.cleanup_identity(path.stat()), frozen_sidecar_identity=pull.cleanup_identity(sidecar.stat()))
            tasks.append(dict(self.task, item_id=n+1, item=item, execution_batch=True))
        for task in tasks:
            with pull.CleanupFile(self.cfg, task['item'], threading.Event(), time.monotonic()+60) as current:
                task['approved_proof'] = current.proof()
        return tasks

    def api(self, cfg, path, body, **kwargs):
        if path.endswith('/observe-batch'):
            self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'observe'})
            return {'results': [fixture_ack(b, b['item_id'], status=200, action='delete_issued', delete=True,
                plan_paused=False, expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
                for b in body['observations']]}
        self.assertTrue(path.endswith('/result-batch'), path)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'result'})
        return {'results': [fixture_ack(b, b['item_id'], status=200, ok=True, state=b['outcome'], plan_paused=False)
                            for b in body['receipts']]}

    def run_group(self, tasks):
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), mock.patch.object(pull, 'restore_api', side_effect=self.api):
            pull.cleanup_process_group(self.cfg, self.inventory, threading.Event(), tasks)

    def test_group_commits_intents_before_sequential_unlinks(self):
        tasks = self.group()
        real_unlink = pull.os.unlink
        order = []
        def unlink(name, **kwargs):
            self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'intent'})
            ledger = [json.loads(line) for line in (self.cfg.state_dir/pull.CLEANUP_LEDGER_NAME).read_text().splitlines()]
            self.assertEqual([row['event'] for row in ledger], ['intent']*3)
            order.append(name)
            return real_unlink(name, **kwargs)
        with mock.patch.object(pull.os, 'unlink', side_effect=unlink), mock.patch.object(pull, 'cleanup_ledger_append_many', wraps=pull.cleanup_ledger_append_many) as ledger:
            self.run_group(tasks)
        self.assertEqual(ledger.call_count, 2)
        self.assertEqual(order, ['0.mp4', '0.mp4.stoarama.json', '1.mp4', '1.mp4.stoarama.json', '2.mp4', '2.mp4.stoarama.json'])
        self.assertEqual(pull.cleanup_journal(self.inventory), [])

    def test_lost_permit_response_never_unlinks_or_repermits(self):
        tasks = self.group()
        def fail(*args, **kwargs):
            self.api(*args, **kwargs)
            raise TimeoutError('response lost after commit')
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), mock.patch.object(pull, 'restore_api', side_effect=fail) as transport, mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            with self.assertRaises(TimeoutError):
                pull.cleanup_process_group(self.cfg, self.inventory, threading.Event(), tasks)
        self.assertEqual(transport.call_count, 1)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'observe'})

    def test_partial_ledger_intent_write_preserves_all_intents_without_unlink(self):
        tasks = self.group()
        real = pull.cleanup_ledger_append_many
        def partial(cfg, rows):
            real(cfg, rows[:1])
            raise OSError('partial append')
        with mock.patch.object(pull, 'cleanup_ledger_append_many', side_effect=partial), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            with self.assertRaises(OSError): self.run_group(tasks)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'intent'})

    def test_partial_ledger_results_leave_every_file_uncertain(self):
        tasks = self.group()
        real = pull.cleanup_ledger_append_many
        def partial(cfg, rows):
            if rows[0]['event'] == 'result':
                real(cfg, rows[:1])
                raise OSError('result fsync failed')
            real(cfg, rows)
        with mock.patch.object(pull, 'cleanup_ledger_append_many', side_effect=partial):
            with self.assertRaises(OSError): self.run_group(tasks)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'intent'})
        for task in tasks: self.assertFalse((self.cfg.output_dir/task['item']['relative_path']).exists())

    def test_group_sqlite_commit_failure_rolls_back_claims_and_rows(self):
        tasks = self.group()
        owned = [(task, pull.cleanup_reserve_item(self.inventory, task['item_id'])) for task in tasks]
        db = self.inventory.db
        class FailedCommit:
            def __getattr__(self, key): return getattr(db, key)
            def commit(self): raise OSError('commit failed')
        self.inventory.db = FailedCommit()
        try:
            with self.assertRaises(OSError):
                pull.cleanup_journal_group(self.inventory, [(t, o, 'intent', {'outcome':'unverified'}) for t,o in owned])
        finally: self.inventory.db = db
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        self.assertTrue(all(claim[1] is None for claim in self.inventory.cleanup_claims.values()))

    def test_lost_receipt_response_keeps_exact_results_for_replay(self):
        tasks = self.group()
        real_api = self.api
        def lost(cfg, path, body, **kwargs):
            result = real_api(cfg, path, body, **kwargs)
            if path.endswith('/result-batch'): raise TimeoutError('lost result ack')
            return result
        with mock.patch.object(self, 'api', side_effect=lost), mock.patch.object(pull, 'cleanup_api_retry', side_effect=lambda cfg,p,b: pull.restore_api(cfg,p,b)):
            with self.assertRaises(TimeoutError): self.run_group(tasks)
        rows = pull.cleanup_journal(self.inventory)
        self.assertEqual(len(rows), 3)
        self.assertEqual({r[1] for r in rows}, {'result'})
        self.assertTrue(all(json.loads(r[2])['outcome']=='deleted' for r in rows))

    def test_directory_fsync_failure_stops_before_next_file(self):
        tasks = self.group()
        real_delete = pull.CleanupFile.delete_exact
        def fail(current, proof):
            with mock.patch.object(pull.os, 'fsync', side_effect=OSError('directory fsync failed')):
                return real_delete(current, proof)
        with mock.patch.object(pull.CleanupFile, 'delete_exact', fail):
            with self.assertRaises(OSError): self.run_group(tasks)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'intent'})
        self.assertFalse((self.cfg.output_dir/tasks[0]['item']['relative_path']).exists())
        self.assertTrue((self.cfg.output_dir/tasks[1]['item']['relative_path']).exists())

    def test_crash_after_each_unlink_retains_intents(self):
        # Each subcase uses a fresh fixture, including SQLite and exact identities.
        for cut in range(1, 7):
            case = CleanupGroupTests('test_group_commits_intents_before_sequential_unlinks')
            case.setUp()
            try:
                tasks = case.group()
                real = pull.os.unlink
                count = [0]
                def crash(name, **kwargs):
                    result = real(name, **kwargs)
                    count[0] += 1
                    if count[0] == cut: raise SystemExit('power loss')
                    return result
                with mock.patch.object(pull.os, 'unlink', side_effect=crash):
                    with self.assertRaises(SystemExit): case.run_group(tasks)
                self.assertEqual({r[1] for r in pull.cleanup_journal(case.inventory)}, {'intent'})
                self.assertTrue(all('unlink_attempted' not in json.loads(r[2]) for r in pull.cleanup_journal(case.inventory)))
            finally: case.doCleanups()

    def test_one_refused_permit_prevents_every_unlink(self):
        tasks = self.group()
        real_api = self.api
        def refused(cfg, path, body, **kwargs):
            result = real_api(cfg, path, body, **kwargs)
            result['results'][1].update(action='keep', delete=False, plan_paused=True)
            return result
        with mock.patch.object(self, 'api', side_effect=refused), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            with self.assertRaises(pull.CleanupResponseError): self.run_group(tasks)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'observe'})

    def test_local_cap_is_reserved_for_whole_group_before_unlink(self):
        tasks = self.group()
        with mock.patch.object(pull, 'CLEANUP_MAX_FILES_24H', 2), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            with self.assertRaises(pull.CleanupPermitRefused): self.run_group(tasks)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'intent'})
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 0)

    def test_slow_group_barriers_expire_before_unlink(self):
        tasks = self.group()
        real_append, clock = pull.cleanup_ledger_append_many, time.monotonic
        offset = [0]
        def slow(cfg, rows):
            real_append(cfg, rows)
            if rows[0]['event']=='intent': offset[0] = 20
        with mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), mock.patch.object(pull, 'cleanup_ledger_append_many', side_effect=slow), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            with self.assertRaises(pull.CleanupPermitRefused): self.run_group(tasks)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'intent'})

    def test_external_namespace_change_is_not_a_group_unlink(self):
        tasks = self.group()
        real_api = self.api
        def mutate(cfg, path, body, **kwargs):
            result = real_api(cfg, path, body, **kwargs)
            (self.path.parent/'external').write_text('external')
            return result
        with mock.patch.object(self, 'api', side_effect=mutate), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            with self.assertRaisesRegex(RuntimeError, 'membership changed'): self.run_group(tasks)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'intent'})

    def test_mismatched_result_ack_keeps_all_exact_results(self):
        tasks = self.group()
        real_api = self.api
        def wrong(cfg, path, body, **kwargs):
            result = real_api(cfg, path, body, **kwargs)
            if path.endswith('/result-batch'): result['results'][1]['proof_sha256']='0'*64
            return result
        with mock.patch.object(self, 'api', side_effect=wrong):
            with self.assertRaises(pull.CleanupResponseError): self.run_group(tasks)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'result'})
