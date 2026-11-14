"""Crash windows of bounded execution groups, against real disposable files/SQLite."""
import copy
import contextlib
import concurrent.futures
import datetime
import hashlib
import json
import select
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import test_cleanup as fixtures
pull = fixtures.pull
fixture_ack = fixtures.fixture_ack


class CleanupGroupTests(unittest.TestCase):
    fixture_pins = fixtures.CleanupTests.fixture_pins

    def setUp(self):
        fixtures.CleanupTests.setUp(self)
        self.logs = []
        logger = mock.patch.object(pull, 'log', side_effect=lambda level, message: self.logs.append((level, message)))
        logger.start()
        self.addCleanup(logger.stop)
        status = mock.patch.object(pull, 'CLEANUP_STATUS', pull.CleanupStatus())
        status.start()
        self.addCleanup(status.stop)

    def group(self, size=3):
        tasks = []
        for n in range(size):
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
            ready = [json.loads(message.split('cleanup pre-permit ', 1)[1])
                     for _, message in self.logs if message.startswith('cleanup pre-permit ')]
            for observation in body['observations']:
                stage = next(s for s in ready if s.get('item_id') == observation['item_id'] and s['stage'] == 'permit_start')
                self.assertTrue({'lease_ms', 'open_ms', 'proof_ms', 'guard_ms', 'fence_ms', 'observe_journal_ms', 'caps_ms'} <= set(stage['stage_timings']))
            rows = {n: phase for n, phase, _ in pull.cleanup_journal(self.inventory)}
            self.assertTrue(all(rows[b['item_id']] == 'observe' for b in body['observations']))
            return {'results': [fixture_ack(b, b['item_id'], status=200, delivery_keep_checked=True, action='delete_issued', delete=True,
                plan_paused=False, expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
                for b in body['observations']]}
        self.assertTrue(path.endswith('/result-batch'), path)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'result'})
        return {'results': [fixture_ack(b, b['item_id'], status=200, ok=True, state=b['outcome'], plan_paused=False)
                            for b in body['receipts']]}

    def run_group(self, tasks):
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), mock.patch.object(pull, 'restore_api', side_effect=self.api):
            pull.cleanup_process_group(self.cfg, self.inventory, threading.Event(), tasks)

    def test_v5_group_rejects_permit_without_delivery_hold_check(self):
        tasks = self.group(1)
        api = self.api
        def unchecked(cfg, path, body, **kwargs):
            response = api(cfg, path, body, **kwargs)
            if path.endswith('/observe-batch'):
                response['results'][0].pop('delivery_keep_checked')
            return response
        with mock.patch.object(self, 'api', side_effect=unchecked), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('unchecked hold permit')):
            self.run_group(tasks)
        self.assertTrue((self.cfg.output_dir/tasks[0]['item']['relative_path']).exists())
        self.assertFalse(pull.cleanup_journal(self.inventory))

    def cycle(self, tasks):
        """Use the real v2 lease parser/coordinator and execution lane."""
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            return pull.cleanup_batch_v2(self.cfg, self.inventory, threading.Event(), executor, None,
                                         task_limit=len(tasks))

    def test_production_singleton_lease_drain_timeout_returns_before_expiry_then_retries(self):
        tasks = self.group(1)
        fence, clock = pull.NASMediaFence(), time.monotonic
        offset, returned, requests = [0], [], []
        def api(cfg, path, body, **kwargs):
            requests.append(path)
            if path.endswith('/lease'):
                self.assertTrue(body['execution_batch'])
                offset[0] += .3
                return dict(tasks=tasks)
            if path.endswith('/reconcile'):
                self.assertTrue(body['return_lease'])
                self.assertIsNone(body['proof'])
                self.assertLess(offset[0], 1800)
                self.assertEqual(pull.cleanup_journal(self.inventory)[0][1], 'observe')
                returned.append(body)
                return fixture_ack(body, action='pending', delete=False)
            return self.api(cfg, path, body, **kwargs)
        def drain(timeout):
            offset[0] += timeout  # Exercise the real 15s drain without sleeping.
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), \
                mock.patch.object(pull, 'NAS_MEDIA_FENCE', fence), \
                mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(fence.condition, 'wait', side_effect=drain), \
                mock.patch.object(pull, 'restore_api', side_effect=api), \
                mock.patch.object(pull.CleanupFile, 'proof', wraps=pull.CleanupFile.proof) as proof:
            fence.writers = 1
            with self.assertRaises(pull.CleanupDrainTimeout):
                self.cycle(tasks)
            proof.assert_not_called()
            self.assertEqual(len(returned), 1)
            self.assertEqual(len(requests), 2)  # lease then reconcile, no permit
            self.assertFalse(pull.cleanup_journal(self.inventory))
            self.assertFalse(self.inventory.cleanup_claims)
            self.assertIsNone(fence.execution_owner)
            self.assertEqual(fence.cleaners, 0)
            self.assertFalse(fence.execution_lock.locked())
        fence.writers = 0
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), \
                mock.patch.object(pull, 'NAS_MEDIA_FENCE', fence), \
                mock.patch.object(pull, 'restore_api', side_effect=api):
            self.cycle(tasks)
        self.assertFalse((self.cfg.output_dir/tasks[0]['item']['relative_path']).exists())
        self.assertFalse(pull.cleanup_journal(self.inventory))
        stages = [json.loads(message.split('cleanup pre-permit ', 1)[1])
                  for _, message in self.logs if message.startswith('cleanup pre-permit ')]
        ended = next(s for s in stages if s['stage'] == 'lane_end')
        self.assertGreaterEqual(ended['stage_timings']['lane_ms'], 15000)
        self.assertGreaterEqual(ended['stage_timings']['lease_ms'], 300)

    def test_slow_real_proofs_return_entire_lease_before_expiry(self):
        tasks = self.group(2)
        for task in tasks:
            task['expires_at'] = (datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=150)).isoformat()
        read, clock, real_datetime = pull.os.read, time.monotonic, datetime.datetime
        offset, returned = [0], []
        class Clock(real_datetime):
            @classmethod
            def now(cls, tz=None):
                return real_datetime.now(tz)+datetime.timedelta(seconds=offset[0])
        def slow_read(*args):
            offset[0] += 10
            return read(*args)
        def api(cfg, path, body, **kwargs):
            if path.endswith('/lease'): return dict(tasks=tasks)
            self.assertTrue(path.endswith('/reconcile'), path)
            self.assertTrue(body['return_lease'])
            self.assertLess(offset[0], 150)
            returned.append(int(path.split('/')[-2]))
            return fixture_ack(body, returned[-1], action='pending', delete=False)
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), \
                mock.patch.object(pull.datetime, 'datetime', Clock), \
                mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, 'read', side_effect=slow_read), \
                mock.patch.object(pull, 'restore_api', side_effect=api), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('slow proof cannot unlink')):
            with self.assertRaises(pull.CleanupInterrupted): self.cycle(tasks)
        self.assertCountEqual(returned, [1, 2])
        self.assertFalse(pull.cleanup_journal(self.inventory))
        self.assertTrue(all((self.cfg.output_dir/t['item']['relative_path']).exists() for t in tasks))
        self.assertTrue(any('proof_end' in m and 'proof_ms' in m for _, m in self.logs))

    def test_prepermit_exception_returns_all_unused_work(self):
        tasks = self.group()
        returned = []
        def api(cfg, path, body, **kwargs):
            if path.endswith('/lease'): return dict(tasks=tasks)
            self.assertTrue(path.endswith('/reconcile'), path)
            returned.append(int(path.split('/')[-2]))
            return fixture_ack(body, returned[-1], action='pending', delete=False)
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), \
                mock.patch.object(pull.CleanupFile, 'proof', side_effect=OSError('NAS read failed')), \
                mock.patch.object(pull, 'restore_api', side_effect=api):
            with self.assertRaisesRegex(OSError, 'NAS read failed'): self.cycle(tasks)
        self.assertCountEqual(returned, [1, 2, 3])
        self.assertFalse(pull.cleanup_journal(self.inventory))

    def test_lost_unused_return_ack_replays_only_reconciliation(self):
        tasks = self.group(1)
        saved = []
        def api(cfg, path, body, **kwargs):
            if path.endswith('/lease'): return dict(tasks=tasks)
            self.assertTrue(path.endswith('/reconcile'), path)
            saved.append(dict(body))
            raise TimeoutError('lost return acknowledgment')
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), \
                mock.patch.object(pull.CleanupFile, 'proof', side_effect=OSError('NAS read failed')), \
                mock.patch.object(pull, 'restore_api', side_effect=api):
            with self.assertRaises(OSError): self.cycle(tasks)
        row = pull.cleanup_journal(self.inventory)[0]
        self.assertEqual(row[1], 'observe')
        self.assertEqual(json.loads(row[2]), saved[0])
        def replay(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith('/reconcile'), path)
            self.assertEqual(body, saved[0])
            return fixture_ack(body, action='pending', delete=False)
        with mock.patch.object(pull, 'restore_api', side_effect=replay), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('return replay cannot unlink')):
            result = pull.cleanup_replay_receipts(self.cfg, self.inventory)
        self.assertEqual(result['failed'], 0)
        self.assertFalse(pull.cleanup_journal(self.inventory))

    def test_kill_during_drain_or_proof_restart_returns_every_lease(self):
        tasks = self.group(8)
        for stage in ('drain', 'proof'):
            with self.subTest(stage=stage):
                # A separate interpreter dies without running Python finally
                # handlers. Restart opens the same on-disk SQLite/WAL journal.
                script = textwrap.dedent("""
                    import concurrent.futures, json, sys, threading
                    from pathlib import Path
                    from types import SimpleNamespace
                    from unittest import mock
                    import test_cleanup
                    pull = test_cleanup.pull
                    root, stage, encoded = sys.argv[1:]
                    cfg = SimpleNamespace(output_dir=Path(root)/'clips', state_dir=Path(root)/'state',
                        inventory_file=Path(root)/'state'/'inventory.sqlite3', dry_run=False)
                    inventory = pull.Inventory(cfg)
                    def halt(*args):
                        print('ready', flush=True)
                        sys.stdin.read()  # Parent SIGKILLs while this stage is active.
                        raise AssertionError('child unexpectedly resumed')
                    def api(cfg, path, body, **kwargs):
                        if path.endswith('/lease'): return dict(tasks=json.loads(encoded))
                        raise AssertionError('no permit or return before kill: '+path)
                    fence = pull.NASMediaFence()
                    fence.writers = int(stage == 'drain')
                    target, method = (fence.condition, 'wait') if stage == 'drain' else (pull.CleanupFile, 'proof')
                    with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), \
                            mock.patch.object(pull, 'NAS_MEDIA_FENCE', fence), \
                            mock.patch.object(pull.os.path, 'ismount', return_value=True), \
                            mock.patch.object(pull, 'cleanup_pins', return_value=test_cleanup.CleanupTests.fixture_pins(
                                None, '2027-01-01T00:00:00+00:00', '2027-01-29T00:00:00+00:00')), \
                            mock.patch.object(pull, 'restore_api', side_effect=api), \
                            mock.patch.object(target, method, side_effect=halt), \
                            mock.patch.object(pull, 'log'), \
                            concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        pull.cleanup_batch_v2(cfg, inventory, threading.Event(), executor, None, task_limit=8)
                """)
                child = subprocess.Popen([sys.executable, '-B', '-c', script, str(self.root), stage, json.dumps(tasks)],
                    cwd=str(Path(__file__).parent), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    self.assertTrue(select.select([child.stdout], [], [], 10)[0], 'child did not reach '+stage)
                    self.assertEqual(child.stdout.readline(), b'ready\n')
                    child.kill()
                    child.wait(timeout=10)
                    self.assertLess(child.returncode, 0)
                finally:
                    if child.poll() is None:
                        child.kill()
                    child.communicate(timeout=10)
                restarted = pull.Inventory(self.cfg)
                try:
                    rows = pull.cleanup_journal(restarted)
                    self.assertEqual(len(rows), 8)
                    saved = {n: json.loads(body) for n, phase, body in rows if phase == 'observe'}
                    returned = []
                    def api(cfg, path, body, **kwargs):
                        self.assertTrue(path.endswith('/reconcile'), path)
                        item_id = int(path.split('/')[-2])
                        self.assertEqual(body, saved[item_id])
                        self.assertTrue(body['return_lease'])
                        self.assertIsNone(body['proof'])
                        self.assertLess(datetime.datetime.now(datetime.timezone.utc),
                                        datetime.datetime.fromisoformat(tasks[item_id-1]['expires_at']))
                        returned.append(item_id)
                        return fixture_ack(body, item_id, action='pending', delete=False)
                    with mock.patch.object(pull, 'restore_api', side_effect=api), \
                            mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('restart cannot unlink')):
                        counts = pull.cleanup_replay_receipts(self.cfg, restarted)
                    self.assertEqual(counts['replayed'], 8)
                    self.assertEqual(counts['failed'], 0)
                    self.assertCountEqual(returned, range(1, 9))
                    self.assertFalse(pull.cleanup_journal(restarted))
                finally:
                    restarted.close()
                self.assertTrue(all((self.cfg.output_dir/t['item']['relative_path']).exists() for t in tasks))

    def test_shutdown_eight_unused_leases_is_one_bounded_return_round(self):
        tasks = self.group(8)
        stop, proving = threading.Event(), threading.Event()
        barrier = threading.Barrier(8)
        snapshots, requests, errors = [], [], []
        def proof(current):
            proving.set()
            stop.wait(10)
            raise pull.CleanupInterrupted('shutdown during proof')
        def api(cfg, path, body, **kwargs):
            if path.endswith('/lease'): return dict(tasks=tasks)
            item_id = int(path.split('/')[-2])
            requests.append((item_id, path, kwargs['timeout']))
            # An independent DB connection verifies durability before ANY
            # return starts, including members whose transports later fail.
            with contextlib.closing(pull.sqlite3.connect(str(self.cfg.inventory_file))) as db:
                snapshots.append(db.execute('SELECT item_id,body FROM cleanup_receipts').fetchall())
            barrier.wait(timeout=5)  # Sequential returns fail this regression.
            threading.Event().wait(kwargs['timeout'])  # Eight real 25s waits overlap.
            if item_id % 2 == 0:
                raise TimeoutError('return acknowledgment lost')
            return fixture_ack(body, item_id, action='pending', delete=False)
        def run():
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    pull.cleanup_batch_v2(self.cfg, self.inventory, stop, executor, None, task_limit=8)
            except BaseException as exc:
                errors.append(exc)
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), \
                mock.patch.object(pull.CleanupFile, 'proof', proof), \
                mock.patch.object(pull, 'restore_api', side_effect=api):
            worker = threading.Thread(target=run)
            worker.start()
            self.assertTrue(proving.wait(5))
            started = time.monotonic()
            stop.set()
            worker.join(pull.HTTP_TIMEOUT_SEC + 5)  # Same bound as real shutdown.
            self.assertFalse(worker.is_alive(), 'cleanup exceeded shutdown join')
            self.assertLess(time.monotonic()-started, pull.CLEANUP_EXECUTION_OBSERVE_SEC+5)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], pull.CleanupInterrupted)
        self.assertEqual(len(requests), 8)
        self.assertTrue(all(path.endswith('/reconcile') and timeout <= 25 for _, path, timeout in requests))
        self.assertEqual(len(snapshots), 8)
        self.assertTrue(all(len(rows) == 8 and all(json.loads(body)['return_lease'] for _, body in rows)
                            for rows in snapshots))
        rows = pull.cleanup_journal(self.inventory)
        self.assertEqual([r[0] for r in rows], [2, 4, 6, 8])
        self.assertFalse(self.inventory.cleanup_claims)
        def replay(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith('/reconcile'))
            return fixture_ack(body, int(path.split('/')[-2]), action='pending', delete=False)
        with mock.patch.object(pull, 'restore_api', side_effect=replay):
            self.assertEqual(pull.cleanup_replay_receipts(self.cfg, self.inventory)['replayed'], 4)
        self.assertFalse(pull.cleanup_journal(self.inventory))

    def test_group_commits_intents_before_sequential_unlinks(self):
        tasks = self.group()
        real_unlink = pull.os.unlink
        order = []
        def unlink(name, **kwargs):
            rows = {n: (phase, json.loads(body)) for n, phase, body in pull.cleanup_journal(self.inventory)}
            item_id = int(name.split('.')[0])+1
            self.assertEqual(rows[item_id][0], 'intent')
            self.assertIs(rows[item_id][1]['unlink_attempted'], True)
            ledger = [json.loads(line) for line in (self.cfg.state_dir/pull.CLEANUP_LEDGER_NAME).read_text().splitlines()]
            self.assertEqual(ledger[-1]['event'], 'intent')
            self.assertEqual(ledger[-1]['item_id'], item_id)
            order.append(name)
            return real_unlink(name, **kwargs)
        with mock.patch.object(pull.os, 'unlink', side_effect=unlink), mock.patch.object(pull, 'cleanup_ledger_append_many', wraps=pull.cleanup_ledger_append_many) as ledger:
            self.run_group(tasks)
        self.assertEqual(ledger.call_count, 6)
        self.assertEqual(order, ['0.mp4', '0.mp4.stoarama.json', '1.mp4', '1.mp4.stoarama.json', '2.mp4', '2.mp4.stoarama.json'])
        self.assertEqual(pull.cleanup_journal(self.inventory), [])

    def test_eight_slow_files_get_fresh_per_file_permits_and_one_receipt_batch(self):
        tasks = self.group(8)
        clock, delete, caps = time.monotonic, pull.CleanupFile.delete_exact, pull.cleanup_reserve_group_caps
        offset, observed, batches = [0], [], []
        def slow_caps(*args):
            reservations = caps(*args)
            offset[0] += 20  # NAS durability before authority must not age a permit.
            return reservations
        def slow_delete(current, proof):
            offset[0] += 4  # Whole lease takes >30s; each permit remains fresh.
            return delete(current, proof)
        def api(cfg, path, body, **kwargs):
            if path.endswith('/observe-batch'):
                self.assertEqual(len(body['observations']), 1)
                observed.append(body['observations'][0]['item_id'])
                return {'results': [fixture_ack(body['observations'][0], observed[-1], status=200,
                    delivery_keep_checked=True, action='delete_issued', delete=True, plan_paused=False,
                    expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())]}
            batches.append(body['receipts'])
            return {'results': [fixture_ack(b, b['item_id'], status=200, ok=True) for b in body['receipts']]}
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), \
                mock.patch.object(pull, 'restore_api', side_effect=api), \
                mock.patch.object(pull, 'cleanup_reserve_group_caps', side_effect=slow_caps), \
                mock.patch.object(pull.CleanupFile, 'delete_exact', slow_delete), \
                mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]):
            pull.cleanup_process_group(self.cfg, self.inventory, threading.Event(), tasks)
        self.assertEqual(observed, list(range(1, 9)))
        self.assertEqual(len(batches), 1)
        self.assertEqual([b['outcome'] for b in batches[0]], ['deleted']*8)
        self.assertFalse(pull.cleanup_journal(self.inventory))

    def test_slow_intents_save_certified_refusals_and_replay_exact_results(self):
        tasks = self.group(8)
        clock, journal = time.monotonic, pull.cleanup_journal_group
        offset = [0]
        def slow(inventory, entries):
            journal(inventory, entries)
            if entries[0][2] == 'intent': offset[0] += 40
        with mock.patch.object(pull, 'cleanup_journal_group', side_effect=slow), \
                mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('late permit must not unlink')), \
                mock.patch.object(pull, 'cleanup_api_retry', side_effect=TimeoutError('lost receipt ack')):
            with self.assertRaises(TimeoutError): self.run_group(tasks)
        saved = {item_id: json.loads(body) for item_id, phase, body in pull.cleanup_journal(self.inventory)
                 if phase == 'result'}
        self.assertEqual(len(saved), 8)
        for receipt in saved.values():
            self.assertEqual(receipt['outcome'], 'unverified')
            self.assertEqual(receipt['refusal_reason'], 'deadline_exceeded')
            self.assertEqual(receipt['refusal_protocol'], pull.CLEANUP_REFUSAL_PROTOCOL)
            self.assertIs(receipt['unlink_attempted'], False)
            self.assertGreaterEqual(receipt['stage_timings']['intent_journal_ms'], 40000)
        replayed = {}
        def replay(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith('/result'), path)
            item_id = int(path.split('/')[-2])
            replayed[item_id] = body
            return fixture_ack(body, item_id, ok=True)
        with mock.patch.object(pull, 'restore_api', side_effect=replay), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('replay cannot unlink')):
            result = pull.cleanup_replay_receipts(self.cfg, self.inventory)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(replayed, saved)
        self.assertFalse(pull.cleanup_journal(self.inventory))

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
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 0)

    def test_partial_ledger_intent_write_preserves_observation_without_unlink(self):
        tasks = self.group()
        real = pull.cleanup_ledger_append_many
        def partial(cfg, rows):
            real(cfg, rows[:1])
            raise OSError('partial append')
        with mock.patch.object(pull, 'cleanup_ledger_append_many', side_effect=partial), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            with self.assertRaises(OSError): self.run_group(tasks)
        self.assertEqual([r[1] for r in pull.cleanup_journal(self.inventory)], ['observe', 'observe', 'observe'])

    def test_partial_ledger_results_leave_issued_file_uncertain(self):
        tasks = self.group()
        real = pull.cleanup_ledger_append_many
        def partial(cfg, rows):
            if rows[0]['event'] == 'result':
                real(cfg, rows[:1])
                raise OSError('result fsync failed')
            real(cfg, rows)
        with mock.patch.object(pull, 'cleanup_ledger_append_many', side_effect=partial):
            with self.assertRaises(OSError): self.run_group(tasks)
        self.assertEqual([r[1] for r in pull.cleanup_journal(self.inventory)], ['intent', 'observe', 'observe'])
        self.assertFalse((self.cfg.output_dir/tasks[0]['item']['relative_path']).exists())
        self.assertTrue((self.cfg.output_dir/tasks[1]['item']['relative_path']).exists())

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

    def test_directory_fsync_failure_records_attempt_without_certifying_refusal(self):
        tasks = self.group()
        real_delete = pull.CleanupFile.delete_exact
        fsync = pull.os.fsync
        def fail(current, proof):
            def sync(fd):
                if fd == current.fds[-1]: raise OSError('directory fsync failed')
                return fsync(fd)
            with mock.patch.object(pull.os, 'fsync', side_effect=sync):
                return real_delete(current, proof)
        with mock.patch.object(pull.CleanupFile, 'delete_exact', fail):
            with self.assertRaises(OSError): self.run_group(tasks)
        outcomes = [json.loads(r[0]) for r in self.inventory.db.execute('SELECT body FROM cleanup_execution_outcomes')]
        self.assertTrue(outcomes[0]['unlink_attempted'])
        self.assertEqual(outcomes[0]['outcome'], 'unverified')
        self.assertNotIn('refusal_protocol', outcomes[0])
        self.assertEqual([r[1] for r in pull.cleanup_journal(self.inventory)], ['result', 'observe', 'observe'])
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
                rows = pull.cleanup_journal(case.inventory)
                crashed = (cut-1)//2
                self.assertEqual([r[1] for r in rows], ['result']*crashed + ['intent'] + ['observe']*(2-crashed))
                self.assertIs(json.loads(rows[crashed][2])['unlink_attempted'], True)
                self.assertNotIn('refusal_protocol', json.loads(rows[crashed][2]))
                replayed = {}
                def replay(cfg, path, body, **kwargs):
                    item_id = int(path.split('/')[-2])
                    if path.endswith('/reconcile'):
                        return fixture_ack(body, item_id, action='pending', delete=False)
                    self.assertTrue(path.endswith('/result'), path)
                    replayed[item_id] = body
                    return fixture_ack(body, item_id, ok=True)
                # Receipts remain valid after the original lease/permit expires.
                with mock.patch.object(pull, 'restore_api', side_effect=replay), \
                        mock.patch.object(pull.time, 'time', return_value=time.time()+3600), \
                        mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('replay cannot unlink')):
                    replay_result = pull.cleanup_replay_receipts(case.cfg, case.inventory)
                self.assertEqual(replay_result['failed'], 0)
                self.assertFalse(pull.cleanup_journal(case.inventory))
                self.assertTrue(all(replayed[n+1]['outcome']=='deleted' for n in range(crashed)))
                uncertain = replayed[crashed+1]
                self.assertEqual(uncertain['outcome'], 'unverified')
                self.assertNotIn('refusal_protocol', uncertain)
            finally: case.doCleanups()

    def test_sigkill_around_attempt_and_result_replays_without_refusal_or_unlink(self):
        for cut in ('ledger_intent', 'marker', 'media', 'sidecar', 'result'):
            case = CleanupGroupTests('test_group_commits_intents_before_sequential_unlinks')
            case.setUp()
            try:
                tasks = case.group(8)
                script = textwrap.dedent("""
                    import json, sys, threading
                    from pathlib import Path
                    from types import SimpleNamespace
                    from unittest import mock
                    import test_cleanup_group as fixtures
                    pull = fixtures.pull
                    root, cut, encoded = sys.argv[1:]
                    case = fixtures.CleanupGroupTests('test_group_commits_intents_before_sequential_unlinks')
                    case.cfg = SimpleNamespace(output_dir=Path(root)/'clips', state_dir=Path(root)/'state',
                        inventory_file=Path(root)/'state'/'inventory.sqlite3', dry_run=False)
                    case.inventory, case.logs = pull.Inventory(case.cfg), []
                    ledger, journal, unlink = pull.cleanup_ledger_append_many, pull.cleanup_journal_group, pull.os.unlink
                    def halt():
                        print('ready', flush=True)
                        sys.stdin.read()
                        raise AssertionError('parent must SIGKILL')
                    def append(cfg, records):
                        ledger(cfg, records)
                        if cut == 'ledger_intent' and records[0]['event'] == 'intent': halt()
                    def checkpoint(inventory, entries):
                        journal(inventory, entries)
                        if (cut == 'marker' and entries[0][2] == 'intent'
                                or cut == 'result' and entries[0][2] == 'result'): halt()
                    def remove(name, **kwargs):
                        unlink(name, **kwargs)
                        if cut == ('sidecar' if name.endswith('.json') else 'media'): halt()
                    with mock.patch.object(pull.os.path, 'ismount', return_value=True), \
                            mock.patch.object(pull, 'cleanup_pins', return_value=case.fixture_pins(
                                '2027-01-01T00:00:00+00:00', '2027-01-29T00:00:00+00:00')), \
                            mock.patch.object(pull, 'log', side_effect=lambda l,m: case.logs.append((l,m))), \
                            mock.patch.object(pull, 'cleanup_ledger_append_many', side_effect=append), \
                            mock.patch.object(pull, 'cleanup_journal_group', side_effect=checkpoint), \
                            mock.patch.object(pull.os, 'unlink', side_effect=remove):
                        case.run_group(json.loads(encoded))
                """)
                child = subprocess.Popen([sys.executable, '-c', script, str(case.root), cut, json.dumps(tasks)],
                    cwd=str(Path(__file__).parent), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    self.assertTrue(select.select([child.stdout], [], [], 15)[0], 'child did not reach crash window')
                    self.assertEqual(child.stdout.readline().strip(), 'ready')
                finally:
                    child.kill()
                    _, errors = child.communicate(timeout=10)
                self.assertFalse(errors, errors)
                restarted = pull.Inventory(case.cfg)
                try:
                    rows = pull.cleanup_journal(restarted)
                    expected = 'observe' if cut == 'ledger_intent' else 'result' if cut == 'result' else 'intent'
                    self.assertEqual(rows[0][1], expected)
                    self.assertNotIn('refusal_protocol', json.loads(rows[0][2]))
                    # SIGKILL runs no finally: all eight conservative cap charges survive.
                    self.assertEqual(restarted.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 8)
                    replayed = []
                    def replay(cfg, path, body, **kwargs):
                        item_id = int(path.split('/')[-2])
                        if path.endswith('/reconcile'):
                            return fixture_ack(body, item_id, action='reconcile' if item_id == 1 else 'pending', delete=False)
                        replayed.append(body)
                        return fixture_ack(body, item_id, ok=True)
                    with mock.patch.object(pull, 'restore_api', side_effect=replay), \
                            mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('replay cannot unlink')):
                        report = pull.cleanup_replay_receipts(case.cfg, restarted)
                    self.assertEqual(report['failed'], 0)
                    self.assertFalse(pull.cleanup_journal(restarted))
                    self.assertEqual(replayed[0]['outcome'], 'deleted' if cut == 'result' else 'unverified')
                    self.assertNotIn('refusal_protocol', replayed[0])
                finally:
                    restarted.close()
            finally:
                case.doCleanups()

    def test_one_refused_permit_prevents_every_unlink(self):
        tasks = self.group()
        real_api = self.api
        def refused(cfg, path, body, **kwargs):
            result = real_api(cfg, path, body, **kwargs)
            result['results'][0].update(action='keep', delete=False, plan_paused=True)
            return result
        with mock.patch.object(self, 'api', side_effect=refused), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            with self.assertRaises(pull.CleanupResponseError): self.run_group(tasks)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'observe'})

    def test_local_cap_reserves_whole_group_or_stops_before_every_permit(self):
        tasks = self.group()
        with mock.patch.object(pull, 'CLEANUP_MAX_FILES_24H', 2), \
                mock.patch.object(pull, 'restore_api', side_effect=AssertionError('no partial reservation')):
            with self.assertRaises(pull.CleanupPermitRefused): self.run_group(tasks)
        self.assertEqual([r[1] for r in pull.cleanup_journal(self.inventory)], ['observe']*3)
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 0)

    def test_caps_and_diagnostics_each_commit_once_for_eight_members(self):
        tasks = self.group(8)
        statements = []
        self.inventory.db.set_trace_callback(statements.append)
        with mock.patch.object(pull, 'cleanup_reserve_group_caps', wraps=pull.cleanup_reserve_group_caps) as caps, \
                mock.patch.object(pull, 'cleanup_execution_outcomes', wraps=pull.cleanup_execution_outcomes) as diagnostics:
            self.run_group(tasks)
        self.inventory.db.set_trace_callback(None)
        self.assertEqual(caps.call_count, 1)
        self.assertEqual(len(caps.call_args.args[1]), 8)
        self.assertEqual(diagnostics.call_count, 1)
        self.assertEqual(len(diagnostics.call_args.args[1]), 8)
        # All cap INSERTs have the same enclosing transaction.
        charge_positions = [n for n, sql in enumerate(statements) if sql.startswith('INSERT INTO cleanup_local_deletions')]
        # SQLite's trace callback repeats the outer INSERT for trigger steps.
        self.assertEqual(len({statements[n] for n in charge_positions}), 8)
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 8)
        self.assertFalse(any(sql == 'COMMIT' for sql in statements[charge_positions[0]:charge_positions[-1]]))

    def test_abort_releases_exact_unattempted_rows_including_unvisited_members(self):
        tasks = self.group(8)
        historic = pull.cleanup_reserve_group_caps(self.inventory, tasks)
        api = self.api
        def fail(cfg, path, body, **kwargs):
            if path.endswith('/observe-batch') and body['observations'][0]['item_id'] == 3:
                raise TimeoutError('third permit response lost')
            return api(cfg, path, body, **kwargs)
        with mock.patch.object(self, 'api', side_effect=fail):
            with self.assertRaises(TimeoutError): self.run_group(tasks)
        rows = self.inventory.db.execute('SELECT id,item_id FROM cleanup_local_deletions ORDER BY id').fetchall()
        self.assertEqual([r[0] for r in rows[:8]], historic)
        self.assertEqual([r[1] for r in rows[8:]], [1, 2])

    def test_group_cap_insert_failure_rolls_back_every_charge(self):
        tasks = self.group(8)
        self.inventory.db.execute("CREATE TRIGGER reject_third_cap BEFORE INSERT ON cleanup_local_deletions WHEN new.item_id=3 BEGIN SELECT RAISE(ABORT,'injected cap failure'); END")
        with self.assertRaisesRegex(Exception, 'injected cap failure'):
            pull.cleanup_reserve_group_caps(self.inventory, tasks)
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 0)

    def test_slow_group_barriers_expire_before_unlink(self):
        tasks = self.group()
        real_append, clock = pull.cleanup_ledger_append_many, time.monotonic
        offset = [0]
        def slow(cfg, rows):
            real_append(cfg, rows)
            if rows[0]['event']=='intent': offset[0] += 20
        with mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), mock.patch.object(pull, 'cleanup_ledger_append_many', side_effect=slow), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            self.run_group(tasks)
        outcomes = [json.loads(r[0]) for r in self.inventory.db.execute('SELECT body FROM cleanup_execution_outcomes')]
        self.assertTrue(all(r['refusal_reason']=='deadline_exceeded' and r['refusal_protocol']==1 for r in outcomes))
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 0)

    def test_unused_cap_release_preserves_other_charges_and_crash_charge_expires(self):
        tasks = self.group()
        charged = pull.cleanup_reserve_group_caps(self.inventory, tasks[:1])
        provisional = pull.cleanup_reserve_group_caps(self.inventory, tasks[1:2])
        pull.cleanup_release_caps(self.inventory, provisional)
        self.assertEqual([r[0] for r in self.inventory.db.execute('SELECT id FROM cleanup_local_deletions')], charged)
        self.assertEqual(pull.cleanup_local_headroom(self.inventory)[0], pull.CLEANUP_MAX_FILES_24H-1)
        with mock.patch.object(pull.time, 'time', return_value=time.time()+86401):
            self.assertEqual(pull.cleanup_local_headroom(self.inventory)[0], pull.CLEANUP_MAX_FILES_24H)

    def test_external_namespace_change_is_not_a_group_unlink(self):
        tasks = self.group()
        real_api = self.api
        def mutate(cfg, path, body, **kwargs):
            result = real_api(cfg, path, body, **kwargs)
            (self.path.parent/'external').write_text('external')
            return result
        with mock.patch.object(self, 'api', side_effect=mutate), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            self.run_group(tasks)
        outcomes = [json.loads(r[0]) for r in self.inventory.db.execute('SELECT body FROM cleanup_execution_outcomes')]
        self.assertTrue(all(r['refusal_reason']=='pre_unlink_failure' and not r['unlink_attempted'] for r in outcomes))
        status, _, _ = pull.CLEANUP_STATUS.snapshot()
        self.assertEqual(status['item_failures'], len(tasks))
        self.assertEqual(status['last_item_failure_stage'], 'unlink')
        self.assertNotIn('aborted_groups', status)

    def test_abort_heartbeat_has_fixed_stage_reason_and_no_exception_contents(self):
        tasks = self.group(1)
        with mock.patch.object(pull, 'cleanup_reserve_group_caps', side_effect=TimeoutError('private credential sentinel')):
            with self.assertRaises(TimeoutError):
                self.run_group(tasks)
        status, _, _ = pull.CLEANUP_STATUS.snapshot()
        self.assertEqual(status['aborted_groups'], 1)
        self.assertEqual((status['last_abort_stage'], status['last_abort_reason']), ('caps', 'transport_error'))
        self.assertEqual(status['last_abort_files'], 1)
        self.assertNotIn('private', json.dumps(status))

    def test_production_ownership_failure_reports_one_abort_before_permits(self):
        tasks = self.group(1)
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_GROUP_VERSION), \
                mock.patch.object(pull, 'cleanup_lease_v2', return_value={'tasks': tasks}), \
                mock.patch.object(pull, 'cleanup_journal_group', side_effect=sqlite3.OperationalError('private schema error')), \
                mock.patch.object(pull, 'restore_api', side_effect=AssertionError('ownership failure cannot request permit')):
            with self.assertRaises(sqlite3.OperationalError):
                self.cycle(tasks)
        status, _, _ = pull.CLEANUP_STATUS.snapshot()
        self.assertEqual(status['aborted_groups'], 1)
        self.assertEqual((status['last_abort_stage'], status['last_abort_reason']), ('ownership', 'sqlite_error'))
        self.assertFalse(self.inventory.cleanup_claims)
        self.assertTrue((self.cfg.output_dir/tasks[0]['item']['relative_path']).exists())

    def test_journal_updates_have_no_ddl_or_full_receipts_read_and_three_namespace_checks(self):
        tasks = self.group(1)
        statements = []
        self.inventory.db.set_trace_callback(statements.append)
        list_parent = pull.CleanupFile.list_parent
        listings = []
        def listing(current):
            listings.append(current.item['relative_path'])
            return list_parent(current)
        with mock.patch.object(pull.CleanupFile, 'list_parent', listing):
            self.run_group(tasks)
        self.inventory.db.set_trace_callback(None)
        self.assertEqual(len(listings), 3)  # before/after hash, after durable marker
        self.assertFalse(any(sql.startswith(('CREATE ', 'PRAGMA table_info')) for sql in statements))
        # Explicit replay/status reads are allowed; item journal writes don't
        # themselves read every receipt. Exercise one outside the fixture API.
        statements.clear()
        self.inventory.db.set_trace_callback(statements.append)
        pull.cleanup_journal(self.inventory, 999, 'result', {'test': True})
        self.inventory.db.set_trace_callback(None)
        self.assertFalse(any('SELECT item_id,phase,body FROM cleanup_receipts' in sql for sql in statements))

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

    def test_lease_margin_prevents_another_permit_and_releases_reservation(self):
        tasks = self.group()
        for task in tasks:
            task['expires_at'] = (datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=40)).isoformat()
        with mock.patch.object(pull, 'restore_api', side_effect=AssertionError('insufficient lease budget')):
            with self.assertRaises(pull.CleanupInterrupted): self.run_group(tasks)
        self.assertEqual(pull.cleanup_local_headroom(self.inventory)[0], pull.CLEANUP_MAX_FILES_24H)
        self.assertEqual({r[1] for r in pull.cleanup_journal(self.inventory)}, {'observe'})

    def test_replaced_path_after_durable_marker_is_preserved(self):
        tasks = self.group(1)
        path = self.cfg.output_dir/tasks[0]['item']['relative_path']
        journal = pull.cleanup_journal_group
        def replace(inventory, entries):
            journal(inventory, entries)
            if entries[0][2] == 'intent' and entries[0][3].get('unlink_attempted') is True:
                replacement = path.with_name('replacement')
                replacement.write_bytes(b'replaced source')
                replacement.replace(path)
        with mock.patch.object(pull, 'cleanup_journal_group', side_effect=replace), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('cannot delete replaced path')):
            self.run_group(tasks)
        self.assertEqual(path.read_bytes(), b'replaced source')
        outcome = json.loads(self.inventory.db.execute('SELECT body FROM cleanup_execution_outcomes').fetchone()[0])
        self.assertEqual(outcome['refusal_reason'], 'pre_unlink_failure')
        self.assertIs(outcome['unlink_attempted'], False)

    def test_slow_attempt_marker_rechecks_deadline_before_syscall(self):
        tasks = self.group()
        journal, clock, offset = pull.cleanup_journal_group, time.monotonic, [0]
        def slow(inventory, entries):
            journal(inventory, entries)
            if entries[0][2] == 'intent' and entries[0][3].get('unlink_attempted') is True:
                offset[0] += 20
        with mock.patch.object(pull, 'cleanup_journal_group', side_effect=slow), \
                mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('deadline crossed during fsync')):
            self.run_group(tasks)
        outcomes = [json.loads(r[0]) for r in self.inventory.db.execute('SELECT body FROM cleanup_execution_outcomes')]
        self.assertTrue(all(r['refusal_reason']=='deadline_exceeded' and r['refusal_protocol']==1 for r in outcomes))
        status, _, _ = pull.CLEANUP_STATUS.snapshot()
        self.assertEqual((status['last_item_failure_stage'], status['last_item_failure_reason']), ('unlink', 'deadline_exceeded'))

    def test_failed_refusal_checkpoint_cannot_certify_an_intent(self):
        tasks = self.group()
        journal, clock, offset = pull.cleanup_journal_group, time.monotonic, [0]
        def fail(inventory, entries):
            if entries[0][2] == 'result':
                raise OSError('refusal result is not durable')
            journal(inventory, entries)
            if entries[0][2] == 'intent': offset[0] += 40
        with mock.patch.object(pull, 'cleanup_journal_group', side_effect=fail), \
                mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('expired permit')):
            with self.assertRaises(OSError): self.run_group(tasks)
        rows = pull.cleanup_journal(self.inventory)
        self.assertEqual([row[1] for row in rows], ['intent', 'observe', 'observe'])
        self.assertNotIn('refusal_protocol', json.loads(rows[0][2]))
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 0)

    def test_per_file_outcomes_and_timings_survive_uploaded_log_redaction(self):
        tasks = self.group()
        self.run_group(tasks)
        ring = pull.ClientLogRing()
        for level, message in self.logs: ring.record(level, message)
        entries, _ = ring.since(0)
        self.assertGreater(len(entries), 3)
        uploaded = []
        with mock.patch.object(pull, 'CLIENT_LOG_RING', ring), \
                mock.patch.object(pull, 'request_json', side_effect=lambda *a, **kw: uploaded.extend(kw['body']['lines'])):
            pull.client_log_upload_once(self.cfg, type('Runtime', (), {'boot_id': 'fixture'})(), self.inventory, 0)
        self.assertEqual(uploaded, entries)
        for entry in uploaded:
            if entry['message'].startswith('cleanup pre-permit '):
                self.assertLess(len(entry['message']), pull.CLIENT_LOG_LINE_MAX)
                continue
            body = json.loads(entry['message'].split('cleanup execution ', 1)[1])
            self.assertEqual(body['journal_phase'], 'acknowledged')
            self.assertTrue(body['media_unlinked'] and body['sidecar_unlinked'])
            self.assertIn('ledger_result_ms', body['stage_timings'])
            self.assertIn('proof_ms', body['stage_timings'])
            self.assertLess(len(entry['message']), pull.CLIENT_LOG_LINE_MAX)
