"""Crash windows of bounded execution groups, against real disposable files/SQLite."""
import copy
import concurrent.futures
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
    fixture_pins = fixtures.CleanupTests.fixture_pins

    def setUp(self):
        fixtures.CleanupTests.setUp(self)
        self.logs = []
        logger = mock.patch.object(pull, 'log', side_effect=lambda level, message: self.logs.append((level, message)))
        logger.start()
        self.addCleanup(logger.stop)

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
        self.assertEqual(returned, [1, 2])
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
        self.assertEqual(returned, [1, 2, 3])
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
            caps(*args)
            offset[0] += 20  # NAS durability before authority must not age a permit.
        def slow_delete(current, proof):
            offset[0] += 4  # Whole lease takes >30s; each permit remains fresh.
            return delete(current, proof)
        def api(cfg, path, body, **kwargs):
            if path.endswith('/observe-batch'):
                self.assertEqual(len(body['observations']), 1)
                observed.append(body['observations'][0]['item_id'])
                return {'results': [fixture_ack(body['observations'][0], observed[-1], status=200,
                    action='delete_issued', delete=True, plan_paused=False,
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

    def test_partial_ledger_intent_write_preserves_issued_intent_without_unlink(self):
        tasks = self.group()
        real = pull.cleanup_ledger_append_many
        def partial(cfg, rows):
            real(cfg, rows[:1])
            raise OSError('partial append')
        with mock.patch.object(pull, 'cleanup_ledger_append_many', side_effect=partial), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
            with self.assertRaises(OSError): self.run_group(tasks)
        self.assertEqual([r[1] for r in pull.cleanup_journal(self.inventory)], ['intent', 'observe', 'observe'])

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

    def test_local_cap_counts_attempts_once_and_stops_before_excess_permit(self):
        tasks = self.group()
        with mock.patch.object(pull, 'CLEANUP_MAX_FILES_24H', 2):
            with self.assertRaises(pull.CleanupPermitRefused): self.run_group(tasks)
        self.assertEqual([r[1] for r in pull.cleanup_journal(self.inventory)], ['result', 'result', 'observe'])
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 2)

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
