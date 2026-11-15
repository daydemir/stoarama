"""V7 durability ordering on real files and FULL-WAL SQLite; no NAS access."""
import datetime
import io
import json
import os
import select
import subprocess
import sys
import textwrap
import threading
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

import test_cleanup_group as fixtures
pull = fixtures.pull


class CleanupBulkTests(unittest.TestCase):
    setUp = fixtures.CleanupGroupTests.setUp
    group = fixtures.CleanupGroupTests.group
    fixture_pins = fixtures.CleanupGroupTests.fixture_pins

    version = "cleanup-batch-v7"

    def api(self, cfg, path, body, **kwargs):
        if path.endswith('/observe-batch'):
            checkpoints = {item_id: (phase, json.loads(encoded))
                           for item_id, phase, encoded in pull.cleanup_journal(self.inventory)}
            for observation in body['observations']:
                phase, checkpoint = checkpoints[observation['item_id']]
                self.assertEqual(phase, 'intent')
                self.assertIs(checkpoint['unlink_attempted'], True)
                self.assertEqual(pull.cleanup_pre_permit_observation(checkpoint),
                                 {key: value for key, value in observation.items() if key != 'item_id'})
            return {'results': [fixtures.fixture_ack(observation, observation['item_id'], status=200,
                delivery_keep_checked=True, action='delete_issued', delete=True, plan_paused=False,
                expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
                for observation in body['observations']]}
        return fixtures.CleanupGroupTests.api(self, cfg, path, body, **kwargs)

    def run_group(self, tasks):
        with mock.patch.object(pull, 'CLIENT_VERSION', self.version), \
                mock.patch.object(pull, 'restore_api', side_effect=self.api):
            pull.cleanup_process_group(self.cfg, self.inventory, threading.Event(), tasks)

    def phases(self):
        return [row[1] for row in pull.cleanup_journal(self.inventory)]

    def test_slow_durable_intents_do_not_age_the_bulk_permit(self):
        tasks = self.group(8)
        journal, clock, api = pull.cleanup_journal_group, time.monotonic, self.api
        offset = [0.0]
        def slow_journal(inventory, entries):
            journal(inventory, entries)
            if entries[0][2] == 'intent':
                offset[0] += 10.0
        def delayed_response(cfg, path, body, **kwargs):
            result = api(cfg, path, body, **kwargs)
            if path.endswith('/observe-batch'):
                offset[0] += 6.2
            return result
        with mock.patch.object(pull, 'cleanup_journal_group', side_effect=slow_journal), \
                mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(self, 'api', side_effect=delayed_response):
            self.run_group(tasks)
        self.assertFalse(self.phases())
        self.assertTrue(all(not (self.cfg.output_dir/t['item']['relative_path']).exists() for t in tasks))

    def test_live_expiry_before_first_unlink_records_certified_refusals(self):
        tasks = self.group(8)
        guard, clock = pull.cleanup_guard_refusal, time.monotonic
        calls, offset = [0], [0.0]
        def slow_guard(*args):
            result = guard(*args)
            calls[0] += 1
            if calls[0] == len(tasks)+1:
                offset[0] += 16.0
            return result
        with mock.patch.object(pull, 'cleanup_guard_refusal', side_effect=slow_guard), \
                mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('expired permit cannot unlink')):
            self.run_group(tasks)
        outcomes = [json.loads(row[0]) for row in self.inventory.db.execute('SELECT body FROM cleanup_execution_outcomes')]
        self.assertEqual(len(outcomes), 8)
        self.assertTrue(all(row['refusal_protocol'] == 1 and row['refusal_reason'] == 'deadline_exceeded'
                            and row['unlink_attempted'] is False for row in outcomes))
        self.assertFalse(self.phases())

    def lose_permit_response(self, tasks):
        issued, api = {}, self.api
        def lose(cfg, path, body, **kwargs):
            response = api(cfg, path, body, **kwargs)
            if path.endswith('/observe-batch'):
                for observation, decision in zip(body['observations'], response['results']):
                    issued[observation['item_id']] = {
                        'proof_json': json.dumps(observation['proof']),
                        'proof_sha256': decision['proof_sha256'],
                    }
                raise TimeoutError('server issued; response lost')
            return response
        with mock.patch.object(self, 'api', side_effect=lose):
            with self.assertRaises(TimeoutError):
                self.run_group(tasks)
        self.assertEqual(self.phases(), ['intent'] * len(tasks))
        return issued

    def test_restart_replay_binds_to_original_issued_proof_serialization(self):
        tasks = self.group(3)
        issued = self.lose_permit_response(tasks)
        self.inventory.close()
        self.inventory = pull.Inventory(self.cfg)
        self.addCleanup(self.inventory.close)
        received = []
        def replay(cfg, path, body, **kwargs):
            item_id = int(path.split('/')[-2])
            self.assertNotIn(pull.CLEANUP_PRE_PERMIT_OBSERVATION_JSON, body)
            if path.endswith('/reconcile'):
                self.assertEqual(json.dumps(body['proof']), issued[item_id]['proof_json'])
                ack = fixtures.fixture_ack(body, item_id, action='reconcile', delete=False)
            else:
                self.assertTrue(path.endswith('/result'))
                self.assertEqual(body['proof_sha256'], issued[item_id]['proof_sha256'])
                self.assertIs(body['unlink_attempted'], True)
                self.assertNotIn('refusal_protocol', body)
                received.append(item_id)
                ack = fixtures.fixture_ack(body, item_id, ok=True)
            # This is the SHA issued before the restart, not one recomputed
            # from the replayed body; recursive sorting must fail this test.
            ack['proof_sha256'] = issued[item_id]['proof_sha256']
            return ack
        with mock.patch.object(pull, 'restore_api', side_effect=replay), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('replay cannot unlink')):
            result = pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=5)
        self.assertEqual(result['failed'], 0)
        self.assertEqual(sorted(received), sorted(issued))
        self.assertFalse(self.phases())

    def test_armed_reconciliation_failures_retain_complete_checkpoint(self):
        tasks = self.group(3)
        issued = self.lose_permit_response(tasks)
        before = pull.cleanup_journal(self.inventory)
        for fault in ('409', 'lost', 'malformed', 'authority'):
            def replay(cfg, path, body, **kwargs):
                self.assertTrue(path.endswith('/reconcile'))
                self.assertNotIn(pull.CLEANUP_PRE_PERMIT_OBSERVATION_JSON, body)
                if fault == '409':
                    raise urllib.error.HTTPError('https://example.invalid', 409, 'conflict', {}, io.BytesIO(b'{}'))
                if fault == 'lost':
                    raise TimeoutError('reconciliation response lost')
                if fault == 'malformed':
                    return {'ok': True}
                item_id = int(path.split('/')[-2])
                ack = fixtures.fixture_ack(body, item_id, action='delete_issued', delete=True)
                ack['proof_sha256'] = issued[item_id]['proof_sha256']
                return ack
            with self.subTest(fault=fault), mock.patch.object(pull, 'restore_api', side_effect=replay), \
                    mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('replay cannot unlink')):
                result = pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=5)
            self.assertEqual(result['failed'], 3)
            self.assertEqual(pull.cleanup_journal(self.inventory), before)
            self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_rejected_receipts').fetchone()[0], 0)

    def test_armed_result_409_and_lost_ack_retain_wrapper_until_positive_ack(self):
        tasks = self.group(3)
        issued = self.lose_permit_response(tasks)
        mode = ['409']
        def replay(cfg, path, body, **kwargs):
            item_id = int(path.split('/')[-2])
            self.assertNotIn(pull.CLEANUP_PRE_PERMIT_OBSERVATION_JSON, body)
            if path.endswith('/reconcile'):
                ack = fixtures.fixture_ack(body, item_id, action='reconcile', delete=False)
            else:
                self.assertTrue(path.endswith('/result'))
                self.assertEqual(body['proof_sha256'], issued[item_id]['proof_sha256'])
                if mode[0] == '409':
                    raise urllib.error.HTTPError('https://example.invalid', 409, 'conflict', {}, io.BytesIO(b'{}'))
                if mode[0] == 'lost':
                    raise TimeoutError('result acknowledgement lost')
                ack = fixtures.fixture_ack(body, item_id, ok=True)
            ack['proof_sha256'] = issued[item_id]['proof_sha256']
            return ack
        with mock.patch.object(pull, 'restore_api', side_effect=replay):
            self.assertEqual(pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=5)['failed'], 3)
            before = pull.cleanup_journal(self.inventory)
            self.assertEqual([row[1] for row in before], ['result'] * 3)
            self.assertTrue(all(pull.CLEANUP_PRE_PERMIT_OBSERVATION_JSON in json.loads(row[2]) for row in before))
            mode[0] = 'lost'
            self.assertEqual(pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=5)['failed'], 3)
            self.assertEqual(pull.cleanup_journal(self.inventory), before)
            mode[0] = 'ack'
            self.assertEqual(pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=5)['failed'], 0)
        self.assertFalse(self.phases())
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_rejected_receipts').fetchone()[0], 0)

    def test_armed_before_request_reconciles_without_a_permit_or_result(self):
        tasks = self.group(3)
        journal = pull.cleanup_journal_group
        class Interrupted(BaseException):
            pass
        def interrupt(inventory, entries):
            journal(inventory, entries)
            if entries[0][2] == 'intent':
                raise Interrupted()
        with mock.patch.object(pull, 'cleanup_journal_group', side_effect=interrupt), \
                mock.patch.object(self, 'api', side_effect=AssertionError('no permit request before durability')):
            with self.assertRaises(Interrupted):
                self.run_group(tasks)
        def replay(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith('/reconcile'))
            return fixtures.fixture_ack(body, int(path.split('/')[-2]), action='pending', delete=False)
        with mock.patch.object(pull, 'restore_api', side_effect=replay), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('replay cannot unlink')):
            self.assertEqual(pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=5)['failed'], 0)
        self.assertFalse(self.phases())
        self.assertTrue(all((self.cfg.output_dir/task['item']['relative_path']).exists() for task in tasks))

    def test_replayer_cannot_steal_a_live_prearmed_group(self):
        tasks = self.group(3)
        entered, release, errors = threading.Event(), threading.Event(), []
        api, calls = self.api, []
        def transport(cfg, path, body, **kwargs):
            calls.append(path)
            if path.endswith('/observe-batch'):
                entered.set()
                self.assertTrue(release.wait(5))
            return api(cfg, path, body, **kwargs)
        def execute():
            try:
                self.run_group(tasks)
            except BaseException as exc:
                errors.append(exc)
        with mock.patch.object(self, 'api', side_effect=transport):
            worker = threading.Thread(target=execute)
            worker.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertEqual(self.phases(), ['intent'] * 3)
                result = pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=5)
                self.assertEqual((result['replayed'], result['failed']), (0, 0))
                self.assertEqual(len(calls), 1)
            finally:
                release.set()
                worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertFalse(errors)
        self.assertFalse(self.phases())

    def test_one_bulk_request_all_intents_before_unlink_all_syncs_before_success(self):
        tasks = self.group(8)
        calls, dirs, unlinks = [], [], []
        api, sync, unlink = self.api, pull.os.fsync, pull.os.unlink
        def transport(cfg, path, body, **kwargs):
            calls.append((path, len(body.get('observations', []))))
            return api(cfg, path, body, **kwargs)
        def remove(name, **kwargs):
            self.assertEqual(self.phases(), ['intent'] * 8)
            self.assertTrue(all(json.loads(row[2])['unlink_attempted'] is True for row in pull.cleanup_journal(self.inventory)))
            unlinks.append(name)
            return unlink(name, **kwargs)
        def fsync(fd):
            import stat
            if stat.S_ISDIR(os.fstat(fd).st_mode) and unlinks:
                self.assertEqual(len(unlinks), 16)
                self.assertEqual(self.phases(), ['intent'] * 8)
                dirs.append(fd)
            return sync(fd)
        with mock.patch.object(self, 'api', side_effect=transport), \
                mock.patch.object(pull.os, 'unlink', side_effect=remove), \
                mock.patch.object(pull.os, 'fsync', side_effect=fsync):
            self.run_group(tasks)
        self.assertEqual([n for p, n in calls if p.endswith('/observe-batch')], [8])
        self.assertEqual(len(dirs), 1)
        self.assertFalse(self.phases())

    def test_no_unlink_on_partial_or_malformed_or_unchecked_bulk_permits(self):
        for fault in ('partial', 'hold', 'refused', 'identity'):
            tasks = self.group(3)
            api = self.api
            def response(cfg, path, body, **kwargs):
                result = api(cfg, path, body, **kwargs)
                if path.endswith('/observe-batch'):
                    if fault == 'partial': result['results'].pop()
                    if fault == 'hold': result['results'][2].pop('delivery_keep_checked')
                    if fault == 'refused': result['results'][2]['delete'] = False
                    if fault == 'identity': result['results'][2]['attempt'] += 1
                return result
            with self.subTest(fault=fault), mock.patch.object(self, 'api', side_effect=response), \
                    mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('invalid bulk authority')):
                with self.assertRaises((pull.CleanupResponseError, pull.CleanupPermitRefused)):
                    self.run_group(tasks)
            self.assertEqual(self.phases(), ['intent'] * 3)
            # Fresh fixture: unresolved observations are preserved, never replaced.
            if fault != 'identity':
                self.doCleanups()
                self.setUp()

    def test_shared_request_clock_expires_after_first_file_without_renewal(self):
        tasks = self.group(3)
        clock, remove = time.monotonic, pull.os.unlink
        offset = [0]
        def unlink(name, **kwargs):
            result = remove(name, **kwargs)
            if name.endswith('.json'): offset[0] = 16
            return result
        with mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, 'unlink', side_effect=unlink):
            with self.assertRaises(pull.CleanupPermitRefused): self.run_group(tasks)
        self.assertFalse((self.cfg.output_dir/tasks[0]['item']['relative_path']).exists())
        self.assertTrue((self.cfg.output_dir/tasks[1]['item']['relative_path']).exists())
        self.assertEqual(self.phases(), ['result'] * 3)
        rows = [json.loads(row[2]) for row in pull.cleanup_journal(self.inventory)]
        self.assertEqual(rows[0]['outcome'], 'deleted')
        self.assertTrue(rows[0]['unlink_attempted'])
        self.assertTrue(all(row['unlink_attempted'] is False and row['refusal_protocol'] == 1 for row in rows[1:]))
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 1)

    def test_directory_sync_failure_persists_uncertain_parent_results(self):
        tasks = self.group(3)
        sync = pull.os.fsync
        def fail(fd):
            import stat
            if stat.S_ISDIR(os.fstat(fd).st_mode) and not (self.cfg.output_dir/tasks[0]['item']['relative_path']).exists():
                raise OSError('directory sync failed')
            return sync(fd)
        with mock.patch.object(pull.os, 'fsync', side_effect=fail):
            with self.assertRaises(OSError): self.run_group(tasks)
        self.assertEqual(self.phases(), ['result'] * 3)
        self.assertTrue(all(not (self.cfg.output_dir/t['item']['relative_path']).exists() for t in tasks))
        self.assertTrue(all(json.loads(r[2])['outcome'] == 'unverified' for r in pull.cleanup_journal(self.inventory)))
        self.assertTrue(all(json.loads(r[2])['unlink_attempted'] is True for r in pull.cleanup_journal(self.inventory)))
        self.assertTrue(all('refusal_protocol' not in json.loads(r[2]) for r in pull.cleanup_journal(self.inventory)))

    def test_sigkill_at_each_durability_boundary_replays_without_permit_or_unlink(self):
        for cut in ('intent_ledger', 'intent', 'permit', 'media', 'sidecar', 'directory', 'result_ledger', 'result'):
            case = type(self)('test_one_bulk_request_all_intents_before_unlink_all_syncs_before_success')
            case.setUp()
            try:
                tasks = case.group(3)
                script = textwrap.dedent('''
                    import json,sys,threading,stat,os
                    from pathlib import Path
                    from types import SimpleNamespace
                    from unittest import mock
                    import test_cleanup_bulk as fixtures
                    pull=fixtures.pull
                    root,cut,encoded,version=sys.argv[1:]
                    case=fixtures.CleanupBulkTests('test_one_bulk_request_all_intents_before_unlink_all_syncs_before_success')
                    case.cfg=SimpleNamespace(output_dir=Path(root)/'clips',state_dir=Path(root)/'state',
                        inventory_file=Path(root)/'state'/'inventory.sqlite3',dry_run=False)
                    case.inventory,case.logs=pull.Inventory(case.cfg),[]
                    case.version=version
                    api=case.api
                    ledger,journal,unlink,sync=pull.cleanup_ledger_append_many,pull.cleanup_journal_group,pull.os.unlink,pull.os.fsync
                    def halt():
                        print('ready',flush=True);sys.stdin.read();raise AssertionError('must SIGKILL')
                    def transport(cfg,path,body,**kwargs):
                        result=api(cfg,path,body,**kwargs)
                        if path.endswith('/observe-batch'):
                            with (Path(root)/'server-issued.json').open('w') as record:
                                json.dump({str(row['item_id']):row['proof_sha256'] for row in result['results']},record)
                                record.flush();os.fsync(record.fileno())
                            if cut=='permit':halt()
                        return result
                    def append(cfg,records):
                        ledger(cfg,records)
                        if cut == records[0]['event']+'_ledger': halt()
                    def checkpoint(inventory,entries):
                        journal(inventory,entries)
                        if cut == entries[0][2]: halt()
                    def remove(name,**kwargs):
                        unlink(name,**kwargs)
                        if cut == ('sidecar' if name.endswith('.json') else 'media'): halt()
                    def fsync(fd):
                        sync(fd)
                        if cut=='directory' and stat.S_ISDIR(os.fstat(fd).st_mode) and not (case.cfg.output_dir/json.loads(encoded)[0]['item']['relative_path']).exists(): halt()
                    with mock.patch.object(pull.os.path,'ismount',return_value=True), \
                        mock.patch.object(pull,'cleanup_pins',return_value=case.fixture_pins('2027-01-01T00:00:00+00:00','2027-01-29T00:00:00+00:00')), \
                        mock.patch.object(pull,'log',side_effect=lambda l,m:case.logs.append((l,m))), \
                        mock.patch.object(case,'api',side_effect=transport), \
                        mock.patch.object(pull,'cleanup_ledger_append_many',side_effect=append), \
                        mock.patch.object(pull,'cleanup_journal_group',side_effect=checkpoint), \
                        mock.patch.object(pull.os,'unlink',side_effect=remove), \
                        mock.patch.object(pull.os,'fsync',side_effect=fsync):
                        case.run_group(json.loads(encoded))
                ''')
                child = subprocess.Popen([sys.executable, '-c', script, str(case.root), cut, json.dumps(tasks), self.version],
                    cwd=str(Path(__file__).parent), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    self.assertTrue(select.select([child.stdout], [], [], 20)[0], 'child never reached '+cut)
                    line = child.stdout.readline().strip()
                    self.assertEqual(line, 'ready', child.stderr.read() if child.poll() is not None else cut)
                finally:
                    child.kill();child.wait(timeout=5)
                    child.stdin.close();child.stdout.close();child.stderr.close()
                rows = pull.cleanup_journal(case.inventory)
                expected = 'observe' if cut=='intent_ledger' else 'result' if cut=='result' else 'intent'
                self.assertEqual([r[1] for r in rows], [expected]*3)
                marker=case.root/'server-issued.json'
                issued=json.loads(marker.read_text()) if marker.exists() else {}
                self.assertEqual(bool(issued),cut not in ('intent_ledger','intent'))
                received = []
                def replay(cfg,path,body,**kwargs):
                    item_id=int(path.split('/')[-2])
                    if path.endswith('/reconcile'):
                        ack=fixtures.fixture_ack(body,item_id,action='reconcile' if issued else 'pending',delete=False)
                        if issued:ack['proof_sha256']=issued[str(item_id)]
                        return ack
                    self.assertTrue(path.endswith('/result'),path)
                    self.assertTrue(issued)
                    self.assertEqual(body['proof_sha256'],issued[str(item_id)])
                    self.assertNotIn(pull.CLEANUP_PRE_PERMIT_OBSERVATION_JSON,body)
                    received.append(body)
                    return fixtures.fixture_ack(body,item_id,ok=True)
                with mock.patch.object(pull,'restore_api',side_effect=replay), \
                        mock.patch.object(pull.os,'unlink',side_effect=AssertionError('replay cannot unlink')):
                    result=pull.cleanup_replay_receipts(case.cfg,case.inventory)
                self.assertEqual(result['failed'],0)
                if expected=='intent' and issued:
                    self.assertEqual(len(received),3)
                    self.assertTrue(all(b['outcome']=='unverified' and 'refusal_protocol' not in b for b in received))
                    self.assertTrue(all(b['unlink_attempted'] is True for b in received))
                if not issued:
                    self.assertFalse(received)
            finally:
                case.doCleanups()

    def test_first_directory_sync_failure_does_not_hide_other_parent_truth(self):
        self.multiple_parent_sync_failure(1)

    def test_last_directory_sync_failure_does_not_hide_other_parent_truth(self):
        self.multiple_parent_sync_failure(2)

    def multiple_parent_sync_failure(self, failure):
        tasks = self.group(4)
        # Two separate parent inodes, with exact fresh proofs for both.
        for task in tasks[2:]:
            item=task['item']; old=self.cfg.output_dir/item['relative_path']
            item['relative_path']=item['relative_path'].replace('/day/', '/other-day/')
            new=self.cfg.output_dir/item['relative_path'];new.parent.mkdir(exist_ok=True)
            old.rename(new)
            old_sidecar=Path(str(old)+'.stoarama.json');new_sidecar=Path(str(new)+'.stoarama.json')
            binding=json.loads(old_sidecar.read_text());binding['relative_path']=item['relative_path']
            old_sidecar.unlink();new_sidecar.write_text(json.dumps(binding))
            item.update(frozen_identity=pull.cleanup_identity(new.stat()), frozen_sidecar_identity=pull.cleanup_identity(new_sidecar.stat()))
        for task in tasks:
            with pull.CleanupFile(self.cfg,task['item'],threading.Event(),time.monotonic()+60) as current:
                task['approved_proof']=current.proof()
        sync=pull.os.fsync;synced=[]
        def fail_last(fd):
            import stat
            if stat.S_ISDIR(os.fstat(fd).st_mode) and not (self.cfg.output_dir/tasks[0]['item']['relative_path']).exists():
                synced.append((os.fstat(fd).st_dev,os.fstat(fd).st_ino))
                if len(synced)==failure: raise OSError('directory sync failed')
            return sync(fd)
        with mock.patch.object(pull.os,'fsync',side_effect=fail_last):
            with self.assertRaises(OSError):self.run_group(tasks)
        self.assertEqual(len(set(synced)),2)
        self.assertEqual(self.phases(),['result']*4)
        rows = [json.loads(row[2]) for row in pull.cleanup_journal(self.inventory)]
        for n, row in enumerate(rows):
            self.assertEqual(row['outcome'], 'unverified' if n//2+1 == failure else 'deleted')
            self.assertIs(row['unlink_attempted'], True)
            self.assertNotIn('refusal_protocol', row)

    def test_partial_ledger_append_never_certifies_a_deleted_result(self):
        for event in ('intent','result'):
            case=type(self)('test_one_bulk_request_all_intents_before_unlink_all_syncs_before_success');case.setUp()
            try:
                tasks=case.group(3);append=pull.cleanup_ledger_append_many
                def partial(cfg,records):
                    if records[0]['event']!=event:return append(cfg,records)
                    data=b''.join(pull.cleanup_json_bytes(r)+b'\n' for r in records)
                    fd=os.open(str(Path(cfg.state_dir)/pull.CLEANUP_LEDGER_NAME),os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600)
                    try:os.write(fd,data[:len(data)//2]);os.fsync(fd)
                    finally:os.close(fd)
                    raise OSError('partial append')
                with mock.patch.object(pull,'cleanup_ledger_append_many',side_effect=partial):
                    with self.assertRaises(OSError):case.run_group(tasks)
                self.assertEqual(case.phases(),['observe' if event=='intent' else 'intent']*3)
                self.assertTrue(all(json.loads(r[2]).get('outcome')!='deleted' for r in pull.cleanup_journal(case.inventory)))
            finally:case.doCleanups()


class R2CopyBulkTests(CleanupBulkTests):
    version = "r2-copy-v2"


class LegacyNASProofBulkTests(CleanupBulkTests):
    version = "v85r-nas-proof-v1"


class NASProofBulkTests(CleanupBulkTests):
    version = "v85r-nas-proof-v2"


class ItemTruthBulkTests(CleanupBulkTests):
    version = "cleanup-batch-v8"
