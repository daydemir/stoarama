"""Exercise the actual cleanup worker against disposable media fixtures only."""
import importlib.util
import datetime
import errno
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

spec = importlib.util.spec_from_file_location('pull', Path(__file__).with_name('stoarama_pull.py'))
pull = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pull)


def fixture_ack(body, item_id=1, **fields):
    ack = dict(delivery_keep_checked=True,item_id=item_id, attempt=body['attempt'], plan_sha256=body['plan_sha256'],
               proof_sha256=(hashlib.sha256(json.dumps(body.get('proof')).encode()).hexdigest()
                             if 'proof_sha256' not in body else body['proof_sha256']))
    ack.update(fields)
    return ack


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root/'state').mkdir()
        (self.root/'clips'/'stream'/'day').mkdir(parents=True)
        self.cfg = SimpleNamespace(output_dir=self.root/'clips', state_dir=self.root/'state',
                                   inventory_file=self.root/'state'/'inventory.sqlite3', dry_run=False)
        self.inventory = pull.Inventory(self.cfg)
        self.addCleanup(self.inventory.close)
        self.path = self.cfg.output_dir/'stream/day/a.mp4'
        self.path.write_bytes(b'fixture media')
        self.item = dict(kind='raw', source_id=11, recording_id=22, stream_id=33,
                         relative_path='stream/day/a.mp4', start_at='2026-09-01T00:00:00+00:00', end_at='2026-09-01T00:01:00+00:00', size_bytes=self.path.stat().st_size,
                         sha256=hashlib.sha256(self.path.read_bytes()).hexdigest(), object_key='fixture', sidecar_required=True)
        self.binding = dict(clip_id=11, recording_id=22, relative_path=self.item['relative_path'],
                            size_bytes=self.item['size_bytes'], sha256=self.item['sha256'],clip_start_at=self.item['start_at'],clip_end_at=self.item['end_at'])
        self.sidecar = Path(str(self.path)+'.stoarama.json')
        self.sidecar.write_text(json.dumps(dict(schema_version=1, **self.binding)))
        self.item['frozen_identity'] = pull.cleanup_identity(self.path.stat())
        self.item['frozen_sidecar_identity'] = pull.cleanup_identity(self.sidecar.stat())
        self.task = dict(item_id=1, attempt=1, plan_sha256='a'*64, item=self.item,
                         expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=30)).isoformat())
        self.mount = mock.patch.object(pull.os.path, 'ismount', return_value=True)
        self.mount.start()
        self.addCleanup(self.mount.stop)
        # Disposable fixture pins: folder "stream" is stream 33, whose keep
        # window lies far from the fixture clip. Guard tests override them.
        self.pins = self.fixture_pins('2027-01-01T00:00:00+00:00', '2027-01-29T00:00:00+00:00')
        self.pinned = mock.patch.object(pull, 'cleanup_pins', side_effect=lambda: self.pins)
        self.pinned.start()
        self.addCleanup(self.pinned.stop)

    def fixture_pins(self, start, end, folders=None):
        parse = lambda v: datetime.datetime.fromisoformat(v)
        return (dict(windows={33: (parse(start), parse(end), 'UTC')}, folders=folders or {'stream': 33}), None)

    def opened(self):
        return pull.CleanupFile(self.cfg, self.item, threading.Event(), time.monotonic()+20)

    def test_readonly_worker_exact_proof_and_durable_ack(self):
        calls=[]
        def api(cfg,path,body, **kwargs):
            calls.append((path,body))
            if path.endswith('/lease'): return dict(tasks=[self.task],retry_after_sec=0)
            self.assertEqual(len(pull.cleanup_journal(self.inventory)),1)
            self.assertEqual(body['reconciliation_protocol'], pull.CLEANUP_RECONCILIATION_PROTOCOL)
            self.assertEqual(body['outcome'],'match')
            self.assertEqual(body['proof']['identity'],self.item['frozen_identity'])
            return fixture_ack(body, action='would_delete',delete=False,recovery='nas_only_at_check')
        with mock.patch.object(pull,'restore_api',side_effect=api), mock.patch.object(pull.os,'unlink',side_effect=AssertionError('dry run must never unlink')):
            pull.cleanup_once(self.cfg,self.inventory,threading.Event())
        self.assertTrue(self.path.exists());self.assertTrue(self.sidecar.exists())
        self.assertEqual(pull.cleanup_journal(self.inventory),[])
        self.assertEqual(len(calls),2)

    def test_transport_uses_live_session_for_saved_body_on_both_transports(self):
        import io
        cfg = SimpleNamespace(api_base='https://example.invalid', api_key='fixture')
        saved = dict(request_id='a' * 32, client_version='saved-version', boot_id='saved-boot')
        original = json.dumps(saved, sort_keys=True)
        captured = []
        def urlopen(request, **kwargs):
            captured.append(dict(request.header_items()))
            return io.BytesIO(b'{}')
        with mock.patch.object(pull.urllib.request, 'getproxies', return_value={}), \
                mock.patch.object(pull.urllib.request, 'urlopen', side_effect=urlopen), \
                mock.patch.object(pull, 'restore_https', return_value=(200, b'{}')) as https:
            for boot, version in [('new-boot', 'new-version'), ('rollback-boot', 'old-version')]:
                with mock.patch.object(pull, 'boot_id', return_value=boot), mock.patch.object(pull, 'CLIENT_VERSION', version):
                    for suffix in ('lease', '1/observe', 'observe-batch', '1/reconcile'):
                        path = '/account/connections/cleanup/' + suffix
                        pull.request_json(cfg, 'POST', path, body=saved)
                        self.assertEqual(captured[-1]['X-stoarama-client-session'], boot + ' ' + version)
                        pull.restore_api(cfg, path, saved)
                        self.assertEqual(https.call_args.args[4]['X-Stoarama-Client-Session'], boot + ' ' + version)
            pull.request_json(cfg, 'POST', '/account/connections/heartbeat', body=saved)
            self.assertNotIn('X-stoarama-client-session', captured[-1])
        self.assertEqual(json.dumps(saved, sort_keys=True), original, 'transport persisted live session into checkpoint')

    def test_leases_advertise_reconciliation_only_replay_capability(self):
        for protocol in (1, 2):
            with self.subTest(protocol=protocol), mock.patch.object(pull, 'restore_api', return_value=dict(tasks=[], retry_after_sec=120)) as api:
                if protocol == 1:
                    pull.cleanup_lease(self.cfg, 1)
                else:
                    pull.cleanup_lease_v2(self.cfg, 64, 64, 1 << 30)
                body = api.call_args.args[2]
                self.assertEqual(body['protocol'], protocol)
                self.assertEqual(body['reconciliation_protocol'], 1)

    def test_approved_disposable_fixture_only_deletes_exact_files(self):
        with self.opened() as f: proof=f.proof()
        self.task['approved_proof']=proof
        extra=self.path.parent/'keep.txt';extra.write_text('keep')
        # Re-observe the complete parent membership after introducing retained data.
        with self.opened() as f: self.task['approved_proof']=f.proof()
        def api(cfg,path,body, **kwargs):
            if path.endswith('/lease'):return dict(tasks=[self.task],retry_after_sec=0)
            if path.endswith('/observe'):
                return dict(delivery_keep_checked=True,action='delete_issued',delete=True,item_id=1,attempt=1,plan_sha256='a'*64,
                            proof_sha256=hashlib.sha256(json.dumps(body['proof']).encode()).hexdigest(),
                            expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
            self.assertEqual(body['outcome'],'deleted');return fixture_ack(body, int(path.split("/")[-2]), ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api):pull.cleanup_once(self.cfg,self.inventory,threading.Event())
        self.assertFalse(self.path.exists());self.assertFalse(self.sidecar.exists());self.assertTrue(extra.exists())
        self.assertTrue(extra.parent.is_dir());self.assertEqual(pull.cleanup_journal(self.inventory),[])

    def test_unsolicited_delete_without_execution_phase_preserves_media(self):
        def api(cfg,path,body, **kwargs):
            if path.endswith('/lease'):return dict(tasks=[self.task],retry_after_sec=0)
            if path.endswith('/observe'):return dict(delete=True,action='delete_issued')
            self.assertEqual(body['outcome'],'unverified');return fixture_ack(body, int(path.split("/")[-2]), ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api), self.assertRaises(pull.CleanupResponseError):
            pull.cleanup_once(self.cfg,self.inventory,threading.Event())
        self.assertEqual(pull.cleanup_journal(self.inventory)[0][1], 'observe')
        self.assertTrue(self.path.exists())

    def test_reject_path_traversal_and_symlinked_ancestor(self):
        self.item['relative_path']='../outside'
        with self.assertRaises(RuntimeError),self.opened():pass
        self.item['relative_path']='alias/day/a.mp4'
        (self.cfg.output_dir/'alias').symlink_to(self.cfg.output_dir/'stream',target_is_directory=True)
        with self.assertRaises(OSError),self.opened():pass

    def test_reject_final_symlink_hardlink_and_device_crossing(self):
        self.path.unlink();self.path.symlink_to(self.sidecar)
        with self.assertRaises(OSError),self.opened():pass
        self.path.unlink();self.path.write_bytes(b'fixture media');os.link(self.path,self.path.with_suffix('.copy'))
        self.item['frozen_identity']=pull.cleanup_identity(self.path.stat())
        with self.assertRaises(RuntimeError),self.opened():pass

    def test_hash_sidecar_source_and_membership_changes_fail_closed(self):
        with self.opened() as f:
            proof=f.proof()
            (self.path.parent/'new.mp4').write_bytes(b'unknown')
            with self.assertRaises(RuntimeError):f.delete_exact(proof)
        self.assertTrue(self.path.exists())
        side=dict(schema_version=1,**self.binding);side['clip_id']=99
        self.sidecar.write_text(json.dumps(side));self.item['frozen_sidecar_identity']=pull.cleanup_identity(self.sidecar.stat())
        with self.assertRaises(RuntimeError),self.opened() as f:f.proof()

    def test_ancestor_swap_and_equal_size_replacement_rejected(self):
        with self.opened() as f:
            old=self.path.parent
            moved=old.with_name('moved');old.rename(moved);old.mkdir()
            with self.assertRaises(RuntimeError):f.check_paths()
        moved.rename(old.with_name('other'))

    def test_cancel_and_update_gate_preserve_files(self):
        event=threading.Event();event.set()
        with self.assertRaises(RuntimeError),pull.CleanupFile(self.cfg,self.item,event,time.monotonic()+20):pass
        ready=threading.Event();ready.set()
        worker=SimpleNamespace(is_alive=lambda:False);cleanup=SimpleNamespace(is_alive=lambda:True)
        self.assertFalse(pull.update_can_exec(ready,worker,joined_worker=worker,cleanup_worker=cleanup))

    def test_lost_observe_ack_replayed_without_delete(self):
        with self.opened() as f:proof=f.proof()
        body=dict(attempt=1,plan_sha256='a'*64,outcome='match',proof=proof,error='',client_version='test')
        pull.cleanup_journal(self.inventory,1,'observe',body)
        calls=[]
        def api(cfg,path,body, **kwargs):
            calls.append(path)
            if path.endswith('/reconcile'):return fixture_ack(body, action='reconcile', delete=False)
            self.assertEqual(body['outcome'],'unverified');return fixture_ack(body, int(path.split("/")[-2]), ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api),mock.patch.object(pull.os,'unlink',side_effect=AssertionError('replay must not delete')):
            pull.cleanup_replay_receipts(self.cfg,self.inventory)
        self.assertEqual(len(calls),2);self.assertTrue(self.path.exists());self.assertFalse(pull.cleanup_journal(self.inventory))

    def test_503_before_issuance_replays_only_to_pending_without_receipt(self):
        with self.opened() as current:
            self.task['approved_proof'] = current.proof()
        for protocol in (1, 2):
            with self.subTest(protocol=protocol):
                calls = []
                def api(cfg, path, body, **kwargs):
                    calls.append(path)
                    if path.endswith('/observe'):
                        raise pull.urllib.error.HTTPError(path, 503, 'policy deadline', {}, None)
                    self.assertTrue(path.endswith('/reconcile'), path)
                    return fixture_ack(body, action='pending', delete=False)
                with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not unlink')):
                    batch_api = pull.CleanupBatchAPI(self.cfg) if protocol == 2 else None
                    try:
                        with self.assertRaises(pull.urllib.error.HTTPError):
                            pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), self.task, batch_api)
                        self.assertEqual(pull.cleanup_journal(self.inventory)[0][1], 'observe')
                        counts = pull.cleanup_replay_receipts(self.cfg, self.inventory, batch_api)
                    finally:
                        if batch_api is not None: batch_api.close()
                self.assertEqual(counts['failed'], 0)
                self.assertEqual(calls, ['/account/connections/cleanup/1/observe', '/account/connections/cleanup/1/reconcile'])
                self.assertEqual(pull.cleanup_journal(self.inventory), [])
                self.assertTrue(self.path.exists()); self.assertTrue(self.sidecar.exists())

    def test_reconciliation_rejects_any_deletion_authority_and_keeps_journal(self):
        body = dict(attempt=1, plan_sha256='a'*64, outcome='match', proof=None, error='')
        pull.cleanup_journal(self.inventory, 1, 'observe', body)
        original = pull.cleanup_journal(self.inventory)
        with mock.patch.object(pull, 'restore_api', return_value=fixture_ack(body, delivery_keep_checked=True, action='delete_issued', delete=True)) as api:
            counts = pull.cleanup_replay_receipts(self.cfg, self.inventory)
        self.assertEqual(counts['failed'], 1)
        self.assertEqual(api.call_count, 1)
        self.assertEqual(pull.cleanup_journal(self.inventory), original)

    def test_interrupted_intent_never_retries_unlink(self):
        pull.cleanup_journal(self.inventory,1,'intent',dict(attempt=1,plan_sha256='a'*64,proof_sha256='b'*64))
        with mock.patch.object(pull,'restore_api',side_effect=lambda cfg, path, body: fixture_ack(body, ok=True)) as api:
            pull.cleanup_replay_receipts(self.cfg,self.inventory)
        self.assertEqual(api.call_args.args[2]['outcome'],'unverified');self.assertTrue(self.path.exists())
        self.assertNotIn('unlink_attempted', api.call_args.args[2])
        self.assertNotIn('refusal_reason', api.call_args.args[2])
        self.assertNotIn('refusal_protocol', api.call_args.args[2])

    def test_inventory_hash_yields_between_chunks_and_cancels_while_paused(self):
        body = b'x' * (3 * 1024 * 1024)
        self.path.write_bytes(body)
        stop = threading.Event()
        fence = pull.NASMediaFence()
        reached, release = threading.Event(), threading.Event()
        reads = []
        results = []
        original_wait = fence.wait_maintenance
        def checkpoint(event):
            reads.append(True)
            if len(reads) == 2:
                reached.set()
                release.wait(2)
            return original_wait(event)
        def hash_file():
            try:
                results.append(pull.sha256_file_throttled(self.path, 1000, stop))
            except pull.InventoryScanStopped:
                results.append('stopped')
        with mock.patch.object(pull, 'NAS_MEDIA_FENCE', fence), mock.patch.object(fence, 'wait_maintenance', side_effect=checkpoint):
            thread = threading.Thread(target=hash_file)
            thread.start()
            self.assertTrue(reached.wait(1))
            with fence.condition:
                fence.execution_owner = threading.get_ident()
            release.set()
            thread.join(.05)
            self.assertTrue(thread.is_alive(), 'in-flight inventory hash ignored cleanup lane')
            stop.set()
            thread.join(2)
        self.assertEqual(results, ['stopped'])
        self.assertEqual(len(reads), 2)

    def test_writer_fence_waits_for_delivery_without_serializing_deliveries(self):
        fence=pull.NASMediaFence();entered=threading.Event();release=threading.Event()
        @fence.delivery
        def writer():entered.set();release.wait(3)
        thread=threading.Thread(target=writer);thread.start();self.assertTrue(entered.wait(1))
        with self.assertRaises(RuntimeError):fence.acquire_cleanup(threading.Event(),time.monotonic()+0.05)
        release.set();thread.join(1)
        fence.acquire_cleanup(threading.Event(),time.monotonic()+1);fence.release_cleanup()

    def test_execution_lane_reserves_io_until_idle_and_yields_for_maintenance(self):
        fence = pull.NASMediaFence()
        stop = threading.Event()
        entered = threading.Event()
        reader = threading.Event()
        @fence.delivery
        def writer():
            entered.set()
        def inventory():
            if fence.wait_maintenance(stop):
                reader.set()
        with mock.patch.object(pull, 'NAS_MEDIA_FENCE', fence):
            lane = pull.CleanupExecutionLane(stop)
            try:
                lane.run(lambda: None)
                threads = [threading.Thread(target=writer), threading.Thread(target=inventory)]
                for worker in threads:
                    worker.start()
                self.assertFalse(entered.wait(0.05))
                self.assertFalse(reader.is_set())
                # Nested per-file fences admit the lane owner even with a
                # delivery writer queued; no work slips between receipts.
                for _ in range(3):
                    fence.acquire_cleanup(stop, time.monotonic()+1)
                    fence.release_cleanup()
                    lane.before_lease()
                    self.assertFalse(entered.is_set())
                with mock.patch.object(pull.time, 'monotonic', return_value=lane.started+301), \
                        mock.patch.object(stop, 'wait', side_effect=lambda seconds: not entered.wait(1)) as wait:
                    lane.before_lease()
                wait.assert_called_once_with(pull.CLEANUP_MAINTENANCE_WINDOW_SEC)
                self.assertIsNone(lane.started)
                self.assertTrue(reader.wait(1))
            finally:
                lane.close()
                for worker in threads:
                    worker.join(1)
        self.assertEqual(fence.cleaners, 0)
        self.assertIsNone(fence.execution_owner)

    def test_execution_lane_cancellation_releases_waiting_delivery(self):
        fence = pull.NASMediaFence()
        stop = threading.Event()
        with mock.patch.object(pull, 'NAS_MEDIA_FENCE', fence):
            lane = pull.CleanupExecutionLane(stop)
            try:
                lane.run(lambda: None)
                stop.set()
                with mock.patch.object(pull.time, 'monotonic', return_value=lane.started+301):
                    with self.assertRaises(pull.CleanupInterrupted):
                        lane.before_lease()
            finally:
                lane.close()
        self.assertEqual(fence.cleaners, 0)
        self.assertIsNone(fence.execution_owner)

    def test_execution_drain_timeout_reopens_background_work_for_maintenance(self):
        fence, stop = pull.NASMediaFence(), threading.Event()
        active, release, resumed, next_joined = (threading.Event() for _ in range(4))
        @fence.background_delivery
        def joined():
            active.set()
            release.wait(2)
        def inventory():
            if fence.wait_maintenance(stop):
                resumed.set()
        worker = threading.Thread(target=joined)
        worker.start()
        self.assertTrue(active.wait(1))
        reader = threading.Thread(target=inventory)
        queued = threading.Thread(target=fence.background_delivery(next_joined.set))
        acquire = fence.acquire_cleanup
        def drain(*args):
            reader.start()
            queued.start()
            self.assertFalse(resumed.wait(.02))
            self.assertFalse(next_joined.is_set(), 'new joined writer entered during drain')
            return acquire(*args)
        with mock.patch.object(pull, 'NAS_MEDIA_FENCE', fence), \
                mock.patch.object(pull, 'CLEANUP_EXECUTION_DRAIN_SEC', .1), \
                mock.patch.object(fence, 'acquire_cleanup', side_effect=drain):
            lane = pull.CleanupExecutionLane(stop)
            try:
                with self.assertRaises(pull.CleanupInterrupted):
                    lane.run(lambda: self.fail('execution ran beside joined writer'))
                self.assertTrue(resumed.wait(1), 'maintenance remained fenced after drain timeout')
                self.assertTrue(next_joined.wait(1), 'joined admission remained fenced after drain timeout')
                self.assertIsNone(lane.started)
                self.assertTrue(fence.execution_lock.acquire(blocking=False))
                fence.execution_lock.release()
            finally:
                release.set()
                lane.close()
                worker.join(1)
                reader.join(1)
                queued.join(1)
        self.assertIsNone(fence.execution_owner)
        self.assertEqual(fence.cleaners, 0)

    def test_unused_execution_lease_return_is_durable_and_replays_only_reconcile(self):
        with self.opened() as current:
            self.task['approved_proof'] = current.proof()
        lane = mock.Mock()
        lane.run.side_effect = pull.CleanupDrainTimeout('long joined download')
        import urllib.error
        error = urllib.error.HTTPError('https://example.test', 502, 'Bad Gateway', {}, None)
        with mock.patch.object(pull, 'restore_api', side_effect=error) as api, \
                mock.patch.object(pull.time, 'sleep'), mock.patch.object(pull.os, 'unlink') as unlink:
            with self.assertRaises(urllib.error.HTTPError):
                pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), self.task, lane=lane)
        unlink.assert_not_called()
        self.assertTrue(all(call.args[1].endswith('/reconcile') for call in api.call_args_list))
        journal = pull.cleanup_journal(self.inventory)
        self.assertEqual(len(journal), 1)
        self.assertEqual(journal[0][1], 'observe')
        body = json.loads(journal[0][2])
        self.assertTrue(body['return_lease'])
        self.assertIsNone(body['proof'])
        with mock.patch.object(pull, 'restore_api', side_effect=lambda cfg, path, body: fixture_ack(body, action='pending', delete=False)) as replay:
            pull.cleanup_replay_receipts(self.cfg, self.inventory)
        self.assertTrue(replay.call_args.args[1].endswith('/reconcile'))
        self.assertFalse(pull.cleanup_journal(self.inventory))
        self.assertTrue(self.path.exists())
        self.assertTrue(self.sidecar.exists())

    def test_interrupted_uncertain_observation_never_returns_execution_lease(self):
        with self.opened() as current:
            self.task['approved_proof'] = current.proof()
        with mock.patch.object(pull, 'restore_api', side_effect=pull.CleanupInterrupted('transport interrupted')) as api:
            with self.assertRaises(pull.CleanupInterrupted):
                pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), self.task)
        self.assertEqual(api.call_count, 1)
        self.assertTrue(api.call_args.args[1].endswith('/observe'))
        body = json.loads(pull.cleanup_journal(self.inventory)[0][2])
        self.assertNotIn('return_lease', body)
        self.assertTrue(self.path.exists())

    def test_interrupted_dry_lease_returns_durably_and_lost_ack_only_reconciles(self):
        stop = threading.Event(); stop.set()
        sent = []
        def lost(cfg, path, body, **kwargs):
            sent.append((path, json.dumps(body, sort_keys=True)))
            raise pull.urllib.error.HTTPError(path, 502, 'lost return acknowledgement', {}, None)
        with mock.patch.object(pull, 'restore_api', side_effect=lost), mock.patch.object(pull.time, 'sleep'):
            with self.assertRaises(pull.urllib.error.HTTPError):
                pull.cleanup_process_task(self.cfg, self.inventory, stop, self.task)
        journal = pull.cleanup_journal(self.inventory)
        self.assertEqual(len(journal), 1)
        body = json.loads(journal[0][2])
        self.assertTrue(body['return_lease'])
        self.assertIsNone(body['proof'])
        self.assertTrue(all(path.endswith('/reconcile') for path, _ in sent))
        self.assertEqual(len({encoded for _, encoded in sent}), 1)
        with mock.patch.object(pull, 'restore_api', side_effect=lambda cfg, path, body: fixture_ack(body, action='pending', delete=False)) as replay:
            pull.cleanup_replay_receipts(self.cfg, self.inventory)
        self.assertTrue(replay.call_args.args[1].endswith('/reconcile'))
        self.assertEqual(replay.call_args.args[2], body)
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        self.assertTrue(self.path.exists())
        self.assertTrue(self.sidecar.exists())

    def test_interrupted_dry_observation_keeps_its_existing_checkpoint(self):
        with mock.patch.object(pull, 'restore_api', side_effect=pull.CleanupInterrupted('transport interrupted')) as api:
            with self.assertRaises(pull.CleanupInterrupted):
                pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), self.task)
        self.assertEqual(api.call_count, 1)
        self.assertTrue(api.call_args.args[1].endswith('/observe'))
        body = json.loads(pull.cleanup_journal(self.inventory)[0][2])
        self.assertNotIn('return_lease', body)
        self.assertTrue(self.path.exists())

    def test_cleanup_loop_retries_writer_drain_within_seconds(self):
        stop = mock.Mock()
        stop.wait.side_effect = [False, False, True]
        with mock.patch.object(pull, 'CleanupBatchAPI') as api, \
                mock.patch.object(pull, 'cleanup_batch_v2', side_effect=[pull.CleanupDrainTimeout('long delivery'), 120]), \
                mock.patch.object(pull, 'restore_wait', return_value=120):
            pull.cleanup_loop(self.cfg, self.inventory, stop)
        self.assertEqual([call.args[0] for call in stop.wait.call_args_list], [15, 2, 120])
        api.return_value.close.assert_called_once()

    def test_stale_observe_and_result_receipts_are_archived_without_blocking(self):
        import urllib.error
        for phase in ('observe','result'):
            with self.subTest(phase=phase):
                body=dict(attempt=1,plan_sha256='a'*64,proof_sha256='b'*64,outcome='unverified')
                pull.cleanup_journal(self.inventory,1,phase,body)
                error=urllib.error.HTTPError('https://example.test',409,'stale',{},None)
                with mock.patch.object(pull,'restore_api',side_effect=error):
                    pull.cleanup_replay_receipts(self.cfg,self.inventory)
                self.assertFalse(pull.cleanup_journal(self.inventory))
        rows=self.inventory.db.execute('SELECT phase,body FROM cleanup_rejected_receipts').fetchall()
        self.assertEqual(len(rows),2);self.assertEqual({r[0] for r in rows},{'observe','result'})
        self.assertTrue(self.path.exists())

    def test_cancelled_lease_is_not_reported_as_file_conflict(self):
        event=threading.Event();event.set()
        def reply(cfg, path, body, **kwargs):
            if path.endswith('/lease'):
                return dict(tasks=[self.task], retry_after_sec=0)
            self.assertTrue(path.endswith('/reconcile'))
            self.assertTrue(body['return_lease'])
            return fixture_ack(body, action='pending', delete=False)
        with mock.patch.object(pull,'restore_api',side_effect=reply) as api:
            with self.assertRaises(pull.CleanupInterrupted):pull.cleanup_once(self.cfg,self.inventory,event)
        self.assertEqual(api.call_count,2)
        self.assertFalse(pull.cleanup_journal(self.inventory));self.assertTrue(self.path.exists())

    def test_late_permit_rejected_independently_of_wall_clock(self):
        with self.opened() as f:self.task['approved_proof']=f.proof()
        original=time.monotonic;offset=[0]
        def api(cfg,path,body, **kwargs):
            if path.endswith('/lease'):return dict(tasks=[self.task],retry_after_sec=0)
            if path.endswith('/observe'):
                offset[0]=35
                return dict(delivery_keep_checked=True,action='delete_issued',delete=True,item_id=1,attempt=1,plan_sha256='a'*64,
                            proof_sha256=hashlib.sha256(json.dumps(body['proof']).encode()).hexdigest(),
                            expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
            self.assertEqual(body['outcome'], 'unverified')
            self.assertEqual(body['refusal_reason'], 'deadline_exceeded')
            self.assertFalse(body['unlink_attempted'])
            self.assertGreaterEqual(body['observe_elapsed_ms'], 35000)
            self.assertGreaterEqual(body['execution_elapsed_ms'], body['observe_elapsed_ms'])
            self.assertIn('deadline exceeded', body['error'])
            return fixture_ack(body, int(path.split("/")[-2]), ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api),mock.patch.object(pull.time,'monotonic',side_effect=lambda:original()+offset[0]), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('late permit must never unlink')):
            pull.cleanup_once(self.cfg,self.inventory,threading.Event())
        self.assertTrue(self.path.exists());self.assertTrue(self.sidecar.exists())


    def test_permit_refusal_reasons_are_distinct(self):
        with self.opened() as current:
            proof = current.proof()
        decision = fixture_ack(dict(attempt=1, plan_sha256='a'*64, proof=proof), delivery_keep_checked=True, action='delete_issued', delete=True,
                               expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
        expired = dict(decision, expires_at=(datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(seconds=1)).isoformat())
        for response, reason in ((dict(decision, proof_sha256='b'*64), 'proof_mismatch'), (expired, 'permit_expired'), (dict(decision, delivery_keep_checked=False), 'local_guard'),
                                 (dict(decision, delivery_keep_checked=None), 'local_guard')):
            with self.subTest(reason=reason), self.assertRaises(pull.CleanupPermitRefused) as caught:
                pull.cleanup_check_permit(self.cfg, True, self.task, proof, response, time.monotonic())
            self.assertEqual(caught.exception.reason, reason)

    def test_expired_permit_receipt_is_never_attempted(self):
        with self.opened() as current:
            self.task['approved_proof'] = current.proof()
        receipts = []
        def api(cfg, path, body, **kwargs):
            if path.endswith('/observe'):
                return fixture_ack(body, delivery_keep_checked=True, action='delete_issued', delete=True,
                                   expires_at=(datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(seconds=1)).isoformat())
            receipts.append(body)
            return fixture_ack(body, ok=True)
        with mock.patch.object(pull, 'restore_api', side_effect=api), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('expired permit must never unlink')):
            pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), self.task)
        self.assertEqual(receipts[0]['refusal_reason'], 'permit_expired')
        self.assertFalse(receipts[0]['unlink_attempted'])
        self.assertIn('permit expired', receipts[0]['error'])

    def test_slow_intent_journal_is_timed_and_never_unlinks(self):
        with self.opened() as current:
            self.task['approved_proof'] = current.proof()
        clock, journal = time.monotonic, pull.cleanup_journal
        offset = [0]
        receipts = []
        def slow_journal(inventory, item_id=None, phase=None, body=None, owner=None):
            if phase == 'intent':
                self.assertNotIn('unlink_attempted', body)
                offset[0] += 40
            return journal(inventory, item_id, phase, body, owner)
        def api(cfg, path, body, **kwargs):
            if path.endswith('/observe'):
                return fixture_ack(body, delivery_keep_checked=True, action='delete_issued', delete=True,
                                   expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
            receipts.append(body)
            return fixture_ack(body, ok=True)
        with mock.patch.object(pull, 'restore_api', side_effect=api), \
                mock.patch.object(pull, 'cleanup_journal', side_effect=slow_journal), \
                mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('late permit must not unlink')):
            pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), self.task)
        receipt = receipts[0]
        self.assertEqual(receipt['refusal_reason'], 'deadline_exceeded')
        self.assertFalse(receipt['unlink_attempted'])
        self.assertEqual(receipt['refusal_protocol'], 1)
        self.assertGreaterEqual(receipt['stage_timings']['intent_journal_ms'], 40000)
        self.assertGreaterEqual(receipt['execution_elapsed_ms'], 40000)
        self.assertTrue(self.path.exists()); self.assertTrue(self.sidecar.exists())

    def test_execution_transport_failure_is_not_retried_or_unlinked(self):
        with self.opened() as current:
            self.task['approved_proof'] = current.proof()
        api = pull.CleanupBatchAPI(self.cfg)
        self.addCleanup(api.close)
        with mock.patch.object(pull, 'restore_api', side_effect=TimeoutError('observation socket timed out')) as request, \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('timeout must never unlink')):
            with self.assertRaises(TimeoutError):
                pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), self.task, api)
        self.assertEqual(request.call_count, 1)
        self.assertTrue(request.call_args.args[1].endswith('/1/observe'))
        self.assertEqual(request.call_args.kwargs, dict(timeout=pull.CLEANUP_EXECUTION_OBSERVE_SEC))
        self.assertEqual(pull.cleanup_journal(self.inventory)[0][1], 'observe')
        outcome = json.loads(self.inventory.db.execute('SELECT body FROM cleanup_execution_outcomes').fetchone()[0])
        self.assertFalse(outcome['unlink_attempted'])
        self.assertIsInstance(outcome['observe_elapsed_ms'], int)
        self.assertGreaterEqual(outcome['execution_elapsed_ms'], outcome['observe_elapsed_ms'])
        self.assertTrue(self.path.exists()); self.assertTrue(self.sidecar.exists())

    def test_eighteen_second_observe_never_unlinks(self):
        with self.opened() as current:
            self.task['approved_proof'] = proof = current.proof()
        decision = fixture_ack({'attempt': 1, 'plan_sha256': 'a'*64, 'proof': proof},
                               delivery_keep_checked=True, action='delete_issued', delete=True,
                               expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
        with self.assertRaises(pull.CleanupPermitRefused) as refused:
            pull.cleanup_check_permit(self.cfg, True, self.task, proof, decision, time.monotonic()-18.3)
        self.assertEqual(refused.exception.reason, 'deadline_exceeded')
        self.assertTrue(self.path.exists()); self.assertTrue(self.sidecar.exists())

    def test_permit_deadline_checked_after_slow_final_directory_walk(self):
        with self.opened() as f:self.task['approved_proof']=f.proof()
        original_clock=time.monotonic;original_members=pull.CleanupFile.list_parent
        offset=[0];permit_seen=[False]
        def members(current):
            result=original_members(current)
            if permit_seen[0]:offset[0]=31
            return result
        def api(cfg,path,body, **kwargs):
            if path.endswith('/lease'):return dict(tasks=[self.task],retry_after_sec=0)
            if path.endswith('/observe'):
                permit_seen[0]=True
                return dict(delivery_keep_checked=True,action='delete_issued',delete=True,item_id=1,attempt=1,plan_sha256='a'*64,
                            proof_sha256=hashlib.sha256(json.dumps(body['proof']).encode()).hexdigest(),
                            expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
            self.assertEqual(body['outcome'], 'unverified')
            self.assertEqual(body['refusal_reason'], 'deadline_exceeded')
            self.assertFalse(body['unlink_attempted'])
            return fixture_ack(body, int(path.split("/")[-2]), ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api),mock.patch.object(pull.time,'monotonic',side_effect=lambda:original_clock()+offset[0]),mock.patch.object(pull.CleanupFile,'list_parent',members):
            pull.cleanup_once(self.cfg,self.inventory,threading.Event())
        self.assertTrue(self.path.exists());self.assertTrue(self.sidecar.exists())


    def test_many_unlisted_siblings_do_not_require_metadata_reads(self):
        names=set()
        for n in range(2000):
            name="retained-%04d.txt" % n
            (self.path.parent/name).write_bytes(b"keep")
            names.add(name)
        original=pull.os.stat
        def checked(name,*args,**kwargs):
            if isinstance(name,str) and name in names:
                raise AssertionError("unrelated sibling metadata must not be read")
            return original(name,*args,**kwargs)
        with mock.patch.object(pull.os,"stat",side_effect=checked),self.opened() as current:
            proof=current.proof()
            self.assertGreaterEqual(current.stage_timings["namespace_ms"],0)
            self.assertGreaterEqual(current.stage_timings["hash_ms"],0)
            current.delete_exact(proof)
        self.assertFalse(self.path.exists())
        self.assertEqual(len(list(self.path.parent.iterdir())),2000)

    def test_sibling_content_changes_never_expand_exact_delete_scope(self):
        sibling=self.path.parent/"retained.txt";sibling.write_text("old")
        with self.opened() as current:
            proof=current.proof()
            sibling.write_text("changed retained content")
            current.delete_exact(proof)
        self.assertEqual(sibling.read_text(),"changed retained content")

    def test_namespace_fingerprint_preserves_non_utf8_names(self):
        parent=os.open(self.path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:
            for name in (b"retained-\xff",b"retained-\xfe"):
                try:
                    fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600,dir_fd=parent)
                except OSError as exc:
                    if exc.errno == errno.EILSEQ:
                        self.skipTest("filesystem rejects non-UTF8 filenames")
                    raise
                os.close(fd)
            with self.opened() as current:
                proof=current.proof()
                os.unlink(b"retained-\xfe",dir_fd=parent)
                with self.assertRaises(RuntimeError):current.delete_exact(proof)
            self.assertTrue(self.path.exists())
        finally:os.close(parent)


    def test_parallel_preflight_observations_never_unlink(self):
        import copy
        barrier=threading.Barrier(4)
        calls=[];lock=threading.Lock();counter=[0]
        def api(cfg,path,body, **kwargs):
            with lock:
                calls.append(path)
                if path.endswith("/lease"):
                    counter[0]+=1
                    task=copy.deepcopy(self.task);task["item_id"]=counter[0]
                    return dict(tasks=[task],retry_after_sec=0)
            barrier.wait(4)
            return fixture_ack(body, int(path.split("/")[-2]), delete=False,action="would_delete",recovery="r2_sha_verified")
        with pull.concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            with mock.patch.object(pull,"restore_api",side_effect=api),mock.patch.object(pull.os,"unlink",side_effect=AssertionError("preflight never unlinks")):
                self.assertEqual(pull.cleanup_batch(self.cfg,self.inventory,threading.Event(),executor,workers=4,task_limit=4),0)
        self.assertEqual(len(calls),8)
        self.assertFalse(pull.cleanup_journal(self.inventory))
        self.assertTrue(self.path.exists());self.assertTrue(self.sidecar.exists())

    def test_execution_drains_preflight_before_serial_processing(self):
        order=[];counter=[0]
        def api(cfg,path,body, **kwargs):
            counter[0]+=1
            if counter[0] == 3: return dict(tasks=[], retry_after_sec=120)
            task=dict(self.task,item_id=counter[0])
            if counter[0]==2:task["approved_proof"]={}
            return dict(tasks=[task],retry_after_sec=0)
        def process(cfg,inventory,stop,task, lane=None):
            if task.get("approved_proof") is not None:
                order.append("execute")
            else:
                order.append("preflight_start");time.sleep(0.03);order.append("preflight_end")
        with pull.concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            with mock.patch.object(pull,"restore_api",side_effect=api),mock.patch.object(pull,"cleanup_process_task",side_effect=process):
                pull.cleanup_batch(self.cfg,self.inventory,threading.Event(),executor)
        self.assertEqual(order,["preflight_start","preflight_end","execute"])
        self.assertEqual(counter[0],3)

    def test_lease_failure_drains_workers_before_next_receipt_replay(self):
        finished=threading.Event();counter=[0]
        def api(cfg,path,body, **kwargs):
            counter[0]+=1
            if counter[0]>1:raise OSError("transient lease failure")
            return dict(tasks=[self.task],retry_after_sec=0)
        def process(*args):
            time.sleep(0.03);finished.set()
        with pull.concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            with mock.patch.object(pull,"restore_api",side_effect=api),mock.patch.object(pull,"cleanup_process_task",side_effect=process),mock.patch.object(pull,"cleanup_replay_receipts") as replay:
                with self.assertRaises(OSError):pull.cleanup_batch(self.cfg,self.inventory,threading.Event(),executor)
                self.assertTrue(finished.is_set())
                self.assertEqual(replay.call_count,1)
        self.assertTrue(self.path.exists())

    def test_rolling_preflight_refills_around_a_slow_file_with_bounded_leases(self):
        import copy
        lock = threading.Lock()
        release_slow = threading.Event()
        counts = dict(leased=0, active=0, peak=0, finished=0)
        def api(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith("/lease"))
            limit = body["limit"]
            with lock:
                self.assertGreaterEqual(limit, 1)
                self.assertLessEqual(limit, 8)
                self.assertLessEqual(limit + counts["active"], 8)
                tasks = []
                for _ in range(min(limit, 24 - counts["leased"])):
                    counts["leased"] += 1
                    tasks.append(dict(copy.deepcopy(self.task), item_id=counts["leased"]))
                return dict(tasks=tasks, retry_after_sec=120)
        def process(cfg, inventory, stop, task):
            with lock:
                counts["active"] += 1
                counts["peak"] = max(counts["peak"], counts["active"])
            try:
                if task["item_id"] == 1:
                    self.assertTrue(release_slow.wait(3), "fast slots waited for slowest file")
                else:
                    time.sleep(0.002)
            finally:
                with lock:
                    counts["active"] -= 1
                    counts["finished"] += 1
                    if counts["finished"] >= 10:
                        release_slow.set()
        with pull.concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            with mock.patch.object(pull, "restore_api", side_effect=api), mock.patch.object(pull, "cleanup_process_task", side_effect=process), mock.patch.object(pull, "cleanup_replay_receipts") as replay:
                self.assertEqual(pull.cleanup_batch(self.cfg, self.inventory, threading.Event(), executor, task_limit=24), 0)
                self.assertEqual(replay.call_count, 1)
        self.assertEqual(counts["leased"], 24)
        self.assertEqual(counts["finished"], 24)
        self.assertEqual(counts["active"], 0)
        self.assertLessEqual(counts["peak"], 8)

    def test_batch_validates_all_task_ids_and_execution_shape_before_dispatch(self):
        for tasks in [
                [dict(self.task, item_id=1), dict(self.task, item_id=1)],
                [dict(self.task, item_id=1), dict(self.task, item_id=True)],
                [dict(self.task, item_id=1), dict(self.task, item_id=2, approved_proof={})],
                [dict(self.task, item_id=n) for n in range(1, 10)]]:
            with mock.patch.object(pull, "restore_api", return_value=dict(tasks=tasks)):
                with self.assertRaises(RuntimeError):
                    pull.cleanup_lease(self.cfg, 8)

    def test_stop_drains_all_preflight_workers_before_return(self):
        import copy
        stop = threading.Event()
        lock = threading.Lock()
        counts = dict(active=0, finished=0, leases=0)
        def api(cfg, path, body, **kwargs):
            counts["leases"] += 1
            return dict(tasks=[dict(copy.deepcopy(self.task), item_id=n) for n in range(1, 9)], retry_after_sec=0)
        def process(cfg, inventory, stop, task):
            with lock:
                counts["active"] += 1
            if task["item_id"] == 1:
                stop.set()
            time.sleep(0.02)
            with lock:
                counts["active"] -= 1
                counts["finished"] += 1
        with pull.concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            with mock.patch.object(pull, "restore_api", side_effect=api), mock.patch.object(pull, "cleanup_process_task", side_effect=process), mock.patch.object(pull, "cleanup_replay_receipts") as replay:
                pull.cleanup_batch(self.cfg, self.inventory, stop, executor)
                self.assertEqual(counts["active"], 0)
                self.assertEqual(counts["finished"], 8)
                self.assertEqual(counts["leases"], 1)
                self.assertEqual(replay.call_count, 1)

    def test_completed_exact_nas_r2_ack_qualifies_only_this_fresh_proof(self):
        def api(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith("/observe"))
            return dict(delete=False, action="would_delete", recovery="r2_sha_verified",
                        item_id=self.task["item_id"], attempt=self.task["attempt"],
                        plan_sha256=self.task["plan_sha256"],
                        proof_sha256=hashlib.sha256(json.dumps(body["proof"]).encode()).hexdigest())
        with mock.patch.object(pull, "restore_api", side_effect=api):
            self.assertTrue(pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), self.task))
        self.assertFalse(pull.cleanup_journal(self.inventory))
        self.assertTrue(self.path.exists()); self.assertTrue(self.sidecar.exists())
        proof = {"sha256": "test"}
        decision = api(None, "/observe", {"proof": proof})
        self.assertTrue(pull.cleanup_verified_ack(self.task, proof, decision))
        for key, value in [
                ("delete", True), ("delete", 0), ("action", "delete_issued"),
                ("recovery", "r2_present_unverified"), ("item_id", self.task["item_id"] + 1),
                ("attempt", self.task["attempt"] + 1), ("plan_sha256", "f" * 64),
                ("proof_sha256", "f" * 64)]:
            self.assertFalse(pull.cleanup_verified_ack(self.task, proof, dict(decision, **{key: value})))
        self.assertFalse(pull.cleanup_verified_ack(dict(self.task, approved_proof={}), proof, decision))
        self.assertFalse(pull.cleanup_verified_ack(self.task, proof, {"delete": False, "action": "would_delete"}))

    def test_cleanup_health_gate_requires_proof_heartbeat_mount_and_preserves_feed_error(self):
        from types import SimpleNamespace
        for mounted, heartbeat, complete in [(False, True, True), (True, False, True), (True, True, False)]:
            event = threading.Event()
            if complete: event.set()
            runtime = SimpleNamespace(heartbeat_succeeded=heartbeat, stable_marked=False)
            with mock.patch.object(pull, "promote_candidate") as promote, mock.patch.object(pull, "mark_runtime") as mark:
                self.assertFalse(pull.mark_cleanup_verified_healthy(SimpleNamespace(is_candidate=True), runtime, mounted, event))
                promote.assert_not_called(); mark.assert_not_called()
        state = self.cfg.state_dir
        cfg = SimpleNamespace(is_candidate=True, state_dir=state,
                              current_file=state / "current.py", previous_file=state / "previous.py",
                              candidate_file=state / "candidate.py", runtime_file=state / "runtime.json")
        cfg.current_file.write_bytes(b"previous verified client")
        cfg.candidate_file.write_bytes(b"new verified client")
        runtime = SimpleNamespace(heartbeat_succeeded=True, stable_marked=False, boot_id="fixture",
                                  started_at="2026-11-03T00:00:00Z", phase=pull.Phase.DEGRADED,
                                  last_error="legacy cursor HTTP 502", last_success_at="old progress")
        event = threading.Event(); event.set()
        with mock.patch.object(pull, "executing_release_sha256", return_value=hashlib.sha256(b"new verified client").hexdigest()):
            self.assertTrue(pull.mark_cleanup_verified_healthy(cfg, runtime, True, event))
        self.assertFalse(cfg.is_candidate); self.assertTrue(runtime.stable_marked)
        self.assertEqual(cfg.current_file.read_bytes(), b"new verified client")
        self.assertEqual(cfg.previous_file.read_bytes(), b"previous verified client")
        self.assertEqual(cfg.candidate_file.read_bytes(), b"new verified client")
        self.assertEqual(json.loads(cfg.runtime_file.read_text())["exit"], "healthy")
        self.assertEqual(runtime.phase, pull.Phase.DEGRADED)
        self.assertEqual(runtime.last_error, "legacy cursor HTTP 502")
        self.assertEqual(runtime.last_success_at, "old progress")
        self.assertFalse(pull.mark_cleanup_verified_healthy(cfg, runtime, True, event))

    def test_rolling_coordinator_marks_health_only_for_completed_verified_result(self):
        tasks = [dict(self.task, item_id=n) for n in range(1, 4)]
        event = threading.Event()
        def api(cfg, path, body, **kwargs):
            count = min(body["limit"], len(tasks))
            leased = tasks[:count]
            del tasks[:count]
            return dict(tasks=leased, retry_after_sec=120)
        def process(cfg, inventory, stop, task):
            return task["item_id"] == 2
        with pull.concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            with mock.patch.object(pull, "restore_api", side_effect=api), mock.patch.object(pull, "cleanup_process_task", side_effect=process), mock.patch.object(pull, "cleanup_replay_receipts"):
                pull.cleanup_batch(self.cfg, self.inventory, threading.Event(), executor,
                                   workers=3, task_limit=3, verified_event=event)
        self.assertTrue(event.is_set())

    def test_newer_staged_artifact_cannot_be_promoted_by_an_older_verified_boot(self):
        from types import SimpleNamespace
        state = self.cfg.state_dir
        cfg = SimpleNamespace(is_candidate=True, state_dir=state,
                              current_file=state / "current.py", previous_file=state / "previous.py",
                              candidate_file=state / "candidate.py", runtime_file=state / "runtime.json")
        cfg.current_file.write_bytes(b"known good client")
        cfg.candidate_file.write_bytes(b"newer unexecuted candidate")
        runtime = SimpleNamespace(heartbeat_succeeded=True, stable_marked=False,
                                  boot_id="fixture", started_at="2026-11-03T00:00:00Z")
        event = threading.Event(); event.set()
        executed = hashlib.sha256(b"currently executing candidate").hexdigest()
        with mock.patch.object(pull, "executing_release_sha256", return_value=executed):
            self.assertFalse(pull.mark_cleanup_verified_healthy(cfg, runtime, True, event))
            with self.assertRaises(pull.CandidateMismatch):
                pull.promote_candidate(cfg)
        self.assertTrue(cfg.is_candidate); self.assertFalse(runtime.stable_marked)
        self.assertEqual(cfg.current_file.read_bytes(), b"known good client")
        self.assertEqual(cfg.candidate_file.read_bytes(), b"newer unexecuted candidate")
        self.assertFalse(cfg.runtime_file.exists())
        self.assertFalse(cfg.previous_file.exists())

    def test_external_stager_cannot_change_the_bytes_installed_after_hash_check(self):
        from types import SimpleNamespace
        state = self.cfg.state_dir
        cfg = SimpleNamespace(is_candidate=True, state_dir=state,
                              current_file=state / "current.py", previous_file=state / "previous.py",
                              candidate_file=state / "candidate.py", runtime_file=state / "runtime.json")
        cfg.current_file.write_bytes(b"known good A")
        cfg.candidate_file.write_bytes(b"executing verified B")
        runtime = SimpleNamespace(heartbeat_succeeded=True, stable_marked=False,
                                  boot_id="fixture", started_at="2026-11-03T00:00:00Z")
        event = threading.Event(); event.set()
        atomic = pull.atomic_write
        def external_stage_during_install(path, data, **kwargs):
            if path == cfg.current_file:
                # A separate/older self-update process need not honor our
                # in-process lock. Force its replacement after verification.
                cfg.candidate_file.write_bytes(b"newer unexecuted C")
            return atomic(path, data, **kwargs)
        with mock.patch.object(pull, "executing_release_sha256", return_value=hashlib.sha256(b"executing verified B").hexdigest()), mock.patch.object(pull, "atomic_write", side_effect=external_stage_during_install):
            self.assertTrue(pull.mark_cleanup_verified_healthy(cfg, runtime, True, event))
        self.assertEqual(cfg.current_file.read_bytes(), b"executing verified B")
        self.assertEqual(cfg.previous_file.read_bytes(), b"known good A")
        self.assertEqual(cfg.candidate_file.read_bytes(), b"newer unexecuted C")
        self.assertEqual(json.loads(cfg.runtime_file.read_text())["exit"], "healthy")
        self.assertTrue(runtime.stable_marked); self.assertFalse(cfg.is_candidate)

    def test_promotion_uses_the_executing_version_manifest_not_mutable_source_path(self):
        from types import SimpleNamespace
        state = self.cfg.state_dir
        cfg = SimpleNamespace(is_candidate=True, state_dir=state,
                              current_file=state / "current.py", previous_file=state / "previous.py",
                              candidate_file=state / "candidate.py", runtime_file=state / "runtime.json",
                              update_manifest_url="https://example.invalid/nas/download/latest.json")
        cfg.current_file.write_bytes(b"known good A")
        cfg.candidate_file.write_bytes(b"newer unexecuted C")
        expected = hashlib.sha256(b"compiled approved B").hexdigest()
        manifest = dict(version="compiled-B", artifact="stoarama-pull-compiled-B.py", sha256=expected)
        with mock.patch.object(pull, "CLIENT_VERSION", "compiled-B"), mock.patch.object(pull, "request_json", return_value=manifest) as get:
            self.assertEqual(pull.executing_release_sha256(cfg), expected)
            self.assertEqual(get.call_args.args[2], "https://example.invalid/nas/download/latest-compiled-B.json")
            with self.assertRaises(pull.CandidateMismatch):
                pull.promote_candidate(cfg)
            self.assertEqual(get.call_count, 1)
        self.assertEqual(cfg.current_file.read_bytes(), b"known good A")
        self.assertEqual(cfg.candidate_file.read_bytes(), b"newer unexecuted C")
        self.assertTrue(cfg.is_candidate)
        bad = SimpleNamespace(update_manifest_url=cfg.update_manifest_url)
        with mock.patch.object(pull, "CLIENT_VERSION", "compiled-B"), mock.patch.object(pull, "request_json", return_value=dict(manifest, version="newer-C")):
            with self.assertRaises(RuntimeError):
                pull.executing_release_sha256(bad)
        self.assertFalse(hasattr(bad, "executing_release_sha256"))

    def test_unavailable_release_metadata_defers_without_marking_storage_blocked(self):
        from types import SimpleNamespace
        cfg = SimpleNamespace(is_candidate=True)
        runtime = SimpleNamespace(heartbeat_succeeded=True, stable_marked=False, phase=pull.Phase.DEGRADED)
        event = threading.Event(); event.set()
        with mock.patch.object(pull, "promote_candidate", side_effect=ValueError("malformed release metadata")), mock.patch.object(pull, "mark_runtime") as mark:
            self.assertFalse(pull.mark_cleanup_verified_healthy(cfg, runtime, True, event))
        mark.assert_not_called()
        self.assertFalse(runtime.stable_marked)
        self.assertEqual(runtime.phase, pull.Phase.DEGRADED)

    def test_backend_rollback_retries_only_optional_telemetry_and_lease_fields(self):
        from types import SimpleNamespace
        payload = {"client_version": "new", "cursor_id": 42,
                   "host": {"machine": "x86_64", "release": {"candidate": False, "stable": True}}}
        runtime = SimpleNamespace(heartbeat_payload=lambda outage: dict(payload))
        calls = []
        def heartbeat(cfg, method, path, **kwargs):
            import copy
            calls.append(copy.deepcopy(kwargs["body"]))
            if len(calls) == 1:
                raise pull.urllib.error.HTTPError(path, 400, "unknown release field", {}, None)
            return {"ok": True}
        with mock.patch.object(pull, "request_json", side_effect=heartbeat):
            self.assertEqual(pull.send_heartbeat(self.cfg, runtime, None), {"ok": True})
        self.assertIn("release", calls[0]["host"])
        self.assertEqual(calls[1], {"client_version": "new", "cursor_id": 42, "host": {"machine": "x86_64"}})
        self.assertIn("release", payload["host"])
        lease_calls = []
        def lease(cfg, path, body):
            lease_calls.append(dict(body))
            if "limit" in body:
                raise pull.urllib.error.HTTPError(path, 400, "unknown limit field", {}, None)
            return {"tasks": [self.task], "retry_after_sec": 0}
        with mock.patch.object(pull, "restore_api", side_effect=lease):
            self.assertEqual(pull.cleanup_lease(self.cfg, 8)["tasks"], [self.task])
        self.assertEqual(lease_calls[0]["limit"], 8)
        self.assertNotIn("limit", lease_calls[1])
        for code in (401, 403, 409, 500):
            with mock.patch.object(pull, "restore_api", side_effect=pull.urllib.error.HTTPError("lease", code, "not compatibility", {}, None)) as request:
                with self.assertRaises(pull.urllib.error.HTTPError):
                    pull.cleanup_lease(self.cfg, 8)
                self.assertEqual(request.call_count, 1)


def approve_api(test, calls, receipt_reply=None):
    def api(cfg, path, body, **kwargs):
        calls.append((path, dict(body) if isinstance(body, dict) else body))
        if path.endswith('/lease'):
            return dict(tasks=[test.task], retry_after_sec=0)
        if path.endswith('/observe'):
            if body['outcome'] != 'match':
                return fixture_ack(body, action=body['outcome'], delete=False)
            return dict(delivery_keep_checked=True, action='delete_issued', delete=True, item_id=1, attempt=1, plan_sha256='a'*64,
                        proof_sha256=hashlib.sha256(json.dumps(body['proof']).encode()).hexdigest(),
                        expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
        if receipt_reply is not None:
            return receipt_reply(body)
        return fixture_ack(body, int(path.split("/")[-2]), ok=True)
    return api


class CleanupGuardTests(unittest.TestCase):
    """Safeguards 4-6: independent keep-window guard, local caps, ledger."""
    setUp = CleanupTests.setUp
    fixture_pins = CleanupTests.fixture_pins
    opened = CleanupTests.opened

    def approve(self):
        with self.opened() as f:
            self.task['approved_proof'] = f.proof()

    def run_execution(self, receipt_reply=None):
        calls = []
        with mock.patch.object(pull, 'restore_api', side_effect=approve_api(self, calls, receipt_reply)):
            pull.cleanup_once(self.cfg, self.inventory, threading.Event())
        return calls

    def ledger(self):
        path = self.cfg.state_dir / pull.CLEANUP_LEDGER_NAME
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def assert_refused(self, calls, reason):
        observes = [b for p, b in calls if p.endswith('/observe')]
        self.assertEqual(len(observes), 1)
        self.assertEqual(observes[0]['outcome'], 'conflict')
        self.assertIn(reason, observes[0]['error'])
        self.assertFalse(any(p.endswith('/result') for p, _ in calls))
        self.assertTrue(self.path.exists()); self.assertTrue(self.sidecar.exists())
        self.assertEqual(self.ledger(), [])

    def test_pinned_copies_match_backend_policy_and_repo_folder_map(self):
        repo = Path(__file__).resolve().parents[2]
        policy = (repo/'backend/internal/api/nas_cleanup_keep28_policy.json').read_bytes()
        embedded = pull.zlib.decompress(pull.base64.b64decode(''.join(pull.CLEANUP_KEEP28_POLICY_ZLIB_B64)))
        self.assertEqual(embedded, policy)
        self.assertEqual(pull.CLEANUP_FOLDER_STREAMS_JSON.encode(), (Path(__file__).with_name('nas_cleanup_folder_streams.json')).read_bytes())
        pins = pull.load_cleanup_pins()
        self.assertEqual(len(pins['windows']), 109)
        self.assertEqual(pins['folders']['recordings/339'], 17200)
        self.assertEqual(pins['folders']['01_na_usa_los-angeles_venice-beach'], 17238)
        self.assertNotIn('recordings', pins['folders'])
        self.assertEqual(pull.cleanup_folder_key('joined/01_na_usa_los-angeles_venice-beach/July/24-Friday/x_hour_08.mp4', 'collated'), '01_na_usa_los-angeles_venice-beach')
        self.assertEqual(pull.cleanup_folder_key('recordings/339/continuous/1.mp4', 'raw'), 'recordings/339')
        self.assertIsNone(pull.cleanup_folder_key('joined/x/y.mp4', 'raw'))
        self.assertIsNone(pull.cleanup_folder_key('x/y.mp4', 'collated'))

    def test_tampered_pins_refuse_every_deletion_but_dry_runs_continue(self):
        self.pinned.stop()
        with mock.patch.object(pull, 'CLEANUP_FOLDER_STREAMS_JSON', pull.CLEANUP_FOLDER_STREAMS_JSON.replace('17200', '17201')), \
                mock.patch.object(pull, '_CLEANUP_PINS', None):
            pins, error = pull.cleanup_pins()
            self.assertIsNone(pins); self.assertIn('SHA mismatch', error)
            # Dry runs still observe (and report a refusal, never would_delete);
            # nothing is unlinked and the worker keeps running.
            self.assert_refused(self.run_execution(), 'pinned keep-window files failed verification')
            self.approve()
            self.assert_refused(self.run_execution(), 'pinned keep-window files failed verification')
        self.pinned.start()

    def test_dry_run_refusal_is_a_conflict_not_would_delete(self):
        self.pins = self.fixture_pins('2026-08-31T00:00:00+00:00', '2026-09-28T00:00:00+00:00')
        calls = self.run_execution()
        self.assert_refused(calls, 'overlaps stream 33 keep window')
        self.assertIsNone(self.task.get('approved_proof'))
        # Local caps never block a dry run.
        self.pins = self.fixture_pins('2027-01-01T00:00:00+00:00', '2027-01-29T00:00:00+00:00')
        with mock.patch.object(pull, 'cleanup_local_caps', return_value='cap reached'):
            calls = self.run_execution()
        self.assertEqual([b['outcome'] for p, b in calls if p.endswith('/observe')], ['match'])

    def test_keep_window_overlap_refuses_without_asking_for_a_permit(self):
        self.pins = self.fixture_pins('2026-08-31T00:00:00+00:00', '2026-09-28T00:00:00+00:00')
        self.approve()
        self.assert_refused(self.run_execution(), 'overlaps stream 33 keep window')
        # Half-open window: a clip ending exactly at keep_start is outside.
        self.pins = self.fixture_pins('2026-09-01T00:01:00+00:00', '2026-09-28T00:00:00+00:00')
        self.assertIsNone(pull.cleanup_guard_refusal(self.task, self.task['approved_proof']))

    def test_unknown_folder_and_disagreeing_stream_refuse(self):
        self.approve()
        self.pins = self.fixture_pins('2027-01-01T00:00:00+00:00', '2027-01-29T00:00:00+00:00', folders={'other': 33})
        self.assertIn('has no pinned stream', pull.cleanup_guard_refusal(self.task, self.task['approved_proof']))
        self.pins = self.fixture_pins('2027-01-01T00:00:00+00:00', '2027-01-29T00:00:00+00:00')
        self.item['stream_id'] = 34
        self.assert_refused(self.run_execution(), 'disagrees with pinned stream 33')

    def test_raw_interval_comes_from_the_physical_sidecar(self):
        self.approve()
        proof = json.loads(json.dumps(self.task['approved_proof']))
        self.pins = self.fixture_pins('2026-08-31T00:00:00+00:00', '2026-09-28T00:00:00+00:00')
        # Even if the server-sent bounds claim otherwise, the sidecar decides.
        self.item['start_at'], self.item['end_at'] = '2025-01-01T00:00:00+00:00', '2025-01-01T00:01:00+00:00'
        self.assertIn('overlaps', pull.cleanup_guard_refusal(self.task, proof))
        del proof['sidecar']
        self.assertIn('no physical sidecar binding', pull.cleanup_guard_refusal(self.task, proof))

    def test_delivery_hold_precedes_windows_and_stitch_inclusion(self):
        for key in (
            'joined/goodplus-20260821-generation-1/objects/813b40a60f9a676c5c6e8a5674f59eb6ebab357dd43c41ec680ee48cf9ba0be6.mp4',
            'datasets/global-street-scores-delivery/47/public/current.json',
            'experimental/v85-retired-WORK-custody/20261013/a2b4891fa5937f63a0d1885ab3afae0690f08c6b450caa22615443445c8cc23b.bin',
        ):
            task = dict(item=dict(object_key=key, permit_reason='stitched_included'))
            self.assertIn('DELIVERY KEEP HOLD', pull.cleanup_guard_refusal(task, {}))
        self.assertFalse(pull.delivery_keep_key_held('staging/not-a-delivery-key'))

    def test_collated_bounds_must_match_the_dated_path(self):
        pins = self.fixture_pins('2026-07-01T00:00:00+00:00', '2026-07-29T00:00:00+00:00', folders={'03_Plac': 33})
        pins[0]['windows'][33] = pins[0]['windows'][33][:2] + ('Europe/Warsaw',)
        self.pins = pins
        rel = 'joined/03_Plac/July/24-Friday/03_Plac_2026_July_W4_Friday_hour_08_part_01_080013-080114.mp4'
        task = dict(item=dict(object_key='test/collated', kind='collated', stream_id=33, relative_path=rel, sidecar_required=False,
                              start_at='2026-07-24T06:00:13Z', end_at='2026-07-24T06:01:14.373Z'))
        self.assertIn('overlaps', pull.cleanup_guard_refusal(task, {}))
        self.pins = self.fixture_pins('2026-08-01T00:00:00+00:00', '2026-08-29T00:00:00+00:00', folders={'03_Plac': 33})
        self.pins[0]['windows'][33] = self.pins[0]['windows'][33][:2] + ('Europe/Warsaw',)
        self.assertIsNone(pull.cleanup_guard_refusal(task, {}))
        for start, end, reason in (('2026-07-24T07:00:13Z', '2026-07-24T07:01:00Z', 'not confirmed by the dated NAS path'),
                                   ('2026-07-24T06:00:13Z', '2026-07-24T09:00:00Z', 'implausibly long'),
                                   (None, '2026-07-24T06:01:00Z', 'keep-window check failed')):
            task['item'].update(start_at=start, end_at=end)
            self.assertIn(reason, pull.cleanup_guard_refusal(task, {}))
        task['item'].update(start_at='2026-08-25T16:43:38Z', end_at='2026-08-25T17:00:38Z',
                            relative_path='joined/03_Plac/July/24-Friday/undated.mp4')
        self.assertIn('not confirmed', pull.cleanup_guard_refusal(task, {}))
        self.assertEqual(pull.cleanup_collated_path_hour('joined/recordings/339/joined/2026-08-25/339_2026-08-25_hour_18_184338-190038.mp4'),
                         (datetime.date(2026, 8, 25), 18))

    def test_local_24h_caps_refuse_beyond_files_and_bytes(self):
        # Reviewed constants: 5 TB (same as the server budget) and 400,000 files.
        self.assertEqual((pull.CLEANUP_MAX_BYTES_24H, pull.CLEANUP_MAX_FILES_24H), (5_000_000_000_000, 400_000))
        self.assertIsNone(pull.cleanup_local_caps(self.inventory, 10))
        reserve = dict(item_id=1, plan_sha256='a'*64, relative_path='x')
        self.assertIsNone(pull.cleanup_local_caps(self.inventory, pull.CLEANUP_MAX_BYTES_24H - 10, reserve))
        self.assertIn('cap reached', pull.cleanup_local_caps(self.inventory, 11))
        self.assertIsNone(pull.cleanup_local_caps(self.inventory, 10))
        self.inventory.db.execute('DELETE FROM cleanup_local_deletions')
        self.inventory.db.executemany('INSERT INTO cleanup_local_deletions(item_id,plan_sha256,relative_path,size_bytes,at_epoch) VALUES(?,?,?,?,?)',
                                      [(n, 'a'*64, 'x', 1, time.time()) for n in range(pull.CLEANUP_MAX_FILES_24H)])
        self.inventory.db.commit()
        self.approve()
        self.assert_refused(self.run_execution(), 'local 24h deletion cap reached')
        self.inventory.db.execute('UPDATE cleanup_local_deletions SET at_epoch=?', (time.time() - 86401,))
        self.inventory.db.commit()
        self.assertIsNone(pull.cleanup_local_caps(self.inventory, 10))

    def test_append_only_ledger_records_intent_and_result_around_unlink(self):
        old = self.cfg.state_dir / pull.CLEANUP_LEDGER_NAME
        old.write_text('{"event":"earlier"}\n')
        order = []
        original_unlink = pull.os.unlink
        def unlink(name, *args, **kwargs):
            order.append(('unlink', [r['event'] for r in self.ledger()]))
            return original_unlink(name, *args, **kwargs)
        self.approve()
        with mock.patch.object(pull.os, 'unlink', side_effect=unlink):
            calls = self.run_execution()
        self.assertFalse(self.path.exists())
        self.assertEqual([b['outcome'] for p, b in calls if p.endswith('/result')], ['deleted'])
        rows = self.ledger()
        self.assertEqual([r['event'] for r in rows], ['earlier', 'intent', 'result'])
        self.assertEqual(order[0], ('unlink', ['earlier', 'intent']))
        intent, result = rows[1], rows[2]
        for field in ('relative_path', 'size_bytes', 'sha256', 'plan_sha256', 'item_id', 'at', 'proof_sha256'):
            self.assertIn(field, intent)
        self.assertEqual(intent['relative_path'], 'stream/day/a.mp4'); self.assertEqual(intent['sha256'], self.item['sha256'])
        self.assertEqual((result['outcome'], result['media_unlinked'], result['sidecar_unlinked']), ('deleted', True, True))
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0], 1)

    def test_ledger_write_failure_prevents_unlink(self):
        self.approve()
        with mock.patch.object(pull, 'cleanup_ledger_append', side_effect=OSError('state volume full')):
            calls = self.run_execution()
        self.assertTrue(self.path.exists()); self.assertTrue(self.sidecar.exists())
        receipts = [b for p, b in calls if p.endswith('/result')]
        self.assertEqual(receipts[0]['outcome'], 'unverified'); self.assertIn('state volume full', receipts[0]['error'])

    def test_ledger_result_failure_does_not_certify_a_refusal(self):
        self.approve()
        append = pull.cleanup_ledger_append
        def ledger(cfg, record):
            if record['event'] == 'result':
                raise OSError('result fsync failed')
            return append(cfg, record)
        with mock.patch.object(pull, 'cleanup_ledger_append', side_effect=ledger), \
                mock.patch.object(pull.CleanupFile, 'delete_exact', side_effect=pull.CleanupPermitRefused('deadline_exceeded', 'too late')):
            calls = self.run_execution()
        receipt = [b for p, b in calls if p.endswith('/result')][0]
        self.assertEqual(receipt['outcome'], 'unverified')
        self.assertNotIn('unlink_attempted', receipt)
        self.assertNotIn('refusal_reason', receipt)
        self.assertNotIn('refusal_protocol', receipt)
        self.assertTrue(self.path.exists()); self.assertTrue(self.sidecar.exists())
        self.assertEqual([row['event'] for row in self.ledger()], ['intent'])

    def test_crash_around_unlink_replays_unknown_intent(self):
        self.approve()
        unlink = pull.os.unlink
        for after_unlink in (False, True):
            if after_unlink:
                self.path.write_bytes(b'fixture media')
                self.item['frozen_identity'] = pull.cleanup_identity(self.path.stat())
                self.approve()
            def crash(name, *args, **kwargs):
                if after_unlink:
                    unlink(name, *args, **kwargs)
                raise SystemExit('process died at unlink')
            with mock.patch.object(pull.os, 'unlink', side_effect=crash), self.assertRaises(SystemExit):
                self.run_execution()
            checkpoint = pull.cleanup_journal(self.inventory)[0]
            self.assertEqual(checkpoint[1], 'intent')
            body = json.loads(checkpoint[2])
            self.assertNotIn('unlink_attempted', body)
            self.assertNotIn('refusal_reason', body)
            self.assertNotIn('refusal_protocol', body)
            self.assertEqual(self.path.exists(), not after_unlink)
            with mock.patch.object(pull, 'restore_api', side_effect=lambda cfg, path, body: fixture_ack(body, ok=True)) as api, \
                    mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('replay must not unlink')):
                pull.cleanup_replay_receipts(self.cfg, self.inventory)
            self.assertNotIn('unlink_attempted', api.call_args.args[2])
            self.assertEqual(api.call_args.args[2]['outcome'], 'unverified')

    def test_media_deleted_sidecar_failed_is_reported_honestly_with_legacy_fallback(self):
        original_unlink = pull.os.unlink
        def unlink(name, *args, **kwargs):
            if name.endswith('.stoarama.json'):
                raise PermissionError('sidecar locked')
            return original_unlink(name, *args, **kwargs)
        def old_server(body):
            if body['outcome'] not in ('deleted', 'unverified'):
                raise pull.urllib.error.HTTPError('result', 400, 'invalid cleanup deletion receipt', {}, None)
            return fixture_ack(body, ok=True)
        self.approve()
        with mock.patch.object(pull.os, 'unlink', side_effect=unlink):
            calls = self.run_execution(receipt_reply=old_server)
        self.assertFalse(self.path.exists()); self.assertTrue(self.sidecar.exists())
        receipts = [b for p, b in calls if p.endswith('/result')]
        self.assertEqual([r['outcome'] for r in receipts], ['media_deleted_sidecar_failed', 'unverified'])
        self.assertTrue(receipts[1]['error'].startswith('media_deleted_sidecar_failed: '))
        self.assertEqual(self.ledger()[-1]['outcome'], 'media_deleted_sidecar_failed')
        self.assertEqual(pull.cleanup_journal(self.inventory), [])



class CleanupProtocol2Tests(unittest.TestCase):
    """Concurrent execution with batched round trips (protocol 2)."""
    setUp = CleanupTests.setUp
    fixture_pins = CleanupTests.fixture_pins

    def make_file(self, n, folder='day'):
        path = self.cfg.output_dir/'stream'/folder/('c%d.mp4' % n)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'media %d' % n)
        item = dict(self.item, source_id=100+n, relative_path='stream/%s/c%d.mp4' % (folder, n), size_bytes=path.stat().st_size,
                    sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        binding = dict(clip_id=item['source_id'], recording_id=22, relative_path=item['relative_path'], size_bytes=item['size_bytes'],
                       sha256=item['sha256'], clip_start_at=item['start_at'], clip_end_at=item['end_at'])
        sidecar = Path(str(path)+'.stoarama.json')
        sidecar.write_text(json.dumps(dict(schema_version=1, **binding)))
        item['frozen_identity'] = pull.cleanup_identity(path.stat())
        item['frozen_sidecar_identity'] = pull.cleanup_identity(sidecar.stat())
        return path, sidecar, item

    def execution_tasks(self, count, folder='day'):
        tasks, paths = [], []
        for n in range(count):
            path, sidecar, item = self.make_file(n, folder)
            with pull.CleanupFile(self.cfg, item, threading.Event(), time.monotonic()+20) as f:
                proof = f.proof()
            tasks.append(dict(item_id=1000+n, attempt=1, plan_sha256='a'*64, item=item, approved_proof=proof,
                              expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=30)).isoformat()))
            paths.append((path, sidecar))
        return tasks, paths

    def fake_server(self, tasks, fail_first=()):
        lock = threading.Lock()
        state = dict(leased=0, calls=[], observe_batches=0, active=0, peak=0, failures=list(fail_first), bodies={})
        def api(cfg, path, body, retry_stale=False, **kwargs):
            with lock:
                state['calls'].append(path)
                if state['failures'] and path.endswith(state['failures'][0]):
                    state['failures'].pop(0)
                    state['bodies'].setdefault(path, []).append(json.dumps(body, sort_keys=True))
                    raise pull.urllib.error.HTTPError(path, 502, 'bad gateway', {}, None)
                state['bodies'].setdefault(path, []).append(json.dumps(body, sort_keys=True))
            if path.endswith('/lease'):
                self.assertEqual(body['protocol'], 2)
                with lock:
                    start = state['leased']
                    selected = tasks[start:start + min(body['limit'], body['execution_files'])]
                    state['leased'] += len(selected)
                return dict(tasks=selected, retry_after_sec=0)
            if path.endswith('/observe'):
                self.assertEqual(kwargs.get('timeout'), pull.CLEANUP_EXECUTION_OBSERVE_SEC)
                self.assertFalse(retry_stale)
                return fixture_ack(body, int(path.split('/')[-2]), delivery_keep_checked=True, action='delete_issued', delete=True,
                                   expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
            if path.endswith('/observe-batch'):
                with lock:
                    state['observe_batches'] += 1
                results = []
                for o in body['observations']:
                    self.assertEqual(len(o['request_id']), 32)
                    results.append(dict(item_id=o['item_id'], status=200, delivery_keep_checked=True, action='delete_issued', delete=True, attempt=o['attempt'], plan_sha256='a'*64,
                                        proof_sha256=hashlib.sha256(json.dumps(o['proof']).encode()).hexdigest(),
                                        expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat()))
                return dict(results=results)
            if path.endswith('/result-batch'):
                for r in body['receipts']:
                    self.assertEqual(r['outcome'], 'deleted', r)
                return dict(results=[fixture_ack(r, r['item_id'], status=200, ok=True) for r in body['receipts']])
            raise AssertionError('unexpected call %s' % path)
        return api, state

    def run_v2(self, api):
        stop = threading.Event()
        with pull.concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor, mock.patch.object(pull, 'restore_api', side_effect=api):
            batch_api = pull.CleanupBatchAPI(self.cfg)
            try:
                while pull.cleanup_batch_v2(self.cfg, self.inventory, stop, executor, batch_api) == 0:
                    pass
            finally:
                batch_api.close()

    def ledger(self):
        path = self.cfg.state_dir/pull.CLEANUP_LEDGER_NAME
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def test_execution_in_one_folder_serializes_lifecycle_and_bounds_leases(self):
        tasks, paths = self.execution_tasks(24)
        keep = self.path.parent/'retained.txt'; keep.write_text('keep')
        original = pull.CleanupFile.proof
        active = dict(now=0, peak=0); lock = threading.Lock()
        def proof(current):
            with lock:
                active['now'] += 1; active['peak'] = max(active['peak'], active['now'])
            try:
                time.sleep(0.01)
                return original(current)
            finally:
                with lock: active['now'] -= 1
        api, state = self.fake_server(tasks)
        with mock.patch.object(pull.CleanupFile, 'proof', proof):
            self.run_v2(api)
        for path, sidecar in paths:
            self.assertFalse(path.exists()); self.assertFalse(sidecar.exists())
        self.assertTrue(keep.exists()); self.assertTrue(self.path.exists()); self.assertTrue(self.sidecar.exists())
        self.assertEqual(active['peak'], 1, 'execution overlapped')
        self.assertEqual(state['observe_batches'], 0, 'execution observations must bypass batch queues')
        self.assertEqual(sum(path.endswith('/observe') for path in state['calls']), 24)
        ledger = self.ledger()
        self.assertEqual(sorted(e['item_id'] for e in ledger if e['event'] == 'intent'), [t['item_id'] for t in tasks])
        self.assertEqual(sum(1 for e in ledger if e['event'] == 'result' and e['outcome'] == 'deleted'), 24)
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        rows = self.inventory.db.execute('SELECT count(*) FROM cleanup_local_deletions').fetchone()[0]
        self.assertEqual(rows, 24)

    def test_execution_loop_refills_immediately_and_waits_only_after_empty_lease(self):
        tasks, paths = self.execution_tasks(3)
        api, state = self.fake_server(tasks)
        waits = []
        class Stop:
            def is_set(self):
                return False
            def wait(self, seconds):
                waits.append(seconds)
                if len(waits) == 1:
                    return False  # initial startup wait
                self.assert_completed()
                return True  # stop after the empty lease, before polling again
            def assert_completed(self):
                assert state['leased'] == 3
                assert sum(p.endswith('/result-batch') for p in state['calls']) == 3
                assert all(not path.exists() for path, _ in paths)
        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull, 'CleanupPreflight'):
            pull.cleanup_loop(self.cfg, self.inventory, Stop())
        self.assertEqual(len(waits), 2, 'a poll wait interrupted serial execution')
        self.assertGreater(waits[-1], 0, 'empty leases must back off')
        calls = [p.rsplit('/', 1)[-1] for p in state['calls']]
        self.assertEqual(calls, ['lease', 'observe', 'result-batch'] * 3 + ['lease'])

    def test_pipeline_proves_successor_while_execution_stays_serial(self):
        tasks, paths = self.execution_tasks(4)
        execution_api, state = self.fake_server(tasks)
        dry_seen = threading.Event()
        dry_leased = False
        def api(cfg, path, body, **kwargs):
            nonlocal dry_leased
            if path.endswith('/lease') and body['execution_files'] == 0:
                self.assertEqual(body['execution_bytes'], 0)
                self.assertLessEqual(body['limit'], 2)
                selected = [] if dry_leased else [dict(self.task)]
                dry_leased = True
                return dict(tasks=selected, retry_after_sec=120)
            if path.endswith('/observe-batch'):
                self.assertTrue(all(o['item_id'] == 1 for o in body['observations']))
                dry_seen.set()
                return dict(results=[fixture_ack(o, o['item_id'], status=200, action='would_delete', delete=False,
                    recovery='r2_sha_verified') for o in body['observations']])
            if path.endswith('/observe'):
                self.assertTrue(dry_seen.wait(5), 'successor dry run did not overlap execution')
            return execution_api(cfg, path, body, **kwargs)
        stop = threading.Event()
        with mock.patch.object(pull, 'restore_api', side_effect=api):
            batch_api = pull.CleanupBatchAPI(self.cfg)
            pipeline = pull.CleanupPreflight(self.cfg, self.inventory, stop)
            try:
                with pull.concurrent.futures.ThreadPoolExecutor(8) as executor:
                    pull.cleanup_batch_v2(self.cfg, self.inventory, stop, executor, batch_api, preflight=pipeline)
            finally:
                pipeline.close()
                batch_api.close()
        self.assertTrue(dry_seen.is_set())
        self.assertTrue(self.path.exists())
        self.assertTrue(all(not path.exists() for path, _ in paths))
        self.assertEqual(state['observe_batches'], 0)
        self.assertEqual(self.inventory.cleanup_claims, {})
        self.assertEqual(pull.cleanup_journal(self.inventory), [])

    def test_pipeline_dry_only_rejects_execution_without_touching_files_or_replaying(self):
        tasks, paths = self.execution_tasks(1)
        active = threading.Event(); active.set()
        with mock.patch.object(pull, 'cleanup_lease_v2', return_value=dict(tasks=tasks)), \
             mock.patch.object(pull, 'cleanup_replay_receipts', side_effect=AssertionError('background replay')), \
             mock.patch.object(pull, 'cleanup_process_task', side_effect=AssertionError('execution in preflight')), \
             pull.concurrent.futures.ThreadPoolExecutor(2) as executor:
            with self.assertRaises(pull.CleanupResponseError):
                pull.cleanup_batch_v2(self.cfg, self.inventory, threading.Event(), executor, None,
                    dry_run_only=True, active=active, inflight=2)
        self.assertTrue(all(path.exists() for path, _ in paths))

    def test_pipeline_pause_drains_two_workers_before_replay_and_shutdown(self):
        entered = threading.Barrier(3)
        release = threading.Event()
        active = threading.Event(); active.set()
        tasks = [dict(self.task, item_id=n) for n in range(10, 14)]
        calls = []
        def lease(cfg, limit, files, size):
            self.assertEqual((files, size), (0, 0))
            self.assertLessEqual(limit, 2)
            chosen, tasks[:] = tasks[:limit], tasks[limit:]
            return dict(tasks=chosen, retry_after_sec=120)
        def worker(*args, **kwargs):
            calls.append(args[3]['item_id'])
            entered.wait(5)
            self.assertTrue(release.wait(5))
        with mock.patch.object(pull, 'cleanup_lease_v2', side_effect=lease), \
             mock.patch.object(pull, 'cleanup_process_task', side_effect=worker), \
             mock.patch.object(pull, 'CleanupBatchAPI'):
            pipeline = pull.CleanupPreflight(self.cfg, self.inventory, threading.Event())
            try:
                pipeline.start()
                entered.wait(5)
                pipeline.active.clear()
                release.set()
                pipeline.pause()
                self.assertEqual(len(calls), 2)
                self.assertIsNone(pipeline.future)
            finally:
                release.set()
                pipeline.close()

    def test_execution_fence_releases_before_slow_pipeline_drain_on_dry_transition(self):
        tasks, _ = self.execution_tasks(1)
        replies = [dict(tasks=tasks), dict(tasks=[self.task]), dict(tasks=[], retry_after_sec=120)]
        executed = threading.Event()
        paused = []
        def worker(cfg, inventory, stop, task, api, lane=None):
            if task.get('approved_proof') is not None:
                lane.run(lambda: executed.set())
                self.assertIsNotNone(pull.NAS_MEDIA_FENCE.execution_owner)
        def pause():
            if executed.is_set():
                # A background hash/transport could block here indefinitely.
                # Delivery must already be free before waiting for that drain.
                self.assertIsNone(pull.NAS_MEDIA_FENCE.execution_owner)
                self.assertEqual(pull.NAS_MEDIA_FENCE.cleaners, 0)
                paused.append(True)
        preflight = SimpleNamespace(start=lambda: None, pause=pause)
        with mock.patch.object(pull, 'cleanup_lease_v2', side_effect=replies), \
             mock.patch.object(pull, 'cleanup_process_task', side_effect=worker), \
             pull.concurrent.futures.ThreadPoolExecutor(2) as executor:
            pull.cleanup_batch_v2(self.cfg, self.inventory, threading.Event(), executor, None, preflight=preflight)
        self.assertTrue(paused)

    def test_direct_execution_callers_hold_one_slot_through_the_entire_lifecycle(self):
        tasks, _ = self.execution_tasks(8)
        active = dict(now=0, peak=0, completed=0)
        lock = threading.Lock()
        start = threading.Barrier(len(tasks))
        def lifecycle(*args):
            with lock:
                active['now'] += 1
                active['peak'] = max(active['peak'], active['now'])
            time.sleep(0.01)  # observation through acknowledged receipt
            with lock:
                active['now'] -= 1
                active['completed'] += 1
        def execute(task):
            start.wait(timeout=5)
            pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), task)
        with mock.patch.object(pull, '_cleanup_process_task', side_effect=lifecycle):
            with pull.concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
                list(executor.map(execute, tasks))
        self.assertEqual(active, dict(now=0, peak=1, completed=8))
        self.assertEqual(self.inventory.cleanup_claims, {})

    def test_foreign_change_in_a_shared_folder_still_refuses(self):
        tasks, paths = self.execution_tasks(2)
        with pull.CleanupFile(self.cfg, tasks[0]['item'], threading.Event(), time.monotonic()+20) as a, \
                pull.CleanupFile(self.cfg, tasks[1]['item'], threading.Event(), time.monotonic()+20) as b:
            pa, pb = a.proof(), b.proof()
            a.delete_exact(pa)  # an own, exact, permitted unlink is accounted for
            (self.path.parent/'foreign.mp4').write_bytes(b'new')
            with self.assertRaises(RuntimeError):
                b.delete_exact(pb)
        self.assertFalse(paths[0][0].exists()); self.assertTrue(paths[1][0].exists()); self.assertTrue(paths[1][1].exists())

    def test_own_unlink_between_proof_and_delete_is_not_a_foreign_change(self):
        tasks, paths = self.execution_tasks(3)
        files = [pull.CleanupFile(self.cfg, t['item'], threading.Event(), time.monotonic()+20) for t in tasks]
        try:
            opened = [f.__enter__() for f in files]
            proofs = [f.proof() for f in opened]
            for f, p in zip(reversed(opened), reversed(proofs)):
                f.delete_exact(p)
        finally:
            for f in files:
                f.__exit__(None, None, None)
        self.assertTrue(all(not p.exists() and not s.exists() for p, s in paths))
        self.assertEqual(pull.CLEANUP_DIRECTORIES.dirs, {})

    def test_transient_502_retries_same_requests_and_never_double_deletes(self):
        tasks, paths = self.execution_tasks(4)
        api, state = self.fake_server(tasks, fail_first=('/lease', '/result-batch'))
        with mock.patch.object(pull, 'CLEANUP_RETRY_DELAYS', (0.01, 0.01, 0.01)):
            self.run_v2(api)
        self.assertTrue(all(not p.exists() for p, _ in paths))
        for path in ('/account/connections/cleanup/lease',):
            bodies = state['bodies'][path]
            self.assertEqual(bodies[0], bodies[1], 'retry changed the request (request ids must be reused)')
        self.assertEqual(sum(1 for e in self.ledger() if e['event'] == 'intent'), 4)

    def test_server_without_protocol_2_falls_back(self):
        def api(cfg, path, body, retry_stale=False, **kwargs):
            raise pull.urllib.error.HTTPError(path, 400, 'cleanup protocol1, client version and bounded limit required', {}, None)
        with mock.patch.object(pull, 'restore_api', side_effect=api):
            with self.assertRaises(pull.CleanupProtocolUnsupported):
                pull.cleanup_lease_v2(self.cfg, 8, 8, 1 << 30)

    def test_lease_beyond_local_caps_is_refused_before_any_work(self):
        tasks, paths = self.execution_tasks(3)
        with mock.patch.object(pull, 'restore_api', return_value=dict(tasks=tasks)):
            with self.assertRaises(RuntimeError):
                pull.cleanup_lease_v2(self.cfg, 8, 2, 1 << 30)
            with self.assertRaises(RuntimeError):
                pull.cleanup_lease_v2(self.cfg, 8, 8, sum(t['item']['size_bytes'] for t in tasks) - 1)
            mixed = [tasks[0], dict(self.task, item_id=7)]
            with mock.patch.object(pull, 'restore_api', return_value=dict(tasks=mixed)):
                with self.assertRaises(RuntimeError):
                    pull.cleanup_lease_v2(self.cfg, 8, 8, 1 << 30)
        self.assertTrue(all(p.exists() for p, _ in paths))
        # The coordinator asks only for the remaining local headroom.
        self.inventory.db.execute("CREATE TABLE IF NOT EXISTS cleanup_local_deletions(id INTEGER PRIMARY KEY AUTOINCREMENT,item_id INTEGER NOT NULL,plan_sha256 TEXT NOT NULL,relative_path TEXT NOT NULL,size_bytes INTEGER NOT NULL,at_epoch REAL NOT NULL)")
        self.inventory.db.executemany("INSERT INTO cleanup_local_deletions(item_id,plan_sha256,relative_path,size_bytes,at_epoch) VALUES(?,?,?,?,?)",
                                      [(n, 'a'*64, 'x', 10, time.time()) for n in range(5)])
        self.inventory.db.commit()
        seen = []
        def api(cfg, path, body, retry_stale=False, **kwargs):
            seen.append(body); return dict(tasks=[], retry_after_sec=120)
        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull, 'CLEANUP_MAX_FILES_24H', 7):
            self.run_v2(api)
        self.assertEqual(seen[0]['execution_files'], 1)
        self.assertEqual(seen[0]['execution_bytes'], pull.CLEANUP_MAX_BYTES_24H - 50)

    def test_dry_run_sends_md5_from_the_same_read_only_when_advertised(self):
        for advertised in (False, True):
            with self.subTest(advertised=advertised):
                seen = []
                def api(cfg, path, body, retry_stale=False, **kwargs):
                    if path.endswith('/lease'):
                        if seen:
                            return dict(tasks=[], retry_after_sec=120)
                        seen.append('leased')
                        task = dict(self.task, item_id=7 if advertised else 8)
                        task.pop('approved_proof', None)
                        reply = dict(tasks=[task], retry_after_sec=0)
                        if advertised:
                            reply['recovery_md5'] = True
                        return reply
                    o = body['observations'][0]
                    seen.append(o.get('md5'))
                    return dict(results=[fixture_ack(o, o['item_id'], status=200, action='would_delete', delete=False)])
                self.run_v2(api)
                expected = hashlib.md5(self.path.read_bytes()).hexdigest() if advertised else None
                self.assertEqual(seen[1], expected)
                self.assertTrue(self.path.exists())

    def test_replay_uses_only_reconciliation_endpoint_in_parallel(self):
        for n in range(6):
            pull.cleanup_journal(self.inventory, 100+n, 'observe', dict(attempt=1, plan_sha256='a'*64, outcome='conflict', proof=None, error='x', request_id='%032x' % n))
        lock = threading.Lock(); calls = []
        def api(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith('/reconcile'), path)
            with lock: calls.append(path)
            return fixture_ack(body, int(path.split('/')[-2]), action='pending', delete=False)
        with mock.patch.object(pull, 'restore_api', side_effect=api):
            batch_api = pull.CleanupBatchAPI(self.cfg)
            try:
                pull.cleanup_replay_receipts(self.cfg, self.inventory, batch_api)
            finally:
                batch_api.close()
        self.assertEqual(len(set(calls)), 6)
        self.assertEqual(pull.cleanup_journal(self.inventory), [])

    def test_replay_never_falls_back_to_permit_endpoint_on_older_server(self):
        for status in (400, 404, 405, 503):
            with self.subTest(status=status):
                pull.cleanup_journal(self.inventory, 200, 'observe', dict(attempt=1, plan_sha256='a'*64, outcome='match', proof=None, error='', request_id='a'*32, md5='b'*32))
                original = pull.cleanup_journal(self.inventory)
                def api(cfg, path, body, **kwargs):
                    self.assertTrue(path.endswith('/reconcile'), path)
                    raise pull.urllib.error.HTTPError(path, status, 'unsupported', {}, None)
                with mock.patch.object(pull, 'restore_api', side_effect=api) as calls:
                    counts = pull.cleanup_replay_receipts(self.cfg, self.inventory)
                self.assertEqual(counts['failed'], 1)
                self.assertEqual(calls.call_count, 1)
                self.assertEqual(pull.cleanup_journal(self.inventory), original)

    def test_health_ack_accepts_md5_etag_matched_recovery(self):
        with pull.CleanupFile(self.cfg, self.item, threading.Event(), time.monotonic()+20) as f:
            proof = f.proof()
        decision = dict(delete=False, action='would_delete', item_id=1, attempt=1, plan_sha256='a'*64,
                        proof_sha256=hashlib.sha256(json.dumps(proof).encode()).hexdigest())
        for recovery, ok in (('r2_sha_verified', True), ('r2_md5_etag_matched', True), ('r2_etag_bound', False), ('unchecked', False)):
            self.assertEqual(pull.cleanup_verified_ack(self.task, proof, dict(decision, recovery=recovery)), ok, recovery)

    def test_incomplete_dry_recovery_retries_identical_observation(self):
        task = dict(self.task)
        task.pop('approved_proof', None)
        sent = []
        def api(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith('/observe-batch'), path)
            observation = body['observations'][0]
            sent.append(json.loads(json.dumps(observation)))
            if len(sent) == 1:
                return dict(results=[dict(item_id=observation['item_id'], status=503,
                                          error='recovery check incomplete; retry', retryable=True)])
            return dict(results=[fixture_ack(observation, observation['item_id'], status=200,
                                             action='would_delete', delete=False, recovery='r2_sha_verified')])
        with mock.patch.object(pull, 'restore_api', side_effect=api), \
             mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('dry run must not delete')):
            batch = pull.CleanupBatchAPI(self.cfg)
            try:
                pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), task, batch)
            finally:
                batch.close()
        self.assertEqual(len(sent), 2)
        self.assertEqual(sent[0], sent[1])
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        self.assertTrue(self.path.exists())
        self.assertTrue(self.sidecar.exists())

    def test_batcher_sends_each_item_once_and_withdraws_abandoned_entries(self):
        sent = []
        release = threading.Event()
        def api(cfg, path, body, retry_stale=False, **kwargs):
            sent.append([o['item_id'] for o in body['observations']])
            release.wait(2)
            return dict(results=[dict(item_id=o['item_id'], status=200, action='would_delete') for o in body['observations']])
        with mock.patch.object(pull, 'restore_api', side_effect=api):
            batcher = pull.CleanupBatcher(self.cfg, '/account/connections/cleanup/observe-batch', 'observations', senders=1, linger=0.2)
            try:
                a = batcher.submit(dict(item_id=5, attempt=1))
                b = batcher.submit(dict(item_id=5, attempt=1))
                c = batcher.submit(dict(item_id=6, attempt=1))
                release.set()
                self.assertEqual(a.result(2)['item_id'], 5); self.assertEqual(b.result(2)['item_id'], 5); c.result(2)
                self.assertEqual(sent, [[5, 6]])
                # An abandoned (timed-out) entry is withdrawn before it is sent.
                release.clear()
                busy = batcher.submit(dict(item_id=7, attempt=1))
                time.sleep(0.3)  # item 7 is in flight; the next one queues
                stale = batcher.submit(dict(item_id=8, attempt=1))
                batcher.withdraw(stale)
                release.set()
                busy.result(2)
                time.sleep(0.4)
                self.assertNotIn([8], sent); self.assertTrue(stale.cancelled())
            finally:
                release.set(); batcher.close()

    def test_batcher_refuses_conflicting_duplicates(self):
        sent = []
        def api(cfg, path, body, retry_stale=False, **kwargs):
            sent.append(body['observations'])
            return dict(results=[dict(item_id=o['item_id'], status=200, action='would_delete') for o in body['observations']])
        with mock.patch.object(pull, 'restore_api', side_effect=api):
            batcher = pull.CleanupBatcher(self.cfg, '/account/connections/cleanup/observe-batch', 'observations', senders=1, linger=0.2)
            try:
                a = batcher.submit(dict(item_id=5, attempt=1, request_id='a'*32))
                b = batcher.submit(dict(item_id=5, attempt=1, request_id='b'*32))
                c = batcher.submit(dict(item_id=5, attempt=1, request_id='a'*32))
                self.assertEqual(a.result(2)['item_id'], 5); self.assertEqual(c.result(2)['item_id'], 5)
                with self.assertRaises(pull.urllib.error.HTTPError) as caught:
                    b.result(2)
                self.assertEqual(caught.exception.code, 409)
                self.assertEqual(sent, [[dict(item_id=5, attempt=1, request_id='a'*32)]])
            finally:
                batcher.close()

    def journal_observe(self, item_id, **extra):
        pull.cleanup_journal(self.inventory, item_id, 'observe', dict(attempt=1, plan_sha256='a'*64, outcome='conflict', proof=None, error='x', request_id='%032x' % item_id, **extra))

    def test_result_transaction_failure_retains_exact_receipt_for_replay(self):
        receipt = dict(attempt=1, plan_sha256='a'*64, proof_sha256='b'*64,
                       outcome='deleted', unlink_attempted=True)
        pull.cleanup_journal(self.inventory, 900, 'result', receipt)
        checkpoint = pull.cleanup_journal(self.inventory)
        # Even repeated failures over hours must stay replayable, never quarantine.
        def unavailable(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith('/900/result'))
            self.assertEqual(body, receipt)
            raise pull.urllib.error.HTTPError(path, 503, 'replay required', {}, None)
        with mock.patch.object(pull, 'restore_api', side_effect=unavailable), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('receipt replay must not unlink')):
            for cycle in range(pull.CLEANUP_REPLAY_QUARANTINE_AFTER + 1):
                with mock.patch.object(pull.time, 'time', return_value=10000 + cycle*901):
                    pull.cleanup_replay_receipts(self.cfg, self.inventory)
                self.assertEqual(pull.cleanup_journal(self.inventory), checkpoint)
        self.assertEqual(self.inventory.db.execute('SELECT count(*) FROM cleanup_quarantine').fetchone()[0], 0)
        # A committed contradiction is a 409; its server evidence is durable.
        def committed(cfg, path, body, **kwargs):
            self.assertEqual(body, receipt)
            raise pull.urllib.error.HTTPError(path, 409, 'committed contradiction', {}, None)
        with mock.patch.object(pull, 'restore_api', side_effect=committed):
            pull.cleanup_replay_receipts(self.cfg, self.inventory)
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        archived = self.inventory.db.execute('SELECT item_id,phase,body FROM cleanup_rejected_receipts').fetchone()
        self.assertEqual((archived[0], archived[1], json.loads(archived[2])), (900, 'result', receipt))

    def test_failing_replay_never_blocks_leasing_and_is_quarantined_not_dropped(self):
        pull.CLEANUP_STATUS.__init__()
        self.journal_observe(900)
        leases = []
        def api(cfg, path, body, retry_stale=False, **kwargs):
            if path.endswith('/lease'):
                leases.append(body)
                return dict(tasks=[], retry_after_sec=120)
            raise pull.urllib.error.HTTPError(path, 422, 'invalid receipt', {}, None)
        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull, 'CLEANUP_RETRY_DELAYS', ()):
            batch_api = pull.CleanupBatchAPI(self.cfg)
            try:
                for cycle in range(pull.CLEANUP_REPLAY_QUARANTINE_AFTER):
                    with mock.patch.object(pull.time, 'time', return_value=10000 + cycle * 901):
                        self.assertEqual(pull.cleanup_batch_v2(self.cfg, self.inventory, threading.Event(), pull.concurrent.futures.ThreadPoolExecutor(2), batch_api), 120)
            finally:
                batch_api.close()
        self.assertEqual(len(leases), pull.CLEANUP_REPLAY_QUARANTINE_AFTER, 'replay failure blocked leasing')
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        rows = self.inventory.db.execute('SELECT item_id,phase,body,failures FROM cleanup_quarantine').fetchall()
        self.assertEqual([(r[0], r[1], r[3]) for r in rows], [(900, 'observe', pull.CLEANUP_REPLAY_QUARANTINE_AFTER)])
        self.assertEqual(json.loads(rows[0][2])['request_id'], '%032x' % 900)
        self.assertTrue(self.path.exists())
        status, error, _ = pull.CLEANUP_STATUS.snapshot()
        self.assertIn('replay', error); self.assertEqual(status['quarantined'], 1)

    def test_protocol1_legacy_ack_quarantines_each_phase_and_leases_unrelated_work(self):
        for coordinator in ('once', 'batch'):
            for phase in ('observe', 'intent', 'result'):
                with self.subTest(coordinator=coordinator, phase=phase):
                    f = CleanupTests()
                    f.setUp()
                    try:
                        if phase == 'observe':
                            body = dict(attempt=1, plan_sha256='a'*64, proof=None, outcome='conflict')
                        else:
                            body = dict(attempt=1, plan_sha256='a'*64, proof_sha256='b'*64, outcome='unverified')
                        pull.cleanup_journal(f.inventory, 900, phase, body)
                        calls, leases = [], []
                        def api(cfg, path, body, **kwargs):
                            calls.append(path)
                            if path.endswith('/lease'):
                                leases.append(body)
                                return dict(tasks=[f.task], retry_after_sec=120)
                            if '/900/' in path:
                                # Valid legacy responses intentionally lack identity echoes.
                                return dict(action='would_delete', delete=False) if path.endswith('/observe') else dict(ok=True)
                            return fixture_ack(body, action='would_delete', delete=False)
                        checkpoint = None
                        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('must not delete')):
                            with pull.concurrent.futures.ThreadPoolExecutor(1) as executor:
                                for cycle in range(pull.CLEANUP_REPLAY_QUARANTINE_AFTER):
                                    with mock.patch.object(pull.time, 'time', return_value=10000 + cycle*901):
                                        if coordinator == 'once':
                                            pull.cleanup_once(f.cfg, f.inventory, threading.Event())
                                        else:
                                            pull.cleanup_batch(f.cfg, f.inventory, threading.Event(), executor, task_limit=1)
                                    if cycle == 0:
                                        checkpoint = pull.cleanup_journal(f.inventory)[0]
                                    if cycle < pull.CLEANUP_REPLAY_QUARANTINE_AFTER - 1:
                                        self.assertEqual(pull.cleanup_journal(f.inventory), [checkpoint])
                                # Quarantine and subsequent legacy cycles do not crash/retry it.
                                before = len([path for path in calls if '/900/' in path])
                                pull.cleanup_once(f.cfg, f.inventory, threading.Event())
                                self.assertEqual(len([path for path in calls if '/900/' in path]), before)
                        self.assertEqual(len(leases), pull.CLEANUP_REPLAY_QUARANTINE_AFTER + 1)
                        self.assertEqual(pull.cleanup_journal(f.inventory), [])
                        rows = f.inventory.db.execute('SELECT item_id,phase,body FROM cleanup_quarantine').fetchall()
                        self.assertEqual(rows, [checkpoint], 'exact current checkpoint must be kept')
                        self.assertTrue(f.path.exists())
                        self.assertTrue(f.sidecar.exists())
                        if phase != 'observe':
                            self.assertFalse(any('/900/observe' in path for path in calls), 'receipt must never request another permit')
                    finally:
                        f.doCleanups()

    def test_protocol1_all_blocked_leases_wait_without_reopening_or_repermitting(self):
        self.journal_observe(1)
        original = pull.cleanup_journal(self.inventory)
        def api(cfg, path, body, **kwargs):
            if path.endswith('/lease'):
                return dict(tasks=[self.task], retry_after_sec=120)
            return dict(action='would_delete', delete=False)
        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull, 'CleanupFile', side_effect=AssertionError('unresolved item reopened')):
            with pull.concurrent.futures.ThreadPoolExecutor(1) as executor:
                retry = pull.cleanup_batch(self.cfg, self.inventory, threading.Event(), executor, task_limit=3)
        self.assertEqual(retry, 120)
        self.assertEqual(pull.cleanup_journal(self.inventory), original)

    def test_protocol1_default_replay_budget_does_not_block_leases(self):
        self.journal_observe(900)
        original = pull.cleanup_journal(self.inventory)
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        def api(cfg, path, body, **kwargs):
            if path.endswith('/lease'):
                self.assertTrue(entered.is_set())
                return dict(tasks=[], retry_after_sec=120)
            entered.set()
            release.wait(5)
            return dict(action='would_delete', delete=False)
        record_failure = pull.cleanup_replay_failed
        def failure(*args):
            try:
                return record_failure(*args)
            finally:
                finished.set()
        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull, 'CLEANUP_REPLAY_BUDGET_SEC', 0.05), mock.patch.object(pull, 'cleanup_replay_failed', side_effect=failure):
            try:
                start = time.monotonic()
                self.assertEqual(pull.cleanup_once(self.cfg, self.inventory, threading.Event()), 120)
                self.assertLess(time.monotonic() - start, 1)
                self.assertEqual(pull.cleanup_journal(self.inventory), original)
                with mock.patch.object(pull, 'restore_api', side_effect=AssertionError('in-flight receipt resubmitted')):
                    pull.cleanup_replay_receipts(self.cfg, self.inventory)
            finally:
                release.set()
                self.assertTrue(finished.wait(5))
        self.assertEqual(pull.cleanup_journal(self.inventory), original)

    def test_replay_is_time_bounded_and_blocked_items_are_not_processed(self):
        pull.CLEANUP_STATUS.__init__()
        self.journal_observe(901)
        release = threading.Event(); processed = []; leases = []
        task = dict(self.task, item_id=901)
        task.pop('approved_proof', None)
        def api(cfg, path, body, retry_stale=False, **kwargs):
            if path.endswith('/lease'):
                leases.append(1)
                return dict(tasks=[dict(task)], retry_after_sec=120) if len(leases) == 1 else dict(tasks=[], retry_after_sec=120)
            release.wait(5)
            return dict(results=[dict(item_id=o['item_id'], status=409, error='stale') for o in body['observations']])
        def process(*args, **kwargs):
            processed.append(args[3]['item_id'])
        started = time.monotonic()
        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull, 'CLEANUP_REPLAY_BUDGET_SEC', 0.3), \
                mock.patch.object(pull, 'cleanup_process_task', side_effect=process):
            batch_api = pull.CleanupBatchAPI(self.cfg)
            try:
                pull.cleanup_batch_v2(self.cfg, self.inventory, threading.Event(), pull.concurrent.futures.ThreadPoolExecutor(2), batch_api)
                self.assertLess(time.monotonic() - started, 3, 'replay was not time bounded')
                self.assertEqual(processed, [], 'an item with an unresolved journal entry was processed')
            finally:
                release.set(); batch_api.close()
        self.assertEqual(pull.CLEANUP_STATUS.snapshot()[0].get('skipped_blocked_items'), 1)

    def test_whole_request_failures_fall_back_to_smaller_batches(self):
        pull.CLEANUP_STATUS.__init__()
        sizes = []
        def api(cfg, path, body, retry_stale=False, **kwargs):
            obs = body['observations']; sizes.append(len(obs))
            if len(obs) > 2 or any(o['item_id'] == 13 for o in obs):
                raise pull.urllib.error.HTTPError(path, 413, 'too large', {}, None)
            return dict(results=[dict(item_id=o['item_id'], status=200, action='would_delete') for o in obs])
        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull, 'CLEANUP_RETRY_DELAYS', ()):
            batcher = pull.CleanupBatcher(self.cfg, '/account/connections/cleanup/observe-batch', 'observations', senders=1, linger=0.3)
            try:
                futures = {n: batcher.submit(dict(item_id=n, attempt=1)) for n in range(10, 18)}
                for n, f in futures.items():
                    if n in (12, 13):
                        with self.assertRaises(pull.urllib.error.HTTPError):
                            f.result(5)
                    else:
                        self.assertEqual(f.result(5)['item_id'], n)
                self.assertLessEqual(batcher.max_items, 4)
            finally:
                batcher.close()
        self.assertEqual(sizes[0], 8)
        status = pull.CLEANUP_STATUS.snapshot()[0]
        self.assertIn('HTTP 413', status['last_batch_failure']); self.assertGreater(status['batch_failures'], 0)

    def test_heartbeat_reports_cleanup_status_and_drops_it_for_old_servers(self):
        pull.CLEANUP_STATUS.__init__()
        pull.CLEANUP_STATUS.update(replay_failed=2, quarantined=1)
        pull.CLEANUP_STATUS.fail('journal replay: 2 failed')
        cfg = SimpleNamespace(progress_file=self.root/'state'/'progress.json', legacy_progress_file=self.root/'state'/'cursor.json',
                              runtime_file=self.root/'state'/'runtime.json', capacity_file=self.root/'state'/'capacity.json', state_dir=self.root/'state',
                              download_workers=12, outage_file=self.root/'state'/'outage.json', dry_run=False, output_dir=self.cfg.output_dir)
        runtime = pull.Runtime(cfg)
        payload = runtime.heartbeat_payload(None)
        self.assertEqual(payload['cleanup']['quarantined'], 1)
        self.assertTrue(payload['client_last_error'].startswith('cleanup: journal replay'))
        sent = []
        def fake(cfg, method, path, body=None, timeout=None):
            sent.append(dict(body))
            if 'cleanup' in body:
                raise pull.urllib.error.HTTPError(path, 400, 'unknown field', {}, None)
            return dict(ok=True)
        with mock.patch.object(pull, 'request_json', side_effect=fake):
            self.assertEqual(pull.send_heartbeat(self.cfg, runtime, None), dict(ok=True))
        self.assertIn('cleanup', sent[0]); self.assertNotIn('cleanup', sent[1])
        self.assertTrue(sent[1]['client_last_error'].startswith('cleanup: journal replay'))
        pull.CLEANUP_STATUS.__init__()

    def test_log_redaction_removes_secrets_signed_urls_and_host_paths(self):
        root = str(self.cfg.output_dir)
        # Fixture values are assembled at runtime so secret scanners never see
        # a credential-shaped literal in the source.
        fake = ''.join(chr(97 + n % 26) for n in range(22))
        prefixed = 'sir' + '_' + fake
        cases = {
            'Authorization: Bearer ' + fake: fake,
            'GET https://acct.example/b/k.mp4?' + 'Sig' + 'nature=' + fake + ' failed': fake,
            'api' + '_key=' + prefixed + ' retry': prefixed,
            'R2_SECRET_ACCESS' + '_KEY: "' + fake + '"': fake,
            'env leak ' + fake + ' seen': fake,
            'open /etc/stoarama/config.env failed': '/etc/stoarama',
        }
        for line, secret in cases.items():
            redacted = pull.redact_log_line(line, root, (fake,) if 'env leak' in line else ())
            self.assertNotIn(secret, redacted, line)
        kept = pull.redact_log_line('cleanup item=5 %s/stream/day/a.mp4 sha 6cd360d73985e0fa33db455c1bfc6190ebb2ef22febdf965e460744e41517111 POST /account/connections/cleanup/lease' % root, root)
        self.assertIn('<nas>/stream/day/a.mp4', kept); self.assertIn('6cd360d7', kept); self.assertIn('/account/connections/cleanup/lease', kept)
        self.assertNotIn(root, kept)
        self.assertLessEqual(len(pull.redact_log_line('x' * 10000)), pull.CLIENT_LOG_LINE_MAX)
        self.assertIn('[TRUNCATED]', pull.redact_log_line('x' * 10000))
        with mock.patch.dict(pull.os.environ, {'STOARAMA_API' + '_KEY': fake}):
            self.assertIn(fake, pull.client_log_secrets(SimpleNamespace(api_key='cfg-' + fake)))

    def test_log_ring_keeps_only_relevant_lines_within_bounds(self):
        ring = pull.ClientLogRing(max_lines=5, max_bytes=10 ** 6)
        ring.record('INFO', 'delivered clip 7')        # neither WARN+ nor a cleanup topic
        ring.record('INFO', 'NAS cleanup lease of 64')  # cleanup topic
        ring.record('WARN', 'storage slow')
        self.assertEqual([e['message'] for e, _ in ring.lines], ['NAS cleanup lease of 64', 'storage slow'])
        for n in range(10):
            ring.record('WARN', 'w%d' % n)
        self.assertEqual(len(ring.lines), 5); self.assertEqual(ring.lines[-1][0]['message'], 'w9'); self.assertGreater(ring.dropped, 0)
        small = pull.ClientLogRing(max_lines=1000, max_bytes=2000)
        for n in range(100):
            small.record('ERROR', 'e' * 100)
        self.assertLessEqual(small.bytes, 2000)
        self.assertTrue(small.error_event.is_set())

    def test_log_upload_carries_identity_counts_and_only_new_lines(self):
        ring = pull.ClientLogRing()
        sent = []
        with mock.patch.object(pull, 'CLIENT_LOG_RING', ring), mock.patch.object(pull, 'request_json', side_effect=lambda cfg, m, path, body=None, timeout=None: sent.append((path, body)) or {}):
            ring.record('WARN', 'first')
            self.journal_observe(77)
            runtime = SimpleNamespace(boot_id='boot-x')
            seq = pull.client_log_upload_once(self.cfg, runtime, self.inventory, 0)
            ring.record('ERROR', 'second')
            pull.client_log_upload_once(self.cfg, runtime, self.inventory, seq)
        path, body = sent[0]
        self.assertEqual(path, '/account/connections/client-logs')
        self.assertEqual((body['client_version'], body['boot_id'], body['journal_entries'], body['quarantined']), (pull.CLIENT_VERSION, 'boot-x', 1, 0))
        self.assertEqual([l['message'] for l in body['lines']], ['first'])
        self.assertEqual([l['message'] for l in sent[1][1]['lines']], ['second'])

    def test_media_fence_admits_concurrent_cleanups_but_never_beside_writers(self):
        fence = pull.NASMediaFence()
        fence.acquire_cleanup(threading.Event(), time.monotonic()+1)
        fence.acquire_cleanup(threading.Event(), time.monotonic()+1)
        entered = threading.Event()
        @fence.delivery
        def writer():
            entered.set()
        thread = threading.Thread(target=writer); thread.start()
        self.assertFalse(entered.wait(0.2), 'writer ran beside a cleanup deletion')
        # A waiting writer blocks new cleanup holders (no starvation).
        with self.assertRaises(pull.CleanupInterrupted):
            fence.acquire_cleanup(threading.Event(), time.monotonic()+0.1)
        fence.release_cleanup(); fence.release_cleanup()
        self.assertTrue(entered.wait(2)); thread.join(1)



    def test_transient_errors_never_quarantine(self):
        errors = [OSError('offline'), TimeoutError('timeout'), RuntimeError('restart')]
        errors += [pull.urllib.error.HTTPError('/test', code, 'temporary', {}, None) for code in (408, 429, 500, 502, 503, 504)]
        for exc in errors:
            self.journal_observe(900)
            original = pull.cleanup_journal(self.inventory)
            for n in range(8):
                with mock.patch.object(pull.time, 'time', return_value=10000 + n * 1000):
                    self.assertFalse(pull.cleanup_replay_failed(self.inventory, 900, 'observe', original[0][2], exc))
            self.assertEqual(pull.cleanup_journal(self.inventory), original)
            self.assertEqual(pull.cleanup_quarantined_items(self.inventory), set())

    def test_quarantine_requires_hour_and_release_replays_without_delete(self):
        self.journal_observe(900)
        original = pull.cleanup_journal(self.inventory)[0]
        exc = pull.CleanupResponseError('malformed')
        for n in range(6):
            with mock.patch.object(pull.time, 'time', return_value=10000 + n):
                self.assertFalse(pull.cleanup_replay_failed(self.inventory, *original, exc))
        # Simulate the receipt having advanced to an intent before failure.
        pull.cleanup_journal(self.inventory, 900, 'intent', dict(attempt=1, plan_sha256='a'*64, proof_sha256='b'*64))
        latest = pull.cleanup_journal(self.inventory)[0]
        with mock.patch.object(pull.time, 'time', return_value=13600):
            self.assertTrue(pull.cleanup_replay_failed(self.inventory, *original, exc))
        self.assertEqual(pull.cleanup_quarantine_status(self.inventory)['quarantine_item_ids'], [900])
        pull.cleanup_release_quarantine(self.inventory, 1)
        self.assertEqual(pull.cleanup_journal(self.inventory), [latest])
        self.assertEqual(pull.cleanup_quarantine_status(self.inventory)['release_generation'], 1)
        calls = []
        def api(cfg, path, body, **kwargs):
            self.assertTrue(path.endswith('/result'))
            self.assertEqual(body['outcome'], 'unverified')
            calls.append(body)
            return fixture_ack(body, int(path.split("/")[-2]), ok=True)
        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('release must not delete')):
            pull.cleanup_replay_receipts(self.cfg, self.inventory)
        self.assertEqual(len(calls), 1)
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        pull.cleanup_release_quarantine(self.inventory, 1)
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        self.assertTrue(self.path.exists())

    def test_upgrade_does_not_count_legacy_transient_failures(self):
        self.journal_observe(900)
        self.inventory.db.execute("CREATE TABLE cleanup_replay_failures(item_id INTEGER PRIMARY KEY,failures INTEGER NOT NULL,last_error TEXT NOT NULL,last_at TEXT NOT NULL)")
        self.inventory.db.execute("INSERT INTO cleanup_replay_failures VALUES(900,100,'network','old')")
        self.inventory.db.commit()
        entry = pull.cleanup_journal(self.inventory)[0]
        exc = pull.CleanupResponseError('invalid response')
        for n in range(4):
            with mock.patch.object(pull.time, 'time', return_value=10000 + 3600*n):
                self.assertFalse(pull.cleanup_replay_failed(self.inventory, *entry, exc))
        with mock.patch.object(pull.time, 'time', return_value=24400):
            self.assertTrue(pull.cleanup_replay_failed(self.inventory, *entry, exc))

    def test_release_preserves_conflicting_receipts(self):
        self.journal_observe(900)
        original = pull.cleanup_journal(self.inventory)[0]
        pull.cleanup_quarantined_items(self.inventory)
        self.inventory.db.execute('INSERT INTO cleanup_quarantine VALUES(?,?,?,?,?,?)', (900, 'result', '{}', 'x', 5, 'now'))
        self.inventory.db.commit()
        pull.cleanup_release_quarantine(self.inventory, 2)
        self.assertEqual(pull.cleanup_journal(self.inventory), [original])
        self.assertEqual(pull.cleanup_quarantined_items(self.inventory), {900})

    def test_worker_defers_existing_journal_in_every_phase_and_protocol1(self):
        for phase in ('observe', 'intent', 'result'):
            pull.cleanup_journal(self.inventory, 1, phase, {'evidence': phase})
            original = pull.cleanup_journal(self.inventory)
            with mock.patch.object(pull, 'cleanup_replay_receipts'), mock.patch.object(pull, 'cleanup_lease', return_value=dict(tasks=[self.task])), mock.patch.object(pull, 'CleanupFile', side_effect=AssertionError('blocked filesystem access')), mock.patch.object(pull, 'restore_api', side_effect=AssertionError('blocked API access')):
                pull.cleanup_once(self.cfg, self.inventory, threading.Event())
            self.assertEqual(pull.cleanup_journal(self.inventory), original)

    def test_heartbeat_release_between_lease_check_and_worker_preserves_evidence(self):
        pull.cleanup_journal(self.inventory, 1, 'intent', {'evidence': 'exact durable bytes'})
        original = pull.cleanup_journal(self.inventory)
        pull.cleanup_quarantined_items(self.inventory)
        self.inventory.db.execute("INSERT INTO cleanup_quarantine VALUES(1,?,?,?,5,'now')", (*original[0][1:], 'rejected'))
        self.inventory.db.execute('DELETE FROM cleanup_receipts WHERE item_id=1')
        self.inventory.db.commit()
        # Simulate a stale pre-submit view; heartbeat releases before the worker
        # starts. The worker must atomically recheck both stores and refuse.
        real_blocked = pull.cleanup_blocked_items
        first = [True]
        def stale_check(inventory):
            if first[0]:
                first[0] = False
                pull.cleanup_release_quarantine(inventory, 1)
                return set()
            return real_blocked(inventory)
        replay = dict(replayed=0, failed=0, pending=0, quarantined=0, errors=[])
        with mock.patch.object(pull, 'cleanup_replay_receipts', return_value=replay), mock.patch.object(pull, 'cleanup_blocked_items', side_effect=stale_check), mock.patch.object(pull, 'cleanup_lease_v2', return_value=dict(tasks=[self.task])), mock.patch.object(pull, 'CleanupFile', side_effect=AssertionError('released receipt processed')):
            with pull.concurrent.futures.ThreadPoolExecutor(1) as executor:
                pull.cleanup_batch_v2(self.cfg, self.inventory, threading.Event(), executor, None, task_limit=1)
        self.assertEqual(pull.cleanup_journal(self.inventory), original)
        self.assertFalse(pull.cleanup_quarantined_items(self.inventory))

    def test_reservation_is_exclusive_and_release_does_not_replace_it(self):
        owner = pull.cleanup_reserve_item(self.inventory, 1)
        self.assertIsNotNone(owner)
        self.assertIsNone(pull.cleanup_reserve_item(self.inventory, 1))
        pull.cleanup_quarantined_items(self.inventory)
        self.inventory.db.execute("INSERT INTO cleanup_quarantine VALUES(1,'intent','exact bytes','rejected',5,'now')")
        self.inventory.db.commit()
        pull.cleanup_release_quarantine(self.inventory, 1)
        self.assertEqual(pull.cleanup_quarantined_items(self.inventory), {1})
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        with self.assertRaises(RuntimeError):
            pull.cleanup_journal(self.inventory, 1, 'observe', {}, owner=owner)

    def test_worker_never_overwrites_or_deletes_changed_checkpoint(self):
        owner = pull.cleanup_reserve_item(self.inventory, 1)
        pull.cleanup_journal(self.inventory, 1, 'observe', {'ours': True}, owner=owner)
        self.inventory.db.execute("UPDATE cleanup_receipts SET phase='intent',body='exact replacement bytes' WHERE item_id=1")
        self.inventory.db.commit()
        original = pull.cleanup_journal(self.inventory)
        for phase in ('observe', 'intent', 'result', None):
            with self.assertRaises(RuntimeError):
                pull.cleanup_journal(self.inventory, 1, phase, {}, owner=owner)
            self.assertEqual(pull.cleanup_journal(self.inventory), original)

    def test_protocol1_quarantine_blocks_worker(self):
        pull.cleanup_quarantined_items(self.inventory)
        self.inventory.db.execute("INSERT INTO cleanup_quarantine VALUES(1,'intent','exact bytes','rejected',5,'now')")
        self.inventory.db.commit()
        with mock.patch.object(pull, 'cleanup_replay_receipts'), mock.patch.object(pull, 'cleanup_lease', return_value=dict(tasks=[self.task])), mock.patch.object(pull, 'CleanupFile', side_effect=AssertionError('quarantined item')):
            with pull.concurrent.futures.ThreadPoolExecutor(1) as executor:
                pull.cleanup_batch(self.cfg, self.inventory, threading.Event(), executor, task_limit=1)
        self.assertEqual(pull.cleanup_quarantined_items(self.inventory), {1})
        self.assertEqual(pull.cleanup_journal(self.inventory), [])

    def test_quarantine_release_rolls_back_move_and_ack_on_failure(self):
        pull.cleanup_journal(self.inventory, 1, 'intent', {'evidence': 'exact bytes'})
        original = pull.cleanup_journal(self.inventory)[0]
        pull.cleanup_quarantined_items(self.inventory)
        self.inventory.db.execute("INSERT INTO cleanup_quarantine VALUES(1,?,?,?,5,'now')", (*original[1:], 'rejected'))
        self.inventory.db.execute('DELETE FROM cleanup_receipts WHERE item_id=1')
        self.inventory.db.execute("CREATE TRIGGER fail_release BEFORE DELETE ON cleanup_quarantine BEGIN SELECT RAISE(ABORT,'fixture'); END")
        self.inventory.db.commit()
        with self.assertRaises(pull.sqlite3.IntegrityError):
            pull.cleanup_release_quarantine(self.inventory, 1)
        self.assertEqual(pull.cleanup_journal(self.inventory), [])
        self.assertEqual(pull.cleanup_quarantined_items(self.inventory), {1})
        self.assertEqual(pull.cleanup_quarantine_status(self.inventory)['release_generation'], 0)

    def test_concurrent_released_item_cannot_replace_active_worker_receipt(self):
        entered, release = threading.Event(), threading.Event()
        def observe(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise RuntimeError('fixture worker timed out')
            return fixture_ack(args[2], action='would_delete', delete=False)
        with mock.patch.object(pull, 'restore_api', side_effect=observe) as post:
            with pull.concurrent.futures.ThreadPoolExecutor(1) as executor:
                first = executor.submit(pull.cleanup_process_task, self.cfg, self.inventory, threading.Event(), self.task)
                try:
                    self.assertTrue(entered.wait(5))
                    original = pull.cleanup_journal(self.inventory)
                    self.assertFalse(pull.cleanup_process_task(self.cfg, self.inventory, threading.Event(), dict(self.task, attempt=2)))
                    self.assertEqual(pull.cleanup_journal(self.inventory), original)
                    self.assertEqual(post.call_count, 1)
                finally:
                    release.set()
                first.result(timeout=5)
        self.assertEqual(pull.cleanup_journal(self.inventory), [])

    def test_stale_replay_snapshot_cannot_remove_replacement_evidence(self):
        self.journal_observe(900)
        journal = pull.cleanup_journal
        replacement = ('intent', '{"evidence":"replacement bytes"}')
        def snapshot(inventory, *args, **kwargs):
            rows = journal(inventory, *args, **kwargs)
            inventory.db.execute('UPDATE cleanup_receipts SET phase=?,body=? WHERE item_id=900', replacement)
            inventory.db.commit()
            return rows
        with mock.patch.object(pull, 'cleanup_journal', side_effect=snapshot), mock.patch.object(pull, 'restore_api', side_effect=AssertionError('stale replay sent')):
            pull.cleanup_replay_receipts(self.cfg, self.inventory)
        self.assertEqual(journal(self.inventory), [(900, *replacement)])

    def test_all_skipped_cycle_waits(self):
        self.journal_observe(900)
        calls = []
        task = dict(self.task, item_id=900)
        def lease(*args):
            calls.append(1)
            return dict(tasks=[task], retry_after_sec=120)
        replay = dict(replayed=0, failed=0, pending=1, quarantined=0, errors=[])
        with mock.patch.object(pull, 'cleanup_replay_receipts', return_value=replay), mock.patch.object(pull, 'cleanup_lease_v2', side_effect=lease), mock.patch.object(pull, 'cleanup_process_task', side_effect=AssertionError('blocked item')):
            with pull.concurrent.futures.ThreadPoolExecutor(1) as executor:
                wait = pull.cleanup_batch_v2(self.cfg, self.inventory, threading.Event(), executor, None, task_limit=8)
        self.assertEqual(wait, 120)
        self.assertLessEqual(len(calls), 8)

    def test_halving_stops_at_two_levels(self):
        with mock.patch.object(pull, 'cleanup_api_retry', side_effect=pull.urllib.error.HTTPError('/x', 503, 'restart', {}, None)) as post:
            batcher = pull.CleanupBatcher(self.cfg, '/x', 'observations', senders=0)
            results = batcher.post([dict(item_id=n) for n in range(64)])
        self.assertEqual(post.call_count, 7)
        self.assertEqual(len(results), 64)
        self.assertTrue(all(isinstance(v, pull.urllib.error.HTTPError) for v in results.values()))

    def test_malformed_replay_never_drops_receipt(self):
        for response in ({}, [], dict(action='reconcile'), dict(action='unknown')):
            self.journal_observe(900)
            original = pull.cleanup_journal(self.inventory)
            with mock.patch.object(pull, 'restore_api', return_value=response):
                counts = pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=1)
            self.assertEqual(counts['failed'], 1)
            self.assertEqual(pull.cleanup_journal(self.inventory), original)

    def test_missing_recovered_result_ack_preserves_advanced_receipt(self):
        self.journal_observe(900)
        def api(cfg, path, body, **kwargs):
            if path.endswith('/reconcile'):
                return fixture_ack(body, 900, action='reconcile', delete=False)
            return None
        with mock.patch.object(pull, 'restore_api', side_effect=api), mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('replay must not delete')):
            counts = pull.cleanup_replay_receipts(self.cfg, self.inventory, budget=1)
        self.assertEqual(counts['failed'], 1)
        rows = pull.cleanup_journal(self.inventory)
        self.assertEqual(rows[0][1], 'result')
        self.assertEqual(json.loads(rows[0][2])['outcome'], 'unverified')

    def test_all_diagnostic_patterns(self):
        self.assertNotIn('hidden-value', json.dumps(pull.redact_diagnostics({'nested': {'api_key': 'hidden-value'}})))
        for line, secret in diagnostic_patterns():
            with self.subTest(line=line):
                self.assertNotIn(secret, pull.redact_log_line(line))
                self.assertNotIn(secret, json.dumps(pull.redact_diagnostics(dict(last_error=line))))
                pull.CLEANUP_STATUS.fail(line)
                self.assertNotIn(secret, pull.CLEANUP_STATUS.snapshot()[1])

    def test_log_upload_error_storm_respects_minimum_gap(self):
        stop = threading.Event()
        clock = [1000.0]
        attempts = []
        def event_wait(wait):
            clock[0] += 0.01
            return True
        def pause(delay):
            clock[0] += delay
            return False
        def upload(*args):
            attempts.append(clock[0])
            if len(attempts) == 4:
                stop.set()
            raise OSError('offline')
        with mock.patch.object(pull.CLIENT_LOG_RING, 'configure'), mock.patch.object(pull.CLIENT_LOG_RING.error_event, 'wait', side_effect=event_wait), mock.patch.object(stop, 'wait', side_effect=pause), mock.patch.object(pull.time, 'monotonic', side_effect=lambda: clock[0]), mock.patch.object(pull, 'client_log_upload_once', side_effect=upload):
            pull.client_log_upload_loop(self.cfg, None, self.inventory, stop)
        self.assertTrue(all(b-a >= 60 for a,b in zip(attempts, attempts[1:])), attempts)


def diagnostic_patterns():
    value = 'fixture-sensitive-value'
    return [
        ('{"api_key": "' + value + '"}', value),
        ("{'token': '" + value + "'}", value),
        ('secret=' + value, value),
        (json.dumps({'api_key': 'prefix\"' + value}), value),
        ('Authorization: Bearer ' + value, value),
        ('{"Authorization": "Bearer ' + value + '"}', value),
        (json.dumps({'Authorization': ['Bearer ' + value]}), value),
        (json.dumps({'Cookie': {'session': value}}), value),
        ('Authorization: Digest username="' + value + '", nonce="' + value + '", cnonce="' + value + '", response="' + value + '"', value),
        (json.dumps(json.dumps({'api_key': value})), value),
        (json.dumps(json.dumps(json.dumps({'password': value}))), value),
        ('fd7a:115c:a1e0::1', 'fd7a:115c:a1e0::1'),
        ('[FD7A:115C:A1E0:abcd:1234:5678:90ab:cdef]:443', 'FD7A:115C:A1E0'),
        ('https://user:' + value + '@host/path', value),
        *[(scheme + '://user:' + value + '@host/db', value) for scheme in ('postgres', 'postgresql', 'mysql', 'redis', 'rediss')],
        ('eyJ' + 'a'*20 + '.' + 'b'*20 + '.' + 'c'*20, 'a'*20),
        ('tskey-' + 'auth-' + value, value),
        ('AKIA' + 'Q'*16, 'Q'*16),
        ('{"R2_ACCESS_KEY_ID": "' + 'a'*32 + '"}', 'a'*32),
        ('https://host/path?X-Amz-Signature=' + value + '&other=x', value),
        ('https://nas.example.ts.net/path', 'nas.example.ts.net'),
        ('100.64.0.1 100.127.255.255', '100.64.0.1'),
        ('100.127.255.255', '100.127.255.255'),
    ]


class CleanupAcknowledgementTests(unittest.TestCase):
    def response(self, case, body, phase):
        if case == 'malformed_json':
            return json.loads('{')
        if case == 'http_5xx':
            raise pull.urllib.error.HTTPError('/fixture', 503, 'unavailable', {}, None)
        ack = fixture_ack(body, action='would_delete', delete=False) if phase == 'observe' else fixture_ack(body, ok=True)
        changes = {'empty': {}, 'negative': {'ok': False}, 'negative_bound': dict(ack, ok=False), 'wrong_item': dict(ack, item_id=999),
                   'wrong_attempt': dict(ack, attempt=2), 'wrong_plan': dict(ack, plan_sha256='f'*64),
                   'wrong_proof': dict(ack, proof_sha256='f'*64), 'boolean_item': dict(ack, item_id=True),
                   'malformed_action': dict(ack, action='unknown'), 'valid': ack}
        return changes[case]

    def fixture(self):
        fixture = CleanupTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        return fixture

    def test_fresh_result_requires_exact_positive_ack_and_replay_never_unlinks(self):
        for protocol in (1, 2):
            for case in ('empty', 'negative', 'negative_bound', 'malformed_json', 'wrong_item', 'wrong_attempt', 'wrong_plan', 'wrong_proof', 'boolean_item', 'http_5xx', 'valid'):
                with self.subTest(protocol=protocol, case=case):
                    f = self.fixture()
                    with f.opened() as current:
                        f.task['approved_proof'] = current.proof()
                    calls = []
                    def observe(cfg, item_id, body, **kwargs):
                        calls.append('observe')
                        return fixture_ack(body, delivery_keep_checked=True, action='delete_issued', delete=True,
                                           expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
                    def result(cfg, item_id, body):
                        calls.append('result')
                        return self.response(case, body, 'result')
                    api = SimpleNamespace(request_ids=protocol == 2, observe=observe, result=result)
                    if case == 'valid':
                        pull.cleanup_process_task(f.cfg, f.inventory, threading.Event(), f.task, api)
                        self.assertEqual(pull.cleanup_journal(f.inventory), [])
                    else:
                        with self.assertRaises((pull.CleanupResponseError, json.JSONDecodeError, pull.urllib.error.HTTPError)):
                            pull.cleanup_process_task(f.cfg, f.inventory, threading.Event(), f.task, api)
                        original = pull.cleanup_journal(f.inventory)
                        self.assertEqual(original[0][1], 'result')
                        self.assertEqual(json.loads(original[0][2])['outcome'], 'deleted')
                        with mock.patch.object(pull, 'CleanupFile', side_effect=AssertionError('must not reopen/unlink')):
                            self.assertFalse(pull.cleanup_process_task(f.cfg, f.inventory, threading.Event(), f.task, api))
                            replay = pull.cleanup_replay_receipts(f.cfg, f.inventory, api)
                            self.assertEqual(replay['failed'], 1)
                            self.assertEqual(pull.cleanup_journal(f.inventory), original)
                            api.result = lambda cfg, item_id, body: fixture_ack(body, ok=True)
                            pull.cleanup_replay_receipts(f.cfg, f.inventory, api)
                        self.assertEqual(pull.cleanup_journal(f.inventory), [])
                    self.assertFalse(f.path.exists())
                    self.assertFalse(f.sidecar.exists())
                    self.assertEqual(calls.count('observe'), 1, 'must never ask for another permit')

    def test_fresh_observation_and_conflict_checkpoint_require_valid_ack(self):
        for conflict in (False, True):
            for case in ('empty', 'negative', 'malformed_json', 'wrong_item', 'wrong_attempt', 'wrong_plan', 'wrong_proof', 'http_5xx', 'malformed_action', 'valid'):
                with self.subTest(conflict=conflict, case=case):
                    f = self.fixture()
                    if conflict:
                        f.item['sha256'] = 'f'*64
                    def observe(cfg, item_id, body, **kwargs):
                        ack = self.response(case, body, 'observe')
                        if case == 'valid' and conflict:
                            ack['action'] = 'conflict'
                        return ack
                    api = SimpleNamespace(request_ids=True, observe=observe)
                    if case == 'valid':
                        pull.cleanup_process_task(f.cfg, f.inventory, threading.Event(), f.task, api)
                        self.assertEqual(pull.cleanup_journal(f.inventory), [])
                    else:
                        with self.assertRaises((pull.CleanupResponseError, json.JSONDecodeError, pull.urllib.error.HTTPError)):
                            pull.cleanup_process_task(f.cfg, f.inventory, threading.Event(), f.task, api)
                        original = pull.cleanup_journal(f.inventory)
                        self.assertEqual(original[0][1], 'observe')
                        self.assertFalse(pull.cleanup_process_task(f.cfg, f.inventory, threading.Event(), f.task, api))
                        self.assertEqual(pull.cleanup_journal(f.inventory), original)
                    self.assertTrue(f.path.exists())
                    self.assertTrue(f.sidecar.exists())

    def test_diagnostic_pairs_and_unicode_names_in_text_and_structures(self):
        secret = 'fixture-sensitive-value'
        for name in ('Authorization', 'aUtHoRiZaTiOn', 'Proxy-Authorization', 'Cookie', 'Set-Cookie', 'token', 'api_key', 'password'):
            escaped = ''.join('\\u%04x' % ord(c) for c in name)
            for key in (name, escaped):
                for pair in ([key, 'Digest username="u", response="%s"' % secret], (key, {'nested': secret})):
                    with self.subTest(key=key, pair=pair):
                        for value in ({'headers': [pair]}, {'outer': {key: secret}}):
                            self.assertNotIn(secret, json.dumps(pull.redact_diagnostics(value)))
                        for line in (repr(pair), json.dumps(pair), 'cleanup ' + json.dumps({key: secret}),
                                     json.dumps(json.dumps({key: secret})), '[[' + json.dumps(key) + ',"' + secret):
                            self.assertNotIn(secret, pull.redact_log_line(line), line)
        self.assertEqual(pull.redact_diagnostics([['Accept', 'application/json']]), [['Accept', 'application/json']])

class DiagnosticSharedFixtureTests(unittest.TestCase):
    def test_config_secret_encodings_are_removed_before_ring_storage(self):
        import base64
        import html
        import urllib.parse
        canary = 'synthetic-R4-<&>"/é+\\canary'
        cfg = SimpleNamespace(api_key=canary, service_token='service-canary-123',
                              pull_token='pull-canary-123', r2={'access_key_id': 'r2-canary-123'},
                              s3={'secret_access_key': 's3-canary-123'}, db_pass='db-canary-123',
                              relative_path='Stream/Day/Case.mp4', short_secret='tiny')
        ring = pull.ClientLogRing()
        with mock.patch.dict(pull.os.environ, {'CUSTOM_CREDENTIAL': 'env-canary-123'}):
            ring.configure(cfg)
        expected = {canary, cfg.service_token, cfg.pull_token, 'r2-canary-123', 's3-canary-123',
                    cfg.db_pass, 'env-canary-123'}
        self.assertTrue(expected.issubset(set(ring.secrets)))
        self.assertNotIn('tiny', ring.secrets)
        self.assertNotIn(cfg.relative_path, ring.secrets)
        raw = canary.encode()
        encodings = [canary, base64.b64encode(raw).decode(), base64.urlsafe_b64encode(raw).decode(),
                     base64.b64encode(raw).decode().rstrip('='),
                     urllib.parse.quote(canary, safe=''), urllib.parse.quote_plus(canary),
                     ''.join('%%%02X' % b for b in raw), ''.join('%%%02x' % b for b in raw),
                     html.escape(canary), html.escape(canary, quote=False), json.dumps(canary)[1:-1],
                     ''.join('\\u%04x' % ord(c) for c in canary), raw.hex(), raw.hex().upper()]
        safe = 'item_id=42 relative_path=Stream/Day/Case.mp4 size_bytes=123456 sha256=' + 'a' * 64
        for encoded in encodings:
            with self.subTest(encoding=encoded):
                ring.record('WARN', 'opaque ' + encoded + ' ' + safe)
                stored = ring.lines[-1][0]['message']
                self.assertNotIn(encoded, stored)
                self.assertNotIn(canary, stored)
                self.assertIn('[REDACTED]', stored)
                self.assertIn(safe, stored)
        ring.record('WARN', 'tiny ' + safe)
        self.assertEqual(ring.lines[-1][0]['message'], 'tiny ' + safe)

    def test_received_cookie_and_session_values_join_secret_backstop(self):
        from email.message import Message
        import io
        headers = Message()
        headers.add_header('Set-Cookie', 'SID=received-cookie-canary; Path=/; HttpOnly')
        headers.add_header('Set-Cookie', 'opaque=second-cookie-canary; Secure')
        ring = pull.ClientLogRing()
        cfg = SimpleNamespace(api_base='https://example.invalid', api_key='test-config-canary')
        with mock.patch.object(pull, 'CLIENT_LOG_RING', ring):
            response = io.BytesIO(json.dumps({'session': 'received-session-canary',
                                             'object_key': 'Stream/Day/Case.mp4'}).encode())
            response.headers = headers
            with mock.patch.object(pull.urllib.request, 'urlopen', return_value=response):
                pull.request_json(cfg, 'GET', '/test')
            self.assertNotIn('Stream/Day/Case.mp4', ring.secrets)
            ring.configure(cfg)  # uploader configuration must retain learned values
            for value in ('received-cookie-canary', 'second-cookie-canary', 'received-session-canary'):
                ring.record('WARN', 'opaque ' + value)
                self.assertNotIn(value, ring.lines[-1][0]['message'])
            error = pull.urllib.error.HTTPError(cfg.api_base, 403, 'denied', headers, None)
            ring.secrets = ()
            with mock.patch.object(pull.urllib.request, 'urlopen', side_effect=error), self.assertRaises(pull.urllib.error.HTTPError):
                pull.request_json(cfg, 'GET', '/test')
            ring.record('WARN', 'opaque received-cookie-canary')
            self.assertNotIn('received-cookie-canary', ring.lines[-1][0]['message'])

    def test_native_cookie_and_buffer_reprs(self):
        from http.cookiejar import Cookie
        from http.cookies import SimpleCookie
        canary = 'synthetic-cookie-canary'
        cookie = Cookie(0, 'SID', canary, None, False, 'example.com', True, False,
                        '/', True, False, None, True, None, None, {})
        simple = SimpleCookie({'SID': canary})
        for value in (cookie, simple, simple['SID']):
            for line in (str(value), repr(value)):
                ring = pull.ClientLogRing()
                ring.record('WARN', line)
                self.assertNotIn(canary, ring.lines[-1][0]['message'])
        for wrap in (bytes, bytearray, memoryview):
            value = [[wrap(b'Cookie'), wrap(canary.encode())]]
            self.assertNotIn(canary, json.dumps(pull.redact_diagnostics(value)))
            ring = pull.ClientLogRing()
            ring.record('WARN', value)
            self.assertNotIn(canary, ring.lines[-1][0]['message'])

    def test_shared_redaction_vectors_and_upload_ring(self):
        fixture = Path(__file__).resolve().parents[2] / 'testdata/nas_diagnostic_redaction.json'
        for vector in json.loads(fixture.read_text())['vectors']:
            with self.subTest(vector=vector['name']):
                value = vector['input']
                result = pull.redact_diagnostics(value)
                rendered = result if isinstance(result, str) else json.dumps(result)
                ring = pull.ClientLogRing()
                ring.record('WARN', value if isinstance(value, str) else repr(value))
                stored = ring.lines[0][0]['message']
                for secret in vector['absent']:
                    self.assertNotIn(secret, rendered)
                    self.assertNotIn(secret, stored)
                for diagnostic in vector['present']:
                    self.assertIn(diagnostic, rendered)
                    self.assertIn(diagnostic, stored)
                if vector['unchanged']:
                    self.assertEqual(result, value)
                    if isinstance(value, str):
                        self.assertEqual(stored, value)

    def test_native_bytes_are_redacted_before_ring_storage(self):
        for name in (b'Cookie', b'Authorization', b'Proxy-Authorization', b'Session'):
            value = {b'headers': [(name, b'synthetic-sensitive-value')]}
            self.assertNotIn('synthetic-sensitive-value', json.dumps(pull.redact_diagnostics(value)))
            ring = pull.ClientLogRing()
            ring.record('WARN', value)
            self.assertNotIn('synthetic-sensitive-value', ring.lines[0][0]['message'])

class DiagnosticPerformanceTests(unittest.TestCase):
    def assert_fast(self, call):
        start = time.perf_counter()
        result = call()
        elapsed = time.perf_counter() - start
        self.assertLess(elapsed, 0.050, 'redaction took %.3f ms' % (elapsed * 1000))
        return result

    def test_pathological_inputs_are_bounded_and_fast(self):
        for size in (8192, 32768, 1024 * 1024):
            for unit in ('%61' + 'a' * size, '=' * size, ':' * size,
                         "'" * size, '"' * size, '\\' * size,
                         '[' * size, '(' * size, 'a.' * (size // 2),
                         'https://' * (size // 8), '["' + ' ' * size,
                         'x-amz-' + 'a' * size, 'token' + 'a' * size,
                         '%25' * (size // 3)):
                with self.subTest(size=size, prefix=unit[:20]):
                    result = self.assert_fast(lambda: pull.redact_log_line(unit))
                    self.assertLessEqual(len(result), pull.CLIENT_LOG_LINE_MAX)
                    self.assertIn('[TRUNCATED]', result)
                    structured = self.assert_fast(lambda: pull.redact_diagnostics({'last_error': unit}))
                    self.assertLessEqual(len(structured['last_error']), pull.CLIENT_LOG_LINE_MAX)

    def test_url_userinfo_nested_queries_and_safe_fragments_are_preserved(self):
        for value in ('1https://u:private-canary@host.invalid',
                      '.-https://u:private-canary@host.invalid',
                      'https://outer.invalid/#https://inner.invalid/file?sig=private-canary',
                      'https://outer.invalid/#safe?note#https://inner.invalid/file?sig=private-canary',
                      'https://?note=https://inner.invalid/file?sig=private-canary'):
            self.assertNotIn('private-canary', pull.redact_log_line(value))
        safe = 'https://example.invalid/file#fragment?size=42'
        self.assertEqual(pull.redact_log_line(safe), safe)

    def test_deep_and_cyclic_diagnostics_fail_closed_quickly(self):
        value = 'private-canary'
        for _ in range(10000):
            value = [value]
        result = self.assert_fast(lambda: pull.redact_diagnostics(value))
        self.assertNotIn('private-canary', json.dumps(result))
        cycle = []
        cycle.append(cycle)
        self.assertIn('[REDACTED]', json.dumps(self.assert_fast(lambda: pull.redact_diagnostics(cycle))))

    def test_cut_sensitive_values_and_known_secret_prefixes(self):
        canary = 'synthetic fragment with spaces and punctuation / end'
        for fragment in (canary[:8], canary[:20], canary[:-1]):
            prefix = 'safe ' * ((pull.DIAGNOSTIC_INPUT_MAX - len(fragment)) // 5)
            prefix += ' ' * (pull.DIAGNOSTIC_INPUT_MAX - len(fragment) - len(prefix))
            text = prefix + canary + ' extra'
            result = pull.redact_log_line(text, secrets=(canary,))
            self.assertNotIn(fragment, result)
            self.assertIn('[TRUNCATED]', result)
            self.assertLessEqual(len(result), pull.CLIENT_LOG_LINE_MAX)
        for lead in ('Authorization: Bearer ', 'password="', '["api_key", "',
                     'Digest ', 'https://host.invalid/file?sig='):
            prefix = 'ok ' * ((pull.DIAGNOSTIC_INPUT_MAX - len(lead) - 10) // 3)
            result = pull.redact_log_line(prefix + lead + 'private-canary-value' * 10)
            self.assertNotIn('private', result)
            self.assertIn('[REDACTED]', result)
            self.assertIn('[TRUNCATED]', result)
        for variant in pull.diagnostic_secret_variants(canary):
            prefix = 'x ' * ((pull.DIAGNOSTIC_INPUT_MAX - 12) // 2)
            result = pull.redact_log_line(prefix + variant + ' extra', secrets=(canary,))
            self.assertNotIn(variant[:8], result)

    def test_buffers_and_each_structured_item_are_capped_before_normalization(self):
        original = pull.normalize_diagnostic_text
        observed = []
        def bounded(text):
            observed.append(len(text))
            self.assertLessEqual(len(text), pull.DIAGNOSTIC_INPUT_MAX)
            return original(text)
        with mock.patch.object(pull, 'normalize_diagnostic_text', side_effect=bounded):
            for wrap in (str, lambda x: x.encode(), lambda x: bytearray(x.encode()),
                         lambda x: memoryview(x.encode()),
                         lambda x: memoryview(x.encode()).cast('I')):
                result = pull.redact_diagnostics({'errors': {'first': wrap('a' * 32768), 'second': wrap('b' * 32768)}})
                self.assertTrue(all('[TRUNCATED]' in x for x in result['errors'].values()))
        self.assertTrue(observed)
        self.assertEqual(list(pull.redact_diagnostics({'a' * 32768: 'private-canary'}).values()), ['[REDACTED]'])

    def test_redaction_exceptions_never_return_raw_text(self):
        class BrokenText:
            def __str__(self):
                raise ValueError('private-canary')
        class BrokenDict(dict):
            def items(self):
                raise ValueError('private-canary')
        self.assertEqual(pull.redact_log_line(BrokenText()), '[REDACTED]')
        self.assertEqual(pull.redact_diagnostics(BrokenDict()), '[REDACTED]')
        for stage in ('bounded_diagnostic_text', 'normalize_diagnostic_text', 'diagnostic_secret_variants',
                      'redact_known_diagnostic_secrets', 'finish_diagnostic_text'):
            with mock.patch.object(pull, stage, side_effect=RuntimeError('private-canary')):
                self.assertEqual(pull.redact_log_line('private-canary', secrets=('private-canary',)), '[REDACTED]')
        with mock.patch.object(pull, 'diagnostic_secret_name', side_effect=RuntimeError('private-canary')):
            self.assertEqual(pull.redact_diagnostics({'api_key': 'private-canary'}), '[REDACTED]')

    def test_log_http_error_returns_quickly_and_ring_is_not_locked_during_redaction(self):
        import io
        from contextlib import redirect_stdout
        ring = pull.ClientLogRing()
        original = pull.redact_log_line
        def unlocked(*args, **kwargs):
            acquired = ring.lock.acquire(blocking=False)
            self.assertTrue(acquired, 'redaction holds ring lock')
            if acquired:
                ring.lock.release()
            return original(*args, **kwargs)
        error = pull.urllib.error.HTTPError('https://example.invalid', 403, '%61' + 'a' * 32768, None, None)
        with mock.patch.object(pull, 'CLIENT_LOG_RING', ring), mock.patch.object(pull, 'redact_log_line', side_effect=unlocked), redirect_stdout(io.StringIO()):
            self.assert_fast(lambda: pull.log('ERROR', error))
        self.assertEqual(len(ring.lines), 1)
        self.assertIn('[TRUNCATED]', ring.lines[0][0]['message'])
        self.assertTrue(ring.error_event.is_set())

if __name__=='__main__':unittest.main()
