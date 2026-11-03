"""Exercise the actual cleanup worker against disposable media fixtures only."""
import importlib.util
import datetime
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

    def opened(self):
        return pull.CleanupFile(self.cfg, self.item, threading.Event(), time.monotonic()+20)

    def test_readonly_worker_exact_proof_and_durable_ack(self):
        calls=[]
        def api(cfg,path,body):
            calls.append((path,body))
            if path.endswith('/lease'): return dict(tasks=[self.task],retry_after_sec=0)
            self.assertEqual(len(pull.cleanup_journal(self.inventory)),1)
            self.assertEqual(body['outcome'],'match')
            self.assertEqual(body['proof']['identity'],self.item['frozen_identity'])
            return dict(action='would_delete',delete=False,recovery='nas_only_at_check')
        with mock.patch.object(pull,'restore_api',side_effect=api), mock.patch.object(pull.os,'unlink',side_effect=AssertionError('dry run must never unlink')):
            pull.cleanup_once(self.cfg,self.inventory,threading.Event())
        self.assertTrue(self.path.exists());self.assertTrue(self.sidecar.exists())
        self.assertEqual(pull.cleanup_journal(self.inventory),[])
        self.assertEqual(len(calls),2)

    def test_approved_disposable_fixture_only_deletes_exact_files(self):
        with self.opened() as f: proof=f.proof()
        self.task['approved_proof']=proof
        extra=self.path.parent/'keep.txt';extra.write_text('keep')
        # Re-observe the complete parent membership after introducing retained data.
        with self.opened() as f: self.task['approved_proof']=f.proof()
        def api(cfg,path,body):
            if path.endswith('/lease'):return dict(tasks=[self.task],retry_after_sec=0)
            if path.endswith('/observe'):
                return dict(action='delete_issued',delete=True,item_id=1,attempt=1,plan_sha256='a'*64,
                            proof_sha256=hashlib.sha256(json.dumps(body['proof']).encode()).hexdigest(),
                            expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
            self.assertEqual(body['outcome'],'deleted');return dict(ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api):pull.cleanup_once(self.cfg,self.inventory,threading.Event())
        self.assertFalse(self.path.exists());self.assertFalse(self.sidecar.exists());self.assertTrue(extra.exists())
        self.assertTrue(extra.parent.is_dir());self.assertEqual(pull.cleanup_journal(self.inventory),[])

    def test_unsolicited_delete_without_execution_phase_preserves_media(self):
        def api(cfg,path,body):
            if path.endswith('/lease'):return dict(tasks=[self.task],retry_after_sec=0)
            if path.endswith('/observe'):return dict(delete=True,action='delete_issued')
            self.assertEqual(body['outcome'],'unverified');return dict(ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api):pull.cleanup_once(self.cfg,self.inventory,threading.Event())
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
        def api(cfg,path,body):
            calls.append(path)
            if path.endswith('/observe'):return dict(action='reconcile',delete=False,proof_sha256='b'*64)
            self.assertEqual(body['outcome'],'unverified');return dict(ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api),mock.patch.object(pull.os,'unlink',side_effect=AssertionError('replay must not delete')):
            pull.cleanup_replay_receipts(self.cfg,self.inventory)
        self.assertEqual(len(calls),2);self.assertTrue(self.path.exists());self.assertFalse(pull.cleanup_journal(self.inventory))

    def test_interrupted_intent_never_retries_unlink(self):
        pull.cleanup_journal(self.inventory,1,'intent',dict(attempt=1,plan_sha256='a'*64,proof_sha256='b'*64))
        with mock.patch.object(pull,'restore_api',return_value=dict(ok=True)) as api:
            pull.cleanup_replay_receipts(self.cfg,self.inventory)
        self.assertEqual(api.call_args.args[2]['outcome'],'unverified');self.assertTrue(self.path.exists())

    def test_writer_fence_waits_for_delivery_without_serializing_deliveries(self):
        fence=pull.NASMediaFence();entered=threading.Event();release=threading.Event()
        @fence.delivery
        def writer():entered.set();release.wait(3)
        thread=threading.Thread(target=writer);thread.start();self.assertTrue(entered.wait(1))
        with self.assertRaises(RuntimeError):fence.acquire_cleanup(threading.Event(),time.monotonic()+0.05)
        release.set();thread.join(1)
        fence.acquire_cleanup(threading.Event(),time.monotonic()+1);fence.release_cleanup()

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
        with mock.patch.object(pull,'restore_api',return_value=dict(tasks=[self.task],retry_after_sec=0)) as api:
            with self.assertRaises(pull.CleanupInterrupted):pull.cleanup_once(self.cfg,self.inventory,event)
        self.assertEqual(api.call_count,1)
        self.assertFalse(pull.cleanup_journal(self.inventory));self.assertTrue(self.path.exists())

    def test_late_permit_rejected_independently_of_wall_clock(self):
        with self.opened() as f:self.task['approved_proof']=f.proof()
        original=time.monotonic;offset=[0]
        def api(cfg,path,body):
            if path.endswith('/lease'):return dict(tasks=[self.task],retry_after_sec=0)
            if path.endswith('/observe'):
                offset[0]=31
                return dict(action='delete_issued',delete=True,item_id=1,attempt=1,plan_sha256='a'*64,
                            proof_sha256=hashlib.sha256(json.dumps(body['proof']).encode()).hexdigest(),
                            expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
            self.assertEqual(body['outcome'],'unverified');return dict(ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api),mock.patch.object(pull.time,'monotonic',side_effect=lambda:original()+offset[0]):
            pull.cleanup_once(self.cfg,self.inventory,threading.Event())
        self.assertTrue(self.path.exists());self.assertTrue(self.sidecar.exists())


    def test_permit_deadline_checked_after_slow_final_directory_walk(self):
        with self.opened() as f:self.task['approved_proof']=f.proof()
        original_clock=time.monotonic;original_members=pull.CleanupFile.parent_members
        offset=[0];permit_seen=[False]
        def members(current):
            result=original_members(current)
            if permit_seen[0]:offset[0]=31
            return result
        def api(cfg,path,body):
            if path.endswith('/lease'):return dict(tasks=[self.task],retry_after_sec=0)
            if path.endswith('/observe'):
                permit_seen[0]=True
                return dict(action='delete_issued',delete=True,item_id=1,attempt=1,plan_sha256='a'*64,
                            proof_sha256=hashlib.sha256(json.dumps(body['proof']).encode()).hexdigest(),
                            expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
            self.assertEqual(body['outcome'],'unverified');return dict(ok=True)
        with mock.patch.object(pull,'restore_api',side_effect=api),mock.patch.object(pull.time,'monotonic',side_effect=lambda:original_clock()+offset[0]),mock.patch.object(pull.CleanupFile,'parent_members',members):
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
                fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600,dir_fd=parent)
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
        def api(cfg,path,body):
            with lock:
                calls.append(path)
                if path.endswith("/lease"):
                    counter[0]+=1
                    task=copy.deepcopy(self.task);task["item_id"]=counter[0]
                    return dict(tasks=[task],retry_after_sec=0)
            barrier.wait(4)
            return dict(delete=False,action="would_delete",recovery="r2_sha_verified")
        with pull.concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            with mock.patch.object(pull,"restore_api",side_effect=api),mock.patch.object(pull.os,"unlink",side_effect=AssertionError("preflight never unlinks")):
                self.assertEqual(pull.cleanup_batch(self.cfg,self.inventory,threading.Event(),executor,workers=4,task_limit=4),0)
        self.assertEqual(len(calls),8)
        self.assertFalse(pull.cleanup_journal(self.inventory))
        self.assertTrue(self.path.exists());self.assertTrue(self.sidecar.exists())

    def test_execution_drains_preflight_before_serial_processing(self):
        order=[];counter=[0]
        def api(cfg,path,body):
            counter[0]+=1
            task=dict(self.task,item_id=counter[0])
            if counter[0]==2:task["approved_proof"]={}
            return dict(tasks=[task],retry_after_sec=0)
        def process(cfg,inventory,stop,task):
            if task.get("approved_proof") is not None:
                order.append("execute")
            else:
                order.append("preflight_start");time.sleep(0.03);order.append("preflight_end")
        with pull.concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            with mock.patch.object(pull,"restore_api",side_effect=api),mock.patch.object(pull,"cleanup_process_task",side_effect=process):
                pull.cleanup_batch(self.cfg,self.inventory,threading.Event(),executor)
        self.assertEqual(order,["preflight_start","preflight_end","execute"])
        self.assertEqual(counter[0],2)

    def test_lease_failure_drains_workers_before_next_receipt_replay(self):
        finished=threading.Event();counter=[0]
        def api(cfg,path,body):
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
        def api(cfg, path, body):
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
        def api(cfg, path, body):
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
        def api(cfg, path, body):
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
        def api(cfg, path, body):
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


if __name__=='__main__':unittest.main()
