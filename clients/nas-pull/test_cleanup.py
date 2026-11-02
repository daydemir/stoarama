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


if __name__=='__main__':unittest.main()
