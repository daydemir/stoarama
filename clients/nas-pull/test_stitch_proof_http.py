"""Proof-only absolute transport deadlines; synthetic children, no network."""
import datetime
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import test_stoarama_pull as base

pull = base.pull

# Substitute only the child's network boundary. The actual fixed child and
# parent framing, pipe, disk-output, timeout, kill and reap paths still execute.
FAKE_NETWORK = r'''
import urllib.request,time
class Response:
    status=200
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def read(self,limit):
        if MODE=='trickle':
            time.sleep(.01)
            return b'x'
        if MODE=='oversized':return b'x'*limit
        value=getattr(self,'body',b'{}');self.body=b''
        return value
class Opener:
    def open(self,*args,**kwargs):
        if MODE=='connect':time.sleep(30)
        return Response()
urllib.request.build_opener=lambda *args:Opener()
'''


class ProofHTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cfg = SimpleNamespace(state_dir=Path(self.temp.name),inventory_file=Path(self.temp.name)/"inventory.sqlite3",
            api_base="https://synthetic.invalid",api_key="SYNTHETIC_TOKEN",stitch_proof_enabled=True)
        self.backing = mock.patch.object(pull,"inventory_scratch_backing",return_value=(0,pull.InventoryScratchFilesystem.EXT4))
        self.backing.start();self.addCleanup(self.backing.stop)

    def stop(self, seconds=30):
        task = dict(expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=5)).isoformat(),
            plan_sha256="a"*64)
        stop = pull.StitchStop(self.cfg,threading.Event(),task,pull.StitchRunKind.PROOF)
        stop.deadline = time.monotonic()+seconds
        return stop

    def child(self, mode):
        return "MODE="+repr(mode)+"\n"+FAKE_NETWORK+pull.STITCH_PROOF_HTTP_CHILD

    def test_real_child_success_and_oversize_are_bounded_and_removed(self):
        with mock.patch.object(pull,"STITCH_PROOF_HTTP_CHILD",self.child("success")):
            self.assertEqual(pull.stitch_proof_request(self.cfg,"/synthetic",self.stop()),{})
        with mock.patch.object(pull,"STITCH_PROOF_HTTP_CHILD",self.child("oversized")):
            with self.assertRaisesRegex(pull.StitchHold,"response exceeds"):
                pull.stitch_proof_request(self.cfg,"/synthetic",self.stop())
        self.assertEqual(list(self.cfg.state_dir.iterdir()),[])

    def test_real_stalled_and_trickling_children_are_killed_and_reaped(self):
        real_popen = subprocess.Popen
        for mode in ("connect","trickle"):
            with self.subTest(mode=mode):
                children=[]
                def spawn(*args,**kwargs):
                    child=real_popen(*args,**kwargs);children.append(child);return child
                started=time.monotonic()
                with mock.patch.object(pull,"STITCH_PROOF_HTTP_CHILD",self.child(mode)), \
                        mock.patch.object(pull.subprocess,"Popen",side_effect=spawn):
                    with self.assertRaises((pull.StitchHold,pull.BenchmarkDeadline)):
                        pull.stitch_proof_request(self.cfg,"/synthetic",self.stop(2.3))
                self.assertEqual(len(children),1)
                self.assertIsNotNone(children[0].poll())
                self.assertNotEqual(children[0].returncode,0)
                self.assertLess(time.monotonic()-started,5)
        self.assertEqual(list(self.cfg.state_dir.iterdir()),[])

    def test_cleanup_cancellation_kills_a_blocked_child(self):
        stop=self.stop();cancel=threading.Timer(.1,stop.stop.set)
        started=time.monotonic()
        cancel.start()
        try:
            with mock.patch.object(pull,"STITCH_PROOF_HTTP_CHILD",self.child("connect")):
                with self.assertRaises(pull.StitchHold):
                    pull.stitch_proof_request(self.cfg,"/synthetic",stop)
        finally:cancel.join()
        self.assertLess(time.monotonic()-started,5)
        self.assertEqual(list(self.cfg.state_dir.iterdir()),[])

    def near_expiry_child(self, stop, clock, observed):
        child=SimpleNamespace(returncode=None)
        child.poll=lambda:child.returncode
        def kill():child.returncode=-9
        child.kill=kill
        def send(process,message,current_stop,deadline):
            observed.append((json.loads(message),min(.2,(deadline-time.monotonic_ns())/10**9)))
            clock[0]=stop.deadline-pull.STITCH_PROOF_HTTP_REAP_SEC+.01
        child.send=send
        child.wait=lambda timeout:child.returncode
        return child

    def test_near_expiry_checkpoint_clamps_total_budget_and_releases_lock(self):
        stop=self.stop();clock=[stop.deadline-pull.STITCH_PROOF_HTTP_REAP_SEC-.01];observed=[]
        child=self.near_expiry_child(stop,clock,observed)
        lock=threading.RLock()
        with mock.patch.object(pull.time,"monotonic",side_effect=lambda:clock[0]), \
                mock.patch.object(pull.time,"monotonic_ns",side_effect=lambda:int(clock[0]*10**9)), \
                mock.patch.object(pull.subprocess,"Popen",return_value=child) as spawn, \
                mock.patch.object(pull,"stitch_proof_input",side_effect=child.send), \
                mock.patch.object(pull,"stitch_resources"),mock.patch.object(pull,"stitch_proof_space"):
            with self.assertRaises(pull.StitchHold):
                with pull.stitch_proof_activity(SimpleNamespace(activity_lock=lock),stop):
                    stop.refresh()
                    if stop.is_set():raise pull.StitchHold("cancelled")
        self.assertFalse(lock._is_owned())
        self.assertEqual(child.returncode,-9)
        self.assertLessEqual(observed[0][0]["timeout"],.011)
        self.assertLessEqual(observed[0][1],.011)
        self.assertEqual(spawn.call_args.kwargs["env"],{})
        self.assertNotIn("SYNTHETIC_TOKEN",repr(spawn.call_args.args))

    def test_near_expiry_corpus_clamps_sql_and_total_network_budget(self):
        stop=self.stop();clock=[stop.deadline-pull.STITCH_PROOF_HTTP_REAP_SEC-.01];observed=[];waits=[]
        child=self.near_expiry_child(stop,clock,observed)
        real_connect=sqlite3.connect
        def connect(*args,**kwargs):
            waits.append(kwargs["timeout"]);return real_connect(*args,**kwargs)
        with mock.patch.object(pull.time,"monotonic",side_effect=lambda:clock[0]), \
                mock.patch.object(pull.time,"monotonic_ns",side_effect=lambda:int(clock[0]*10**9)), \
                mock.patch.object(pull.subprocess,"Popen",return_value=child), \
                mock.patch.object(pull,"stitch_proof_input",side_effect=child.send), \
                mock.patch.object(pull.sqlite3,"connect",side_effect=connect):
            with self.assertRaises(pull.BenchmarkDeadline):pull.stitch_corpus(self.cfg,stop)
        self.assertLessEqual(waits[0],pull.STITCH_PROOF_HTTP_REAP_SEC+.011)
        self.assertLessEqual(observed[0][0]["timeout"],.011)
        self.assertLessEqual(observed[0][1],.011)
        with closing(real_connect(self.cfg.inventory_file)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM v85r_stitch_corpus_meta").fetchone()[0],0)
        self.assertEqual(child.returncode,-9)

    def test_insufficient_reap_budget_refuses_before_spawning(self):
        stop=self.stop(.01)
        with mock.patch.object(pull.subprocess,"Popen") as spawn:
            with self.assertRaises(pull.BenchmarkDeadline):
                pull.stitch_proof_request(self.cfg,"/synthetic",stop)
        spawn.assert_not_called()

    def test_last_ten_milliseconds_corpus_sql_is_clamped_without_network(self):
        stop=self.stop();waits=[];real_connect=sqlite3.connect
        def connect(*args,**kwargs):
            waits.append(kwargs["timeout"]);return real_connect(*args,**kwargs)
        with mock.patch.object(pull.time,"monotonic",return_value=stop.deadline-.01), \
                mock.patch.object(pull.time,"monotonic_ns",return_value=int((stop.deadline-.01)*10**9)), \
                mock.patch.object(pull.sqlite3,"connect",side_effect=connect), \
                mock.patch.object(pull.subprocess,"Popen") as spawn:
            with self.assertRaises(pull.BenchmarkDeadline):pull.stitch_corpus(self.cfg,stop)
        self.assertLessEqual(waits[0],.011)
        spawn.assert_not_called()

    def test_real_corpus_sql_writer_contention_uses_remaining_deadline(self):
        with closing(sqlite3.connect(self.cfg.inventory_file)) as db:
            db.execute("CREATE TABLE v85r_stitch_corpus_meta(sha TEXT PRIMARY KEY)")
            db.execute("CREATE TABLE v85r_stitch_edges(a TEXT,b TEXT,nomination INTEGER,witness TEXT,PRIMARY KEY(a,b))")
            db.commit()
        stop=self.stop(.15)
        blocker=sqlite3.connect(self.cfg.inventory_file)
        self.addCleanup(blocker.close)
        blocker.execute("BEGIN IMMEDIATE")
        started=time.monotonic()
        with mock.patch.object(pull,"stitch_proof_http") as http:
            with self.assertRaises(sqlite3.OperationalError):pull.stitch_corpus(self.cfg,stop)
        self.assertLess(time.monotonic()-started,2)
        http.assert_not_called()


if __name__=="__main__":unittest.main()
