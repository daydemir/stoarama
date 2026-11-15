"""Cancellation through the real build setup and audit I/O; no native/network."""
import datetime
import ctypes
import hashlib
import io
import json
from pathlib import Path
import struct
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock
from contextlib import ExitStack

import test_stoarama_pull as base

pull = base.pull


class BuildCancellationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.cfg=SimpleNamespace(state_dir=self.root,inventory_file=self.root/"inventory.sqlite3",stitch_proof_enabled=True)
        plan=dict(policy=pull.STITCH_POLICY,revision=pull.STITCH_REVISION,corpus_sha256=pull.STITCH_CORPUS_SHA,
            keep_policy_sha256=pull.CLEANUP_KEEP28_POLICY_SHA256,recording_id=444,stream_id=17208,
            clips=[dict(source_id=n,relative_path="synthetic/%d.mp4"%n,size_bytes=1,sha256="a"*64) for n in (1,2)],
            joins=[True],eligible=[True,True],
            output_folder="synthetic_V85r_proof_keep28_v2/444/2026-08-09T09-00-00Z/proof-"+pull.STITCH_PROOF_SOURCE_SHA[:16])
        self.task=dict(id=1,kind=pull.STITCH_PROOF_KIND,source_plan_sha256=pull.STITCH_PROOF_SOURCE_SHA,plan=plan,
            plan_sha256=hashlib.sha256(pull.cleanup_json_bytes(plan)).hexdigest(),
            expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=5)).isoformat())

    def actual_build_cancellation(self, expired):
        tools={};pins={}
        for name in ("ffmpeg","ffprobe"):
            path=self.root/name;body=(name.encode()*600000)
            path.write_bytes(body);tools[name]=str(path);pins[name]=(hashlib.sha256(body).hexdigest(),len(body))
        for stage in ("provenance","bundle"):
            with self.subTest(stage=stage,expired=expired):
                if stage=="bundle":pull.stitch_v85r_bundle(self.root,threading.Event())
                stop=pull.StitchStop(self.cfg,threading.Event(),self.task,pull.StitchRunKind.PROOF)
                clock=[stop.deadline-.01];reads=[];real_read=pull.os.read
                lock=threading.RLock();inventory=SimpleNamespace(activity_lock=lock)
                def read(fd,size):
                    reads.append(clock[0]);body=real_read(fd,size)
                    if expired:clock[0]=stop.deadline+.01
                    else:stop.stop.set()
                    return body
                real_bundle=pull.stitch_v85r_bundle
                with ExitStack() as context:
                    context.enter_context(mock.patch.object(pull.time,"monotonic",side_effect=lambda:clock[0]))
                    context.enter_context(mock.patch.object(pull.os,"read",side_effect=read))
                    context.enter_context(mock.patch.object(pull,"V85R_TOOLS",pins))
                    context.enter_context(mock.patch.object(pull,"stitch_proof_check"))
                    context.enter_context(mock.patch.object(stop,"refresh"))
                    context.enter_context(mock.patch.object(pull,"log"))
                    context.enter_context(mock.patch.object(pull,"_verified_binary",side_effect=AssertionError("uncancellable hash")))
                    directory=context.enter_context(mock.patch.object(pull,"StitchProofDirectory"))
                    bundle=context.enter_context(mock.patch.object(pull,"stitch_v85r_bundle",wraps=real_bundle))
                    if stage=="bundle":context.enter_context(mock.patch.object(pull,"stitch_v85r_provenance",return_value={}))
                    with self.assertRaises(pull.StitchHold):
                        with pull.stitch_proof_activity(inventory,stop):
                            pull.stitch_build(self.cfg,inventory,self.task,stop.stop,tools,
                                kind=pull.StitchRunKind.PROOF,proof_stop=stop)
                    directory.assert_not_called()
                    if stage=="provenance":bundle.assert_not_called()
                    else:
                        self.assertEqual(bundle.call_count,1)
                        self.assertIs(bundle.call_args.args[1],stop)
                self.assertEqual(len(reads),1)
                self.assertLess(reads[0],stop.deadline)
                self.assertFalse(lock._is_owned())
                self.assertEqual(stop.failure["stage"],stage)
                self.assertIsNone(stop.monitor)

    def test_actual_build_expiry_stops_provenance_and_bundle_before_more_reads(self):
        self.actual_build_cancellation(True)

    def test_actual_build_cleanup_cancellation_releases_activity_lock(self):
        self.actual_build_cancellation(False)

    def test_cold_bundle_cancellation_keeps_valid_member_and_resumes(self):
        stop=threading.Event();writes=[];real_write=pull.atomic_write
        def write(path,body,mode):
            real_write(path,body,mode=mode);writes.append(path)
            self.assertTrue((path.stat().st_mode & mode & 0o100)==(mode & 0o100))
            stop.set()
        with mock.patch.object(pull,"atomic_write",side_effect=write):
            with self.assertRaises(pull.StitchHold):pull.stitch_v85r_bundle(self.root,stop)
        self.assertEqual(len(writes),1)
        before=hashlib.sha256(writes[0].read_bytes()).hexdigest()
        installed=pull.stitch_v85r_bundle(self.root,threading.Event())
        self.assertEqual(hashlib.sha256(writes[0].read_bytes()).hexdigest(),before)
        self.assertTrue((installed/"manifest.json").is_file())

    def test_packet_and_frame_iteration_check_before_the_next_read(self):
        class Counting(io.StringIO):
            calls=0
            def readline(self,*args):self.calls+=1;return super().readline(*args)
        packet="pts=0|dts=0|duration=1|size=1|flags=K__|data_hash=SHA256:"+"a"*64+"\n"
        frame="0,0,0,1,1,"+"b"*64+"\n"
        for reader,line in ((pull.stitch_packet_rows,packet),(pull.stitch_frame_rows,frame)):
            source=Counting(line*3);stop=threading.Event()
            with mock.patch("builtins.open",return_value=source):
                rows=reader("synthetic",stop);next(rows);stop.set()
                with self.assertRaises(pull.StitchHold):next(rows)
            self.assertEqual(source.calls,1)

    def test_mp4_metadata_cancellation_stops_after_first_box_read(self):
        path=self.root/"synthetic.mp4";path.write_bytes(struct.pack(">I4s",8,b"free")*100)
        stop=threading.Event();real_open=open;reads=[]
        class Source:
            def __enter__(self):self.file=real_open(path,"rb");return self
            def __exit__(self,*args):self.file.close()
            def fileno(self):return self.file.fileno()
            def seek(self,*args):return self.file.seek(*args)
            def read(self,size):
                reads.append(size);body=self.file.read(size);stop.set();return body
        with mock.patch("builtins.open",return_value=Source()):
            with self.assertRaisesRegex(pull.StitchHold,"cancelled"):
                pull.stitch_editlist_metadata(path,stop)
        self.assertEqual(reads,[8])

    def test_bounded_audit_reads_do_not_continue_after_cancellation(self):
        stop=threading.Event();source=io.BytesIO(b"a"*(3*1024*1024));reads=[]
        real_read=source.read
        def read(size):
            reads.append(size);body=real_read(size);stop.set();return body
        with mock.patch.object(source,"read",side_effect=read),mock.patch.object(pull.Path,"open",return_value=source):
            with self.assertRaises(pull.StitchHold):pull.stitch_read_path(self.root/"synthetic",4*1024*1024,stop)
        self.assertEqual(reads,[1024*1024])

    def test_proof_frozen_lookup_is_readonly_equivalent_and_deadline_bounded(self):
        previous,clip=self.task["plan"]["clips"]
        witness={"synthetic":True}
        database=sqlite3.connect(self.cfg.inventory_file);self.addCleanup(database.close)
        database.execute("CREATE TABLE v85r_stitch_edges(a TEXT,b TEXT,witness TEXT,PRIMARY KEY(a,b))")
        database.execute("INSERT INTO v85r_stitch_edges VALUES(?,?,?)",
            (pull.stitch_source_key(previous),pull.stitch_source_key(clip),json.dumps(witness)))
        database.commit()
        inventory=SimpleNamespace(lock=threading.RLock(),db=database)
        self.assertEqual(pull.stitch_frozen(inventory,previous,clip,threading.Event()),witness)
        stop=pull.StitchStop(self.cfg,threading.Event(),self.task,pull.StitchRunKind.PROOF)
        waits=[];real_connect=sqlite3.connect
        def connect(filename,**kwargs):
            self.assertTrue(filename.endswith("?mode=ro"));self.assertTrue(kwargs["uri"])
            waits.append(kwargs["timeout"]);return real_connect(filename,**kwargs)
        with mock.patch.object(pull.time,"monotonic",return_value=stop.deadline-.01), \
                mock.patch.object(pull.sqlite3,"connect",side_effect=connect):
            self.assertEqual(pull.stitch_frozen(inventory,previous,clip,stop),witness)
        self.assertLessEqual(waits[0],.011)

    def test_proof_publication_hash_cancellation_precedes_rename(self):
        stop=pull.StitchStop(self.cfg,threading.Event(),self.task,pull.StitchRunKind.PROOF)
        path=self.root/"stage.mp4";path.write_bytes(b"x"*(3*1024*1024))
        self.cfg.output_dir=self.root
        directory=pull.StitchProofDirectory(self.cfg,"synthetic",1,stop)
        directory.fds=[-1]
        clock=[stop.deadline-.01];real_read=pull.os.read;reads=[]
        def read(fd,size):
            reads.append(clock[0]);body=real_read(fd,size);clock[0]=stop.deadline+.01;return body
        with mock.patch.object(type(directory),"path",new_callable=mock.PropertyMock,return_value=self.root), \
                mock.patch.object(pull.sys,"platform","linux"), \
                mock.patch.object(pull.os,"uname",return_value=SimpleNamespace(machine="x86_64")), \
                mock.patch.object(pull,"joined_entry_stat",return_value=SimpleNamespace(st_nlink=1)), \
                mock.patch.object(pull.time,"monotonic",side_effect=lambda:clock[0]), \
                mock.patch.object(pull.os,"read",side_effect=read), \
                mock.patch.object(ctypes,"CDLL") as rename:
            with self.assertRaises(pull.StitchHold):directory.publish("stage.mp4","proof.mp4")
        rename.assert_not_called();self.assertEqual(len(reads),1)
        self.assertTrue(path.exists())

    def test_native_spawn_rechecks_deadline_after_output_setup(self):
        deadline=time.monotonic()+1;clock=[deadline-.01];real_tempfile=tempfile.TemporaryFile
        def opened():
            file=real_tempfile();clock[0]=deadline+.01;return file
        with mock.patch.object(pull.time,"monotonic",side_effect=lambda:clock[0]), \
                mock.patch.object(pull.tempfile,"TemporaryFile",side_effect=opened), \
                mock.patch.object(pull.subprocess,"Popen") as spawn:
            with self.assertRaises(pull.BenchmarkDeadline):
                pull.run_measured(["synthetic"],deadline,threading.Event(),self.root/"audit",niced=False)
        spawn.assert_not_called()


if __name__=="__main__":unittest.main()
