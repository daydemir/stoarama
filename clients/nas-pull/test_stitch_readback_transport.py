"""Synthetic metadata transport, real isolated children, no network or media."""
import json
import hashlib
import os
from pathlib import Path
import platform
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock
import urllib.request

import test_stitch_proof_http as transport


pull = transport.pull
ID = "11111111-1111-4111-8111-111111111111"


class EvidenceTransportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.cfg = SimpleNamespace(state_dir=Path(temporary.name),api_base="https://synthetic.invalid",
                                   api_key="SYNTHETIC_TOKEN")
        self.clock = [100.0]
        patch = mock.patch.object(pull.time,"monotonic",side_effect=lambda:self.clock[0])
        patch.start();self.addCleanup(patch.stop)
        backing = mock.patch.object(pull,"inventory_scratch_backing",return_value=(0,pull.InventoryScratchFilesystem.EXT4))
        backing.start();self.addCleanup(backing.stop)
        self.pacer = pull.StitchEvidencePacer()
        self.key = pull.StitchEvidenceKey(pull.StitchEvidenceKind.READBACK,ID,"a"*64,"b"*64,ID)
        self.body = b"SYNTHETIC"*6000

    def child(self, ack):
        # Only replace the network boundary; fixed child/framing/reap code runs.
        return ("MODE='success'\n"+transport.FAKE_NETWORK+
                "Response.body="+repr(json.dumps(ack).encode())+"\n"+pull.STITCH_PROOF_HTTP_CHILD)

    def send(self, cursor=0, key=None):
        return pull.stitch_evidence_send(self.cfg,self.pacer,key or self.key,self.body,cursor,ID,threading.Event())

    def test_shared_quiet_period_and_real_child_exact_ack_advance_only_the_supplied_cursor(self):
        with mock.patch.object(pull.subprocess,"Popen") as spawn:
            result=self.send();self.assertIs(result.state,pull.StitchEvidenceDispatch.QUIET);spawn.assert_not_called()
        self.clock[0]=160
        with mock.patch.object(pull,"STITCH_PROOF_HTTP_CHILD",self.child(dict(ok=True,stored_lines=64))):
            first=self.send()
        self.assertEqual(first,pull.StitchEvidenceProgress(pull.StitchEvidenceDispatch.ACKNOWLEDGED,64))
        self.assertEqual(list(self.cfg.state_dir.iterdir()),[])
        # Another stream shares this reservation; OFF does not authorize new reads.
        other=self.key._replace(kind=pull.StitchEvidenceKind.NATIVE)
        self.assertIs(self.send(key=other).state,pull.StitchEvidenceDispatch.QUIET)
        self.clock[0]=220
        remaining=(len(self.body)+511)//512-64
        with mock.patch.object(pull,"STITCH_PROOF_HTTP_CHILD",self.child(dict(ok=True,stored_lines=remaining))):
            last=self.send(cursor=64)
        self.assertEqual(last.cursor,(len(self.body)+511)//512)
        self.assertIs(self.send(cursor=last.cursor).state,pull.StitchEvidenceDispatch.COMPLETE)

    def test_invalid_lost_or_boolean_ack_never_advances_and_still_consumes_the_minute(self):
        for ack in (dict(ok=True,stored_lines=63),dict(ok=True,stored_lines=True),
                    dict(ok=True,stored_lines=64,unknown=True),dict(ok=False,stored_lines=64)):
            self.clock[0]=self.pacer.allowed_at
            with self.subTest(ack=ack),mock.patch.object(pull,"STITCH_PROOF_HTTP_CHILD",self.child(ack)):
                with self.assertRaisesRegex(pull.StitchHold,"not acknowledged"):self.send()
            self.assertIs(self.send().state,pull.StitchEvidenceDispatch.QUIET)
        self.clock[0]=self.pacer.allowed_at
        with mock.patch.object(pull,"stitch_bounded_http",side_effect=OSError("SYNTHETIC lost ACK")):
            with self.assertRaises(OSError):self.send()
        self.assertIs(self.send().state,pull.StitchEvidenceDispatch.QUIET)

    def test_redaction_change_refuses_before_spawn_or_cursor_advance(self):
        self.clock[0]=160
        with mock.patch.object(pull,"redact_log_line",return_value="changed"),mock.patch.object(pull.subprocess,"Popen") as spawn:
            with self.assertRaisesRegex(pull.StitchHold,"redaction changed"):self.send()
            spawn.assert_not_called()

    def test_post_is_exact_existing_metadata_path_and_native_get_wrapper_stays_get_only(self):
        stop=pull.StitchEvidenceStop(threading.Event())
        requests = (
            urllib.request.Request(self.cfg.api_base+"/other",method="POST",data=b"{}"),
            urllib.request.Request("https://elsewhere.invalid/account/connections/client-logs",method="POST",data=b"{}"),
            urllib.request.Request(self.cfg.api_base+"/account/connections/client-logs",method="POST",data=b"x"*(128*1024+1)),
            urllib.request.Request(self.cfg.api_base+"/account/connections/client-logs",method="POST"),
            urllib.request.Request(self.cfg.api_base+"/other",method="GET",data=b"{}"),
        )
        with mock.patch.object(pull.subprocess,"Popen") as spawn:
            for request in requests:
                with self.subTest(url=request.full_url),self.assertRaises(pull.StitchHold):
                    with pull.stitch_bounded_http(self.cfg,request,stop,4096,20):pass
            valid=urllib.request.Request(self.cfg.api_base+"/account/connections/client-logs",method="POST",data=b"{}")
            with self.assertRaises(pull.StitchHold):
                with pull.stitch_proof_http(self.cfg,valid,stop,4096,20):pass
            spawn.assert_not_called()

    def test_expired_or_cancelled_metadata_budget_refuses_before_spawning(self):
        request=urllib.request.Request(self.cfg.api_base+"/account/connections/client-logs",method="POST",data=b"{}")
        stop=pull.StitchEvidenceStop(threading.Event())
        self.clock[0]=stop.deadline-pull.STITCH_PROOF_HTTP_REAP_SEC+.001
        with mock.patch.object(pull.subprocess,"Popen") as spawn:
            with self.assertRaises((pull.StitchHold,pull.BenchmarkDeadline)):
                with pull.stitch_bounded_http(self.cfg,request,stop,4096,20):pass
            spawn.assert_not_called()


class EvidenceStorageTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        self.root=Path(temporary.name).resolve()
        self.cfg=SimpleNamespace(state_dir=self.root,output_dir=self.root/"UNUSED-NO-MEDIA")
        self.stop=threading.Event()
        backing=mock.patch.object(pull,"inventory_scratch_backing",return_value=(0,pull.InventoryScratchFilesystem.EXT4))
        backing.start();self.addCleanup(backing.stop)

    def test_locked_atomic_checkpoint_cas_survives_reopen_and_unknown_state_never_defaults(self):
        first=pull.cleanup_json_bytes(dict(schema="SYNTHETIC",cursor=0,integrity="clear"))
        second=pull.cleanup_json_bytes(dict(schema="SYNTHETIC",cursor=64,integrity="refused"))
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as store:
            absent=store.snapshot();self.assertFalse(absent.present)
            initial=store.replace(absent,first);self.assertEqual(initial.body,first)
            terminal=store.replace(initial,second);self.assertEqual(terminal.body,second)
            with self.assertRaises(pull.StitchReadbackIntegrity):store.replace(initial,first)
            with self.assertRaises(pull.StitchReadbackIntegrity):store.replace(terminal,b'{ "cursor" : 0 }')
            with self.assertRaises(pull.StitchHold):store.replace(terminal,b'{"cursor":0,"cursor":1}')
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as store:
            self.assertEqual(store.snapshot(),terminal)
            self.assertEqual(set(os.listdir(store.fds[-1])),{".lock","state.json"})
        self.assertFalse(self.cfg.output_dir.exists())

    def test_second_writer_refuses_and_lock_inode_is_never_replaced(self):
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as first:
            identity=os.fstat(first.lock_fd)
            with self.assertRaisesRegex(pull.StitchHold,"another evidence writer"):
                with pull.StitchEvidenceDirectory(self.cfg,self.stop):pass
            self.assertEqual(os.stat(".lock",dir_fd=first.fds[-1]).st_ino,identity.st_ino)
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as next_store:
            self.assertEqual(os.fstat(next_store.lock_fd).st_ino,identity.st_ino)

    def test_missing_checkpoint_beside_preserved_evidence_never_resets_integrity_history(self):
        body=b"SYNTHETIC preserved bytes";digest=hashlib.sha256(body).hexdigest()
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as store:
            store.replace(store.snapshot(),b'{"integrity":"refused"}')
            blob=self.root/store.relative/(digest+".blob");blob.write_bytes(body);blob.chmod(0o600)
            # Fault injection affects only this test's disposable metadata.
            os.unlink("state.json",dir_fd=store.fds[-1])
            with self.assertRaisesRegex(pull.StitchReadbackIntegrity,"checkpoint missing"):store.snapshot()
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as store:
            with self.assertRaisesRegex(pull.StitchReadbackIntegrity,"checkpoint missing"):store.snapshot()

    def test_cancelled_or_failed_stage_is_removed_without_changing_checkpoint(self):
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as store:
            initial=store.replace(store.snapshot(),b'{"synthetic":true}')
            with mock.patch.object(pull.os,"fsync",side_effect=OSError("SYNTHETIC fsync failure")):
                with self.assertRaises(OSError):store.replace(initial,b'{"synthetic":false}')
            self.assertEqual(store.snapshot(),initial)
            self.assertEqual(set(os.listdir(store.fds[-1])),{".lock","state.json"})
            with self.assertRaises(pull.StitchHold):
                with store.stage(b"SYNTHETIC"):
                    self.stop.set();store.check()
            self.stop.clear();self.assertEqual(store.snapshot(),initial)
            self.assertEqual(set(os.listdir(store.fds[-1])),{".lock","state.json"})

    def test_existing_content_addressed_blob_is_rehashed_and_never_overwritten(self):
        body=b"SYNTHETIC preserved metadata\n";digest=hashlib.sha256(body).hexdigest()
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as store:
            path=self.root/store.relative/(digest+".blob");path.write_bytes(body);path.chmod(0o600)
            identity=path.stat().st_ino
            self.assertEqual(store.put(body),digest);self.assertEqual(store.read(digest),body)
            self.assertEqual(path.stat().st_ino,identity)
            path.write_bytes(b"changed")
            with self.assertRaises(pull.StitchReadbackIntegrity):store.read(digest)
            with self.assertRaises(pull.StitchReadbackIntegrity):store.put(body)
            self.assertEqual(path.read_bytes(),b"changed")

    def test_storage_cap_refuses_without_pruning_and_symlinks_unknown_files_refuse(self):
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as store:
            for number in range(64):
                fd=os.open("%064x.blob"%number,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600,dir_fd=store.fds[-1])
                try:os.ftruncate(fd,1024*1024)
                finally:os.close(fd)
            store.capacity(0)
            with self.assertRaisesRegex(pull.StitchHold,"storage cap"):store.capacity(1)
            self.assertEqual(len(os.listdir(store.fds[-1])),65)
        with tempfile.TemporaryDirectory() as other:
            cfg=SimpleNamespace(state_dir=Path(other).resolve(),output_dir=self.cfg.output_dir)
            with pull.StitchEvidenceDirectory(cfg,self.stop) as store:
                (cfg.state_dir/store.relative/"unknown").write_bytes(b"SYNTHETIC")
                with self.assertRaises(pull.StitchReadbackIntegrity):store.capacity(0)

    @unittest.skipUnless(sys.platform.startswith("linux") and platform.machine()=="x86_64","requires real NAS renameat2 target")
    def test_real_linux_create_only_publication_and_collision(self):
        with pull.StitchEvidenceDirectory(self.cfg,self.stop) as store:
            body=b"SYNTHETIC create-only metadata";digest=store.put(body)
            self.assertEqual(store.read(digest),body)
            original=os.stat(digest+".blob",dir_fd=store.fds[-1])
            self.assertEqual(store.put(body),digest)
            self.assertEqual(os.stat(digest+".blob",dir_fd=store.fds[-1]).st_ino,original.st_ino)
            self.assertEqual(set(os.listdir(store.fds[-1])),{".lock",digest+".blob"})


if __name__ == "__main__":
    unittest.main()
