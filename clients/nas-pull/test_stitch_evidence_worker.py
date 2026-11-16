"""SYNTHETIC lifecycle tests. No API, NAS, credentials or media authority."""
import base64
import copy
import datetime
import gzip
import hashlib
import json
import threading
import tempfile
from pathlib import Path
import time
from types import SimpleNamespace
import unittest
from unittest import mock

import test_stitch_readback as fixtures
import test_stoarama_pull as base

pull = base.pull
ID = fixtures.ID


def context_fixture():
    producer,reader,pair = copy.deepcopy(fixtures.profiles())
    producer.update(profile_id="nas-combined-v3-337c",client_version="r2-copy-v3",client_artifact=copy.deepcopy(reader["client_artifact"]))
    producer["contracts"]["attestation_schema"] = copy.deepcopy(reader["contracts"]["attestation_schema"])
    pair["pair_id"] = "nas-combined-v3-337c-self"
    documents = fixtures.seal_profiles((producer,reader,pair))
    profiles = {}
    for name,document in zip(("producer_profile","reader_profile","profile_pair"),documents):
        body = pull.cleanup_json_bytes(document)
        profiles[name+"_sha256"] = hashlib.sha256(body).hexdigest()
        profiles[name+"_b64"] = base64.b64encode(body).decode("ascii")
    task,receipt = fixtures.synthetic_native()
    task["expires_at"] = (datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=10)).isoformat().replace("+00:00","Z")
    task["plan"]["output_folder"] = "synthetic_V85r_proof_keep28_v3/444/2026-08-09T09-00-00Z/proof-"+pull.STITCH_PROOF_SOURCE_SHA[:16]+"/run-"+ID
    task["plan_sha256"] = hashlib.sha256(pull.cleanup_json_bytes(task["plan"])).hexdigest()
    receipt.update(client_version="r2-copy-v3",client_artifact_sha256="b"*64,plan_sha256=task["plan_sha256"])
    receipt["run"]["id"] = ID
    receipt["result"]["plan_sha256"] = task["plan_sha256"]
    receipt["result"]["outputs"][0]["relative_path"] = task["plan"]["output_folder"]+"/proof.mp4"
    context = dict(schema="stoarama.nas-stitch-admission.v1",mode=1,expires_at=task["expires_at"],profiles=profiles,task=task,native_run_id=ID)
    return context,receipt


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.context,self.receipt = context_fixture()
        self.store = fixtures.SyntheticMemoryEvidenceStore()
        self.ledger = pull.StitchEvidenceLedger(self.store)
        self.now = datetime.datetime.now(datetime.timezone.utc)
        self.event = threading.Event()
        self.cfg = SimpleNamespace(stitch_switches=pull.StitchSwitches(),stitch_proof_enabled=True,stitch_enabled=False,
            output_dir="/SYNTHETIC/no-media",is_candidate=False,
            inventory_scan_interval_sec=86400,inventory_hash_mbps=20,inventory_scan_delay_ms=0)
        self.cfg.stitch_switches.apply(dict(ok=True,stitch_proof_task_id=1))
        self.ledger.observe_mode(pull.NASStitchMode.NATIVE,self.now, ID)
        self.ledger.bind_admission(pull.cleanup_json_bytes(self.context))
        self.ledger.prepare_native(self.context)
        self.bundle = gzip.compress(pull.cleanup_json_bytes(self.receipt),mtime=0)
        self.execution = fixtures.readback()["reader_execution"]
        self.execution.update(client_artifact_sha256="b"*64,client_artifact_size_bytes=2)

    def register(self):
        self.ledger.register_native(self.bundle)

    def readback(self):
        result = fixtures.readback()
        result.update(native_run_id=ID,native_bundle_sha256=hashlib.sha256(self.bundle).hexdigest(),
            native_plan_sha256=self.context["task"]["plan_sha256"],
            producer_profile_sha256=self.context["profiles"]["producer_profile_sha256"],
            reader_profile_sha256=self.context["profiles"]["reader_profile_sha256"],
            profile_pair_sha256=self.context["profiles"]["profile_pair_sha256"],
            reader_execution=self.execution,outputs=[{k:self.receipt["result"]["outputs"][0][k] for k in ("relative_path","sha256","size_bytes")}],
            native_registration_sha256=hashlib.sha256(pull.cleanup_json_bytes(self.receipt)).hexdigest())
        return pull.cleanup_json_bytes(result)

    def test_completed_registration_binds_original_run_profile_and_task(self):
        self.register()
        self.assertEqual(self.ledger.state["native"]["stage"],"registered")
        self.assertEqual(self.store.read(self.ledger.state["native"]["bundle_sha256"]),self.bundle)
        self.assertEqual(self.receipt["run"]["started_at"],"2000-01-01T00:00:00Z")
        self.assertEqual(self.ledger.history,[])

    def test_original_producer_or_run_change_refuses_registration(self):
        for name in ("run","producer","plan"):
            receipt=copy.deepcopy(self.receipt)
            if name=="run":receipt["run"]["id"]="22222222-2222-4222-8222-222222222222"
            if name=="producer":receipt["client_artifact_sha256"]="0"*64
            if name=="plan":receipt["plan_sha256"]="0"*64
            with self.subTest(name=name),self.assertRaises(pull.StitchReadbackIntegrity):
                self.ledger.register_native(gzip.compress(pull.cleanup_json_bytes(receipt),mtime=0))
            self.assertEqual(self.ledger.state["native"]["stage"],"prepared")

    def test_failed_or_interrupted_native_attempt_is_not_automatically_reentered(self):
        for stage in ("prepared","failed"):
            if stage=="failed":self.ledger.fail_native()
            with mock.patch.object(pull,"stitch_build",side_effect=AssertionError("native reentered")),self.assertRaises(pull.StitchHold):
                pull.stitch_evidence_native(self.cfg,None,self.event,self.ledger,self.execution)
            self.assertEqual(self.ledger.state["native"]["stage"],stage)

    def test_native_setup_hard_deadline_cannot_be_extended_by_future_server_expiry(self):
        task=copy.deepcopy(self.context["task"])
        task["expires_at"]=(self.now+datetime.timedelta(days=1)).isoformat()
        stop=pull.StitchStop(self.cfg,self.event,task,pull.StitchRunKind.PROOF)
        self.assertLessEqual(stop.deadline-stop.started,15*60-30)
        self.assertEqual(stop.task,task)

    def test_restart_adopts_only_existing_complete_registration_without_media_work(self):
        with mock.patch.object(pull,"stitch_readback_file",return_value=((len(self.bundle),hashlib.sha256(self.bundle).hexdigest()),self.bundle)) as read,\
                mock.patch.object(pull,"stitch_build",side_effect=AssertionError("native")),\
                mock.patch.object(pull,"stitch_hash",side_effect=AssertionError("source media")):
            pull.stitch_evidence_reconcile(self.cfg,self.event,self.ledger)
        self.assertEqual(self.ledger.state["native"]["stage"],"registered")
        self.assertEqual(read.call_args.args[1],self.context["task"]["plan"]["output_folder"]+"/receipt.json.gz")
        self.assertEqual(self.ledger.history,[])

    def test_restart_missing_registration_retains_failed_run_and_never_repairs(self):
        try:
            raise FileNotFoundError("SYNTHETIC missing metadata")
        except FileNotFoundError as cause:
            error=pull.StitchReadbackIntegrity("unavailable");error.__cause__=cause
        with mock.patch.object(pull,"stitch_readback_file",side_effect=error),\
                mock.patch.object(pull,"stitch_build",side_effect=AssertionError("native")):
            pull.stitch_evidence_reconcile(self.cfg,self.event,self.ledger)
        self.assertEqual(self.ledger.state["native"]["stage"],"failed")
        self.assertEqual(self.ledger.state["native"]["run_id"],ID)

    def test_readback_mode_requires_preserved_native_and_never_falls_through(self):
        self.cfg.stitch_switches.apply(dict(ok=True,stitch_proof_task_id=2))
        self.ledger.observe_mode(pull.NASStitchMode.READBACK,self.now, ID)
        with mock.patch.object(pull,"stitch_build",side_effect=AssertionError("native fallback")),\
                mock.patch.object(pull,"stitch_corpus",side_effect=AssertionError("corpus download")),\
                mock.patch.object(pull,"stitch_v85r_tools",side_effect=AssertionError("tool install")),\
                mock.patch.object(pull,"stitch_evidence_admission",side_effect=AssertionError("missing native admission")),\
                self.assertRaises(pull.StitchHold):
            pull.stitch_evidence_readback(self.cfg,self.event,self.ledger,self.execution)

    def test_readback_queues_exact_original_bytes_and_preserves_native_times(self):
        self.register()
        stop=SimpleNamespace(deadline=time.monotonic()+90)
        with mock.patch.object(pull,"StitchReadbackStop",return_value=stop),\
                mock.patch.object(pull,"stitch_readback_run",return_value=self.readback()),\
                mock.patch.object(pull,"stitch_check"),mock.patch.object(pull,"STITCH_STATUS"):
            pull.stitch_evidence_readback(self.cfg,self.event,self.ledger,self.execution)
        self.assertEqual(len(self.ledger.history),1)
        self.assertEqual(len(self.ledger.state["queue"]),6)
        native=self.ledger.state["queue"][0]
        self.assertEqual(native["kind"],"proof-v1")
        self.assertEqual(self.store.read(native["blob_sha256"]),self.bundle)
        self.assertEqual(self.receipt["run"]["started_at"],"2000-01-01T00:00:00Z")
        preserved=dict(self.store.blobs)
        self.ledger.observe_mode(pull.NASStitchMode.DISABLED,self.now, ID)
        self.assertFalse(self.ledger.due());self.assertEqual(self.store.blobs,preserved)

    def test_off_still_drains_durable_metadata_without_native_or_output_read(self):
        key=pull.StitchEvidenceKey(pull.StitchEvidenceKind.READBACK,ID,"a"*64,"b"*64,ID)
        self.ledger.enqueue(key,b"SYNTHETIC")
        self.cfg.stitch_switches.apply(dict(ok=True,stitch_proof_task_id=0))
        def send(*args):
            self.event.set()
            return pull.StitchEvidenceProgress(pull.StitchEvidenceDispatch.ACKNOWLEDGED,1)
        with mock.patch.object(pull,"stitch_evidence_identity",return_value=self.execution),\
                mock.patch.object(pull,"StitchEvidenceDirectory") as directory,\
                mock.patch.object(pull,"stitch_evidence_reconcile"),\
                mock.patch.object(pull,"stitch_evidence_send",side_effect=send) as transport,\
                mock.patch.object(pull,"stitch_evidence_native",side_effect=AssertionError("native while OFF")),\
                mock.patch.object(pull,"stitch_evidence_readback",side_effect=AssertionError("rehash while OFF")),\
                mock.patch.object(pull,"stitch_readback_file",side_effect=AssertionError("media read while OFF")),\
                mock.patch.object(pull,"STITCH_STATUS"):
            directory.return_value.__enter__.return_value=self.store
            pull.stitch_evidence_loop(self.cfg,None,self.event)
        self.assertEqual(transport.call_count,1)
        restarted=pull.StitchEvidenceLedger(self.store)
        self.assertEqual(restarted.state["queue"],[])
        self.assertEqual(self.store.read(hashlib.sha256(b"SYNTHETIC").hexdigest()),b"SYNTHETIC")

    def test_existing_ordinary_entry_remains_separate_and_requires_its_own_switch(self):
        self.cfg.stitch_enabled=True
        self.cfg.stitch_switches.apply(dict(ok=True,stitch_enabled=True,stitch_proof_task_id=1))
        self.assertIs(self.cfg.stitch_switches.proof[0],pull.NASStitchMode.DISABLED)
        def ordinary(*args):self.event.set();return 60
        with mock.patch.object(pull,"stitch_evidence_identity",return_value=self.execution),\
                mock.patch.object(pull,"StitchEvidenceDirectory") as directory,\
                mock.patch.object(pull,"stitch_evidence_reconcile"),\
                mock.patch.object(pull,"stitch_evidence_native",side_effect=AssertionError("native proof")),\
                mock.patch.object(pull,"stitch_evidence_readback",side_effect=AssertionError("readback")),\
                mock.patch.object(pull,"stitch_once",side_effect=ordinary) as call,\
                mock.patch.object(pull,"STITCH_STATUS"):
            directory.return_value.__enter__.return_value=self.store
            pull.stitch_evidence_loop(self.cfg,None,self.event)
        call.assert_called_once_with(self.cfg,None,self.event)

    def test_native_status_is_bounded_and_does_not_redecode_bundle_each_tick(self):
        self.register()
        self.ledger.bind_session(dict(native_run_id=ID,producer_profile_sha256=self.context["profiles"]["producer_profile_sha256"],
            reader_profile_sha256=self.context["profiles"]["reader_profile_sha256"],profile_pair_sha256=self.context["profiles"]["profile_pair_sha256"],reader_boot_id=ID))
        first=pull.stitch_evidence_status(self.ledger,self.execution)
        with mock.patch.object(pull,"stitch_proof_unpack",side_effect=AssertionError("repeat decompression")):
            second=pull.stitch_evidence_status(self.ledger,self.execution)
        self.assertEqual(first,second);self.assertLess(len(pull.cleanup_json_bytes(second)),6000)
        first["native"]["outputs"][0]["sha256"]="0"*64
        self.assertNotEqual(first,pull.stitch_evidence_status(self.ledger,self.execution))

    def test_off_heartbeat_keeps_complete_reader_status_separate_from_release_and_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            cfg=base.NASPullTests().config(Path(directory))
            runtime=pull.Runtime(cfg)
            self.assertFalse(runtime.stitch_enabled)
            status=dict(stitch_reader=dict(SYNTHETIC="complete metadata"),state="readback_verified",outputs_verified=10**20)
            with mock.patch.object(pull,"CLIENT_VERSION","r2-copy-v3"),\
                    mock.patch.object(pull,"STITCH_STATUS") as source:
                source.snapshot.return_value=(status,"",None)
                payload=runtime.heartbeat_payload(None)
            self.assertEqual(payload["stitch"]["stitch_reader"],status["stitch_reader"])
            self.assertNotIn("outputs_verified",payload["stitch"])
            self.assertFalse(cfg.stitch_enabled)
            self.assertFalse(cfg.stitch_proof_enabled)
            self.assertLess(len(pull.cleanup_json_bytes(payload["stitch"])),8192)


if __name__=="__main__":unittest.main()
