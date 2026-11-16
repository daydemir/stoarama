"""Closed SYNTHETIC lifecycle matrix; no API, credentials, media or NAS I/O."""
import base64
import copy
import datetime
from enum import Enum
import gzip
import hashlib
import itertools
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

import test_stitch_evidence_worker as worker

pull=worker.pull
ACT_A="11111111-1111-4111-8111-111111111111"
ACT_B="22222222-2222-4222-8222-222222222222"
RUN_A="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
RUN_B="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
BOOT="cccccccc-cccc-4ccc-8ccc-cccccccccccc"
MATRIX=json.loads((Path(__file__).parent/"stitch-proof/activation-state-matrix.json").read_bytes())


class Outcome(Enum):
    HOLD="hold"
    NATIVE="native-once"
    ORIGINAL="original-readback"
    RENEW="renew-readback"


class Boundary(RuntimeError):
    """Stop at the real admission/rehash boundary before any I/O."""


class ActivationMatrixTests(unittest.TestCase):
    def test_matrix_covers_all_72_requested_combinations_with_240_explicit_refinements(self):
        self.assertEqual(set(MATRIX),{"schema","synthetic","rows"})
        self.assertIs(MATRIX["synthetic"],True)
        self.assertEqual(MATRIX["schema"],"stoarama.synthetic-activation-state-matrix.v1")
        rows=MATRIX["rows"]
        self.assertEqual(len(rows),240)
        self.assertEqual(len({row["id"] for row in rows}),240)
        full=set();coarse=set()
        for row in rows:
            self.assertEqual(set(row),{"id","native","activation","window","mode","selector","outcome"})
            Outcome(row["outcome"])
            full.add(tuple(row[key] for key in ("native","activation","window","mode","selector")))
            coarse.add(("A" if row["native"]=="registered-A" else "none",row["activation"],
                "reopened" if row["window"].startswith("reopened") else row["window"],row["mode"],row["selector"]))
        self.assertEqual(full,set(itertools.product(
            ("registered-A","none-unentered","none-entered","none-prepared","none-failed"),
            ("A","B","missing"),("open","expired","reopened-open","reopened-expired"),(1,2),("match","mismatch"))))
        self.assertEqual(coarse,set(itertools.product(("A","none"),("A","B","missing"),
            ("open","expired","reopened"),(1,2),("match","mismatch"))))

    def test_each_original_binding_and_midcall_nonce_change_refuses_before_reads(self):
        mutations={
            "entry":lambda ledger,cfg:ledger.state["window"].update(entered=False),
            "window-run":lambda ledger,cfg:ledger.state["window"].update(run_id=RUN_B),
            "missing-admission":lambda ledger,cfg:ledger.state["window"].pop("admission_sha256"),
            "admission-sha":lambda ledger,cfg:ledger.state["window"].update(admission_sha256="f"*64),
            "window-activation":lambda ledger,cfg:ledger.state["window"].update(activation_id=ACT_B),
            "registered-context":lambda ledger,cfg:ledger.state["native"].update(context_sha256="f"*64),
            "registered-run":lambda ledger,cfg:ledger.state["native"].update(run_id=RUN_B),
            "midcall-nonce":lambda ledger,cfg:cfg.stitch_switches.apply(dict(ok=True,
                stitch_proof_task_id=1,stitch_proof_activation_id=ACT_B)),
        }
        for name,mutate in mutations.items():
            with self.subTest(binding=name):
                row=dict(MATRIX["rows"][0],outcome=Outcome.HOLD.value)
                self.exercise(row,mutate)

    def exercise(self,row,mutate=None):
        expected=Outcome(row["outcome"])
        origin=2**60+12345;clock=[origin]
        real_datetime=datetime.datetime
        now=[real_datetime(2026,11,16,tzinfo=datetime.timezone.utc)]
        class FrozenDatetime(real_datetime):
            @classmethod
            def now(cls,tz=None):return now[0]
        cfg=SimpleNamespace(stitch_switches=pull.StitchSwitches(),stitch_proof_enabled=True,
            stitch_enabled=False,output_dir="/SYNTHETIC/no-media")
        event=threading.Event();store=worker.fixtures.SyntheticMemoryEvidenceStore()
        execution=copy.deepcopy(worker.fixtures.readback()["reader_execution"])
        execution.update(client_boot_id=BOOT,client_artifact_sha256="b"*64,client_artifact_size_bytes=2)
        with mock.patch.object(pull.datetime,"datetime",FrozenDatetime),\
                mock.patch.object(pull.time,"monotonic_ns",side_effect=lambda:clock[0]),\
                mock.patch.object(pull,"stitch_resources"),mock.patch.object(pull,"STITCH_STATUS"),\
                mock.patch.object(pull,"stitch_build",side_effect=AssertionError("native media I/O")):
            context,receipt=worker.context_fixture()
            context.update(native_run_id=RUN_A,activation_id=ACT_A)
            task=context["task"]
            task["plan"]["output_folder"]=task["plan"]["output_folder"].rsplit("/run-",1)[0]+"/run-"+RUN_A
            task["plan_sha256"]=hashlib.sha256(pull.cleanup_json_bytes(task["plan"])).hexdigest()
            task["activation_id"]=ACT_A
            context["expires_at"]=task["expires_at"]=(now[0]+datetime.timedelta(seconds=40)).isoformat().replace("+00:00","Z")
            raw_task=pull.cleanup_json_bytes(task)
            context.update(task_b64=base64.b64encode(raw_task).decode("ascii"),task_sha256=hashlib.sha256(raw_task).hexdigest())
            raw_context=pull.cleanup_json_bytes(context)
            receipt["run"]["id"]=RUN_A;receipt["plan_sha256"]=task["plan_sha256"]
            receipt["result"]["plan_sha256"]=task["plan_sha256"]
            receipt["result"]["outputs"][0]["relative_path"]=task["plan"]["output_folder"]+"/proof.mp4"
            bundle=gzip.compress(pull.cleanup_json_bytes(receipt),mtime=0)
            ledger=pull.StitchEvidenceLedger(store)
            with mock.patch.object(pull.uuid,"uuid4",return_value=pull.uuid.UUID(RUN_A)):
                ledger.observe_mode(pull.NASStitchMode.NATIVE,now[0],BOOT,ACT_A)
            if row["native"]!="none-unentered":ledger.enter_native()
            if row["native"]!="none-unentered":
                original=copy.deepcopy(ledger.state);blobs=dict(store.blobs)
                for field,value in (("activation_id",ACT_B),("native_run_id",RUN_B)):
                    changed=copy.deepcopy(context);changed[field]=value
                    if field=="activation_id":
                        changed["task"]["activation_id"]=value
                        changed_task=pull.cleanup_json_bytes(changed["task"])
                        changed.update(task_b64=base64.b64encode(changed_task).decode("ascii"),
                            task_sha256=hashlib.sha256(changed_task).hexdigest())
                    with self.assertRaises(pull.StitchReadbackIntegrity):ledger.bind_admission(pull.cleanup_json_bytes(changed))
                    self.assertEqual(ledger.state,original);self.assertEqual(store.blobs,blobs)
            if row["native"] in ("registered-A","none-prepared","none-failed"):
                ledger.bind_admission(raw_context);ledger.prepare_native(context)
                original_window=copy.deepcopy(ledger.state["window"])
                self.assertEqual(ledger.bind_admission(raw_context),raw_context)
                changed=copy.deepcopy(context);changed["task"]["expires_at"]="2099-01-01T00:00:00Z"
                changed_task=pull.cleanup_json_bytes(changed["task"])
                changed.update(task_b64=base64.b64encode(changed_task).decode("ascii"),task_sha256=hashlib.sha256(changed_task).hexdigest())
                with self.assertRaises(pull.StitchReadbackIntegrity):ledger.bind_admission(pull.cleanup_json_bytes(changed))
                self.assertEqual(ledger.state["window"],original_window)
            if row["native"]=="none-failed":ledger.fail_native()
            if row["native"]=="registered-A":ledger.register_native(bundle)
            if row["window"] in ("expired","reopened-expired"):
                seconds=871 if row["native"] in ("none-unentered","none-entered") else 11
                clock[0]+=seconds*10**9;now[0]+=datetime.timedelta(seconds=seconds)
                with self.assertRaises(pull.StitchHold):ledger.native_deadline_ns(now[0])
            mode=pull.NASStitchMode(row["mode"])
            activation={"A":ACT_A,"B":ACT_B,"missing":""}[row["activation"]]
            if mode is pull.NASStitchMode.NATIVE and not activation:
                before=copy.deepcopy(ledger.state)
                with self.assertRaises(pull.StitchHold):ledger.observe_mode(mode,now[0],BOOT,activation)
                self.assertEqual(ledger.state,before);self.assertIs(expected,Outcome.HOLD)
                return
            with mock.patch.object(pull.uuid,"uuid4",return_value=pull.uuid.UUID(RUN_B)):
                ledger.observe_mode(mode,now[0],BOOT,activation)
            if mode is pull.NASStitchMode.NATIVE and activation==ACT_B:
                original=copy.deepcopy(ledger.state)
                with self.assertRaises(pull.StitchHold):ledger.observe_mode(mode,now[0],BOOT,ACT_A)
                self.assertEqual(ledger.state,original)
            if row["window"].startswith("reopened"):
                before=copy.deepcopy(ledger.state)
                ledger.observe_mode(pull.NASStitchMode.DISABLED,now[0],BOOT)
                ledger=pull.StitchEvidenceLedger(store)
                ledger.observe_mode(mode,now[0],BOOT,activation)
                self.assertEqual(ledger.state,before)
            actual=mode if row["selector"]=="match" else (pull.NASStitchMode.READBACK if mode is pull.NASStitchMode.NATIVE else pull.NASStitchMode.NATIVE)
            try:cfg.stitch_switches.apply(dict(ok=True,stitch_proof_task_id=int(actual),stitch_proof_activation_id=activation))
            except ValueError:self.assertEqual((actual,activation),(pull.NASStitchMode.NATIVE,""))
            if mutate is not None:mutate(ledger,cfg)
            if expected is Outcome.HOLD:
                with mock.patch.object(store,"read",side_effect=AssertionError("metadata read before refusal")),\
                        mock.patch.object(pull,"stitch_evidence_admission",side_effect=AssertionError("admission before refusal")),\
                        mock.patch.object(pull,"stitch_readback_run",side_effect=AssertionError("media read before refusal")),\
                        self.assertRaises(pull.StitchHold):
                    if mode is pull.NASStitchMode.NATIVE:pull.stitch_evidence_native(cfg,None,event,ledger,execution)
                    pull.stitch_evidence_readback(cfg,event,ledger,execution)
                return
            if expected is Outcome.NATIVE:
                def admission(*args):
                    self.assertTrue(ledger.state["window"]["entered"])
                    self.assertEqual(ledger.state["window"]["activation_id"],activation)
                    self.assertEqual(args[3],ledger.state["window"]["run_id"])
                    raise Boundary("first native admission only")
                with mock.patch.object(pull,"stitch_evidence_admission",side_effect=admission) as request,self.assertRaises(Boundary):
                    pull.stitch_evidence_native(cfg,None,event,ledger,execution)
                self.assertEqual(request.call_count,1)
                ledger=pull.StitchEvidenceLedger(store);ledger.observe_mode(mode,now[0],BOOT,activation)
                with mock.patch.object(pull,"stitch_evidence_admission",side_effect=AssertionError("re-admission")),self.assertRaises(pull.StitchHold):
                    pull.stitch_evidence_native(cfg,None,event,ledger,execution)
                return
            def rehash(_cfg,stop,_profiles,_key,task_bytes,bundle_bytes):
                self.assertEqual(task_bytes,raw_task);self.assertEqual(bundle_bytes,bundle)
                self.assertEqual(stop.deadline_ns-clock[0],(10 if expected is Outcome.ORIGINAL else 90)*10**9)
                self.assertEqual(json.loads(task_bytes)["activation_id"],ACT_A)
                raise Boundary("original bytes at bounded rehash")
            fresh=dict(schema="stoarama.nas-stitch-admission.v1",mode=2,profiles=context["profiles"],
                expires_at=(now[0]+datetime.timedelta(seconds=120)).isoformat())
            with mock.patch.object(pull,"stitch_evidence_admission",return_value=fresh) as request,\
                    mock.patch.object(pull,"stitch_readback_run",side_effect=rehash),self.assertRaises(Boundary):
                if mode is pull.NASStitchMode.NATIVE:pull.stitch_evidence_native(cfg,None,event,ledger,execution)
                pull.stitch_evidence_readback(cfg,event,ledger,execution)
            self.assertEqual(request.call_count,int(expected is Outcome.RENEW))


def matrix_test(row):
    def test(self):
        self.exercise(row)
        if row["selector"]=="mismatch":
            with self.subTest(actual_selector=0):
                self.exercise(row,lambda ledger,cfg:cfg.stitch_switches.apply(dict(ok=True,stitch_proof_task_id=0)))
    test.__doc__="SYNTHETIC "+json.dumps(row,sort_keys=True)
    return test


for row in MATRIX["rows"]:
    setattr(ActivationMatrixTests,"test_"+row["id"],matrix_test(row))


if __name__=="__main__":unittest.main()
