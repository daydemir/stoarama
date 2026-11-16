"""Explicitly SYNTHETIC offline assembly. Never an accepted NAS artifact."""
import copy
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import test_stitch_evidence_worker as worker
import test_stitch_readback as fixture

ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location("offline_export",ROOT/"export_stitch_readback.py")
export=importlib.util.module_from_spec(spec);spec.loader.exec_module(export)
pull=worker.pull


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.context,self.receipt=worker.context_fixture()
        # Inert byte fixture only. No process imports/runs this stamped NAS app.
        self.source=(ROOT/"stoarama_pull.py").read_bytes().replace(b'CLIENT_VERSION = "development"',b'CLIENT_VERSION = "r2-copy-v3"',1)
        source_sha=export.sha(self.source)
        p,r,k=copy.deepcopy(fixture.profiles())
        p.update(profile_id="nas-combined-v3-337c",client_version="r2-copy-v3",client_artifact=copy.deepcopy(r["client_artifact"]))
        for profile in (p,r):profile["client_artifact"].update(file="artifacts/SYNTHETIC-client.py",sha256=source_sha,size_bytes=len(self.source))
        p["contracts"]["attestation_schema"]=copy.deepcopy(r["contracts"]["attestation_schema"])
        validator=(ROOT/"verify_stitch_readback.py").read_bytes()
        r["contracts"]["readback_validator"]=dict(file="verifiers/readback.py",sha256=export.sha(validator),size_bytes=len(validator))
        k.update(pair_id="nas-combined-v3-337c-self",readback_validator=copy.deepcopy(r["contracts"]["readback_validator"]))
        p,r,k=fixture.seal_profiles((p,r,k))
        self.components={p["client_artifact"]["file"]:self.source,
            p["package_manifest"]["file"]:(ROOT/"v85r/manifest.json").read_bytes(),
            p["contracts"]["native_receipt_schema"]["file"]:(ROOT/"stitch-proof/native-receipt.schema.json").read_bytes(),
            p["contracts"]["native_verifier"]["file"]:(ROOT/"verify_stitch_proof.py").read_bytes(),
            p["contracts"]["attestation_schema"]["file"]:(ROOT/"stitch-proof/attestation-v2.schema.json").read_bytes(),
            r["contracts"]["readback_validator"]["file"]:validator}
        profiles=[pull.cleanup_json_bytes(doc) for doc in (p,r,k)]
        self.receipt["client_artifact_sha256"]=source_sha
        self.bundle=gzip.compress(pull.cleanup_json_bytes(self.receipt),mtime=0)
        task=pull.cleanup_json_bytes(self.context["task"])
        self.task_sha=export.sha(task)
        self.readback=fixture.readback()
        self.readback.update(native_task_sha256=self.task_sha,native_activation_id=self.context["activation_id"],native_run_id=self.receipt["run"]["id"],native_plan_sha256=self.context["task"]["plan_sha256"],
            native_bundle_sha256=export.sha(self.bundle),producer_profile_sha256=export.sha(profiles[0]),
            reader_profile_sha256=export.sha(profiles[1]),profile_pair_sha256=export.sha(profiles[2]),
            native_registration_sha256=export.sha(pull.cleanup_json_bytes(self.receipt)),
            outputs=[{name:self.receipt["result"]["outputs"][0][name] for name in ("relative_path","sha256","size_bytes")}])
        self.readback["reader_execution"].update(client_artifact_sha256=source_sha,client_artifact_size_bytes=len(self.source))
        self.raw=pull.cleanup_json_bytes(self.readback);self.readback_sha=export.sha(self.raw)
        self.key=(self.readback["native_plan_sha256"],self.readback["native_run_id"],self.readback["profile_pair_sha256"],self.readback["id"])
        key=pull.StitchEvidenceKey(pull.StitchEvidenceKind.READBACK,self.key[1],self.key[0],self.key[2],self.key[3])
        lines=[]
        for kind,body in zip(pull.StitchEvidenceKind,(self.bundle,task,*profiles,self.raw)):
            cursor=0
            while cursor<(len(body)+511)//512:
                rows,cursor=pull.stitch_evidence_chunks(key._replace(kind=kind),body,cursor)
                lines.extend(dict(row,client_version="r2-copy-v3",boot_id=fixture.ID) for row in rows)
        self.pages=[dict(connection_id=13,lines=lines)]

    def assemble(self,pages=None):
        return export.assemble(self.pages if pages is None else pages,self.key,self.readback_sha,self.task_sha,fixture.ID,export.sha(self.source),export.sha(self.source),
            lambda ref:self.components[ref["file"]])

    def test_complete_offline_replay_exports_exact_registration_and_full_result_request(self):
        with mock.patch.object(socket,"socket",side_effect=AssertionError("network")),mock.patch.object(subprocess,"Popen",side_effect=AssertionError("child")):
            parts=self.assemble()
        self.assertEqual(parts["local-registration.json"],pull.cleanup_json_bytes(self.receipt))
        self.assertEqual(parts["native-bundle.json.gz"],self.bundle)
        native=json.loads(parts["native-replay.json"]);readback=json.loads(parts["readback-replay.json"])
        self.assertEqual(native["native_replay"],"passed");self.assertEqual(readback["readback_binding"],"passed")
        self.assertEqual(native["run_id"],self.receipt["run"]["id"])
        request=json.loads(parts["server-validation-request.json"])
        import base64
        result=base64.b64decode(request["native_result_b64"],validate=True)
        self.assertEqual(result,pull.cleanup_json_bytes(self.receipt["result"]))
        self.assertIn("media_seconds",json.loads(result)["outputs"][0])
        self.assertNotIn("validated_at",request)
        self.assertNotIn("deployment_id",request)

    def test_identical_duplicate_chunks_and_order_changes_preserve_exact_bytes(self):
        pages=copy.deepcopy(self.pages);pages[0]["lines"].reverse()
        pages[0]["lines"]+=copy.deepcopy(pages[0]["lines"])
        self.assertEqual(self.assemble(pages),self.assemble())

    def test_previous_valid_readback_contexts_do_not_conflict_with_selected_component_bytes(self):
        pages=copy.deepcopy(self.pages)
        predecessor=[]
        previous="22222222-2222-4222-8222-222222222222"
        for row in pages[0]["lines"]:
            if row["message"].startswith("V85r proof-v1 "):continue
            fields=row["message"].split(" ");fields[6]=previous
            predecessor.append(dict(row,message=" ".join(fields)))
        pages[0]["lines"]=predecessor+pages[0]["lines"]+predecessor
        self.assertEqual(self.assemble(pages),self.assemble())

    def test_selected_identity_redaction_missing_chunk_and_census_conflicts_refuse(self):
        for mutate in (lambda rows:rows.pop(),lambda rows:rows[-1].update(boot_id="wrong"),
                lambda rows:rows[-1].update(client_version="development"),
                lambda rows:rows[-1].update(message=rows[-1]["message"]+"0"),
                lambda rows:rows.append(dict(rows[-1],message=rows[-1]["message"][:-2]+"ff"))):
            pages=copy.deepcopy(self.pages);mutate(pages[0]["lines"])
            with self.assertRaises((export.ExportError,ValueError)):self.assemble(pages)

    def test_component_checksum_or_frozen_verifier_change_refuses_before_export(self):
        for name in tuple(self.components):
            original=self.components[name];self.components[name]=b"changed"
            with self.subTest(component=name),self.assertRaises((export.ExportError,ValueError)):self.assemble()
            self.components[name]=original
        with self.assertRaises(export.ExportError):export.artifact_version((ROOT/"stoarama_pull.py").read_bytes(),"r2-copy-v3")

    def test_operation_argv_source_pin_is_checked_before_any_executable_extraction(self):
        with mock.patch.object(export,"load_verifier",side_effect=AssertionError("executed unpinned source")),self.assertRaises(export.ExportError):
            export.assemble(self.pages,self.key,self.readback_sha,self.task_sha,fixture.ID,"0"*64,export.sha(self.source),lambda ref:self.components[ref["file"]])

    def test_regular_path_capture_and_create_only_export_reject_aliases(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve();(root/"component").write_bytes(b"metadata")
            self.assertEqual(export.read_component(root,"component",8,(export.sha(b"metadata"),8)),b"metadata")
            (root/"alias").symlink_to(root/"component")
            os.link(root/"component",root/"hardlink")
            for name in ("alias","hardlink","../component","component/../component"):
                with self.subTest(name=name),self.assertRaises((export.ExportError,OSError)):export.read_component(root,name,8)
            out=root/"new"
            export.write_new(out,{"nested/result.json":b"SYNTHETIC"})
            self.assertEqual((out/"nested/result.json").read_bytes(),b"SYNTHETIC")
            with self.assertRaises(FileExistsError):export.write_new(out,{"other":b"must not overwrite"})
            self.assertEqual((out/"nested/result.json").read_bytes(),b"SYNTHETIC")

    def test_exact_isolated_cli_produces_components_and_request_without_server_authority(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve();components=root/"inputs";components.mkdir()
            for name,raw in self.components.items():
                path=components/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
            logs=root/"SYNTHETIC-logs.json";logs.write_bytes(pull.cleanup_json_bytes(self.pages[0]))
            out=root/"output"
            argv=[sys.executable,"-I","-S","-B",str(ROOT/"export_stitch_readback.py"),
                "--logs",str(logs),"--components-root",str(components),"--out",str(out),
                "--plan-sha",self.key[0],"--native-run",self.key[1],"--pair-sha",self.key[2],
                "--readback-id",self.key[3],"--readback-sha",self.readback_sha,"--native-task-sha",self.task_sha,
                "--reader-boot",fixture.ID,"--producer-client-sha",export.sha(self.source),"--reader-client-sha",export.sha(self.source)]
            result=subprocess.run(argv,env={},stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
            self.assertEqual(result.returncode,0,result.stderr.decode());self.assertEqual(result.stderr,b"")
            ref=json.loads(result.stdout);manifest=(out/ref["file"]).read_bytes()
            self.assertEqual((ref["sha256"],ref["size_bytes"]),(export.sha(manifest),len(manifest)))
            self.assertEqual(json.loads(manifest)["authority"],"none")
            self.assertEqual((out/"local-registration.json").read_bytes(),pull.cleanup_json_bytes(self.receipt))
            self.assertTrue((out/"server-validation-request.json").is_file())
            self.assertFalse((out/"server-validation.json").exists())
            self.assertFalse((out/"attestation.json").exists())


if __name__=="__main__":unittest.main()
