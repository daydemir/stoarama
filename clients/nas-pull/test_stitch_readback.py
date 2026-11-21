"""SYNTHETIC contract examples only; no app import, media or authority export."""
import ast
import base64
import binascii
import copy
import datetime
from enum import Enum, IntEnum
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import tempfile
import threading
import time
from types import SimpleNamespace
from typing import NamedTuple
import unittest
from unittest import mock
import uuid
import zlib


ROOT = Path(__file__).parent
SOURCE = (ROOT / "stoarama_pull.py").read_bytes()
SCHEMA = (ROOT / "stitch-proof/attestation-v2.schema.json").read_bytes()


class Hold(ValueError):
    pass


def load_contract():
    names = {"StitchReadbackContractError", "StitchReadbackDefinition", "stitch_readback_validate",
             "stitch_seconds_ns", "stitch_delta_ns", "StitchDeadline", "NASStitchMode", "StitchProofSwitch", "StitchSwitches", "StitchProfileKey", "StitchClientIdentity",
             "StitchProfileBytes", "StitchProducerProfile", "StitchProfilePair",
             "stitch_readback_profiles", "stitch_proof_json", "StitchReadbackIntegrity",
             "stitch_readback_file", "stitch_readback_registration", "stitch_readback_native",
             "stitch_readback_pipeline", "StitchReadbackStop", "stitch_readback_run",
             "stitch_check", "certification_identity", "stitch_proof_check", "cleanup_json_bytes",
             "stitch_proof_unpack", "stitch_proof_validate", "stitch_proof_success",
             "StitchEvidenceKind", "StitchEvidenceKey", "stitch_evidence_chunks", "StitchEvidencePacer",
             "StitchEvidenceSnapshot", "StitchNativeEvidenceStage", "stitch_evidence_index",
             "StitchEvidenceLedger", "StitchEvidenceDispatch", "StitchEvidenceProgress",
             "stitch_evidence_task", "parse_certification_timestamp", "MediaCertificationError"}
    constants = {"CLIENT_VERSION", "LOW_MEMORY_COPY_CLIENT_VERSION",
                 "STITCH_READBACK_SCHEMA_SHA", "STITCH_READBACK_SCHEMA_JSON",
                 "STITCH_PROOF_SCHEMA_SHA", "STITCH_PROOF_SCHEMA_JSON", "V85R_MANIFEST_SHA",
                 "STITCH_POLICY", "STITCH_REVISION", "STITCH_CORPUS_SHA", "STITCH_PROOF_SOURCE_SHA",
                 "STITCH_PROOF_KIND", "CLEANUP_KEEP28_POLICY_SHA256", "STITCH_PROOF_MAX_BYTES",
                 "V85R_TOOL_VERSION", "V85R_TOOLS"}
    namespace = dict(StitchHold=Hold, Enum=Enum, IntEnum=IntEnum, hashlib=hashlib, json=json, re=re,
                     datetime=datetime, threading=threading, NamedTuple=NamedTuple,
                     env_str=lambda key, default: default, os=os, stat_module=stat, Path=Path,
                     time=time, uuid=uuid, zlib=zlib, base64=base64, binascii=binascii)
    selected = []
    for node in ast.parse(SOURCE).body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names:
            selected.append(node)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in constants:
                    if target.id == "STITCH_PROOF_MAX_BYTES":
                        assert ast.unparse(node.value) == "128 * 1024 * 1024"
                        namespace[target.id] = 128*1024*1024
                    else:
                        namespace[target.id] = ast.literal_eval(node.value)
    exec(compile(ast.Module(body=selected, type_ignores=[]), "<synthetic readback contract>", "exec"), namespace)
    return namespace


CORE = load_contract()
DEFINITION = CORE["StitchReadbackDefinition"]
UTC = "2026-11-15T00:00:00Z"
ID = "11111111-1111-4111-8111-111111111111"
SHA = "a" * 64
FILE = dict(file="synthetic/component.json", sha256=SHA, size_bytes=1)
POLICY = {k: v["const"] for k, v in json.loads(SCHEMA)["$defs"]["policy"]["properties"].items()}
TOOLS = {name: {k: v["const"] for k, v in json.loads(SCHEMA)["$defs"][name]["properties"].items()}
         for name in ("ffmpeg", "ffprobe")}
HOST = dict(connection_id=13, system="Linux", machine="x86_64", cpu_model="SYNTHETIC", cpu_count=1)


def producer():
    return dict(schema="stoarama.nas-stitch-producer-profile.v1", profile_id="nas-v2-5009-337c",
        repository="daydemir/stoarama", base_commit_sha="a"*40, reviewed_head_sha="b"*40,
        merged_commit_sha="c"*40, source_sha256=SHA, client_version="v85r-nas-proof-v2",
        client_artifact=copy.deepcopy(FILE), contracts={k: copy.deepcopy(FILE) for k in
            ("native_receipt_schema", "native_verifier", "attestation_schema")},
        native_kind="nas-only-v85r-proof-v1", native_schema_id="stoarama.nas-stitch-native-receipt.v1",
        package_manifest=copy.deepcopy(FILE), policy=copy.deepcopy(POLICY), tools=copy.deepcopy(TOOLS))


def readback():
    return dict(schema="stoarama.nas-stitch-readback.v2", native_run_id=ID, native_bundle_sha256=SHA,
        native_plan_sha256=SHA,native_task_sha256=SHA, producer_profile_sha256=SHA, reader_profile_sha256=SHA,
        profile_pair_sha256=SHA, id=ID, started_at=UTC, verified_at=UTC,
        reader_execution=dict(connection_id=13, account_id=47, client_boot_id=ID, host=copy.deepcopy(HOST),
            client_version="r2-copy-v3", client_artifact_sha256=SHA, client_artifact_size_bytes=1),
        outputs=[dict(relative_path="SYNTHETIC/proof.mp4", sha256=SHA, size_bytes=1)],
        policy=copy.deepcopy(POLICY), tools=copy.deepcopy(TOOLS), native_registration_sha256=SHA)


def server_validation():
    original=readback()
    value={name:copy.deepcopy(original[name]) for name in (
        "native_run_id","native_bundle_sha256","native_plan_sha256","native_task_sha256","producer_profile_sha256",
        "reader_profile_sha256","profile_pair_sha256","reader_execution","outputs","policy","tools")}
    value.update(schema="stoarama.nas-stitch-validation.v2",validated_at=UTC,readback_receipt_sha256=SHA,
        reader_heartbeat_received_at=UTC,reader_release_stable=True,reader_release_candidate=False,
        reader_inventory_mismatch_count=0,registered_locally_only=True,grants_retirement=False,
        server_execution=dict(repository="daydemir/stoarama",commit_sha="a"*40,
            service_id="srv-d6usqn94tr6s73d94cd0",instance_id="SYNTHETIC-NOT-A-DEPLOYMENT"),
        scope=dict(stream_id=17208,recording_id=444,source_task_id=1,
            source_plan_sha256=CORE["STITCH_PROOF_SOURCE_SHA"],proof_plan_sha256=SHA,clip_ids=[373218,373262]))
    return value


def current_context():
    value=server_validation();value.pop("validated_at")
    value.update(schema="stoarama.nas-stitch-current-context.v1",checked_at=UTC,status_received_at=UTC,
        readback_verified_at=UTC,readback_id=ID,source_scope_approved_until="2026-11-15T01:00:00Z",integrity="clear")
    return value


def profiles():
    def file(name, sha, size): return dict(file="synthetic/"+name, sha256=sha, size_bytes=size)
    package = file("manifest.json", CORE["V85R_MANIFEST_SHA"], 5416)
    native = file("native.json", CORE["STITCH_PROOF_SCHEMA_SHA"], len(CORE["STITCH_PROOF_SCHEMA_JSON"].encode()))
    verifier = file("native.py", "81ff91e5accd93d32ff475024c10281177684b583e121a6c5d0c1f1938d49532", 18817)
    schema = file("readback.json", CORE["STITCH_READBACK_SCHEMA_SHA"], len(SCHEMA))
    p = producer(); p["package_manifest"] = package
    p["contracts"] = dict(native_receipt_schema=native, native_verifier=verifier,
        attestation_schema=file("v1.json", "03d9a5fa930b67249036a6b522dcffa59f6bba9744f79abc0747f85d9ee63abc", 17438))
    reader = dict(schema="stoarama.nas-stitch-reader-profile.v1", profile_id="nas-combined-v3-337c",
        repository=p["repository"], base_commit_sha=p["base_commit_sha"], reviewed_head_sha=p["reviewed_head_sha"],
        merged_commit_sha=p["merged_commit_sha"], source_sha256=SHA, client_version="r2-copy-v3",
        client_artifact=file("reader.py", "b"*64, 2),
        contracts=dict(readback_schema=schema, readback_validator=copy.deepcopy(FILE), attestation_schema=schema),
        capabilities=["stitch-proof", "r2-copy-control-v3", "cleanup-bulk-v8"],
        package_manifest=package, policy=copy.deepcopy(POLICY), tools=copy.deepcopy(TOOLS))
    raw = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    pair = dict(schema="stoarama.nas-stitch-profile-pair.v1", pair_id="nas-v2-5009-to-combined-v3-337c",
        producer_profile=file("producer.json", hashlib.sha256(raw(p)).hexdigest(), len(raw(p))),
        reader_profile=file("reader.json", hashlib.sha256(raw(reader)).hexdigest(), len(raw(reader))),
        native_kind=p["native_kind"], native_schema_id=p["native_schema_id"], native_schema=native,
        native_verifier=verifier, readback_schema=schema, readback_validator=copy.deepcopy(FILE),
        package_manifest=package, policy=copy.deepcopy(POLICY), tools=copy.deepcopy(TOOLS))
    return tuple(copy.deepcopy(document) for document in (p, reader, pair))


def seal_profiles(documents):
    """Reseal references so negative tests reach the intended semantic check."""
    producer, reader, pair = documents
    for name, value in (("producer_profile", producer), ("reader_profile", reader)):
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        pair[name].update(sha256=hashlib.sha256(raw).hexdigest(), size_bytes=len(raw))
    return documents


def synthetic_native():
    """Load only the existing explicit synthetic fixture, never the NAS app."""
    tree=ast.parse((ROOT/"test_stitch_proof.py").read_bytes())
    fixture=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=="synthetic_receipt")
    namespace=dict(pull=SimpleNamespace(**dict(CORE,CLIENT_VERSION="v85r-nas-proof-v2")),
                   struct=struct,json=json,hashlib=hashlib)
    exec(compile(ast.Module(body=[fixture],type_ignores=[]),"<synthetic native fixture>","exec"),namespace)
    return namespace["synthetic_receipt"]()


class ReadbackContractTests(unittest.TestCase):
    def validate(self, value, definition):
        CORE["stitch_readback_validate"](value, definition)

    def test_source_embeds_exact_versioned_contract_without_changing_native_schema(self):
        self.assertEqual(CORE["STITCH_READBACK_SCHEMA_JSON"].encode(), SCHEMA)
        self.assertEqual(CORE["STITCH_READBACK_SCHEMA_SHA"], hashlib.sha256(SCHEMA).hexdigest())
        native = (ROOT / "stitch-proof/native-receipt.schema.json").read_bytes()
        self.assertEqual(hashlib.sha256(native).hexdigest(), "18fffd8f94334210eb12ccee8c0856a824cd9ae9c3dc97b6fe6124e99a0906d4")

    def test_role_discriminator_rejects_aliasing_old_producer_to_new_label(self):
        value = producer()
        self.validate(value, DEFINITION.PRODUCER)
        value["client_version"] = "r2-copy-v3"
        with self.assertRaises(Hold): self.validate(value, DEFINITION.PRODUCER)
        value["profile_id"] = "nas-combined-v3-337c"
        self.validate(value, DEFINITION.PRODUCER)
        value["client_version"] = "v85r-nas-proof-v2"
        with self.assertRaises(Hold): self.validate(value, DEFINITION.PRODUCER)

    def test_file_references_refuse_traversal_urls_empty_components_and_oversize(self):
        self.validate(producer(), DEFINITION.PRODUCER)
        for path in ("../file", "a/../file", "/tmp/file", "https://example.com/a", "a//b", "a/", "a/./b"):
            value = producer(); value["client_artifact"]["file"] = path
            with self.subTest(path=path), self.assertRaises(Hold): self.validate(value, DEFINITION.PRODUCER)
        for size in (True, 0, 8*1024*1024+1):
            value = producer(); value["client_artifact"]["size_bytes"] = size
            with self.subTest(size=size), self.assertRaises(Hold): self.validate(value, DEFINITION.PRODUCER)

    def test_readback_is_closed_typed_and_utc_without_claiming_native_success(self):
        self.validate(readback(), DEFINITION.READBACK)
        for timestamp in ("2026-02-30T00:00:00Z", "2026-11-15T00:00:00+00:00", "2026-11-15T00:00:00", "not-a-time"):
            value = readback(); value["verified_at"] = timestamp
            with self.subTest(timestamp=timestamp), self.assertRaises(Hold): self.validate(value, DEFINITION.READBACK)
        for mutate in (lambda x: x.update(passed=True), lambda x: x.pop("native_run_id"),
                lambda x: x["reader_execution"].update(client_artifact_size_bytes=True),
                lambda x: x["reader_execution"].update(client_version="development"),
                lambda x: x.update(id="not-a-uuid"), lambda x: x.update(outputs=[])):
            value = readback(); mutate(value)
            with self.assertRaises(Hold): self.validate(value, DEFINITION.READBACK)

    def test_server_inventory_counter_has_no_version_alias_or_untyped_zero(self):
        schema=json.loads(SCHEMA)
        for builder,definition,name in ((server_validation,DEFINITION.VALIDATION,"validation"),
                (current_context,DEFINITION.CURRENT,"current_context")):
            self.assertIn("reader_inventory_mismatch_count",schema["$defs"][name]["required"])
            self.assertNotIn("reader_version_mismatch_count",schema["$defs"][name]["properties"])
            self.validate(builder(),definition)
            for count in (False,True,"0",0.0,1,-1,None):
                value=builder();value["reader_inventory_mismatch_count"]=count
                with self.subTest(name=name,count=count),self.assertRaises(Hold):self.validate(value,definition)
            for replace in (False,True):
                value=builder();value["reader_version_mismatch_count"]=0
                if replace:value.pop("reader_inventory_mismatch_count")
                with self.assertRaises(Hold):self.validate(value,definition)

    def test_inventory_zero_never_substitutes_for_version_release_boot_or_integrity_guards(self):
        for builder,definition in ((server_validation,DEFINITION.VALIDATION),(current_context,DEFINITION.CURRENT)):
            mutations=(lambda value:value["reader_execution"].update(client_version="development"),
                lambda value:value["reader_execution"].update(client_boot_id="unknown"),
                lambda value:value.update(reader_release_stable=False),
                lambda value:value.update(reader_release_candidate=True))
            for mutate in mutations:
                value=builder();mutate(value)
                self.assertEqual(value["reader_inventory_mismatch_count"],0)
                with self.assertRaises(Hold):self.validate(value,definition)
        value=current_context();value["integrity"]="refused"
        with self.assertRaises(Hold):self.validate(value,DEFINITION.CURRENT)

    def test_definition_is_typed_and_schema_errors_do_not_become_if_else_admission(self):
        with self.assertRaises(Hold): self.validate(readback(), "readback")
        original, digest = CORE["STITCH_READBACK_SCHEMA_JSON"], CORE["STITCH_READBACK_SCHEMA_SHA"]
        try:
            schema = json.loads(original)
            schema["$defs"]["producer_profile"]["allOf"][0]["if"]["unsupported"] = True
            raw = json.dumps(schema); CORE["STITCH_READBACK_SCHEMA_JSON"] = raw
            CORE["STITCH_READBACK_SCHEMA_SHA"] = hashlib.sha256(raw.encode()).hexdigest()
            with self.assertRaises(CORE["StitchReadbackContractError"]): self.validate(producer(), DEFINITION.PRODUCER)
        finally:
            CORE["STITCH_READBACK_SCHEMA_JSON"], CORE["STITCH_READBACK_SCHEMA_SHA"] = original, digest

    def test_mode_changes_permanently_cancel_previous_native_or_readback_job(self):
        switches = CORE["StitchSwitches"]()
        mode = CORE["NASStitchMode"]
        switches.apply(dict(ok=True, stitch_proof_task_id=1,stitch_proof_activation_id=ID))
        first = switches.proof[1]
        self.assertIs(switches.proof[0], mode.NATIVE)
        switches.apply(dict(ok=True, stitch_proof_task_id=2))
        second = switches.proof[1]
        self.assertTrue(first.is_set()); self.assertFalse(second.is_set())
        self.assertIs(switches.proof[0], mode.READBACK)
        switches.apply(dict(ok=True, stitch_proof_task_id=0))
        switches.apply(dict(ok=True, stitch_proof_task_id=2))
        self.assertTrue(second.is_set()); self.assertFalse(switches.proof[1].is_set())

    def test_activation_change_cancels_atomically_and_missing_native_nonce_refuses(self):
        switches=CORE["StitchSwitches"]()
        switches.apply(dict(ok=True,stitch_proof_task_id=1,stitch_proof_activation_id=ID))
        first=switches.proof
        switches.apply(dict(ok=True,stitch_proof_task_id=1,stitch_proof_activation_id="22222222-2222-4222-8222-222222222222"))
        self.assertTrue(first.cancel.is_set());self.assertFalse(switches.proof.cancel.is_set())
        self.assertEqual(switches.proof.activation_id,"22222222-2222-4222-8222-222222222222")
        with self.assertRaises(ValueError):switches.apply(dict(ok=True,stitch_proof_task_id=1))
        self.assertTrue(switches.proof.cancel.is_set());self.assertEqual(switches.proof.mode,CORE["NASStitchMode"].DISABLED)

    def test_unknown_or_boolean_mode_refuses_and_cancels_instead_of_native_fallback(self):
        for value in (True, None, 3, "1", -1):
            switches = CORE["StitchSwitches"]()
            switches.apply(dict(ok=True, stitch_proof_task_id=1,stitch_proof_activation_id=ID)); old = switches.proof[1]
            with self.subTest(value=value), self.assertRaises(ValueError):
                switches.apply(dict(ok=True, stitch_proof_task_id=value))
            self.assertTrue(old.is_set())
            self.assertIs(switches.proof[0], CORE["NASStitchMode"].DISABLED)

    def bind_profiles(self, documents, mode=None, key=None, identity=None):
        raw = CORE["StitchProfileBytes"](*(json.dumps(x, sort_keys=True, separators=(",", ":")).encode() for x in documents))
        expected = key or CORE["StitchProfileKey"](*(hashlib.sha256(x).hexdigest() for x in raw))
        client = identity or CORE["StitchClientIdentity"]("b"*64, 2, "r2-copy-v3")
        return CORE["stitch_readback_profiles"](raw, expected, client,
            CORE["NASStitchMode"].READBACK if mode is None else mode)

    def test_original_producer_and_current_reader_stay_distinct_and_native_refuses_old_producer(self):
        docs = profiles()
        bound = self.bind_profiles(docs)
        self.assertEqual(bound[0]["client_version"], "v85r-nas-proof-v2")
        self.assertEqual(bound[1]["client_version"], "r2-copy-v3")
        with self.assertRaises(Hold): self.bind_profiles(docs, CORE["NASStitchMode"].NATIVE)
        with self.assertRaisesRegex(Hold, "typed readback profile binding"):
            self.bind_profiles(docs, CORE["NASStitchMode"].DISABLED)

    def test_combined_self_pair_admits_current_native_producer_without_aliasing(self):
        producer, reader, pair = copy.deepcopy(profiles())
        producer.update(profile_id="nas-combined-v3-337c", client_version="r2-copy-v3",
                        client_artifact=copy.deepcopy(reader["client_artifact"]))
        producer["contracts"]["attestation_schema"] = copy.deepcopy(reader["contracts"]["attestation_schema"])
        pair["pair_id"] = "nas-combined-v3-337c-self"
        docs = seal_profiles((producer, reader, pair))
        bound = self.bind_profiles(docs, CORE["NASStitchMode"].NATIVE)
        self.assertEqual(bound[0]["client_artifact"], bound[1]["client_artifact"])
        docs[0]["client_artifact"]["file"]="synthetic/original-client.py"
        seal_profiles(docs)
        self.bind_profiles(docs,CORE["NASStitchMode"].NATIVE)
        docs[1]["client_artifact"]["sha256"]="c"*64
        seal_profiles(docs)
        with self.assertRaisesRegex(Hold,"original exact artifact"):
            self.bind_profiles(docs,CORE["NASStitchMode"].READBACK,
                identity=CORE["StitchClientIdentity"]("c"*64,2,"r2-copy-v3"))

    def test_resealed_semantic_drift_reaches_pipeline_and_contract_checks(self):
        mutations = (
            (lambda p,r,k: p["package_manifest"].update(sha256="f"*64), "pipeline/package drift"),
            (lambda p,r,k: k["readback_validator"].update(sha256="f"*64), "current readback contract"),
            (lambda p,r,k: p["contracts"]["native_verifier"].update(sha256="f"*64), "native contract"),
            (lambda p,r,k: p["contracts"]["attestation_schema"].update(size_bytes=1), "original producer schema"),
        )
        for mutate, message in mutations:
            docs = copy.deepcopy(profiles()); mutate(*docs); seal_profiles(docs)
            with self.subTest(message=message), self.assertRaisesRegex(Hold, message):
                self.bind_profiles(docs)

    def test_pair_file_bindings_and_current_executing_bytes_cannot_be_replaced(self):
        self.bind_profiles(profiles())
        for mutate in (lambda p,r,k: k["producer_profile"].update(sha256="0"*64),
                lambda p,r,k: k["reader_profile"].update(size_bytes=1),
                lambda p,r,k: k.update(pair_id="nas-combined-v3-337c-self"),
                lambda p,r,k: k["readback_validator"].update(sha256="f"*64),
                lambda p,r,k: p["package_manifest"].update(sha256="f"*64)):
            docs = copy.deepcopy(profiles()); mutate(*docs)
            with self.assertRaises(Hold): self.bind_profiles(docs)
        with self.assertRaises(Hold):
            self.bind_profiles(profiles(), key=CORE["StitchProfileKey"]("0"*64, "0"*64, "0"*64))
        with self.assertRaises(Hold):
            self.bind_profiles(profiles(), identity=CORE["StitchClientIdentity"]("c"*64, 2, "r2-copy-v3"))


class ReadbackFileTests(unittest.TestCase):
    def read(self, root, relative, limit=4*1024*1024, stop=None, expected=None, retain=False):
        return CORE["stitch_readback_file"](root, relative, limit, stop or threading.Event(), expected, retain)

    def test_streamed_hash_and_bounded_metadata_have_exact_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); body = b"SYNTHETIC\n"*150000
            (root/"file").write_bytes(body)
            expected = (len(body), hashlib.sha256(body).hexdigest())
            self.assertEqual(self.read(root, "file", expected=expected), (expected, None))
            self.assertEqual(self.read(root, "file", expected=expected, retain=True), (expected, body))
            with self.assertRaises(CORE["StitchReadbackIntegrity"]): self.read(root, "file", 2)
            with self.assertRaises(CORE["StitchReadbackIntegrity"]): self.read(root, "file", expected=(1, SHA))
            with self.assertRaises(CORE["StitchReadbackIntegrity"]): self.read(root, "file", 17*1024*1024, retain=True)

    def test_all_symlinks_hardlinks_and_noncanonical_paths_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); (root/"real").mkdir(); (root/"real/file").write_bytes(b"test")
            (root/"link").symlink_to(root/"real", target_is_directory=True)
            (root/"real/link").symlink_to(root/"real/file")
            for path in ("link/file", "real/link", "real/../real/file", "real//file", "/real/file", "real/./file", "real/file/"):
                with self.subTest(path=path), self.assertRaises(CORE["StitchReadbackIntegrity"]): self.read(root,path)
            os.link(root/"real/file", root/"alias")
            with self.assertRaises(CORE["StitchReadbackIntegrity"]): self.read(root,"alias")
            os.mkfifo(root/"fifo")
            with self.assertRaises(CORE["StitchReadbackIntegrity"]): self.read(root,"fifo")

    def test_leaf_or_ancestor_replacement_during_hashing_refuses_even_same_bytes(self):
        for ancestor in (False, True):
            with self.subTest(ancestor=ancestor), tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve(); (root/"parent").mkdir(); (root/"parent/file").write_bytes(b"test")
                original_read = os.read; changed = False
                def replace(fd, length):
                    nonlocal changed
                    body = original_read(fd,length)
                    if body and not changed:
                        changed = True
                        if ancestor:
                            (root/"parent").rename(root/"old"); (root/"parent").mkdir()
                        else:
                            (root/"parent/file").rename(root/"parent/old")
                        (root/"parent/file").write_bytes(b"test")
                    return body
                with mock.patch.object(os,"read",side_effect=replace), self.assertRaises(CORE["StitchReadbackIntegrity"]):
                    self.read(root,"parent/file")

    def test_cancel_after_block_prevents_further_reads_and_closes_every_descriptor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); (root/"file").write_bytes(b"a"*(2*1024*1024))
            stop = threading.Event(); original_open, original_read = os.open, os.read; opened=[]; reads=[]
            def open_file(*args,**kwargs):
                fd=original_open(*args,**kwargs); opened.append(fd); return fd
            def cancel_after_read(fd,length):
                reads.append(fd); body=original_read(fd,length); stop.set(); return body
            with mock.patch.object(os,"open",side_effect=open_file), mock.patch.object(os,"read",side_effect=cancel_after_read):
                with self.assertRaises(Hold): self.read(root,"file",stop=stop)
            self.assertEqual(len(reads),1)
            for fd in opened:
                with self.assertRaises(OSError): os.fstat(fd)

    def test_registration_sha_is_full_original_decoded_bytes_not_normalized_json(self):
        raw=b'{ "explicitly_synthetic" : true, "number": 1 }\n'
        bundle=gzip.compress(raw,mtime=0)
        self.assertEqual(CORE["stitch_readback_registration"](bundle),hashlib.sha256(raw).hexdigest())
        for invalid in (bundle+b"x", bundle+bundle, bundle[:-2], b"notgzip"):
            with self.subTest(invalid=invalid[:10]), self.assertRaises((Hold,zlib.error)):
                CORE["stitch_readback_registration"](invalid)


class ReadbackDeadlineTests(unittest.TestCase):
    def setUp(self):
        self.old = dict(CORE)
        self.clock = 100.0
        self.switches = CORE["StitchSwitches"](); self.switches.apply(dict(ok=True,stitch_proof_task_id=2))
        self.cfg=SimpleNamespace(stitch_switches=self.switches)
        CORE.update(time=SimpleNamespace(monotonic=lambda:self.clock,monotonic_ns=lambda:int(self.clock*10**9)),
            utc_now=lambda:UTC, parse_certification_timestamp=lambda value,label:datetime.datetime.fromisoformat(value.replace("Z","+00:00")),
            stitch_resources=lambda cfg:None,
            NAS_MEDIA_FENCE=SimpleNamespace(condition=threading.Condition(),execution_owner=None,cleaners=0))

    def tearDown(self):
        CORE.clear();CORE.update(self.old)

    def test_original_ninety_second_budget_does_not_extend_and_off_latches(self):
        expiry=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=1)).isoformat().replace("+00:00","Z")
        stop=CORE["StitchReadbackStop"](self.cfg,threading.Event(),expiry)
        self.assertEqual(stop.deadline,190)
        self.clock=189.99;self.assertFalse(stop.is_set())
        self.clock=190;self.assertTrue(stop.is_set())
        self.clock=100
        self.assertTrue(stop.is_set())
        stop=CORE["StitchReadbackStop"](self.cfg,threading.Event(),expiry)
        self.switches.apply(dict(ok=True,stitch_proof_task_id=0));self.switches.apply(dict(ok=True,stitch_proof_task_id=2))
        self.assertTrue(stop.is_set())

    def test_admission_expiry_margin_cleanup_and_already_expired_refuse(self):
        expiry=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=31)).isoformat().replace("+00:00","Z")
        stop=CORE["StitchReadbackStop"](self.cfg,threading.Event(),expiry)
        self.assertLessEqual(stop.deadline,101)
        CORE["NAS_MEDIA_FENCE"].execution_owner=object();self.assertTrue(stop.is_set())
        CORE["NAS_MEDIA_FENCE"].execution_owner=None;self.assertTrue(stop.is_set())
        with self.assertRaises(Hold): CORE["StitchReadbackStop"](self.cfg,threading.Event(),UTC)

    def test_resource_deterioration_or_cleanup_request_latches_without_extending_deadline(self):
        expiry=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=1)).isoformat().replace("+00:00","Z")
        for resource in (True,False):
            stop=CORE["StitchReadbackStop"](self.cfg,threading.Event(),expiry)
            deadline=stop.deadline;self.clock+=6
            if resource:
                CORE["stitch_resources"]=mock.Mock(side_effect=Hold("low memory"))
            else:
                CORE["NAS_MEDIA_FENCE"].cleaners=1
            self.assertTrue(stop.is_set());self.assertEqual(stop.deadline,deadline)
            CORE["stitch_resources"]=lambda cfg:None;CORE["NAS_MEDIA_FENCE"].cleaners=0
            self.assertTrue(stop.is_set())


class ReadbackRunTests(unittest.TestCase):
    def setUp(self):
        self.old=dict(CORE)
        self.task,self.native=synthetic_native()
        self.docs=profiles(); self.native.update(client_version="v85r-nas-proof-v2",client_artifact_sha256="a"*64)
        self.bundle=gzip.compress(CORE["cleanup_json_bytes"](self.native),mtime=0)
        self.raw=CORE["StitchProfileBytes"](*(CORE["cleanup_json_bytes"](d) for d in self.docs))
        self.key=CORE["StitchProfileKey"](*(hashlib.sha256(d).hexdigest() for d in self.raw))
        self.cfg=SimpleNamespace(current_file=Path("/SYNTHETIC/state/client.py"),state_dir=Path("/SYNTHETIC/state"),
            runtime_file=Path("/SYNTHETIC/state/runtime.json"),output_dir=Path("/SYNTHETIC/media"),
            executing_release_sha256="b"*64,is_candidate=False)
        self.stop=SimpleNamespace(is_set=lambda:False,started_at=UTC,run_id=ID)
        self.pipeline=mock.Mock()
        self.files=mock.Mock(side_effect=self.read_file)
        CORE.update(CLIENT_VERSION="r2-copy-v3",utc_now=lambda:UTC,_read_text=lambda path:"model name : SYNTHETIC\n",
            parse_certification_timestamp=lambda value,label:datetime.datetime.fromisoformat(value.replace("Z","+00:00")),
            stitch_resources=mock.Mock(),stitch_readback_pipeline=self.pipeline,stitch_readback_file=self.files)
        self.platform=mock.patch.multiple("platform",system=lambda:"Linux",machine=lambda:"x86_64")
        self.platform.start()

    def tearDown(self):
        self.platform.stop();CORE.clear();CORE.update(self.old)

    def read_file(self,root,relative,limit,stop,expected=None,retain=False):
        if relative=="client.py":return (2,"b"*64),None
        if relative=="runtime.json":return (1,SHA),CORE["cleanup_json_bytes"](dict(boot_id=ID))
        self.assertEqual(relative,self.native["result"]["outputs"][0]["relative_path"])
        self.assertEqual(expected,(1,"e"*64))
        return expected,None

    def run_readback(self,bundle=None):
        return CORE["stitch_readback_run"](self.cfg,self.stop,self.raw,self.key,
            CORE["cleanup_json_bytes"](self.task),self.bundle if bundle is None else bundle)

    def test_preserved_native_bytes_times_and_full_registration_binding_survive_readback(self):
        before=copy.deepcopy(self.native)
        body=self.run_readback();result=json.loads(body)
        self.assertEqual(self.native,before)
        self.assertEqual(result["native_run_id"],before["run"]["id"])
        self.assertEqual(result["native_bundle_sha256"],hashlib.sha256(self.bundle).hexdigest())
        self.assertEqual(result["native_registration_sha256"],hashlib.sha256(gzip.decompress(self.bundle)).hexdigest())
        self.assertEqual(result["reader_execution"]["client_artifact_sha256"],"b"*64)
        self.assertEqual(result["producer_profile_sha256"],self.key.producer)
        self.assertEqual(result["started_at"],UTC);self.assertEqual(result["verified_at"],UTC)
        self.pipeline.assert_called_once_with(self.cfg,self.docs[1],self.stop)
        self.assertEqual([call.args[1] for call in self.files.call_args_list],
                         ["client.py","synthetic-proof/proof.mp4","runtime.json"])
        # No source media call, native process, repair or credential/network API exists in this loader.
        self.assertNotIn("stitch_build",CORE);self.assertNotIn("request_json",CORE)

    def test_failed_singleton_hold_or_original_producer_substitution_refuses_without_native_fallback(self):
        for mutate in (lambda r:r["result"].update(error="synthetic failure"),
                lambda r:r["result"].update(left_out=[373218]),
                lambda r:r["result"]["outputs"][0].update(inputs=r["result"]["outputs"][0]["inputs"][:1]),
                lambda r:r.update(client_artifact_sha256="b"*64),
                lambda r:r["result"]["outputs"][0].update(relative_path="other/proof.mp4")):
            value=copy.deepcopy(self.native);mutate(value)
            with self.subTest(mutate=mutate),self.assertRaises(CORE["StitchReadbackIntegrity"]):
                self.run_readback(gzip.compress(CORE["cleanup_json_bytes"](value),mtime=0))
        self.pipeline.assert_not_called()

    def test_absent_promoted_identity_refuses_before_io_or_manifest_fetch(self):
        for identity,candidate in ((None,False),("not-a-sha",False),("b"*64,True)):
            self.cfg.executing_release_sha256=identity;self.cfg.is_candidate=candidate
            with self.assertRaises(Hold):self.run_readback()
        self.files.assert_not_called();self.pipeline.assert_not_called()

    def test_resource_hold_is_transient_but_output_or_package_integrity_failure_is_distinct(self):
        CORE["stitch_resources"].side_effect=Hold("resource hold")
        with self.assertRaises(Hold) as error:self.run_readback()
        self.assertNotIsInstance(error.exception,CORE["StitchReadbackIntegrity"])
        CORE["stitch_resources"].side_effect=None
        self.pipeline.side_effect=CORE["StitchReadbackIntegrity"]("package changed")
        with self.assertRaises(CORE["StitchReadbackIntegrity"]):self.run_readback()
        self.pipeline.side_effect=None
        self.files.side_effect=CORE["StitchReadbackIntegrity"]("output changed")
        with self.assertRaises(CORE["StitchReadbackIntegrity"]):self.run_readback()


class ReadbackPipelineTests(unittest.TestCase):
    def test_installed_member_and_tool_are_rehashed_without_executing_or_repairing_them(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();module=b"explicitly synthetic inert text\n";tool=b"not executable\n"
            manifest=CORE["cleanup_json_bytes"](dict(files={"runner.py":hashlib.sha256(module).hexdigest()}))
            manifest_sha=hashlib.sha256(manifest).hexdigest()
            package=root/"tools"/("v85r-"+manifest_sha);package.mkdir(parents=True)
            (package/"manifest.json").write_bytes(manifest);(package/"runner.py").write_bytes(module)
            tools=root/"tools/v85r-ffmpeg-SYNTHETIC";tools.mkdir();(tools/"ffprobe").write_bytes(tool)
            reader=dict(package_manifest=dict(sha256=manifest_sha,size_bytes=len(manifest)),
                tools=dict(ffprobe=dict(version="SYNTHETIC",sha256=hashlib.sha256(tool).hexdigest(),size_bytes=len(tool))))
            cfg=SimpleNamespace(state_dir=root);stop=threading.Event()
            CORE["stitch_readback_pipeline"](cfg,reader,stop)
            (package/"runner.py").write_bytes(b"changed")
            with self.assertRaises(CORE["StitchReadbackIntegrity"]):CORE["stitch_readback_pipeline"](cfg,reader,stop)
            (package/"runner.py").write_bytes(module);(tools/"ffprobe").write_bytes(b"changed")
            with self.assertRaises(CORE["StitchReadbackIntegrity"]):CORE["stitch_readback_pipeline"](cfg,reader,stop)


class EvidenceFramingTests(unittest.TestCase):
    def setUp(self):
        self.old=dict(CORE);CORE["utc_now_precise"]=lambda:UTC

    def tearDown(self):
        CORE.clear();CORE.update(self.old)

    def key(self,kind):
        return CORE["StitchEvidenceKey"](kind,ID,SHA,"b"*64,ID)

    def test_historical_native_framing_is_exact_and_new_kinds_bind_all_identifiers(self):
        body=b"SYNTHETIC"*6000;digest=hashlib.sha256(body).hexdigest()
        kind=CORE["StitchEvidenceKind"]
        for member in kind:
            cursor=0;decoded=[]
            while cursor < (len(body)+511)//512:
                batch,end=CORE["stitch_evidence_chunks"](self.key(member),body,cursor)
                self.assertLessEqual(len(batch),64);self.assertGreater(end,cursor)
                for index,line in enumerate(batch,cursor):
                    message=line["message"];fields=message.split(" ")
                    if member is kind.NATIVE:
                        self.assertEqual(message,"V85r proof-v1 %s %s %d/%d %s"%(SHA,digest,index,(len(body)+511)//512,body[index*512:(index+1)*512].hex()))
                    else:
                        self.assertEqual(fields[:9],["V85r",member.value,SHA,digest,ID,"b"*64,ID,str(len(body)),"%d/%d"%(index,(len(body)+511)//512)])
                    decoded.append(bytes.fromhex(fields[-1]))
                cursor=end
            self.assertEqual(b"".join(decoded),body)
            self.assertEqual(CORE["stitch_evidence_chunks"](self.key(member),body,cursor),([],cursor))

    def test_body_cursor_and_typed_identity_bounds_refuse(self):
        kind=CORE["StitchEvidenceKind"]
        for body,cursor in ((b"",0),(b"a",True),(b"a",-1),(b"a",2),(b"x"*(65536+1),0)):
            with self.assertRaises(Hold):CORE["stitch_evidence_chunks"](self.key(kind.READBACK),body,cursor)
        for key in (self.key(kind.READBACK)._replace(plan="A"*64),self.key(kind.READBACK)._replace(native_run="not-a-uuid"),
                    self.key(kind.READBACK)._replace(kind="readback-v2")):
            with self.assertRaises(Hold):CORE["stitch_evidence_chunks"](key,b"a",0)

    def test_one_shared_reservation_counts_failed_attempt_and_restart_has_full_quiet_minute(self):
        clock=[100.0];CORE["time"]=SimpleNamespace(monotonic=lambda:clock[0])
        pacer=CORE["StitchEvidencePacer"]()
        self.assertFalse(pacer.take());clock[0]=159.999;self.assertFalse(pacer.take())
        clock[0]=160
        results=[]
        threads=[threading.Thread(target=lambda:results.append(pacer.take())) for _ in range(16)]
        for thread in threads:thread.start()
        for thread in threads:thread.join()
        self.assertEqual(results.count(True),1)
        # Treat that reservation as a lost ACK: it still consumes the minute.
        clock[0]=219.999;self.assertFalse(pacer.take());clock[0]=220;self.assertTrue(pacer.take())
        restart=CORE["StitchEvidencePacer"]();self.assertFalse(restart.take())
        clock[0]=279.999;self.assertFalse(restart.take());clock[0]=280;self.assertTrue(restart.take())


class SyntheticMemoryEvidenceStore:
    """CAS test double only; no registry, media, native or production authority."""
    def __init__(self):
        self.body=None;self.blobs={};self.fail=False

    def snapshot(self):
        return CORE["StitchEvidenceSnapshot"](self.body is not None,
            hashlib.sha256(self.body).hexdigest() if self.body is not None else "",self.body or b"")

    def replace(self,expected,body):
        if self.fail:raise OSError("SYNTHETIC failed durable checkpoint")
        if expected != self.snapshot():raise Hold("SYNTHETIC CAS mismatch")
        self.body=bytes(body);return self.snapshot()

    def put(self,body):
        digest=hashlib.sha256(body).hexdigest();self.blobs[digest]=bytes(body);return digest

    def read(self,digest,*,stop=None):
        if stop is not None:CORE["stitch_check"](stop)
        return self.blobs[digest]


class EvidenceLedgerTests(unittest.TestCase):
    def setUp(self):
        self.old=dict(CORE);self.clock=100.0
        self.now=datetime.datetime.fromisoformat(UTC.replace("Z","+00:00"))
        now=self.now
        class FrozenDatetime(datetime.datetime):
            @classmethod
            def now(cls,tz=None):return now
        CORE.update(time=SimpleNamespace(monotonic=lambda:self.clock,monotonic_ns=lambda:int(self.clock*10**9)),utc_now_precise=lambda:UTC,
                    datetime=SimpleNamespace(datetime=FrozenDatetime,timezone=datetime.timezone,timedelta=datetime.timedelta))
        self.store=SyntheticMemoryEvidenceStore();self.ledger=CORE["StitchEvidenceLedger"](self.store)

    def tearDown(self):
        CORE.clear();CORE.update(self.old)

    def session(self):
        return dict(native_run_id=ID,producer_profile_sha256=SHA,reader_profile_sha256=SHA,
                    profile_pair_sha256=SHA,reader_boot_id=ID)

    def test_native_window_and_original_admission_do_not_extend_after_restart_or_refresh(self):
        mode=CORE["NASStitchMode"]
        self.assertFalse(self.ledger.due())
        self.ledger.observe_mode(mode.NATIVE,self.now, ID,ID)
        expiry=self.ledger.state["window"]["expires_at"]
        self.ledger.enter_native()
        task=dict(expires_at=expiry,activation_id=ID,explicitly_synthetic="original admission")
        raw=CORE["cleanup_json_bytes"](task)
        original=CORE["cleanup_json_bytes"](dict(task=task,activation_id=ID,native_run_id=self.ledger.state["window"]["run_id"],task_b64=base64.b64encode(raw).decode("ascii"),
            task_sha256=hashlib.sha256(raw).hexdigest()))
        self.assertEqual(self.ledger.bind_admission(original),original)
        self.ledger.observe_mode(mode.NATIVE,self.now+datetime.timedelta(minutes=10), ID,ID)
        self.assertEqual(self.ledger.state["window"]["expires_at"],expiry)
        restarted=CORE["StitchEvidenceLedger"](self.store)
        self.assertEqual(restarted.original_admission(),original)
        restarted.observe_mode(mode.NATIVE,self.now+datetime.timedelta(minutes=20), ID,ID)
        with self.assertRaises(Hold):restarted.native_deadline_ns(self.now+datetime.timedelta(minutes=15))
        changed=json.loads(original);changed["task"]["expires_at"]="2099-01-01T00:00:00Z"
        raw=CORE["cleanup_json_bytes"](changed["task"])
        changed.update(task_b64=base64.b64encode(raw).decode("ascii"),task_sha256=hashlib.sha256(raw).hexdigest())
        with self.assertRaises(CORE["StitchReadbackIntegrity"]):restarted.bind_admission(CORE["cleanup_json_bytes"](changed))
        self.assertEqual(restarted.native_deadline_ns(self.now),(100+15*60-30)*10**9)
        restarted.observe_mode(mode.DISABLED,self.now+datetime.timedelta(minutes=20), ID)
        restarted.observe_mode(mode.NATIVE,self.now+datetime.timedelta(minutes=21), ID,ID)
        self.assertEqual(restarted.state["window"]["expires_at"],expiry)

    def test_activation_spend_survives_off_restart_and_replay_after_new_uuid(self):
        mode=CORE["NASStitchMode"];later="22222222-2222-4222-8222-222222222222"
        self.ledger.observe_mode(mode.NATIVE,self.now,ID,ID)
        self.ledger.enter_native()
        original=copy.deepcopy(self.ledger.state["window"])
        for off in (mode.DISABLED,mode.READBACK,mode.DISABLED):
            self.ledger.observe_mode(off,self.now,ID)
            self.ledger=CORE["StitchEvidenceLedger"](self.store)
            self.ledger.observe_mode(mode.NATIVE,self.now+datetime.timedelta(minutes=10),ID,ID)
            self.assertEqual(self.ledger.state["window"],original)
            with self.assertRaises(Hold):self.ledger.enter_native()
        self.ledger.observe_mode(mode.NATIVE,self.now,ID,later)
        self.assertNotEqual(self.ledger.state["window"]["run_id"],original["run_id"])
        self.assertEqual(self.ledger.state["used_activation_ids"],[ID,later])
        self.assertFalse(self.ledger.state["window"]["entered"])
        with self.assertRaises(Hold):self.ledger.observe_mode(mode.NATIVE,self.now,ID,ID)

    def test_activation_bound_never_prunes_and_invalid_native_id_never_commits(self):
        mode=CORE["NASStitchMode"]
        before=self.store.snapshot()
        for activation in ("", "00000000-0000-0000-0000-000000000000", "ABCDEFAB-1111-4111-8111-111111111111",None):
            with self.assertRaises(Hold):self.ledger.observe_mode(mode.NATIVE,self.now,ID,activation)
            self.assertEqual(self.store.snapshot(),before)
        for index in range(1,65):
            self.ledger.observe_mode(mode.NATIVE,self.now,ID,str(uuid.UUID(int=index)))
        before=self.store.snapshot()
        with self.assertRaises(Hold):self.ledger.observe_mode(mode.NATIVE,self.now,ID,str(uuid.UUID(int=65)))
        self.assertEqual(self.store.snapshot(),before)

    def test_restart_profile_or_boot_change_drops_accepting_history_not_preserved_blobs(self):
        self.ledger.observe_mode(CORE["NASStitchMode"].READBACK,self.now, ID)
        self.ledger.bind_session(self.session());self.ledger.complete_readback(CORE["cleanup_json_bytes"](readback()))
        original_blobs=dict(self.store.blobs)
        restarted=CORE["StitchEvidenceLedger"](self.store)
        self.assertEqual(restarted.history,[]);self.assertEqual(len(restarted.state["history"]),1)
        self.assertTrue(restarted.due())
        changed=self.session();changed["reader_boot_id"]="22222222-2222-4222-8222-222222222222"
        restarted.bind_session(changed)
        self.assertEqual(restarted.history,[]);self.assertEqual(restarted.state["history"],[])
        self.assertEqual(self.store.blobs,original_blobs)

    def test_four_observations_six_hour_cadence_and_hold_never_refresh_prior_time(self):
        self.ledger.observe_mode(CORE["NASStitchMode"].READBACK,self.now, ID)
        self.ledger.bind_session(self.session())
        for number in range(5):
            value=readback();value["id"]="%08x-1111-4111-8111-111111111111"%(number+1)
            self.ledger.complete_readback(CORE["cleanup_json_bytes"](value))
        self.assertEqual(len(self.ledger.history),4)
        previous=copy.deepcopy(self.ledger.history)
        self.assertFalse(self.ledger.due());self.clock+=6*3600;self.assertTrue(self.ledger.due())
        self.ledger.hold();self.assertFalse(self.ledger.due());self.clock+=59.999;self.assertFalse(self.ledger.due())
        self.clock+=.001;self.assertTrue(self.ledger.due());self.assertEqual(self.ledger.history,previous)
        self.assertEqual(len(self.store.blobs),5)

    def test_integrity_refusal_survives_restart_mode_changes_and_cannot_complete(self):
        mode=CORE["NASStitchMode"];self.ledger.observe_mode(mode.READBACK,self.now, ID)
        self.ledger.bind_session(self.session());self.ledger.refuse()
        restarted=CORE["StitchEvidenceLedger"](self.store)
        restarted.observe_mode(mode.DISABLED,self.now, ID);restarted.observe_mode(mode.READBACK,self.now, ID)
        with self.assertRaises(CORE["StitchReadbackIntegrity"]):restarted.due()
        with self.assertRaises(CORE["StitchReadbackIntegrity"]):restarted.complete_readback(CORE["cleanup_json_bytes"](readback()))
        self.assertEqual(restarted.history,[])

    def test_exact_ack_persists_cursor_and_complete_queue_row_drops_without_deleting_blob(self):
        key=CORE["StitchEvidenceKey"](CORE["StitchEvidenceKind"].READBACK,ID,SHA,SHA,ID)
        body=b"SYNTHETIC"*6000;self.ledger.enqueue(key,body);self.ledger.enqueue(key,body)
        self.assertEqual(len(self.ledger.state["queue"]),1)
        item=dict(self.ledger.state["queue"][0]);progress=CORE["StitchEvidenceProgress"]
        with self.assertRaises(CORE["StitchReadbackIntegrity"]):
            self.ledger.acknowledge(item,progress(CORE["StitchEvidenceDispatch"].ACKNOWLEDGED,65))
        self.ledger.acknowledge(item,progress(CORE["StitchEvidenceDispatch"].ACKNOWLEDGED,64))
        restarted=CORE["StitchEvidenceLedger"](self.store);item=dict(restarted.state["queue"][0])
        self.assertEqual(item["cursor"],64)
        restarted.acknowledge(item,progress(CORE["StitchEvidenceDispatch"].ACKNOWLEDGED,(len(body)+511)//512))
        self.assertEqual(restarted.state["queue"],[]);self.assertEqual(self.store.read(item["blob_sha256"]),body)

    def test_failed_checkpoint_does_not_advance_memory_state_and_malformed_index_refuses(self):
        before=self.ledger.copy();self.store.fail=True
        with self.assertRaises(OSError):self.ledger.observe_mode(CORE["NASStitchMode"].READBACK,self.now, ID)
        self.assertEqual(self.ledger.state,before)
        self.store.fail=False
        for mutate in (lambda value:value.update(mode=True),lambda value:value.update(mode=3),
                lambda value:value.update(mode=1),lambda value:value.update(window=1),
                lambda value:value.update(extra=True),lambda value:value.update(native={"stage":"unknown"})):
            value=copy.deepcopy(before);mutate(value)
            with self.assertRaises(CORE["StitchReadbackIntegrity"]):CORE["stitch_evidence_index"](CORE["cleanup_json_bytes"](value))

    def test_clock_rollback_or_new_boot_cannot_extend_original_native_window(self):
        mode=CORE["NASStitchMode"];self.ledger.observe_mode(mode.NATIVE,self.now,ID,ID)
        original=self.ledger.state["window"]["deadline_monotonic_ns"]
        self.clock+=15*60
        restarted=CORE["StitchEvidenceLedger"](self.store)
        restarted.observe_mode(mode.NATIVE,self.now-datetime.timedelta(days=1),ID,ID)
        self.assertEqual(restarted.state["window"]["deadline_monotonic_ns"],original)
        with self.assertRaises(Hold):restarted.native_deadline_ns(self.now-datetime.timedelta(days=1))
        self.clock=1
        restarted.observe_mode(mode.NATIVE,self.now,"22222222-2222-4222-8222-222222222222",ID)
        with self.assertRaises(Hold):restarted.native_deadline_ns(self.now)
        with self.assertRaises(Hold):restarted.due()


if __name__ == "__main__":
    unittest.main()
