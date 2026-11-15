"""Explicitly SYNTHETIC readback relationship tests; no actual NAS authority."""
import base64
import builtins
import copy
from contextlib import ExitStack
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import unittest
from unittest import mock

import test_stitch_readback as fixture


ROOT=Path(__file__).parent
SCRIPT=(ROOT/'verify_stitch_readback.py').read_bytes()
spec=importlib.util.spec_from_file_location('readback_verifier',ROOT/'verify_stitch_readback.py')
verify=importlib.util.module_from_spec(spec);spec.loader.exec_module(verify)


def synthetic_stream():
    source=(ROOT/'stoarama_pull.py').read_bytes();source_sha=hashlib.sha256(source).hexdigest()
    producer,reader,pair=fixture.profiles()
    reader['client_artifact'].update(sha256=source_sha,size_bytes=len(source))
    validator=dict(file='synthetic/readback-verifier.py',sha256=hashlib.sha256(SCRIPT).hexdigest(),size_bytes=len(SCRIPT))
    reader['contracts']['readback_validator']=validator;pair['readback_validator']=copy.deepcopy(validator)
    fixture.seal_profiles((producer,reader,pair))
    canonical=fixture.CORE['cleanup_json_bytes']
    profiles=[canonical(value) for value in (producer,reader,pair)]
    task,native=fixture.synthetic_native()
    native.update(client_artifact_sha256=producer['client_artifact']['sha256'],client_version=producer['client_version'])
    bundle=gzip.compress(canonical(native),mtime=0)
    readback=fixture.readback()
    readback.update(native_run_id=native['run']['id'],native_bundle_sha256=hashlib.sha256(bundle).hexdigest(),
        native_plan_sha256=native['plan_sha256'],producer_profile_sha256=hashlib.sha256(profiles[0]).hexdigest(),
        reader_profile_sha256=hashlib.sha256(profiles[1]).hexdigest(),profile_pair_sha256=hashlib.sha256(profiles[2]).hexdigest(),
        native_registration_sha256=hashlib.sha256(gzip.decompress(bundle)).hexdigest(),
        outputs=[{name:native['result']['outputs'][0][name] for name in ('relative_path','sha256','size_bytes')}])
    readback['reader_execution'].update(client_artifact_sha256=source_sha,client_artifact_size_bytes=len(source))
    components=[source,canonical(readback),bundle,canonical(task),*profiles]
    digests=[hashlib.sha256(value).hexdigest() for value in components]
    envelope=dict(schema=verify.INPUT_SCHEMA,**{name:base64.b64encode(body).decode('ascii')
        for (name,_),body in zip(verify.COMPONENTS,components)})
    return envelope,digests,components


class PureReadbackVerifierTests(unittest.TestCase):
    def test_checksum_bound_memory_replay_has_no_io_or_native_entry(self):
        envelope,digests,parts=synthetic_stream()
        with ExitStack() as guards:
            for target,name in ((builtins,'open'),(os,'open'),(socket,'socket'),(subprocess,'Popen')):
                guards.enter_context(mock.patch.object(target,name,side_effect=AssertionError('replay attempted I/O')))
            actual=verify.replay_stream(fixture.CORE['cleanup_json_bytes'](envelope),digests)
        self.assertEqual(actual['readback_binding'],'passed')
        self.assertEqual(actual['reader_artifact_sha256'],digests[0])
        self.assertEqual(actual['native_task_sha256'],digests[3])
        self.assertEqual(actual['native_bundle_sha256'],digests[2])
        self.assertNotIn('native_replay',actual)
        self.assertNotIn('grants_retirement',actual)
        core=verify.load_core(parts[0],digests[0])
        self.assertFalse(hasattr(core,'stitch_build'));self.assertFalse(hasattr(core,'stitch_readback_run'))

    def test_each_component_hash_and_canonical_envelope_is_required(self):
        envelope,digests,_=synthetic_stream();raw=fixture.CORE['cleanup_json_bytes'](envelope)
        for index in range(len(digests)):
            changed=digests[:];changed[index]='0'*64
            with self.subTest(index=index),self.assertRaises(verify.ReadbackError):verify.replay_stream(raw,changed)
        for mutate in (lambda value:value.update(schema='unknown'),lambda value:value.update(extra='SYNTHETIC'),
                lambda value:value.update(native_task_b64=value['native_task_b64']+'='),
                lambda value:value.update(native_bundle_b64=True)):
            value=copy.deepcopy(envelope);mutate(value)
            with self.assertRaises(verify.ReadbackError):verify.replay_stream(fixture.CORE['cleanup_json_bytes'](value),digests)
        duplicate=b'{"schema":"duplicate",'+raw[1:]
        with self.assertRaises(verify.ReadbackError):verify.replay_stream(duplicate,digests)
        with self.assertRaises(verify.ReadbackError):verify.replay_stream(b'x'*(verify.INPUT_LIMIT+1),digests)

    def test_resealed_readback_substitutions_fail_the_actual_native_relationship(self):
        envelope,digests,parts=synthetic_stream()
        for mutate in (lambda value:value.update(native_run_id=fixture.ID),
                lambda value:value.update(native_bundle_sha256='0'*64),
                lambda value:value.update(native_registration_sha256='0'*64),
                lambda value:value['outputs'][0].update(sha256='0'*64),
                lambda value:value.update(profile_pair_sha256='0'*64),
                lambda value:value['reader_execution'].update(client_artifact_size_bytes=1),
                lambda value:value.update(started_at='1999-01-01T00:00:00Z'),
                lambda value:value.update(extra=True)):
            readback=json.loads(parts[1]);mutate(readback)
            raw=fixture.CORE['cleanup_json_bytes'](readback)
            changed=copy.deepcopy(envelope);changed['readback_receipt_b64']=base64.b64encode(raw).decode()
            pins=digests[:];pins[1]=hashlib.sha256(raw).hexdigest()
            with self.subTest(mutate=mutate),self.assertRaises((verify.ReadbackError,ValueError)):
                verify.replay_stream(fixture.CORE['cleanup_json_bytes'](changed),pins)

    def test_absent_new_reader_core_refuses_without_old_native_or_path_fallback(self):
        envelope,digests,parts=synthetic_stream()
        source=parts[0].replace(b'def stitch_readback_profiles(',b'def unsupported_readback_profiles(')
        self.assertNotEqual(source,parts[0])
        envelope['reader_source_b64']=base64.b64encode(source).decode();digests[0]=hashlib.sha256(source).hexdigest()
        with self.assertRaisesRegex(verify.ReadbackError,'unsupported reader verifier revision'):
            verify.replay_stream(fixture.CORE['cleanup_json_bytes'](envelope),digests)

    def test_exact_isolated_c_argv_stdout_and_failure_contract(self):
        envelope,digests,_=synthetic_stream();raw=fixture.CORE['cleanup_json_bytes'](envelope)
        argv=[sys.executable,'-I','-S','-B','-c',SCRIPT.decode(),'replay-readback-stream',*digests]
        self.assertLess(len(SCRIPT),65536)
        result=subprocess.run(argv,input=raw,env={},stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        self.assertEqual(result.stderr,b'')
        expected=verify.replay_stream(raw,digests)
        self.assertEqual(result.stdout,(json.dumps(expected,sort_keys=True,separators=(',',':'))+'\n').encode())
        for wrong in (argv[:-1],[*argv[:6],'collect',*digests]):
            result=subprocess.run(wrong,input=raw,env={},stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
            self.assertEqual(result.returncode,1);self.assertEqual(result.stdout,b'')
            self.assertEqual(result.stderr,b'readback replay refused\n')


if __name__=='__main__':
    unittest.main()
