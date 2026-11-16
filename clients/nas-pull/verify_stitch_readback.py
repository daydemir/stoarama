#!/usr/bin/env python3
"""Pure readback binding replay; native inventory replay remains pinned81ff.

Only immutable caller-owned byte copies enter this stream protocol. There is no
pathname, credential, network, media, collection or native-activation entrypoint.
Pair acceptance and server/provider freshness belong to the separate caller.
"""
import ast
import base64
import builtins
import datetime
from enum import Enum, IntEnum
import hashlib
import json
import re
import sys
from types import SimpleNamespace
from typing import NamedTuple
import uuid
import zlib


class ReadbackError(ValueError):
    pass


INPUT_SCHEMA = 'stoarama.nas-stitch-readback-replay-input.v1'
INPUT_LIMIT = 16*1024*1024
COMPONENTS = (
    ('reader_source_b64', 8*1024*1024),
    ('readback_receipt_b64', 65536),
    ('native_bundle_b64', 1024*1024),
    ('native_task_b64', 1024*1024),
    ('producer_profile_b64', 65536),
    ('reader_profile_b64', 65536),
    ('profile_pair_b64', 65536),
)
FUNCTIONS = frozenset((
    'cleanup_json_bytes', 'stitch_proof_json', 'stitch_proof_check',
    'stitch_proof_success', 'stitch_proof_unpack', 'stitch_proof_validate',
    'stitch_readback_validate', 'stitch_readback_profiles',
    'stitch_readback_native', 'stitch_readback_registration',
))
CLASSES = frozenset((
    'NASStitchMode', 'StitchReadbackContractError', 'StitchReadbackDefinition',
    'StitchProfileKey', 'StitchClientIdentity', 'StitchProfileBytes',
    'StitchProducerProfile', 'StitchProfilePair', 'StitchReadbackIntegrity',
))
CONSTANTS = frozenset((
    'STITCH_POLICY', 'STITCH_REVISION', 'STITCH_CORPUS_SHA', 'STITCH_PROOF_SOURCE_SHA',
    'STITCH_PROOF_KIND', 'STITCH_PROOF_SCHEMA_JSON', 'STITCH_PROOF_SCHEMA_SHA',
    'STITCH_READBACK_SCHEMA_JSON', 'STITCH_READBACK_SCHEMA_SHA',
    'V85R_MANIFEST_SHA', 'CLEANUP_KEEP28_POLICY_SHA256',
))


def strict_json(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ReadbackError('duplicate JSON field')
            value[key] = item
        return value
    def invalid(value):
        raise ReadbackError('nonfinite JSON value')
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=invalid)


def checked(raw, limit, digest):
    if (type(raw) is not bytes or not 0 < len(raw) <= limit or type(digest) is not str
            or re.fullmatch(r'[a-f0-9]{64}', digest) is None or hashlib.sha256(raw).hexdigest() != digest):
        raise ReadbackError('readback component byte binding differs')
    return raw


def load_core(source, digest):
    tree = ast.parse(checked(source, 8*1024*1024, digest))
    namespace = dict(__name__='pinned_readback_contract', json=json, hashlib=hashlib,
        re=re, datetime=datetime, zlib=zlib, uuid=uuid, Enum=Enum, IntEnum=IntEnum,
        NamedTuple=NamedTuple, StitchHold=ReadbackError)
    declarations, found, constants = [], set(), set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in FUNCTIONS | CLASSES:
            if node.name in found or node.decorator_list:
                raise ReadbackError('duplicate/decorated readback declaration')
            found.add(node.name)
            declarations.append(node)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in CONSTANTS:
                    if target.id in constants:
                        raise ReadbackError('duplicate readback constant')
                    namespace[target.id] = ast.literal_eval(node.value)
                    constants.add(target.id)
    if found != FUNCTIONS | CLASSES or constants != CONSTANTS:
        raise ReadbackError('unsupported reader verifier revision')
    names = ('__build_class__', 'dict', 'list', 'set', 'tuple', 'len', 'str', 'bytes', 'int',
             'float', 'bool', 'range', 'type', 'isinstance', 'any', 'all', 'enumerate', 'zip',
             'iter', 'next', 'max', 'min', 'abs', 'sorted', 'ValueError', 'KeyError', 'Exception')
    namespace['__builtins__'] = {name: getattr(builtins, name) for name in names}
    exec(compile(ast.Module(body=declarations,type_ignores=[]), '<pinned readback contract>', 'exec'), namespace)
    return SimpleNamespace(**namespace)


def replay_stream(raw, digests):
    if type(raw) is not bytes or not 0 < len(raw) <= INPUT_LIMIT or len(digests) != len(COMPONENTS):
        raise ReadbackError('readback input bound/argv')
    envelope = strict_json(raw)
    if (type(envelope) is not dict or set(envelope) != {'schema', *(name for name, _ in COMPONENTS)}
            or envelope['schema'] != INPUT_SCHEMA):
        raise ReadbackError('readback input schema/fields')
    decoded = {}
    for (name, limit), digest in zip(COMPONENTS, digests):
        encoded = envelope[name]
        if type(encoded) is not str or not 0 < len(encoded) <= 4*((limit+2)//3):
            raise ReadbackError('readback component encoding bound/type')
        try:
            body = base64.b64decode(encoded, validate=True)
        except ValueError:
            raise ReadbackError('invalid readback component base64') from None
        if base64.b64encode(body).decode('ascii') != encoded:
            raise ReadbackError('noncanonical readback component base64')
        decoded[name] = checked(body, limit, digest)
    reader_sha, readback_sha, bundle_sha, task_sha, producer_sha, profile_sha, pair_sha = digests
    core = load_core(decoded['reader_source_b64'], reader_sha)
    profiles = core.StitchProfileBytes(*(decoded[name] for name in
        ('producer_profile_b64', 'reader_profile_b64', 'profile_pair_b64')))
    key = core.StitchProfileKey(producer_sha, profile_sha, pair_sha)
    readback = strict_json(decoded['readback_receipt_b64'])
    core.stitch_readback_validate(readback, core.StitchReadbackDefinition.READBACK)
    execution = readback['reader_execution']
    identity = core.StitchClientIdentity(reader_sha, len(decoded['reader_source_b64']), execution['client_version'])
    producer, reader, pair = core.stitch_readback_profiles(profiles, key, identity, core.NASStitchMode.READBACK)
    native = core.stitch_proof_unpack(decoded['native_bundle_b64'])
    task = strict_json(decoded['native_task_b64'])
    core.stitch_readback_native(native, task, producer)
    outputs = [{name: value[name] for name in ('relative_path', 'sha256', 'size_bytes')}
               for value in native['result']['outputs']]
    expected = dict(native_run_id=native['run']['id'],native_bundle_sha256=bundle_sha,
        native_plan_sha256=native['plan_sha256'],native_task_sha256=task_sha,producer_profile_sha256=producer_sha,
        reader_profile_sha256=profile_sha,profile_pair_sha256=pair_sha,outputs=outputs,
        policy=reader['policy'],tools=reader['tools'],
        native_registration_sha256=core.stitch_readback_registration(decoded['native_bundle_b64']))
    if producer['client_version']=='r2-copy-v3':
        expected['native_activation_id']=task['activation_id']
    elif 'native_activation_id' in readback:
        raise ReadbackError('historical producer has no activation UUID')
    if any(readback[name] != value for name, value in expected.items()):
        raise ReadbackError('readback/native relationship differs')
    if execution['client_artifact_sha256'] != reader_sha or execution['client_artifact_size_bytes'] != identity.size_bytes:
        raise ReadbackError('readback executing artifact differs')
    times = [datetime.datetime.fromisoformat(value.replace('Z','+00:00')) for value in
        (native['verified_at'],readback['started_at'],readback['verified_at'])]
    if times != sorted(times):
        raise ReadbackError('readback time ordering differs')
    return dict(schema='stoarama.nas-stitch-readback-replay.v1',readback_binding='passed',
        reader_artifact_sha256=reader_sha,readback_receipt_sha256=readback_sha,
        native_bundle_sha256=bundle_sha,native_task_sha256=task_sha,
        producer_profile_sha256=producer_sha,reader_profile_sha256=profile_sha,profile_pair_sha256=pair_sha,
        native_run_id=native['run']['id'],native_plan_sha256=native['plan_sha256'],
        readback_id=readback['id'],readback_verified_at=readback['verified_at'],
        reader_boot_id=execution['client_boot_id'],output_sha256=outputs[0]['sha256'],
        output_size_bytes=outputs[0]['size_bytes'],native_registration_sha256=expected['native_registration_sha256'])


def main():
    if len(sys.argv) != 9 or sys.argv[1] != 'replay-readback-stream':
        raise ReadbackError('fixed replay-readback-stream argv required')
    result = replay_stream(sys.stdin.buffer.read(INPUT_LIMIT+1), sys.argv[2:])
    sys.stdout.write(json.dumps(result,sort_keys=True,separators=(',',':'))+'\n')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        # Never echo an artifact body, pathname, secret or partially valid result.
        sys.stderr.write('readback replay refused\n')
        raise SystemExit(1)
