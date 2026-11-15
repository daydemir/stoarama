#!/usr/bin/env python3
"""Checksum-pinned, credential-free native receipt replay and operator export."""
import sys
sys.dont_write_bytecode = True
import argparse
import ast
import datetime
from fractions import Fraction
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
from types import SimpleNamespace
import zlib


class ProofError(ValueError):
    pass


def strict_json(raw):
    def unique(pairs):
        value={}
        for key,item in pairs:
            if key in value:raise ProofError('duplicate JSON field')
            value[key]=item
        return value
    def invalid(number):raise ProofError('nonfinite JSON number')
    return json.loads(raw,object_pairs_hook=unique,parse_constant=invalid)


def read_bounded(path,limit,expected_sha=None):
    # The artifact gate also pins every ancestor beneath its supplied directory.
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    try:
        before=os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or not 0<before.st_size<=limit:raise ProofError('artifact file bound/type')
        chunks=[];size=0;digest=hashlib.sha256()
        while True:
            block=os.read(fd,min(1024*1024,limit+1-size))
            if not block:break
            size+=len(block);digest.update(block);chunks.append(block)
            if size>limit:raise ProofError('artifact too large')
        if tuple(getattr(os.fstat(fd),k) for k in ("st_dev","st_ino","st_mode","st_size","st_mtime_ns","st_ctime_ns"))!=tuple(getattr(before,k) for k in ("st_dev","st_ino","st_mode","st_size","st_mtime_ns","st_ctime_ns")) or size!=before.st_size:raise ProofError('artifact changed during read')
        if expected_sha is not None and digest.hexdigest()!=expected_sha:raise ProofError('artifact checksum mismatch')
        return b''.join(chunks)
    finally:os.close(fd)


PURE_FUNCTIONS=frozenset(('cleanup_json_bytes','stitch_signature','stitch_packet_records','stitch_frame_records','stitch_source_records','stitch_output_records','stitch_proof_json','stitch_proof_check','stitch_proof_success','stitch_proof_unpack','stitch_proof_validate'))
PURE_CONSTANTS=frozenset(('STITCH_POLICY','STITCH_REVISION','STITCH_CORPUS_SHA','STITCH_PROOF_SOURCE_SHA','STITCH_PROOF_KIND','STITCH_PROOF_AUDIT_NAMES','STITCH_PROOF_SCHEMA_JSON','STITCH_PROOF_SCHEMA_SHA','V85R_MANIFEST_SHA','V85R_TOOL_VERSION','V85R_TOOLS','CLEANUP_KEEP28_POLICY_SHA256'))


def load_core(client_source,expected_sha):
    # Do not import the NAS app: that would initialize its diagnostics/auth readers.
    # Only these reviewed pure declarations execute, without file/network/auth APIs.
    tree=ast.parse(read_bounded(client_source,8*1024*1024,expected_sha))
    namespace={'json':json,'hashlib':hashlib,'Fraction':Fraction,'zlib':zlib,'re':re,'datetime':datetime,'StitchHold':ProofError}
    declarations=[];found=set()
    for node in tree.body:
        if isinstance(node,ast.FunctionDef) and node.name in PURE_FUNCTIONS:
            if node.name in found or node.decorator_list:raise ProofError('duplicate/decorated verifier function')
            found.add(node.name);declarations.append(node)
        elif isinstance(node,(ast.Assign,ast.AnnAssign)):
            targets=node.targets if isinstance(node,ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target,ast.Name) and target.id in PURE_CONSTANTS:
                    if target.id in namespace:raise ProofError('duplicate verifier constant')
                    namespace[target.id]=ast.literal_eval(node.value)
    if found!=PURE_FUNCTIONS or not PURE_CONSTANTS<=namespace.keys():raise ProofError('unsupported client verifier revision')
    safe={k:__builtins__[k] if isinstance(__builtins__,dict) else getattr(__builtins__,k) for k in ('dict','list','set','len','str','int','float','bool','range','type','any','all','enumerate','zip','iter','next','max','min','abs','sorted','tuple','ValueError','KeyError','Exception')}
    namespace['__builtins__']=safe
    exec(compile(ast.Module(body=declarations,type_ignores=[]),'<pinned native inventory verifier>','exec'),namespace)
    return SimpleNamespace(**namespace)


def reassemble(logs,plan_sha,expected_sha):
    chunks={};count=None
    pattern=re.compile(r'^V85r proof-v1 ([0-9a-f]{64}) ([0-9a-f]{64}) ([0-9]+)/([0-9]+) ([0-9a-f]+)$')
    for row in logs['lines']:
        line=row['message']
        if not line.startswith('V85r proof-v1 '+plan_sha+' '+expected_sha+' '):continue
        match=pattern.fullmatch(line)
        if not match:raise ProofError('malformed/redacted/truncated proof transport')
        index,total=int(match[3]),int(match[4]);part=match[5]
        if not 1<=total<=2048 or not 0<=index<total or len(part)>1024 or len(part)%2:raise ProofError('proof chunk bound')
        if count is not None and total!=count:raise ProofError('conflicting proof census')
        if index in chunks and chunks[index]!=part:raise ProofError('conflicting proof chunk')
        count=total;chunks[index]=part
    if count is None or set(chunks)!=set(range(count)):raise ProofError('incomplete proof receipt')
    body=bytes.fromhex(''.join(chunks[n] for n in range(count)))
    if hashlib.sha256(body).hexdigest()!=expected_sha:raise ProofError('proof receipt digest mismatch')
    return body


def verify(core,task,body,client_sha):
    core.stitch_proof_check(task)
    receipt=core.stitch_proof_unpack(body)
    if receipt['plan_sha256']!=task['plan_sha256'] or receipt['client_artifact_sha256']!=client_sha:raise ProofError('proof plan/pipeline mismatch')
    result=receipt['result'];output=result['outputs'][0]
    if output['relative_path']!=task['plan']['output_folder']+'/proof.mp4':raise ProofError('output namespace differs')
    if output['readback_sha256']!=output['sha256'] or output['size_bytes']<=0:raise ProofError('output readback mismatch')
    if not all(output.get(k) is True for k in ('full_decode','seam_decode','packet_chain','native_audit','original_sources_no_audio','fsynced')):raise ProofError('incomplete output audit')
    provenance=output['provenance']
    if (provenance['policy']!=core.STITCH_POLICY or provenance['manifest_sha256']!=core.V85R_MANIFEST_SHA
            or provenance['ffmpeg_sha256']!=core.V85R_TOOLS['ffmpeg'][0] or provenance['ffprobe_sha256']!=core.V85R_TOOLS['ffprobe'][0]
            or provenance['ffmpeg_version']!=core.V85R_TOOL_VERSION or provenance['ffprobe_version']!=core.V85R_TOOL_VERSION
            or provenance.get('october_ffmpeg_recovered') is not True):raise ProofError('unqualified proof toolchain')
    if len(receipt['measurements'])!=15:raise ProofError('native proof stage census differs')
    commands=[x['argv'] for x in receipt['measurements']]
    if sum('-ss' in c for c in commands)!=1 or not any('-c:v' in c and 'copy' in c for c in commands):raise ProofError('missing remux/seeked seam decode')
    if sum('-xerror' in c and '-err_detect' in c for c in commands)!=4:raise ProofError('missing strict source/output/seam decodes')
    files=receipt['audits'];sources=[]
    for n,(clip,proof) in enumerate(zip(task['plan']['clips'],output['inputs'])):
        if proof['sha256']!=clip['sha256'] or proof['size_bytes']!=clip['size_bytes']:raise ProofError('source identity mismatch')
        prefix='source-%03d'%n;signature=core.stitch_signature(strict_json(files[prefix+'.json'])['streams'])
        if files[prefix+'.packets']!=files[prefix+'.hidden']:raise ProofError('source hidden inventory differs')
        packets=lambda text=files[prefix+'.packets']:core.stitch_packet_records(io.StringIO(text))
        frames=lambda text=files[prefix+'.frames']:core.stitch_frame_records(io.StringIO(text))
        source=core.stitch_source_records(signature,packets,frames)
        if source['count']!=proof['packets'] or proof['frames']!=proof['packets'] or source['chain']!=proof['packet_chain_sha256']:raise ProofError('source native receipt differs')
        sources.append(source)
    signature=core.stitch_signature(strict_json(files['output.json'])['streams'])
    if any(s['signature']!=signature for s in sources):raise ProofError('codec identity differs')
    if files['output.packets']!=files['output.hidden']:raise ProofError('output hidden inventory differs')
    core.stitch_output_records(signature,sources,core.stitch_packet_records(io.StringIO(files['output.packets'])),core.stitch_frame_records(io.StringIO(files['output.frames'])))
    decision=strict_json(files['v85r-decision.json']);seam=result['seams'][0]
    if any(decision.get(k)!=seam.get(k) for k in ('decision','status','nomination','join','diagnostic_sha256')):raise ProofError('fresh policy decision receipt differs')
    if abs(float(sum(source['span'] for source in sources))-output['media_seconds'])>0.000001:raise ProofError('media timing receipt differs')
    return receipt


def write_new(path,body):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        with os.fdopen(fd,'wb',closefd=False) as out:out.write(body);out.flush();os.fsync(fd)
    finally:os.close(fd)


def file_ref(root,name):
    raw=read_bounded(root/name,16*1024*1024)
    return dict(file=name,sha256=hashlib.sha256(raw).hexdigest(),size_bytes=len(raw))


def export_payload(root,response_path,pipeline_path):
    """Export verified observations; independent review/authority is separate."""
    task=strict_json(read_bounded(root/'source-plan.json',1024*1024))
    source=root/'client.py';sha=file_ref(root,'client.py')['sha256'];core=load_core(source,sha)
    bundle=read_bounded(root/'receipt.json.gz',1024*1024)
    receipt=verify(core,task,bundle,sha)
    response=strict_json(read_bounded(response_path,64*1024))
    pipeline=strict_json(read_bounded(pipeline_path,64*1024))
    if (set(pipeline)!={'repository','base_commit_sha','reviewed_head_sha','merged_commit_sha','client_version','client_artifact_sha256'}
            or pipeline['repository']!='daydemir/stoarama' or pipeline['client_artifact_sha256']!=sha or pipeline['client_version']!=receipt['client_version']
            or any(not re.fullmatch('[0-9a-f]{40}',pipeline[k]) for k in ('base_commit_sha','reviewed_head_sha','merged_commit_sha'))):raise ProofError('pipeline release binding differs')
    output=receipt['result']['outputs'][0]
    binding=dict(verified=True,kind=core.STITCH_PROOF_KIND,plan_sha256=task['plan_sha256'],output_sha256=output['sha256'],registered_locally_only=True,grants_retirement=False)
    if (any(type(response.get(k)) is not type(v) or response.get(k)!=v for k,v in binding.items())
            or response.get('receipt_sha256')!=hashlib.sha256(bundle).hexdigest() or response.get('client_artifact_sha256')!=sha or response.get('run_id')!=receipt['run']['id']):raise ProofError('server validation binding differs')
    policy=dict(name=core.STITCH_POLICY,revision=core.STITCH_REVISION,keep28_sha256=core.CLEANUP_KEEP28_POLICY_SHA256,corpus_sha256=core.STITCH_CORPUS_SHA,package_manifest_sha256=core.V85R_MANIFEST_SHA)
    tools={name:dict(version=core.V85R_TOOL_VERSION,sha256=pin,size_bytes=size) for name,(pin,size) in core.V85R_TOOLS.items()}
    media=dict(relative_path=output['relative_path'],sha256=output['sha256'],size_bytes=output['size_bytes'])
    validation=dict(schema='stoarama.nas-stitch-validation.v1',validated_at=response['validated_at'],run_id=receipt['run']['id'],proof_plan_sha256=task['plan_sha256'],client_artifact_sha256=sha,policy=policy,tools=tools,server_binding=binding,nas_readback=dict(verified_at=response['nas_readback_at'],client_boot_id=response['client_boot_id'],run_id=receipt['run']['id'],receipt_sha256=hashlib.sha256(bundle).hexdigest(),outputs=[media]))
    write_new(root/'server-validation.json',core.cleanup_json_bytes(validation))
    evidence={key:file_ref(root,name) for key,name in dict(native_bundle='receipt.json.gz',local_registration='verified-receipt.json',source_plan='source-plan.json',server_validation='server-validation.json',verifier='verifier.py',client_artifact='client.py').items()}
    if read_bounded(root/'verifier.py',1024*1024)!=read_bounded(Path(__file__),1024*1024):raise ProofError('export verifier differs')
    run=dict(receipt['run'],media_seconds=output['media_seconds'],build_wall_seconds=receipt['build_wall_seconds'])
    payload=dict(schema='stoarama.nas-stitch-payload.v1',connection_id=13,account_id=47,scope=dict(stream_id=17208,recording_id=444,source_task_id=1,source_plan_sha256=core.STITCH_PROOF_SOURCE_SHA,proof_plan_sha256=task['plan_sha256'],clip_ids=[373218,373262]),policy=policy,pipeline=pipeline,tools=tools,run=run,inputs=[dict(clip_id=c['source_id'],relative_path=c['relative_path'],sha256=c['sha256'],size_bytes=c['size_bytes']) for c in task['plan']['clips']],outputs=[dict(media,source_clip_ids=[373218,373262],readback_sha256=output['readback_sha256'])],evidence=evidence,verification=dict(current_policy='passed',native_inventory_replay='passed',server_binding='passed',result_sha256=hashlib.sha256(core.cleanup_json_bytes(receipt['result'])).hexdigest()))
    write_new(root/'payload.json',core.cleanup_json_bytes(payload))
    print(json.dumps(file_ref(root,'payload.json')))


def main():
    parser=argparse.ArgumentParser(__doc__);sub=parser.add_subparsers(dest='command',required=True)
    for name in ('collect','replay-bundle'):
        p=sub.add_parser(name);p.add_argument('--plan',type=Path,required=True);p.add_argument('--client-source',type=Path,required=True);p.add_argument('--expected-client-sha256',required=True);p.add_argument('--expected-sha256',required=True)
        if name=='collect':p.add_argument('--logs',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
        else:p.add_argument('--bundle',type=Path,required=True)
    p=sub.add_parser('export-payload');p.add_argument('--artifact-dir',type=Path,required=True);p.add_argument('--server-response',type=Path,required=True);p.add_argument('--pipeline',type=Path,required=True)
    args=parser.parse_args()
    if args.command=='export-payload':return export_payload(args.artifact_dir,args.server_response,args.pipeline)
    core=load_core(args.client_source,args.expected_client_sha256)
    task=strict_json(read_bounded(args.plan,1024*1024))
    body=(reassemble(strict_json(read_bounded(args.logs,16*1024*1024)),task['plan_sha256'],args.expected_sha256) if args.command=='collect' else read_bounded(args.bundle,1024*1024,args.expected_sha256))
    receipt=verify(core,task,body,args.expected_client_sha256)
    if args.command=='collect':
        args.out.mkdir(parents=True,exist_ok=False)
        write_new(args.out/'receipt.json.gz',body);write_new(args.out/'verified-receipt.json',core.cleanup_json_bytes(receipt))
        write_new(args.out/'source-plan.json',read_bounded(args.plan,1024*1024));write_new(args.out/'client.py',read_bounded(args.client_source,8*1024*1024,args.expected_client_sha256));write_new(args.out/'verifier.py',read_bounded(Path(__file__),1024*1024))
        result=dict(receipt['result']);result['outputs']=[{k:v for k,v in o.items() if k!='media_seconds'} for o in result['outputs']]
        write_new(args.out/'result.json',core.cleanup_json_bytes(dict(result=result,receipt_sha256=args.expected_sha256,run_id=receipt['run']['id'],client_artifact_sha256=receipt['client_artifact_sha256'])))
        fd=os.open(args.out,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)
    media=receipt['result']['outputs'][0]['media_seconds'];wall=receipt['build_wall_seconds']
    print(json.dumps(dict(schema='stoarama.nas-stitch-native-replay.v1',native_replay='passed',connection_id=13,run_id=receipt['run']['id'],receipt_sha256=args.expected_sha256,plan_sha256=task['plan_sha256'],client_artifact_sha256=args.expected_client_sha256,output_sha256=receipt['result']['outputs'][0]['sha256'],output_size_bytes=receipt['result']['outputs'][0]['size_bytes'],media_seconds=media,build_wall_seconds=wall,recording_hours_per_wall_hour=media/wall)))

if __name__=='__main__':main()
