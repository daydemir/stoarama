"""Isolated proof authority/transport/inventory tests; never production proof."""
import copy
import base64
import gzip
import hashlib
import importlib.util
import json
import os
import subprocess
from pathlib import Path
import sys
import tempfile
import threading
import urllib.error
from types import SimpleNamespace
import unittest
from unittest import mock

spec=importlib.util.spec_from_file_location('verify_proof',Path(__file__).with_name('verify_stitch_proof.py'))
verify=importlib.util.module_from_spec(spec);spec.loader.exec_module(verify)
spec=importlib.util.spec_from_file_location('test_proof_client',Path(__file__).with_name('stoarama_pull.py'))
pull=importlib.util.module_from_spec(spec);spec.loader.exec_module(pull)


def synthetic_receipt():
    """Mechanical replay fixture only: invented inventories, never NAS evidence."""
    clips=[dict(source_id=clip,relative_path='synthetic/%d.mp4'%clip,sha256=sha,size_bytes=size) for clip,sha,size in zip(
        (373218,373262),('7bfea81fc9dd6fc83d15759b061a05b82f9b7a70a618e728d9a4e490a0534505','600d2998f6799386ccf07be1687074e3a761babdd435ad0f79ce068fa7f166a4'),(11371311,11249068))]
    plan=dict(policy=pull.STITCH_POLICY,revision=pull.STITCH_REVISION,corpus_sha256=pull.STITCH_CORPUS_SHA,keep_policy_sha256=pull.CLEANUP_KEEP28_POLICY_SHA256,output_folder="synthetic-proof",clips=clips,joins=[True],eligible=[True,True])
    task=dict(kind=pull.STITCH_PROOF_KIND,id=1,source_plan_sha256=pull.STITCH_PROOF_SOURCE_SHA,plan=plan,plan_sha256=hashlib.sha256(pull.cleanup_json_bytes(plan)).hexdigest())
    probe=json.dumps(dict(streams=[dict(codec_type='video',codec_name='h264',codec_tag_string='avc1',width=10,height=10,pix_fmt='yuv420p',time_base='1/1',extradata_hash='SHA256:'+'a'*64)]))
    def packet(pts):return 'pts=%d|dts=%d|duration=60|size=1|flags=K__|data_hash=SHA256:%s\n'%(pts,pts,'b'*64)
    def frame(pts):return '0,0,%d,60,1,%s\n'%(pts,'c'*64)
    audits={};inputs=[]
    for n,c in enumerate(clips):
        prefix='source-%03d'%n
        audits.update({prefix+'.json':probe,prefix+'.packets':packet(0),prefix+'.hidden':packet(0),prefix+'.frames':frame(0)})
        inputs.append(dict(repaired=False,retirement_held=False,raw_proof={'synthetic':True},clip_id=c['source_id'],sha256=c['sha256'],size_bytes=c['size_bytes'],packets=1,frames=1,packet_chain_sha256=hashlib.sha256(('1:SHA256:'+'b'*64+'\n').encode()).hexdigest()))
    seam=dict(policy=pull.STITCH_POLICY,frozen=True,reason='synthetic_fixture',left_id=373218,right_id=373262,decision='join',join=True,nomination=True,status='APPROVED85_nomination_requires_runtime_native_join_checks',diagnostic_sha256='d'*64)
    audits.update({'output.json':probe,'output.packets':packet(0)+packet(60),'output.hidden':packet(0)+packet(60),'output.frames':frame(0)+frame(60),'v85r-decision.json':json.dumps(seam)})
    provenance=dict(policy=pull.STITCH_POLICY,manifest_sha256=pull.V85R_MANIFEST_SHA,ffmpeg_sha256=pull.V85R_TOOLS['ffmpeg'][0],ffprobe_sha256=pull.V85R_TOOLS['ffprobe'][0],ffmpeg_version=pull.V85R_TOOL_VERSION,ffprobe_version=pull.V85R_TOOL_VERSION,october_ffmpeg_recovered=True)
    output=dict(policy=pull.STITCH_POLICY,relative_path='synthetic-proof/proof.mp4',sha256='e'*64,readback_sha256='e'*64,size_bytes=1,inputs=inputs,provenance=provenance,media_seconds=120,**{k:True for k in ('full_decode','seam_decode','packet_chain','native_audit','original_sources_no_audio','fsynced')})
    result=dict(policy=pull.STITCH_POLICY,plan_sha256=task['plan_sha256'],outputs=[output],seams=[seam],repairs=[],left_out=[],error='')
    commands=[['runner.py']]+[['ffprobe']]*6+[['ffmpeg','-xerror','-err_detect','explode']]*3+[['ffmpeg','-xerror','-err_detect','explode','-ss','58']]+[['ffmpeg','-c:v','copy']]+[['ffprobe']]*3
    receipt=dict(schema='stoarama.nas-stitch-native-receipt.v1',connection_id=13,account_id=47,kind=pull.STITCH_PROOF_KIND,source_plan_sha256=pull.STITCH_PROOF_SOURCE_SHA,plan_sha256=task['plan_sha256'],run=dict(id='00000000-0000-4000-8000-000000000001',client_boot_id='00000000-0000-4000-8000-000000000002',host=dict(connection_id=13,system='Linux',machine='x86_64',cpu_model='SYNTHETIC',cpu_count=1),started_at='2000-01-01T00:00:00Z',completed_at='2000-01-01T00:00:15Z'),client_artifact_sha256='f'*64,client_version=pull.CLIENT_VERSION,result=result,audits=audits,measurements=[dict(argv=c,returncode=0,wall_s=1,cpu_s=1) for c in commands],build_wall_seconds=15,verified_at='2000-01-01T00:00:15Z')
    return task,receipt


class ProofTests(unittest.TestCase):
    def stream_fixture(self):
        # Explicitly synthetic/stale native inventories: never production authority.
        source=Path(__file__).with_name('stoarama_pull.py').read_bytes()
        client_sha=hashlib.sha256(source).hexdigest()
        task,receipt=synthetic_receipt();receipt['client_artifact_sha256']=client_sha
        plan=pull.cleanup_json_bytes(task);bundle=gzip.compress(pull.cleanup_json_bytes(receipt),mtime=0)
        envelope=dict(schema=verify.REPLAY_INPUT_SCHEMA,client_source_b64=base64.b64encode(source).decode(),source_plan_b64=base64.b64encode(plan).decode(),native_bundle_b64=base64.b64encode(bundle).decode())
        return envelope,(client_sha,hashlib.sha256(bundle).hexdigest(),hashlib.sha256(plan).hexdigest()),(source,plan,bundle)

    def test_replay_stream_uses_only_checksum_bound_memory(self):
        envelope,pins,parts=self.stream_fixture()
        with mock.patch.object(verify,'read_bounded',side_effect=AssertionError('no path read')),mock.patch.object(verify,'load_core',side_effect=AssertionError('no path loader')):
            actual=verify.replay_stream(pull.cleanup_json_bytes(envelope),*pins)
        core=verify.load_core_bytes(parts[0],pins[0]);task=verify.strict_json(parts[1])
        receipt=verify.verify(core,task,parts[2],pins[0])
        self.assertEqual(actual,verify.replay_result(receipt,task,pins[0],pins[1]))
        self.assertEqual(actual['media_seconds'],120)

    def test_replay_stream_rejects_hash_encoding_schema_and_inventory_changes(self):
        envelope,pins,parts=self.stream_fixture()
        raw=pull.cleanup_json_bytes(envelope)
        for n in range(3):
            wrong=list(pins);wrong[n]='0'*64
            with self.assertRaises(verify.ProofError):verify.replay_stream(raw,*wrong)
        mutations=[
            lambda e:e.__setitem__('schema','unknown'),
            lambda e:e.__setitem__('extra',True),
            lambda e:e.pop('native_bundle_b64'),
            lambda e:e.__setitem__('source_plan_b64',None),
            lambda e:e.__setitem__('source_plan_b64',''),
            lambda e:e.__setitem__('source_plan_b64','%%%'),
            lambda e:e.__setitem__('source_plan_b64','Zg==='),
            lambda e:e.__setitem__('source_plan_b64','Zh=='), # Nonzero pad bits.
            lambda e:e.__setitem__('source_plan_b64','é'),
            lambda e:e.__setitem__('source_plan_b64','A'*(4*((1024*1024+2)//3)+1)),
            lambda e:e.__setitem__('source_plan_b64',base64.b64encode(b'x'*(1024*1024+1)).decode()),
        ]
        for change in mutations:
            bad=copy.deepcopy(envelope);change(bad)
            with self.assertRaises((verify.ProofError,ValueError)):verify.replay_stream(pull.cleanup_json_bytes(bad),*pins)
        for bad in (b'{}',b'[]',b'null',b'',b' '*(verify.REPLAY_INPUT_MAX_BYTES+1),raw[:-1]+b',"schema":"duplicate"}',raw[:-1]+b',"x":NaN}'):
            with self.assertRaises((verify.ProofError,ValueError)):verify.replay_stream(bad,*pins)
        receipt=verify.strict_json(gzip.decompress(parts[2]));receipt['audits']['output.frames']=receipt['audits']['output.frames'].splitlines()[0]+'\n'
        bad_bundle=gzip.compress(pull.cleanup_json_bytes(receipt),mtime=0)
        bad=dict(envelope,native_bundle_b64=base64.b64encode(bad_bundle).decode())
        with self.assertRaises(verify.ProofError):verify.replay_stream(pull.cleanup_json_bytes(bad),pins[0],hashlib.sha256(bad_bundle).hexdigest(),pins[2])
        receipt=verify.strict_json(gzip.decompress(parts[2]));receipt['build_wall_seconds']=1e-320
        bad_bundle=gzip.compress(pull.cleanup_json_bytes(receipt),mtime=0)
        bad=dict(envelope,native_bundle_b64=base64.b64encode(bad_bundle).decode())
        with self.assertRaises(verify.ProofError):verify.replay_stream(pull.cleanup_json_bytes(bad),pins[0],hashlib.sha256(bad_bundle).hexdigest(),pins[2])

    def test_isolated_inline_stream_exec_survives_original_paths_replaced(self):
        envelope,pins,parts=self.stream_fixture()
        script=Path(__file__).with_name('verify_stitch_proof.py').read_bytes()
        self.assertLessEqual(len(script),64*1024)
        self.assertNotIn(b'\0',script)
        # The consumer snapshots bytes before execve. Later pathname contents
        # cannot select the interpreter program or any of its native inputs.
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for name,body in zip(('verifier.py','client.py','plan.json','receipt.gz'),(script,*parts)):
                path=root/name;path.write_bytes(body);path.write_bytes(b'changed after checksum; must never reopen')
            (root/'json.py').write_text('raise RuntimeError("cwd import forbidden")')
            before={p.name:p.read_bytes() for p in root.iterdir()}
            argv=[sys.executable,'-I','-S','-B','-c',script.decode('utf-8'),'replay-stream','--expected-client-sha256',pins[0],'--expected-sha256',pins[1],'--expected-plan-sha256',pins[2]]
            run=subprocess.run(argv,input=pull.cleanup_json_bytes(envelope),cwd=root,env={},capture_output=True,timeout=20)
            self.assertEqual(run.returncode,0,run.stderr);self.assertEqual(run.stderr,b'')
            actual=json.loads(run.stdout);self.assertEqual(actual['native_replay'],'passed')
            self.assertEqual(set(actual),{'schema','native_replay','connection_id','run_id','receipt_sha256','plan_sha256','client_artifact_sha256','output_sha256','output_size_bytes','media_seconds','build_wall_seconds','recording_hours_per_wall_hour'})
            self.assertEqual({p.name:p.read_bytes() for p in root.iterdir()},before)
            bad=subprocess.run(argv,input=b'{"schema":"unknown"}',cwd=root,env={},capture_output=True,timeout=20)
            self.assertNotEqual(bad.returncode,0);self.assertEqual(bad.stdout,b'')

    def test_transport_ack_recovers_eviction_lost_ack_and_restart(self):
        # Synthetic maximum-sized-ish data; delivery cannot depend on ring phase.
        body=os.urandom(800*1024);digest=hashlib.sha256(body).hexdigest();plan='a'*64
        persisted=[];requests=[];failed=False
        def server(cfg,method,path,**kw):
            nonlocal failed
            self.assertEqual((method,path),('POST','/account/connections/client-logs'))
            lines=kw['body']['lines'];self.assertLessEqual(len(lines),64)
            persisted.extend(lines);requests.append(lines)
            if not failed:
                failed=True
                raise urllib.error.URLError('synthetic ACK lost after commit')
            return dict(ok=True,stored_lines=len(lines))
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(pull,'CLIENT_LOG_RING',pull.ClientLogRing(max_lines=2,max_bytes=4096)),mock.patch.object(pull,'request_json',side_effect=server),mock.patch.object(pull,'log'):
            cfg=SimpleNamespace(state_dir=Path(tmp));task=dict(plan_sha256=plan)
            with self.assertRaises(urllib.error.URLError):pull.stitch_proof_emit(cfg,task,body,'synthetic-boot')
            self.assertFalse(list(Path(tmp).iterdir())) # Failed ACK cannot advance.
            for cycle in range(3):
                # A new cfg/ring models restart; cursor alone survives.
                for minute in range(25):
                    pull.CLIENT_LOG_RING.record('ERROR','background eviction '*200)
                    pull.stitch_proof_emit(SimpleNamespace(state_dir=Path(tmp)),task,body,'restarted-boot')
                self.assertEqual(verify.reassemble(dict(lines=persisted),plan,digest),body)
            self.assertEqual([x["message"] for x in requests[0]],[x["message"] for x in requests[1]]) # Lost ACK retries exact chunk bytes.
            self.assertGreater(pull.CLIENT_LOG_RING.dropped,0)
            state=json.loads(next(Path(tmp).iterdir()).read_text());before=dict(state)
            with mock.patch.object(pull,'request_json',return_value=dict(ok=True,stored_lines='64')):
                with self.assertRaises(pull.StitchHold):pull.stitch_proof_emit(cfg,task,body,'restarted-boot')
            self.assertEqual(json.loads(next(Path(tmp).iterdir()).read_text()),before)

    def test_candidate_or_wrong_executing_artifact_cannot_claim_proof(self):
        stop=threading.Event()
        with self.assertRaises(pull.StitchHold):pull.stitch_proof_client(SimpleNamespace(is_candidate=True),stop)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'client.py';path.write_bytes(b'synthetic source')
            cfg=SimpleNamespace(is_candidate=False,current_file=path)
            with mock.patch.object(pull,'executing_release_sha256',return_value='0'*64):
                with self.assertRaises(pull.StitchHold):pull.stitch_proof_client(cfg,stop)
            sha=hashlib.sha256(path.read_bytes()).hexdigest()
            with mock.patch.object(pull,'executing_release_sha256',return_value=sha):self.assertEqual(pull.stitch_proof_client(cfg,stop),sha)

    def test_embedded_receipt_schema_matches_frozen_file(self):
        raw=Path(__file__).with_name('stitch-proof').joinpath('native-receipt.schema.json').read_bytes()
        self.assertEqual(raw,pull.STITCH_PROOF_SCHEMA_JSON.encode())
        self.assertEqual(hashlib.sha256(raw).hexdigest(),pull.STITCH_PROOF_SCHEMA_SHA)

    def test_exact_pinned_source_replays_and_native_corruption_fails(self):
        source=Path(__file__).with_name('stoarama_pull.py');sha=hashlib.sha256(source.read_bytes()).hexdigest()
        core=verify.load_core(source,sha)
        task,receipt=synthetic_receipt();receipt['client_artifact_sha256']=sha
        def replay(value):return verify.verify(core,task,gzip.compress(pull.cleanup_json_bytes(value)),sha)
        self.assertEqual(replay(receipt)['result']['outputs'][0]['media_seconds'],120)
        for change in [
            lambda r:r['audits'].__setitem__('output.frames',r['audits']['output.frames'].splitlines()[0]+'\n'),
            lambda r:r['audits'].__setitem__('output.hidden',r['audits']['output.hidden']+'extra'),
            lambda r:r['audits'].__setitem__('source-000.packets',r['audits']['source-000.packets'].replace('duration=60','duration=0')),
            lambda r:r['audits'].__setitem__('v85r-decision.json','{}'),
            lambda r:r['measurements'][0].__setitem__('returncode',1),
            lambda r:r['result']['outputs'][0].__setitem__('media_seconds',119),
            lambda r:r.__setitem__('client_artifact_sha256','0'*64),
            lambda r:r['result']['outputs'][0]['inputs'][0].__setitem__('repaired',True),
            lambda r:r['result'].__setitem__('plan_sha256','0'*64),
            lambda r:r['result']['outputs'][0].__setitem__('unknown',True),
            lambda r:r['run']['host'].__setitem__('system','Darwin'),
            lambda r:r['run']['host'].__setitem__('machine','arm64'),
            lambda r:r['run'].__setitem__('started_at','2000-01-01T00:00:16Z'),
            lambda r:r.__setitem__('unknown',True)]:
            bad=copy.deepcopy(receipt);change(bad)
            with self.assertRaises((verify.ProofError,ValueError)):replay(bad)
        with self.assertRaises(verify.ProofError):verify.load_core(source,'0'*64)

    def test_isolated_replay_cli_has_no_writes_or_inherited_credentials(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=Path(__file__).with_name('stoarama_pull.py');sha=hashlib.sha256(source.read_bytes()).hexdigest()
            task,receipt=synthetic_receipt();receipt['client_artifact_sha256']=sha
            (root/'plan.json').write_bytes(pull.cleanup_json_bytes(task));bundle=gzip.compress(pull.cleanup_json_bytes(receipt));(root/'bundle.gz').write_bytes(bundle)
            before={p.name:p.read_bytes() for p in root.iterdir()}
            run=subprocess.run([sys.executable,'-I','-S','-B',str(Path(__file__).with_name('verify_stitch_proof.py')),'replay-bundle','--plan',str(root/'plan.json'),'--client-source',str(source),'--expected-client-sha256',sha,'--bundle',str(root/'bundle.gz'),'--expected-sha256',hashlib.sha256(bundle).hexdigest()],cwd=root,env={},capture_output=True,text=True,timeout=20)
            self.assertEqual(run.returncode,0,run.stderr);self.assertEqual(json.loads(run.stdout)['native_replay'],'passed');self.assertEqual(run.stderr,'')
            self.assertEqual({p.name:p.read_bytes() for p in root.iterdir()},before)

    def test_pure_loader_ignores_application_initialization(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=Path(tmp)/'source.py';source.write_bytes(Path(__file__).with_name('stoarama_pull.py').read_bytes()+b'\nraise RuntimeError("must never import app")\n')
            core=verify.load_core(source,hashlib.sha256(source.read_bytes()).hexdigest());self.assertEqual(core.STITCH_POLICY,'v85r')
            source.write_bytes(source.read_bytes()+b'\ndef stitch_proof_json(raw): return {}\n')
            with self.assertRaises(verify.ProofError):verify.load_core(source,hashlib.sha256(source.read_bytes()).hexdigest())

    def test_collect_exports_exact_sources_and_payload_binding(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=Path(__file__).with_name('stoarama_pull.py');sha=hashlib.sha256(source.read_bytes()).hexdigest()
            task,receipt=synthetic_receipt();receipt['client_artifact_sha256']=sha
            bundle=gzip.compress(pull.cleanup_json_bytes(receipt));digest=hashlib.sha256(bundle).hexdigest()
            (root/'plan.json').write_bytes(pull.cleanup_json_bytes(task))
            chunks=[bundle.hex()[n:n+1024] for n in range(0,len(bundle)*2,1024)]
            logs=dict(lines=[dict(message='V85r proof-v1 %s %s %d/%d %s'%(task['plan_sha256'],digest,n,len(chunks),chunk)) for n,chunk in enumerate(chunks)])
            (root/'logs.json').write_bytes(pull.cleanup_json_bytes(logs));out=root/'artifact'
            run=subprocess.run([sys.executable,'-I','-S','-B',str(Path(__file__).with_name('verify_stitch_proof.py')),'collect','--plan',str(root/'plan.json'),'--client-source',str(source),'--expected-client-sha256',sha,'--logs',str(root/'logs.json'),'--expected-sha256',digest,'--out',str(out)],env={},capture_output=True,text=True,timeout=20)
            self.assertEqual(run.returncode,0,run.stderr);self.assertEqual((out/'client.py').read_bytes(),source.read_bytes());self.assertEqual((out/'receipt.json.gz').read_bytes(),bundle)
            self.assertEqual((out/'verified-receipt.json').read_bytes(),gzip.decompress(bundle))
            response=dict(verified=True,kind=pull.STITCH_PROOF_KIND,plan_sha256=task['plan_sha256'],output_sha256='e'*64,registered_locally_only=True,grants_retirement=False,validated_at='2000-01-01T00:00:00Z',nas_readback_at='2000-01-01T00:00:00Z',client_boot_id='synthetic-boot',run_id=receipt['run']['id'],receipt_sha256=digest,client_artifact_sha256=sha)
            (root/'server.json').write_bytes(pull.cleanup_json_bytes(response));(root/'pipeline.json').write_bytes(pull.cleanup_json_bytes(dict(repository='daydemir/stoarama',base_commit_sha='1'*40,reviewed_head_sha='2'*40,merged_commit_sha='3'*40,client_version=pull.CLIENT_VERSION,client_artifact_sha256=sha)))
            verify.export_payload(out,root/'server.json',root/'pipeline.json')
            payload=json.loads((out/'payload.json').read_text());self.assertEqual(payload['scope']['proof_plan_sha256'],task['plan_sha256'])
            for ref in payload['evidence'].values():
                raw=(out/ref['file']).read_bytes();self.assertEqual(len(raw),ref['size_bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),ref['sha256'])
            self.assertFalse((out/'attestation.json').exists()) # An export cannot approve itself.

    def test_hash_closes_descriptor_if_path_disappears_after_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);path=root/'clip';path.write_bytes(b'abc');opened=[];original_open=os.open;original_read=os.read
            def open_fd(*args,**kwargs):
                fd=original_open(*args,**kwargs);opened.append(fd);return fd
            def read_fd(fd,size):
                data=original_read(fd,size)
                if not data:path.unlink()
                return data
            with mock.patch.object(pull.os,'open',side_effect=open_fd),mock.patch.object(pull.os,'read',side_effect=read_fd):
                with self.assertRaises(FileNotFoundError):pull.hash_certification_file_cancellable(root,'clip',3,hashlib.sha256(b'abc').hexdigest(),threading.Event())
            for fd in opened:
                with self.assertRaises(OSError):os.fstat(fd)

    def test_authority_is_separate_and_off_or_update_latches_cancellation(self):
        with mock.patch.dict(os.environ,{},clear=True):cfg=pull.Config()
        self.assertFalse(cfg.stitch_proof_enabled)
        on=dict(ok=True,stitch_proof_task_id=1,stitch_enabled=False)
        cfg.stitch_switches.apply(on);self.assertTrue(cfg.stitch_proof_enabled)
        self.assertFalse(cfg.stitch_enabled);self.assertFalse(cfg.stitch_delete_enabled)
        old=cfg.stitch_switches.proof[1]
        for bad in [None,{},dict(on,stitch_proof_task_id=True),dict(on,stitch_proof_task_id=2),dict(on,stitch_enabled=True)]:
            cfg.stitch_switches.apply(bad);self.assertTrue(old.is_set());self.assertFalse(cfg.stitch_proof_enabled)
            cfg.stitch_switches.apply(on);old=cfg.stitch_switches.proof[1]
        stop=pull.StitchStop(cfg,threading.Event(),kind=pull.StitchRunKind.PROOF)
        self.assertFalse(stop.is_set());cfg.stitch_switches.apply(None);self.assertTrue(stop.is_set())

    def test_complete_transport_survives_redaction_replay_and_refuses_conflicts(self):
        body=gzip.compress(b'not a production receipt')
        sha=hashlib.sha256(body).hexdigest();plan='a'*64
        parts=[body.hex()[n:n+12] for n in range(0,len(body)*2,12)]
        lines=['V85r proof-v1 %s %s %d/%d %s'%(plan,sha,n,len(parts),part) for n,part in enumerate(parts)]
        logs=dict(lines=[dict(message=pull.redact_log_line(line,None,())) for line in lines])
        self.assertEqual(verify.reassemble(logs,plan,sha),body)
        logs['lines']+=logs['lines'];self.assertEqual(verify.reassemble(logs,plan,sha),body)
        for changed in [logs['lines'][1:], [dict(message=lines[0]+'00')]+logs['lines'][1:], [dict(message=lines[0].replace(parts[0],'[REDACTED]'))]+logs['lines'][1:]]:
            if changed==logs['lines'][1:]:changed=[x for x in changed if x['message']!=lines[0]]
            with self.assertRaises(ValueError):verify.reassemble(dict(lines=changed),plan,sha)

    def test_duplicate_json_and_compression_bombs_fail_closed(self):
        with self.assertRaises(pull.StitchHold):pull.stitch_proof_json('{"x":1,"x":2}')
        for body in [gzip.compress(b'x'*(16*1024*1024+1)),gzip.compress(b'{}')+b'extra']:
            with self.assertRaises(pull.StitchHold):pull.stitch_proof_unpack(body)

    def test_synthetic_policy_callback_cannot_enter_proof(self):
        with self.assertRaisesRegex(pull.StitchHold,'synthetic'):
            pull.stitch_build(None,None,{},None,None,admitted=lambda *args:True,kind=pull.StitchRunKind.PROOF)

    def test_singleton_hold_and_repair_are_not_success(self):
        value=dict(outputs=[dict(inputs=[dict(clip_id=373218),dict(clip_id=373262)])],seams=[dict(decision='join',status='APPROVED85_nomination_requires_runtime_native_join_checks')],repairs=[],left_out=[],error='')
        pull.stitch_proof_success(value)
        for bad in [dict(value,repairs=[{}]),dict(value,left_out=[373218]),dict(value,seams=[dict(decision='hold')]),dict(value,outputs=[dict(inputs=[dict(clip_id=373218)])])]:
            with self.assertRaises(pull.StitchHold):pull.stitch_proof_success(bad)

    def test_cancellable_hash_and_inventory_replay_reject_frame_loss(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);stop=threading.Event();stop.set()
            path=root/'file';path.write_bytes(b'abc')
            with self.assertRaises(pull.StitchHold):pull.stitch_proof_digest(path,stop)
            signature=('h264','avc1',10,10,'yuv420p','1/1','SHA256:'+'a'*64)
            packet='pts=0|dts=0|duration=1|size=1|flags=K__|data_hash=SHA256:'+'b'*64+'\n'
            (root/'packets').write_text(packet);(root/'frames').write_text('0,0,0,1,1,'+'c'*64+'\n')
            source=pull.stitch_source_inventory(signature,root/'packets',root/'frames')
            pull.stitch_output_inventory(signature,[source],root/'packets',root/'frames')
            (root/'missing.frames').write_text('')
            with self.assertRaises(pull.StitchHold):pull.stitch_output_inventory(signature,[source],root/'packets',root/'missing.frames')

    @unittest.skipUnless(sys.platform.startswith('linux'),'actual Linux renameat2 required; exercised by Linux CI')
    def test_atomic_publication_adopts_exact_bytes_without_alias_or_quarantine(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=SimpleNamespace(output_dir=Path(tmp))
            for data in [b'media',b'media',b'conflict']:
                with pull.StitchProofDirectory(cfg,'proof',1,threading.Event()) as directory:
                    (directory.path/'run.partial.mp4').write_bytes(data)
                    if data==b'conflict':
                        with self.assertRaises(pull.StitchHold):directory.publish('run.partial.mp4','proof.mp4')
                    else:directory.publish('run.partial.mp4','proof.mp4')
                self.assertEqual((Path(tmp)/'proof/proof.mp4').read_bytes(),b'media')
                self.assertEqual((Path(tmp)/'proof/proof.mp4').stat().st_nlink,1)
                self.assertFalse((Path(tmp)/'.stoarama-quarantine').exists())

if __name__=='__main__':unittest.main()
