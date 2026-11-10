"""Fake-NAS integration: real ffmpeg, synthetic media, real cleanup unlink path.

The synthetic policy adapter replaces corpus lookup only; synthetic identities
cannot enter the production pinned export. No fixture uses real NAS data.
"""
import datetime
import hashlib
import importlib.util
import json
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

spec = importlib.util.spec_from_file_location('stitch_pull', Path(__file__).with_name('stoarama_pull.py'))
pull = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pull)


@unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'), 'ffmpeg/ffprobe required')
class StitchIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.cfg = SimpleNamespace(output_dir=root/'clips', state_dir=root/'state', inventory_file=root/'state/inventory.sqlite3',
                                   dry_run=False, stitch_enabled=True, stitch_delete_enabled=True, min_free_bytes=1)
        self.cfg.state_dir.mkdir(); (self.cfg.output_dir/'stream/day').mkdir(parents=True)
        self.inventory = pull.Inventory(self.cfg)
        self.addCleanup(self.inventory.close)
        self.tools = dict(ffmpeg=shutil.which('ffmpeg'), ffprobe=shutil.which('ffprobe'))
        self.stop = threading.Event()
        self.clips = []
        for n in range(4):
            path = self.cfg.output_dir/('stream/day/%d.mp4' % n)
            subprocess.run([self.tools['ffmpeg'], '-nostdin', '-v', 'error', '-f', 'lavfi', '-i',
                            'testsrc2=size=64x64:rate=5:duration=%d' % (4 if n==2 else 2), '-c:v', 'libx264', '-threads', '1',
                            '-pix_fmt', 'yuv420p', '-bf', '0', '-x264-params', 'aud=1:repeat-headers=1', str(path)], check=True)
            if n == 2:  # moov at EOF removed: complete original cannot be decoded
                data=bytearray(path.read_bytes()[:path.stat().st_size*3//4]);data[40:44]=b'\0'*4;path.write_bytes(data)
            clip = dict(kind='raw', source_id=n+1, recording_id=22, stream_id=33, source_ids=[n+1],
                        relative_path=str(path.relative_to(self.cfg.output_dir)), size_bytes=path.stat().st_size,
                        sha256=hashlib.sha256(path.read_bytes()).hexdigest(), sidecar_required=True,
                        start_at='2026-09-01T00:00:%02dZ' % (2*n), end_at='2026-09-01T00:00:%02dZ' % (2*n+2),
                        frozen_identity=[0]*5, frozen_sidecar_identity=[0]*5)
            Path(str(path)+'.stoarama.json').write_text(json.dumps(dict(schema_version=1, clip_id=n+1, recording_id=22,
                relative_path=clip['relative_path'], size_bytes=clip['size_bytes'], sha256=clip['sha256'],
                clip_start_at=clip['start_at'], clip_end_at=clip['end_at'])))
            self.clips.append(clip)
        parse = datetime.datetime.fromisoformat
        pins = (dict(folders={'stream':33}, windows={33:(parse('2026-09-01T00:00:00+00:00'),parse('2026-09-29T00:00:00+00:00'),'UTC')}),None)
        for patch in [mock.patch.object(pull,'cleanup_pins',return_value=pins), mock.patch.object(pull.os.path,'ismount',return_value=True),
                      mock.patch.object(pull,'stitch_resources'), mock.patch.object(pull,'stitch_v85r_provenance', return_value=dict(policy='v85r',manifest_sha256=pull.V85R_MANIFEST_SHA,ffmpeg_version='fixture',ffprobe_version='fixture'))]:
            patch.start();self.addCleanup(patch.stop)
        self.task = dict(id=7, lease_token='f'*64, plan_sha256='a'*64,
            expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=2)).isoformat(),
            plan=dict(policy=pull.STITCH_POLICY,revision=pull.STITCH_REVISION,corpus_sha256=pull.STITCH_CORPUS_SHA,
                      keep_policy_sha256=pull.CLEANUP_KEEP28_POLICY_SHA256,stream_id=33,recording_id=22,
                      clips=self.clips,joins=[True,False,False],eligible=[True,True,True,False],
                      output_folder='stream_V85r_keep28/22/2026-09-01T00-00-00Z'))

    def build(self):
        return pull.stitch_build(self.cfg,self.inventory,self.task,self.stop,self.tools,admitted=lambda c,previous=None:True)

    def cleanup_task(self,result,index=0):
        proof=result['outputs'][0]['inputs'][index]
        item=dict(self.clips[index],permit_reason='stitched_included',stitch_task_id=7,
                  stitch_output_path=result['outputs'][0]['relative_path'],stitch_output_sha256=result['outputs'][0]['sha256'],
                  stitch_output_size_bytes=result['outputs'][0]['size_bytes'],frozen_identity=proof['raw_proof']['identity'],
                  frozen_sidecar_identity=proof['raw_proof']['sidecar']['identity'])
        return dict(item_id=index+100,attempt=1,plan_sha256='c'*64,item=item,approved_proof=proof['raw_proof'],expires_at=self.task['expires_at'])

    def api(self,cfg,path,body,timeout=None):
        if path.endswith('/ready'):
            return dict(ready=True,id=7)
        if '/stitch/' in path:
            return dict(recorded=True,id=7)
        digest=hashlib.sha256(json.dumps(body.get('proof')).encode()).hexdigest() if 'proof' in body else body['proof_sha256']
        ack=dict(item_id=int(path.split('/')[-2]),attempt=body['attempt'],plan_sha256=body['plan_sha256'],proof_sha256=digest)
        if path.endswith('/observe'):
            if body['outcome']!='match':
                return dict(ack,action='conflict',delete=False)
            self.assertEqual(timeout,pull.CLEANUP_EXECUTION_OBSERVE_SEC)
            return dict(ack,action='delete_issued',delete=True,expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(seconds=30)).isoformat())
        return dict(ack,ok=True)

    def test_stitch_register_then_delete_only_fully_included(self):
        result=self.build()
        self.assertEqual(result['left_out'],[3,4])
        self.assertEqual(len(result['repairs']),1)
        self.assertFalse(result['repairs'][0]['complete_original'])
        self.assertTrue(result['repairs'][0]['synthesized_timestamps'])
        self.assertEqual(len(result['outputs']),1)
        self.assertEqual([p['clip_id'] for p in result['outputs'][0]['inputs']],[1,2])
        # Even a server mistakenly offering a cleanup task before registration
        # cannot pass the independent local receipt gate.
        with mock.patch.object(pull,'restore_api',side_effect=self.api):
            pull.cleanup_process_task(self.cfg,self.inventory,self.stop,self.cleanup_task(result))
            self.assertTrue((self.cfg.output_dir/self.clips[0]['relative_path']).exists())
            pull.stitch_register(self.cfg,self.inventory,7,result)
            pull.cleanup_process_task(self.cfg,self.inventory,self.stop,self.cleanup_task(result))
        self.assertFalse((self.cfg.output_dir/self.clips[0]['relative_path']).exists())
        for c in self.clips[1:]: self.assertTrue((self.cfg.output_dir/c['relative_path']).exists())
        self.assertTrue((self.cfg.state_dir/pull.CLEANUP_LEDGER_NAME).exists())

    def test_fresh_output_rehash_refuses_tamper_and_local_switch(self):
        result=self.build(); task=self.cleanup_task(result)
        with mock.patch.object(pull,'restore_api',side_effect=self.api): pull.stitch_register(self.cfg,self.inventory,7,result)
        self.cfg.stitch_delete_enabled=False
        self.assertIn('switch off',pull.stitch_output_recheck(self.cfg,task['item']))
        self.cfg.stitch_delete_enabled=True
        output=self.cfg.output_dir/result['outputs'][0]['relative_path'];output.write_bytes(b'corrupt')
        with mock.patch.object(pull,'restore_api',side_effect=self.api): pull.cleanup_process_task(self.cfg,self.inventory,self.stop,task)
        self.assertTrue((self.cfg.output_dir/self.clips[0]['relative_path']).exists())

    def test_crash_after_publication_before_journal_rebuilds_without_raw_loss(self):
        with mock.patch.object(pull,'stitch_journal',side_effect=RuntimeError('simulated crash')):
            with self.assertRaisesRegex(RuntimeError,'simulated crash'): self.build()
        for c in self.clips:self.assertTrue((self.cfg.output_dir/c['relative_path']).exists())
        result=self.build()  # atomic create-only outputs adopted, never replaced
        self.assertEqual(len(result['outputs']),1)
        self.assertEqual(len(list(self.cfg.output_dir.rglob('run-*.mp4'))),1)

    def test_lost_registration_response_reconciles_before_new_lease(self):
        result=self.build()
        with mock.patch.object(pull,'restore_api',side_effect=ConnectionError('lost ACK')):
            with self.assertRaises(ConnectionError):pull.stitch_register(self.cfg,self.inventory,7,result)
        with mock.patch.object(pull,'restore_api',side_effect=self.api) as api:
            pull.stitch_once(self.cfg,self.inventory,self.stop)
        self.assertEqual(api.call_count,2)
        self.assertIsNone(pull.stitch_output_recheck(self.cfg,self.cleanup_task(result)['item']))
        for c in self.clips:self.assertTrue((self.cfg.output_dir/c['relative_path']).exists())

    def test_dry_run_checks_inclusion_while_local_deletion_is_disabled(self):
        result=self.build();task=self.cleanup_task(result)
        with mock.patch.object(pull,'restore_api',side_effect=self.api):pull.stitch_register(self.cfg,self.inventory,7,result)
        self.cfg.stitch_delete_enabled=False
        task.pop('approved_proof')
        self.assertIsNone(pull.cleanup_guard_refusal(task,result['outputs'][0]['inputs'][0]['raw_proof'],self.cfg))
        self.assertIn('switch off',pull.stitch_output_recheck(self.cfg,task['item']))

    def test_v85r_status_hold_splits_output_and_retains_both_raw_endpoints(self):
        self.task['plan']['feature_inputs']=[dict(clip_id=c['source_id']) for c in self.clips]
        held=dict(policy='v85r',join=False,nomination=True,status='APPROVED85_nomination_requires_runtime_native_join_checks',
                  decision='hold',reason='october_decision_difference')
        split=dict(policy='v85r',join=False,nomination=False,status='SPLIT_frozen_score_reject',decision='split',reason='policy_negative')
        with mock.patch.object(pull,'stitch_v85r_measure',side_effect=[held,split,split]):
            result=pull.stitch_build(self.cfg,self.inventory,self.task,self.stop,self.tools)
        self.assertEqual(len(result['outputs']),2)
        self.assertEqual(result['policy'],'v85r')
        for output in result['outputs']:
            self.assertEqual(output['policy'],'v85r')
            self.assertTrue(output['inputs'][0]['retirement_held'])
        with mock.patch.object(pull,'restore_api',side_effect=self.api):
            pull.stitch_register(self.cfg,self.inventory,7,result)
            pull.cleanup_process_task(self.cfg,self.inventory,self.stop,self.cleanup_task(result))
        self.assertTrue((self.cfg.output_dir/self.clips[0]['relative_path']).exists())
        self.assertTrue((self.cfg.output_dir/self.clips[1]['relative_path']).exists())

    def test_symlink_output_directory_and_bad_policy_are_refused(self):
        self.task['plan']['policy']='collation-v2'
        with self.assertRaises(pull.StitchHold):self.build()
        self.task['plan']['policy']=pull.STITCH_POLICY
        (self.cfg.output_dir/'stream_V85r_keep28').symlink_to(self.cfg.state_dir,target_is_directory=True)
        with self.assertRaises(OSError):self.build()


if __name__=='__main__': unittest.main()
