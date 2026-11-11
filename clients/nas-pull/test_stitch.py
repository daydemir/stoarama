"""Fake-NAS integration: real ffmpeg, synthetic media, real cleanup unlink path.

The synthetic policy adapter replaces corpus lookup only; synthetic identities
cannot enter the production pinned export. No fixture uses real NAS data.
"""
import datetime
import hashlib
import importlib.util
import itertools
import json
import os
import shutil
import subprocess
import sys
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
ORIGINAL_CHECKPOINT = pull.stitch_checkpoint


class StitchSwitchTests(unittest.TestCase):
    def config(self, enabled=None, deletion=None):
        env = {}
        if enabled is not None:
            env['STOARAMA_V85_STITCH_ENABLED'] = enabled
        if deletion is not None:
            env['STOARAMA_V85_STITCH_DELETION_ENABLED'] = deletion
        with mock.patch.dict(os.environ, env, clear=True):
            return pull.Config()

    def test_env_vetoes_and_server_flags_resolve_independently_each_cycle(self):
        for local, local_delete, remote, remote_delete in itertools.product(
                (None, 'false', 'true'), (None, 'false', 'true'),
                (None, False, True), (None, False, True)):
            with self.subTest(local=local, local_delete=local_delete, remote=remote, remote_delete=remote_delete):
                cfg = self.config(local, local_delete)
                self.assertFalse(cfg.stitch_enabled)
                self.assertFalse(cfg.stitch_delete_enabled)
                # Start ON to prove missing flags revoke previous authority.
                cfg.stitch_switches.apply(dict(ok=True, stitch_enabled=True, stitch_deletion_enabled=True))
                response = dict(ok=True)
                if remote is not None:
                    response['stitch_enabled'] = remote
                if remote_delete is not None:
                    response['stitch_deletion_enabled'] = remote_delete
                cfg.stitch_switches.apply(response)
                enabled = local != 'false' and remote is True
                self.assertEqual(cfg.stitch_enabled, enabled)
                self.assertEqual(cfg.stitch_delete_enabled,
                                 enabled and local_delete != 'false' and remote_delete is True)

    def test_unknown_or_failed_response_revokes_authority(self):
        cfg = self.config()
        on = dict(ok=True, stitch_enabled=True, stitch_deletion_enabled=True)
        for response in (None, [], {}, dict(on, ok=False), dict(on, ok='true'),
                         dict(on, stitch_enabled='true'), dict(on, stitch_enabled=1)):
            cfg.stitch_switches.apply(on)
            cfg.stitch_switches.apply(response)
            self.assertFalse(cfg.stitch_enabled)
            self.assertFalse(cfg.stitch_delete_enabled)
        for value in ('true', 1, None):
            cfg.stitch_switches.apply(dict(on, stitch_deletion_enabled=value))
            self.assertTrue(cfg.stitch_enabled)
            self.assertFalse(cfg.stitch_delete_enabled)

    def test_heartbeat_applies_switches_on_every_cycle_and_transport_failure(self):
        cfg = self.config()
        runtime = SimpleNamespace(inventory=None, apply_joined_protocol_response=mock.Mock())
        on = dict(ok=True, stitch_enabled=True, stitch_deletion_enabled=True)
        responses = [on, dict(on, stitch_enabled=False), on, {}, on, ConnectionError('offline')]
        expected = [True, False, True, False, True, False]
        stop = threading.Event()
        def wait(_):
            enabled = expected.pop(0)
            self.assertEqual((cfg.stitch_enabled, cfg.stitch_delete_enabled), (enabled, enabled))
            self.assertEqual(runtime.stitch_enabled, enabled)
            if not expected:
                stop.set()
        with tempfile.TemporaryDirectory() as root, \
             mock.patch.object(pull, 'load_outage', return_value=None), \
             mock.patch.object(pull, 'send_heartbeat', side_effect=responses) as send, \
             mock.patch.object(stop, 'wait', side_effect=wait):
            cfg.outage_file = Path(root)/'outage.json'
            pull.heartbeat_loop(cfg, runtime, stop)
        self.assertEqual(send.call_count, len(responses))

    def test_off_blocks_new_leases_including_after_maintenance_wait(self):
        cfg = self.config()
        stop = threading.Event()
        inventory = mock.MagicMock()
        inventory.db.execute.return_value.fetchone.return_value = None
        on = dict(ok=True, stitch_enabled=True)
        def pause(_):
            cfg.stitch_switches.apply(dict(ok=True, stitch_enabled=False))
            return True
        with mock.patch.object(pull, 'restore_api') as api, \
             mock.patch.object(pull, 'stitch_journal'), \
             mock.patch.object(pull, 'stitch_resources'), \
             mock.patch.object(pull.NAS_MEDIA_FENCE, 'wait_maintenance', side_effect=pause):
            self.assertEqual(pull.stitch_once(cfg, inventory, stop), 30)
            cfg.stitch_switches.apply(on)
            self.assertEqual(pull.stitch_once(cfg, inventory, stop), 30)
            api.assert_not_called()

    def test_server_off_cancels_live_child_even_if_server_turns_back_on(self):
        cfg = self.config()
        on = dict(ok=True, stitch_enabled=True)
        cfg.stitch_switches.apply(on)
        stop = pull.StitchStop(cfg, threading.Event())
        started, errors = threading.Event(), []
        popen = subprocess.Popen
        def launch(*args, **kwargs):
            child = popen(*args, **kwargs)
            started.set()
            return child
        def run():
            try:
                pull.stitch_run([sys.executable, '-c', 'import time; time.sleep(30)'],
                                time.monotonic()+40, stop)
            except Exception as exc:
                errors.append(exc)
        # This test exercises cancellation on macOS too, without Linux ionice.
        with mock.patch.object(pull.sys, 'platform', 'darwin'), \
             mock.patch.object(pull.subprocess, 'Popen', side_effect=launch):
            worker = threading.Thread(target=run)
            worker.start()
            try:
                self.assertTrue(started.wait(5))
                cfg.stitch_switches.apply(dict(ok=True, stitch_enabled=False))
                cfg.stitch_switches.apply(on)
                worker.join(5)
                self.assertFalse(worker.is_alive())
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], pull.BenchmarkError)
                self.assertEqual(str(errors[0]), 'client shutting down')
                self.assertEqual(stop.reason, 'stitch switch off')
                self.assertFalse(pull.StitchStop(cfg, threading.Event()).is_set())
            finally:
                stop.stop.set()
                worker.join(5)


class StitchRepairTests(unittest.TestCase):
    def test_truncated_nal_length_without_payload_retains_verified_prefix(self):
        media = b'\x00\x00\x00\x00mdat\x00\x00\x00\x02\x65\x01\x00\x00\x00\x10'
        nals, reason, gaps, gap_indices = pull.stitch_repair_mdat(media)
        self.assertEqual(nals, [b'\x65\x01'])
        self.assertEqual(reason, 'partial')
        self.assertEqual((gaps, gap_indices), (0, []))


class StitchPublicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.cfg = SimpleNamespace(output_dir=root/'clips', state_dir=root/'state', min_free_bytes=1)
        self.cfg.output_dir.mkdir(); self.cfg.state_dir.mkdir()
        self.stop = threading.Event()

    def directory(self):
        return pull.StitchDirectory(self.cfg, 'stream_V85r_keep28/22/hour', 7, self.stop)

    def test_all_output_kinds_use_shared_quarantine_and_startup_reporting(self):
        with self.directory() as directory:
            for name in ('run.mp4', 'run.mp4.proof.json', 'run.mp4.registered.json',
                         'repaired.mp4', 'repaired.mp4.repair.json'):
                (directory.path/'scratch').write_bytes(b'verified output')
                directory.publish('scratch', name)
                self.assertEqual((directory.path/name).read_bytes(), b'verified output')
                self.assertEqual((directory.path/name).stat().st_nlink, 1)
                self.assertFalse((directory.path/'scratch').exists())
        aliases = list((self.cfg.output_dir/'.stoarama-quarantine/aliases').iterdir())
        self.assertEqual(len(aliases), 5)
        self.assertTrue(all(p.read_bytes() == b'verified output' for p in aliases))
        records = [json.loads(line) for line in (self.cfg.state_dir/'joined-alias-quarantine.jsonl').read_text().splitlines()]
        self.assertEqual(sum(r['operation'] == 'canonical_copy' and r['state'] == 'complete' for r in records), 5)
        notice, _, status = pull.joined_quarantine_startup_status(self.cfg)
        self.assertIn('aliases=5', notice)
        self.assertEqual(status['alias_count'], 5)
        self.assertEqual(status['pending_recovery_count'], 0)
        self.assertTrue(all(str(self.cfg.output_dir/'stream_V85r_keep28/22/hour') in r['canonical_path']
                            for r in records if r['operation'] == 'canonical_copy'))

    def test_interrupted_link_replays_without_writing_through_canonical(self):
        with self.directory() as directory:
            (directory.path/'scratch').write_bytes(b'original bytes')
            with mock.patch.object(pull, 'quarantine_joined_aliases', side_effect=OSError('simulated crash')):
                with self.assertRaisesRegex(OSError, 'simulated crash'):
                    directory.publish('scratch', 'run.mp4')
            original = (directory.path/'run.mp4').stat().st_ino
            self.assertFalse((directory.path/'scratch').exists())
            (directory.path/'scratch').write_bytes(b'original bytes')
            directory.publish('scratch', 'run.mp4')
            self.assertNotEqual((directory.path/'run.mp4').stat().st_ino, original)
            self.assertEqual((directory.path/'run.mp4').stat().st_nlink, 1)
            self.assertEqual((directory.path/'run.mp4').read_bytes(), b'original bytes')
        aliases = list((self.cfg.output_dir/'.stoarama-quarantine/aliases').iterdir())
        self.assertEqual(len(aliases), 2)
        self.assertTrue(all(p.read_bytes() == b'original bytes' for p in aliases))

    def test_copy_ledger_failure_replays_with_all_bytes_preserved(self):
        append = pull.append_joined_quarantine_ledger
        def fail(cfg, record):
            if record['operation'] == 'canonical_copy' and record['state'] == 'intent':
                raise OSError('ledger unavailable')
            append(cfg, record)
        with self.directory() as directory:
            (directory.path/'scratch').write_bytes(b'original bytes')
            with mock.patch.object(pull, 'append_joined_quarantine_ledger', side_effect=fail):
                with self.assertRaisesRegex(pull.ExistingFileMismatch, 'ledger unavailable'):
                    directory.publish('scratch', 'run.mp4')
            self.assertEqual((directory.path/'run.mp4').read_bytes(), b'original bytes')
            (directory.path/'scratch').write_bytes(b'original bytes')
            directory.publish('scratch', 'run.mp4')
            self.assertEqual((directory.path/'run.mp4').stat().st_nlink, 1)
        self.assertTrue(all(p.read_bytes() == b'original bytes'
                            for p in self.cfg.output_dir.rglob('*') if p.is_file()))

    def test_unknown_and_canonical_aliases_refuse_publication(self):
        for outside in (False, True):
            with self.subTest(outside=outside), self.directory() as directory:
                name = 'outside.mp4' if outside else 'canonical.mp4'
                (directory.path/name).write_bytes(b'original bytes')
                alias = self.cfg.output_dir/'unknown-link' if outside else directory.path/'another.mp4'
                os.link(directory.path/name, alias)
                (directory.path/'scratch').write_bytes(b'original bytes')
                with self.assertRaises(pull.ExistingFileMismatch):
                    directory.publish('scratch', name)
                self.assertEqual(alias.read_bytes(), b'original bytes')
                self.assertEqual((directory.path/name).read_bytes(), b'original bytes')


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
                      mock.patch.object(pull,'stitch_resources'), mock.patch.object(pull,'stitch_checkpoint'), mock.patch.object(pull,'stitch_v85r_provenance', return_value=dict(policy='v85r',manifest_sha256=pull.V85R_MANIFEST_SHA,ffmpeg_version='fixture',ffprobe_version='fixture'))]:
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

    def test_switch_off_during_output_rehash_keeps_raw(self):
        result=self.build(); task=self.cleanup_task(result)
        with mock.patch.object(pull,'restore_api',side_effect=self.api): pull.stitch_register(self.cfg,self.inventory,7,result)
        real=pull.hash_certification_file
        def flip(*a,**k):
            out=real(*a,**k); self.cfg.stitch_delete_enabled=False; return out
        with mock.patch.object(pull,'hash_certification_file',side_effect=flip):
            self.assertIn('switch off',pull.stitch_output_recheck(self.cfg,task['item']))
        self.cfg.stitch_delete_enabled=True
        with mock.patch.object(pull,'hash_certification_file',side_effect=flip), \
                mock.patch.object(pull,'restore_api',side_effect=self.api):
            pull.cleanup_process_task(self.cfg,self.inventory,self.stop,task)
        self.assertTrue((self.cfg.output_dir/self.clips[0]['relative_path']).exists())

    def test_switch_off_at_unlink_boundary_keeps_raw(self):
        result=self.build(); task=self.cleanup_task(result)
        with mock.patch.object(pull,'restore_api',side_effect=self.api): pull.stitch_register(self.cfg,self.inventory,7,result)
        real=pull.cleanup_local_caps
        def flip(*a,**k):
            out=real(*a,**k)
            if len(a)>2: self.cfg.stitch_delete_enabled=False
            return out
        with mock.patch.object(pull,'cleanup_local_caps',side_effect=flip), \
                mock.patch.object(pull,'restore_api',side_effect=self.api):
            pull.cleanup_process_task(self.cfg,self.inventory,self.stop,task)
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

    def test_long_stitch_build_cannot_hold_cleanup_execution_lane(self):
        result = self.build()
        with mock.patch.object(pull, 'restore_api', side_effect=self.api):
            pull.stitch_register(self.cfg, self.inventory, 7, result)
        building, resume = threading.Event(), threading.Event()
        errors = []
        original = pull.stitch_audit_source
        def slow_source(*args):
            building.set()
            if not resume.wait(5):
                raise RuntimeError('test did not release slow native tool')
            return original(*args)
        def build_again():
            try:
                self.build()
            except Exception as exc:
                errors.append(exc)
        lane = pull.CleanupExecutionLane(self.stop)
        with mock.patch.object(pull, 'stitch_audit_source', side_effect=slow_source), mock.patch.object(pull, 'restore_api', side_effect=self.api):
            worker = threading.Thread(target=build_again)
            worker.start()
            try:
                self.assertTrue(building.wait(5))
                self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
                # A real proven-inclusion retirement finishes while the native
                # build is deliberately still blocked, rather than timing out.
                pull.cleanup_process_task(self.cfg, self.inventory, self.stop, self.cleanup_task(result), lane=lane)
                self.assertFalse((self.cfg.output_dir/self.clips[0]['relative_path']).exists())
                self.assertTrue(worker.is_alive())
                resume.set()
                worker.join(5)
                self.assertFalse(worker.is_alive())
                self.assertEqual(len(errors), 1)
                self.assertIsInstance(errors[0], pull.StitchHold)
            finally:
                resume.set()
                worker.join(5)
                lane.close()
        for clip in self.clips[1:]:
            self.assertTrue((self.cfg.output_dir/clip['relative_path']).exists())

    def test_cleanup_lane_cancels_running_native_child_without_waiting_for_it(self):
        started, errors = threading.Event(), []
        stop = pull.StitchStop(self.cfg, self.stop)
        def run_child():
            started.set()
            try:
                # Native child watchdog uses the same stop object as FFmpeg.
                pull.stitch_run([sys.executable, '-c', 'import time; time.sleep(30)'], time.monotonic()+40, stop)
            except Exception as exc:
                errors.append(exc)
        worker = threading.Thread(target=run_child)
        worker.start()
        self.assertTrue(started.wait(5))
        lane = pull.CleanupExecutionLane(self.stop)
        try:
            lane.run(lambda: None)
            worker.join(5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(len(errors), 1)
            self.assertEqual(stop.reason, 'cleanup execution lane active')
        finally:
            lane.close()
            self.stop.set()
            worker.join(5)

    def test_stitch_stop_fails_closed_on_remote_freeze_or_unavailable_checkpoint(self):
        # Exercise the real checkpoint rather than the fixture's default stub.
        # The API contract itself is covered below through the original module
        # function saved before fixture patches are installed.
        for response in ({'continue': False}, {}):
            with mock.patch.object(pull, 'restore_api', return_value=response):
                with self.assertRaises(pull.StitchHold):
                    ORIGINAL_CHECKPOINT(self.cfg, self.task)
        with mock.patch.object(pull, 'restore_api', side_effect=ConnectionError('offline')):
            stop = pull.StitchStop(self.cfg, self.stop, self.task)
            with mock.patch.object(pull, 'stitch_checkpoint', side_effect=ORIGINAL_CHECKPOINT):
                stop.refresh()
                self.assertTrue(stop.is_set())
                self.assertIn('offline', stop.reason)

    def test_symlink_output_directory_and_bad_policy_are_refused(self):
        self.task['plan']['policy']='collation-v2'
        with self.assertRaises(pull.StitchHold):self.build()
        self.task['plan']['policy']=pull.STITCH_POLICY
        (self.cfg.output_dir/'stream_V85r_keep28').symlink_to(self.cfg.state_dir,target_is_directory=True)
        with self.assertRaises(OSError):self.build()


if __name__=='__main__': unittest.main()
