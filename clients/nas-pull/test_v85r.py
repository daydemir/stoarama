"""Pins, recovered classifier replay, conservative fallback and actual Linux producer."""
import copy
import gzip
import hashlib
import io
import threading
import importlib.util
import json
import os
from pathlib import Path
import platform
import subprocess
import sqlite3
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('v85r_pull', ROOT/'stoarama_pull.py')
pull = importlib.util.module_from_spec(spec); spec.loader.exec_module(pull)
spec = importlib.util.spec_from_file_location('v85r_runner', ROOT/'v85r/runner.py')
runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)


class V85rTests(unittest.TestCase):
    def test_all_packaged_files_match_reviewed_pins_and_installed_bytes(self):
        raw=(ROOT/'v85r/manifest.json').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),pull.V85R_MANIFEST_SHA)
        manifest=json.loads(raw)
        self.assertEqual(manifest['policy'],'v85r')
        with tempfile.TemporaryDirectory() as state:
            installed=pull.stitch_v85r_bundle(state)
            for name,pin in manifest['files'].items():
                self.assertEqual(hashlib.sha256((installed/name).read_bytes()).hexdigest(),pin,name)
                if name!='remeasure-linux-amd64':
                    self.assertEqual(hashlib.sha256((ROOT/'v85r'/name).read_bytes()).hexdigest(),pin,name)
            (installed/'runner.py').write_text('tampered')
            with self.assertRaisesRegex(pull.StitchHold,'installed V85r member hash'):
                pull.stitch_v85r_bundle(state)

    def test_bundle_tamper_and_wrong_ffmpeg_refused(self):
        with tempfile.TemporaryDirectory() as state, mock.patch.object(pull,'V85R_BUNDLE',('0' if pull.V85R_BUNDLE[0]!='0' else '1')+pull.V85R_BUNDLE[1:]):
            with self.assertRaises((pull.StitchHold,ValueError)):
                pull.stitch_v85r_bundle(state)
        with self.assertRaises(pull.StitchHold):
            pull.stitch_v85r_provenance(dict(ffmpeg=__file__,ffprobe=__file__))

    def test_tool_download_installs_only_hash_verified_bytes(self):
        from types import SimpleNamespace
        body=b'synthetic pinned executable'
        sha=hashlib.sha256(body).hexdigest()
        capability=dict(sha256=sha,size_bytes=len(body),url='https://tools.invalid/artifact',if_match='"fixture"')
        with tempfile.TemporaryDirectory() as state, mock.patch.object(pull.sys,'platform','linux'), \
             mock.patch('platform.machine',return_value='x86_64'), mock.patch.object(pull,'V85R_TOOLS',{'ffmpeg':(sha,len(body))}), \
             mock.patch.object(pull,'request_json',return_value=capability), mock.patch.object(pull.urllib.request,'urlopen',return_value=io.BytesIO(body)):
            tools=pull.stitch_v85r_tools(SimpleNamespace(state_dir=state),time.monotonic()+20,threading.Event())
            self.assertEqual(Path(tools['ffmpeg']).read_bytes(),body)
        with tempfile.TemporaryDirectory() as state, mock.patch.object(pull.sys,'platform','linux'), \
             mock.patch('platform.machine',return_value='x86_64'), mock.patch.object(pull,'V85R_TOOLS',{'ffmpeg':(sha,len(body))}), \
             mock.patch.object(pull,'request_json',return_value=capability), mock.patch.object(pull.urllib.request,'urlopen',return_value=io.BytesIO(b'tampered')):
            with self.assertRaises(pull.StitchHold):
                pull.stitch_v85r_tools(SimpleNamespace(state_dir=state),time.monotonic()+20,threading.Event())
            self.assertEqual(list(Path(state).rglob('ffmpeg')),[])

    def recorded_rows(self):
        raw = (ROOT/'testdata/v85r-october-evidence.json.gz').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), 'e62739b52a73cdd10dd20ab8d023aab801f0efbb1ef165cf8bba23bf9ce374e2')
        rows = json.loads(gzip.decompress(raw))['rows']
        self.assertEqual(len(rows), 14)
        return rows

    def policy(self):
        c = runner.classifier()
        bad, human = c.C.load_vetoes(ROOT/'v85r/source71.json', ROOT/'v85r/human12.json')
        return c, bad, human

    def test_complete_october_recorded_evidence_replays_through_production_adapter(self):
        c, bad, human = self.policy()
        for case in self.recorded_rows():
            row = case['row']
            with self.subTest(case=case['case']):
                self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                                 tuple(case['expected']))

    def stationary_row(self):
        row = copy.deepcopy(self.recorded_rows()[0]['row'])
        spatial = row['spatial_temporal_diagnostic']
        spatial.update(tail_dx_pixels_per_frame=0, tail_dy_pixels_per_frame=0,
                       head_dx_pixels_per_frame=0, head_dy_pixels_per_frame=0,
                       motion_observable=False, ambiguities=['no_observable_motion'],
                       texture_gradient_mad=3, minimum_spatial_novelty_margin=2, heldout_pixels=1000,
                       forward_heldout_residual_mad=10, backward_heldout_residual_mad=10,
                       internal_heldout_residual_mad=.1, direction_speed_disagreement=0,
                       max_acceleration_pixels_per_frame_squared=0, boundary_unwarped_mad=10)
        row['spatial_provenance']['diagnostic_sha256'] = hashlib.sha256(json.dumps(
            spatial, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        return row

    def test_stationary_successor_veto_reaches_adapter_and_runtime(self):
        c, bad, human = self.policy()
        row = self.stationary_row()
        result = c.decision(row, bad, human)
        self.assertTrue(result['baselineK_nomination'])
        self.assertEqual(result['status'], 'REJECT_stationary_severe_seam_forecast')
        self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                         (False, result['status']))
        request = dict(prev=row['source_bindings'][0], next=row['source_bindings'][1],
                       tools=dict(ffmpeg='fixture', ffprobe='fixture'))
        # The live producer projects bindings to these three identity fields.
        row['source_bindings'] = [dict(clip_id=b['clip_id'], sha256=b['sha256'], size_bytes=b['size_bytes'])
                                  for b in row['source_bindings']]
        row['spatial_provenance']['source_bindings'] = row['source_bindings']
        native = dict(evidence_row=row, diagnostic_window20=row['diagnostic_window20'])
        with mock.patch.object(runner.subprocess, 'run', return_value=mock.Mock(stdout=json.dumps(native))):
            decision = runner.run(request)
        self.assertEqual(decision['status'], 'REJECT_stationary_severe_seam_forecast')
        self.assertEqual(decision['decision'], 'split')
        self.assertFalse(decision['join'])

    def test_missing_or_unauthenticated_evidence_holds_a_baseline_positive(self):
        c, bad, human = self.policy()
        original = self.recorded_rows()[0]['row']
        for field in ('full_source_integrity', 'pair', 'spatial_temporal_diagnostic', 'spatial_provenance',
                      'research_packet_replay', 'research_audio_inventory'):
            row = copy.deepcopy(original)
            del row[field]
            self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                             (False, 'UNKNOWN_missing_authenticated_evidence'), field)
        for field in ('motion_observable', 'ambiguities', 'heldout_pixels', 'forward_heldout_residual_mad'):
            row = copy.deepcopy(original)
            del row['spatial_temporal_diagnostic'][field]
            row['spatial_provenance']['diagnostic_sha256'] = hashlib.sha256(json.dumps(
                row['spatial_temporal_diagnostic'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                             (False, 'UNKNOWN_missing_authenticated_evidence'), field)
        for mutate in (lambda r: r['full_source_integrity']['contexts'][0].update(VerifiedProofSHA='0'*64),
                       lambda r: r['spatial_provenance'].update(diagnostic_sha256='0'*64),
                       lambda r: r['spatial_provenance'].update(original_endpoint_frames_preserved=False),
                       lambda r: r['pair']['prev'].update(sha256='0'*64)):
            row = copy.deepcopy(original)
            mutate(row)
            self.assertFalse(runner.decide(c, row, row['source_bindings'], bad, human, set())[0])

    def test_spatial_positive_research_route_never_promotes(self):
        c, bad, human = self.policy()
        row = self.stationary_row()
        spatial = row['spatial_temporal_diagnostic']
        spatial.update(tail_dx_pixels_per_frame=1, head_dx_pixels_per_frame=1,
                       motion_observable=True, ambiguities=[],
                       forward_heldout_residual_mad=.1, backward_heldout_residual_mad=.1)
        row['spatial_provenance']['diagnostic_sha256'] = hashlib.sha256(json.dumps(
            spatial, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        row['diagnostic_window20']['frame_match'].update(tail_frames=99, head_frames=99)
        for context in row['full_source_integrity']['contexts']:
            context.update(FramePTS=[i*.04 for i in range(1500)], LastFrameDuration=.04,
                           RegisteredCaptureSpan=60)
        # Supply complete matching capture identity, required by that research lane.
        for n, side in enumerate(('prev', 'next')):
            row['pair'][side].update(recording_id=1, recording_job_id=1,
                                     capture_lease_token='fixture', capture_sequence=n+1)
        raw = c.decision(row, bad, human)
        self.assertTrue(raw['spatial_nomination'])
        self.assertFalse(raw['baselineK_nomination'])
        self.assertEqual(runner.decide(c, row, row['source_bindings'], bad, human, set()),
                         (False, 'SPLIT_frozen_score_reject'))

    def test_conservative_fallback(self):
        keys=[[1,'a'*64,10],[2,'b'*64,11]]
        frozen=dict(source_key=keys,nomination=True,status=runner.APPROVED)
        def decide(**kw):
            return runner.resolve(True,runner.APPROVED,keys,**kw)
        self.assertEqual(decide(frozen=frozen)['reason'],'exact_october')
        self.assertTrue(decide()['join'])
        for change in [dict(source_key=[[9,'a'*64,10],keys[1]]),dict(status='UNKNOWN_october'),dict(nomination=False)]:
            self.assertEqual(decide(frozen=dict(frozen,**change))['decision'],'hold')
        for kw in [dict(missing=True),dict(decoder_errors=True)]:
            self.assertEqual(decide(frozen=frozen,**kw)['decision'],'hold')
        for status in ['UNKNOWN_missing','HOLD_native','RESEARCH_nomination','unrecognized']:
            self.assertEqual(runner.resolve(True,status,keys)['decision'],'hold')
        self.assertEqual(runner.resolve(False,'SPLIT_frozen_score_reject',keys)['decision'],'split')
        self.assertTrue(runner.has_decode_error(dict(native=dict(decode_error='failed'))))
        self.assertTrue(runner.has_decode_error(dict(decode_errors=1)))

    @unittest.skipUnless(os.environ.get('V85R_FROZEN_CORPUS'),'exact recovered JSONL required')
    def test_client_indexes_entire_exact_corpus_including_unknown_rows(self):
        import threading
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as state:
            filename=str(Path(state)/'inventory.sqlite3')
            db=sqlite3.connect(filename)
            self.addCleanup(db.close)
            inventory=SimpleNamespace(lock=threading.RLock(),db=db)
            cfg=SimpleNamespace(inventory_file=filename,api_base='https://api.invalid',api_key='fixture')
            with mock.patch.object(pull.urllib.request,'urlopen',side_effect=lambda *a,**kw:open(os.environ['V85R_FROZEN_CORPUS'],'rb')):
                pull.stitch_corpus(cfg,inventory)
            self.assertEqual(db.execute('SELECT count(*) FROM v85r_stitch_edges').fetchone()[0],73843)
            self.assertEqual(db.execute('SELECT count(*) FROM v85r_stitch_edges WHERE nomination=1').fetchone()[0],52462)

    @unittest.skipUnless(platform.system()=='Linux' and platform.machine() in ('x86_64','amd64') and os.environ.get('V85R_TEST_TOOLS'),
                         'Linux amd64 with pinned V85R_TEST_TOOLS required')
    def test_actual_packaged_linux_producer_and_frozen_status_hold(self):
        tools={name:str(Path(os.environ['V85R_TEST_TOOLS'])/name) for name in ('ffmpeg','ffprobe')}
        provenance=pull.stitch_v85r_provenance(tools)
        self.assertEqual(provenance['ffmpeg_version'],'N-126826-gc0e8b139fd-20260924')
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp);bundle=pull.stitch_v85r_bundle(work)
            media=work/'source.mp4'
            subprocess.run([tools['ffmpeg'],'-nostdin','-v','error','-f','lavfi','-i','testsrc2=size=128x96:rate=25:duration=60',
                            '-c:v','libx264','-preset','ultrafast','-threads','1','-bf','0','-g','25','-pix_fmt','yuv420p',str(media)],check=True)
            sha=hashlib.sha256(media.read_bytes()).hexdigest()
            a=dict(clip_id=1,recording_id=1,recording_job_id=1,sha256=sha,size_bytes=media.stat().st_size,
                   nominal_seconds=60,start_utc='2026-09-01T00:00:00Z',end_utc='2026-09-01T00:01:00Z')
            b=dict(a,clip_id=2,start_utc=a['end_utc'],end_utc='2026-09-01T00:02:00Z')
            request=dict(prev=a,next=b,prev_path=str(media),next_path=str(media),tools=tools,actual_span=False)
            def measure():
                (work/'request.json').write_text(json.dumps(request))
                p=subprocess.run(['python3','-I','-B',str(bundle/'runner.py'),str(work/'request.json')],capture_output=True,text=True,check=True,timeout=400)
                return json.loads(p.stdout)
            produced = subprocess.run([str(bundle/'remeasure-linux-amd64')], input=json.dumps(request),
                                      capture_output=True, text=True, check=True, timeout=200,
                                      env=dict(os.environ, V85R_FFMPEG=tools['ffmpeg'], V85R_FFPROBE=tools['ffprobe']))
            evidence = json.loads(produced.stdout)['evidence_row']
            self.assertEqual(evidence['source_bindings'], [dict(clip_id=z['clip_id'], sha256=z['sha256'],
                             size_bytes=z['size_bytes']) for z in (a, b)])
            self.assertEqual(evidence['spatial_provenance']['within_side_frame_counts'], [6, 6])
            self.assertTrue(evidence['spatial_provenance']['presentation_order_verified'])
            self.assertEqual(evidence['spatial_temporal_diagnostic']['status'], 'diagnostic_only')
            for context in evidence['full_source_integrity']['contexts']:
                self.assertTrue(context['StrictDecodePassed'])
                self.assertEqual(len(context['FramePTS']), 1500)
                self.assertEqual(context['OwnerPinnedProofSHA'], context['VerifiedProofSHA'])
            actual=measure()
            self.assertIn('diagnostic_sha256',actual) # cannot pass through an import/measurement failure
            self.assertNotEqual(actual['reason'],'measurement_failed')
            request['frozen']=dict(source_key=[[z['clip_id'],sha,z['size_bytes']] for z in (a,b)],
                                   nomination=actual['nomination'],status='UNKNOWN_october_status_difference')
            self.assertEqual(measure()['decision'],'hold')


if __name__=='__main__':unittest.main()
