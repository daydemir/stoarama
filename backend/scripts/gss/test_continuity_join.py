import base64
import dataclasses
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import best14_ledger as L  # noqa: E402
import continuity_join as C  # noqa: E402

K = C.KeepReason
VIDEO = ('h264', 'High', '1920', '1080', 'yuv420p', '25/1', '1/12800', 'SHA256:aa')
AUDIO = ('aac', '44100', '1', '1/44100', 'SHA256:bb')
MEDIA = C.Media(VIDEO, AUDIO, 0.0, 300.0, 0.04)
CLOCK = (60338.5, 60338.5, 60338.5)
DATES = ("2026-09-19",) * 3
BEFORE, AFTER = (60338.2,60338.7,60338.2),(60338.8,60338.3,60338.8)
BEFORE_RAW = tuple([t]*3 for t in ('2026-09-19 16:45:38','2026-09-19 16:45:38','2026-09-19 16:45:37'))
AFTER_RAW = tuple([t]*3 for t in ('2026-09-19 16:45:39','2026-09-19 16:45:39','2026-09-19 16:45:40'))
BEFORE_PTS = tuple([int(round((300-d)*10)), '1/10'] for d in C.READ_AT_S)
AFTER_PTS = tuple([int(round(d*10)), '1/10'] for d in C.READ_AT_S)
BEFORE_EDGE = {'raw':BEFORE_RAW[0], 'pts':[29996,'1/100']}
AFTER_EDGE = {'raw':AFTER_RAW[0], 'pts':[0,'1/100']}


def seam(reason='frame_jump', gap=0.0, before=CLOCK, after=CLOCK, ratio=0.9, prev=MEDIA, nxt=MEDIA, span=300.0, before_dates=DATES, after_dates=DATES):
    return C.Seam(reason, gap, span, prev, nxt, tuple(before), tuple(after), ratio, before_dates, after_dates)


class RuleTests(unittest.TestCase):
    def test_clock_agreeing_seam_joins(self):
        self.assertEqual(C.decide(seam()), (True, None, 0.0))
        self.assertFalse(C.decide(seam(after=(60339.5,) * 3))[0])  # touching half-open intervals cannot prove continuity

    def test_clock_disagreeing_seam_never_joins(self):
        for after in ((60458.5,) * 3, (60218.5,) * 3, (60340.0,) * 3):  # 120 s skip, 120 s back, 1.5 s
            for reason in ('frame_jump', 'packet_replay', 'frame_overlap', 'frame_low_motion'):
                for ratio in (0.0, 0.5, 1.49):
                    self.assertEqual(C.decide(seam(reason, after=after, ratio=ratio))[:2], (False, K.CLOCK_DISAGREES))

    def test_frame_jump_without_clock_proof_never_joins(self):
        for reason in ('frame_jump', 'packet_replay'):
            for before, after, why in (((), (), K.CLOCK_UNREADABLE), ((None, None, None), CLOCK, K.CLOCK_UNREADABLE),
                                       ((60338.5, None, None), CLOCK, K.CLOCK_UNREADABLE),
                                       (CLOCK, (60338.5, None, None), K.CLOCK_UNREADABLE),
                                       ((60338.5, 60341.0, 60343.5), CLOCK, K.CLOCK_INCONSISTENT),
                                       ((60338.5, 60340.5, None), CLOCK, K.CLOCK_UNREADABLE)):
                self.assertEqual(C.decide(seam(reason, before=before, after=after, ratio=0.1))[:2], (False, why))

    def test_one_misread_among_three_is_not_outvoted(self):
        self.assertEqual(C.side_offset((60338.5, 60400.5, 60338.5)), (None, K.CLOCK_INCONSISTENT))
        self.assertEqual(C.decide(seam(before=(60338.5, 60344.5, 60338.5)))[:2], (False, K.CLOCK_INCONSISTENT))
        self.assertEqual(C.decide(seam(after=(60458.5, 60338.5, 60458.5)))[:2], (False, K.CLOCK_INCONSISTENT))  # the contradictory reading stays split
        self.assertEqual(C.side_offset((60338.5, 60340.0, None)), (None, K.CLOCK_UNREADABLE))  # two that disagree: no majority

    def test_frame_diff_only_vetoes(self):
        self.assertEqual(C.decide(seam(ratio=1.5))[:2], (False, K.FRAME_DIFF_VETO))
        self.assertEqual(C.decide(seam(ratio=None))[:2], (False, K.FRAME_DIFF_UNAVAILABLE))
        self.assertEqual(C.decide(seam(before=(), ratio=0.0))[:2], (False, K.CLOCK_UNREADABLE))

    def test_clock_must_tick_and_nearest_read_cannot_be_outvoted(self):
        frozen_before = (4000.2, 4000.7, 4001.2)
        frozen_after = (3999.8, 3999.3, 3998.8)
        self.assertFalse(C.decide(seam(before=frozen_before, after=frozen_after))[0])
        self.assertFalse(C.decide(seam(before=(60350.5, 60338.5, 60338.5)))[0])
        self.assertTrue(C.decide(seam(before=(60338.5, None, 60338.5)))[0])

    def test_integer_second_intervals_must_have_common_seam_instant(self):
        # Actual seam times 38.25 and 39.75, exact floor-second reads.
        before = (60338.2,60337.7,60338.2)
        after = (60338.8,60339.3,60338.8)
        self.assertEqual(C.decide(seam(before=before,after=after))[:2],(False,K.CLOCK_DISAGREES))
        # Missing middle samples can make continuity feasible while leaving a
        # possible jump larger than the approved one-second clock resolution.
        self.assertEqual(C.decide(seam(before=(60338.2,None,60338.2),after=(60338.8,None,60338.8)))[:2],(False,K.CLOCK_DISAGREES))
        self.assertTrue(C.decide(seam(before=BEFORE,after=AFTER))[0])

    def test_date_skip_and_unreadable_date_cannot_join(self):
        self.assertEqual(C.parse_display_date('2026-09-19 16:45:38'),'2026-09-19')
        self.assertEqual(C.parse_display_date('19/09/26 16:45:38'),'2026-09-19')
        self.assertEqual(C.parse_display_date('09-19-2026 16:45:38'),'2026-09-19')
        self.assertEqual(C.parse_display_date('27.Sep 2026 07:39:20'),'2026-09-27')
        self.assertEqual(C.parse_display_date('27. Oct 2026 19:39:20'),'2026-10-27')
        self.assertIsNone(C.parse_display_date('27.Se 2026 07:39:20'))
        self.assertEqual(C.parse_clock_token('27. Oct 2026 19:39:20'),(70760,True))
        self.assertEqual(C.parse_clock_token('27. Oct 2026:39:20'),(None,False))
        self.assertIsNone(C.parse_display_date('08-05-2026 16:45:38'))
        self.assertIsNone(C.parse_display_date('16:45:38'))
        self.assertEqual(C.decide(seam(after_dates=('2026-09-20',)*3))[:2], (False,K.CLOCK_DATE_UNPROVEN))
        self.assertEqual(C.decide(seam(after_dates=(None,)*3))[:2], (False,K.CLOCK_DATE_UNPROVEN))

    def test_time_token_boundaries_and_meridiem_never_inferred(self):
        self.assertEqual(C.parse_clock_token('2026-09-21 3:00:06'), (10806, False))
        self.assertEqual(C.parse_clock_token('2026-09-19:00:06'), (None, False))
        self.assertEqual(C.parse_clock_token('2026-09-21 13:00:06'), (46806, True))
        self.assertEqual(C.parse_clock_token('2026-09-19 01:00:06 am'), (3606, True))
        self.assertEqual(C.parse_clock_token('2026-09-19 01:00:06 p.m.'), (46806, True))
        self.assertEqual(C.parse_clock_token('2026-09-19 04:00:06 PM2.5'), (14406,False))
        for label in ('PM 2.5','PM: 2.5','PM₂.₅'):
            self.assertEqual(C.parse_clock_token('2026-09-19 04:00:06 '+label),(14406,False))
        self.assertEqual(C.parse_clock_token('2026-09-19 04:00:06 amsterdam'), (14406,False))
        self.assertEqual(C.parse_clock_token('2026-09-19 01:00:06'), (3606, False))

    def test_media_and_timing_must_match(self):
        other = C.Media(VIDEO[:7] + ('SHA256:cc',), AUDIO, 0.0, 300.0, 0.04)
        self.assertEqual(C.decide(seam(nxt=other))[:2], (False, K.MEDIA_MISMATCH))
        self.assertEqual(C.decide(seam(nxt=C.Media(VIDEO, (), 0.0, 300.0, 0.04)))[:2], (False, K.MEDIA_MISMATCH))
        self.assertEqual(C.decide(seam(gap=0.5))[:2], (False, K.DB_GAP))
        self.assertEqual(C.decide(seam(gap=0.04))[0], True)
        self.assertEqual(C.decide(seam(span=301.0))[:2], (False, K.DURATION_DISCONTINUITY))  # previous file lost its tail
        self.assertEqual(C.decide(seam(nxt=C.Media(VIDEO, AUDIO, 0.5, 300.0, 0.04)))[:2], (False, K.DURATION_DISCONTINUITY))

    def test_collation_hard_splits_stay(self):
        for reason in ('different_capture_lease', 'capture_sequence_not_consecutive', 'codec_params_differ', 'different_capture_attempt',
                       'capture_lease_indicator_changed', 'unknown_reason', 'none'):
            self.assertEqual(C.decide(seam(reason))[:2], (False, K.COLLATION_HARD_SPLIT))
        for reason in ('prev_clip_cut_short', 'content_span_mismatch'):
            self.assertEqual(C.decide(seam(reason))[:2], (False, K.DURATION_DISCONTINUITY))

    def test_failed_evidence_stays_split(self):
        ev = {'recording_id': 433, 'collation_reason': 'frame_jump', 'db_gap_s': 0.0, 'prev_span_s': 300.0, 'error': 'timeout'}
        self.assertEqual(C.verdict(ev)[:2], (False, K.EVIDENCE_FAILED))
        self.assertEqual(C.verdict(dict(ev, recording_id=999))[:2], (False, K.NO_CLOCK_PROFILE))
        self.assertEqual(C.side_offset((100.2, 101.7, 101.2)), (None, K.CLOCK_INCONSISTENT))
        self.assertEqual(C.decide(seam(gap=float('nan')))[:2], (False, K.EVIDENCE_FAILED))
        self.assertEqual(C.decide(seam(ratio=float('nan')))[:2], (False, K.FRAME_DIFF_UNAVAILABLE))
        self.assertEqual(C.decide(seam('db_gap'))[:2], (False, K.DB_GAP))

    def test_known_clock_discontinuities_never_join(self):
        """Every 0 s seam the final scaled census flagged and the independent review's 10, through the
        rule with offset = clock minus seam-relative time: no confirmed (by eye) discontinuity joins."""
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'testdata', 'clock_discontinuities.json')) as f:
            cases = json.load(f)['cases']
        self.assertEqual((len(cases), sum(x['visual'] == 'real' for x in cases)), (122, 61))
        for x in cases:
            before = tuple(None if c is None else c - (t - x['prev_duration_s']) for t, c in sorted(x['prev_reads'], reverse=True))
            after = tuple(None if c is None else c - t for t, c in x['next_reads'])
            join, why, _ = C.decide(seam('frame_jump', before=before, after=after, ratio=0.5, span=x['prev_duration_s'],
                                         prev=C.Media(VIDEO, AUDIO, 0.0, x['prev_duration_s'], 0.04)))
            if x['visual'] == 'real':
                self.assertFalse(join, (x['source'], x['stream'], x['seam']))
                self.assertIn(why, (K.CLOCK_DISAGREES, K.CLOCK_INCONSISTENT, K.CLOCK_UNREADABLE))

    def test_seam_timing(self):
        f = 1 / 30
        a = [(i * f, f) for i in range(30)]           # 1.000 s of video
        b = [(i * f, f) for i in range(15)]
        ok = a + [(1.02 + t, d) for t, d in b]        # audio outlasts a's video by 20 ms: the file ends at 1.02
        self.assertEqual(C.seam_timing_problems(ok, [(1.02, a), (0.5, b)], f), [])
        gap = a + [(1.12 + t, d) for t, d in b]       # 100 ms more than the container explains
        self.assertRegex(C.seam_timing_problems(gap, [(1.02, a), (0.5, b)], f)[0], 'video dts step')
        self.assertRegex(C.seam_timing_problems(a + [(1.2 + t, d) for t, d in b], [(1.2, a), (0.5, b)], f)[0], 'video ends 0.2000')
        self.assertRegex(C.seam_timing_problems(ok[:-1], [(1.02, a), (0.5, b)], f)[0], 'video packets')

    def test_parse_clock(self):
        for txt, want in (('Mierové námestie 19/09/26 16:45:38', 60338), ('08-05-2026 11:20:46 AM', 40846),
                          ('08-05-2026 01:20:46 PM', 4846 + 12 * 3600), ('12:00:05 AM', 5), ('12:00:05 PM', 43205),
                          ('2026-08-03 7:26:40', 26800), ('16:4538', 60338), ('no clock', None), ('29:00:00', None)):
            self.assertEqual(C.parse_clock(txt), want, txt)

    def test_clock_profiles(self):
        self.assertEqual(len(C.CLOCK_CROPS), 18)
        self.assertEqual(C.CLOCK_CROPS[433], 'crop=500:50:1420:1030')  # the pilot's crop, unchanged
        with tempfile.NamedTemporaryFile('w', suffix='.tsv', delete=False) as f:
            f.write('recording_id\tcrop\n1\tcrop=1:2:3:4,lutyuv=y=0\n')
        with self.assertRaisesRegex(SystemExit, 'crop=W:H:X:Y'):
            C.read_clock_profiles(f.name)
        os.remove(f.name)

    def test_head_verification(self):
        sha, md5 = 'ab' * 32, 'cd' * 16
        head = {'ContentLength': 10, 'ChecksumSHA256': base64.b64encode(bytes.fromhex(sha)).decode(), 'Metadata': {'sha256': sha}, 'ETag': f'"{md5}"'}
        self.assertEqual(C.head_problems(head, 10, sha, md5), [])
        self.assertEqual(len(C.head_problems(dict(head, ContentLength=11), 10, sha, md5)), 1)  # another size
        self.assertEqual(len(C.head_problems(dict(head, ETag='"' + 'ef' * 16 + '"'), 10, sha, md5)), 1)  # same size, other bytes
        self.assertEqual(len(C.head_problems({'ContentLength': 10, 'ETag': f'"{md5}"'}, 10, sha, md5)), 2)  # no server checksum

    def test_runs(self):
        self.assertEqual(C.runs('abcde', [True, False, True, True]), [['a', 'b'], ['c', 'd', 'e']])
        self.assertEqual(C.runs('ab', [False]), [['a'], ['b']])


class ExactPlanTests(unittest.TestCase):
    def fixture(self):
        gen = 'a' * 32
        registry = {'generation': gen, 'files': {str(i): {'recording_id': 433, 'key': f'k{i}', 'size_bytes': 42,
                    'sha256': 'b' * 64, 'etag': '"' + 'c' * 32 + '"', 'verified': True} for i in (1, 2, 3)}}
        ledger = [{'generation': gen, 'published': 'y', 'recording_id': '433', 'local_date': '2026-09-19',
                   'delivery_hour': '1', 'hour_id': 'h1', 'served': '3', 'file_ids': '1,2,3',
                   'object_keys': 'k1,k2,k3', 'sources': 'original,original,original'}]
        seams = [{'recording_id': '433', 'prev_file': str(i), 'next_file': str(i+1), 'prev_key': f'k{i}',
                  'next_key': f'k{i+1}', 'gap_s': '0', 'reason': 'frame_jump', 'adjacent': 'True'} for i in (1, 2)]
        source = [{'recording_id': '433', 'file_id': str(i), 'key': f'k{i}', 'size': '42',
                   'order_ok': 'True', 'wall_span_seconds': '300'} for i in (1, 2, 3)]
        rows = C.plan(ledger, seams, source, 433, registry)
        decisions = [dict(r, prev_media=dataclasses.asdict(MEDIA), next_media=dataclasses.asdict(MEDIA),
                          clock_before=BEFORE, clock_after=AFTER, clock_before_dates=DATES, clock_after_dates=DATES,
                          clock_before_raw=BEFORE_RAW, clock_after_raw=AFTER_RAW, clock_before_pts=BEFORE_PTS,
                          clock_after_pts=AFTER_PTS, clock_before_edge=BEFORE_EDGE, clock_after_edge=AFTER_EDGE,frame_ratio=.5, join=True,
                          keep_reason=None, clock_delta_s=0.6) for r in rows]
        return rows, decisions, ledger, seams, source, registry

    def test_exact_plan_and_recomputed_verdicts(self):
        plan, decisions, *rest = self.fixture()
        self.assertEqual(len(C.checked_verdicts(plan, decisions)), 1)
        for changed in (decisions[:1], decisions + decisions[:1], [dict(decisions[0], join=False), decisions[1]],
                        [dict(decisions[0], next_key='wrong'), decisions[1]],
                        [dict(decisions[0], clock_after=(0, 0, 0)), decisions[1]]):
            with self.assertRaises(ValueError):
                C.checked_verdicts(plan, changed)
        bad_plan = [plan[0], dict(plan[1], prev_file='9')]
        bad_decisions = [decisions[0], dict(decisions[1], prev_file='9')]
        with self.assertRaisesRegex(ValueError, 'nonconsecutive'):
            C.checked_verdicts(bad_plan, bad_decisions)

    def test_verdict_binds_exact_raw_ocr(self):
        _, decisions, *_ = self.fixture()
        e = decisions[0]
        self.assertEqual(C.verdict(e), (True,None,.6))
        self.assertFalse(C.verdict(dict(e, clock_before_pts=()))[0])
        self.assertFalse(C.verdict(dict(e, clock_after_pts=([4,'1/10'],AFTER_PTS[1],AFTER_PTS[-1])))[0])
        self.assertFalse(C.verdict(dict(e, clock_after_pts=([21,'1/100'],AFTER_PTS[1],AFTER_PTS[-1])))[0])
        self.assertFalse(C.verdict(dict(e,clock_before_edge={'pts':BEFORE_EDGE['pts'],'raw':['2026-09-19 16:45:36']*3}))[0])
        self.assertFalse(C.verdict(dict(e,clock_after_edge={'pts':AFTER_EDGE['pts'],'raw':['2026-09-19 16:45:41']*3}))[0])
        self.assertFalse(C.verdict(dict(e,clock_before_edge={'pts':[29980,'1/100'],'raw':BEFORE_EDGE['raw']}))[0])
        self.assertFalse(C.verdict({k:v for k,v in e.items() if k!='clock_before_edge'})[0])
        self.assertFalse(C.verdict(dict(e, clock_after_raw=[['2026-09-19 01:45:39 am']*3]*3))[0])
        self.assertFalse(C.verdict(dict(e, clock_after_raw=[['2026-09-19 04:45:39']*3]*3))[0])
        self.assertFalse(C.verdict(dict(e, clock_after_raw=()))[0])
        self.assertFalse(C.verdict(dict(e, clock_before_raw=[BEFORE_RAW[0],['2026-09-19 2026-09-20 16:45:38']*3,BEFORE_RAW[-1]],clock_before_dates=(DATES[0],None,DATES[-1])))[0])
        self.assertFalse(C.verdict(dict(e, clock_before_raw=[BEFORE_RAW[0],['2026-09-19 16:45:38','2026-09-19 16:45:38','2026-09-20 16:45:38'],BEFORE_RAW[-1]]))[0])
        self.assertFalse(C.verdict(dict(e, clock_before_raw=[BEFORE_RAW[0],BEFORE_RAW[1][:2],BEFORE_RAW[-1]]))[0])
        self.assertFalse(C.verdict(dict(e, clock_before_raw=[[t.replace('16:','04:')+' PM2.5' for t in texts] for texts in BEFORE_RAW]))[0])
        self.assertFalse(C.verdict(dict(e, clock_before=(BEFORE[0],None,BEFORE[-1]),
            clock_before_dates=(DATES[0],None,DATES[-1]),
            clock_before_raw=[BEFORE_RAW[0],['2026-09-19 23:24:15']*3,BEFORE_RAW[-1]]))[0])

    def test_revoked_run_cannot_reuse_receipt(self):
        plan, decisions, *rest = self.fixture()
        by_hour = C.checked_verdicts(plan, decisions)
        receipt = dict(plan_sha256='p', verdicts_sha256='v', batch='b', served_inputs=['1','2','3'])
        self.assertEqual(C.resume_runs([receipt], 'p','v','b',by_hour), {('1','2','3')})
        with self.assertRaisesRegex(ValueError, 'different exact plan'):
            C.resume_runs([receipt], 'p','new','b',by_hour)
        revoked = {h: [dict(row, join=False) for row in rows] for h, rows in by_hour.items()}
        with self.assertRaisesRegex(ValueError, 'do not accept'):
            C.resume_runs([receipt], 'p','v','b',revoked)

    def test_old_seam_or_parent_mismatch_refused(self):
        _, _, ledger, seams, source, registry = self.fixture()
        with self.assertRaisesRegex(ValueError, 'exact current'):
            C.plan(ledger, [dict(seams[0], next_key='different'), seams[1]], source, 433, registry)
        with self.assertRaisesRegex(ValueError, 'exact published'):
            C.plan([dict(ledger[0], published='n')], seams, source, 433, registry)
        with self.assertRaisesRegex(ValueError, 'exact parent file identity'):
            C.plan(ledger, seams, source, 433, dict(registry, files=dict(registry['files'], **{'1': dict(registry['files']['1'], key='new')})))


@unittest.skipUnless(shutil.which(C.FFMPEG) and shutil.which(C.FFPROBE), 'needs ffmpeg and ffprobe')
class BuildTests(unittest.TestCase):
    def test_zero_frame_seek_cannot_read_previous_side_png(self):
        with tempfile.TemporaryDirectory() as d:
            for name, seconds in (('long',2), ('short',.4)):
                subprocess.run([C.FFMPEG,'-v','error','-f','lavfi','-i',f'color=c=red:s=320x240:r=25:d={seconds}',
                                '-c:v','libx264',os.path.join(d,name+'.mp4')],check=True)
            actual_run = subprocess.run
            def with_ocr(args, **kwargs):
                if args[0] == 'tesseract':
                    return subprocess.CompletedProcess(args, 0, stdout='2026-09-19 12:34:56', stderr='')
                return actual_run(args, **kwargs)
            with mock.patch.object(C.subprocess,'run',side_effect=with_ocr):
                got = C.read_clock(os.path.join(d,'long.mp4'),1.2,'crop=100:50:0:0',d)
                self.assertEqual(got[:3],(45296,'2026-09-19',['2026-09-19 12:34:56']*3))
                self.assertEqual(got[3][0]*C.Fraction(got[3][1]), C.Fraction('1.2'))
                self.assertEqual(C.read_clock(os.path.join(d,'short.mp4'),1.2,'crop=100:50:0:0',d),(None,None,[],None))
                last=C.read_clock(os.path.join(d,'long.mp4'),0.5,'crop=100:50:0:0',d,last=True)
                self.assertEqual(C.stamp_time(last[3]),C.Fraction('1.96'))
                self.assertEqual(C.edge_distance(C.probe(os.path.join(d,'long.mp4')),last[3],True),C.Fraction('.04'))
            def date_only(args, **kwargs):
                if args[0] == 'tesseract':
                    return subprocess.CompletedProcess(args,0,stdout='27.Sep 2026',stderr='')
                return actual_run(args, **kwargs)
            with mock.patch.object(C.subprocess,'run',side_effect=date_only):
                got=C.read_clock(os.path.join(d,'long.mp4'),1.2,'crop=100:50:0:0',d)
                self.assertIsNone(got[0]); self.assertIsNone(got[1]); self.assertIn('27.Sep 2026',got[2])

    def test_real_vfr_seek_retains_later_frame_pts_and_refuses(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, 'vfr.mp4')
            subprocess.run([C.FFMPEG,'-v','error','-f','lavfi','-i','color=c=red:s=320x240:r=25:d=4',
                            '-vf',"select='not(between(n,4,9))'",'-fps_mode','vfr','-c:v','libx264',p],check=True)
            actual_run = subprocess.run
            def with_ocr(args, **kwargs):
                if args[0] == 'tesseract':
                    return subprocess.CompletedProcess(args,0,stdout='2026-09-19 16:45:35',stderr='')
                return actual_run(args, **kwargs)
            with mock.patch.object(C.subprocess,'run',side_effect=with_ocr):
                got=C.read_clock(p,.2,'crop=100:50:0:0',d)
            self.assertEqual(got[3][0]*C.Fraction(got[3][1]), C.Fraction('0.4'))
            with self.assertRaisesRegex(ValueError, 'outside the requested'):
                C.sample_distances(C.probe(p), [got[3], None, None], False)

    def test_join_keeps_audio_and_duration_and_the_ledger_reads_its_proof(self):
        with tempfile.TemporaryDirectory() as d:
            src = os.path.join(d, 'src')
            os.makedirs(src)
            for i, n in enumerate((3, 2, 4)):  # one 9 s stream cut into three files
                subprocess.run([C.FFMPEG, '-v', 'error', '-f', 'lavfi', '-i', f'testsrc=size=320x240:rate=25:duration=9',
                                '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=44100:duration=9',
                                '-ss', str((0, 3, 5)[i]), '-t', str(n), '-c:v', 'libx264', '-g', '25', '-c:a', 'aac',
                                os.path.join(src, f'{i}.mp4')], check=True)
            run = [{'file_id': str(100 + i), 'key': f'{i}.mp4', 'recording_id': 433, 'local_date': '2026-09-19',
                    'delivery_hour': 3, 'hour_id': 'h-1', 'generation': 'a' * 32,
                    'size_bytes': os.path.getsize(os.path.join(src, f'{i}.mp4')),
                    'sha256': C.sha256_file(os.path.join(src, f'{i}.mp4')),
                    'etag': '"' + C.file_md5(os.path.join(src, f'{i}.mp4')) + '"'} for i in range(3)]
            fetch = lambda key, path, etag: shutil.copy(os.path.join(src, key), path)
            out = os.path.join(d, 'out')
            registry = {'generation': 'a' * 32, 'files': {x['file_id']: dict(x, verified=True) for x in run}}
            store = mock.Mock(); store.load.return_value = {'repairs': []}
            verdicts = []
            for i, (prev, nxt) in enumerate(zip(run, run[1:])):
                row = {'generation': 'a' * 32, 'recording_id': 433, 'local_date': '2026-09-19', 'delivery_hour': 3,
                       'hour_id': 'h-1', 'seam': i+1, 'served': 3, 'collation_reason': 'frame_jump', 'db_gap_s': 0,
                       'prev_span_s': 300, 'prev_media': dataclasses.asdict(MEDIA), 'next_media': dataclasses.asdict(MEDIA),
                       'clock_before': BEFORE, 'clock_after': AFTER, 'clock_before_dates': DATES, 'clock_after_dates': DATES,
                       'clock_before_raw': BEFORE_RAW, 'clock_after_raw': AFTER_RAW, 'clock_before_pts': BEFORE_PTS,
                       'clock_after_pts': AFTER_PTS, 'clock_before_edge':BEFORE_EDGE,'clock_after_edge':AFTER_EDGE,'frame_ratio': .5, 'join': True,
                       'keep_reason': None, 'clock_delta_s': 0.6}
                for side, x in (('prev', prev), ('next', nxt)):
                    row.update({f'{side}_{k}': v for k, v in C.source_identity(registry, x['file_id'], x['key'], 433).items()})
                verdicts.append(row)
            parts = C.parent_lineage(registry, store)
            proof = C.build_run(run, verdicts, 'clock-prestitch-20261126', out, os.path.join(d, 'work'), parts, fetch)

            obj = os.path.join(out, proof['output']['key'])
            m = C.probe(obj)
            self.assertAlmostEqual(m.duration_s, sum(i['duration_s'] for i in proof['inputs']), delta=C.OUTPUT_TOLERANCE_S)
            self.assertEqual(m.audio[0], 'aac')
            self.assertEqual(C.sha256_file(obj), proof['output']['sha256'])
            self.assertEqual([i['output_id'] for i in proof['inputs']], [100, 101, 102])
            md5 = C.file_md5(obj)
            listing = {proof['output']['key']: (proof['output']['size_bytes'], md5)}
            proof_key = proof['output']['key'].replace('/objects/', '/proofs/').replace('.mp4', '.json')
            proof_path = os.path.join(out, proof_key)
            listing[proof_key] = (os.path.getsize(proof_path), C.file_md5(proof_path))
            uploads = {proof['output']['key']: (proof['output']['size_bytes'], md5, proof['output']['sha256']),
                       proof_key: (*listing[proof_key], C.sha256_file(proof_path))}
            [a] = L.read_proofs(out, listing, uploads, registry, store)
            self.assertEqual((a.source, a.input_ids, a.path), (L.Source.CLOCK_ASSEMBLY, (100, 101, 102), None))
            self.assertEqual(C.build_run(run, verdicts, 'clock-prestitch-20261126', out, os.path.join(d, 'work2'), parts, fetch), proof)
            # Adjacent runs can share the same outer work path. A second output
            # must not truncate the first content-addressed inode.
            smaller = C.build_run(run[:2], verdicts[:1], 'clock-prestitch-20261126', out, os.path.join(d, 'work2'), parts, fetch)
            self.assertNotEqual(smaller['output']['sha256'], proof['output']['sha256'])
            self.assertEqual(C.sha256_file(obj), proof['output']['sha256'])
            with self.assertRaises(C.BuildRefused):
                C.build_run(run, [], 'clock-prestitch-20261126', out, os.path.join(d, 'work3'), parts, fetch)
            with self.assertRaisesRegex(ValueError, 'incomplete boundary'):
                C.validate_clock_proof(dict(proof, seams=[]), registry, store)
            bad = dict(proof, inputs=[dict(proof['inputs'][0], output_id=999)] + proof['inputs'][1:])
            with self.assertRaisesRegex(ValueError, 'lineage differs'):
                C.validate_clock_proof(bad, registry, store)



if __name__ == '__main__':
    unittest.main()
