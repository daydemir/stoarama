import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import best14_ledger as L  # noqa: E402
import continuity_join as C  # noqa: E402

K = C.KeepReason
VIDEO = ('h264', 'High', '1920', '1080', 'yuv420p', '25/1', '1/12800', 'SHA256:aa')
AUDIO = ('aac', '44100', '1', '1/44100', 'SHA256:bb')
MEDIA = C.Media(VIDEO, AUDIO, 0.0, 300.0, 0.04)
CLOCK = (60338.5, 60338.5, 60338.5)


def seam(reason='frame_jump', gap=0.0, before=CLOCK, after=CLOCK, ratio=0.9, prev=MEDIA, nxt=MEDIA, span=300.0):
    return C.Seam(reason, gap, span, prev, nxt, tuple(before), tuple(after), ratio)


class RuleTests(unittest.TestCase):
    def test_clock_agreeing_seam_joins(self):
        self.assertEqual(C.decide(seam()), (True, None, 0.0))
        self.assertEqual(C.decide(seam(after=(60339.5, 60339.5, None)))[0], True)  # 1 s is the tolerance

    def test_clock_disagreeing_seam_never_joins(self):
        for after in ((60458.5,) * 3, (60218.5,) * 3, (60340.0,) * 3):  # 120 s skip, 120 s back, 1.5 s
            for reason in ('frame_jump', 'packet_replay', 'frame_overlap', 'frame_low_motion', 'none'):
                for ratio in (0.0, 0.5, 1.49):
                    self.assertEqual(C.decide(seam(reason, after=after, ratio=ratio))[:2], (False, K.CLOCK_DISAGREES))

    def test_frame_jump_without_clock_proof_never_joins(self):
        for reason in ('frame_jump', 'packet_replay'):
            for before, after, why in (((), (), K.CLOCK_UNREADABLE), ((None, None, None), CLOCK, K.CLOCK_UNREADABLE),
                                       ((60338.5, None, None), CLOCK, K.CLOCK_UNREADABLE),
                                       (CLOCK, (60338.5, None, None), K.CLOCK_UNREADABLE),
                                       ((60338.5, 60341.0, 60343.5), CLOCK, K.CLOCK_INCONSISTENT),
                                       ((60338.5, 60340.5, None), CLOCK, K.CLOCK_INCONSISTENT)):
                self.assertEqual(C.decide(seam(reason, before=before, after=after, ratio=0.1))[:2], (False, why))

    def test_one_misread_among_three_is_outvoted(self):
        self.assertEqual(C.side_offset((60338.5, 60400.5, 60338.5)), (60338.5, None))
        self.assertEqual(C.decide(seam(before=(60338.5, 60344.5, 60338.5)))[:2], (True, None))
        self.assertEqual(C.decide(seam(after=(60458.5, 60338.5, 60458.5)))[:2], (False, K.CLOCK_DISAGREES))  # the majority decides
        self.assertEqual(C.side_offset((60338.5, 60340.0, None)), (None, K.CLOCK_INCONSISTENT))  # two that disagree: no majority

    def test_frame_diff_only_vetoes(self):
        self.assertEqual(C.decide(seam(ratio=1.5))[:2], (False, K.FRAME_DIFF_VETO))
        self.assertEqual(C.decide(seam(ratio=None))[:2], (False, K.FRAME_DIFF_UNAVAILABLE))
        self.assertEqual(C.decide(seam(before=(), ratio=0.0))[:2], (False, K.CLOCK_UNREADABLE))

    def test_media_and_timing_must_match(self):
        other = C.Media(VIDEO[:7] + ('SHA256:cc',), AUDIO, 0.0, 300.0, 0.04)
        self.assertEqual(C.decide(seam(nxt=other))[:2], (False, K.MEDIA_MISMATCH))
        self.assertEqual(C.decide(seam(nxt=C.Media(VIDEO, (), 0.0, 300.0, 0.04)))[:2], (False, K.MEDIA_MISMATCH))
        self.assertEqual(C.decide(seam(gap=0.5))[:2], (False, K.DB_GAP))
        self.assertEqual(C.decide(seam(gap=0.04))[0], True)
        self.assertEqual(C.decide(seam(span=301.0))[:2], (False, K.DURATION_DISCONTINUITY))  # previous file lost its tail
        self.assertEqual(C.decide(seam(nxt=C.Media(VIDEO, AUDIO, 0.5, 300.0, 0.04)))[:2], (False, K.DURATION_DISCONTINUITY))

    def test_collation_hard_splits_stay(self):
        for reason in ('different_capture_lease', 'capture_sequence_not_consecutive', 'codec_params_differ'):
            self.assertEqual(C.decide(seam(reason))[:2], (False, K.COLLATION_HARD_SPLIT))
        for reason in ('prev_clip_cut_short', 'content_span_mismatch'):
            self.assertEqual(C.decide(seam(reason))[:2], (False, K.DURATION_DISCONTINUITY))

    def test_failed_evidence_stays_split(self):
        ev = {'collation_reason': 'frame_jump', 'db_gap_s': 0.0, 'prev_span_s': 300.0, 'error': 'timeout'}
        self.assertEqual(C.verdict(ev)[:2], (False, K.EVIDENCE_FAILED))

    def test_known_clock_discontinuities_never_join(self):
        """Every known 0 s seam whose clock jumps (115 cases from the scaled audit and the independent
        review), through the rule with offset = clock minus seam-relative time."""
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'testdata', 'clock_discontinuities.json')) as f:
            cases = json.load(f)['cases']
        self.assertEqual(len(cases), 115)
        for x in cases:
            before = tuple(None if c is None else c - (t - x['prev_duration_s']) for t, c in x['prev_reads'])
            after = tuple(None if c is None else c - t for t, c in x['next_reads'])
            join, why, _ = C.decide(seam('frame_jump', before=before, after=after, ratio=0.5, span=x['prev_duration_s'],
                                         prev=C.Media(VIDEO, AUDIO, 0.0, x['prev_duration_s'], 0.04)))
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

    def test_runs(self):
        self.assertEqual(C.runs('abcde', [True, False, True, True]), [['a', 'b'], ['c', 'd', 'e']])
        self.assertEqual(C.runs('ab', [False]), [['a'], ['b']])


@unittest.skipUnless(shutil.which(C.FFMPEG) and shutil.which(C.FFPROBE), 'needs ffmpeg and ffprobe')
class BuildTests(unittest.TestCase):
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
                    'delivery_hour': 3, 'hour_id': 'h-1'} for i in range(3)]
            fetch = lambda key, path: shutil.copy(os.path.join(src, key), path)
            out = os.path.join(d, 'out')
            proof = C.build_run(run, [], 'clock-prestitch-20261126', out, os.path.join(d, 'work'),
                                lambda fid: [int(fid)], fetch)
            obj = os.path.join(out, proof['output']['key'])
            m = C.probe(obj)
            self.assertAlmostEqual(m.duration_s, sum(i['duration_s'] for i in proof['inputs']), delta=C.OUTPUT_TOLERANCE_S)
            self.assertEqual(m.audio[0], 'aac')
            self.assertEqual(C.sha256_file(obj), proof['output']['sha256'])
            self.assertEqual([i['output_id'] for i in proof['inputs']], [100, 101, 102])
            listing = {proof['output']['key']: (proof['output']['size_bytes'], '0' * 32)}
            [a] = L.read_proofs(out, listing)
            self.assertEqual((a.source, a.input_ids, a.path), (L.Source.CLOCK_ASSEMBLY, (100, 101, 102), None))
            with self.assertRaisesRegex(SystemExit, 'create-only'):
                C.build_run(run, [], 'clock-prestitch-20261126', out, os.path.join(d, 'work2'), lambda fid: [int(fid)], fetch)


if __name__ == '__main__':
    unittest.main()
