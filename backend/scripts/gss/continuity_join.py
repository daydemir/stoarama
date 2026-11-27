"""Continuity join: join adjacent served files of one GSS delivery hour only where the
burned-in clock proves the video runs on, so fewer parts per hour and no join across
a real time skip.

Deniz, Nov 26: "safety is important not to stitch things that are actually skips in
time.. but otherwise do relaly want things as stitched as possible". The rule
(project lead, binding) joins two adjacent served files of the same hour only when
ALL of these hold:
  - the burned-in clock offset (clock reading minus seam-relative file time) agrees
    within CLOCK_TOLERANCE_S across the seam, each side the median of the largest
    agreeing subset (at least MIN_READS) of reads at READ_AT_S from the join;
  - codec, resolution, fps, timebase (and the codec extradata, which stream copy
    needs) match;
  - there is no PTS/duration discontinuity: the DB gap is within one frame, the
    previous file is not short of its DB span (no lost tail), the next file starts
    at 0, and collation did not split for a duration reason;
  - the frame-diff ratio at the seam is below FRAME_DIFF_VETO. Frame-diff is a veto
    only, never a reason to join.
A seam collation split as frame_jump or packet_replay therefore joins only with the
clock proof; every other seam stays split. A recording without a CLOCK_CROPS profile
never joins. There is no class rule: an independent check found 10 of 199 within-lease
0 s seams with matching media whose clock jumps -8.7 to +19.9 s
(seam-audit-20261126/independent-verify/zero-gap-exceptions.tsv); that and the scaled audit's
105 near-seam discontinuities are the test fixture testdata/clock_discontinuities.json, and the rule
refuses all 115. The audit behind it:
records/finish-plan-20261117/gss-delivery/seam-audit-20261126/README.md.

Subcommands (all local files; `evidence` and `build` read R2 through presigned GETs
from R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY):
  plan      one recording's seams between adjacent served files of an hour, from the
            best-14 ledger (served order and keys), the audit's split-seams table
            (collation reason, DB gap) and served-files table (DB span).
  evidence  ffprobe, clock reads and the frame-diff ratio for every seam the
            metadata does not already keep split.
  decide    the rule, per seam: join, or the KeepReason it stays split.
  build     each run of joined seams as one ffmpeg stream copy (audio kept), checked
            (duration = sum of inputs, packet timing at every seam, full decode),
            written create-only under --out laid out by R2 key:
              joined/<batch>/objects/<sha256>.mp4
              joined/<batch>/proofs/<sha256>.json   (best14_ledger.py --clock-proofs)
Uploading is separate and create-only; the inputs stay in R2, held and unreferenced.
"""
import argparse
import collections
import concurrent.futures
import csv
import dataclasses
import datetime
import enum
import gzip
import hashlib
import hmac
import itertools
import json
import os
import re
import statistics
import subprocess
import sys
import tempfile
import urllib.parse

CLOCK_TOLERANCE_S = 1.0
MIN_READS = 2
READ_SPREAD_S = 1.0  # reads of one side that agree lie within this
# Seconds before the seam (previous file) and after it (next file): the frames nearest the join, so a jump
# or replay that resolves within ~1 s of it (seen on 437 and 442) still splits the reads.
READ_AT_S = (0.2, 0.7, 1.2)
FRAME_DIFF_VETO = 1.5
FRAME_DIFF_HALF_S = 10.0
TAIL_TOLERANCE_S = 0.2  # previous file's media end vs its DB span (the audit's content-vs-DB tolerance)
OUTPUT_TOLERANCE_S = 0.1  # joined duration vs the sum of its inputs
BATCH_PREFIX = 'clock-prestitch-'
PASS_VERSION = 'continuity-join/1'
FFMPEG = os.environ.get('FFMPEG_BIN', 'ffmpeg')
FFPROBE = os.environ.get('FFPROBE_BIN', 'ffprobe')
DECODE_THREADS = os.environ.get('FFMPEG_THREADS', '0')  # 0 = ffmpeg's choice

# Per-recording clock crop (ffmpeg crop on the decoded frame), from the seam audit's
# clockocr.py, where it read 433's clock on 64 seams.
CLOCK_CROPS = {433: 'crop=500:50:1420:1030'}
CLOCK_THRESHOLDS = (190, 150, 220)
CLOCK_RX = (re.compile(r'([0-2]\d)\s*:\s*([0-5]\d)\s*:\s*([0-5]\d)'), re.compile(r'([0-2]\d):([0-5]\d)([0-5]\d)(?!\d)'))

# Collation split reasons that are themselves a time or stream discontinuity.
HARD_SPLITS = frozenset({'different_capture_lease', 'capture_sequence_not_consecutive', 'codec_params_differ'})
DURATION_SPLITS = frozenset({'prev_clip_cut_short', 'content_span_mismatch'})


class KeepReason(str, enum.Enum):
    """Why a seam stays split. Every seam that does not join has exactly one."""
    COLLATION_HARD_SPLIT = 'collation_hard_split'  # capture lease, sequence or codec split
    DB_GAP = 'db_gap'                              # DB gap above one frame
    DURATION_DISCONTINUITY = 'duration_discontinuity'
    MEDIA_MISMATCH = 'media_mismatch'
    CLOCK_UNREADABLE = 'clock_unreadable'          # fewer than MIN_READS reads on a side
    CLOCK_INCONSISTENT = 'clock_inconsistent'      # a side's reads disagree
    CLOCK_DISAGREES = 'clock_disagrees'            # a real time skip (or overlap)
    FRAME_DIFF_VETO = 'frame_diff_veto'
    FRAME_DIFF_UNAVAILABLE = 'frame_diff_unavailable'
    EVIDENCE_FAILED = 'evidence_failed'            # a probe or read raised


MEDIA_FIELDS = ('codec_name', 'profile', 'width', 'height', 'pix_fmt', 'r_frame_rate', 'time_base', 'extradata_hash')
AUDIO_FIELDS = ('codec_name', 'sample_rate', 'channels', 'time_base', 'extradata_hash')


@dataclasses.dataclass(frozen=True)
class Media:
    """What ffprobe says about one served file."""
    video: tuple       # MEDIA_FIELDS values
    audio: tuple       # AUDIO_FIELDS values, () without audio
    start_s: float     # first video packet
    duration_s: float  # container duration
    frame_s: float     # one video frame


@dataclasses.dataclass(frozen=True)
class Seam:
    collation_reason: str
    db_gap_s: float
    prev_span_s: float  # DB wall span of the previous file
    prev: Media = None
    next: Media = None
    before: tuple = ()  # clock offsets read before the seam (None = unreadable)
    after: tuple = ()
    frame_ratio: float = None


def side_offset(reads):
    """(median offset, None) of the largest subset of at least MIN_READS reads that agree within
    READ_SPREAD_S, so one OCR misread among three is outvoted; else (None, KeepReason). Two equally
    large agreeing subsets with different medians are inconsistent."""
    got = [r for r in reads if r is not None]
    if len(got) < MIN_READS:
        return None, KeepReason.CLOCK_UNREADABLE
    for n in range(len(got), MIN_READS - 1, -1):
        meds = [statistics.median(c) for c in itertools.combinations(got, n) if max(c) - min(c) <= READ_SPREAD_S]
        if meds:
            if max(meds) - min(meds) > READ_SPREAD_S:
                return None, KeepReason.CLOCK_INCONSISTENT
            return meds[0], None
    return None, KeepReason.CLOCK_INCONSISTENT


def metadata_keep(seam):
    """The KeepReason the seam metadata alone decides, or None when it needs media evidence."""
    if seam.collation_reason in HARD_SPLITS:
        return KeepReason.COLLATION_HARD_SPLIT
    if seam.collation_reason in DURATION_SPLITS:
        return KeepReason.DURATION_DISCONTINUITY
    return None


def clock_check(seam):
    """(KeepReason or None, clock delta): the clock must prove the seam continuous."""
    b, why = side_offset(seam.before)
    a, why_a = side_offset(seam.after)
    if why or why_a:
        return why or why_a, None
    delta = round(a - b, 3)
    return (KeepReason.CLOCK_DISAGREES if abs(delta) > CLOCK_TOLERANCE_S else None), delta


def decide(seam):
    """(join, KeepReason or None, clock delta). Joins only when every check holds."""
    keep = metadata_keep(seam)
    if keep:
        return False, keep, None
    if seam.prev is None or seam.next is None:
        raise ValueError('decide needs both sides probed')
    frame = max(seam.prev.frame_s, seam.next.frame_s)
    if abs(seam.db_gap_s) > frame:
        return False, KeepReason.DB_GAP, None
    if seam.prev.video != seam.next.video or seam.prev.audio != seam.next.audio:
        return False, KeepReason.MEDIA_MISMATCH, None
    if (seam.prev.duration_s < seam.prev_span_s - TAIL_TOLERANCE_S or abs(seam.next.start_s) > frame
            or abs(seam.prev.start_s) > frame):
        return False, KeepReason.DURATION_DISCONTINUITY, None
    why, delta = clock_check(seam)
    if why:
        return False, why, delta
    if seam.frame_ratio is None:
        return False, KeepReason.FRAME_DIFF_UNAVAILABLE, delta
    if seam.frame_ratio >= FRAME_DIFF_VETO:
        return False, KeepReason.FRAME_DIFF_VETO, delta
    return True, None, delta


def runs(served, joins):
    """Group an hour's served files (in order) into runs; joins[i] joins served[i] and served[i+1]."""
    out = [[served[0]]]
    for x, j in zip(served[1:], joins):
        if j:
            out[-1].append(x)
        else:
            out.append([x])
    return out


# ---- R2 reads ----------------------------------------------------------------

def presign(key, expires=7200):
    acct, ak, sk, bucket = (os.environ[x] for x in ('R2_ACCOUNT_ID', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET'))
    host = f'{acct}.r2.cloudflarestorage.com'
    now = datetime.datetime.now(datetime.timezone.utc)
    amz, d = now.strftime('%Y%m%dT%H%M%SZ'), now.strftime('%Y%m%d')
    scope = f'{d}/auto/s3/aws4_request'
    path = '/' + bucket + '/' + urllib.parse.quote(key, safe='/-_.~')
    q = {'X-Amz-Algorithm': 'AWS4-HMAC-SHA256', 'X-Amz-Credential': f'{ak}/{scope}', 'X-Amz-Date': amz,
         'X-Amz-Expires': str(expires), 'X-Amz-SignedHeaders': 'host'}
    qs = '&'.join(f'{urllib.parse.quote(k, safe="-_.~")}={urllib.parse.quote(v, safe="-_.~")}' for k, v in sorted(q.items()))
    creq = f'GET\n{path}\n{qs}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD'
    sts = f'AWS4-HMAC-SHA256\n{amz}\n{scope}\n' + hashlib.sha256(creq.encode()).hexdigest()
    k = ('AWS4' + sk).encode()
    for m in (d, 'auto', 's3', 'aws4_request'):
        k = hmac.new(k, m.encode(), hashlib.sha256).digest()
    return f'https://{host}{path}?{qs}&X-Amz-Signature=' + hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()


# ---- media evidence ----------------------------------------------------------

def fraction(s):
    n, d = s.split('/')
    return float(n) / float(d)


def probe(src):
    p = subprocess.run([FFPROBE, '-v', 'error', '-show_data_hash', 'sha256', '-show_streams', '-show_format', '-of', 'json', src],
                       capture_output=True, text=True, timeout=300, check=True)
    j = json.loads(p.stdout)
    vs = [s for s in j['streams'] if s['codec_type'] == 'video']
    aus = [s for s in j['streams'] if s['codec_type'] == 'audio']
    if len(vs) != 1 or len(aus) > 1:
        raise ValueError(f'{len(vs)} video and {len(aus)} audio streams')
    v = vs[0]
    return Media(tuple(str(v.get(f)) for f in MEDIA_FIELDS), tuple(str(aus[0].get(f)) for f in AUDIO_FIELDS) if aus else (),
                 float(v.get('start_time', 0)), float(j['format']['duration']), 1 / fraction(v['r_frame_rate']))


def read_clock(src, t, crop, tmp):
    """Burned-in clock seconds-of-day at file time t, or None."""
    raw = os.path.join(tmp, f'{t:.3f}.png')
    subprocess.run([FFMPEG, '-v', 'error', '-nostdin', '-ss', f'{t:.3f}', '-i', src, '-frames:v', '1',
                    '-vf', crop + ',scale=iw*3:ih*3,format=gray', '-y', raw], capture_output=True, timeout=300)
    if not os.path.exists(raw):
        return None
    for th in CLOCK_THRESHOLDS:
        png = f'{raw}.{th}.png'
        subprocess.run([FFMPEG, '-v', 'error', '-nostdin', '-i', raw, '-vf', f'lutyuv=y=if(gt(val\\,{th})\\,0\\,255)', '-y', png],
                       capture_output=True, timeout=60)
        c = parse_clock(subprocess.run(['tesseract', png, '-', '--psm', '7'], capture_output=True, text=True, timeout=60).stdout)
        if c is not None:
            return c
    return None


def parse_clock(txt):
    """Seconds of day of the last HH:MM:SS in OCR text, or None."""
    txt = txt.replace(' ', '').replace('O', '0').replace('o', '0').strip()
    for rx in CLOCK_RX:
        m = rx.findall(txt)
        if m:
            h, mi, s = map(int, m[-1])
            return h * 3600 + mi * 60 + s
    return None


GRID_W, GRID_H = 96, 64


def gray_frames(src, ss, dur):
    p = subprocess.run([FFMPEG, '-v', 'error', '-nostdin', '-ss', f'{max(ss, 0):.3f}', '-i', src, '-t', f'{dur:.3f}', '-an',
                        '-vf', f'scale={GRID_W}:{GRID_H},format=gray', '-f', 'rawvideo', '-'], capture_output=True, timeout=600)
    n = GRID_W * GRID_H
    return [p.stdout[i * n:(i + 1) * n] for i in range(len(p.stdout) // n)]


def coarse(f):
    return [sum(f[(by * 4 + y) * GRID_W + bx * 6 + x] for y in range(4) for x in range(6)) / 24 for by in range(16) for bx in range(16)]


def frame_ratio(prev_frames, next_frames):
    """Seam step over the largest other step in the window, on 16x16 area means (the audit's coarse ratio)."""
    fr = [coarse(f) for f in prev_frames + next_frames]
    b = len(prev_frames)
    if b < 8 or len(fr) - b < 8:
        return None
    d = [sum(abs(x - y) for x, y in zip(fr[i - 1], fr[i])) / len(fr[i]) for i in range(1, len(fr))]
    near = max(d[i - 1] for i in range(b - 3, b + 4))
    rest = max(d[i - 1] for i in range(1, len(fr)) if abs(i - b) > 5)
    return round(near / rest, 3) if rest else 99.0


def seam_evidence(row):
    """Media evidence of one planned seam (row from `plan`), gathered only as far as the rule
    needs it: nothing for a seam the metadata keeps or on a recording without a clock
    profile, clock reads only within a frame of DB gap, and the frame-diff veto only where
    the clock agrees."""
    out = dict(row)
    crop = CLOCK_CROPS.get(row['recording_id'])
    if crop is None or metadata_keep(Seam(row['collation_reason'], row['db_gap_s'], row['prev_span_s'])):
        return out
    prev_src, next_src = presign(row['prev_key']), presign(row['next_key'])
    try:
        pm, nm = probe(prev_src), probe(next_src)
        out.update(prev_media=dataclasses.asdict(pm), next_media=dataclasses.asdict(nm))
        if abs(row['db_gap_s']) > max(pm.frame_s, nm.frame_s):
            return out
        with tempfile.TemporaryDirectory() as tmp:
            before = [(lambda c: None if c is None else round(c + d, 3))(read_clock(prev_src, pm.duration_s - d, crop, tmp)) for d in READ_AT_S]
            after = [(lambda c: None if c is None else round(c - d, 3))(read_clock(next_src, d, crop, tmp)) for d in READ_AT_S]
        out.update(clock_before=before, clock_after=after)
        if clock_check(Seam('', 0.0, 0.0, before=tuple(before), after=tuple(after)))[0] is None:
            out['frame_ratio'] = frame_ratio(gray_frames(prev_src, pm.duration_s - FRAME_DIFF_HALF_S, FRAME_DIFF_HALF_S + 1),
                                             gray_frames(next_src, 0, FRAME_DIFF_HALF_S))
    except Exception as e:  # recorded, and decide() keeps the seam split
        out['error'] = repr(e)[:300]
    return out


def media_of(d):
    return Media(tuple(d['video']), tuple(d['audio']), d['start_s'], d['duration_s'], d['frame_s'])


def seam_of(ev):
    return Seam(ev['collation_reason'], ev['db_gap_s'], ev['prev_span_s'],
                media_of(ev['prev_media']) if 'prev_media' in ev else None,
                media_of(ev['next_media']) if 'next_media' in ev else None,
                tuple(ev.get('clock_before', ())), tuple(ev.get('clock_after', ())), ev.get('frame_ratio'))


def verdict(ev):
    seam = seam_of(ev)
    keep = metadata_keep(seam)
    if keep is None and 'error' in ev:
        return False, KeepReason.EVIDENCE_FAILED, None
    return decide(seam)


# ---- plan --------------------------------------------------------------------

def read_tsv(path):
    csv.field_size_limit(1 << 30)
    with (gzip.open(path, 'rt', encoding='utf-8') if path.endswith('.gz') else open(path, encoding='utf-8')) as f:
        return list(csv.DictReader(f, delimiter='\t'))


def plan(ledger, seams, served_files, recording_id):
    """Every seam between adjacent served files of one hour of the recording, in order."""
    split = {(r['prev_file'], r['next_file']): r for r in seams if int(r['recording_id']) == recording_id}
    span = {r['file_id']: float(r['wall_span_seconds']) for r in served_files if int(r['recording_id']) == recording_id}
    out = []
    for h in ledger:
        if int(h['recording_id']) != recording_id or not h['file_ids']:
            continue
        ids, keys, srcs = h['file_ids'].split(','), h['object_keys'].split(','), h['sources'].split(',')
        if len(ids) != int(h['served']) or len(keys) != len(ids):
            raise SystemExit(f'{h["hour_id"]}: served {h["served"]} but {len(ids)} ids and {len(keys)} keys')
        for k in range(len(ids) - 1):
            s = split.get((ids[k], ids[k + 1]))
            if s is None:
                raise SystemExit(f'{h["hour_id"]}: no split-seam row for {ids[k]} -> {ids[k + 1]}')
            out.append({'recording_id': recording_id, 'local_date': h['local_date'], 'delivery_hour': int(h['delivery_hour']),
                        'hour_id': h['hour_id'], 'seam': k + 1, 'served': len(ids),
                        'prev_file': ids[k], 'next_file': ids[k + 1], 'prev_key': keys[k], 'next_key': keys[k + 1],
                        'prev_source': srcs[k], 'next_source': srcs[k + 1],
                        'collation_reason': s['reason'], 'db_gap_s': float(s['gap_s']), 'prev_span_s': span[ids[k]]})
    return out


# ---- build -------------------------------------------------------------------

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def video_packet_times(path):
    p = subprocess.run([FFPROBE, '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'packet=dts_time,duration_time',
                        '-of', 'csv=p=0', path], capture_output=True, text=True, timeout=1200, check=True)
    return [(float(a), float(b)) for a, b in (line.split(',')[:2] for line in p.stdout.split() if line)]


SEAM_STEP_TOLERANCE_S = 0.002  # timebase rounding only


def seam_timing_problems(out_packets, inputs, frame_s):
    """Problems with the video packet timing of a stream copy of inputs [(container seconds,
    [(dts, duration)])]. Each input's packets appear unchanged and in order; at each seam the
    next file starts at the previous file's container end, so the dts step must equal that end
    minus the previous last packet's dts plus the next first dts. The previous file's audio may
    outlast its video (AAC framing and priming) by at most OUTPUT_TOLERANCE_S: more is lost video."""
    problems = []
    if len(out_packets) != sum(len(pk) for _, pk in inputs):
        return [f'{len(out_packets)} video packets vs {sum(len(pk) for _, pk in inputs)} in the inputs']
    k = 0
    for (dur, pk), (_, nxt) in zip(inputs, inputs[1:]):
        k += len(pk)
        last, last_d = pk[-1]
        overhang = dur - (last + last_d)
        if not -frame_s / 2 <= overhang <= OUTPUT_TOLERANCE_S:
            problems.append(f'video ends {overhang:.4f} s before its file at packet {k}')
        want = dur - last + nxt[0][0]
        got = out_packets[k][0] - out_packets[k - 1][0]
        if abs(got - want) > SEAM_STEP_TOLERANCE_S:
            problems.append(f'video dts step {got:.4f} at packet {k}, expected {want:.4f}')
    return problems


def check_output(path, inputs, frame_s):
    """Problems with a joined file: duration vs inputs, packet timing at every seam, a clean decode."""
    problems = []
    want = sum(i['duration_s'] for i in inputs)
    got = probe(path)
    if abs(got.duration_s - want) > OUTPUT_TOLERANCE_S:
        problems.append(f'duration {got.duration_s:.3f} vs inputs {want:.3f}')
    if got.video != media_of(inputs[0]['media']).video or got.audio != media_of(inputs[0]['media']).audio:
        problems.append('stream parameters differ from the inputs')
    pk = video_packet_times(path)
    if any(b[0] <= a[0] for a, b in zip(pk, pk[1:])):
        problems.append('non-increasing video dts')
    problems += seam_timing_problems(pk, [(i['duration_s'], video_packet_times(i['local'])) for i in inputs], frame_s)
    dec = subprocess.run([FFMPEG, '-v', 'error', '-nostdin', '-threads', DECODE_THREADS, '-i', path, '-map', '0', '-f', 'null', '-'],
                         capture_output=True, text=True, timeout=3600)
    if dec.returncode or dec.stderr.strip():
        problems.append('decode: ' + dec.stderr.strip()[:200])
    return problems


def ffmpeg_version():
    return subprocess.run([FFMPEG, '-version'], capture_output=True, text=True).stdout.splitlines()[0]


def download(key, path):
    subprocess.run(['curl', '-fsS', '--retry', '5', '-o', path + '.tmp', presign(key)], check=True, timeout=3600)
    os.rename(path + '.tmp', path)


def build_run(run, verdicts, batch, out_dir, work, catalog_parts, fetch=download):
    """Join one run of served files; returns the proof (also written under out_dir)."""
    os.makedirs(work, exist_ok=True)
    local = []
    for x in run:
        path = os.path.join(work, x['file_id'].replace(':', '_') + '.mp4')
        if not os.path.exists(path):
            fetch(x['key'], path)
        local.append(path)
    probes = [probe(p) for p in local]
    inputs = [dict(x, local=p, media=dataclasses.asdict(m), duration_s=m.duration_s, sha256=sha256_file(p), size_bytes=os.path.getsize(p))
              for x, p, m in zip(run, local, probes)]
    lst = os.path.join(work, 'concat.txt')
    with open(lst, 'w', encoding='utf-8') as f:
        f.writelines(f"file '{p}'\n" for p in local)
    tmp_out = os.path.join(work, 'joined.mp4')
    subprocess.run([FFMPEG, '-v', 'error', '-nostdin', '-y', '-f', 'concat', '-safe', '0', '-i', lst,
                    '-map', '0:v:0', '-map', '0:a?', '-c', 'copy', '-movflags', '+faststart', tmp_out], check=True, timeout=3600)
    frame_s = media_of(inputs[0]['media']).frame_s
    problems = check_output(tmp_out, inputs, frame_s)
    if problems:
        raise SystemExit(f'{run[0]["file_id"]}..: joined file fails its checks: {problems}')
    sha = sha256_file(tmp_out)
    key = f'joined/{batch}/objects/{sha}.mp4'
    proof = {
        'pass_version': PASS_VERSION, 'production_version': 'clock-prestitched', 'ffmpeg': ffmpeg_version(),
        'rule': {'clock_tolerance_s': CLOCK_TOLERANCE_S, 'min_reads': MIN_READS, 'read_spread_s': READ_SPREAD_S,
                 'frame_diff_veto': FRAME_DIFF_VETO, 'tail_tolerance_s': TAIL_TOLERANCE_S, 'clock_crop': CLOCK_CROPS[run[0]['recording_id']]},
        'recording_id': run[0]['recording_id'], 'local_date': run[0]['local_date'], 'delivery_hour': run[0]['delivery_hour'],
        'hour_id': run[0]['hour_id'],
        'output': {'key': key, 'sha256': sha, 'size_bytes': os.path.getsize(tmp_out), 'duration_s': probe(tmp_out).duration_s},
        'inputs': [{'output_id': int(p), 'served_file': x['file_id'], 'served_key': x['key'], 'served_sha256': x['sha256'],
                    'served_size_bytes': x['size_bytes'], 'duration_s': x['duration_s']}
                   for x in inputs for p in catalog_parts(x['file_id'])],
        'served_inputs': [x['file_id'] for x in inputs],
        'seams': verdicts,
    }
    obj = os.path.join(out_dir, key)
    prf = os.path.join(out_dir, f'joined/{batch}/proofs/{sha}.json')
    for p in (obj, prf):
        if os.path.exists(p):
            raise SystemExit(f'{p} exists: outputs are create-only')
        os.makedirs(os.path.dirname(p), exist_ok=True)
    os.rename(tmp_out, obj)
    with open(prf, 'x', encoding='utf-8') as f:
        json.dump(proof, f, indent=1)
        f.write('\n')
    return proof


# ---- CLI ---------------------------------------------------------------------

def read_jsonl(path):
    with open(path, encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('plan')
    p.add_argument('--ledger', required=True, help='best14-ledger.tsv of the live generation')
    p.add_argument('--seams', required=True, help="seam audit split-seams.tsv(.gz)")
    p.add_argument('--served-files', required=True, help='seam audit served-files.tsv(.gz)')
    p.add_argument('--recording', type=int, required=True)
    p.add_argument('--out', required=True)
    e = sub.add_parser('evidence')
    e.add_argument('--plan', required=True)
    e.add_argument('--out', required=True)
    e.add_argument('--workers', type=int, default=8)
    d = sub.add_parser('decide')
    d.add_argument('--evidence', required=True)
    d.add_argument('--out', required=True)
    b = sub.add_parser('build')
    b.add_argument('--verdicts', required=True)
    b.add_argument('--parts', required=True, help='JSON {served file id: [part output ids]}; a part maps to itself')
    b.add_argument('--batch', required=True, help=f'{BATCH_PREFIX}YYYYMMDD')
    b.add_argument('--out', required=True)
    b.add_argument('--work', required=True)
    a = ap.parse_args(argv)

    if a.cmd == 'plan':
        rows = plan(read_tsv(a.ledger), read_tsv(a.seams), read_tsv(a.served_files), a.recording)
        with open(a.out, 'x', encoding='utf-8') as f:
            f.writelines(json.dumps(r) + '\n' for r in rows)
        print(json.dumps({'seams': len(rows), 'hours': len({r['hour_id'] for r in rows}),
                          'reasons': collections.Counter(r['collation_reason'] for r in rows)}))
    elif a.cmd == 'evidence':
        rows = read_jsonl(a.plan)
        done = {(r['prev_file'], r['next_file']) for r in read_jsonl(a.out)} if os.path.exists(a.out) else set()
        todo = [r for r in rows if (r['prev_file'], r['next_file']) not in done]
        with concurrent.futures.ThreadPoolExecutor(a.workers) as ex, open(a.out, 'a', encoding='utf-8') as f:
            for n, r in enumerate(ex.map(seam_evidence, todo), 1):
                f.write(json.dumps(r) + '\n')
                f.flush()
                if n % 50 == 0:
                    print(f'{n}/{len(todo)}', file=sys.stderr, flush=True)
    elif a.cmd == 'decide':
        out, counts = [], collections.Counter()
        for ev in read_jsonl(a.evidence):
            join, why, delta = verdict(ev)
            if not join and why is None:
                raise SystemExit(f'{ev["prev_file"]} -> {ev["next_file"]}: no evidence')
            counts['join' if join else why.value] += 1
            out.append(dict(ev, join=join, keep_reason=why.value if why else None, clock_delta_s=delta))
        with open(a.out, 'x', encoding='utf-8') as f:
            f.writelines(json.dumps(r) + '\n' for r in out)
        print(json.dumps(dict(counts)))
    elif a.cmd == 'build':
        if not a.batch.startswith(BATCH_PREFIX):
            raise SystemExit(f'--batch must start with {BATCH_PREFIX}')
        with open(a.parts, encoding='utf-8') as f:
            parts = json.load(f)
        by_hour = collections.defaultdict(list)
        for v in read_jsonl(a.verdicts):
            by_hour[v['hour_id']].append(v)
        proofs = []
        for hour_id, vs in sorted(by_hour.items()):
            vs.sort(key=lambda v: v['seam'])
            if not any(v['join'] for v in vs):
                continue
            served = [{'file_id': vs[0]['prev_file'], 'key': vs[0]['prev_key']}] + \
                     [{'file_id': v['next_file'], 'key': v['next_key']} for v in vs]
            for x in served:
                x.update(recording_id=vs[0]['recording_id'], local_date=vs[0]['local_date'],
                         delivery_hour=vs[0]['delivery_hour'], hour_id=hour_id)
            k = 0
            for run in runs(served, [v['join'] for v in vs]):
                seam_vs = vs[k:k + len(run) - 1]
                k += len(run)
                if len(run) < 2:
                    continue
                proof = build_run(run, [{key: v[key] for key in ('seam', 'collation_reason', 'db_gap_s', 'clock_before', 'clock_after',
                                                                  'clock_delta_s', 'frame_ratio')} for v in seam_vs],
                                  a.batch, a.out, os.path.join(a.work, hour_id), lambda fid: parts[fid])
                proofs.append(proof['output']['key'])
                print(json.dumps({'hour_id': hour_id, 'served_inputs': len(run), 'key': proof['output']['key']}), flush=True)
        print(json.dumps({'assemblies': len(proofs)}))


if __name__ == '__main__':
    main()
