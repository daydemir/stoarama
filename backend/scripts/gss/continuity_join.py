"""Continuity join: join adjacent served files of one GSS delivery hour only where the
burned-in clock proves the video runs on, so fewer parts per hour and no join across
a real time skip.

Deniz, Nov 26: "safety is important not to stitch things that are actually skips in
time.. but otherwise do relaly want things as stitched as possible". The rule
(project lead, binding) joins two adjacent served files of the same hour only when
ALL of these hold:
  - the burned-in clock offset (clock reading minus seam-relative file time) agrees
    within CLOCK_TOLERANCE_S across the seam, each side must have readable nearest and farthest frames showing an advancing
    clock with all readable offsets agreeing; nearest offsets also agree;
  - codec, resolution, fps, timebase (and the codec extradata, which stream copy
    needs) match;
  - there is no PTS/duration discontinuity: the DB gap is within one frame, the
    previous file is not short of its DB span (no lost tail), the next file starts
    at 0, and collation did not split for a duration reason;
  - the frame-diff ratio at the seam is below FRAME_DIFF_VETO. Frame-diff is a veto
    only, never a reason to join.
A seam collation split as frame_jump or packet_replay therefore joins only with the
clock proof; every other seam stays split. A recording without a clock-profiles.tsv row
never joins. There is no class rule: an independent check found 10 of 199 within-lease
0 s seams with matching media whose clock jumps -8.7 to +19.9 s
(seam-audit-20261126/independent-verify/zero-gap-exceptions.tsv). Those 10 and the final scaled
census's 112 flagged zero-gap seams (51 real by eye) are testdata/clock_discontinuities.json; the
rule refuses every real one. The audit behind it:
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
            With --upload-log each run is uploaded right after it is built: create-only
            (If-None-Match: *) with a SHA-256 the server checks, verified by HEAD (size,
            checksum, sha256 metadata, ETag = MD5), logged (best14_ledger.py
            --clock-uploads binds each proof to its listed object with it), and the local
            video is deleted, so scratch holds one run at a time. A run whose joined file
            fails its checks stays split (refused.jsonl).
The inputs stay in R2, held and unreferenced.
"""
import argparse
import base64
import collections
import concurrent.futures
import csv
import dataclasses
import datetime
import enum
import gzip
import hashlib
import hmac
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from best14_ledger import VETOED_PAIRS  # noqa: E402
from build_generation import AssetStore  # noqa: E402

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
PASS_VERSION = 'continuity-join/2'
FFMPEG = os.environ.get('FFMPEG_BIN', 'ffmpeg')
FFPROBE = os.environ.get('FFPROBE_BIN', 'ffprobe')
DECODE_THREADS = os.environ.get('FFMPEG_THREADS', '0')  # 0 = ffmpeg's choice



def read_clock_profiles(path):
    """{recording_id: ffmpeg crop} from clock-profiles.tsv: each clocked stream's clock region on its
    frame scaled to PROFILE_WIDTH wide (the scaled seam census's crops, seam-audit-20261126/scaled/raw/crops.tsv.gz)."""
    with open(path, encoding='utf-8') as f:
        rows = list(csv.DictReader(f, delimiter='\t'))
    out = {int(r['recording_id']): r['crop'] for r in rows}
    bad = [c for c in out.values() if not re.fullmatch(r'crop=\d+:\d+:\d+:\d+', c)]
    if bad or len(out) != len(rows):
        raise SystemExit(f'{path}: crops must be unique crop=W:H:X:Y rows, got {bad[:3]}')
    return out


PROFILE_WIDTH = 1920
CLOCK_CROPS = read_clock_profiles(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'clock-profiles.tsv'))
CLOCK_THRESHOLDS = (190, 150, 220)
CLOCK_RX = (re.compile(r'([0-2]?\d)\s*:\s*([0-5]\d)\s*:\s*([0-5]\d)(?!\d)(AM|PM)?'),
            re.compile(r'([0-2]?\d):([0-5]\d)([0-5]\d)(?!\d)(AM|PM)?'))

# Collation split reasons that are themselves a time or stream discontinuity.
JOINABLE_REASONS = frozenset({'frame_jump', 'packet_replay', 'frame_overlap', 'frame_low_motion'})
DURATION_SPLITS = frozenset({'prev_clip_cut_short', 'content_span_mismatch'})


class KeepReason(str, enum.Enum):
    """Why a seam stays split. Every seam that does not join has exactly one."""
    COLLATION_HARD_SPLIT = 'collation_hard_split'  # capture lease, sequence or codec split
    DB_GAP = 'db_gap'                              # DB gap above one frame
    DURATION_DISCONTINUITY = 'duration_discontinuity'
    MEDIA_MISMATCH = 'media_mismatch'
    NO_CLOCK_PROFILE = 'no_clock_profile'
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
    """All readable offsets must agree; do not outvote a contradictory frame."""
    got = [r for r in reads if r is not None]
    if len(got) < MIN_READS or len(reads) != len(READ_AT_S) or reads[0] is None or reads[-1] is None:
        return None, KeepReason.CLOCK_UNREADABLE
    if not all(math.isfinite(r) for r in got) or max(got) - min(got) > READ_SPREAD_S:
        return None, KeepReason.CLOCK_INCONSISTENT
    return statistics.median(got), None


def metadata_keep(seam):
    """The KeepReason the seam metadata alone decides, or None when it needs media evidence."""
    if seam.collation_reason in DURATION_SPLITS:
        return KeepReason.DURATION_DISCONTINUITY
    if seam.collation_reason == 'db_gap':
        return KeepReason.DB_GAP
    if seam.collation_reason not in JOINABLE_REASONS:
        return KeepReason.COLLATION_HARD_SPLIT
    return None


def clock_check(seam):
    """Prove advancing clocks on both sides plus agreement at the nearest reads."""
    b, why = side_offset(seam.before)
    a, why_a = side_offset(seam.after)
    if why or why_a:
        return why or why_a, None
    # The evidence offsets were formed from integer displayed seconds, plus or
    # minus the exact fixed sample position. A stationary display is not a clock.
    before = [round(c - t, 3) for c, t in zip(seam.before, READ_AT_S) if c is not None][::-1]
    after = [round(c + t, 3) for c, t in zip(seam.after, READ_AT_S) if c is not None]
    for reads in (before, after):
        if reads[-1] - reads[0] < 0.999 or any(y < x for x, y in zip(reads, reads[1:])):
            return KeepReason.CLOCK_INCONSISTENT, None
    delta = round(a - b, 3)
    if abs(seam.after[0] - seam.before[0]) > CLOCK_TOLERANCE_S or abs(delta) > CLOCK_TOLERANCE_S:
        return KeepReason.CLOCK_DISAGREES, delta
    return None, delta


def decide(seam):
    """(join, KeepReason or None, clock delta). Joins only when every check holds."""
    keep = metadata_keep(seam)
    if keep:
        return False, keep, None
    if seam.prev is None or seam.next is None:
        raise ValueError('decide needs both sides probed')
    numbers = (seam.db_gap_s, seam.prev_span_s, seam.prev.frame_s, seam.next.frame_s,
               seam.prev.duration_s, seam.next.duration_s, seam.prev.start_s, seam.next.start_s)
    if not all(math.isfinite(n) for n in numbers) or min(seam.prev_span_s, seam.prev.frame_s, seam.next.frame_s,
                                                       seam.prev.duration_s, seam.next.duration_s) <= 0:
        return False, KeepReason.EVIDENCE_FAILED, None
    frame = max(seam.prev.frame_s, seam.next.frame_s)
    if abs(seam.db_gap_s) > frame:
        return False, KeepReason.DB_GAP, None
    if (seam.prev.video != seam.next.video or seam.prev.audio != seam.next.audio or
            len(seam.prev.video) != len(MEDIA_FIELDS) or
            (seam.prev.audio and len(seam.prev.audio) != len(AUDIO_FIELDS)) or
            any(v in ('', 'None') for v in seam.prev.video + seam.prev.audio)):
        return False, KeepReason.MEDIA_MISMATCH, None
    if (seam.prev.duration_s < seam.prev_span_s - TAIL_TOLERANCE_S or abs(seam.next.start_s) > frame
            or abs(seam.prev.start_s) > frame):
        return False, KeepReason.DURATION_DISCONTINUITY, None
    why, delta = clock_check(seam)
    if why:
        return False, why, delta
    if seam.frame_ratio is None or not math.isfinite(seam.frame_ratio) or seam.frame_ratio < 0:
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


def http_headers(etag):
    if etag is None:
        return []
    if not re.fullmatch(r'"[0-9a-f]{32}(?:-[0-9]+)?"', etag):
        raise ValueError('invalid source ETag')
    return ['-headers', f'If-Match: {etag}\r\n']


def probe(src, etag=None):
    p = subprocess.run([FFPROBE, '-v', 'error', '-show_data_hash', 'sha256', '-show_streams', '-show_format', '-of', 'json', *http_headers(etag), src],
                       capture_output=True, text=True, timeout=300, check=True)
    j = json.loads(p.stdout)
    vs = [s for s in j['streams'] if s['codec_type'] == 'video']
    aus = [s for s in j['streams'] if s['codec_type'] == 'audio']
    if len(vs) != 1 or len(aus) > 1:
        raise ValueError(f'{len(vs)} video and {len(aus)} audio streams')
    v = vs[0]
    return Media(tuple(str(v.get(f)) for f in MEDIA_FIELDS), tuple(str(aus[0].get(f)) for f in AUDIO_FIELDS) if aus else (),
                 float(v.get('start_time', 0)), float(j['format']['duration']), 1 / fraction(v['r_frame_rate']))


def read_clock(src, t, crop, tmp, etag=None):
    """Burned-in clock seconds-of-day at file time t, or None."""
    raw = os.path.join(tmp, f'{t:.3f}.png')
    subprocess.run([FFMPEG, '-v', 'error', '-nostdin', '-ss', f'{t:.3f}', *http_headers(etag), '-i', src, '-frames:v', '1',
                    '-vf', f'scale={PROFILE_WIDTH}:-2,{crop},scale=iw*3:ih*3,format=gray', '-y', raw],
                   capture_output=True, timeout=300, check=True)
    if not os.path.exists(raw):
        return None
    for th in CLOCK_THRESHOLDS:
        png = f'{raw}.{th}.png'
        subprocess.run([FFMPEG, '-v', 'error', '-nostdin', '-i', raw, '-vf', f'lutyuv=y=if(gt(val\\,{th})\\,0\\,255)', '-y', png],
                       capture_output=True, timeout=60, check=True)
        c = parse_clock(subprocess.run(['tesseract', png, '-', '--psm', '7'], capture_output=True, text=True, timeout=60, check=True).stdout)
        if c is not None:
            return c
    return None


def parse_clock(txt):
    """Seconds of day of the last H:MM:SS or HH:MM:SS (12-hour with AM/PM, or 24-hour) in OCR text, or None."""
    txt = txt.replace(' ', '').replace('O', '0').replace('o', '0').strip()
    for rx in CLOCK_RX:
        m = rx.findall(txt)
        if m:
            h, mi, sec, ap = m[-1]
            h = int(h) % 12 + (12 if ap == 'PM' else 0) if ap else int(h)
            return h * 3600 + int(mi) * 60 + int(sec) if h < 24 else None
    return None


GRID_W, GRID_H = 96, 64


def gray_frames(src, ss, dur, etag=None):
    p = subprocess.run([FFMPEG, '-v', 'error', '-nostdin', '-ss', f'{max(ss, 0):.3f}', *http_headers(etag), '-i', src, '-t', f'{dur:.3f}', '-an',
                        '-vf', f'scale={GRID_W}:{GRID_H},format=gray', '-f', 'rawvideo', '-'], capture_output=True, timeout=600, check=True)
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
        pe, ne = row['prev_etag'], row['next_etag']
        pm, nm = probe(prev_src, pe), probe(next_src, ne)
        out.update(prev_media=dataclasses.asdict(pm), next_media=dataclasses.asdict(nm))
        if abs(row['db_gap_s']) > max(pm.frame_s, nm.frame_s):
            return out
        with tempfile.TemporaryDirectory() as tmp:
            before = [(lambda c: None if c is None else round(c + d, 3))(read_clock(prev_src, pm.duration_s - d, crop, tmp, pe)) for d in READ_AT_S]
            after = [(lambda c: None if c is None else round(c - d, 3))(read_clock(next_src, d, crop, tmp, ne)) for d in READ_AT_S]
        out.update(clock_before=before, clock_after=after)
        if clock_check(Seam('', 0.0, 0.0, before=tuple(before), after=tuple(after)))[0] is None:
            out['frame_ratio'] = frame_ratio(gray_frames(prev_src, pm.duration_s - FRAME_DIFF_HALF_S, FRAME_DIFF_HALF_S + 1, pe),
                                             gray_frames(next_src, 0, FRAME_DIFF_HALF_S, ne))
    except Exception as e:  # recorded, and decide() keeps the seam split
        # Exceptions from subprocesses may include signed source URLs.
        out['error'] = type(e).__name__
    return out


def media_of(d):
    return Media(tuple(d['video']), tuple(d['audio']), d['start_s'], d['duration_s'], d['frame_s'])


def seam_of(ev):
    return Seam(ev['collation_reason'], ev['db_gap_s'], ev['prev_span_s'],
                media_of(ev['prev_media']) if 'prev_media' in ev else None,
                media_of(ev['next_media']) if 'next_media' in ev else None,
                tuple(ev.get('clock_before', ())), tuple(ev.get('clock_after', ())), ev.get('frame_ratio'))


def verdict(ev):
    if ev['recording_id'] not in CLOCK_CROPS:
        return False, KeepReason.NO_CLOCK_PROFILE, None
    seam = seam_of(ev)
    keep = metadata_keep(seam)
    if keep is None and 'error' in ev:
        return False, KeepReason.EVIDENCE_FAILED, None
    if keep is None and (seam.prev is None or seam.next is None):
        return False, KeepReason.EVIDENCE_FAILED, None
    return decide(seam)


# ---- plan --------------------------------------------------------------------

def read_tsv(path):
    csv.field_size_limit(1 << 30)
    with (gzip.open(path, 'rt', encoding='utf-8') if path.endswith('.gz') else open(path, encoding='utf-8')) as f:
        return list(csv.DictReader(f, delimiter='\t'))


def read_registry(path):
    with open(path, 'rb') as f:
        body = f.read()
    return json.loads(gzip.decompress(body) if body[:2] == b'\x1f\x8b' else body)


def source_identity(registry, fid, key, recording_id):
    f = registry['files'][fid]
    if (f['key'] != key or int(f['recording_id']) != recording_id or not f.get('verified') or
            not re.fullmatch('[0-9a-f]{64}', f['sha256']) or int(f['size_bytes']) <= 0):
        raise ValueError(f'{fid}: exact parent file identity differs')
    if f.get("etag") is None:
        raise ValueError(f'{fid}: parent file lacks an ETag')
    http_headers(f['etag'])
    return {'file': fid, 'key': key, 'sha256': f['sha256'], 'size_bytes': int(f['size_bytes']), 'etag': f['etag']}


def plan(ledger, seams, served_files, recording_id, registry):
    """Every adjacent served seam of the exact published parent, in order."""
    generation = registry['generation']
    if any(r['generation'] != generation or r['published'] != 'y' for r in ledger):
        raise ValueError('ledger is not the exact published parent generation')
    split = {(r['prev_file'], r['next_file']): r for r in seams if int(r['recording_id']) == recording_id}
    prior = {r['file_id']: r for r in served_files if int(r['recording_id']) == recording_id}
    out = []
    for h in ledger:
        if int(h['recording_id']) != recording_id or not h['file_ids']:
            continue
        ids, keys, srcs = h['file_ids'].split(','), h['object_keys'].split(','), h['sources'].split(',')
        if not len(ids) == len(keys) == len(srcs) == int(h['served']) or len(set(ids)) != len(ids):
            raise ValueError(f'{h["hour_id"]}: invalid served file sequence')
        identities = [source_identity(registry, fid, key, recording_id) for fid, key in zip(ids, keys)]
        for k in range(len(ids) - 1):
            seam = split.get((ids[k], ids[k + 1]))
            if seam is None or (seam['prev_key'], seam['next_key']) != (keys[k], keys[k + 1]):
                raise ValueError(f'{h["hour_id"]}: no exact current split-seam row for {ids[k]} -> {ids[k + 1]}')
            src = prior.get(ids[k])
            if src is None or src['key'] != keys[k] or int(src['size']) != identities[k]['size_bytes'] or src['order_ok'] != 'True':
                raise ValueError(f'{ids[k]}: old seam span is not bound to the parent file')
            row = {'generation': generation, 'recording_id': recording_id, 'local_date': h['local_date'],
                   'delivery_hour': int(h['delivery_hour']), 'hour_id': h['hour_id'], 'seam': k + 1, 'served': len(ids),
                   'prev_source': srcs[k], 'next_source': srcs[k + 1],
                   'collation_reason': seam['reason'] if seam['adjacent'] == 'True' else 'nonadjacent_original_parts',
                   'db_gap_s': float(seam['gap_s']), 'prev_span_s': float(src['wall_span_seconds'])}
            for side, identity in (('prev', identities[k]), ('next', identities[k + 1])):
                row.update({f'{side}_{field}': value for field, value in identity.items()})
            out.append(row)
    return out


def parent_lineage(registry, store):
    parts = {}
    for repair in store.load('/public/growth.json')['repairs']:
        fid = 'sha256:' + repair['sha256']
        if fid in parts:
            raise ValueError(f'{fid}: duplicate parent repair')
        parts[fid] = tuple(int(i) for i in repair['input_ids'])
    def get(fid):
        if fid not in registry['files']:
            raise ValueError(f'{fid}: source absent from exact parent')
        return (int(fid),) if fid.isdigit() else parts[fid]
    return get


def check_plan_parent(rows, registry):
    for row in rows:
        if row['generation'] != registry['generation']:
            raise ValueError('plan belongs to a different parent')
        for side in ('prev', 'next'):
            identity = source_identity(registry, row[f'{side}_file'], row[f'{side}_key'], row['recording_id'])
            if any(row[f'{side}_{k}'] != v for k, v in identity.items()):
                raise ValueError('plan source identity differs from exact parent')


def validate_clock_proof(proof, registry, store):
    """Independent publisher gate: the proof must describe every exact parent boundary."""
    if proof['pass_version'] != PASS_VERSION or proof['parent_generation'] != registry['generation']:
        raise ValueError('clock proof is not from the current reviewed reader and parent')
    rid, files, seams = proof['recording_id'], proof['served_inputs'], proof['seams']
    if rid not in CLOCK_CROPS or len(files) < 2 or len(set(files)) != len(files) or len(seams) != len(files) - 1:
        raise ValueError('clock proof has an incomplete boundary sequence')
    parts = parent_lineage(registry, store)
    expected = []
    for fid in files:
        identity = source_identity(registry, fid, registry['files'][fid]['key'], rid)
        expected += [dict(output_id=i, served_file=fid, served_key=identity['key'], served_sha256=identity['sha256'],
                          served_size_bytes=identity['size_bytes']) for i in parts(fid)]
    inputs = proof['inputs']
    if len(expected) != len(inputs) or any(any(actual.get(k) != v for k, v in exp.items()) for exp, actual in zip(expected, inputs)):
        raise ValueError('clock proof original-output lineage differs from exact parent')
    for i, ev in enumerate(seams):
        if (ev['generation'], ev['recording_id'], ev['local_date'], ev['delivery_hour'], ev['hour_id'],
                ev['prev_file'], ev['next_file']) != (proof['parent_generation'], rid, proof['local_date'], proof['delivery_hour'],
                                                     proof['hour_id'], files[i], files[i+1]):
            raise ValueError('clock proof boundary does not match served sequence')
        if i and ev['seam'] != seams[i-1]['seam'] + 1:
            raise ValueError('clock proof boundaries are not consecutive')
        check_plan_parent([ev], registry)
        join, why, delta = verdict(ev)
        if not join or ev.get('join') is not True or ev.get('keep_reason') is not None or ev.get('clock_delta_s') != delta:
            raise ValueError('clock proof contains a boundary without verified advancing-clock evidence')


def seam_id(row):
    return tuple(row[k] for k in ('generation', 'recording_id', 'local_date', 'delivery_hour', 'hour_id', 'seam'))


def checked_verdicts(planned, decisions):
    """Require the entire exact plan and recompute each verdict before grouping runs."""
    if len({(r['generation'], r['recording_id']) for r in planned}) > 1:
        raise ValueError('one exact parent and recording per plan')
    by_id = {seam_id(r): r for r in planned}
    actual = {seam_id(r): r for r in decisions}
    if len(by_id) != len(planned) or len(actual) != len(decisions) or set(by_id) != set(actual):
        raise ValueError('verdicts must match every planned seam exactly once')
    by_hour = collections.defaultdict(list)
    for ident, row in by_id.items():
        ev = actual[ident]
        if any(ev.get(k) != v for k, v in row.items()):
            raise ValueError(f'{ident}: verdict does not describe exact planned seam')
        join, why, delta = verdict(ev)
        if (ev.get('join') is not join or ev.get('keep_reason') != (why.value if why else None) or
                ev.get('clock_delta_s') != delta):
            raise ValueError(f'{ident}: verdict differs from recomputed rule')
        by_hour[ident[:-1]].append(ev)
    for hour, rows in by_hour.items():
        rows.sort(key=lambda r: r['seam'])
        if not re.fullmatch(r'[A-Za-z0-9_.:-]+', hour[-1]):
            raise ValueError('invalid hour identity')
        if [r['seam'] for r in rows] != list(range(1, len(rows) + 1)) or any(r['served'] != len(rows) + 1 for r in rows):
            raise ValueError(f'{hour}: incomplete served sequence')
        for before, after in zip(rows, rows[1:]):
            if any(before[f'next_{f}'] != after[f'prev_{f}'] for f in ('file', 'key', 'sha256', 'size_bytes', 'etag')):
                raise ValueError(f'{hour}: nonconsecutive served file identity')
    return by_hour


# ---- build -------------------------------------------------------------------

def file_md5(path):
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


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


def download(key, path, etag):
    try:
        subprocess.run(['curl', '-fsS', '--retry', '5', '-H', f'If-Match: {etag}', '-o', path + '.tmp', presign(key)],
                       check=True, capture_output=True, timeout=3600)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        raise BuildRefused([f'{key}: exact source download failed']) from None
    os.rename(path + '.tmp', path)


class BuildRefused(Exception):
    """A joined file failed its checks: the run stays split."""


def build_run(run, verdicts, batch, out_dir, work, catalog_parts, fetch=download):
    """Join one run of served files; returns the proof (also written under out_dir)."""
    os.makedirs(work, exist_ok=True)
    if len(run) < 2 or len(verdicts) != len(run) - 1 or any(not verdict(v)[0] for v in verdicts):
        raise BuildRefused(['missing or rejected boundary evidence'])
    expansion = [tuple(int(i) for i in catalog_parts(x['file_id'])) for x in run]
    ids = tuple(i for group in expansion for i in group)
    if (any(not group for group in expansion) or any(i <= 0 for i in ids) or len(set(ids)) != len(ids) or
            any(x['file_id'].isdigit() and group != (int(x['file_id']),) for x, group in zip(run, expansion)) or
            VETOED_PAIRS.intersection(zip(ids, ids[1:]))):
        raise BuildRefused(['invalid or vetoed original-output lineage'])
    local = []
    for x in run:
        path = os.path.join(work, x['file_id'].replace(':', '_') + '.mp4')
        if not os.path.exists(path):
            fetch(x['key'], path, x['etag'])
        if (os.path.getsize(path), sha256_file(path)) != (x['size_bytes'], x['sha256']):
            raise BuildRefused([f'{x["file_id"]}: downloaded bytes differ from exact parent'])
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
        raise BuildRefused(problems)
    sha = sha256_file(tmp_out)
    key = f'joined/{batch}/objects/{sha}.mp4'
    proof = {
        'pass_version': PASS_VERSION, 'parent_generation': run[0]['generation'],
        'production_version': 'clock-prestitched', 'ffmpeg': ffmpeg_version(),
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
        os.makedirs(os.path.dirname(p), exist_ok=True)
    # Retry the exact same create-only pair after an interruption. An existing
    # byte mismatch never gets overwritten or treated as success.
    proof_bytes = (json.dumps(proof, indent=1) + '\n').encode()
    try:
        os.link(tmp_out, obj)
    except FileExistsError:
        if os.path.getsize(obj) != proof['output']['size_bytes'] or sha256_file(obj) != sha:
            raise SystemExit(f'{obj} exists with differing bytes')
    fd, scratch = tempfile.mkstemp(dir=os.path.dirname(prf), prefix='.proof-')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(proof_bytes)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(scratch, prf)
        except FileExistsError:
            with open(prf, 'rb') as f:
                if f.read() != proof_bytes:
                    raise SystemExit(f'{prf} exists with differing bytes')
    finally:
        os.unlink(scratch)
    return proof


# ---- create-only upload ------------------------------------------------------

def head_problems(head, size, sha256, md5):
    """Problems with an object's HEAD (ContentLength, ChecksumSHA256, Metadata.sha256, ETag) against the
    uploaded file's size, sha256 (hex) and MD5 (hex, R2's single-part ETag)."""
    want = (size, base64.b64encode(bytes.fromhex(sha256)).decode(), sha256, md5)
    got = (head.get('ContentLength'), head.get('ChecksumSHA256'), (head.get('Metadata') or {}).get('sha256'), (head.get('ETag') or '').strip('"'))
    return [f'{n} {g!r} != {w!r}' for n, g, w in zip(('size', 'checksum_sha256', 'meta_sha256', 'etag'), got, want) if g != w]


def aws(*args):
    env = dict(os.environ, AWS_ACCESS_KEY_ID=os.environ['R2_ACCESS_KEY_ID'], AWS_SECRET_ACCESS_KEY=os.environ['R2_SECRET_ACCESS_KEY'],
               AWS_DEFAULT_REGION='auto')
    return subprocess.run(['aws', '--endpoint-url', f'https://{os.environ["R2_ACCOUNT_ID"]}.r2.cloudflarestorage.com', 's3api', *args,
                           '--bucket', os.environ['R2_BUCKET']], capture_output=True, text=True, env=env, timeout=7200)


def upload_create_only(key, path, content_type):
    """Upload path to key only if the key does not exist (If-None-Match: *), with a SHA-256 the
    server checks, then verify by HEAD. A key that already exists is never overwritten; it passes
    only if its HEAD matches this file. Returns the verified {key, size_bytes, sha256, etag}."""
    sha, size = sha256_file(path), os.path.getsize(path)
    md5 = file_md5(path)
    put = aws('put-object', '--key', key, '--body', path, '--content-type', content_type, '--if-none-match', '*',
              '--checksum-algorithm', 'SHA256', '--checksum-sha256', base64.b64encode(bytes.fromhex(sha)).decode(),
              '--metadata', f'sha256={sha}')
    if put.returncode and 'PreconditionFailed' not in put.stderr:
        raise SystemExit(f'upload of {key} failed: {put.stderr.strip()[:300]}')
    head = aws('head-object', '--key', key, '--checksum-mode', 'ENABLED')
    if head.returncode:
        raise SystemExit(f'HEAD of {key} failed: {head.stderr.strip()[:300]}')
    problems = head_problems(json.loads(head.stdout), size, sha, md5)
    if problems:
        raise SystemExit(f'{key}: HEAD differs from the uploaded file: {problems}')
    return {'key': key, 'size_bytes': size, 'sha256': sha, 'etag': md5}


# ---- CLI ---------------------------------------------------------------------

def read_jsonl(path):
    with open(path, encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]


def resume_runs(receipts, plan_sha, verdicts_sha, batch, by_hour):
    if any(r.get('plan_sha256') != plan_sha or r.get('verdicts_sha256') != verdicts_sha or r.get('batch') != batch for r in receipts):
        raise ValueError('upload log belongs to a different exact plan, evidence or batch')
    done = {tuple(r['served_inputs']) for r in receipts}
    if len(done) != len(receipts):
        raise ValueError('duplicate upload receipt')
    accepted = set()
    for rows in by_hour.values():
        ids = [rows[0]['prev_file']] + [v['next_file'] for v in rows]
        accepted.update(tuple(r) for r in runs(ids, [v['join'] for v in rows]) if len(r) > 1)
    if not done <= accepted:
        raise ValueError('upload receipt contains a run the current verdicts do not accept')
    return done


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('plan')
    p.add_argument('--ledger', required=True, help='best14-ledger.tsv of the live generation')
    p.add_argument('--registry', required=True, help='exact parent registry')
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
    b.add_argument('--plan', required=True, help='complete original exact-parent seam plan')
    b.add_argument('--registry', required=True, help='exact published parent registry')
    b.add_argument('--assets', required=True, help='parent asset mirror; growth is verified against registry')
    b.add_argument('--batch', required=True, help=f'{BATCH_PREFIX}YYYYMMDD')
    b.add_argument('--out', required=True)
    b.add_argument('--work', required=True)
    b.add_argument('--upload-log', help='create-only upload each checked run; append exact evidence-bound receipt')
    b.add_argument('--shard', default='0/1', help='K/N: build only the runs whose index modulo N is K')
    a = ap.parse_args(argv)

    if a.cmd == 'plan':
        rows = plan(read_tsv(a.ledger), read_tsv(a.seams), read_tsv(a.served_files), a.recording, read_registry(a.registry))
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
        registry = read_registry(a.registry)
        parts = parent_lineage(registry, AssetStore(registry, a.assets))
        planned = read_jsonl(a.plan)
        check_plan_parent(planned, registry)
        by_hour = checked_verdicts(planned, read_jsonl(a.verdicts))
        os.makedirs(a.out, exist_ok=True)
        shard, shards = map(int, a.shard.split('/'))
        if not 0 <= shard < shards:
            raise ValueError('invalid build shard')
        plan_sha = sha256_file(a.plan)
        verdicts_sha = sha256_file(a.verdicts)
        receipts = read_jsonl(a.upload_log) if a.upload_log and os.path.exists(a.upload_log) else []
        done = resume_runs(receipts, plan_sha, verdicts_sha, a.batch, by_hour)
        proofs, refused, idx = [], 0, 0
        for hour, vs in sorted(by_hour.items()):
            hour_id = hour[-1]
            if not any(v['join'] for v in vs):
                continue
            def served_identity(row, side):
                x = {f: row[f'{side}_{f}'] for f in ('key', 'sha256', 'size_bytes', 'etag')}
                x['file_id'] = row[f'{side}_file']
                return x
            served = [served_identity(vs[0], 'prev')] + [served_identity(v, 'next') for v in vs]
            for x in served:
                x.update(generation=vs[0]['generation'], recording_id=vs[0]['recording_id'], local_date=vs[0]['local_date'],
                         delivery_hour=vs[0]['delivery_hour'], hour_id=hour_id)
            k = 0
            for run in runs(served, [v['join'] for v in vs]):
                seam_vs = vs[k:k + len(run) - 1]
                k += len(run)
                if len(run) < 2:
                    continue
                idx += 1
                ids = tuple(x['file_id'] for x in run)
                if idx % shards != shard or ids in done:
                    continue
                work = os.path.join(a.work, hour_id)
                try:
                    proof = build_run(run, seam_vs, a.batch, a.out, work, parts)
                except BuildRefused as e:
                    refused += 1
                    with open(os.path.join(a.out, 'refused.jsonl'), 'a', encoding='utf-8') as f:
                        f.write(json.dumps({'hour_id': hour_id, 'served_inputs': list(ids), 'problems': e.args[0]}) + '\n')
                    print(json.dumps({'hour_id': hour_id, 'refused': e.args[0][:3]}), flush=True)
                    shutil.rmtree(work, ignore_errors=True)
                    continue
                o = proof['output']
                if a.upload_log:
                    obj = os.path.join(a.out, o['key'])
                    prf_key = f'joined/{a.batch}/proofs/{o["sha256"]}.json'
                    up = {'hour_id': hour_id, 'plan_sha256': plan_sha, 'verdicts_sha256': verdicts_sha, 'batch': a.batch, 'served_inputs': list(ids), 'duration_s': o['duration_s'],
                          'object': upload_create_only(o['key'], obj, 'video/mp4'),
                          'proof': upload_create_only(prf_key, os.path.join(a.out, prf_key), 'application/json')}
                    with open(a.upload_log, 'a', encoding='utf-8') as f:
                        f.write(json.dumps(up) + '\n')
                        f.flush()
                        os.fsync(f.fileno())
                    os.remove(obj)
                    shutil.rmtree(work, ignore_errors=True)
                proofs.append(o['key'])
                print(json.dumps({'hour_id': hour_id, 'served_inputs': len(run), 'key': o['key']}), flush=True)
        print(json.dumps({'assemblies': len(proofs), 'refused': refused}))


if __name__ == '__main__':
    main()
