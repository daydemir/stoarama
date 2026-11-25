#!/usr/bin/env python3
"""Prepare read-only NAS cleanup dry-run plans for the active 20-day policy.

The active policy keeps exactly one period per stream: the best 14 local days
shifted to 20 local days, [best-3d, best+17d), the exact manifest pinned by
migration 0236 and embedded in
backend/internal/api/nas_cleanup_keep20_policy.json. A keep-everything stream
stays whole in R2; its NAS copies outside the window are listed like any other
and leave only after the server's live R2 SHA verification. This
script lists every NAS file that lies entirely outside its stream's window:

  * raw clips present on the NAS (nas_inventory_files matching the catalog
    path/size/SHA), and
  * collated outputs acknowledged on the NAS (recording_collation_output_acks
    matching the catalog size/SHA) whose source clips are all outside too.

Files reserved by any non-retired plan or ever issued a deletion permit are
skipped, regardless of policy. Closing a paused plan releases only never-issued
files for a fresh plan; use a new --label-prefix (labels are immutable). Items are
grouped per stream, then per kind and recovery class (R2 copy present vs. R2
purged; NAS-only deletion was removed, so those plans can never be enabled), ordered by
descending bytes per file, and cut into plans of at most --max-items (<=20000) items and --max-bytes
(<=2 TB, the backend's per-plan cap). Labels are deterministic:
<prefix>-s<stream>-<raw|col>[-nasonly]-<n>.
R2-present plans precede NAS-only plans; within each class, plans with more
bytes per file run first. Labels remain bound to their stream/kind/chunk.

With --submit, the admitted plans that can ever be enabled (R2 copy present)
are also grouped into batch manifests (--batch-by stream: one per stream, or
all: one for everything, split at 500 plans): batch-<prefix>-<group>.json with
the ordered plan ids/digests and batch_sha256. The owner approves a batch once
with `stoaramactl nas-cleanup enable-batch --batch-input FILE
--expected-batch-sha256 SHA --reason TEXT --approval-hours H` after the plans'
dry runs are complete; the backend still enables one plan at a time and
re-gates every plan before enabling it.

Default is a read-only preview: the database runs in a READ ONLY transaction
and plan inputs are written to --out for review. --submit admits each plan via
the service API (POST /api/v1/service/nas-cleanup/plans). Admission only creates
a dry-run plan: it never arms or enables deletion. The server re-derives every
identity and re-checks folder ownership, the frozen NAS identity index and the
keep window; a 409 batch is bisected and single rejected items are reported.

Batch (two streams, one approval per stream):
  prepare_keep28_plans.py --stream 3 --stream 4 --out k28 --submit

Pilot (one stream, a few GB):
  prepare_keep28_plans.py --stream 3 --max-bytes 5e9 --max-plans 1 --out pilot
  prepare_keep28_plans.py --stream 3 --max-bytes 5e9 --max-plans 1 --out pilot --submit

--day-plans prepares NAS-only day-directory plans instead (owner, verbatim:
"if its outside of keep everything, then a nas copy only needs to be checked
if its in the keep 20? otherwise it can be deleted right?"; "yes, delete").
A unit is one stream-day of raw NAS footage in one directory: the reviewed
folder map names the stream, the path names the local day, the whole day lies
outside the pinned window with a one-day guard band, and no catalogued clip of
it is held. Its files and bytes are the present NAS inventory of that day; each
plan carries those totals and the day digest, which the server recomputes
before admitting it (dry run only). For a non-Keep-Everything stream no R2 copy
is checked. Labels: <prefix>-s<stream>-day-<n>.

Keep Everything streams (Deniz, verbatim: "nas shoul dnot have any of the keep
everything (except the keep 20)"; "if its the same size and same file name or
whatever checks we have already?") come after the others. A KE unit names its
exact media files, and a file is named only when its NAS size is the
catalogued clip size and its R2 counterpart is live in the catalogue (the raw
object, or a stitched output listing the clip, neither retired) AND a fresh
read-only R2 listing (`r2prune list`, --r2prune) shows that key with that
size. Every other file stays on the NAS and is counted as excluded.

Environment: DATABASE_URL (read-only use) and, for --submit, SERVICE_TOKEN.
"""
import argparse
import base64
import datetime
import hashlib
import http.client
import json
import os
import pathlib
import re
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import zoneinfo
from collections import defaultdict

from reviewed_scope import ReadFailed, admissible_candidates

# The active 20-day cleanup window, the exact manifest migration 0236 pins.
POLICY_SHA = 'bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393'
POLICY_PATH = pathlib.Path(__file__).resolve().parents[2] / 'internal/api/nas_cleanup_keep20_policy.json'
# The superseded 28-day selection stays readable as provenance for the derived
# window; it never admits a new plan.
LEGACY_POLICY_SHA = '060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'
LEGACY_POLICY_PATH = pathlib.Path(__file__).resolve().parents[2] / 'internal/api/nas_cleanup_keep28_policy.json'
FOLDERS_SHA = '94efc6fe9f0b83db55be31d4c2fad7f47cff781fd01d88bc33c4c90cae0e7ab8'
FOLDERS_PATH = pathlib.Path(__file__).resolve().parents[2] / 'internal/api/nas_cleanup_folder_streams.json'
DAY_DIGEST_TAG = 'stoarama-nas-cleanup-day-v1'
MAX_DAY_UNITS = 1000  # backend nasCleanupDayMaxUnits
MAX_DAY_NAMES = 100000  # backend nasCleanupDayMaxNames
R2_BUCKET = os.environ.get('R2_BUCKET', 'stoarama')
MONTHS = ('January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December')
WEEKDAYS = ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
DAY_PLAZA_DIR = re.compile(r'(?:[^/]+/)+(%s)/(0[1-9]|[12][0-9]|3[01])-(%s)' % ('|'.join(MONTHS), '|'.join(WEEKDAYS)))
DAY_FLAT_DIR = re.compile(r'(?:[^/]+/)+[1-9][0-9]*/(continuous|[1-9][0-9]*)')
DAY_PLAZA_FILE = re.compile(r'[^/]*_(20[0-9]{2})_(%s)_W([1-6])_(%s)_hour_(?:[0-9]{2}|[0-9]{6})\.mp4' % ('|'.join(MONTHS), '|'.join(WEEKDAYS)))
DAY_SECONDS = re.compile(r'([1-9][0-9]{9})\.mp4')
DAY_MILLIS = re.compile(r'([1-9][0-9]{12})\.mp4')
LABEL = re.compile(r'^[a-z0-9][a-z0-9._-]{0,79}$')
MAX_PLAN_ITEMS = 20000
MAX_PLAN_BYTES = 2_000_000_000_000  # backend nasCleanupMaxPlanBytes
MAX_BATCH_PLANS = 500
# A read failed before any submission: nothing was admitted and the runner may
# retry this stream later. Every other failure stops the runner.
EXIT_READ_FAILED = 4
EXIT_ADMISSION_REJECTED = 5
# Mirrors the runner's SHORT_READ_RETRY_DELAYS (120 s) for an unconfirmed day admission.
ADMISSION_RETRY_DELAYS = (5, 10, 20, 30, 30, 25, None)
TRANSIENT_HTTP = {408, 429, 500, 502, 503, 504}


class AdmissionRejected(RuntimeError):
    """A confirmed HTTP 409 from the atomic admission POST, never a later GET."""
    def __init__(self, label, reason):
        super().__init__("atomic admission rejected")
        self.label, self.reason = label, reason


def batch_digest(policy_sha, plans):
    """Same digest as the backend's cleanupBatchDigest and stoaramactl."""
    text = 'stoarama-nas-cleanup-batch-v1\npolicy %s\n' % policy_sha
    text += ''.join('plan %d %s\n' % (int(p['id']), p['plan_sha256']) for p in plans)
    return hashlib.sha256(text.encode()).hexdigest()


def batch_manifests(receipts, prefix, by):
    """Ordered batch manifests over admitted plans that can ever be enabled."""
    groups = {}
    for r in receipts:
        if '-nasonly-' in r['label']:
            continue
        key = 's%d' % r['stream_id'] if by == 'stream' else 'all'
        groups.setdefault(key, []).append(r)
    out = []
    for key, plans in sorted(groups.items()):
        for n in range(0, len(plans), MAX_BATCH_PLANS):
            part = plans[n:n + MAX_BATCH_PLANS]
            name = '%s-%s' % (prefix, key) + ('-%d' % (n // MAX_BATCH_PLANS + 1) if len(plans) > MAX_BATCH_PLANS else '')
            entries = [{'id': p['id'], 'label': p['label'], 'plan_sha256': p['plan_sha256'], 'items': p['items'], 'bytes': p['bytes']} for p in part]
            out.append({'name': name, 'policy_sha256': POLICY_SHA, 'plans': entries, 'files': sum(p['items'] for p in part),
                        'bytes': sum(p['bytes'] for p in part), 'batch_sha256': batch_digest(POLICY_SHA, entries)})
    return out


def load_legacy_policy(path=None):
    """The retained 28-day selection the active window is derived from."""
    raw = pathlib.Path(path or LEGACY_POLICY_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != LEGACY_POLICY_SHA:
        raise SystemExit('keep28 policy SHA differs from the retained policy')
    rows = json.loads(raw)
    ids = [r['stream_id'] for r in rows]
    if len(rows) != 109 or len(set(ids)) != 109:
        raise SystemExit('keep28 policy must hold exactly one window for each of 109 streams')
    return {r['stream_id']: r for r in rows}


def load_policy(path, legacy=None):
    """Validate the active 20-day manifest against the retained 28-day one.

    Each stream keeps its identity and canonical timezone, sits on local
    midnights, spans exactly 20 local days [best-3d, best+17d), and stays inside
    the reviewed 28-day window. Returns the same row shape the candidate query
    consumes (`tz`, `keep_start_utc`, `keep_end_utc`) plus the keep-everything
    pin. DST windows are 481 UTC hours, so the span is checked in local days.
    """
    raw = pathlib.Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != POLICY_SHA:
        raise SystemExit('keep20 policy SHA differs from the pinned policy')
    rows = json.loads(raw)
    ids = [r['stream_id'] for r in rows]
    if len(rows) != 109 or len(set(ids)) != 109:
        raise SystemExit('keep20 policy must hold exactly one window for each of 109 streams')
    legacy = legacy if legacy is not None else load_legacy_policy()
    out = {}
    for row in rows:
        sid, tz = row['stream_id'], row['timezone']
        if sid not in legacy or legacy[sid]['tz'] != tz:
            raise SystemExit('keep20 stream %d identity or timezone differs' % sid)
        start = datetime.datetime.fromisoformat(row['start_at']).astimezone(zoneinfo.ZoneInfo(tz))
        end = datetime.datetime.fromisoformat(row['end_at']).astimezone(zoneinfo.ZoneInfo(tz))
        if start.time() != datetime.time(0) or end.time() != datetime.time(0):
            raise SystemExit('keep20 stream %d bounds are not local midnights' % sid)
        best = datetime.date.fromisoformat(legacy[sid]['best_start'])
        if start.date() != best - datetime.timedelta(days=3) or end.date() != best + datetime.timedelta(days=17):
            raise SystemExit('keep20 stream %d window is not [best-3d, best+17d)' % sid)
        if (end.date() - start.date()).days != 20:
            raise SystemExit('keep20 stream %d window is not 20 local days' % sid)
        if row['start_at'] < legacy[sid]['keep_start_utc'] or row['end_at'] > legacy[sid]['keep_end_utc']:
            raise SystemExit('keep20 stream %d window exceeds the legacy window' % sid)
        out[sid] = dict(row, tz=tz, keep_start_utc=row['start_at'], keep_end_utc=row['end_at'],
                        keep_everything=bool(row['keep_everything']))
    return out


def pg_env():
    url = urllib.parse.urlsplit(os.environ['DATABASE_URL'])
    env = os.environ.copy()
    env.update(PGHOST=url.hostname or '', PGPORT=str(url.port or 5432),
               PGUSER=urllib.parse.unquote(url.username or ''), PGPASSWORD=urllib.parse.unquote(url.password or ''),
               PGDATABASE=url.path.lstrip('/'), PGCONNECT_TIMEOUT='10',
               PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=300000 -c work_mem=16MB -c timezone=UTC')
    for k, v in urllib.parse.parse_qsl(url.query):
        if k in ('sslmode', 'connect_timeout'):
            env['PG' + k.upper()] = v
    return env


def transient_sql_failure(stderr):
    """Fixed reason when psql stderr (VERBOSITY sqlstate or verbose) shows a
    retryable read failure; None for any other failure, which must stop."""
    codes = set(re.findall(r'(?:ERROR|FATAL|PANIC):\s+([0-9A-Z]{5})\b', stderr))
    connection = {c for c in codes if c.startswith('08') or c in {'53300', '57P01', '57P02', '57P03', '40001', '40P01', '55P03'}}
    if codes - connection - {'57014'} or 'PANIC' in stderr:
        return None  # Any other reported condition, or any PANIC, must stop.
    if codes == {'57014'}:
        return 'statement timeout'
    if codes or any(x in stderr for x in ('connection to server', 'server closed the connection', 'SSL SYSCALL')):
        return 'database connection or transaction unavailable'
    return None


def query(sql, psql):
    try:
        p = subprocess.run([psql, '-XqAt', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=verbose'], input=('BEGIN READ ONLY;' + sql + ';ROLLBACK;').encode(),
                           env=pg_env(), capture_output=True, timeout=330)
    except subprocess.TimeoutExpired:
        raise ReadFailed('read-only catalogue query timed out') from None
    if p.returncode:
        error = p.stderr.decode(errors='replace')
        raise (ReadFailed if transient_sql_failure(error) else RuntimeError)('read-only catalogue query failed: ' + error[:400])
    return json.loads(p.stdout)


def candidates(policy, streams, bucket, psql, scope_cache=None):
    # One bounded read-only query per stream keeps each transaction short.
    rows = []
    for sid in sorted(streams or policy):
        part = stream_candidates(policy[sid], bucket, psql)
        rows += part
        print(json.dumps({'phase': 'listed_stream', 'stream_id': sid, 'files': len(part), 'bytes': sum(r['size_bytes'] for r in part)}), file=sys.stderr, flush=True)
    return admissible_candidates(rows, scope_cache)


def stream_candidates(w, bucket, psql):
    # Keep the stream estimate at one, fence hold checks behind materialized
    # identity candidates, and parameterize both large-table reads. Without
    # these boundaries PostgreSQL pushed delivery_nas_held through the UNION
    # into whole-catalog scans before restricting the requested stream.
    windows = [{'stream_id': w['stream_id'], 's': w['keep_start_utc'], 'e': w['keep_end_utc']}]
    encoded = base64.b64encode(json.dumps(windows).encode()).decode()
    bucket_literal = "'" + bucket.replace("'", "''") + "'"
    return query("""WITH w AS MATERIALIZED (
 SELECT (x->>'stream_id')::bigint stream_id,(x->>'s')::timestamptz s,(x->>'e')::timestamptz e
 FROM jsonb_array_elements(convert_from(decode('""" + encoded + """','base64'),'UTF8')::jsonb) x LIMIT 1
), raw AS MATERIALIZED (
 SELECT 'raw' kind,c.id source_id,r.stream_id,c.display_path path,c.size_bytes,lower(c.sha256) sha256,(c.purged_at IS NOT NULL) r2_purged
 FROM w JOIN recordings r ON r.stream_id=w.stream_id
 CROSS JOIN LATERAL (SELECT c.* FROM recording_clips c WHERE c.recording_id=r.id OFFSET 0) c
 CROSS JOIN LATERAL (SELECT f.* FROM nas_inventory_files f WHERE f.connection_id=13 AND f.clip_id=c.id OFFSET 0) f
 WHERE f.state='present' AND f.relative_path=c.display_path AND f.sha256=lower(c.sha256) AND f.size_bytes=c.size_bytes
  AND r.account_id=47 AND c.bucket=""" + bucket_literal + """ AND c.size_bytes>0
  AND c.object_key<>'' AND lower(c.sha256) ~ '^[0-9a-f]{64}$'
  AND c.clip_end_at>c.clip_start_at AND c.clip_start_at>='2020-01-01' AND c.clip_end_at<'2028-01-01'
  AND NOT (c.clip_start_at<w.e AND c.clip_end_at>w.s)
), col AS MATERIALIZED (
 SELECT 'collated' kind,o.id source_id,r.stream_id,'joined/'||o.nas_relative_path path,o.size_bytes,lower(o.sha256) sha256,false r2_purged
 FROM recording_collation_outputs o JOIN recording_collation_output_acks a ON a.output_id=o.id AND a.connection_id=13 AND a.sha256=lower(o.sha256) AND a.size_bytes=o.size_bytes
 JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id JOIN w ON w.stream_id=r.stream_id
 WHERE r.account_id=47 AND o.end_at>o.start_at AND o.start_at>='2020-01-01' AND o.end_at<'2028-01-01' AND NOT (o.start_at<w.e AND o.end_at>w.s)
  AND o.size_bytes>0 AND o.object_key<>'' AND lower(o.sha256) ~ '^[0-9a-f]{64}$'
  AND cardinality(o.source_clip_ids) BETWEEN 1 AND 10000
  AND NOT EXISTS (SELECT 1 FROM unnest(o.source_clip_ids) sid LEFT JOIN recording_clips c ON c.id=sid LEFT JOIN recordings cr ON cr.id=c.recording_id
   WHERE c.id IS NULL OR cr.stream_id IS DISTINCT FROM r.stream_id OR c.bucket IS DISTINCT FROM """ + bucket_literal + """
    OR NOT (c.clip_end_at>c.clip_start_at) OR (c.clip_start_at<w.e AND c.clip_end_at>w.s))
), candidates AS MATERIALIZED (
 SELECT * FROM raw UNION ALL SELECT * FROM col
), planned AS MATERIALIZED (
 SELECT DISTINCT p.kind,p.source_id FROM nas_cleanup_reserved_files p
 JOIN candidates c ON c.kind=p.kind AND c.source_id=p.source_id
 WHERE p.account_id=47 AND p.connection_id=13
)
SELECT coalesce(json_agg(x ORDER BY x.stream_id,x.kind DESC,x.r2_purged,x.size_bytes DESC,x.path),'[]')
FROM candidates x WHERE NOT delivery_nas_held(x.kind,x.source_id,x.path,x.sha256,x.size_bytes) AND NOT EXISTS (SELECT 1 FROM planned p WHERE p.kind=x.kind AND p.source_id=x.source_id)""", psql)


def batches(rows, prefix, max_items, max_bytes):
    groups = {}
    for r in rows:
        key = (r['stream_id'], 'raw' if r['kind'] == 'raw' else 'col', r['r2_purged'])
        groups.setdefault(key, []).append(r)
    out = []
    for (sid, kind, purged), items in sorted(groups.items()):
        n, current, size = 0, [], 0
        for r in sorted(items, key=lambda r: (-r['size_bytes'], r['path'])):
            if current and (len(current) >= max_items or (max_bytes and size + r['size_bytes'] > max_bytes)):
                n += 1
                out.append((f"{prefix}-s{sid}-{kind}{'-nasonly' if purged else ''}-{n:04d}", current))
                current, size = [], 0
            current.append(r)
            size += r['size_bytes']
        if current:
            n += 1
            out.append((f"{prefix}-s{sid}-{kind}{'-nasonly' if purged else ''}-{n:04d}", current))
    for label, _ in out:
        assert LABEL.match(label), label
    # The fixed per-file durability cost dominates small-file deletion. Prefer
    # plans reclaiming more bytes per operation; recovery-present work wins.
    return sorted(out, key=lambda p: (p[1][0]['r2_purged'],
                  -sum(r['size_bytes'] for r in p[1]) / len(p[1]), p[0]))


def load_folders(path=None):
    raw = pathlib.Path(path or FOLDERS_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != FOLDERS_SHA:
        raise SystemExit('reviewed folder map SHA differs')
    return json.loads(raw)


def folder_key(directory):
    """The NAS client's cleanup_folder_key for a raw directory."""
    parts = directory.split('/')
    if parts[0] == 'joined':
        return None
    if parts[0] == 'recordings':
        return 'recordings/' + parts[1] if len(parts) > 1 else None
    return parts[0]


def day_file_date(directory, name, zone):
    """Local date a raw media name in directory names, or None (server and client share the rule)."""
    plaza = DAY_PLAZA_DIR.fullmatch(directory)
    if plaza:
        named = DAY_PLAZA_FILE.fullmatch(name)
        if not named or named[2] != plaza[1] or named[4] != plaza[3]:
            return None
        day = int(plaza[2])
        try:
            date = datetime.date(int(named[1]), MONTHS.index(plaza[1]) + 1, day)
        except ValueError:
            return None
        return date if WEEKDAYS[date.weekday()] == plaza[3] and (day - 1) // 7 + 1 == int(named[3]) else None
    flat = DAY_FLAT_DIR.fullmatch(directory)
    named = flat and (DAY_SECONDS if flat[1] == 'continuous' else DAY_MILLIS).fullmatch(name)
    if not named:
        return None
    return datetime.datetime.fromtimestamp(int(named[1]) / (1 if flat[1] == 'continuous' else 1000), zone).date()


def pg_timestamp(value):
    """A PostgreSQL JSON timestamptz (any fraction width) for Python 3.9."""
    match = re.fullmatch(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})', value)
    if not match:
        raise RuntimeError('invalid catalogue timestamp')
    return datetime.datetime.fromisoformat(match[1] + '.' + (match[2] or '').ljust(6, '0')[:6] + match[3].replace('Z', '+00:00'))


def day_outside(window, date):
    """The whole local day lies outside the window with a full guard day each side."""
    zone = zoneinfo.ZoneInfo(window['tz'])
    first = datetime.datetime.fromisoformat(window['keep_start_utc']).astimezone(zone).date()
    last = (datetime.datetime.fromisoformat(window['keep_end_utc']) - datetime.timedelta(microseconds=1)).astimezone(zone).date()
    return date <= first - datetime.timedelta(days=2) or date >= last + datetime.timedelta(days=2)


def day_digest(units):
    """Same digest as the backend's cleanupDayDigest and the runner."""
    text = '%s\npolicy %s\n' % (DAY_DIGEST_TAG, POLICY_SHA)
    for u in units:
        text += 'unit %d %s %d %d %s\n' % (u['stream_id'], u['local_date'], u['files'], u['bytes'], u['relative_dir'])
        text += ''.join('name %d %s\n' % (n['size_bytes'], n['name']) for n in u.get('names', ()))
    return hashlib.sha256(text.encode()).hexdigest()


def r2_live_raw(clip, bucket):
    """SQL: the clip's raw R2 object is live in the catalogue (the server's cleanupDayR2LiveSQL raw branch)."""
    return ("(%s.bucket=%s AND %s.purged_at IS NULL AND %s.object_key<>'' AND NOT EXISTS(SELECT 1 FROM r2_object_retirements x "
            "WHERE x.kind='raw' AND x.source_id=%s.id))" % (clip, "'" + bucket.replace("'", "''") + "'", clip, clip, clip))


# SQL: a stitched output is live in R2 (the server's cleanupDayR2LiveSQL stitched branch).
R2_LIVE_OUTPUT = ("NOT EXISTS(SELECT 1 FROM r2_object_retirements x WHERE x.kind='collated' AND x.source_id=o.id) "
                  "AND NOT EXISTS(SELECT 1 FROM r2_retirement_reservations x WHERE x.kind='collated' AND x.source_id=o.id)")


def day_stream_rows(w, psql, bucket=R2_BUCKET):
    """Every catalogued clip of one stream: path, bounds, held flag, present NAS size (-1 if absent), id,
    catalogued size, live raw R2 key (or null) and when the NAS last reported it; a Keep Everything stream
    also lists its live stitched outputs."""
    sid, ke = int(w['stream_id']), 'true' if w['keep_everything'] else 'false'
    outputs = ("""(SELECT coalesce(json_agg(json_build_array(o.object_key,o.size_bytes,o.source_clip_ids)),'[]')
  FROM recordings r JOIN recording_collation_hours h ON h.recording_id=r.id JOIN recording_collation_outputs o ON o.collation_hour_id=h.id
  WHERE r.account_id=47 AND r.stream_id=""" + str(sid) + " AND " + R2_LIVE_OUTPUT + ")") if w['keep_everything'] else "'[]'::json"
    return query("""SELECT json_build_object('clips',(SELECT coalesce(json_agg(json_build_array(c.display_path,c.clip_start_at,c.clip_end_at,
  nas_cleanup_day_clip_held(c.id,""" + ke + """),coalesce((SELECT f.size_bytes FROM nas_inventory_files f WHERE f.connection_id=13 AND f.clip_id=c.id
   AND f.state='present' AND f.relative_path=c.display_path),-1),c.id,c.size_bytes,CASE WHEN """ + r2_live_raw('c', bucket) + """ THEN c.object_key END,
  (SELECT f.client_updated_at FROM nas_inventory_files f WHERE f.connection_id=13 AND f.clip_id=c.id AND f.relative_path=c.display_path))),'[]')
  FROM recordings r JOIN recording_clips c ON c.recording_id=r.id
  WHERE r.account_id=47 AND r.stream_id=""" + str(sid) + """ AND c.display_path<>''),
 'outputs',""" + outputs + """,
 'claimed',(SELECT coalesce(json_agg(json_build_array(u.relative_dir,u.local_date::text,u.state,u.names,u.updated_at)),'[]') FROM nas_cleanup_day_units u
  WHERE u.stream_id=""" + str(sid) + """ AND u.state IN ('pending','permitted','deleted')),
 'held_dirs',(SELECT coalesce(json_agg(DISTINCT regexp_replace(h.relative_path,'/[^/]*$','')),'[]')
  FROM delivery_keep_holds h WHERE """ + ke + """ AND h.relative_path<>''),
 'keep20_active',EXISTS(SELECT 1 FROM nas_media_keep20_epoch WHERE connection_id=13))""", psql)


def list_roots(keys):
    """The r2prune lane's listing roots: each key's folder three levels deep; nested roots collapse to the outer."""
    roots = []
    for d in sorted({'/'.join(k.split('/')[:3]) + '/' if k.count('/') >= 3 else k.rsplit('/', 1)[0] + '/' for k in keys}):
        if not roots or not d.startswith(roots[-1]):
            roots.append(d)
    return roots


def r2_listing(r2prune, out):
    """A read-only R2 lister: {key: size} for the wanted keys, from `r2prune list` (lane TSV:
    key, size, etag, mtime) over their roots, kept at `out`. R2 credentials come from the environment."""
    def listed(keys):
        if not keys:
            return {}
        args = [a for r in list_roots(keys) for a in ('--prefix', r)]
        subprocess.run([r2prune, 'list', '--workers', '8', '--out', str(out)] + args, check=True, timeout=3600)
        sizes = {}
        with open(out) as f:
            for line in f:
                key, size, _etag, _mtime = line.rstrip('\n').split('\t')
                if key in keys:
                    sizes[key] = int(size)
        return sizes
    return listed


def day_units(w, folders, facts, listed=None):
    """Admissible units of one stream, the server's rule applied to the same catalogue, and the
    Keep Everything media excluded for lacking a listed R2 counterpart: (units, {files, bytes})."""
    sid, ke = w['stream_id'], w['keep_everything']
    excluded = {'files': 0, 'bytes': 0}
    if not facts['keep20_active']:
        return [], excluded
    zone = zoneinfo.ZoneInfo(w['tz'])
    start = datetime.datetime.fromisoformat(w['keep_start_utc'])
    end = datetime.datetime.fromisoformat(w['keep_end_utc'])
    # Live units, and deleted ones: the inventory lists deleted files as present until the next NAS rescan.
    # A KE directory-day is replanned once deleted: a deleted unit's name stays out only until the NAS
    # reports that file again after the deletion (a kept size mismatch or a file restored later).
    claimed = {(d, day) for d, day, state, _, _ in facts['claimed'] if not ke or state != 'deleted'}
    deleted_names = {}
    for d, _, state, names, at in facts['claimed']:
        for n in names or ():
            if state == 'deleted':
                deleted_names[d, n['name']] = max(pg_timestamp(at), deleted_names.get((d, n['name']), pg_timestamp(at)))
    held_dirs = set(facts['held_dirs'])
    groups, refused = defaultdict(list), set()
    for path, a, b, held, size, cid, catalogued, raw_key, seen in facts['clips']:
        directory, name = path.rsplit('/', 1) if '/' in path else ('', path)
        if not (DAY_PLAZA_DIR.fullmatch(directory) or DAY_FLAT_DIR.fullmatch(directory)) or folders.get(folder_key(directory)) != sid:
            continue
        date = day_file_date(directory, name, zone)
        if date is None:
            refused.add(directory)  # The server refuses a directory with an undated catalogued name.
            continue
        if seen is not None and (directory, name) in deleted_names and pg_timestamp(seen) <= deleted_names[directory, name]:
            size = -1  # Unlinked by a deleted unit; the inventory has not rescanned it since.
        groups[directory, date].append((pg_timestamp(a), pg_timestamp(b), held, size, cid, name, catalogued, raw_key))
    eligible = [((directory, date), clips) for (directory, date), clips in sorted(groups.items(), key=lambda g: (g[0][1], g[0][0]))
                if not (directory in refused or directory in held_dirs or (directory, date.isoformat()) in claimed
                        or not any(c[3] >= 0 for c in clips) or not day_outside(w, date)
                        or any(held or not (b > a) or (a < end and b > start) for a, b, held, *_ in clips))]
    counterparts = defaultdict(list)  # clip id -> [(R2 key, size)]
    if ke:
        for key, size, sources in facts['outputs']:
            for cid in sources:
                counterparts[cid].append((key, size))
        for _, clips in eligible:
            for c in clips:
                if c[7]:
                    counterparts[c[4]].append((c[7], c[6]))
        sizes = listed({k for _, clips in eligible for c in clips if c[3] >= 0 for k, _ in counterparts[c[4]]})
    units = []
    for (directory, date), clips in eligible:
        present = [c for c in clips if c[3] >= 0]
        names = present
        if ke:
            names = sorted((c for c in present if c[3] == c[6] and any(sizes.get(k) == z for k, z in counterparts[c[4]])),
                           key=lambda c: c[5])
            excluded['files'] += len(present) - len(names)
            excluded['bytes'] += sum(c[3] for c in present) - sum(c[3] for c in names)
        if not names:
            continue
        unit = {'relative_dir': directory, 'local_date': date.isoformat(), 'stream_id': sid,
                'files': len(names), 'bytes': sum(c[3] for c in names)}
        if ke:
            unit['names'] = [{'name': c[5], 'size_bytes': c[3]} for c in names]
        units.append(unit)
    return units, excluded


def day_plans(units, prefix, max_units):
    """Plans of at most max_units units and MAX_DAY_NAMES Keep Everything names each."""
    parts = []
    for u in units:
        if not parts or len(parts[-1]) >= max_units or sum(len(x.get('names', ())) for x in parts[-1]) + len(u.get('names', ())) > MAX_DAY_NAMES:
            parts.append([])
        parts[-1].append(u)
    out = []
    for n, part in enumerate(parts, 1):
        label = '%s-s%d-day-%04d' % (prefix, part[0]['stream_id'], n)
        assert LABEL.match(label), label
        out.append({'label': label, 'policy_sha256': POLICY_SHA, 'plan_sha256': day_digest(part),
                    'files': sum(u['files'] for u in part), 'bytes': sum(u['bytes'] for u in part), 'units': part})
    return out


def validate_day_admission(plan, result, detail):
    try:
        valid = (type(result['id']) is int and result['id'] > 0 and detail['id'] == result['id']
                 and result['plan_sha256'] == detail['plan_sha256'] == plan['plan_sha256'] == day_digest(detail['units'])
                 and detail['label'] == plan['label'] and detail['mode'] == 'dry_run' and detail['policy_sha256'] == POLICY_SHA
                 and detail['files'] == plan['files'] == sum(u['files'] for u in detail['units'])
                 and detail['bytes'] == plan['bytes'] == sum(u['bytes'] for u in detail['units'])
                 and [(u['relative_dir'], u['local_date'], u['stream_id'], u['files'], u['bytes'], u.get('names')) for u in detail['units']]
                 == [(u['relative_dir'], u['local_date'], u['stream_id'], u['files'], u['bytes'], u.get('names')) for u in plan['units']]
                 and all(u['state'] == 'pending' and u['receipts'] == [] for u in detail['units']))
    except (KeyError, TypeError):
        valid = False
    if not valid:
        raise RuntimeError('admitted day plan contract differs; reconcile its label before retrying')


def day_admitted(plan, psql):
    """Read-only: the id of the day plan holding this proposal's label, or None."""
    rows = query("SELECT coalesce(json_agg(json_build_array(id,plan_sha256)),'[]') FROM nas_cleanup_day_plans "
                 "WHERE account_id=47 AND connection_id=13 AND label='%s'" % plan['label'], psql)
    if rows and rows[0][1] != plan['plan_sha256']:
        raise RuntimeError('day plan label %s holds another digest; reconcile before retrying' % plan['label'])
    return rows[0][0] if rows else None


def day_admit(api, plan, psql):
    """Admit one day plan: its id, or (None, the HTTP 409) for a refusal. A 5xx or transport
    failure can follow a commit (the request outlives the proxy), so before any retry or
    refusal decision read the label back and adopt a plan already admitted under it."""
    for delay in ADMISSION_RETRY_DELAYS:
        refusal = None
        try:
            return service(api, 'POST', 'day-plans', plan)['id'], None
        except urllib.error.HTTPError as exc:
            if exc.code != 409 and exc.code not in TRANSIENT_HTTP:
                raise RuntimeError('day plan admission refused for %s: HTTP %d' % (plan['label'], exc.code)) from None
            refusal = exc if exc.code == 409 else None
        except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.IncompleteRead):
            pass
        try:
            pid = day_admitted(plan, psql)
        except ReadFailed:
            pid = None
            if refusal:
                raise RuntimeError('day plan admission refusal for %s could not be read back' % plan['label']) from None
        if pid or refusal:
            return pid, None if pid else refusal
        if delay is None:
            raise RuntimeError('day plan admission not confirmed for %s; inspect the label before retrying' % plan['label'])
        time.sleep(delay)


def day_main(a, policy):
    """Prepare (and with --submit admit) NAS-only day-directory plans."""
    if a.max_units < 1 or a.max_units > MAX_DAY_UNITS:
        raise SystemExit('--max-units must be 1..%d' % MAX_DAY_UNITS)
    folders = load_folders()
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    units, excluded = [], {'files': 0, 'bytes': 0}
    # Keep Everything streams after the others.
    streams = sorted(a.stream or policy, key=lambda sid: (policy[sid]['keep_everything'], sid))
    for sid in streams:
        ke = policy[sid]['keep_everything']
        if ke and not a.r2prune:
            raise SystemExit('Keep Everything day plans need --r2prune for the read-only R2 listing')
        listed = r2_listing(a.r2prune, out / ('r2-listing-s%d.tsv' % sid)) if ke else None
        part, missing = day_units(policy[sid], folders, day_stream_rows(policy[sid], a.psql, a.bucket), listed)
        units += part
        excluded = {k: excluded[k] + missing[k] for k in excluded}
        print(json.dumps({'phase': 'listed_stream_days', 'stream_id': sid, 'keep_everything': ke, 'units': len(part),
                          'files': sum(u['files'] for u in part), 'bytes': sum(u['bytes'] for u in part),
                          'excluded_no_r2_files': missing['files'], 'excluded_no_r2_bytes': missing['bytes']}), file=sys.stderr, flush=True)
    plans = []
    for sid in streams:
        plans += day_plans([u for u in units if u['stream_id'] == sid], a.label_prefix, a.max_units)
    plans = [] if a.summary_only else plans[:a.max_plans or None]
    for plan in plans:
        (out / (plan['label'] + '.json')).write_text(json.dumps(plan))
    summary = {'policy_sha256': POLICY_SHA, 'mode': 'day_plans', 'streams': sorted(a.stream or policy), 'observed_at_epoch': time.time(),
               'candidate_units': len(units), 'candidate_files': sum(u['files'] for u in units),
               'candidate_bytes': sum(u['bytes'] for u in units),
               'excluded_no_r2_files': excluded['files'], 'excluded_no_r2_bytes': excluded['bytes'],
               'plans': [{k: p[k] for k in ('label', 'plan_sha256', 'files', 'bytes')} | {'units': len(p['units']), 'stream_id': p['units'][0]['stream_id']}
                         for p in plans], 'receipts': [], 'deletion_enabled': False}
    for plan in plans if a.submit else ():
        pid, refusal = day_admit(a.api, plan, a.psql)
        if refusal:
            if summary['receipts'] or len(plans) != 1 or len(a.stream) != 1:
                raise RuntimeError('day plan admission refused for %s: HTTP 409' % plan['label'])
            evidence = dict(stream_id=a.stream[0], label=plan['label'], http_status=409,
                            reason=refusal.read(2048).decode(errors='replace'), rejected_at_epoch=time.time())
            with (out / 'admission-rejected.json').open('x') as f:
                json.dump(evidence, f); f.flush(); os.fsync(f.fileno())
            return EXIT_ADMISSION_REJECTED
        detail = service(a.api, 'GET', 'day-plans/' + str(pid))
        validate_day_admission(plan, {'id': pid, 'plan_sha256': plan['plan_sha256']}, detail)
        summary['receipts'].append({'id': pid, 'label': plan['label'], 'stream_id': plan['units'][0]['stream_id'],
                                    'plan_sha256': plan['plan_sha256'], 'units': len(plan['units']),
                                    'files': plan['files'], 'bytes': plan['bytes']})
        print(json.dumps({'phase': 'day_plan_admitted_dry_run', **summary['receipts'][-1]}), flush=True)
    temp = out / 'summary.tmp'
    temp.write_text(json.dumps(summary, indent=1))
    temp.replace(out / 'summary.json')
    print(json.dumps({'phase': 'DAY_PLANS_SUBMITTED' if a.submit else 'DAY_PLANS_PREVIEW', 'candidate_units': summary['candidate_units'],
                      'candidate_files': summary['candidate_files'], 'candidate_bytes': summary['candidate_bytes'],
                      'excluded_no_r2_files': excluded['files'], 'excluded_no_r2_bytes': excluded['bytes'],
                      'plans': len(plans), 'admitted': len(summary['receipts']), 'out': str(out), 'deletion_enabled': False}), flush=True)
    return 0


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError('operator redirect refused')


def service(api, method, path, body=None):
    req = urllib.request.Request(api.rstrip('/') + '/api/v1/service/nas-cleanup/' + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={'Authorization': 'Bearer ' + os.environ['SERVICE_TOKEN'], 'User-Agent': 'stoarama-operator/1', 'Content-Type': 'application/json'})
    with urllib.request.build_opener(NoRedirect()).open(req, timeout=300) as response:
        # Plan status is ~2.8 KB per completed item; 64 MiB covers the
        # server's 20000-item plan cap.
        return json.loads(response.read(64 << 20))


def submit(api, label, rows, receipts, rejected, atomic=False):
    try:
        result = service(api, 'POST', 'plans', {'label': label, 'items': [{'kind': r['kind'], 'source_id': r['source_id']} for r in rows]})
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise RuntimeError(f'admission not confirmed for {label}: HTTP {exc.code}; inspect the label before retrying')
        if atomic:
            reason = exc.read(2048).decode(errors='replace')
            exc.close()
            raise AdmissionRejected(label, reason) from None
        if len(rows) == 1:
            rejected.append({'label': label, 'kind': rows[0]['kind'], 'source_id': rows[0]['source_id'], 'path': rows[0]['path'], 'reason': exc.read(2000).decode(errors='replace')})
            return
        mid = len(rows) // 2
        submit(api, label + 'a', rows[:mid], receipts, rejected, atomic=atomic)
        submit(api, label + 'b', rows[mid:], receipts, rejected, atomic=atomic)
        return
    detail = service(api, 'GET', 'plans/' + str(result['id']))
    validate_admission(result, detail, label, rows)
    receipts.append({'id': result['id'], 'label': label, 'stream_id': rows[0]['stream_id'], 'plan_sha256': result['plan_sha256'], 'items': len(rows), 'bytes': sum(r['size_bytes'] for r in rows), 'master_deletion_enabled': detail['master_deletion_enabled']})
    print(json.dumps({'phase': 'plan_admitted_dry_run', **receipts[-1]}), flush=True)


def validate_admission(result, detail, label, rows):
    # Admission is unarmed and read-only regardless of the global master.
    # Only the runner's independent enable gates may arm this plan.
    try:
        expected = [(r['kind'], r['source_id']) for r in rows]
        actual = [(i['kind'], i['source_id']) for i in detail['items']]
        reports = detail['results']
        valid = (type(detail['master_deletion_enabled']) is bool
                 and type(result['id']) is int and result['id'] > 0
                 and detail['id'] == result['id'] and detail['label'] == label
                 and re.fullmatch('[a-f0-9]{64}', result['plan_sha256'])
                 and detail['plan_sha256'] == result['plan_sha256']
                 and detail['mode'] == 'dry_run' and detail['policy_current'] is True
                 and detail['policy_sha256'] == POLICY_SHA and detail['allow_nas_only'] is False
                 and detail['approval_reason'] == '' and detail['approved_until'] is None
                 and len(expected) == len(set(expected)) == len(actual) == len(set(actual))
                 and set(expected) == set(actual) and len(reports) == len(expected)
                 and all(type(r['ordinal']) is int for r in reports)
                 and sorted(r['ordinal'] for r in reports) == list(range(1, len(expected) + 1))
                 and all(r['delete_ever_issued'] is False
                         and r['state'] in (None, 'pending', 'leased', 'would_delete') for r in reports))
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        raise RuntimeError('admitted plan contract differs; reconcile its label before retrying')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--stream', type=int, action='append', default=[], help='limit to stream id (repeatable)')
    ap.add_argument('--max-items', type=int, default=MAX_PLAN_ITEMS, help='items per plan (1..20000)')
    ap.add_argument('--max-bytes', type=float, default=MAX_PLAN_BYTES, help='byte cap per plan (<=2e12, the backend cap; e.g. 5e9 for a pilot)')
    ap.add_argument('--batch-by', choices=('stream', 'all'), default='stream', help='--submit: group admitted plans into batch manifests')
    ap.add_argument('--i-understand-large-batch', action='store_true', help='required with --batch-by all (one approval spanning every stream)')
    ap.add_argument('--max-plans', type=int, default=0, help='stop after N plans (0 = all)')
    ap.add_argument('--label-prefix', default='k28')
    ap.add_argument('--bucket', default=R2_BUCKET)
    ap.add_argument('--policy', default=str(POLICY_PATH))
    ap.add_argument('--psql', default='psql')
    ap.add_argument('--out', required=True, help='directory for plan inputs and the summary')
    ap.add_argument('--submit', action='store_true', help='admit plans as read-only dry runs via the service API')
    ap.add_argument('--summary-only', action='store_true', help='write candidate totals only; never admit or write plans')
    ap.add_argument('--atomic-admission', action='store_true', help='stop on admission rejection; never split one proposal into several plans')
    ap.add_argument('--api', default='https://stoarama.com')
    ap.add_argument('--scope-cache', help='content-addressed cache of pinned reviewed-scope objects (re-verified on every read)')
    ap.add_argument('--day-plans', action='store_true', help='prepare NAS-only day-directory plans (Keep Everything streams last, names only)')
    ap.add_argument('--r2prune', help='--day-plans: r2prune binary for the read-only R2 listing Keep Everything streams need')
    ap.add_argument('--max-units', type=int, default=500, help='--day-plans: directory-days per plan (1..1000)')
    a = ap.parse_args()
    if a.summary_only and (a.submit or len(a.stream) != 1):
        raise SystemExit('--summary-only requires exactly one stream and no --submit')
    if not 1 <= a.max_items <= MAX_PLAN_ITEMS or not 0 < a.max_bytes <= MAX_PLAN_BYTES or a.max_plans < 0 or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,39}', a.label_prefix):
        raise SystemExit('invalid batching arguments')
    if a.batch_by == 'all' and not a.i_understand_large_batch:
        raise SystemExit('--batch-by all puts every stream under one approval; add --i-understand-large-batch to confirm')
    policy = load_policy(a.policy)
    unknown = set(a.stream) - set(policy)
    if unknown:
        raise SystemExit(f'streams without a keep20 window: {sorted(unknown)}')
    if a.day_plans:
        return day_main(a, policy)
    rows = candidates(policy, set(a.stream), a.bucket, a.psql, a.scope_cache)
    plans = [] if a.summary_only else batches(rows, a.label_prefix, a.max_items, int(a.max_bytes))
    if a.max_plans:
        plans = plans[:a.max_plans]
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    for label, items in plans:
        (out / (label + '.json')).write_text(json.dumps({'label': label, 'items': [{'kind': r['kind'], 'source_id': r['source_id']} for r in items]}))
    recoverable = [r for r in rows if not r['r2_purged']]
    summary = {'policy_sha256': POLICY_SHA, 'bucket': a.bucket, 'streams': sorted(a.stream or policy), 'observed_at_epoch': time.time(),
               'recoverable_files': len(recoverable), 'recoverable_bytes': sum(r['size_bytes'] for r in recoverable),
               'candidate_files': len(rows), 'candidate_bytes': sum(r['size_bytes'] for r in rows),
               'plans': [{'label': l, 'stream_id': i[0]['stream_id'], 'items': len(i), 'bytes': sum(r['size_bytes'] for r in i)} for l, i in plans],
               'deletion_enabled': False}
    receipts, rejected = [], []
    if a.submit:
        try:
            for label, items in plans:
                submit(a.api, label, items, receipts, rejected, atomic=a.atomic_admission)
        except AdmissionRejected as exc:
            # Only a single atomic POST with no earlier success proves that
            # this stream admitted nothing. Partial or ambiguous runs still fail.
            if len(plans) != 1 or len(a.stream) != 1 or receipts:
                raise
            evidence = dict(stream_id=a.stream[0], label=exc.label, http_status=409,
                            reason=exc.reason, rejected_at_epoch=time.time())
            with (out / 'admission-rejected.json').open('x') as f:
                json.dump(evidence, f); f.flush(); os.fsync(f.fileno())
            directory = os.open(out, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return EXIT_ADMISSION_REJECTED
        summary.update(receipts=receipts, rejected_at_admission=rejected)
        batch_list = batch_manifests(receipts, a.label_prefix, a.batch_by)
        for b in batch_list:
            (out / ('batch-%s.json' % b['name'])).write_text(json.dumps(b, indent=1))
        summary['batches'] = [{'name': b['name'], 'plans': len(b['plans']), 'files': b['files'], 'bytes': b['bytes'], 'batch_sha256': b['batch_sha256']} for b in batch_list]
    temp = out / 'summary.tmp'
    temp.write_text(json.dumps(summary, indent=1))
    temp.replace(out / 'summary.json')
    print(json.dumps({'phase': 'KEEP28_PLANS_SUBMITTED' if a.submit else 'KEEP28_PLANS_PREVIEW', 'candidate_files': summary['candidate_files'],
                      'candidate_bytes': summary['candidate_bytes'], 'plans': len(plans), 'admitted': len(receipts), 'rejected_items': len(rejected),
                      'out': str(out), 'deletion_enabled': False}), flush=True)


def entrypoint():
    try:
        return main()
    except ReadFailed:
        traceback.print_exc()
        return EXIT_READ_FAILED


if __name__ == '__main__':
    sys.exit(entrypoint())
