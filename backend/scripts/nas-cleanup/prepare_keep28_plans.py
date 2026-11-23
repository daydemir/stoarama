#!/usr/bin/env python3
"""Prepare read-only NAS cleanup dry-run plans for the active 20-day policy.

The active policy keeps exactly one period per stream: the best 14 local days
shifted to 20 local days, [best-3d, best+17d), the exact manifest pinned by
migration 0236 and embedded in
backend/internal/api/nas_cleanup_keep20_policy.json. Every stream pinned
keep-everything is skipped entirely. This
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
descending bytes per file, and cut into plans of at most --max-items (<=10000) items and --max-bytes
(<=100 GB, the backend's per-plan cap). Labels are deterministic:
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

Environment: DATABASE_URL (read-only use) and, for --submit, SERVICE_TOKEN.
"""
import argparse
import base64
import datetime
import hashlib
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

from reviewed_scope import ReadFailed, admissible_candidates

# The active 20-day cleanup window, the exact manifest migration 0236 pins.
POLICY_SHA = 'bcc670a34024c2f8e2ba3df2566397f370d03e93ff33a7bb828c4b83eb123393'
POLICY_PATH = pathlib.Path(__file__).resolve().parents[2] / 'internal/api/nas_cleanup_keep20_policy.json'
# The superseded 28-day selection stays readable as provenance for the derived
# window; it never admits a new plan.
LEGACY_POLICY_SHA = '060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'
LEGACY_POLICY_PATH = pathlib.Path(__file__).resolve().parents[2] / 'internal/api/nas_cleanup_keep28_policy.json'
LABEL = re.compile(r'^[a-z0-9][a-z0-9._-]{0,79}$')
MAX_PLAN_ITEMS = 10000
MAX_PLAN_BYTES = 100_000_000_000  # backend nasCleanupMaxPlanBytes
MAX_BATCH_PLANS = 500
# A read failed before any submission: nothing was admitted and the runner may
# retry this stream later. Every other failure stops the runner.
EXIT_READ_FAILED = 4
EXIT_ADMISSION_REJECTED = 5


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
    # One bounded read-only query per stream keeps each transaction short. A
    # keep-everything stream has no eligible file at all, so it is never listed.
    rows = []
    for sid in sorted(streams or policy):
        if policy[sid].get('keep_everything'):
            print(json.dumps({'phase': 'kept_stream', 'stream_id': sid, 'files': 0, 'bytes': 0}), file=sys.stderr, flush=True)
            continue
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


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError('operator redirect refused')


def service(api, method, path, body=None):
    req = urllib.request.Request(api.rstrip('/') + '/api/v1/service/nas-cleanup/' + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={'Authorization': 'Bearer ' + os.environ['SERVICE_TOKEN'], 'User-Agent': 'stoarama-operator/1', 'Content-Type': 'application/json'})
    with urllib.request.build_opener(NoRedirect()).open(req, timeout=300) as response:
        return json.loads(response.read(16 << 20))


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
    ap.add_argument('--max-items', type=int, default=MAX_PLAN_ITEMS, help='items per plan (1..10000)')
    ap.add_argument('--max-bytes', type=float, default=MAX_PLAN_BYTES, help='byte cap per plan (<=1e11, the backend cap; e.g. 5e9 for a pilot)')
    ap.add_argument('--batch-by', choices=('stream', 'all'), default='stream', help='--submit: group admitted plans into batch manifests')
    ap.add_argument('--i-understand-large-batch', action='store_true', help='required with --batch-by all (one approval spanning every stream)')
    ap.add_argument('--max-plans', type=int, default=0, help='stop after N plans (0 = all)')
    ap.add_argument('--label-prefix', default='k28')
    ap.add_argument('--bucket', default=os.environ.get('R2_BUCKET', 'stoarama'))
    ap.add_argument('--policy', default=str(POLICY_PATH))
    ap.add_argument('--psql', default='psql')
    ap.add_argument('--out', required=True, help='directory for plan inputs and the summary')
    ap.add_argument('--submit', action='store_true', help='admit plans as read-only dry runs via the service API')
    ap.add_argument('--summary-only', action='store_true', help='write candidate totals only; never admit or write plans')
    ap.add_argument('--atomic-admission', action='store_true', help='stop on admission rejection; never split one proposal into several plans')
    ap.add_argument('--api', default='https://stoarama.com')
    ap.add_argument('--scope-cache', help='content-addressed cache of pinned reviewed-scope objects (re-verified on every read)')
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
