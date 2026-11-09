#!/usr/bin/env python3
"""Prepare read-only NAS cleanup dry-run plans for the keep28 policy.

keep28 keeps exactly one period per stream (best 14 local days +/-7 days, the
policy embedded in backend/internal/api/nas_cleanup_keep28_policy.json). This
script lists every NAS file that lies entirely outside its stream's window:

  * raw clips present on the NAS (nas_inventory_files matching the catalog
    path/size/SHA), and
  * collated outputs acknowledged on the NAS (recording_collation_output_acks
    matching the catalog size/SHA) whose source clips are all outside too.

Files already in an active plan bound to the same policy are skipped. Items are
grouped per stream, then per kind and recovery class (R2 copy present vs. R2
purged; NAS-only deletion was removed, so those plans can never be enabled), ordered by
path, and cut into plans of at most --max-items (<=10000) items and --max-bytes
(<=100 GB, the backend's per-plan cap). Labels are deterministic:
<prefix>-s<stream>-<raw|col>[-nasonly]-<n>.

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
keep28 window; a 409 batch is bisected and single rejected items are reported.

Batch (two streams, one approval per stream):
  prepare_keep28_plans.py --stream 3 --stream 4 --out k28 --submit

Pilot (one stream, a few GB):
  prepare_keep28_plans.py --stream 3 --max-bytes 5e9 --max-plans 1 --out pilot
  prepare_keep28_plans.py --stream 3 --max-bytes 5e9 --max-plans 1 --out pilot --submit

Environment: DATABASE_URL (read-only use) and, for --submit, SERVICE_TOKEN.
"""
import argparse
import base64
import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

POLICY_SHA = '060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'
POLICY_PATH = pathlib.Path(__file__).resolve().parents[2] / 'internal/api/nas_cleanup_keep28_policy.json'
LABEL = re.compile(r'^[a-z0-9][a-z0-9._-]{0,79}$')
MAX_PLAN_ITEMS = 10000
MAX_PLAN_BYTES = 100_000_000_000  # backend nasCleanupMaxPlanBytes
MAX_BATCH_PLANS = 500


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


def load_policy(path):
    raw = pathlib.Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != POLICY_SHA:
        raise SystemExit('keep28 policy SHA differs from the pinned policy')
    rows = json.loads(raw)
    ids = [r['stream_id'] for r in rows]
    if len(rows) != 109 or len(set(ids)) != 109:
        raise SystemExit('keep28 policy must hold exactly one window for each of 109 streams')
    return {r['stream_id']: r for r in rows}


def pg_env():
    url = urllib.parse.urlsplit(os.environ['DATABASE_URL'])
    env = os.environ.copy()
    env.update(PGHOST=url.hostname or '', PGPORT=str(url.port or 5432),
               PGUSER=urllib.parse.unquote(url.username or ''), PGPASSWORD=urllib.parse.unquote(url.password or ''),
               PGDATABASE=url.path.lstrip('/'), PGCONNECT_TIMEOUT='10',
               PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=300000 -c timezone=UTC')
    for k, v in urllib.parse.parse_qsl(url.query):
        if k in ('sslmode', 'connect_timeout'):
            env['PG' + k.upper()] = v
    return env


def query(sql, psql):
    p = subprocess.run([psql, '-XqAt', '-v', 'ON_ERROR_STOP=1'], input=('BEGIN READ ONLY;' + sql + ';ROLLBACK;').encode(),
                       env=pg_env(), capture_output=True, timeout=330)
    if p.returncode:
        raise RuntimeError('read-only catalogue query failed: ' + p.stderr.decode()[:400])
    return json.loads(p.stdout)


def candidates(policy, streams, bucket, psql):
    # One bounded read-only query per stream keeps each transaction short.
    rows = []
    for sid in sorted(streams or policy):
        part = stream_candidates(policy[sid], bucket, psql)
        rows += part
        print(json.dumps({'phase': 'listed_stream', 'stream_id': sid, 'files': len(part), 'bytes': sum(r['size_bytes'] for r in part)}), file=sys.stderr, flush=True)
    return rows


def stream_candidates(w, bucket, psql):
    windows = [{'stream_id': w['stream_id'], 's': w['keep_start_utc'], 'e': w['keep_end_utc']}]
    encoded = base64.b64encode(json.dumps(windows).encode()).decode()
    bucket_literal = "'" + bucket.replace("'", "''") + "'"
    return query("""WITH w AS MATERIALIZED (
 SELECT (x->>'stream_id')::bigint stream_id,(x->>'s')::timestamptz s,(x->>'e')::timestamptz e
 FROM jsonb_array_elements(convert_from(decode('""" + encoded + """','base64'),'UTF8')::jsonb) x
), planned AS MATERIALIZED (
 SELECT DISTINCT i->>'kind' kind,(i->>'source_id')::bigint source_id FROM nas_cleanup_plans p CROSS JOIN LATERAL jsonb_array_elements(p.manifest) i
 WHERE p.account_id=47 AND p.connection_id=13 AND p.report_sha256='""" + POLICY_SHA + """' AND p.mode<>'retired'
), raw AS (
 SELECT 'raw' kind,c.id source_id,r.stream_id,c.display_path path,c.size_bytes,(c.purged_at IS NOT NULL) r2_purged
 FROM nas_inventory_files f JOIN recording_clips c ON c.id=f.clip_id JOIN recordings r ON r.id=c.recording_id JOIN w ON w.stream_id=r.stream_id
 WHERE f.connection_id=13 AND f.state='present' AND f.relative_path=c.display_path AND f.sha256=lower(c.sha256) AND f.size_bytes=c.size_bytes
  AND r.account_id=47 AND c.bucket=""" + bucket_literal + """ AND c.size_bytes>0
  AND c.clip_end_at>c.clip_start_at AND c.clip_start_at>='2020-01-01' AND c.clip_end_at<'2028-01-01'
  AND NOT (c.clip_start_at<w.e AND c.clip_end_at>w.s)
), col AS (
 SELECT 'collated' kind,o.id source_id,r.stream_id,'joined/'||o.nas_relative_path path,o.size_bytes,false r2_purged
 FROM recording_collation_outputs o JOIN recording_collation_output_acks a ON a.output_id=o.id AND a.connection_id=13 AND a.sha256=lower(o.sha256) AND a.size_bytes=o.size_bytes
 JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id JOIN w ON w.stream_id=r.stream_id
 WHERE r.account_id=47 AND o.end_at>o.start_at AND o.start_at>='2020-01-01' AND o.end_at<'2028-01-01' AND NOT (o.start_at<w.e AND o.end_at>w.s)
  AND cardinality(o.source_clip_ids)>0
  AND NOT EXISTS (SELECT 1 FROM unnest(o.source_clip_ids) sid LEFT JOIN recording_clips c ON c.id=sid LEFT JOIN recordings cr ON cr.id=c.recording_id
   WHERE c.id IS NULL OR cr.stream_id IS DISTINCT FROM r.stream_id OR c.bucket IS DISTINCT FROM """ + bucket_literal + """
    OR NOT (c.clip_end_at>c.clip_start_at) OR (c.clip_start_at<w.e AND c.clip_end_at>w.s))
)
SELECT coalesce(json_agg(x ORDER BY x.stream_id,x.kind DESC,x.r2_purged,x.path),'[]') FROM (
 SELECT * FROM raw UNION ALL SELECT * FROM col
) x WHERE NOT EXISTS (SELECT 1 FROM planned p WHERE p.kind=x.kind AND p.source_id=x.source_id)""", psql)


def batches(rows, prefix, max_items, max_bytes):
    groups = {}
    for r in rows:
        key = (r['stream_id'], 'raw' if r['kind'] == 'raw' else 'col', r['r2_purged'])
        groups.setdefault(key, []).append(r)
    out = []
    for (sid, kind, purged), items in sorted(groups.items()):
        n, current, size = 0, [], 0
        for r in items:
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
    return out


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError('operator redirect refused')


def service(api, method, path, body=None):
    req = urllib.request.Request(api.rstrip('/') + '/api/v1/service/nas-cleanup/' + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={'Authorization': 'Bearer ' + os.environ['SERVICE_TOKEN'], 'User-Agent': 'Stoarama-Operator/1.0', 'Content-Type': 'application/json'})
    with urllib.request.build_opener(NoRedirect()).open(req, timeout=300) as response:
        return json.loads(response.read(16 << 20))


def submit(api, label, rows, receipts, rejected):
    try:
        result = service(api, 'POST', 'plans', {'label': label, 'items': [{'kind': r['kind'], 'source_id': r['source_id']} for r in rows]})
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise RuntimeError(f'admission not confirmed for {label}: HTTP {exc.code}; inspect the label before retrying')
        if len(rows) == 1:
            rejected.append({'label': label, 'kind': rows[0]['kind'], 'source_id': rows[0]['source_id'], 'path': rows[0]['path'], 'reason': exc.read(2000).decode(errors='replace')})
            return
        mid = len(rows) // 2
        submit(api, label + 'a', rows[:mid], receipts, rejected)
        submit(api, label + 'b', rows[mid:], receipts, rejected)
        return
    detail = service(api, 'GET', 'plans/' + str(result['id']))
    assert detail['mode'] == 'dry_run' and detail['master_deletion_enabled'] is False and detail['policy_current'] is True
    assert {(i['kind'], i['source_id']) for i in detail['items']} == {(r['kind'], r['source_id']) for r in rows}
    receipts.append({'id': result['id'], 'label': label, 'stream_id': rows[0]['stream_id'], 'plan_sha256': result['plan_sha256'], 'items': len(rows), 'bytes': sum(r['size_bytes'] for r in rows)})
    print(json.dumps({'phase': 'plan_admitted_dry_run', **receipts[-1]}), flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--stream', type=int, action='append', default=[], help='limit to stream id (repeatable)')
    ap.add_argument('--max-items', type=int, default=MAX_PLAN_ITEMS, help='items per plan (1..10000)')
    ap.add_argument('--max-bytes', type=float, default=MAX_PLAN_BYTES, help='byte cap per plan (<=1e11, the backend cap; e.g. 5e9 for a pilot)')
    ap.add_argument('--batch-by', choices=('stream', 'all'), default='stream', help='--submit: group admitted plans into batch manifests')
    ap.add_argument('--max-plans', type=int, default=0, help='stop after N plans (0 = all)')
    ap.add_argument('--label-prefix', default='k28')
    ap.add_argument('--bucket', default=os.environ.get('R2_BUCKET', 'stoarama'))
    ap.add_argument('--policy', default=str(POLICY_PATH))
    ap.add_argument('--psql', default='psql')
    ap.add_argument('--out', required=True, help='directory for plan inputs and the summary')
    ap.add_argument('--submit', action='store_true', help='admit plans as read-only dry runs via the service API')
    ap.add_argument('--api', default='https://stoarama.com')
    a = ap.parse_args()
    if not 1 <= a.max_items <= MAX_PLAN_ITEMS or not 0 < a.max_bytes <= MAX_PLAN_BYTES or a.max_plans < 0 or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,15}', a.label_prefix):
        raise SystemExit('invalid batching arguments')
    policy = load_policy(a.policy)
    unknown = set(a.stream) - set(policy)
    if unknown:
        raise SystemExit(f'streams without a keep28 window: {sorted(unknown)}')
    rows = candidates(policy, set(a.stream), a.bucket, a.psql)
    plans = batches(rows, a.label_prefix, a.max_items, int(a.max_bytes))
    if a.max_plans:
        plans = plans[:a.max_plans]
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    for label, items in plans:
        (out / (label + '.json')).write_text(json.dumps({'label': label, 'items': [{'kind': r['kind'], 'source_id': r['source_id']} for r in items]}))
    summary = {'policy_sha256': POLICY_SHA, 'candidate_files': len(rows), 'candidate_bytes': sum(r['size_bytes'] for r in rows),
               'plans': [{'label': l, 'stream_id': i[0]['stream_id'], 'items': len(i), 'bytes': sum(r['size_bytes'] for r in i)} for l, i in plans],
               'deletion_enabled': False}
    receipts, rejected = [], []
    if a.submit:
        for label, items in plans:
            submit(a.api, label, items, receipts, rejected)
        summary.update(receipts=receipts, rejected_at_admission=rejected)
        batches = batch_manifests(receipts, a.label_prefix, a.batch_by)
        for b in batches:
            (out / ('batch-%s.json' % b['name'])).write_text(json.dumps(b, indent=1))
        summary['batches'] = [{'name': b['name'], 'plans': len(b['plans']), 'files': b['files'], 'bytes': b['bytes'], 'batch_sha256': b['batch_sha256']} for b in batches]
    (out / 'summary.json').write_text(json.dumps(summary, indent=1))
    print(json.dumps({'phase': 'KEEP28_PLANS_SUBMITTED' if a.submit else 'KEEP28_PLANS_PREVIEW', 'candidate_files': summary['candidate_files'],
                      'candidate_bytes': summary['candidate_bytes'], 'plans': len(plans), 'admitted': len(receipts), 'rejected_items': len(rejected),
                      'out': str(out), 'deletion_enabled': False}), flush=True)


if __name__ == '__main__':
    sys.exit(main())
