# usage: /usr/bin/python3 -I cycle.py LANE_DIR
# Run the existing keep-20 R2 plan with current NAS evidence and journal
# acknowledged deletions for catalog receipt replay.
import urllib.parse
import csv
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
PSQL = '/opt/homebrew/opt/libpq/bin/psql'
AWS = os.path.expanduser('~/aws-cli/aws')
DBENV = os.path.expanduser('~/Build/stoarama/local/recording-supervisor.env')
R2ENV = os.path.expanduser('~/Build/stoarama/local/render.env')
RELEASE = 'nas_keep_20,unresolved_best14_delivery_identity,delivery_repairs_and_unresolved_stitch_provenance'
RECENT = '2026-11-18T18:00:00Z'
DELETE_CLASS = 'delete_window_20d_on_nas'
KINDS = ('raw', 'collated')
ROWS_PER_STATEMENT = 5000
SHA256 = re.compile('[0-9a-f]{64}')

# Redundancy gate. Deniz, 2026-11-27, verbatim: "yuea double check everything be 100%
# sure redundancy before deleting". The inventory evidence above only plans a delete.
# Before any R2 DELETE, the NAS client (the selected-verify lease of every r2-copy-v8
# release) reads each chunk key's NAS copy live, at its canonical path, and hashes all
# of its bytes; every key must come back `match` at the catalog size and sha256, in a
# plan created by this cycle. Any other outcome stops the cycle before its DELETE. A
# cycle deletes at most NAS_VERIFY_CHUNK keys; the rest are logged skipped_changed
# absent_from_fresh_listing and planned again by a later cycle. A key without a catalog
# sha256 is never deleted (receipt_ready) and is counted as no_hash.
NAS_VERIFY_CHUNK = 1000
NAS_VERIFY_WAIT_SEC = 2 * 3600
NAS_VERIFY_POLL_SEC = 30
NAS_VERIFY_CLOCK_SKEW_SEC = 300
NAS_VERIFY_PENDING = ('pending', 'leased')

# Planning evidence: a present NAS inventory row (raw) or ack (collated) at the same
# canonical path with the same size, and the same sha where both are recorded. Only
# in-window, non-KE keys are fed, and nothing with a NAS cleanup delete ever issued.
# These rows can be weeks old, so they only plan: the redundancy gate (nas_verify)
# re-proves every key live before its DELETE.
RECEIPTS = r"""BEGIN READ ONLY;
SET LOCAL lock_timeout='2s';
SET LOCAL statement_timeout='115s';
-- Memory-safe on the 2 GB production DB (OOM-killed 2026-11-26): no forced hash
-- joins or parallel workers. The planner probes NAS rows by clip index; measured
-- 34 s, every hash under 2 MB (the forced plan hashed 71k wide clip rows and
-- seq-scanned all NAS rows, and hit this timeout in cycle 20261127T001522Z).
-- Live window clips come from migration 0273's partial index: the clip-start index
-- alone fetched every purged clip before each window end (1.0M heap rows for 350k).
SET LOCAL work_mem='32MB';
SET LOCAL max_parallel_workers_per_gather=0;
COPY (
 WITH issued AS MATERIALIZED (
   SELECT plan_id, ordinal, item FROM nas_cleanup_items WHERE delete_ever_issued
 ), legacy_plans AS (
   SELECT DISTINCT plan_id FROM issued WHERE item IS NULL
 ), legacy_items AS (
   SELECT p.id, m.ordinal, m.item
   FROM legacy_plans l JOIN nas_cleanup_plans p ON p.id = l.plan_id
   CROSS JOIN LATERAL jsonb_array_elements(p.manifest) WITH ORDINALITY AS m(item, ordinal)
 ), nas_deleted AS MATERIALIZED (
   SELECT item->>'kind' AS kind, (item->>'source_id')::bigint AS source_id
   FROM issued WHERE item IS NOT NULL
   UNION ALL
   SELECT m.item->>'kind', (m.item->>'source_id')::bigint
   FROM issued i JOIN legacy_items m ON m.id = i.plan_id AND m.ordinal = i.ordinal
   WHERE i.item IS NULL
 ), windows AS NOT MATERIALIZED (
   SELECT r.id AS recording_id, r.stream_id, w.start_at, w.end_at
   FROM nas_media_keep20_windows w
   JOIN recordings r ON r.stream_id = w.stream_id AND r.account_id = 47
   WHERE NOT w.keep_everything
 ),
 raw AS (SELECT c.object_key,
        floor(extract(epoch FROM greatest(f.verified_at, f.server_received_at)))::bigint AS seen,
        f.file_mtime_ns / 1000000000 AS mtime,
        'raw' AS kind, c.id AS source_id, w.stream_id, c.size_bytes,
        lower(c.sha256) AS sha256, btrim(c.etag, '"') AS etag
 FROM windows w
 JOIN recording_clips c ON c.recording_id = w.recording_id
  AND c.clip_start_at < w.end_at AND c.clip_end_at > w.start_at
 JOIN nas_inventory_files f ON f.connection_id = 13 AND f.clip_id = c.id
 WHERE c.bucket = 'stoarama' AND c.purged_at IS NULL
   AND f.state = 'present' AND f.relative_path = c.display_path AND f.size_bytes = c.size_bytes
   AND (nullif(c.sha256, '') IS NULL OR nullif(f.sha256, '') IS NULL OR lower(f.sha256) = lower(c.sha256))
 OFFSET 0
 )
 SELECT r.* FROM raw r WHERE r.source_id NOT IN
   (SELECT d.source_id FROM nas_deleted d WHERE d.kind = 'raw' AND d.source_id IS NOT NULL)
 UNION ALL
 SELECT o.object_key, floor(extract(epoch FROM a.verified_at))::bigint,
        floor(extract(epoch FROM a.verified_at))::bigint,
        'collated', o.id, w.stream_id, o.size_bytes, lower(o.sha256), NULL
 FROM recording_collation_output_acks a
 JOIN recording_collation_outputs o ON o.id = a.output_id
 JOIN recording_collation_hours h ON h.id = o.collation_hour_id
 JOIN windows w ON w.recording_id = h.recording_id
 WHERE a.connection_id = 13 AND a.nas_relative_path = o.nas_relative_path AND a.size_bytes = o.size_bytes
   AND (nullif(a.sha256, '') IS NULL OR nullif(o.sha256, '') IS NULL OR lower(a.sha256) = lower(o.sha256))
   AND o.start_at < w.end_at AND o.end_at > w.start_at
   AND o.id NOT IN (SELECT d.source_id FROM nas_deleted d WHERE d.kind = 'collated' AND d.source_id IS NOT NULL)
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\t');
ROLLBACK;
"""

def env(path):
    out = subprocess.run(['/bin/sh', '-c', 'set -a; . "$0"; set +a; env -0', path],
                         check=True, capture_output=True).stdout
    return dict(kv.split('=', 1) for kv in out.decode().split('\0') if '=' in kv)


def pg_env(path, read_only):
    """libpq PG* variables from DATABASE_URL, so no credential appears in argv (ps)."""
    e = env(path)
    u = urllib.parse.urlsplit(e.pop('DATABASE_URL'))
    e.update(PGHOST=u.hostname or '', PGPORT=str(u.port or 5432), PGUSER=urllib.parse.unquote(u.username or ''),
             PGPASSWORD=urllib.parse.unquote(u.password or ''), PGDATABASE=u.path.lstrip('/'))
    for k, v in urllib.parse.parse_qsl(u.query):
        if k == 'sslmode':
            e['PGSSLMODE'] = v
    e['PGOPTIONS'] = '-c default_transaction_read_only=' + ('on' if read_only else 'off')
    return e


def psql(sql, out, *, script=None):
    e = pg_env(DBENV, True)
    with open(out, 'w') as f:
        result = subprocess.run([PSQL, '-XqAt', '-v', 'ON_ERROR_STOP=1'] + sql,
                                stdout=f, stderr=subprocess.PIPE, env=e, input=script, text=True)
    if result.returncode:
        error = result.stderr.replace(e['PGPASSWORD'], '[redacted]')
        raise RuntimeError(f'psql read failed for {out}: {error}')


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def log(L, rec):
    rec['at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with open(L + '/runs.jsonl', 'a') as f:
        f.write(json.dumps(rec) + '\n')
    print(json.dumps(rec))


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def durable_write(path, contents):
    with open(path + '.tmp', 'w', encoding='utf-8') as f:
        f.write(contents)
        f.flush()
        os.fsync(f.fileno())
    os.replace(path + '.tmp', path)
    sync_dir(os.path.dirname(path))


def receipt_source(kind, sid, stream, size, digest, etag):
    return {'kind': kind, 'source_id': int(sid), 'stream_id': int(stream),
            'size': int(size), 'sha256': digest, 'etag': etag}


def receipt_ready(key, size, etag, source):
    if (key != key.strip() or size <= 0 or source['size'] != size
        or not SHA256.fullmatch(source['sha256']) or not etag):
        return False
    if source['kind'] == 'raw':
        return key.startswith('managed/') and etag == source['etag']
    return key.startswith(('joined/goodplus-', 'joined/nightly-collation-v2/', 'joined/best14-20261021/'))


def list_roots(sources):
    """The folders holding receipt candidates (three levels deep), so the cycle
    lists those instead of the whole bucket. Nested roots collapse to the outer."""
    dirs = sorted({'/'.join(k.split('/')[:3]) + '/' if k.count('/') >= 3 else k.rsplit('/', 1)[0] + '/' for k in sources})
    roots = []
    for d in dirs:
        if not roots or not d.startswith(roots[-1]):
            roots.append(d)
    return roots


def preflight_script(candidates):
    """preflight.sql once per chunk, all in one read-only REPEATABLE READ snapshot, so
    the vetoes equal one statement over every candidate. A single 137k-row JSON literal
    preceded the 2026-11-26 production OOM; each chunk sends two short arrays instead."""
    script = ["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;",
              "SET LOCAL lock_timeout='2s'; SET LOCAL statement_timeout='900s';"]
    for i in range(0, len(candidates), ROWS_PER_STATEMENT):
        chunk = candidates[i:i + ROWS_PER_STATEMENT]
        if any(kind not in KINDS or type(sid) is not int for kind, sid in chunk):
            raise ValueError(f'unexpected preflight candidate in chunk at {i}')
        script += [r"\set kinds '{%s}'" % ','.join(kind for kind, _ in chunk),
                   r"\set ids '{%s}'" % ','.join(str(sid) for _, sid in chunk),
                   r"\i '%s'" % (HERE / 'preflight.sql')]
    return '\n'.join(script + ['ROLLBACK;']) + '\n'


def preflight(W, present, sources):
    candidates = [(sources[k]['kind'], sources[k]['source_id']) for k in present]
    if not candidates:
        return set()
    out = W + '/catalog-vetoes.tsv'
    psql([], out, script=preflight_script(candidates))
    with open(out) as f:
        return {(kind, int(sid)) for kind, sid in (line.rstrip('\n').split('|') for line in f)}


def read_manifest(W, expected):
    path = W + '/plan/delete-manifest.jsonl'
    if sha(path) != expected:
        raise ValueError(f'{W}: manifest digest changed')
    manifest = {}
    with open(path) as f:
        for line in f:
            row = json.loads(line)
            key = row['key']
            if key in manifest or row['class'] != DELETE_CLASS:
                raise ValueError(f'{W}: duplicate key or unexpected class in manifest')
            manifest[key] = row
    return manifest


def copy_field(value):
    if value is None:
        return r'\N'
    return (str(value).replace('\\', '\\\\').replace('\t', r'\t')
            .replace('\n', r'\n').replace('\r', r'\r'))


def pg_time(text):
    """A timestamptz as psql's text output prints it in UTC (`2026-11-27 09:02:02.35+00`)."""
    if not text.endswith('+00'):
        raise ValueError(f'unexpected timestamp {text!r}')
    fmt = '%Y-%m-%d %H:%M:%S.%f' if '.' in text else '%Y-%m-%d %H:%M:%S'
    return datetime.datetime.strptime(text[:-3], fmt).replace(tzinfo=datetime.timezone.utc)


def selected_verify(L, args):
    """`stoaramactl nas-selected-verify ARGS --json`; the catalog URL stays in the env."""
    e = env(DBENV)
    res = subprocess.run([L + '/stoaramactl', 'nas-selected-verify'] + args + ['--json'],
                         env=e, capture_output=True, text=True)
    if res.returncode:
        error = res.stderr
        secret = urllib.parse.urlsplit(e['DATABASE_URL']).password
        for value in filter(None, (e['DATABASE_URL'], secret, secret and urllib.parse.unquote(secret))):
            error = error.replace(value, '[redacted]')
        raise RuntimeError(f'nas-selected-verify {args[0]} failed: {error[-500:]}')
    return json.loads(res.stdout)


def nas_verify(L, W, label, chunk):
    """Prove every chunk key's NAS copy live, or raise. chunk maps each R2 key to its
    receipt source (kind, source_id, size, sha256). The NAS client hashes each NAS file
    in full; the report must match every source exactly, in a plan this call created."""
    started = datetime.datetime.now(datetime.timezone.utc)
    ids = {kind: sorted(s['source_id'] for s in chunk.values() if s['kind'] == kind) for kind in KINDS}
    flags = {'raw': '--clip-ids', 'collated': '--collated-ids'}
    args = ['--label', label] + [a for kind in KINDS if ids[kind] for a in (flags[kind], ','.join(map(str, ids[kind])))]
    plan = selected_verify(L, ['request'] + args + ['--dry-run'])
    if plan['items'] != len(chunk):
        raise RuntimeError(f'{label}: verify plan has {plan["items"]} items for {len(chunk)} keys')
    selected_verify(L, ['request'] + args + ['--expected-plan-sha256', plan['plan_sha256']])
    deadline = time.monotonic() + NAS_VERIFY_WAIT_SEC
    while True:
        report = selected_verify(L, ['report', '--label', label])
        if report['plan_sha256'] != plan['plan_sha256']:
            raise RuntimeError(f'{label}: report names another plan')
        failed = [i for i in report['items'] if i['state'] not in NAS_VERIFY_PENDING + ('match',)]
        if failed:
            durable_write(W + '/nas-verify.json', json.dumps(report, sort_keys=True) + '\n')
            f = failed[0]
            raise RuntimeError(f'{label}: {len(failed)} NAS copies not verified, first {f["kind"]} '
                               f'{f["source_id"]} {f["state"]}: {f["error"][:200]}')
        if all(i['state'] == 'match' for i in report['items']):
            break
        if time.monotonic() > deadline:
            raise RuntimeError(f'{label}: NAS verify not finished in {NAS_VERIFY_WAIT_SEC} s')
        time.sleep(NAS_VERIFY_POLL_SEC)
    durable_write(W + '/nas-verify.json', json.dumps(report, sort_keys=True) + '\n')
    created = pg_time(report['created_at'])
    if created < started - datetime.timedelta(seconds=NAS_VERIFY_CLOCK_SKEW_SEC):
        raise RuntimeError(f'{label}: verify plan predates this chunk')
    items = {(i['kind'], i['source_id']): i for i in report['items']}
    if len(items) != len(report['items']) or len(items) != len(chunk):
        raise RuntimeError(f'{label}: report items differ from the chunk')
    for key, s in chunk.items():
        i = items.get((s['kind'], s['source_id']))
        if (i is None or i['state'] != 'match' or i['size_bytes'] != s['size'] or i['sha256'] != s['sha256']
                or not SHA256.fullmatch(i['sha256']) or pg_time(i['completed_at']) < created):
            raise RuntimeError(f'{label}: NAS copy of {key} is not a fresh exact match')
    return len(items)


def verified_chunk(L, W, stamp, planned, sources):
    """Choose this cycle's chunk, prove its NAS copies live, and write the only listing
    its r2prune delete may act on. Returns the chunk's keys."""
    chunk = sorted(planned)[:NAS_VERIFY_CHUNK]
    nas_verify(L, W, 'r2prune-lane-' + stamp.lower(), {k: sources[k] for k in chunk})
    keep = set(chunk)
    with open(W + '/snapshot.tsv') as snap:
        rows = [line for line in snap if line.split('\t', 1)[0] in keep]
    if len(rows) != len(chunk):
        raise RuntimeError(f'{W}: chunk keys missing from the snapshot')
    durable_write(W + '/delete-listing.tsv', ''.join(rows))
    return chunk


def receipt_tsv(W, intent):
    """Render acknowledged deletions only. A partial ledger stays unresolved."""
    manifest = read_manifest(W, intent['manifest_sha256'])
    sources_path = W + '/receipt-sources.json'
    if sha(sources_path) != intent['sources_sha256']:
        raise ValueError(f'{W}: receipt source digest changed')
    with open(sources_path) as f:
        sources = json.load(f)
    if sources.keys() != manifest.keys():
        raise ValueError(f'{W}: receipt sources differ from manifest')
    ledger_path = W + '/ledger.jsonl'
    if not os.path.isfile(ledger_path) or os.path.getsize(ledger_path) == 0:
        return '', 0, len(manifest)
    with open(ledger_path, 'rb') as f:
        os.fsync(f.fileno())
        header_line = f.readline()
        if not header_line.endswith(b'\n'):
            return '', 0, len(manifest)
        header = json.loads(header_line)
        if (header['type'] != 'run' or header['manifest_sha256'] != intent['manifest_sha256']
            or header['limit'] != 0):
            raise ValueError(f'{W}: ledger does not describe the bound manifest')
        policy = {k: header[k] for k in ('inputs_sha256', 'recent_from', 'release') if k in header}
        policy_sha = hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        seen, deleted = set(), {}
        for line in f:
            if not line.endswith(b'\n'):
                break
            row = json.loads(line)
            key = row['key']
            if (key in seen or key not in manifest or row['size'] != manifest[key]['size']
                or row['outcome'] not in {'deleted', 'failed', 'skipped_protected', 'skipped_changed'}):
                raise ValueError(f'{W}: unexpected ledger row for {key}')
            seen.add(key)
            if row['outcome'] == 'deleted':
                deleted[key] = row['at']
    result = []
    for seq, (key, at) in enumerate(sorted(deleted.items()), 1):
        planned, source = manifest[key], sources[key]
        etag = planned['etag'].strip('"')
        if not receipt_ready(key, planned['size'], etag, source):
            raise ValueError(f'{W}: receipt source cannot account for {key}')
        result.append('\t'.join(map(copy_field, (
            seq, source['kind'], source['source_id'], source['stream_id'], key,
            planned['size'], source['sha256'], etag, at, 'r2prune_ledger_' + planned['class'],
            'none', intent['manifest_sha256'], policy_sha, None))) + '\n')
    return ''.join(result), len(deleted), len(manifest) - len(seen)


def apply_receipts(W, total, lock_fd):
    env_db = pg_env(DBENV, False)
    start_seq = 1
    for attempt in range(30):
        res = subprocess.run([PSQL, '-XqAt', '-v', 'ON_ERROR_STOP=1',
                              '-v', f'start_seq={start_seq}', '-v', f'total={total}',
                              '-f', str(HERE / 'receipt-apply.sql')],
                             cwd=W, env=env_db, capture_output=True, text=True, pass_fds=(lock_fd,))
        if res.returncode == 0:
            return
        if '55P03:' not in res.stderr or attempt == 29:
            error = res.stderr.replace(env_db['PGPASSWORD'], '[redacted]')
            raise RuntimeError(f'{W}: receipt apply failed\n{error}')
        for line in res.stdout.splitlines():
            if line.startswith('committed:'):
                start_seq = int(line.split(':', 1)[1]) + 1
        time.sleep(10)


def finish_receipts(W, lock_fd, replay=False):
    with open(W + '/delete-intent.json') as f:
        intent = json.load(f)
    tsv, deleted, unresolved = receipt_tsv(W, intent)
    durable_write(W + '/receipt-backfill.tsv', tsv)
    if deleted:
        apply_receipts(W, deleted, lock_fd)
    # Replaying an older cycle: receipt what the ledger acknowledges, then close the
    # intent so one crash cannot wedge every later cycle. Unlogged keys still in R2
    # are simply planned again from the next listing; the count is kept here.
    result = {'complete': unresolved == 0 or replay, 'deleted_receipts': deleted,
              'unresolved': unresolved, 'abandoned_unresolved': unresolved if replay else 0,
              'manifest_sha256': intent['manifest_sha256']}
    durable_write(W + '/receipt-result.json', json.dumps(result) + '\n')
    if unresolved and not replay:
        raise RuntimeError(f'{W}: {unresolved} planned keys lack complete ledger outcomes')


def main(L):
    # <lane>/STOP pauses the lane: no replay, DB or R2 work until it is removed.
    if os.path.exists(L + '/STOP'):
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        return log(L, {'cycle': stamp, 'skipped': 'STOP'})
    # The kernel releases the lock after a crash so the journal can replay.
    with open(L + '/cycle.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for intent in sorted(Path(L, 'cycles').glob('*/delete-intent.json')):
            result = intent.with_name('receipt-result.json')
            if not result.exists() or not json.loads(result.read_text())['complete']:
                finish_receipts(str(intent.parent), lock.fileno(), replay=True)
        stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        W = f'{L}/cycles/{stamp}'
        os.makedirs(W)
        sync_dir(L + '/cycles')
        try:
            run(L, W, stamp, lock.fileno())
        except BaseException as exc:
            ledger = Path(W, 'ledger.jsonl')
            outcomes = {}
            if ledger.exists():
                for line in ledger.read_text().splitlines():
                    o = json.loads(line).get('outcome')
                    if o:
                        outcomes[o] = outcomes.get(o, 0) + 1
            log(L, {'cycle': stamp, 'failed': type(exc).__name__, 'error': str(exc)[:300], 'outcomes': outcomes})
            raise


def run(L, W, stamp, lock_fd):
    I = L + '/inputs'
    hold = {json.loads(l)['object_key'] for l in list(open(I + '/durham-holds.jsonl'))[1:]}
    psql([], W + '/receipts.tsv', script=RECEIPTS)
    ev, nasm, sources = {}, {}, {}
    ambiguous = set()
    skipped = {'durham_hold': 0, 'no_hash': 0, 'receipt_identity': 0, 'catalog_veto': 0}
    with open(W + '/receipts.tsv', newline='') as f:
        for k, seen, mtime, kind, sid, stream, size, digest, etag in csv.reader(f, delimiter='\t'):
            if k in hold:
                skipped['durham_hold'] += 1
                continue
            source = receipt_source(kind, sid, stream, size, digest, etag)
            if k in sources and sources[k] != source:
                ambiguous.add(k)
            sources[k] = source
            ev[k] = min(int(seen), ev.get(k, int(seen)))
            nasm[k] = max(int(mtime), nasm.get(k, 0))
    psql(['-f', L + '/protect.sql'], W + '/protect-db-now.tsv')
    # GSS delivery freeze (Deniz Nov 25: "i wnat to save it if its not"): every key the
    # delivery manifest served, plus every R2-live raw source of those outputs. Fail closed.
    with open(os.path.expanduser('~/stoarama-gss-freeze/gss-protect.tsv')) as g, open(W + '/protect-db-now.tsv', 'a') as f:
        rows = g.read()
        if rows.count("\n") < 1035152:
            raise ValueError('GSS protect list is short')
        f.write(rows)
    r2 = env(R2ENV)
    with open(W + '/snapshot.tsv', 'w') as snap:
        roots = list_roots(sources)
        args = [a for r in roots for a in ('--prefix', r)]
        p = subprocess.Popen([L + '/r2prune', 'list', '--workers', '8', '--out', '-'] + args, stdout=subprocess.PIPE, env=r2, text=True)
        listed = 0
        for line in p.stdout:
            listed += 1
            key, size, etag, _mtime = line.rstrip('\n').split('\t')
            if key in sources:
                if not SHA256.fullmatch(sources[key]['sha256']):
                    skipped['no_hash'] += 1
                elif key not in ambiguous and receipt_ready(key, int(size), etag.strip('"'), sources[key]):
                    snap.write(line)
                else:
                    skipped['receipt_identity'] += 1
        if p.wait() != 0:
            sys.exit('r2prune list failed')
    present = [l.split('\t', 1)[0] for l in open(W + '/snapshot.tsv')]
    blocked = preflight(W, present, sources)
    if blocked:
        with open(W + '/snapshot.tsv') as current, open(W + '/snapshot-safe.tsv', 'w') as safe:
            for line in current:
                source = sources[line.split('\t', 1)[0]]
                if (source['kind'], source['source_id']) not in blocked:
                    safe.write(line)
                else:
                    skipped['catalog_veto'] += 1
        os.replace(W + '/snapshot-safe.tsv', W + '/snapshot.tsv')
        present = [line.split('\t', 1)[0] for line in open(W + '/snapshot.tsv')]
    if not present:
        shutil.rmtree(W)
        return log(L, {'cycle': stamp, 'receipts': len(ev), 'present': 0, 'listed': listed, 'skipped': skipped})
    with open(W + '/nas-evidence.tsv', 'w') as f:
        for k in present:
            f.write('\t'.join(['raw', k, 't', str(ev[k]), str(nasm[k]), str(ev[k])]) + '\n')
    rules = ['--identities', I + '/identities.tsv', '--derived-identities', I + '/derived-identities.tsv',
             '--closure', I + '/closure-db.tsv', '--controller-protect', I + '/controller-protect.jsonl',
             '--picks', I + '/picks.json', '--release', RELEASE, '--recent-from', RECENT]
    rules += ['--db-protect', W + '/protect-db-now.tsv', '--nas-evidence', W + '/nas-evidence.tsv']
    subprocess.run([L + '/r2prune', 'plan', '--snapshot', W + '/snapshot.tsv', '--out', W + '/plan'] + rules, check=True)
    summ = json.load(open(W + '/plan/summary.json'))
    manifest = sha(W + '/plan/delete-manifest.jsonl')
    assert manifest == summ['manifest_sha256']
    dele = summ['delete']['objects']
    chunk = []
    if dele:
        assert {c for c in summ['by_class'] if c.startswith('delete_')} == {DELETE_CLASS}, summ['by_class']
        planned = read_manifest(W, manifest)
        if len(planned) != dele:
            raise ValueError(f'{W}: manifest count differs from summary')
        bound_sources = {key: sources[key] for key in planned}
        if len({(s['kind'], s['source_id']) for s in bound_sources.values()}) != dele:
            raise ValueError(f'{W}: manifest sources are ambiguous')
        for key, row in planned.items():
            if not receipt_ready(key, row['size'], row['etag'].strip('"'), bound_sources[key]):
                raise ValueError(f'{W}: manifest key lacks matching receipt source: {key}')
        chunk = verified_chunk(L, W, stamp, planned, bound_sources)
        source_path = W + '/receipt-sources.json'
        durable_write(source_path, json.dumps(bound_sources, sort_keys=True) + '\n')
        with open(W + '/plan/delete-manifest.jsonl', 'rb') as f:
            os.fsync(f.fileno())
        sync_dir(W + '/plan')
        durable_write(W + '/delete-intent.json', json.dumps({
            'manifest_sha256': manifest, 'sources_sha256': sha(source_path)}) + '\n')
        delete = subprocess.run([L + '/r2prune', 'delete', '--manifest', W + '/plan/delete-manifest.jsonl', '--summary', W + '/plan/summary.json',
                        '--fresh-listing', W + '/delete-listing.tsv', '--expected-sha256', manifest, '--ledger', W + '/ledger.jsonl',
                        '--workers', '8'] + rules, env=r2, pass_fds=(lock_fd,))
        finish_receipts(W, lock_fd)
        delete.check_returncode()
    out = {}
    for l in open(W + '/ledger.jsonl') if dele else []:
        d = json.loads(l)
        if 'outcome' in d:
            out[d['outcome']] = out.get(d['outcome'], 0) + 1
    gone = [json.loads(l)['key'] for l in open(W + '/ledger.jsonl') if '"deleted"' in l] if dele else []
    e404 = 0
    sample = random.sample(gone, min(10, len(gone)))
    for k in sample:
        r = subprocess.run([AWS, 's3api', 'head-object', '--endpoint-url', f"https://{r2['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
                            '--bucket', r2['R2_BUCKET'], '--key', k], capture_output=True, text=True,
                           env={**r2, 'AWS_ACCESS_KEY_ID': r2['R2_ACCESS_KEY_ID'], 'AWS_SECRET_ACCESS_KEY': r2['R2_SECRET_ACCESS_KEY'],
                                'AWS_DEFAULT_REGION': 'auto'})
        e404 += r.returncode != 0 and '(404)' in r.stderr
    for f in ('snapshot.tsv', 'delete-listing.tsv', 'protect-db-now.tsv', 'receipts.tsv', 'plan/classified.tsv', 'catalog-vetoes.tsv'):
        Path(W, f).unlink(missing_ok=True)
    log(L, {'cycle': stamp, 'receipts': len(ev), 'present': len(present), 'listed': listed, 'skipped': skipped,
         'plan': {c: v['objects'] for c, v in summ['by_class'].items()}, 'delete_bytes': summ['delete']['bytes'],
         'manifest_sha256': manifest, 'nas_verified': len(chunk), 'deferred': dele - len(chunk),
         'outcomes': out, 'head_404': f'{e404}/{len(sample)}'})
    if e404 != len(sample):
        sys.exit('HEAD sample found deleted keys still present')


if __name__ == '__main__':
    main(sys.argv[1])
