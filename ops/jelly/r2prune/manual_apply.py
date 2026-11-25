# usage: /usr/bin/python3 -I manual_apply.py delete WORKDIR -- <r2prune delete args>
#        /usr/bin/python3 -I manual_apply.py receipts WORKDIR
# The only way to run a manual `r2prune delete`: every acknowledged delete gets its
# r2_object_retirements receipt (and raw purged_at) through receipt-apply.sql, the
# R2 prune lane's receipt path, in the same invocation. Fails closed: a key without
# a catalog identity, or one the receipt path would refuse, stops the run before any
# DELETE, and a receipt write that fails exits non-zero with the intent still open.
import argparse
import csv
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.parse

HERE = Path(__file__).resolve().parent
LANE = Path.home() / 'stoarama-r2prune-lane'
DEFAULTS = {
    'r2prune': str(LANE / 'r2prune'),
    'lock': str(LANE / 'cycle.lock'),
    'psql': '/opt/homebrew/opt/libpq/bin/psql',
    'db_env': str(Path.home() / 'Build/stoarama/local/recording-supervisor.env'),
    'r2_env': str(Path.home() / 'Build/stoarama/local/render.env'),
}
BUCKET = 'stoarama'
COLLATED = ('joined/goodplus-', 'joined/nightly-collation-v2/', 'joined/best14-20261021/')
OUTCOMES = {'deleted', 'failed', 'skipped_protected', 'skipped_changed'}


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def durable_write(path, contents):
    path = Path(path)
    tmp = path.with_name(path.name + '.tmp')
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(contents)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def env_file(path):
    out = subprocess.run(['/bin/sh', '-c', 'set -a; . "$0"; set +a; env -0', path],
                         check=True, capture_output=True).stdout
    return dict(kv.split('=', 1) for kv in out.decode().split('\0') if '=' in kv)


def pg_env(path, read_only):
    """libpq PG* variables from DATABASE_URL, so no credential appears in argv."""
    e = env_file(path)
    u = urllib.parse.urlsplit(e.pop('DATABASE_URL'))
    e.update(PGHOST=u.hostname or '', PGPORT=str(u.port or 5432), PGUSER=urllib.parse.unquote(u.username or ''),
             PGPASSWORD=urllib.parse.unquote(u.password or ''), PGDATABASE=u.path.lstrip('/'),
             PGOPTIONS='-c default_transaction_read_only=' + ('on' if read_only else 'off'))
    for k, v in urllib.parse.parse_qsl(u.query):
        if k == 'sslmode':
            e['PGSSLMODE'] = v
    return e


def psql(cfg, args, *, read_only, script=None, cwd=None):
    e = pg_env(cfg.db_env, read_only)
    res = subprocess.run([cfg.psql, '-XqAt', '-v', 'ON_ERROR_STOP=1'] + args, input=script or '', cwd=cwd,
                         env=e, capture_output=True, text=True)
    return res, (lambda s: s.replace(e['PGPASSWORD'], '[redacted]') if e['PGPASSWORD'] else s)


def read_manifest(path):
    manifest = {}
    with open(path, encoding='utf-8') as f:
        for line in f:
            row = json.loads(line)
            if row['key'] in manifest:
                raise SystemExit(f'duplicate key in manifest: {row["key"]}')
            manifest[row['key']] = row
    return manifest


def kind_of(key):
    if key.startswith('managed/'):
        return 'raw'
    if key.startswith(COLLATED):
        return 'collated'
    return None


def receipt_ready(key, size, etag, source):
    """cycle.py's receipt_ready: the catalog identity accounts for the listed object."""
    if (key != key.strip() or size <= 0 or source['size'] != size
            or not re.fullmatch('[0-9a-f]{64}', source['sha256']) or not etag):
        return False
    if source['kind'] == 'raw':
        return key.startswith('managed/') and etag == source['etag']
    return key.startswith(COLLATED)


# Exactly one catalog row per key, with the identity receipt-apply.sql verifies.
SOURCES_SQL = """BEGIN READ ONLY;
SET LOCAL statement_timeout='900s';
COPY (
 WITH k AS (SELECT DISTINCT key FROM jsonb_array_elements_text(:'keys'::jsonb) key)
 SELECT k.key, 'raw', c.id, r.stream_id, c.size_bytes, lower(c.sha256), btrim(c.etag,'"')
 FROM k JOIN recording_clips c ON c.bucket='stoarama' AND c.object_key=k.key JOIN recordings r ON r.id=c.recording_id
 WHERE k.key LIKE 'managed/%'
 UNION ALL
 SELECT k.key, 'collated', o.id, ro.stream_id, o.size_bytes, lower(o.sha256), NULL
 FROM k JOIN recording_collation_outputs o ON o.object_key=k.key
 JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings ro ON ro.id=h.recording_id
 WHERE k.key NOT LIKE 'managed/%'
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\\t');
ROLLBACK;
"""


def resolve_sources(cfg, manifest):
    """{key: receipt source} from the catalog, read-only; refuses any key it cannot account for."""
    bad = {k: 'no catalog kind for this prefix' for k in manifest if not kind_of(k)}
    keys = json.dumps(sorted(k for k in manifest if kind_of(k)), ensure_ascii=False)
    if '$k$' in keys:
        raise SystemExit('a manifest key contains $k$')
    res, redact = psql(cfg, [], read_only=True, script=SOURCES_SQL.replace(":'keys'", '$k$' + keys + '$k$'))
    if res.returncode:
        raise SystemExit('catalog lookup failed: ' + redact(res.stderr))
    found = {}
    for key, kind, sid, stream, size, digest, etag in csv.reader(res.stdout.splitlines(), delimiter='\t'):
        found.setdefault(key, []).append({'kind': kind, 'source_id': int(sid), 'stream_id': int(stream),
                                          'size': int(size), 'sha256': digest, 'etag': etag})
    sources = {}
    for key, row in manifest.items():
        if key in bad:
            continue
        rows = found.get(key, [])
        if len(rows) != 1:
            bad[key] = f'{len(rows)} catalog rows'
        elif not receipt_ready(key, row['size'], row['etag'].strip('"'), rows[0]):
            bad[key] = 'catalog identity differs from the manifest'
        else:
            sources[key] = rows[0]
    if len({(s['kind'], s['source_id']) for s in sources.values()}) != len(sources):
        raise SystemExit('two manifest keys share one catalog source')
    if bad:
        for key, why in sorted(bad.items())[:20]:
            print(f'unreceiptable: {key}: {why}', file=sys.stderr)
        raise SystemExit(f'{len(bad)} manifest keys have no receipt identity; remove them from the plan')
    return sources


def copy_field(value):
    if value is None:
        return r'\N'
    return (str(value).replace('\\', '\\\\').replace('\t', r'\t')
            .replace('\n', r'\n').replace('\r', r'\r'))


def ts(s):
    m = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z', s)
    if not m:
        raise ValueError(f'unexpected ledger time {s}')
    return m[1] + '.' + (m[2] or '').ljust(9, '0')


def ledger_deletes(W, manifest, manifest_sha):
    """Earliest acknowledged delete per key across every ledger in W, and the run policy digest."""
    deleted, policies = {}, set()
    for path in sorted(Path(W).glob('ledger-*.jsonl')):
        with open(path, 'rb') as f:
            os.fsync(f.fileno())
            first = f.readline()
            if not first.endswith(b'\n'):
                continue
            header = json.loads(first)
            if header['type'] != 'run' or header['manifest_sha256'] != manifest_sha:
                raise SystemExit(f'{path}: ledger does not describe the bound manifest')
            policy = {k: header[k] for k in ('inputs_sha256', 'recent_from', 'release') if k in header}
            policies.add(hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(',', ':')).encode()).hexdigest())
            for line in f:
                if not line.endswith(b'\n'):
                    break
                row = json.loads(line)
                if row['key'] not in manifest or row['size'] != manifest[row['key']]['size'] or row['outcome'] not in OUTCOMES:
                    raise SystemExit(f'{path}: unexpected ledger row for {row["key"]}')
                if row['outcome'] == 'deleted' and (row['key'] not in deleted or ts(row['at']) < ts(deleted[row['key']])):
                    deleted[row['key']] = row['at']
    if len(policies) > 1:
        raise SystemExit(f'{W}: ledgers disagree on the run policy')
    return deleted, policies.pop() if policies else None


def receipt_rows(deleted, manifest, sources, manifest_sha, policy_sha):
    """cycle.py's receipt_tsv rows: reason r2prune_ledger_<class>, choice none."""
    out = []
    for seq, (key, at) in enumerate(sorted(deleted.items()), 1):
        planned, source = manifest[key], sources[key]
        etag = planned['etag'].strip('"')
        if not receipt_ready(key, planned['size'], etag, source):
            raise SystemExit(f'receipt source cannot account for {key}')
        out.append('\t'.join(map(copy_field, (
            seq, source['kind'], source['source_id'], source['stream_id'], key, planned['size'],
            source['sha256'], etag, at, 'r2prune_ledger_' + planned['class'], 'none', manifest_sha, policy_sha, None))) + '\n')
    return ''.join(out)


def preflight(cfg, W, manifest, sources, manifest_sha):
    """dry-run.sql's checks on every planned key not yet deleted from W, as if deleted now:
    any refusal stops before DELETE. Keys an earlier ledger in W deleted already have receipts."""
    now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    done, _ = ledger_deletes(W, manifest, manifest_sha)
    tsv = receipt_rows({k: now for k in manifest if k not in done}, manifest, sources, manifest_sha, '0' * 64)
    path = Path(W, 'preflight')
    path.mkdir(exist_ok=True)
    durable_write(path / 'receipt-backfill.tsv', tsv)
    res, redact = psql(cfg, ['-f', str(HERE / 'dry-run.sql')], read_only=True, cwd=path)
    if res.returncode:
        raise SystemExit('receipt preflight failed: ' + redact(res.stderr))
    durable_write(path / 'dry-run.out', res.stdout)
    blocked = [line for line in res.stdout.splitlines() if line.startswith('blocked|')]
    if blocked:
        print('\n'.join(blocked), file=sys.stderr)
        raise SystemExit('the receipt path would refuse planned keys; see preflight/dry-run.out and remove them from the plan')


def apply_receipts(cfg, W, total):
    start_seq = 1
    for attempt in range(30):
        res, redact = psql(cfg, ['-v', f'start_seq={start_seq}', '-v', f'total={total}', '-f', str(HERE / 'receipt-apply.sql')],
                           read_only=False, cwd=W)
        if res.returncode == 0:
            return
        if '55P03:' not in res.stderr or attempt == 29:
            raise SystemExit(f'{W}: receipt apply failed; rerun `manual_apply.py receipts {W}`\n' + redact(res.stderr))
        for line in res.stdout.splitlines():
            if line.startswith('committed:'):
                start_seq = int(line.split(':', 1)[1]) + 1
        time.sleep(10)


def finish_receipts(cfg, W):
    intent = json.loads(Path(W, 'delete-intent.json').read_text())
    if sha(intent['manifest']) != intent['manifest_sha256'] or sha(Path(W, 'receipt-sources.json')) != intent['sources_sha256']:
        raise SystemExit(f'{W}: manifest or receipt sources changed since the intent')
    manifest = read_manifest(intent['manifest'])
    sources = json.loads(Path(W, 'receipt-sources.json').read_text())
    deleted, policy_sha = ledger_deletes(W, manifest, intent['manifest_sha256'])
    durable_write(Path(W, 'receipt-backfill.tsv'), receipt_rows(deleted, manifest, sources, intent['manifest_sha256'], policy_sha))
    if deleted:
        apply_receipts(cfg, W, len(deleted))
    durable_write(Path(W, 'receipt-result.json'), json.dumps({
        'complete': True, 'deleted_receipts': len(deleted), 'manifest_sha256': intent['manifest_sha256'],
        'at': datetime.datetime.now(datetime.timezone.utc).isoformat()}) + '\n')
    print(json.dumps({'workdir': str(W), 'deleted_receipts': len(deleted)}))


def option(args, name):
    for i, a in enumerate(args):
        if a == name and i + 1 < len(args):
            return args[i + 1]
        if a.startswith(name + '='):
            return a.split('=', 1)[1]
    return None


def delete(cfg, W, args, lock_fd):
    manifest_path, expected = option(args, '--manifest'), option(args, '--expected-sha256')
    if option(args, '--ledger') is not None:
        raise SystemExit('the wrapper owns --ledger; do not pass it')
    if not manifest_path or not expected:
        raise SystemExit('--manifest and --expected-sha256 are required')
    manifest_path = str(Path(manifest_path).resolve())
    if sha(manifest_path) != expected:
        raise SystemExit('manifest digest differs from --expected-sha256')
    Path(W).mkdir(parents=True, exist_ok=True)
    intent_path = Path(W, 'delete-intent.json')
    if intent_path.exists():
        intent = json.loads(intent_path.read_text())
        if intent['manifest_sha256'] != expected:
            raise SystemExit(f'{W} is bound to another manifest; use a new workdir')
        if not Path(W, 'receipt-result.json').exists():
            finish_receipts(cfg, W)
        sources = json.loads(Path(W, 'receipt-sources.json').read_text())
        manifest = read_manifest(manifest_path)
    else:
        manifest = read_manifest(manifest_path)
        sources = resolve_sources(cfg, manifest)
        durable_write(Path(W, 'receipt-sources.json'), json.dumps(sources, sort_keys=True) + '\n')
    preflight(cfg, W, manifest, sources, expected)
    durable_write(intent_path, json.dumps({'manifest': manifest_path, 'manifest_sha256': expected,
                                           'sources_sha256': sha(Path(W, 'receipt-sources.json'))}) + '\n')
    Path(W, 'receipt-result.json').unlink(missing_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    run = subprocess.run([cfg.r2prune, 'delete', '--ledger', str(Path(W, f'ledger-{stamp}.jsonl'))] + args,
                         env=env_file(cfg.r2_env), pass_fds=(lock_fd,))
    finish_receipts(cfg, W)
    if run.returncode:
        raise SystemExit(f'r2prune delete exited {run.returncode}; receipts for its acknowledged deletes are written')


def main(argv):
    p = argparse.ArgumentParser(description='Manual r2prune delete that writes its retirement receipts at apply time.')
    for k, v in DEFAULTS.items():
        p.add_argument('--' + k.replace('_', '-'), default=v)
    sub = p.add_subparsers(dest='cmd', required=True)
    d = sub.add_parser('delete', help='resolve receipt sources, preflight, run r2prune delete, write receipts')
    d.add_argument('workdir')
    d.add_argument('args', nargs=argparse.REMAINDER, help='-- then the r2prune delete arguments (not --ledger)')
    r = sub.add_parser('receipts', help='write the receipts of a workdir whose intent is still open')
    r.add_argument('workdir')
    cfg = p.parse_args(argv)
    W = str(Path(cfg.workdir).resolve())
    with open(cfg.lock, 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if cfg.cmd == 'receipts':
            finish_receipts(cfg, W)
        else:
            delete(cfg, W, cfg.args[1:] if cfg.args[:1] == ['--'] else cfg.args, lock.fileno())


if __name__ == '__main__':
    main(sys.argv[1:])
