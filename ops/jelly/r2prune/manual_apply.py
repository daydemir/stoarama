# usage: /usr/bin/python3 -I manual_apply.py delete [--chunk N] [--chunks K] WORKDIR -- <r2prune delete args>
#        /usr/bin/python3 -I manual_apply.py receipts WORKDIR
#        /usr/bin/python3 -I manual_apply.py backfill WORKDIR --manifest M --ledger L
#        /usr/bin/python3 -I manual_apply.py status ROOT [ROOT ...]
# The only way to run a manual `r2prune delete`: every acknowledged delete gets its
# r2_object_retirements receipt (and raw purged_at) through receipt-apply.sql, the
# R2 prune lane's receipt path, in the same invocation. Fails closed: a key without
# a catalog identity, or one the receipt path would refuse, stops the run before any
# DELETE, and a receipt write that fails exits non-zero with the intent still open.
import argparse
import concurrent.futures
import csv
import datetime
import email.utils
import enum
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = Path(__file__).resolve().parent
LANE = Path.home() / 'stoarama-r2prune-lane'
DEFAULTS = {
    'r2prune': str(LANE / 'r2prune'),
    'lock': str(LANE / 'cycle.lock'),
    'lane_cycles': str(LANE / 'cycles'),
    'psql': '/opt/homebrew/opt/libpq/bin/psql',
    'db_env': str(Path.home() / 'Build/stoarama/local/recording-supervisor.env'),
    'r2_env': str(Path.home() / 'Build/stoarama/local/render.env'),
}
BUCKET = 'stoarama'
COLLATED = ('joined/goodplus-', 'joined/nightly-collation-v2/', 'joined/best14-20261021/')
KE_CLASS = 'delete_ke_raw_stitched'  # class (a): Keep Everything raw inside a stitched R2 output
OUTCOMES = {'deleted', 'failed', 'skipped_protected', 'skipped_changed'}
FINAL = OUTCOMES - {'failed'}  # a failed DELETE may still have removed the object
ABSENT = 'absent_from_fresh_listing'
CHUNK = 5000
# Rows per catalog statement (source lookup, receipt dry run). One literal of every
# row of a 293k-key backfill dropped the production connection; each statement's
# checks are per row, so splitting changes no result.
ROWS_PER_STATEMENT = 5000


class Mode(enum.Enum):
    LISTING = 'listing'    # the operator's --fresh-listing, pinned for the life of the intent
    VERIFY = 'verify'      # class (a): the wrapper writes one live-verified listing per chunk
    BACKFILL = 'backfill'  # receipts for a run made without the wrapper; never deletes


class Result(enum.Enum):
    """receipt-result.json's state. Only COMPLETE closes the intent."""
    COMPLETE = 'complete'
    OPEN = 'open'          # some manifest key has no final ledger outcome
    DEFERRED = 'deferred'  # backfill --defer-blocked: some acknowledged deletes are held, unreceipted


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


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


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


def batches(rows):
    return [rows[i:i + ROWS_PER_STATEMENT] for i in range(0, len(rows), ROWS_PER_STATEMENT)]


def read_only_copy(cfg, sql, **literals):
    """Runs one read-only COPY script with each :'name' replaced by a dollar-quoted JSON literal."""
    for name, value in literals.items():
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        if '$k$' in text:
            raise SystemExit('a key contains $k$')
        sql = sql.replace(f":'{name}'", '$k$' + text + '$k$')
    res, redact = psql(cfg, [], read_only=True, script=sql)
    if res.returncode:
        raise SystemExit('catalog lookup failed: ' + redact(res.stderr))
    return list(csv.reader(res.stdout.splitlines(), delimiter='\t'))


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
    found = {}
    rows = [row for keys in batches(sorted(k for k in manifest if kind_of(k))) for row in read_only_copy(cfg, SOURCES_SQL, keys=keys)]
    for key, kind, sid, stream, size, digest, etag in rows:
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


def read_listing_keys(path):
    with open(path, encoding='utf-8') as f:
        return {line.split('\t', 1)[0] for line in f}


def ledgers(W, intent):
    """(ledger, listing it was run against or None) for every ledger the intent owns."""
    mode = Mode(intent['mode'])
    if mode is Mode.BACKFILL:
        return [(Path(intent['ledger']), None)]
    out = []
    for path in sorted(Path(W).glob('ledger-*.jsonl')):
        stamp = path.name[len('ledger-'):-len('.jsonl')]
        out.append((path, Path(W, f'listing-{stamp}.tsv') if mode is Mode.VERIFY else None))
    return out


def read_ledger(path, listing, intent, manifest):
    """(header or None, the manifest keys this ledger's listing could act on, its complete rows).
    Refuses a ledger that is not the bound manifest's, was run against another listing than
    the intent pins, logs a key twice, or acts on a key outside its listing."""
    with open(path, 'rb') as f:
        os.fsync(f.fileno())
        first = f.readline()
        if not first.endswith(b'\n'):
            return None, set(), []  # r2prune died before its header: it touched nothing
        header = json.loads(first)
        if header['type'] != 'run' or header['manifest_sha256'] != intent['manifest_sha256']:
            raise SystemExit(f'{path}: ledger does not describe the bound manifest')
        want = sha(listing) if listing else intent['fresh_listing_sha256']
        if header.get('fresh_listing_sha256') != want:
            raise SystemExit(f'{path}: ledger was run against another fresh listing than the intent pins')
        scope = read_listing_keys(listing) & manifest.keys() if listing else manifest.keys()
        seen, rows = set(), []
        for line in f:
            if not line.endswith(b'\n'):
                break  # a half-written last line is unlogged
            row = json.loads(line)
            key = row['key']
            if (key in seen or key not in manifest or row['size'] != manifest[key]['size']
                    or row['outcome'] not in OUTCOMES):
                raise SystemExit(f'{path}: duplicate or unexpected ledger row for {key}')
            seen.add(key)
            if key not in scope and (row['outcome'], row.get('detail')) != ('skipped_changed', ABSENT):
                raise SystemExit(f'{path}: {key} is outside this ledger\'s listing but logged {row["outcome"]}')
            rows.append((row, line))
    return header, scope, rows


class State:
    """Every ledger of an intent folded: a key outside a chunk's listing is deferred to a
    later chunk (no state), never final."""

    def __init__(self, W, intent, manifest):
        self.deleted, self.final, self.attempted, policies = {}, set(), set(), set()
        for path, listing in ledgers(W, intent):
            header, scope, rows = read_ledger(path, listing, intent, manifest)
            if header is None:
                continue
            policy = {k: header[k] for k in ('inputs_sha256', 'recent_from', 'release') if k in header}
            policies.add(hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(',', ':')).encode()).hexdigest())
            self.attempted |= scope
            for row, _ in rows:
                key = row['key']
                if key not in scope:
                    continue
                if row['outcome'] in FINAL:
                    self.final.add(key)
                if row['outcome'] == 'deleted' and (key not in self.deleted or ts(row['at']) < ts(self.deleted[key][0])):
                    self.deleted[key] = (row['at'], path)
        if len(policies) > 1:
            raise SystemExit(f'{W}: ledgers disagree on the run policy')
        self.policy = policies.pop() if policies else None
        self.unresolved = manifest.keys() - self.final


def compact_ledger(path, listing, intent, manifest):
    """Drops a chunk ledger's deferred rows (keys outside its listing, which r2prune logs
    as absent): one manifest-sized ledger per chunk would not fit on disk. Header and every
    row inside the listing stay byte for byte."""
    header, scope, rows = read_ledger(path, listing, intent, manifest)
    if header is None:
        return
    with open(path, 'rb') as f:
        first = f.readline()
    durable_write(path, (first + b''.join(line for row, line in rows if row['key'] in scope)).decode('utf-8'))


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


def dry_run(cfg, W, tsv, refuse=True):
    """dry-run.sql (plain read-only SELECTs) on receipt rows, ROWS_PER_STATEMENT rows per run;
    any refusal stops unless refuse is False. Every check is per row, so the merged output is
    the one-shot output: the summary (action|kind|reasons|rows|bytes|raw_purged_at_updates)
    summed per group, then every non-insert row (seq|action|reasons|kind|source_id|size) in seq
    order. Returns ({action: rows}, {key: (action, reasons, size)} for every non-insert row)."""
    by_seq = {int(f[0]): (f[4].replace('\\\\', '\\'), int(f[5])) for f in (l.split('\t') for l in tsv.splitlines())}
    path = Path(W, 'preflight')
    path.mkdir(exist_ok=True)
    summary, rows = {}, []
    for part in batches(tsv.splitlines(keepends=True)):
        durable_write(path / 'receipt-backfill.tsv', ''.join(part))
        res, redact = psql(cfg, ['-f', str(HERE / 'dry-run.sql')], read_only=True, cwd=path)
        if res.returncode:
            raise SystemExit('receipt preflight failed: ' + redact(res.stderr))
        for line in res.stdout.splitlines():
            f = line.split('|')
            if f[0].isdigit():
                rows.append((int(f[0]), line))
            else:
                total = summary.setdefault(tuple(f[:3]), [0, 0, 0])
                for i, n in enumerate(f[3:6]):
                    total[i] += int(n)
    durable_write(path / 'dry-run.out', ''.join('|'.join(k + tuple(map(str, v))) + '\n' for k, v in sorted(summary.items()))
                  + ''.join(line + '\n' for _, line in sorted(rows)))
    blocked = ['|'.join(k + tuple(map(str, v))) for k, v in sorted(summary.items()) if k[0] == 'blocked']
    if blocked:
        print('\n'.join(blocked), file=sys.stderr)
        if refuse:
            raise SystemExit('the receipt path would refuse planned keys; see preflight/dry-run.out and remove them from the plan')
    actions = {}
    for (action, _, _), (n, _, _) in summary.items():
        actions[action] = actions.get(action, 0) + n
    keys = {}
    for seq, line in rows:
        _, action, reasons = line.split('|')[:3]
        key, size = by_seq[seq]
        keys[key] = (action, reasons, size)
    return actions, keys


def preflight(cfg, W, keys, manifest, sources, manifest_sha):
    """The receipt path's checks on every key this r2prune call may delete, as if deleted now."""
    at = now_utc().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    dry_run(cfg, W, receipt_rows({k: at for k in keys}, manifest, sources, manifest_sha, '0' * 64))


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


def load_intent(W):
    """The intent with its manifest and receipt sources, both re-checked against its digests."""
    intent = json.loads(Path(W, 'delete-intent.json').read_text())
    Mode(intent['mode'])
    if sha(intent['manifest']) != intent['manifest_sha256'] or sha(Path(W, 'receipt-sources.json')) != intent['sources_sha256']:
        raise SystemExit(f'{W}: manifest or receipt sources changed since the intent')
    if 'ledger' in intent and sha(intent['ledger']) != intent['ledger_sha256']:
        raise SystemExit(f'{W}: the backfilled ledger changed since the intent')
    sources = json.loads(Path(W, 'receipt-sources.json').read_text())
    return intent, read_manifest(intent['manifest']), sources


def is_complete(W):
    result = Path(W, 'receipt-result.json')
    return result.exists() and json.loads(result.read_text())['complete']  # lane cycles write the same field


def finish_receipts(cfg, W, only=None, skip=frozenset(), deferred=frozenset()):
    """Receipts for the acknowledged deletes in W (only those first acknowledged by ledger
    `only`, when given; never those in skip, already applied, or deferred, held). The intent
    closes only when every manifest key has a final ledger outcome and none is deferred.
    Returns the folded State."""
    intent, manifest, sources = load_intent(W)
    st = State(W, intent, manifest)
    deleted = {k: at for k, (at, ledger) in st.deleted.items()
               if (only is None or ledger == only) and k not in skip and k not in deferred}
    state = Result.DEFERRED if deferred else Result.OPEN if st.unresolved else Result.COMPLETE
    durable_write(Path(W, 'receipt-backfill.tsv'), receipt_rows(deleted, manifest, sources, intent['manifest_sha256'], st.policy))
    if deleted:
        apply_receipts(cfg, W, len(deleted))
    durable_write(Path(W, 'receipt-result.json'), json.dumps({
        'complete': state is Result.COMPLETE, 'state': state.value, 'deleted_receipts': len(st.deleted) - len(deferred),
        'deferred': len(deferred), 'unresolved': len(st.unresolved),
        'manifest_sha256': intent['manifest_sha256'], 'at': now_utc().isoformat()}) + '\n')
    print(json.dumps({'workdir': str(W), 'state': state.value, 'deleted_receipts': len(st.deleted) - len(deferred),
                      'applied_now': len(deleted), 'deferred': len(deferred), 'unresolved': len(st.unresolved)}))
    return st


def options(args, name):
    """Every value Go's flag package would read for -name or --name."""
    values = []
    for i, a in enumerate(args):
        flag, eq, value = a.partition('=')
        if flag in ('-' + name, '--' + name):
            values.append(value if eq else args[i + 1] if i + 1 < len(args) else '')
    return values


def one_option(args, name):
    values = options(args, name)
    if len(values) != 1 or not values[0]:
        raise SystemExit(f'pass --{name} exactly once')
    return values[0]


def open_intents(roots):
    """(workdir, manifest path) of every intent under roots without a complete receipt result.
    Lane cycles keep their manifest at plan/delete-manifest.jsonl."""
    for root in roots:
        for path in sorted(Path(root).glob('*/delete-intent.json')):
            if not is_complete(path.parent):
                intent = json.loads(path.read_text())
                yield path.parent, Path(intent.get('manifest') or path.parent / 'plan/delete-manifest.jsonl')


# ---- class (a): live re-verify of each chunk before its r2prune call ----

def r2_store(path):
    e = env_file(path)
    endpoint = e.get('R2_ENDPOINT') or f'https://{e["R2_ACCOUNT_ID"]}.r2.cloudflarestorage.com'
    return {'endpoint': endpoint.rstrip('/'), 'access': e['R2_ACCESS_KEY_ID'], 'secret': e['R2_SECRET_ACCESS_KEY'],
            'bucket': e['R2_BUCKET']}


def r2_head(store, key):
    """(size, etag, mtime) of a live R2 object, or None when absent. SigV4, region auto, path style."""
    path = urllib.parse.quote(f'/{store["bucket"]}/{key}', safe='/')
    for attempt in range(8):
        at = now_utc()
        amz, day = at.strftime('%Y%m%dT%H%M%SZ'), at.strftime('%Y%m%d')
        payload = hashlib.sha256(b'').hexdigest()
        headers = {'host': urllib.parse.urlsplit(store['endpoint']).netloc, 'x-amz-content-sha256': payload, 'x-amz-date': amz}
        signed = ';'.join(sorted(headers))
        canonical = '\n'.join(['HEAD', path, ''] + [f'{k}:{headers[k]}' for k in sorted(headers)] + ['', signed, payload])
        scope = f'{day}/auto/s3/aws4_request'
        k = ('AWS4' + store['secret']).encode()
        for part in (day, 'auto', 's3', 'aws4_request'):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        sig = hmac.new(k, '\n'.join(['AWS4-HMAC-SHA256', amz, scope, hashlib.sha256(canonical.encode()).hexdigest()]).encode(),
                       hashlib.sha256).hexdigest()
        headers['authorization'] = f'AWS4-HMAC-SHA256 Credential={store["access"]}/{scope}, SignedHeaders={signed}, Signature={sig}'
        try:
            with urllib.request.urlopen(urllib.request.Request(store['endpoint'] + path, method='HEAD', headers=headers), timeout=60) as r:
                return (int(r.headers['Content-Length']), r.headers['ETag'].strip('"'),
                        int(email.utils.parsedate_to_datetime(r.headers['Last-Modified']).timestamp()))
        except urllib.error.HTTPError as err:
            if err.code == 404:
                return None
            if err.code not in (429, 500, 502, 503, 504) or attempt == 7:
                raise SystemExit(f'R2 HEAD {key}: HTTP {err.code}')
        except (urllib.error.URLError, OSError) as err:
            if attempt == 7:
                raise SystemExit(f'R2 HEAD {key}: {type(err).__name__}')
        time.sleep(2 ** attempt)


def load_coverage(path, manifest):
    """{raw key: [(output key, output size)]}: the --ke-coverage rows r2prune would match for
    each manifest raw (raw_size equal to the planned size)."""
    cover = {}
    with open(path, encoding='utf-8') as f:
        for line in f:
            raw, raw_size, out, out_size = line.rstrip('\n').split('\t')
            if raw in manifest and int(raw_size) == manifest[raw]['size']:
                cover.setdefault(raw, []).append((out, int(out_size)))
    missing = [k for k in manifest if k not in cover]
    if missing:
        raise SystemExit(f'{len(missing)} manifest keys have no --ke-coverage row at their size, e.g. {missing[0]}')
    return cover


VERIFY_SQL = """BEGIN READ ONLY;
SET LOCAL statement_timeout='900s';
COPY (
 WITH r AS (SELECT * FROM jsonb_to_recordset(:'raws'::jsonb) AS x(key text, clip bigint)),
      p AS (SELECT * FROM jsonb_to_recordset(:'pairs'::jsonb) AS x(raw text, clip bigint, output text)),
      o AS (SELECT DISTINCT output AS key FROM p)
 SELECT 'raw', r.key, c.size_bytes::text, (c.purged_at IS NULL)::text, NULL, NULL, NULL, NULL
 FROM r JOIN recording_clips c ON c.id=r.clip AND c.bucket='stoarama' AND c.object_key=r.key
 UNION ALL
 SELECT 'output', o.key, count(x.id)::text, min(x.size_bytes)::text,
  EXISTS(SELECT FROM r2_object_retirements z WHERE z.bucket='stoarama' AND z.object_key=o.key)::text,
  EXISTS(SELECT FROM r2_retirement_reservations z WHERE z.bucket='stoarama' AND z.object_key=o.key)::text,
  r2_owner_keep_held('stoarama',o.key)::text,
  EXISTS(SELECT FROM r2_nonraw_intents z WHERE z.bucket='stoarama' AND z.object_key=o.key)::text
 FROM o LEFT JOIN recording_collation_outputs x ON x.object_key=o.key GROUP BY o.key
 UNION ALL
 SELECT 'pair', p.raw, p.output, EXISTS(SELECT FROM recording_collation_outputs x
   WHERE x.object_key=p.output AND p.clip=ANY(x.source_clip_ids))::text, NULL, NULL, NULL, NULL
 FROM p
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\\t');
ROLLBACK;
"""


def verify_chunk(cfg, store, batch, manifest, sources, cover, attempted, held):
    """The listing rows for one chunk, each proven live right now; stops on the first mismatch.
    A raw an earlier chunk already sent to DELETE (failed or unlogged) may be absent: it is
    listed at its planned identity so r2prune's idempotent DELETE acknowledges it."""
    outputs = sorted({o for k in batch for o, _ in cover[k]})
    for o in outputs:
        if o in held:
            raise SystemExit(f'verify: covering output {o} is in another open intent\'s manifest')
    with concurrent.futures.ThreadPoolExecutor(16) as ex:
        heads = dict(zip(batch + outputs, ex.map(lambda k: r2_head(store, k), batch + outputs)))
    rows = []
    for k in batch:
        m = manifest[k]
        planned = (m['size'], m['etag'].strip('"'), m['mtime'])
        if heads[k] != planned and not (heads[k] is None and k in attempted):
            raise SystemExit(f'verify: R2 HEAD of {k} is {heads[k]}, planned {planned}')
        rows.append((k,) + planned)
    for o in outputs:
        sizes = {size for k in batch for out, size in cover[k] if out == o}
        if heads[o] is None or {heads[o][0]} != sizes:
            raise SystemExit(f'verify: R2 HEAD of covering output {o} is {heads[o]}, coverage sizes {sorted(sizes)}')
        rows.append((o,) + heads[o])
    db = {}
    for row in read_only_copy(cfg, VERIFY_SQL, raws=[{'key': k, 'clip': sources[k]['source_id']} for k in batch],
                              pairs=[{'raw': k, 'clip': sources[k]['source_id'], 'output': o} for k in batch for o, _ in cover[k]]):
        db[(row[0], row[1]) if row[0] != 'pair' else ('pair', row[1], row[2])] = row
    for k in batch:
        got = db.get(('raw', k))
        if got is None or got[2:4] != [str(manifest[k]['size']), 'true']:
            raise SystemExit(f'verify: recording_clips row for {k} is {got and got[2:4]}, want size {manifest[k]["size"]} and purged_at NULL')
    for o in outputs:
        got = db[('output', o)]
        if got[2:] != ['1', str(heads[o][0]), 'false', 'false', 'false', 'false']:
            raise SystemExit(f'verify: covering output {o} catalog [rows, size, receipt, reservation, owner keep, nonraw intent] '
                             f'is {got[2:]}, want one row at size {heads[o][0]} with none of the others')
    for k in batch:
        for o, _ in cover[k]:
            if db[('pair', k, o)][3] != 'true':
                raise SystemExit(f'verify: {k} is no longer in source_clip_ids of {o}')
    return ''.join('\t'.join(map(str, r)) + '\n' for r in sorted(rows))


# ---- commands ----

def run_r2prune(cfg, W, args, lock_fd, listing):
    """One r2prune call with a new ledger (and, in verify mode, the chunk's own listing);
    returns (its exit code, the ledger path, the listing path or None)."""
    stamp = now_utc().strftime('%Y%m%dT%H%M%S%fZ')
    ledger, listing_path = Path(W, f'ledger-{stamp}.jsonl'), None
    if listing is not None:
        listing_path = Path(W, f'listing-{stamp}.tsv')
        durable_write(listing_path, listing)
        args = args + ['--fresh-listing', str(listing_path)]
    Path(W, 'receipt-result.json').unlink(missing_ok=True)
    run = subprocess.run([cfg.r2prune, 'delete', '--ledger', str(ledger)] + args, env=env_file(cfg.r2_env), pass_fds=(lock_fd,))
    return run.returncode, ledger, listing_path


def delete(cfg, W, args, lock_fd):
    if options(args, 'ledger'):
        raise SystemExit('the wrapper owns --ledger; do not pass it')
    manifest_path, expected = str(Path(one_option(args, 'manifest')).resolve()), one_option(args, 'expected-sha256')
    if sha(manifest_path) != expected:
        raise SystemExit('manifest digest differs from --expected-sha256')
    manifest = read_manifest(manifest_path)
    classes = {row['class'] for row in manifest.values()}
    mode = Mode.VERIFY if KE_CLASS in classes else Mode.LISTING
    if mode is Mode.VERIFY:
        if classes != {KE_CLASS}:
            raise SystemExit(f'a {KE_CLASS} manifest must hold only that class; plan it separately')
        if options(args, 'fresh-listing') or options(args, 'limit'):
            raise SystemExit(f'for {KE_CLASS} the wrapper owns --fresh-listing and chunks with --chunk/--chunks; drop --fresh-listing and --limit')
        listing_sha = None
    else:
        if cfg.chunk is not None or cfg.chunks is not None:
            raise SystemExit(f'--chunk and --chunks apply only to {KE_CLASS} manifests')
        listing_sha = sha(one_option(args, 'fresh-listing'))
    Path(W).mkdir(parents=True, exist_ok=True)
    intent_path = Path(W, 'delete-intent.json')
    if intent_path.exists():
        intent, _, sources = load_intent(W)
        if intent['manifest_sha256'] != expected or Mode(intent['mode']) is not mode:
            raise SystemExit(f'{W} is bound to another manifest; use a new workdir')
        if is_complete(W):
            print(json.dumps({'workdir': W, 'complete': True}))
            return
        if intent.get('fresh_listing_sha256') != listing_sha:
            raise SystemExit(f'{W}: the open intent pins fresh listing {intent["fresh_listing_sha256"]}; '
                             'rerun with that same --fresh-listing until it closes')
        finish_receipts(cfg, W)
    else:
        sources = resolve_sources(cfg, manifest)
        durable_write(Path(W, 'receipt-sources.json'), json.dumps(sources, sort_keys=True) + '\n')
        intent = {'mode': mode.value, 'manifest': manifest_path, 'manifest_sha256': expected,
                  'sources_sha256': sha(Path(W, 'receipt-sources.json')), 'fresh_listing_sha256': listing_sha}

    def call(keys, listing):
        """Preflight, bind the intent, one r2prune call, then its receipts."""
        preflight(cfg, W, keys, manifest, sources, expected)
        if not intent_path.exists():
            durable_write(intent_path, json.dumps(intent) + '\n')
        rc, ledger, listing_path = run_r2prune(cfg, W, args, lock_fd, listing)
        if listing_path:
            compact_ledger(ledger, listing_path, intent, manifest)
        st = finish_receipts(cfg, W, only=ledger)
        if rc:
            raise SystemExit(f'r2prune delete exited {rc}; receipts for its acknowledged deletes are written')
        return st

    if mode is Mode.LISTING:
        st = call(sorted(manifest.keys() - State(W, intent, manifest).deleted.keys()), None)
        if st.unresolved and options(args, 'limit') in ([], ['0']):
            raise SystemExit(f'{len(st.unresolved)} planned keys have no final ledger outcome (unlogged or failed); '
                             'rerun the same command with the same --fresh-listing')
        return

    store = r2_store(cfg.r2_env)
    cover = load_coverage(one_option(args, 'ke-coverage'), manifest)
    held = set()
    for other, path in open_intents({Path(W).parent, Path(cfg.lane_cycles)}):
        if other != Path(W):
            held |= read_manifest(path).keys()
    for n in range(cfg.chunks or sys.maxsize):
        st = State(W, intent, manifest)
        batch = (sorted(st.unresolved & st.attempted) + [k for k in manifest if k not in st.attempted])[:cfg.chunk or CHUNK]
        if not batch:
            return
        st = call(batch, verify_chunk(cfg, store, batch, manifest, sources, cover, st.attempted, held))
        left = st.unresolved & set(batch)
        if left:
            raise SystemExit(f'chunk {n + 1}: {len(left)} keys have no final ledger outcome (unlogged or failed); '
                             'rerun the same command to settle them first')


def backfill(cfg, W, manifest_path, ledger_path, defer_blocked):
    """Receipts for a run made without the wrapper, at its ledger's own times. Never deletes."""
    manifest_path, ledger_path = str(Path(manifest_path).resolve()), str(Path(ledger_path).resolve())
    manifest_sha = sha(manifest_path)
    with open(ledger_path, 'rb') as f:
        header = json.loads(f.readline())
    if header['type'] != 'run' or header['manifest_sha256'] != manifest_sha:
        raise SystemExit(f'{ledger_path}: ledger header does not describe manifest {manifest_sha}')
    Path(W).mkdir(parents=True, exist_ok=True)
    intent_path = Path(W, 'delete-intent.json')
    if intent_path.exists():
        intent, manifest, sources = load_intent(W)
        if (intent['mode'], intent['manifest'], intent['ledger']) != (Mode.BACKFILL.value, manifest_path, ledger_path):
            raise SystemExit(f'{W} is bound to another backfill; use a new workdir')
        if is_complete(W):
            st = State(W, intent, manifest)
            actions, _ = dry_run(cfg, W, receipt_rows({k: at for k, (at, _) in st.deleted.items()}, manifest, sources,
                                                   manifest_sha, st.policy))
            if set(actions) - {'already_applied'}:
                raise SystemExit(f'{W}: closed backfill no longer reads as applied: {actions}')
            print(json.dumps({'workdir': W, 'already_applied': actions.get('already_applied', 0)}))
            return
    else:
        manifest = read_manifest(manifest_path)
        sources = resolve_sources(cfg, manifest)
        durable_write(Path(W, 'receipt-sources.json'), json.dumps(sources, sort_keys=True) + '\n')
        intent = {'mode': Mode.BACKFILL.value, 'manifest': manifest_path, 'manifest_sha256': manifest_sha,
                  'sources_sha256': sha(Path(W, 'receipt-sources.json')), 'ledger': ledger_path,
                  'ledger_sha256': sha(ledger_path), 'fresh_listing_sha256': header.get('fresh_listing_sha256')}
    # Every run dry-runs every acknowledged delete: already applied rows are skipped, and with
    # --defer-blocked a held key is recorded in deferred-blocked.tsv and left unreceipted (its
    # hold is never touched) while the rest apply. A later run applies whatever its hold released.
    st = State(W, intent, manifest)
    _, rows = dry_run(cfg, W, receipt_rows({k: at for k, (at, _) in st.deleted.items()}, manifest, sources, manifest_sha, st.policy),
                      refuse=not defer_blocked)
    deferred = {k: v for k, v in rows.items() if v[0] == 'blocked'}
    if not intent_path.exists():
        durable_write(intent_path, json.dumps(intent) + '\n')
    if deferred:
        durable_write(Path(W, 'deferred-blocked.tsv'), ''.join(f'{k}\t{size}\t{reasons}\n' for k, (_, reasons, size) in sorted(deferred.items())))
    else:
        Path(W, 'deferred-blocked.tsv').unlink(missing_ok=True)
    st = finish_receipts(cfg, W, skip={k for k, v in rows.items() if v[0] == 'already_applied'}, deferred=set(deferred))
    if st.unresolved:
        raise SystemExit(f'{len(st.unresolved)} manifest keys have no final outcome in {ledger_path}; '
                         'they may or may not be gone from R2 and need their own reconciliation')


def status(roots):
    n = 0
    for W, _ in open_intents(roots):
        n += 1
        intent = json.loads(Path(W, 'delete-intent.json').read_text())
        result = Path(W, 'receipt-result.json')
        r = json.loads(result.read_text()) if result.exists() else {}
        print(json.dumps({'workdir': str(W), 'mode': intent.get('mode', 'lane'), 'manifest_sha256': intent['manifest_sha256'],
                          'state': r.get('state', Result.OPEN.value), 'unresolved': r.get('unresolved'), 'deferred': r.get('deferred', 0)}))
    if n:
        raise SystemExit(f'{n} open delete intents')
    print(json.dumps({'open': 0}))


def main(argv):
    p = argparse.ArgumentParser(description='Manual r2prune delete that writes its retirement receipts at apply time.')
    for k, v in DEFAULTS.items():
        p.add_argument('--' + k.replace('_', '-'), default=v)
    sub = p.add_subparsers(dest='cmd', required=True)
    d = sub.add_parser('delete', help='resolve receipt sources, preflight, run r2prune delete, write receipts')
    d.add_argument('--chunk', type=int, help=f'{KE_CLASS}: raws live-verified per r2prune call (default {CHUNK})')
    d.add_argument('--chunks', type=int, help=f'{KE_CLASS}: stop after this many chunks (a canary)')
    d.add_argument('workdir')
    d.add_argument('args', nargs=argparse.REMAINDER, help='-- then the r2prune delete arguments (not --ledger)')
    r = sub.add_parser('receipts', help='write the receipts of a workdir whose intent is still open')
    r.add_argument('workdir')
    b = sub.add_parser('backfill', help='receipts for a run made without this wrapper; never deletes')
    b.add_argument('workdir')
    b.add_argument('--manifest', required=True)
    b.add_argument('--ledger', required=True)
    b.add_argument('--defer-blocked', action='store_true',
                   help='apply every receipt the dry run admits; record held keys in deferred-blocked.tsv and keep the intent open')
    s = sub.add_parser('status', help='list open delete intents under each root; exits non-zero if any')
    s.add_argument('roots', nargs='+')
    cfg = p.parse_args(argv)
    if cfg.cmd == 'status':
        return status(cfg.roots)
    W = str(Path(cfg.workdir).resolve())
    with open(cfg.lock, 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f'{cfg.lock} is held: a lane cycle or another manual apply is running')
        if cfg.cmd == 'receipts':
            finish_receipts(cfg, W)
        elif cfg.cmd == 'backfill':
            backfill(cfg, W, cfg.manifest, cfg.ledger, cfg.defer_blocked)
        else:
            delete(cfg, W, cfg.args[1:] if cfg.args[:1] == ['--'] else cfg.args, lock.fileno())


if __name__ == '__main__':
    main(sys.argv[1:])
