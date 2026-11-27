# Stage / swap / roll back a GSS public generation built by build_generation.py.
#   gss_gen.py stage  DIR          create-only asset uploads (sha+ETag checked), hold rows (checked against
#                                  the live hold of each replaced asset), create-only generations/<id>.json
#   gss_gen.py swap ATTEMPT PARENT CANDIDATE       conditional forward move of both pointers
#   gss_gen.py rollback ATTEMPT CANDIDATE PARENT   cancel attempt, then repair either/both pointers
#   gss_gen.py verify ID                   verify both pointers against the staged registry
# Credentials only via env files; nothing secret is printed.
import time, csv, gzip, hashlib, json, os, subprocess, sys, urllib.parse
import fcntl
import re
import tempfile
from pathlib import Path

H = os.path.expanduser('~/stoarama-gss-freeze')
P = 'datasets/global-street-scores-delivery/47/public'
AWS = '/Users/jelly/.local/bin/aws'


def r2env():
    e = dict(os.environ)
    for l in open(os.path.expanduser('~/Build/stoarama/local/render.env')):
        k, _, v = l.strip().replace('export ', '', 1).partition('=')
        if k.startswith('R2_'):
            e[k] = v.strip().strip('"\'')
    e.update(AWS_ACCESS_KEY_ID=e['R2_ACCESS_KEY_ID'], AWS_SECRET_ACCESS_KEY=e['R2_SECRET_ACCESS_KEY'], AWS_DEFAULT_REGION='auto')
    return e


# Load credentials only on actual CLI invocation, never on import.
E = B = EP = None


def s3(*args, check=True):
    p = subprocess.run([AWS, 's3api', '--endpoint-url', EP, '--output', 'json', *args], env=E, capture_output=True, text=True)
    if check and p.returncode:
        raise SystemExit('s3api failed: ' + p.stderr[-300:])
    return p


def head(key):
    p = s3('head-object', '--bucket', B, '--key', key, check=False)
    return json.loads(p.stdout) if p.returncode == 0 else None


def pg(sql, write=False):
    e = dict(os.environ)
    for l in open(os.path.expanduser('~/Build/stoarama/local/recording-supervisor.env')):
        k, _, v = l.strip().replace('export ', '', 1).partition('=')
        if k == 'DATABASE_URL':
            u = urllib.parse.urlsplit(v.strip().strip('"\''))
            e.update(PGHOST=u.hostname, PGPORT=str(u.port or 5432), PGUSER=urllib.parse.unquote(u.username),
                     PGPASSWORD=urllib.parse.unquote(u.password), PGDATABASE=u.path.lstrip('/'),
                     PGOPTIONS='-c statement_timeout=3600000' + ('' if write else ' -c default_transaction_read_only=on'))
            e.update({'PG' + a.upper(): b for a, b in urllib.parse.parse_qsl(u.query) if a == 'sslmode'})
    p = subprocess.run(['/opt/homebrew/opt/libpq/bin/psql', '-XqAt', '-F', '\t', '-v', 'ON_ERROR_STOP=1'],
                       input=sql, env=e, capture_output=True, text=True)
    if p.returncode:
        raise SystemExit('psql failed: ' + p.stderr[-400:])
    return p.stdout


CURRENT = P + '/current.json'


def generation_id(gen):
    if not isinstance(gen, str) or not re.fullmatch('[0-9a-f]{32}', gen):
        raise ValueError('invalid generation ID')
    return gen


def registry(body):
    return json.loads(gzip.decompress(body))


def get(key):
    # A single GET supplies both the bytes and their ETag; HEAD followed by an
    # unconditional GET could pair a new body with an old ETag.
    with tempfile.TemporaryDirectory() as directory:
        path = directory + '/registry.json.gz'
        result = s3('get-object', '--bucket', B, '--key', key, path)
        return Path(path).read_bytes(), json.loads(result.stdout)['ETag']


def staged(gen):
    body, _ = get(P + '/generations/%s.json' % generation_id(gen))
    if registry(body)['generation'] != gen:
        raise RuntimeError('staged registry has wrong generation: ' + gen)
    return body


def pin_id():
    return generation_id(pg('SELECT generation FROM gss_public_generation_pin WHERE singleton').strip())


def snapshot(allowed):
    body, etag = get(CURRENT)
    current = generation_id(registry(body)['generation'])
    pin = pin_id()
    if current not in allowed or pin not in allowed or body != allowed[current]:
        raise RuntimeError(f'unexpected pointer state: current={current}, pin={pin}')
    return current, etag, pin


def verify(gen):
    snapshot({gen: staged(gen)})
    print('verified both pointers', gen)


def put_current(old, etag, new_body):
    arch = P + '/archive/current-%s.json' % old
    if not head(arch):
        s3('copy-object', '--bucket', B, '--key', arch, '--copy-source', B + '/' + CURRENT,
           '--copy-source-if-match', etag, '--if-none-match', '*')
    with tempfile.TemporaryDirectory() as directory:
        path = directory + '/registry.json.gz'
        Path(path).write_bytes(new_body)
        s3('put-object', '--bucket', B, '--key', CURRENT, '--body', path,
           '--content-type', 'application/gzip', '--if-match', etag)


def move_pin(old, new):
    # Both IDs have been validated. The singleton and expected generation are
    # part of the UPDATE predicate; a competing pin must never be overwritten.
    got = pg("UPDATE gss_public_generation_pin SET generation='%s', pinned_at=clock_timestamp() "
             "WHERE singleton AND generation='%s' RETURNING generation" % (new, old), write=True).strip()
    if got != new:
        raise RuntimeError('pin compare-and-set failed')


def move(source, target, bodies, initial):
    current, etag, pin = initial
    if current != target:
        put_current(source, etag, bodies[target])
    # Check the object readback before touching the pin, and refuse unrelated
    # state even on a retry after an ambiguous command outcome.
    current, _, pin = snapshot(bodies)
    if current != target:
        raise RuntimeError('current.json readback differs')
    if pin != target:
        move_pin(source, target)
    snapshot({target: bodies[target]})
    print('verified both pointers', target)


FAILURES = (Exception, SystemExit, KeyboardInterrupt)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_durable_json(path, value):
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as f:
            tmp = f.name
            json.dump(value, f, sort_keys=True)
            f.write('\n')
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        tmp = None
        sync_directory(path.parent)
    finally:
        if tmp is not None:
            os.unlink(tmp)


def attempt_identity(attempt, parent, candidate):
    return {'attempt': generation_id(attempt), 'parent': generation_id(parent),
            'candidate': generation_id(candidate)}


def record_attempt(attempt, parent, candidate, *, cancel):
    """Called under generation.lock. Reserve an ID once, or durably cancel it.

    A rollback may arrive before the original swap acquires the lock. Its
    cancellation must survive that delayed process (and a process restart).
    Pending and cancelled IDs are never reusable, even for the same pair.
    """
    directory = Path(H) / 'generation-attempts'
    directory.mkdir(mode=0o700, exist_ok=True)
    sync_directory(H)
    path = directory / (generation_id(attempt) + '.json')
    identity = {'parent': generation_id(parent), 'candidate': generation_id(candidate)}
    if path.exists():
        prior = json.loads(path.read_text())
        if (not isinstance(prior, dict) or type(prior.get('cancelled')) is not bool
                or prior not in (dict(identity, cancelled=False), dict(identity, cancelled=True))):
            raise RuntimeError('publication attempt belongs to a different pair or is invalid')
        if not cancel:
            if prior['cancelled']:
                return False
            raise RuntimeError('publication attempt already started')
    write_durable_json(path, dict(identity, cancelled=cancel))
    return True


def claim_transition(attempt, parent, candidate):
    # Only called after the exact-parent precondition, under generation.lock.
    # The last forward attempt owns recovery until another validated attempt
    # takes ownership. A stale rollback cannot undo a successor's publication.
    write_durable_json(Path(H) / 'generation-transition.json',
                       attempt_identity(attempt, parent, candidate))


def owns_transition(attempt, parent, candidate):
    path = Path(H) / 'generation-transition.json'
    return path.exists() and json.loads(path.read_text()) == attempt_identity(attempt, parent, candidate)


def rollback(attempt, candidate, parent):
    try:
        record_attempt(attempt, parent, candidate, cancel=True)
    except FAILURES as error:
        print('UNRESOLVED DIVERGENCE: attempt cancellation not durable:', error, file=sys.stderr)
        return 8
    try:
        bodies = {candidate: staged(candidate), parent: staged(parent)}
        if registry(bodies[candidate]).get('parent_generation') != parent:
            raise RuntimeError('candidate was not built from expected parent')
        if not owns_transition(attempt, parent, candidate):
            # Cancellation still fences a delayed original swap. Without
            # ownership, however, only readback of an already-restored parent
            # is allowed; never move a successor's pointers.
            snapshot({parent: bodies[parent]})
            print('verified both parent pointers without transition ownership', parent)
            return 0
        move(candidate, parent, bodies, snapshot(bodies))
    except FAILURES as error:
        # An error response does not prove the last write failed. Only a fresh
        # readback of both exact parent pointers can establish a safe outcome.
        print('rollback command failed:', error, file=sys.stderr)
        try:
            verify(parent)
        except FAILURES as read_error:
            print('UNRESOLVED DIVERGENCE: rollback not verified:', read_error, file=sys.stderr)
            return 8
    return 0


def swap(attempt, parent, candidate):
    try:
        if not record_attempt(attempt, parent, candidate, cancel=False):
            print('REFUSED: publication attempt already cancelled', file=sys.stderr)
            return 3
    except FAILURES as error:
        print('UNRESOLVED DIVERGENCE: attempt reservation failed:', error, file=sys.stderr)
        return 8
    try:
        bodies = {parent: staged(parent), candidate: staged(candidate)}
        if registry(bodies[candidate]).get('parent_generation') != parent:
            raise RuntimeError('candidate was not built from expected parent')
        initial = snapshot({parent: bodies[parent]})
    except FAILURES as error:
        print('REFUSED: swap precondition failed:', error, file=sys.stderr)
        return 3
    try:
        claim_transition(attempt, parent, candidate)
        move(parent, candidate, bodies, initial)
    except FAILURES as error:
        print('swap failed:', error, file=sys.stderr)
        return 8 if rollback(attempt, candidate, parent) else 4
    return 0


def stage(directory):
    d = os.path.join(H, directory)
    reg = json.loads(gzip.decompress(open(d + '/registry.json.gz', 'rb').read()))
    gen = generation_id(reg['generation'])
    ups = list(csv.DictReader(open(d + '/uploads.tsv'), delimiter='\t'))
    for u in ups:
        path = os.path.join(d, u['local_path'])
        data = open(path, 'rb').read()
        assert hashlib.sha256(data).hexdigest() == u['sha256'] and len(data) == int(u['size_bytes']), u['key']
        h = head(u['key'])
        if h:
            assert h['ContentLength'] == len(data) and h['ETag'].strip('"') == u['etag'], 'existing object differs: ' + u['key']
            continue
        out = json.loads(s3('put-object', '--bucket', B, '--key', u['key'], '--body', path, '--content-type', u['content_type'],
                            '--if-none-match', '*').stdout)
        assert out['ETag'].strip('"') == u['etag'], ('etag mismatch', u['key'])
    holds = list(csv.DictReader(open(d + '/holds.tsv'), delimiter='\t'))
    keys = {h['object_key'] for h in holds}
    have = {l.split('\t')[0] for l in pg("SELECT object_key FROM delivery_keep_holds WHERE object_key IN (%s)"
                                           % ','.join("'%s'" % k for k in keys)).splitlines()}
    q = lambda s: "'" + s.replace("'", "''") + "'"
    vals = ['(%s,%s,%s,%s,%s,%s,%s,%s)' % (q(h['bucket']), q(h['object_key']), q(h['etag']), int(h['size_bytes']),
            ('NULL' if h['recording_id'] in ('', '\\N') else h['recording_id']), q(h['kind']), q(h['sha256']), q(h['relative_path'])) for h in holds if h['object_key'] not in have]
    # The per-row admission guard is slow (~2 s/row); commit in small chunks so progress survives the timeout.
    for i in range(0, len(vals), 50):
        for attempt in range(60):  # the R2 lane holds r2_inverse_ke_epoch during a cycle; the fence uses NOWAIT
            try:
                pg('BEGIN; INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,recording_id,kind,sha256,relative_path) VALUES '
                   + ','.join(vals[i:i + 50]) + '; COMMIT;', write=True)
                break
            except SystemExit as e:
                if 'could not obtain lock' not in str(e) or attempt == 59:
                    raise
                time.sleep(30)
        print('holds committed', min(i + 50, len(vals)), 'of', len(vals), flush=True)
    held = {}
    for line in pg("SELECT object_key,etag,size_bytes FROM delivery_keep_holds WHERE bucket='stoarama'").splitlines():
        k, e, s = line.split('\t'); held[k] = (e.strip('"'), int(s))
    objs = list(reg['files'].values()) + list(reg['assets'].values())
    bad = [o['key'] for o in objs if held.get(o['key']) != (o['etag'].strip('"'), o['size_bytes'])]
    assert not bad, ('unheld objects', len(bad), bad[:3])
    gk = P + '/generations/%s.json' % gen
    if not head(gk):
        s3('put-object', '--bucket', B, '--key', gk, '--body', d + '/registry.json.gz', '--content-type', 'application/gzip', '--if-none-match', '*')
    assert staged(gen) == Path(d + '/registry.json.gz').read_bytes(), 'staged registry differs'
    print(json.dumps({'generation': gen, 'uploaded_or_present': len(ups), 'holds_added': len(vals), 'objects_held': len(objs)}))


def main(args):
    global E, B, EP
    if len(args) == 2 and args[0] in ('stage', 'verify'):
        step, arg = args
        if step == 'verify':
            generation_id(arg)
    elif len(args) == 4 and args[0] in ('swap', 'rollback'):
        step, attempt, arg, target = args
        generation_id(attempt)
        generation_id(arg)
        generation_id(target)
        if arg == target:
            raise ValueError('source and target generations must differ')
    else:
        raise SystemExit('usage: gss_gen.py stage DIR | verify ID | swap ATTEMPT PARENT CANDIDATE | rollback ATTEMPT CANDIDATE PARENT')
    E = r2env()
    B = E['R2_BUCKET']
    EP = 'https://%s.r2.cloudflarestorage.com' % E['R2_ACCOUNT_ID']
    # All supported Jelly operations share a lock; it is released by the OS
    # even if the process dies. Never unlink the lock file.
    with open(os.path.join(H, 'generation.lock'), 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if step == 'stage':
            stage(arg)
        elif step == 'verify':
            verify(arg)
        elif step == 'swap':
            return swap(attempt, arg, target)
        else:
            return rollback(attempt, arg, target)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
