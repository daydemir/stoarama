#!/usr/bin/env python3
"""Finite, checkpointed best14 staging executor. Never publishes or deletes sources.

Run from the owning scheduler with scoped DO/R2 credentials on the control host.
Guests receive expiring per-object capabilities only. All mutations have durable
intents, and uncertain submissions are reconciled before any repeat dispatch.
"""
import argparse
import base64
import concurrent.futures
import datetime
import fcntl
import gzip
import hashlib
import hmac
import json
import os
import pathlib
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request

TAG = 'stoarama-best14-20261021'
SELECTION = '4227ac46d1adee99a59b706289bed6ddca82919d2931fdae0067cb0020a7f165'
GIB = 1024 ** 3


class GuestStatus(RuntimeError):
    def __init__(self, status):
        self.status = status
        super().__init__('guest_status_' + str(status))


def write(path, data):
    temporary = path.with_suffix(path.suffix + '.pending')
    with temporary.open('w') as stream:
        os.chmod(temporary, 0o600)
        json.dump(data, stream, separators=(',', ':'))
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def provider(method, path, data=None):
    request = urllib.request.Request('https://api.digitalocean.com/v2/microvms' + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers={'Authorization': 'Bearer ' + os.environ['DIGITALOCEAN_TOKEN'],
                 'Content-Type': 'application/json'}, method=method)
    try:
        with urllib.request.urlopen(request, timeout=50) as response:
            raw = response.read(16 * 1024 ** 2)
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        raise RuntimeError('provider_status_' + str(error.code)) from None
    except (OSError, TimeoutError):
        raise RuntimeError('provider_transport_uncertain') from None


def guest(worker, route, payload=None):
    packed = base64.b64encode(gzip.compress(json.dumps(payload).encode())).decode()
    script = ("import urllib.request,urllib.error,json,base64,gzip;"
              "data=gzip.decompress(base64.b64decode(" + repr(packed) + "));"
              "req=urllib.request.Request('http://127.0.0.1:8080" + route + "',"
              "data=" + ('data' if payload is not None else 'None') + ","
              "headers={'Content-Type':'application/json'},method=" + repr('POST' if payload is not None else 'GET') + ");"
              "\ntry:\n r=urllib.request.urlopen(req,timeout=15);print(r.read().decode())"
              "\nexcept urllib.error.HTTPError as e:\n print(json.dumps({'_http_status':e.code}))")
    if len(script) > 110000:
        raise ValueError('capability request exceeds guest argument bound')
    result = provider('POST', '/' + worker['id'] + '/exec', {'argv': ['python3', '-c', script]})
    if result.get('exit_code') != 0:
        raise RuntimeError('guest_request_failed')
    decoded = json.loads(result['stdout'])
    if '_http_status' in decoded:
        raise GuestStatus(decoded['_http_status'])
    return decoded


def presign(method, bucket, key, headers=None):
    headers = headers or {}
    host = os.environ['R2_ACCOUNT_ID'] + '.r2.cloudflarestorage.com'
    uri = '/' + urllib.parse.quote(bucket, safe='') + '/' + urllib.parse.quote(key, safe='/~')
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    day = stamp[:8]
    scope = day + '/auto/s3/aws4_request'
    canonical_headers = {k.lower(): str(v).strip() for k, v in headers.items()}
    canonical_headers['host'] = host
    signed = ';'.join(sorted(canonical_headers))
    values = {'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
              'X-Amz-Credential': os.environ['R2_ACCESS_KEY_ID'] + '/' + scope,
              'X-Amz-Date': stamp, 'X-Amz-Expires': '3600', 'X-Amz-SignedHeaders': signed}
    query = '&'.join(urllib.parse.quote(k, safe='') + '=' + urllib.parse.quote(v, safe='~') for k, v in sorted(values.items()))
    canonical = method + '\n' + uri + '\n' + query + '\n' + ''.join(k + ':' + canonical_headers[k] + '\n' for k in sorted(canonical_headers)) + '\n' + signed + '\nUNSIGNED-PAYLOAD'
    signing = ('AWS4' + os.environ['R2_SECRET_ACCESS_KEY']).encode()
    for part in (day, 'auto', 's3', 'aws4_request'):
        signing = hmac.new(signing, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(signing, ('AWS4-HMAC-SHA256\n' + stamp + '\n' + scope + '\n' + hashlib.sha256(canonical.encode()).hexdigest()).encode(), hashlib.sha256).hexdigest()
    return {'url': 'https://' + host + uri + '?' + query + '&X-Amz-Signature=' + signature, 'headers': headers}


def head(clip):
    cap = presign('HEAD', clip['bucket'], clip['object_key'])
    try:
        with urllib.request.urlopen(urllib.request.Request(cap['url'], method='HEAD'), timeout=15) as response:
            if int(response.headers.get('Content-Length', '-1')) != clip['size_bytes']:
                return None
            etag = response.headers.get('ETag')
            return etag if etag else None
    except (OSError, TimeoutError):
        return None


def read_work(row):
    with gzip.open(row['path'], 'rb') as stream:
        raw = stream.read(4 * 1024 ** 2 + 1)
    work = json.loads(raw)
    canonical = json.dumps(work, sort_keys=True, separators=(',', ':')).encode()
    if hashlib.sha256(canonical).hexdigest() != row['id'] or work['batch_id'] != 'best14-20261021':
        raise ValueError('frozen_work_identity_differs')
    return work


def budget(root, workers, db):
    # Deliberately overestimate compute with full elapsed wall time, including
    # paused time; charge provisioned disk rather than smaller actual used disk.
    now = time.time()
    # Provider emits nanoseconds; Python3.9 accepts only microseconds. Rounding
    # UTC creation down to whole seconds also preserves the conservative bound.
    def created(worker):
        value = worker['created_at']
        if not value.endswith('Z'):
            raise ValueError('provider_creation_time_not_utc')
        return datetime.datetime.strptime(value[:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=datetime.timezone.utc).timestamp()
    machine = sum(max(0, now - created(w)) / 3600 * (0.078 + w['size']['disk'] * 0.05 / 672) for w in workers)
    transfer = 0.0
    for row in db.execute('SELECT j.state,j.source_bytes,j.result,coalesce(e.count,0) AS exports FROM jobs j LEFT JOIN export_attempts e ON e.id=j.id WHERE j.attempt>0'):
        if row['state'] == 'staged_verified':
            result = json.loads(row['result'])
            transfer += sum(o['bytes'] for o in result['export_receipts']) / GIB * 0.01 * row['exports']
        elif row['state'] not in ('held', 'held_structure'):
            # Three admitted exports, each capped at 2x source plus 128MiB.
            transfer += (6 * row['source_bytes'] + 384 * 1024 ** 2) / GIB * 0.01
    result = {'observed_at': now, 'additional_limit_usd': 100,
              'compute_and_disk_upper_bound_usd': machine,
              'reserved_egress_usd': transfer, 'upper_bound_usd': machine + transfer,
              'drain_reserve_usd': 5, 'public_changed': False}
    write(root / 'budget-ledger.json', result)
    return result


def reconcile(root, db, worker, row):
    try:
        state = guest(worker, '/job?id=' + row['id'])
    except GuestStatus as error:
        if error.status != 404:
            raise
        # The exact owner has authoritatively reported this hash absent. Retry
        # the same job/attempt with fresh capabilities, bounded by a separate
        # submission counter; never create a second media attempt on uncertainty.
        ready = guest(worker, '/ready')
        if ready['active_jobs'] == 0 and ready['build_complete']:
            submit(db, worker, row, read_work(row))
        return
    if state.get('job_id') != row['id']:
        raise ValueError('remote_job_identity_differs')
    remote = state['state']
    if remote in ('accepted', 'running', 'exporting'):
        return
    if remote == 'failed':
        # Deterministic failures remain held for qualification/reselection.
        db.execute('UPDATE jobs SET state=?,finished_at=?,result=? WHERE id=?', ('held', time.time(), json.dumps(state), row['id']))
        db.commit()
        return
    result = state.get('result', {})
    if result.get('ready_to_publish') is not False:
        raise ValueError('remote_staging_flag_differs')
    manifest = result['manifest']
    work = read_work(row)
    if manifest['hour_id'] != work['hour_id']:
        raise ValueError('remote_hour_identity_differs')
    if remote in ('staged', 'export_failed'):
        if not manifest.get('outputs'):
            db.execute('UPDATE jobs SET state=?,finished_at=?,result=? WHERE id=?', ('held', time.time(), json.dumps(state), row['id']))
            db.commit()
            return
        caps = {}
        if sum(o['size_bytes'] for o in manifest['outputs']) > 2 * row['source_bytes'] + 128 * 1024 ** 2:
            raise ValueError('remote_output_exceeds_reserved_transfer')
        for output in manifest['outputs']:
            key = output['object_key']
            if not key.startswith('joined/best14-20261021/objects/') or key != 'joined/best14-20261021/objects/' + output['sha256'] + '.mp4':
                raise ValueError('remote_output_identity_differs')
            proof = result['full_decode_proofs'][key]
            if proof['strict_full_decode'] is not True or (proof['sha256'], proof['bytes']) != (output['sha256'], output['size_bytes']):
                raise ValueError('remote_decode_proof_differs')
            bucket = os.environ['R2_BUCKET']
            caps[key] = {'bucket': bucket,
                'put': presign('PUT', bucket, key, {'If-None-Match': '*', 'Content-Length': str(output['size_bytes']), 'Content-Type': 'video/mp4'}),
                'get': presign('GET', bucket, key)}
        # Bounded retry of export. Job artifacts remain intact on any failure.
        attempts = db.execute('SELECT count FROM export_attempts WHERE id=?', (row['id'],)).fetchone()
        count = attempts[0] if attempts else 0
        if count >= 3:
            db.execute('UPDATE jobs SET state=? WHERE id=?', ('held_export', row['id']))
            db.commit()
            return
        db.execute('INSERT OR REPLACE INTO export_attempts VALUES(?,?)', (row['id'], count + 1))
        db.commit()
        guest(worker, '/export', {'job_id': row['id'], 'capabilities': caps})
        return
    if remote in ('exported', 'released'):
        receipts = state['export_receipts']
        expected = {(o['object_key'], o['size_bytes'], o['sha256']) for o in manifest['outputs']}
        actual = {(o['object_key'], o['bytes'], o['sha256']) for o in receipts if o.get('full_readback_verified') is True}
        if actual != expected or len(receipts) != len(expected):
            raise ValueError('remote_readback_accounting_differs')
        # Durable local receipt precedes any guest scratch release.
        write(root / 'results' / (row['id'] + '.json'), state)
        if remote != 'released':
            guest(worker, '/release', {'job_id': row['id']})
        db.execute('UPDATE jobs SET state=?,finished_at=?,result=? WHERE id=?', ('staged_verified', time.time(), json.dumps(state), row['id']))
        db.commit()


def submit(db, worker, row, work):
    prior = db.execute('SELECT count FROM submission_attempts WHERE id=?', (row['id'],)).fetchone()
    count = prior[0] if prior else 0
    if count >= 3:
        db.execute('UPDATE jobs SET state=?,result=? WHERE id=?', ('held_submission', 'confirmed_absent_after_bounded_submissions', row['id']))
        db.commit()
        return False
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        etags = list(pool.map(head, work['clips']))
    if not all(etags):
        db.execute('UPDATE jobs SET result=? WHERE id=?', ('source_head_wait', row['id']))
        db.execute('INSERT OR REPLACE INTO deferred_jobs VALUES(?,?)', (row['id'], time.time() + 300))
        db.commit()
        return False
    ready = guest(worker, '/ready')
    if ready['active_jobs'] != 0 or not ready['build_complete']:
        return False
    sources = {str(c['clip_id']): presign('GET', c['bucket'], c['object_key'], {'If-Match': etag}) for c, etag in zip(work['clips'], etags)}
    payload = {'job_id': row['id'], 'attempt': 1, 'request': {'work': work, 'sources': sources, 'timeout_seconds': 1800}}
    db.execute('INSERT OR REPLACE INTO submission_attempts VALUES(?,?)', (row['id'], count + 1))
    db.execute('UPDATE jobs SET state=?,worker_id=?,attempt=1,started_at=? WHERE id=?', ('submitted', worker['id'], time.time(), row['id']))
    db.commit()
    accepted = guest(worker, '/job', payload)
    if accepted.get('job_id') != row['id']:
        raise ValueError('job_acceptance_identity_differs')
    return True


def dispatch(root, db, worker, ledger):
    # Wait for every source to pass cheap structure screening. NAS restores will
    # be screened after R2 upload; until then they remain queued, never omitted.
    screen = sqlite3.connect('file:' + str(root / 'structure-screen.sqlite3') + '?mode=ro', uri=True)
    rows = db.execute("SELECT * FROM jobs WHERE state='queued' AND attempt=0 AND source_bytes>0 AND source_bytes<=? AND source_files<=240 AND id NOT IN (SELECT id FROM deferred_jobs WHERE until>?) ORDER BY nas_sources,recording_id,local_date,hour", (8 * GIB, time.time())).fetchall()
    for row in rows:
        work = read_work(row)
        statuses = [screen.execute('SELECT sha256,status FROM screened WHERE clip_id=?', (c['clip_id'],)).fetchone() for c in work['clips']]
        if any(s and s[1] == 'structural_failure' for s in statuses):
            db.execute('UPDATE jobs SET state=?,result=? WHERE id=?', ('held_structure', 'source_structure_failure_requires_qualification', row['id']))
            db.commit()
            continue
        if not all(s and s[0] == c['sha256'] and s[1] == 'structure_present' for s, c in zip(statuses, work['clips'])):
            continue
        reservation = (6 * row['source_bytes'] + 384 * 1024 ** 2) / GIB * 0.01
        if ledger['upper_bound_usd'] + reservation + 5 >= 95:
            return
        if not submit(db, worker, row, work):
            continue
        screen.close()
        return
    screen.close()


def must_pause(root, db, ledger):
    if ledger['upper_bound_usd'] < 90:
        return False
    draining = root / 'budget-draining.json'
    if not draining.exists():
        write(draining, {'started_at': time.time()})
    hard_stop = (ledger['upper_bound_usd'] >= 94 or
                 time.time() - json.loads(draining.read_text())['started_at'] >= 3600)
    return hard_stop or not db.execute("SELECT 1 FROM jobs WHERE state='submitted' LIMIT 1").fetchone()


def pause_workers(root, workers, ledger):
    outcomes = {}
    for worker in workers:
        try:
            provider('POST', '/' + worker['id'] + '/pause', {})
            outcomes[worker['id']] = 'pause_accepted'
        except Exception:
            outcomes[worker['id']] = 'pause_unconfirmed_retry_pending'
    write(root / 'budget-stopped.json', ledger)
    write(root / 'budget-pause-outcomes.json', outcomes)
    if all(value == 'pause_accepted' for value in outcomes.values()):
        write(root / 'STOP', {'reason': 'budget_admission_closed', 'submitted_intents_preserved': True})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-root', required=True, type=pathlib.Path)
    args = parser.parse_args()
    root = args.run_root.resolve()
    lock = (root / 'executor.lock').open('a+')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return
    authority = json.loads((root / 'run-authority.json').read_text())
    if authority['additional_budget_usd'] != 100:
        raise ValueError('additional_budget_authority_differs')
    if json.loads((root / 'work-queue-summary.json').read_text())['selection_sha256'] != SELECTION:
        raise ValueError('frozen_selection_differs')
    if (root / 'STOP').exists():
        return
    if os.statvfs(root).f_bavail * os.statvfs(root).f_frsize < 5 * GIB + 32 * 1024 ** 2:
        raise RuntimeError('control_checkpoint_storage_floor')
    workers = json.loads((root / 'execution-workers.json').read_text())
    # Provisioning/scaling is a separate admitted operation; this executor can
    # only use the explicitly enrolled, source-pinned workers.
    if not 1 <= len(workers) <= 4 or len({w['id'] for w in workers}) != len(workers):
        raise ValueError('worker_scope_differs')
    (root / 'results').mkdir(mode=0o700, exist_ok=True)
    db = sqlite3.connect(root / 'execution.sqlite3')
    db.row_factory = sqlite3.Row
    db.execute('CREATE TABLE IF NOT EXISTS export_attempts(id TEXT PRIMARY KEY,count INTEGER)')
    db.execute('CREATE TABLE IF NOT EXISTS submission_attempts(id TEXT PRIMARY KEY,count INTEGER)')
    db.execute('CREATE TABLE IF NOT EXISTS deferred_jobs(id TEXT PRIMARY KEY,until REAL)')
    started = time.monotonic()
    while time.monotonic() - started < 42:
        ledger = budget(root, workers, db)
        # Check the hard bound before guest queries can resume paused compute.
        if must_pause(root, db, ledger):
            pause_workers(root, workers, ledger)
            break
        for worker in workers:
            try:
                active = db.execute("SELECT * FROM jobs WHERE worker_id=? AND state='submitted'", (worker['id'],)).fetchall()
                if len(active) > 1:
                    raise ValueError('multiple_jobs_per_worker')
                if active:
                    reconcile(root, db, worker, active[0])
                elif ledger['upper_bound_usd'] < 90:
                    # Check local eligibility before touching the guest; an
                    # idle worker stays paused while sources are unavailable.
                    dispatch(root, db, worker, ledger)
            except Exception as error:
                # Persist only classified local messages, never remote URLs.
                write(root / ('executor-error-' + worker['id'] + '.json'), {'observed_at': time.time(), 'class': type(error).__name__, 'detail': str(error) if isinstance(error, (ValueError, RuntimeError)) else 'transport_or_io_failure'})
        counts = dict(db.execute('SELECT state,count(*) FROM jobs GROUP BY state'))
        write(root / 'execution-status.json', {'observed_at': time.time(), 'counts': counts, 'workers': len(workers), 'new_publication_ready': False, 'public_changed': False, 'no_source_deletion': True})
        time.sleep(5)
    db.close()


if __name__ == '__main__':
    main()
