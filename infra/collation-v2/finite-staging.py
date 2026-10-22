#!/usr/bin/env python3
"""Finite, checkpointed best14 staging executor. Never publishes or deletes sources.

Run from the owning scheduler with scoped DO/R2 credentials on the control host.
Guests receive expiring per-object capabilities only. All mutations have durable
intents, and uncertain submissions are reconciled before any repeat dispatch.
Run-root provisioning must create executor.lock before enrolling cloud workers.
"""
import argparse
import base64
import concurrent.futures
import datetime
import errno
import fcntl
import gzip
import hashlib
import hmac
import json
import math
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


class WorkerBusy(RuntimeError):
    pass


class ControlStorageError(RuntimeError):
    pass


def is_control_storage_error(error):
    if isinstance(error, ControlStorageError):
        return True
    if isinstance(error, OSError):
        return error.errno in (errno.ENOSPC, errno.EDQUOT, errno.EIO, errno.EROFS, errno.ENODEV)
    if isinstance(error, sqlite3.OperationalError):
        code = getattr(error, 'sqlite_errorcode', 0) & 255
        # Python 3.9 lacks sqlite_errorcode, so retain its stable error messages.
        return code in (8, 10, 13, 14) or str(error).lower() in (
            'database or disk is full', 'disk i/o error',
            'attempt to write a readonly database', 'unable to open database file')
    return False


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
    # R2 bills daily peak bytes. Charge the entire NAS restore cohort from run
    # start, plus each verified staged output, rounding age up conservatively.
    storage = 428364261889 / 1e9 * 0.015 / 30 * (math.ceil(max(0, now - min(created(w) for w in workers)) / 86400) + 1)
    for row in db.execute('SELECT j.state,j.source_bytes,j.result,j.finished_at,coalesce(e.count,0) AS exports FROM jobs j LEFT JOIN export_attempts e ON e.id=j.id WHERE j.attempt>0'):
        if row['state'] == 'staged_verified':
            result = json.loads(row['result'])
            exported = result.get('export_bytes')
            if exported is None:
                exported = sum(o['bytes'] for o in result['export_receipts'])
            transfer += exported / GIB * 0.01 * row['exports']
            storage += exported / 1e9 * 0.015 / 30 * (math.ceil(max(0, now - row['finished_at']) / 86400) + 1)
        elif row['state'] not in ('held', 'held_structure'):
            # Three admitted exports, each capped at 2x source plus 128MiB.
            transfer += (6 * row['source_bytes'] + 384 * 1024 ** 2) / GIB * 0.01
            if row['exports']:
                # A timed-out export can leave complete objects in R2. Retain
                # their full output bound even when verification stays held.
                storage += (2 * row['source_bytes'] + 128 * 1024 ** 2) / 1e9 * 0.015 / 30 * (math.ceil(max(0, now - min(created(w) for w in workers)) / 86400) + 1)
    head_reads = db.execute('SELECT coalesce(sum(count),0) FROM head_attempts').fetchone()[0]
    screen_status = root / 'structure-screen-status.json'
    range_reads = json.loads(screen_status.read_text()).get('range_requests', 0) if screen_status.exists() else 0
    operations = 7 + (head_reads + range_reads) / 1e6 * 0.36
    result = {'observed_at': now, 'additional_limit_usd': 100,
              'compute_and_disk_upper_bound_usd': machine,
              'reserved_egress_usd': transfer, 'r2_storage_upper_bound_usd': storage,
              'operations_reserve_usd': operations,
              'upper_bound_usd': machine + transfer + storage + operations,
              'drain_reserve_usd': 5, 'public_changed': False}
    write(root / 'budget-ledger.json', result)
    return result


def archive_receipt(state):
    # Full manifests belong beside staged media. Keep only a small receipt on
    # the control host, whose storage floor must also survive the entire run.
    value = {key: state[key] for key in ('job_id', 'attempt', 'result', 'export_receipts')}
    body = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    sha = hashlib.sha256(body).hexdigest()
    bucket = os.environ['R2_BUCKET']
    key = 'staging/best14-20261021/receipts/' + state['job_id'] + '/' + sha + '.json'
    cap = presign('PUT', bucket, key, {'If-None-Match': '*', 'Content-Length': str(len(body)), 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(urllib.request.Request(cap['url'], data=body, headers=cap['headers'], method='PUT'), timeout=20) as response:
            if response.status not in (200, 201):
                raise ValueError('receipt_upload_status_differs')
    except urllib.error.HTTPError as error:
        if error.code != 412:
            raise RuntimeError('receipt_upload_rejected') from None
    except (OSError, TimeoutError):
        pass
    cap = presign('GET', bucket, key)
    try:
        with urllib.request.urlopen(urllib.request.Request(cap['url'], method='GET'), timeout=20) as response:
            downloaded = response.read(len(body) + 1)
            if response.status != 200 or len(downloaded) != len(body) or hashlib.sha256(downloaded).hexdigest() != sha:
                raise ValueError('receipt_readback_identity_differs')
    except (OSError, TimeoutError):
        raise RuntimeError('receipt_readback_unconfirmed') from None
    return {'job_id': state['job_id'], 'attempt': state['attempt'],
            'export_receipts_sha256': hashlib.sha256(json.dumps(state['export_receipts'], sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            'receipt_bucket': bucket, 'receipt_key': key,
            'receipt_sha256': sha, 'receipt_bytes': len(body),
            'export_bytes': sum(o['bytes'] for o in state['export_receipts']),
            'output_count': len(state['export_receipts']), 'ready_to_publish': False}


def reconcile(root, db, worker, row):
    deferred = db.execute('SELECT until FROM deferred_jobs WHERE id=?', (row['id'],)).fetchone()
    if deferred and deferred[0] > time.time():
        return
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
            submit(db, worker, row, read_work(row), media_attempt=row['attempt'])
        return
    if state.get('job_id') != row['id']:
        raise ValueError('remote_job_identity_differs')
    if state.get('attempt') != row['attempt']:
        # The local intent precedes dispatch. A lost retry POST can leave the
        # previous failed attempt on the guest. Re-send the intended attempt;
        # the worker serializes admission and returns any existing acceptance.
        if (state.get('attempt') == row['attempt'] - 1 and state.get('state') == 'failed' and
                state.get('failure') == 'worker_restarted'):
            submit(db, worker, row, read_work(row), media_attempt=row['attempt'])
            return
        raise ValueError('remote_media_attempt_differs')
    remote = state['state']
    if remote in ('accepted', 'running', 'exporting'):
        return
    if remote == 'failed':
        if state.get('failure') == 'worker_restarted' and row['attempt'] < 3:
            submit(db, worker, row, read_work(row), media_attempt=row['attempt'] + 1)
            return
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
        archive_id = row['id'] + ':attempt-' + str(row['attempt'])
        archived = db.execute('SELECT receipt FROM archive_receipts WHERE id=?', (archive_id,)).fetchone()
        receipt = json.loads(archived[0]) if archived else None
        export_hash = hashlib.sha256(json.dumps(receipts, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        if not (receipt and receipt.get('job_id') == row['id'] and receipt.get('attempt') == row['attempt'] and
                receipt.get('export_receipts_sha256') == export_hash):
            prior = db.execute('SELECT count FROM archive_attempts WHERE id=?', (row['id'],)).fetchone()
            count = prior[0] if prior else 0
            if count >= 3:
                db.execute('UPDATE jobs SET state=? WHERE id=?', ('held_receipt', row['id']))
                db.commit()
                return
            db.execute('INSERT OR REPLACE INTO archive_attempts VALUES(?,?)', (row['id'], count + 1))
            db.commit()
            receipt = archive_receipt(state)
            db.execute('INSERT OR REPLACE INTO archive_receipts VALUES(?,?)', (archive_id, json.dumps(receipt)))
            db.commit()
        write(root / 'results' / (row['id'] + '.json'), receipt)
        if remote != 'released':
            guest(worker, '/release', {'job_id': row['id']})
        db.execute('UPDATE jobs SET state=?,finished_at=?,result=? WHERE id=?', ('staged_verified', time.time(), json.dumps(receipt), row['id']))
        db.commit()


def submit(db, worker, row, work, media_attempt=1):
    prior = db.execute('SELECT count FROM submission_attempts WHERE id=?', (row['id'],)).fetchone()
    count = prior[0] if prior else 0
    if count >= 3:
        db.execute('UPDATE jobs SET state=?,result=? WHERE id=?', ('held_submission', 'confirmed_absent_after_bounded_submissions', row['id']))
        db.commit()
        return False
    ready = guest(worker, '/ready')
    if ready['active_jobs'] != 0 or not ready['build_complete']:
        # Stop this worker's scan rather than probing every waiting candidate.
        raise WorkerBusy('worker_not_idle')
    db.execute('INSERT INTO head_attempts(count) VALUES(?)', (len(work['clips']),))
    db.commit()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        etags = list(pool.map(head, work['clips']))
    if not all(etags):
        db.execute('UPDATE jobs SET result=? WHERE id=?', ('source_head_wait', row['id']))
        db.execute('INSERT OR REPLACE INTO deferred_jobs VALUES(?,?)', (row['id'], time.time() + 300))
        db.commit()
        return False
    sources = {str(c['clip_id']): presign('GET', c['bucket'], c['object_key'], {'If-Match': etag}) for c, etag in zip(work['clips'], etags)}
    payload = {'job_id': row['id'], 'attempt': media_attempt, 'request': {'work': work, 'sources': sources, 'timeout_seconds': 1800}}
    db.execute('INSERT OR REPLACE INTO submission_attempts VALUES(?,?)', (row['id'], count + 1))
    db.execute('UPDATE jobs SET state=?,worker_id=?,attempt=?,started_at=? WHERE id=?', ('submitted', worker['id'], media_attempt, time.time(), row['id']))
    db.commit()
    try:
        accepted = guest(worker, '/job', payload)
    except GuestStatus as error:
        if error.status not in (409, 503, 507):
            raise
        # These handler responses precede admission. Refund the transport
        # attempt and defer the job; the exact same attempt remains reusable.
        db.execute('INSERT OR REPLACE INTO submission_attempts VALUES(?,?)', (row['id'], count))
        db.execute('UPDATE jobs SET state=?,attempt=?,result=? WHERE id=?', ('queued' if row['attempt'] == 0 else 'submitted', row['attempt'], 'worker_capacity_wait', row['id']))
        db.execute('INSERT OR REPLACE INTO deferred_jobs VALUES(?,?)', (row['id'], time.time() + 300))
        db.commit()
        return False
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
        statuses = [(s[0].hex() if isinstance(s[0], bytes) else s[0],
                     {1: 'structure_present', 2: 'structural_failure', 3: 'hold'}.get(s[1], s[1])) if s else None for s in statuses]
        if any(s and s[0] == c['sha256'] and s[1] == 'structural_failure' for s, c in zip(statuses, work['clips'])):
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


def pause_for_control_storage(root, workers):
    outcomes = {}
    for worker in workers:
        try:
            provider('POST', '/' + worker['id'] + '/pause', {})
            outcomes[worker['id']] = 'pause_accepted'
        except Exception:
            outcomes[worker['id']] = 'pause_unconfirmed_retry_pending'
    # Pausing compute must not depend on being able to write a checkpoint.
    try:
        write(root / 'storage-pause-outcomes.json', {'observed_at': time.time(), 'outcomes': outcomes,
              'reason': 'control_checkpoint_storage_floor', 'submitted_intents_preserved': True})
    except OSError:
        pass


def require_control_capacity(root, workers):
    try:
        space = os.statvfs(root)
    except OSError as error:
        raise ControlStorageError('control_checkpoint_storage_unavailable') from error
    if space.f_bavail * space.f_frsize >= 5 * GIB + 8 * 1024 ** 2:
        return
    raise ControlStorageError('control_checkpoint_storage_floor_pause_attempted')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-root', required=True, type=pathlib.Path)
    args = parser.parse_args()
    root = args.run_root.resolve()
    # The provisioning owner precreates this file before any VM can run.
    # Opening read-only never needs to allocate a new inode on a full disk.
    with (root / 'executor.lock').open('r') as lock:
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
        workers = json.loads((root / 'execution-workers.json').read_text())
        # Provisioning/scaling is a separate admitted operation; this executor can
        # only use the explicitly enrolled, source-pinned workers.
        if not 1 <= len(workers) <= 4 or len({w['id'] for w in workers}) != len(workers):
            raise ValueError('worker_scope_differs')
        try:
            execute(root, workers)
        except (ControlStorageError, OSError, sqlite3.OperationalError) as error:
            if is_control_storage_error(error):
                pause_for_control_storage(root, workers)
            raise


def execute(root, workers):
    require_control_capacity(root, workers)
    (root / 'results').mkdir(mode=0o700, exist_ok=True)
    db = sqlite3.connect(root / 'execution.sqlite3')
    try:
        db.row_factory = sqlite3.Row
        db.execute('CREATE TABLE IF NOT EXISTS export_attempts(id TEXT PRIMARY KEY,count INTEGER)')
        db.execute('CREATE TABLE IF NOT EXISTS submission_attempts(id TEXT PRIMARY KEY,count INTEGER)')
        db.execute('CREATE TABLE IF NOT EXISTS deferred_jobs(id TEXT PRIMARY KEY,until REAL)')
        db.execute('CREATE TABLE IF NOT EXISTS archive_attempts(id TEXT PRIMARY KEY,count INTEGER)')
        db.execute('CREATE TABLE IF NOT EXISTS archive_receipts(id TEXT PRIMARY KEY,receipt TEXT)')
        db.execute('CREATE TABLE IF NOT EXISTS head_attempts(count INTEGER)')
        started = time.monotonic()
        while time.monotonic() - started < 42:
            require_control_capacity(root, workers)
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
                    if is_control_storage_error(error):
                        raise  # The outer storage handler pauses every enrolled VM.
                    # Persist only classified local messages, never remote URLs.
                    write(root / ('executor-error-' + worker['id'] + '.json'), {'observed_at': time.time(), 'class': type(error).__name__, 'detail': str(error) if isinstance(error, (ValueError, RuntimeError)) else 'transport_or_io_failure'})
            counts = dict(db.execute('SELECT state,count(*) FROM jobs GROUP BY state'))
            write(root / 'execution-status.json', {'observed_at': time.time(), 'counts': counts, 'workers': len(workers), 'new_publication_ready': False, 'public_changed': False, 'no_source_deletion': True})
            time.sleep(5)
    finally:
        db.close()


if __name__ == '__main__':
    main()
