#!/usr/bin/env python3
"""Finite cloud owner for the frozen best14 queue. No publication or source deletion."""
import argparse
import contextlib
import fcntl
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import signal
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request

spec = importlib.util.spec_from_file_location('finite', pathlib.Path(__file__).with_name('finite-staging.py'))
finite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(finite)
JSON_FILES = ('run-authority.json', 'work-queue-summary.json', 'execution-workers.json',
              'controller-reservation.json', 'execution-status.json', 'budget-ledger.json',
              'structure-screen-status.json', 'STOP', 'budget-draining.json',
              'budget-stopped.json', 'budget-pause-outcomes.json', 'cloud-stop-outcomes.json',
              'static-archive-receipt.json', 'controller-retirement-intent.json', 'cloud-error.json')


def load_workers(root):
    workers = json.loads((root / 'execution-workers.json').read_text())
    if not 4 <= len(workers) <= 8 or len({w['id'] for w in workers}) != len(workers):
        raise ValueError('cloud_worker_roster_differs')
    return workers


def preflight(root):
    authority = json.loads((root / 'run-authority.json').read_text())
    summary = json.loads((root / 'work-queue-summary.json').read_text())
    if authority['additional_budget_usd'] != 100 or summary['selection_sha256'] != finite.SELECTION:
        raise ValueError('cloud_frozen_authority_differs')
    workers = load_workers(root)
    config = json.loads((root / 'controller-reservation.json').read_text())
    if config.get('schema') != 'best14-cloud-controller-v1' or not 0 < config['hourly_usd'] <= 0.02:
        raise ValueError('cloud_controller_scope_differs')
    if not 3600 <= config['max_runtime_seconds'] <= 7 * 86400:
        raise ValueError('cloud_lifetime_differs')
    if (not isinstance(config.get('droplet_id'), int) or config['droplet_id'] <= 0 or
            config.get('name') != 'stoarama-best14-controller-20261024' or
            not isinstance(config.get('created_at_unix'), (int, float)) or
            not 0 < config['created_at_unix'] <= time.time()):
        raise ValueError('cloud_controller_identity_differs')
    selection = root / 'audit' / 'selection-final-v2.json'
    if hashlib.sha256(selection.read_bytes()).hexdigest() != finite.SELECTION:
        raise ValueError('cloud_selection_bytes_differ')
    with contextlib.closing(sqlite3.connect('file:' + str(root / 'execution.sqlite3') + '?mode=ro', uri=True)) as db:
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('cloud_queue_integrity')
        count = 0
        for path, job in db.execute('SELECT path,id FROM jobs'):
            p = pathlib.Path(path)
            if p.parent != root / 'work' or p.name != job + '.json.gz' or p.is_symlink():
                raise ValueError('cloud_work_path_differs')
            finite.read_work({'path': str(p), 'id': job})
            count += 1
        if count != 4342:
            raise ValueError('cloud_queue_count_differs')
    with contextlib.closing(sqlite3.connect('file:' + str(root / 'structure-screen.sqlite3') + '?mode=ro', uri=True)) as db:
        if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
            raise ValueError('cloud_screen_integrity')
    if not (root / 'executor.lock').is_file():
        raise ValueError('cloud_executor_lock_missing')
    return workers, config


def checkpoint_bytes(root):
    # Exact allowlist: environment files, source capabilities and keys never enter backups.
    with tempfile.TemporaryDirectory(dir=root, prefix='checkpoint-') as scratch:
        scratch = pathlib.Path(scratch)
        names = []
        for name in ('execution.sqlite3', 'structure-screen.sqlite3'):
            with contextlib.closing(sqlite3.connect('file:' + str(root / name) + '?mode=ro', uri=True)) as source:
                with contextlib.closing(sqlite3.connect(scratch / name)) as target:
                    source.backup(target)
            names.append(name)
        for name in JSON_FILES:
            p = root / name
            if p.exists():
                if p.is_symlink():
                    raise ValueError('checkpoint_symlink')
                (scratch / name).write_bytes(p.read_bytes())
                names.append(name)
        out = io.BytesIO()
        with tarfile.open(fileobj=out, mode='w:gz') as archive:
            for name in names:
                archive.add(scratch / name, arcname=name, recursive=False)
        body = out.getvalue()
    if len(body) > 64 * 1024 ** 2:
        raise ValueError('checkpoint_bound')
    return body


def archive_checkpoint(root):
    body = checkpoint_bytes(root)
    sha = hashlib.sha256(body).hexdigest()
    bucket = os.environ['R2_BUCKET']
    key = 'staging/best14-20261021/control-checkpoints/' + sha + '.tar.gz'
    cap = finite.presign('PUT', bucket, key, {'If-None-Match': '*', 'Content-Length': str(len(body)), 'Content-Type': 'application/gzip'})
    try:
        with urllib.request.urlopen(urllib.request.Request(cap['url'], data=body, headers=cap['headers'], method='PUT'), timeout=45) as response:
            if response.status not in (200, 201):
                raise RuntimeError('checkpoint_upload_status')
    except urllib.error.HTTPError as error:
        if error.code != 412:
            raise RuntimeError('checkpoint_upload_rejected') from None
    except (OSError, TimeoutError):
        pass  # A timed-out upload is reconciled by the full readback below.
    cap = finite.presign('GET', bucket, key)
    try:
        with urllib.request.urlopen(cap['url'], timeout=45) as response:
            back = response.read(len(body) + 1)
        if len(back) != len(body) or hashlib.sha256(back).hexdigest() != sha:
            raise ValueError('checkpoint_readback_differs')
    except (OSError, TimeoutError):
        raise RuntimeError('checkpoint_readback_unconfirmed') from None
    finite.write(root / 'checkpoint-receipt.json', {'observed_at': time.time(), 'bucket': bucket,
                 'key': key, 'sha256': sha, 'bytes': len(body), 'full_readback_verified': True})


def stop_screen(screen):
    if screen is not None and screen.poll() is None:
        os.killpg(screen.pid, signal.SIGTERM)
        try:
            screen.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(screen.pid, signal.SIGKILL)
            screen.wait()


def pause_and_confirm(root, workers):
    finite.pause_for_control_storage(root, workers)
    outcomes = {}
    for worker in workers:
        try:
            state = finite.provider('GET', '/' + worker['id'])
            state = state.get('microvm', state)
            outcomes[worker['id']] = state.get('state') == 'paused'
        except Exception:
            outcomes[worker['id']] = False
    finite.write(root / 'cloud-stop-outcomes.json', {'observed_at': time.time(), 'confirmed_paused': outcomes})
    if not all(outcomes.values()):
        raise RuntimeError('cloud_worker_pause_unconfirmed')


def terminal(root, config):
    if (root / 'STOP').exists():
        return 'stop_requested'
    if time.time() >= config['created_at_unix'] + config['max_runtime_seconds']:
        return 'finite_controller_deadline'
    return None


def retire_controller(root, config):
    # Only this disposable coordinator is retired, after its full immutable
    # inputs and latest SQLite state have separately passed R2 readback.
    receipt = json.loads((root / 'static-archive-receipt.json').read_text())
    current = json.loads((root / 'checkpoint-receipt.json').read_text())
    if (not receipt.get('full_readback_verified') or not current.get('full_readback_verified') or
            not receipt.get('key', '').startswith('staging/best14-20261021/cloud-inputs/') or
            not current.get('key', '').startswith('staging/best14-20261021/control-checkpoints/') or
            time.time() - current['observed_at'] > 120):
        raise ValueError('cloud_retirement_backups_unconfirmed')
    ident = config['droplet_id']
    if not isinstance(ident, int) or ident <= 0:
        raise ValueError('cloud_retirement_identity')
    url = 'https://api.digitalocean.com/v2/droplets/' + str(ident)
    headers = {'Authorization': 'Bearer ' + os.environ['DIGITALOCEAN_TOKEN']}
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=45) as response:
            droplet = json.loads(response.read())['droplet']
        if (droplet['id'] != ident or droplet['name'] != config['name'] or
                'best14-controller-20261024' not in droplet['tags']):
            raise ValueError('cloud_retirement_provider_identity')
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers, method='DELETE'), timeout=45) as response:
            if response.status != 204:
                raise RuntimeError('cloud_retirement_unconfirmed')
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise RuntimeError('cloud_retirement_provider_rejected') from None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-root', type=pathlib.Path, required=True)
    parser.add_argument('--preflight', action='store_true')
    args = parser.parse_args()
    root = args.run_root.resolve()
    if args.preflight:
        workers, config = preflight(root)
        print(json.dumps({'preflight': 'passed', 'jobs': 4342, 'workers': len(workers)}))
        return
    workers = load_workers(root)
    with (root / 'cloud-owner.lock').open('a+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        try:
            workers, config = preflight(root)
        except BaseException:
            finite.pause_for_control_storage(root, workers)
            raise
        env = dict(os.environ, BEST14_RUN_ROOT=str(root))
        screen = None
        screen_started = 0
        checkpoint_at = None
        try:
            while True:
                reason = terminal(root, config)
                if reason:
                    stop_screen(screen)
                    pause_and_confirm(root, workers)
                    finite.write(root / 'STOP', {'reason': reason, 'public_changed': False})
                    finite.write(root / 'controller-retirement-intent.json', {'observed_at': time.time(), 'droplet_id': config['droplet_id'], 'reason': reason, 'preserve_worker_disks': True})
                    archive_checkpoint(root)
                    retire_controller(root, config)
                    return
                if screen is not None and screen.poll() is None and time.monotonic() - screen_started > 240:
                    stop_screen(screen)
                if screen is None or screen.poll() is not None:
                    screen = subprocess.Popen([sys.executable, str(pathlib.Path(__file__).with_name('source-screen.py'))],
                               env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                    screen_started = time.monotonic()
                # R2 backup health gates new dispatch. At most one five-minute window
                # can advance without a fully read-back checkpoint.
                if checkpoint_at is None or time.monotonic() - checkpoint_at >= 300:
                    archive_checkpoint(root)
                    checkpoint_at = time.monotonic()
                sys.argv = ['finite-staging', '--run-root', str(root)]
                finite.main()
                finite.write(root / 'cloud-health.json', {'observed_at': time.time(), 'owner': 'digitalocean',
                    'jelly_required': False, 'checkpoint_at_monotonic': checkpoint_at,
                    'last_pass': 'completed', 'public_changed': False})
                time.sleep(2)
        except BaseException as error:
            # Persist a bounded classification; never serialize remote errors or URLs.
            try:
                finite.write(root / 'cloud-error.json', {'observed_at': time.time(),
                    'class': type(error).__name__, 'detail': 'controller_pass_failed'})
            except Exception:
                pass
            try:
                stop_screen(screen)
            finally:
                finite.pause_for_control_storage(root, workers)
            raise
        finally:
            stop_screen(screen)


if __name__ == '__main__':
    main()
