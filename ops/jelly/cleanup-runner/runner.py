#!/usr/bin/env python3
"""Supervise the 2026-11-11 keep28 standing approval. No third-party Python packages."""
import argparse
import base64
import fcntl
import hashlib
import html
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
import urllib.error
import urllib.request

API = 'https://stoarama-api.onrender.com'
SERVICE = 'srv-d6usqn94tr6s73d94cd0'
POLICY = 'backend/internal/api/nas_cleanup_keep28_policy.json'
POLICY_SHA = '060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'
DAY_CAP = 5_000_000_000_000
PLAN_CAP = 100_000_000_000
RECOVERY = {'r2_sha_verified', 'r2_md5_etag_matched'}
DEPLOY_TERMINAL = {'live', 'deactivated', 'canceled', 'build_failed', 'update_failed', 'pre_deploy_failed'}
REASON = ('Deniz standing approval 2026-11-11: NAS files outside pinned keep28 only; '
          'server dry run + independent verification PASS; verified R2 copies retained; '
          '5 TB/24h; one plan; master OFF between batches.')


class Halt(Exception):
    """Only fixed, credential-free messages may enter the log or owner email."""


class LockHeld(Exception):
    """Another runner owns the master switch; leave it alone."""


def require(ok, message):
    if not ok:
        raise Halt(message)


def critical(message):
    try:
        print('CRITICAL: ' + message, file=sys.stderr, flush=True)
    except OSError:
        pass  # A broken log sink must not prevent shutdown or email.


def utcnow():
    return datetime.now(timezone.utc)


def timestamp(value):
    d = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(d.tzinfo is not None, 'timestamp has no timezone')
    return d


def outside(sid, start, end, windows):
    require(sid in windows, 'stream has no pinned window')
    a, b = timestamp(start), timestamp(end)
    ws, we = windows[sid]
    require(2020 <= a.year and b.year < 2028 and b > a, 'uncertain media interval')
    return b <= ws or a >= we


def budget_permits(recent, proposed):
    return 0 <= recent and 0 < proposed <= PLAN_CAP and recent + proposed <= DAY_CAP


def results(plan):
    items, rows = plan['items'], plan['results']
    require(items and len(items) == len(rows), 'incomplete plan report')
    require(sorted(r['ordinal'] for r in rows) == list(range(1, len(items) + 1)),
            'duplicate or missing result ordinal')
    return sorted(rows, key=lambda r: r['ordinal'])


def dry_state(plan):
    require(plan['mode'] == 'dry_run', 'dry run left dry_run mode')
    rows = results(plan)
    require(not any(r['delete_ever_issued'] for r in rows), 'dry run has issued deletions')
    require(all(r['state'] in {'pending', 'leased', 'would_delete'} for r in rows),
            'dry run contains a non-permitting item')
    require(all(r['recovery'] in RECOVERY for r in rows if r['state'] == 'would_delete'),
            'dry run recovery is not verified')
    return all(r['state'] == 'would_delete' for r in rows)


def verify(plan, catalog, windows, now):
    require(dry_state(plan), 'dry run is incomplete')
    require(plan['policy_current'] is True and plan['policy_sha256'] == POLICY_SHA,
            'plan policy differs from pinned policy')
    require(plan['allow_nas_only'] is False, 'NAS-only deletion forbidden')
    require(catalog['protected_overlap'] == 0, 'protected identity overlap')
    identities = {(c['kind'], c['source_id']): c for c in catalog['items']}
    require(len(identities) == len(plan['items']), 'catalog row missing or duplicated')
    streams, seen = set(), set()
    for item, row in zip(plan['items'], results(plan)):
        identity = (item['kind'], item['source_id'])
        require(identity not in seen and identity in identities, 'uncertain source identity')
        seen.add(identity)
        current = identities[identity]
        for key in ('stream_id', 'size_bytes', 'sha256', 'start_at', 'end_at', 'object_key', 'bucket', 'relative_path'):
            a, b = item[key], current[key]
            require(timestamp(a) == timestamp(b) if key in {'start_at', 'end_at'} else a == b,
                    'catalog identity mismatch')
        require(item['size_bytes'] > 0 and re.fullmatch('[a-f0-9]{64}', item['sha256'])
                and item['object_key'] and item['bucket'], 'incomplete file identity')
        sid = item['stream_id']
        streams.add(sid)
        require(outside(sid, current['start_at'], current['end_at'], windows), 'file intersects keep28')
        sources = current['sources']
        expected = set(item['source_ids']) | set(current['source_ids'])
        resolved = [c['source_id'] for c in sources or []]
        require(expected and sorted(resolved) == sorted(expected)
                and sorted(item['source_ids']) == sorted(current['source_ids']),
                'source catalog incomplete or duplicated')
        for clip in sources:
            require(clip['stream_id'] == sid and clip['bucket'] == item['bucket']
                    and outside(sid, clip['start_at'], clip['end_at'], windows), 'source intersects keep28 or is uncertain')
        require(0 <= (now - timestamp(row['updated_at'])).total_seconds() < 20 * 3600,
                'observation is stale or in the future')
    size = sum(i['size_bytes'] for i in plan['items'])
    require(len(streams) == 1 and 0 < size <= PLAN_CAP, 'plan must be <=100 GB and one stream')
    return size


def execution_state(plan):
    rows = results(plan)
    reason = plan['approval_reason']
    expired = (reason.startswith('auto-paused: approval expired; ')
               or reason == 'auto-paused: execution lease expired without a report')
    if all(r['state'] == 'deleted' and r['delete_ever_issued'] for r in rows):
        require(plan['mode'] in {'enabled', 'paused'}, 'unexpected completed plan mode')
        if plan['mode'] == 'paused' and not (expired or reason == 'completed: all items deleted'):
            return 'anomaly'
        return 'done'
    if plan['mode'] == 'paused':
        return 'expired' if expired else 'anomaly'
    require(plan['mode'] == 'enabled', 'execution left enabled mode unexpectedly')
    require(all(r['state'] in {'pending', 'leased', 'would_delete', 'delete_issued', 'deleted'} for r in rows),
            'execution contains an anomalous item')
    return 'running'


def human_size(value):
    value = int(value or 0)
    return '{:,.1f} {}'.format(value / (10**12 if value >= 10**12 else 10**9),
                               'TB' if value >= 10**12 else 'GB')


def render_email(status, reason, now):
    """Render only owner-facing fields; never dump internal status or errors."""
    daily = reason.startswith('daily progress ')
    files = '{:,}'.format(int(status.get('deleted_files') or 0))
    size = human_size(status.get('deleted_bytes'))
    plan = status.get('current_plan')
    plan = str(int(plan)) if plan is not None else 'none'
    phases = {
        'dry_run': 'checking the next batch (no deletion)',
        'dry_run_pass': 'next batch checked (no deletion)',
        'master_false': 'idle, deletion switched off',
        'monitoring': 'deleting batch ' + plan,
        'enabling': 'starting batch ' + plan,
        'master_true': 'deletion switch enabled',
        'master_true_requested': 'preparing the deletion switch',
        'waiting_for_budget': 'waiting for the daily deletion allowance',
        'batch_complete': 'batch finished',
        'recovery_verified': 'R2 recovery copies verified',
        'complete': 'cleanup complete', 'stopped': 'cleanup stopped',
        'failed': 'cleanup stopped for owner review',
        'starting': 'starting cleanup checks',
    }
    phase = phases.get(status.get('phase'), 'checking cleanup status')
    subject = ('Stoarama NAS cleanup — {} {}: {} files ({}) deleted so far'.format(
        now.strftime('%b'), now.day, files, size) if daily
        else 'ACTION NEEDED: ' + reason)
    lines = ['The runner is ' + phase + '. Current plan: ' + plan + '.',
             'Deleted so far: ' + files + ' files (' + size + ').']
    if status.get('deleted_24h_files') is not None:
        lines.append('Deleted in the last 24 hours: {:,} files ({}).'.format(
            int(status['deleted_24h_files']), human_size(status.get('deleted_24h_bytes'))))
    if not daily:
        # OFF may still be retrying; do not claim a confirmed shutdown in that case.
        lines.append('What to do: reply or ask Claude to review. ' + (
            'Deletion is off.' if status.get('phase') == 'master_false'
            else 'Deletion is being switched off; check shutdown confirmation.'))
        if status.get('last_error'):
            lines.append('Last error: ' + status['last_error'])  # Fixed, sanitized by caller.
    rows = []
    for row in status.get('per_stream') or []:
        sid = str(int(row['stream_id']))
        name = row.get('stream_name')
        # Stream names are catalog text, not diagnostic output. Omit URL-like names.
        name = name if isinstance(name, str) and not re.search(r'[:/\\\r\n]', name) else None
        rows.append((sid + (' — ' + name if name else ''),
                     '{:,}'.format(int(row['files'])), human_size(row['bytes'])))
    text = '\n\n'.join(lines)
    text += '\n\nPer stream (stream, files, size):\n' + ('\n'.join(
        ' | '.join(row) for row in rows) if rows else 'No deleted files reported yet.')
    footer = 'Sent by the Jelly cleanup runner. R2 copies are kept.'
    text += '\n\n' + footer
    esc = html.escape
    body = '<!doctype html><html><head><meta name="viewport" content="width=device-width, initial-scale=1"></head>'
    body += '<body style="margin:0;padding:16px;background:#f5f6f8;color:#202532;font-family:Arial,sans-serif;line-height:1.5">'
    body += '<div style="max-width:640px;margin:auto;padding:20px;background:#fff;border-radius:8px">'
    body += '<h1 style="font-size:22px;line-height:1.3">' + esc(subject) + '</h1>'
    body += ''.join('<p>' + esc(line) + '</p>' for line in lines)
    body += '<h2 style="font-size:18px">Per stream</h2><table style="width:100%;border-collapse:collapse;font-size:14px">'
    body += '<tr>' + ''.join('<th scope="col" style="text-align:left;padding:8px;border-bottom:2px solid #ddd">' + label + '</th>' for label in ('Stream', 'Files', 'Size')) + '</tr>'
    body += ''.join('<tr>' + ''.join('<td style="padding:8px;border-bottom:1px solid #eee;overflow-wrap:anywhere">' + esc(cell) + '</td>' for cell in row) + '</tr>' for row in rows)
    body += '</table>' + ('' if rows else '<p>No deleted files reported yet.</p>')
    body += '<p style="font-size:12px;color:#576174">' + esc(footer) + '</p></div></body></html>'
    return subject, text, body


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise Halt('HTTP redirect refused')


def request(url, token, method='GET', body=None):
    req = urllib.request.Request(url, method=method,
        data=None if body is None else json.dumps(body).encode(), headers={
            'Authorization': 'Bearer ' + token, 'User-Agent': 'stoarama-operator/1',
            'Content-Type': 'application/json'})
    try:
        with urllib.request.build_opener(NoRedirect()).open(req, timeout=120) as response:
            raw = response.read(32 << 20)
            return json.loads(raw) if raw else {}
    except Exception:
        # HTTP bodies, subprocess stderr and exception strings can contain credentials.
        raise Halt('HTTP request failed (' + method + ')') from None


def command(argv, **kwargs):
    try:
        return subprocess.run(argv, capture_output=True, check=True, timeout=360, **kwargs).stdout
    except Exception:
        raise Halt('local command failed; inspect privately') from None


def render_key():
    # Deliberately supports only Render CLI's simple api: / key: mapping.
    lines = (Path.home() / '.render/cli.yaml').read_text().splitlines()
    in_api = False
    for line in lines:
        if line == 'api:':
            in_api = True
        elif in_api and line and not line[0].isspace():
            break
        elif in_api:
            match = re.fullmatch(r'\s+key:\s*[\'\"]?([A-Za-z0-9_-]+)[\'\"]?\s*', line)
            if match:
                return match[1]
    raise Halt('Render CLI api.key unavailable')


class Runner:
    def __init__(self, repo, state, dry_run=False):
        self.repo, self.root, self.dry_run = repo, state, dry_run
        self.stopping = False
        self.lock = None
        self.status = {'current_plan': None, 'last_error': None}

    def initialize(self):
        # Keep all startup I/O inside main's fail-safe, with usable in-memory state.
        self.repo = self.repo.resolve()
        self.root = self.root.expanduser()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.lock = (self.root / 'lock').open('a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise LockHeld from None
        path = self.root / 'status.json'
        try:
            try:
                raw = path.read_text()
            except FileNotFoundError:
                return
            status = json.loads(raw)
            require(isinstance(status, dict)
                    and (status['current_plan'] is None or type(status['current_plan']) is int and status['current_plan'] > 0)
                    and (status['last_error'] is None or isinstance(status['last_error'], str)),
                    'invalid ownership state')
        except Exception:
            raise Halt('execution ownership unreadable; unknown enabled plan; owner review required') from None
        self.status = status

    def load_policy(self):
        repo = self.repo
        raw = command(['git', '-C', str(repo), 'show', 'origin/main:' + POLICY])
        require(hashlib.sha256(raw).hexdigest() == POLICY_SHA, 'origin/main keep28 policy differs')
        self.windows = {r['stream_id']: (timestamp(r['keep_start_utc']), timestamp(r['keep_end_utc'])) for r in json.loads(raw)}
        require(len(self.windows) == 109, 'incomplete pinned policy')
        require((repo / POLICY).read_bytes() == raw, 'checkout keep28 policy differs from origin/main')

    def save(self, phase, **values):
        self.status.update(values, phase=phase, updated_at=utcnow().isoformat(), dry_run=self.dry_run)
        tmp = self.root / 'status.tmp'
        with tmp.open('w') as f:
            json.dump(self.status, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(self.root / 'status.json')
        directory = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        with (self.root / 'runner.jsonl').open('a') as f:
            f.write(json.dumps({'at': self.status['updated_at'], 'phase': phase, **values}) + '\n')
            f.flush()
            os.fsync(f.fileno())

    def query(self, sql):
        try:
            if not hasattr(self, 'prepare'):
                spec = importlib.util.spec_from_file_location('prepare_keep28', self.repo / 'backend/scripts/nas-cleanup/prepare_keep28_plans.py')
                self.prepare = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(self.prepare)
            return self.prepare.query(sql, 'psql')
        except Exception:
            raise Halt('read-only catalog query failed') from None

    def api(self, method, path, body=None):
        return request(API + '/api/v1/service/nas-cleanup/' + path, os.environ['SERVICE_TOKEN'], method, body)

    def plan(self, pid):
        p = self.api('GET', 'plans/' + str(int(pid)))
        require(p['id'] == pid, 'plan identity differs')
        results(p)
        return p

    def active(self):
        return self.query("SELECT json_build_object('plans', (SELECT coalesce(json_agg(id),'[]') FROM nas_cleanup_plans WHERE mode='enabled'), 'batches', (SELECT count(*) FROM nas_cleanup_batches WHERE mode='enabled'))")

    def idle(self):
        active = self.active()
        require(not active['plans'] and active['batches'] == 0, 'another plan or batch is enabled')

    def render(self, method, path, body=None):
        return request('https://api.render.com/v1/services/' + SERVICE + path, render_key(), method, body)

    def live(self):
        rows = self.render('GET', '/deploys?limit=100')
        deploys = [r['deploy'] for r in rows]
        require(all(d['status'] in DEPLOY_TERMINAL for d in deploys),
                'another Render deployment is in progress')
        live = [d for d in deploys if d['status'] == 'live']
        require(len(live) == 1 and re.fullmatch('[a-f0-9]{40}', live[0]['commit']['id']), 'current live commit unavailable')
        return live[0]

    def alert(self, subject):
        # Must work even when state storage or email delivery is broken.
        critical(subject)
        try:
            self.notify(subject)
        except Exception:
            self.status['owner_notification_failed'] = True
            critical('owner email failed; operator intervention required')

    def step_failed(self, step):
        # Callers supply fixed step names, never URLs, SQL, or exception text.
        critical('STEP FAILED: ' + step)
        try:
            with (self.root / 'runner.jsonl').open('a') as f:
                f.write(json.dumps({'at': utcnow().isoformat(), 'phase': 'step_failed', 'step': step}) + '\n')
        except Exception:
            pass  # Diagnostics must never block shutdown, even before initialize().

    def read(self, step, action):
        # Retry only reads. Bound the added backoff to two minutes; each I/O also
        # retains its existing timeout. Mutations must not be blindly replayed.
        for delay in (5, 10, 20, 30, 30, 25, None):
            try:
                return action()
            except Exception:
                if delay is None:
                    raise  # The enclosing OFF/ON handler logs the exhausted step.
                self.step_failed(step)
                time.sleep(delay)

    def off(self):
        delay = 15
        pending = None
        while True:
            try:
                # This PUT must precede every other dependency, including state writes.
                step = 'OFF env PUT'
                self.render('PUT', '/env-vars/NAS_CLEANUP_DELETION_ENABLED', {'value': 'false'})
                if pending is None:
                    step = 'OFF deploy list GET'
                    deploys = [r['deploy'] for r in self.read(step,
                        lambda: self.render('GET', '/deploys?limit=100'))]
                    live = [d for d in deploys if d['status'] == 'live']
                    require(len(live) == 1 and re.fullmatch('[a-f0-9]{40}', live[0]['commit']['id']),
                            'current live commit unavailable')
                    commit = live[0]['commit']['id']
                    step = 'OFF deploy cancel POST'
                    for d in deploys:
                        if d['status'] not in DEPLOY_TERMINAL:
                            self.render('POST', '/deploys/' + d['id'] + '/cancel')
                    step = 'OFF deploy create POST'
                    dep = self.render('POST', '/deploys', {'commitId': commit, 'clearCache': 'do_not_clear'})
                    # Remember only a deploy proven created AFTER the successful false PUT.
                    require(dep['id'] not in {d['id'] for d in deploys}, 'OFF deploy was not newly created')
                    pending = (dep['id'], commit)
                dep_id, commit = pending
                deadline = time.monotonic() + 1800
                while True:
                    step = 'OFF deploy status GET'
                    d = self.read(step, lambda: self.render('GET', '/deploys/' + dep_id))
                    if d['status'] in DEPLOY_TERMINAL and d['status'] != 'live':
                        pending = None  # A terminal deploy cannot converge; replace it.
                        raise Halt('OFF redeploy failed')
                    if d['status'] == 'live':
                        if d['commit']['id'] != commit:
                            pending = None
                        require(d['commit']['id'] == commit, 'Render deployed a different commit')
                        break
                    # Even after a timeout or exhausted read retries, keep waiting on
                    # this false-env deploy. Never cancel our own in-progress OFF.
                    require(time.monotonic() < deadline, 'OFF redeploy timed out')
                    time.sleep(15)
                step = 'OFF live deploy GET'
                if self.read(step, self.live)['id'] != dep_id:
                    pending = None
                    raise Halt('live deployment changed concurrently')
                step = 'OFF env GET'
                env = self.read(step, lambda: self.render('GET', '/env-vars/NAS_CLEANUP_DELETION_ENABLED'))
                if env['value'] != 'false':
                    pending = None
                require(env['value'] == 'false', 'Render master env differs')
                step = 'OFF plan id query'
                pid = self.status['current_plan'] or self.read(step, lambda: self.query(
                    'SELECT id FROM nas_cleanup_plans WHERE account_id=47 AND connection_id=13 ORDER BY id DESC LIMIT 1'))
                step = 'OFF runtime plan GET'
                if not pid or self.read(step, lambda: self.plan(pid))['master_deletion_enabled'] is not False:
                    pending = None
                    raise Halt('live API master OFF not confirmed')
                step = 'OFF final live deploy GET'
                if self.read(step, self.live)['id'] != dep_id:
                    pending = None
                    raise Halt('live deployment changed during OFF verification')
                break
            except Exception:
                self.step_failed(step)
                self.status['shutdown_or_recovery_failed'] = True
                self.alert('MASTER OFF NOT CONFIRMED; retrying shutdown')
                time.sleep(delay)
                delay = min(delay * 2, 300)
        # A disk error must not invalidate a confirmed OFF or block failure notification.
        self.status.update(phase='master_false', live_commit=commit)

    def master(self, enabled, emergency=False):
        if not enabled:
            self.off()
            return
        require(not emergency, 'emergency enable forbidden')
        try:
            step = 'ON idle query'
            self.read(step, self.idle)
            step = 'ON live deploy GET'
            live = self.read(step, self.live)
            step = 'ON state write'
            self.save('master_true_requested', live_commit=live['commit']['id'])
            step = 'ON env PUT'
            self.render('PUT', '/env-vars/NAS_CLEANUP_DELETION_ENABLED', {'value': 'true'})
            # Never deploy HEAD, origin/main, or a fix: redeploy exactly the live SHA.
            step = 'ON deploy create POST'
            dep = self.render('POST', '/deploys', {'commitId': live['commit']['id'], 'clearCache': 'do_not_clear'})
            deadline = time.monotonic() + 1800
            while True:
                step = 'ON deploy status GET'
                d = self.read(step, lambda: self.render('GET', '/deploys/' + dep['id']))
                if d['status'] == 'live':
                    require(d['commit']['id'] == live['commit']['id'], 'Render deployed a different commit')
                    break
                require(d['status'] in {'created', 'build_in_progress', 'pre_deploy_in_progress', 'update_in_progress', 'queued'}, 'Render redeploy failed')
                require(time.monotonic() < deadline, 'Render redeploy timed out')
                time.sleep(15)
            step = 'ON live deploy GET'
            require(self.read(step, self.live)['id'] == dep['id'], 'live deployment changed concurrently')
            step = 'ON env GET'
            env = self.read(step, lambda: self.render('GET', '/env-vars/NAS_CLEANUP_DELETION_ENABLED'))
            require(env['value'] == 'true', 'Render master env differs')
            pid = self.status['current_plan']
            if pid:
                step = 'ON runtime plan GET'
                require(self.read(step, lambda: self.plan(pid))['master_deletion_enabled'] is True, 'live API master differs')
            step = 'ON state write'
            self.save('master_true')
        except Exception:
            self.step_failed(step)
            raise

    def stopped(self):
        return self.stopping or (self.root / 'STOP').exists()

    def check_delivery(self):
        require(os.environ.get('CLEANUP_OWNER_EMAIL') and os.environ.get('EMAIL_FROM')
                and os.environ.get('EMAIL_RESEND_API_KEY'), 'owner email configuration missing')

    def notify(self, subject):
        self.check_delivery()
        # Establish the old minimal payload before rendering. Formatting failures
        # must never suppress an owner notification or expose exception details.
        text = json.dumps({k: self.status.get(k) for k in ('phase', 'deleted_files', 'deleted_bytes', 'current_plan', 'last_error')})
        payload = {'from': os.environ['EMAIL_FROM'], 'to': [os.environ['CLEANUP_OWNER_EMAIL']],
                   'subject': '[Stoarama NAS cleanup] ' + subject, 'text': text}
        try:
            title, plain, rich = render_email(self.status, subject, utcnow())
            payload.update(subject=title, text=plain, html=rich)
        except Exception:
            pass  # No exception strings: this is also the emergency alert path.
        reply = request('https://api.resend.com/emails', os.environ['EMAIL_RESEND_API_KEY'], 'POST', payload)
        require(bool(reply.get('id')), 'owner email delivery not accepted')

    def progress(self):
        rows = self.query("""SELECT coalesce(json_agg(x),'[]') FROM (
          SELECT (p.manifest->(i.ordinal-1)->>'stream_id')::bigint stream_id,
            count(*) files,sum((p.manifest->(i.ordinal-1)->>'size_bytes')::bigint) bytes,
            count(*) FILTER (WHERE i.updated_at>now()-interval '24 hours') recent_files,
            coalesce(sum((p.manifest->(i.ordinal-1)->>'size_bytes')::bigint)
              FILTER (WHERE i.updated_at>now()-interval '24 hours'),0) recent_bytes
          FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id
          WHERE p.account_id=47 AND p.connection_id=13 AND i.state='deleted' AND i.delete_ever_issued GROUP BY 1 ORDER BY 1) x""")
        self.add_stream_names(rows)
        self.save(self.status.get('phase', 'starting'), deleted_files=sum(r['files'] for r in rows),
                  deleted_bytes=sum(r['bytes'] for r in rows), per_stream=rows,
                  deleted_24h_files=sum(r.get('recent_files', 0) for r in rows),
                  deleted_24h_bytes=sum(r.get('recent_bytes', 0) for r in rows))
        today = utcnow().date().isoformat()
        if self.status.get('last_note_day') != today:
            self.notify('daily progress ' + today)
            self.save(self.status['phase'], last_note_day=today, daily_note={
                'files': self.status['deleted_files'], 'bytes': self.status['deleted_bytes'],
                'current_plan': self.status['current_plan']})

    def add_stream_names(self, rows):
        # Cosmetic only: a failed name lookup must never block progress or alerts.
        try:
            ids = sorted({int(r['stream_id']) for r in rows if r.get('stream_id') is not None})
            if not ids:
                return
            names = self.query("SELECT coalesce(json_object_agg(id,name),'{}') FROM streams WHERE id = ANY(ARRAY["
                               + ','.join(str(i) for i in ids) + "]::bigint[])")
            for r in rows:
                name = names.get(str(r.get('stream_id'))) if isinstance(names, dict) else None
                if isinstance(name, str):
                    r['stream_name'] = name
        except Exception:
            pass

    def recent_bytes(self):
        # Count ever-issued permits, including uncertain receipts: stricter than deleted bytes.
        return self.query("""SELECT coalesce(sum((p.manifest->(i.ordinal-1)->>'size_bytes')::bigint),0)
          FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id
          WHERE i.delete_ever_issued AND i.updated_at>now()-interval '24 hours'""")

    def catalog(self, plan):
        encoded = base64.b64encode(json.dumps(plan['items']).encode()).decode()
        pid = int(plan['id'])
        return self.query("""WITH wanted AS (SELECT e->>'kind' kind,(e->>'source_id')::bigint source_id
          FROM jsonb_array_elements(convert_from(decode('""" + encoded + """','base64'),'UTF8')::jsonb) e),
          raw AS (SELECT 'raw' kind,c.id source_id,r.stream_id,c.size_bytes,lower(c.sha256) sha256,
            c.clip_start_at start_at,c.clip_end_at end_at,c.object_key,c.bucket,c.display_path relative_path,ARRAY[c.id] source_ids
            FROM wanted w JOIN recording_clips c ON w.kind='raw' AND w.source_id=c.id
            JOIN recordings r ON r.id=c.recording_id WHERE r.account_id=47),
          col AS (SELECT 'collated' kind,o.id source_id,r.stream_id,o.size_bytes,lower(o.sha256) sha256,
            o.start_at,o.end_at,o.object_key,'""" + os.environ.get('R2_BUCKET', 'stoarama').replace("'", "''") + """'::text bucket,
            'joined/'||o.nas_relative_path relative_path,o.source_clip_ids source_ids
            FROM wanted w JOIN recording_collation_outputs o ON w.kind='collated' AND o.id=w.source_id
            JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id WHERE r.account_id=47),
          cat AS (SELECT * FROM raw UNION ALL SELECT * FROM col)
          SELECT json_build_object('items',(SELECT coalesce(json_agg(x),'[]') FROM (
            SELECT cat.*,(SELECT json_agg(json_build_object('source_id',c.id,'stream_id',r.stream_id,
              'start_at',c.clip_start_at,'end_at',c.clip_end_at,'bucket',c.bucket))
              FROM recording_clips c JOIN recordings r ON r.id=c.recording_id
              WHERE r.account_id=47 AND c.id=ANY(cat.source_ids)) sources FROM cat) x),
            'protected_overlap',(SELECT count(*) FROM nas_cleanup_reserved_files f JOIN wanted w USING(kind,source_id)
              WHERE f.account_id=47 AND f.connection_id=13 AND f.label<>(SELECT label FROM nas_cleanup_plans WHERE id=""" + str(pid) + ")))")

    def await_dry(self, pid):
        deadline = time.monotonic() + 20 * 3600
        while True:
            if self.stopped():
                return None
            p = self.plan(pid)
            require(p['master_deletion_enabled'] is False, 'master unexpectedly ON during dry run')
            if dry_state(p):
                return p
            require(time.monotonic() < deadline, 'dry run timed out')
            self.progress()
            time.sleep(60)

    def pick(self):
        ids = self.query("""SELECT coalesce(json_agg(id ORDER BY id),'[]') FROM nas_cleanup_plans p
          WHERE account_id=47 AND connection_id=13 AND mode='dry_run'
          AND report_sha256='""" + POLICY_SHA + """' AND EXISTS(SELECT 1 FROM nas_cleanup_items WHERE plan_id=p.id)
          AND NOT EXISTS(SELECT 1 FROM nas_cleanup_items WHERE plan_id=p.id
            AND (state<>'would_delete' OR delete_ever_issued OR updated_at<=now()-interval '20 hours'))""")
        if ids:
            return ids[0]
        for sid in sorted(self.windows):
            if self.stopped():
                return None
            # A new prefix per admission avoids immutable-label reuse, even on one day.
            prefix = 'k28auto-' + utcnow().strftime('%Y%m%d-%H%M%S')
            out = self.root / 'plans' / (prefix + '-s' + str(sid))
            command([sys.executable, str(self.repo / 'backend/scripts/nas-cleanup/prepare_keep28_plans.py'),
                '--stream', str(sid), '--max-bytes', str(PLAN_CAP), '--max-plans', '1',
                '--label-prefix', prefix, '--out', str(out), '--api', API, '--submit'], env=os.environ.copy())
            summary = json.loads((out / 'summary.json').read_text())
            require(not summary['rejected_at_admission'], 'preparer rejected an uncertain file')
            if summary['receipts']:
                return summary['receipts'][0]['id']
        # The preparer excludes reserved files. Never call that a completed cleanup.
        pending = self.query("""SELECT count(*) FROM nas_cleanup_items i JOIN nas_cleanup_plans p ON p.id=i.plan_id
          WHERE p.account_id=47 AND p.connection_id=13 AND i.state<>'deleted'
          AND (p.mode<>'retired' OR i.delete_ever_issued)""")
        require(pending == 0, 'no eligible work but reserved or uncertain files remain')
        self.confirm_empty()
        return None

    def confirm_empty(self):
        # Inventory/acks can outlive a deletion receipt. Only confirmed deleted
        # identities are subtracted. Unmapped files and invalid spans block completion.
        policy = [{'id': sid, 'a': a.isoformat(), 'b': b.isoformat()}
                  for sid, (a, b) in self.windows.items()]
        encoded = base64.b64encode(json.dumps(policy).encode()).decode()
        count = self.query("""WITH w AS (SELECT (e->>'id')::bigint id,
          (e->>'a')::timestamptz a,(e->>'b')::timestamptz b
          FROM jsonb_array_elements(convert_from(decode('""" + encoded + """','base64'),'UTF8')::jsonb) e),
          deleted AS (SELECT p.manifest->(i.ordinal-1) e FROM nas_cleanup_items i
            JOIN nas_cleanup_plans p ON p.id=i.plan_id
            WHERE p.account_id=47 AND p.connection_id=13 AND i.state='deleted' AND i.delete_ever_issued),
          files AS (
            SELECT 'raw' kind,c.id source_id,r.stream_id,c.clip_start_at a,c.clip_end_at b,
              f.relative_path path,f.sha256 sha,f.size_bytes size,
              (f.relative_path=c.display_path AND f.sha256=lower(c.sha256) AND f.size_bytes=c.size_bytes) valid
            FROM nas_inventory_files f LEFT JOIN recording_clips c ON c.id=f.clip_id
            LEFT JOIN recordings r ON r.id=c.recording_id
            WHERE f.connection_id=13 AND f.state='present'
            UNION ALL
            SELECT 'collated',o.id,r.stream_id,o.start_at,o.end_at,'joined/'||o.nas_relative_path,a.sha256,a.size_bytes,
              (a.sha256=lower(o.sha256) AND a.size_bytes=o.size_bytes)
            FROM recording_collation_output_acks a JOIN recording_collation_outputs o ON o.id=a.output_id
            JOIN recording_collation_hours h ON h.id=o.collation_hour_id JOIN recordings r ON r.id=h.recording_id
            WHERE a.connection_id=13)
          SELECT count(*) FROM files f LEFT JOIN w ON w.id=f.stream_id
          WHERE NOT EXISTS(SELECT 1 FROM deleted d WHERE d.e->>'kind'=f.kind
            AND (d.e->>'source_id')::bigint=f.source_id AND d.e->>'relative_path'=f.path
            AND d.e->>'sha256'=f.sha AND (d.e->>'size_bytes')::bigint=f.size)
          AND (w.id IS NULL OR f.valid IS NOT TRUE OR f.a IS NULL OR f.b IS NULL
            OR f.b<=f.a OR f.a<'2020-01-01' OR f.b>='2028-01-01' OR NOT(f.a<w.b AND f.b>w.a))""")
        require(count == 0, 'outside-window or uncertain NAS files remain; owner reconciliation needed')

    def recovery(self, plan):
        # No API route exists at this revision. CLI GETs the plan and HEADs every issued R2 object.
        raw = command(['/Users/jelly/tmp-safeguards/stoaramactl', 'nas-cleanup', 'verify-recovery',
                       '--plan-id', str(plan['id']), '--backend-api-url', API], env=os.environ.copy())
        r = json.loads(raw)
        issued = sum(x['delete_ever_issued'] for x in results(plan))
        require(r['plan_id'] == plan['id'] and r['plan_sha256'] == plan['plan_sha256']
                and r['items'] == len(plan['items']) and r['delete_ever_issued'] == issued
                and r['verified'] == issued and r['problems'] == 0, 'R2 recovery verification failed')
        self.save('recovery_verified', recovery_issued=issued)

    def drain(self, pid):
        deadline = time.monotonic() + 900
        while self.query("SELECT count(*) FROM nas_cleanup_items WHERE plan_id=" + str(int(pid))
                        + " AND (state='delete_issued' OR (state='leased' AND lease_expires_at>now()))"):
            require(time.monotonic() < deadline, 'execution receipts did not drain; owner reconciliation needed')
            self.progress()
            time.sleep(30)

    def finish(self, plan):
        self.master(False, emergency=True)
        self.drain(plan['id'])
        plan = self.plan(plan['id'])
        self.recovery(plan)
        # Verification can race a manual pause: classify a fresh report before close.
        plan = self.plan(plan['id'])
        state = execution_state(plan)
        require(state in {'done', 'expired'}, 'plan paused for a non-expiry reason')
        require(not any(r['state'] in {'delete_issued', 'unverified', 'media_deleted_sidecar_failed', 'conflict', 'missing', 'keep'}
                        for r in results(plan)), 'uncertain execution receipt requires owner')
        if state == 'expired' and any(not r['delete_ever_issued'] for r in results(plan)):
            closed = self.api('POST', 'plans/' + str(plan['id']) + '/close', {
                'expected_plan_sha256': plan['plan_sha256'], 'reason': 'Standing approval: expiry; retain issued R2 holds and replan never-issued remainder.'})
            require(closed['mode'] == 'retired', 'close was not confirmed')
        self.save('batch_complete', current_plan=None, started_plan_sha256=None)
        self.progress()

    def monitor(self, pid):
        while True:
            p = self.plan(pid)
            state = execution_state(p)
            if state != 'running':
                self.finish(p)
                return
            active = self.active()
            require(active['plans'] == [pid] and active['batches'] == 0, 'concurrent cleanup execution')
            require(self.live()['commit']['id'] == self.status['live_commit'], 'live commit changed during execution')
            # STOP/SIGTERM drains the active plan naturally; never pause it for deployment.
            self.progress()
            time.sleep(60)

    def run(self):
        # Owner policy: OFF is the first barrier unless resuming our healthy execution.
        active = self.active()
        pid = self.status['current_plan']
        if not active['plans']:
            self.master(False, emergency=True)
            require(active['batches'] == 0, 'another batch is enabled')
        else:
            require(active['plans'] == [pid] and active['batches'] == 0,
                    'unknown enabled plan or batch; owner review required')
            require(not self.dry_run and not self.status.get('last_error'),
                    'enabled plan cannot be resumed by this runner')
            p = self.plan(pid)
            require(p['mode'] == 'enabled' and execution_state(p) in {'running', 'done'}
                    and self.status.get('started_plan_sha256') == p['plan_sha256']
                    and timestamp(p['approved_until']) > utcnow()
                    and p['master_deletion_enabled'] is True,
                    'saved enabled plan is unhealthy; owner review required')
        require(not self.status.get('last_error'), 'failure is latched; owner must review before restart')
        self.load_policy()
        self.check_delivery()
        if active['plans']:
            self.monitor(pid)
        elif pid:
            p = self.plan(pid)
            require(p['mode'] != 'enabled', 'plan became enabled during startup')
            if p['mode'] == 'paused':
                require(not self.dry_run, 'dry-run cannot reconcile an interrupted execution')
                self.finish(p)
        self.idle()
        self.progress()  # Accepted owner delivery before any deletion is enabled.
        while not self.stopped():
            pid = self.status['current_plan'] or self.pick()
            if pid is None:
                self.save('stopped' if self.stopped() else 'complete', current_plan=None)
                return
            self.save('dry_run', current_plan=pid)
            p = self.await_dry(pid)
            if p is None:
                break
            size = verify(p, self.catalog(p), self.windows, utcnow())
            if not budget_permits(self.recent_bytes(), size):
                self.save('waiting_for_budget')
                self.progress()
                time.sleep(60)
                continue
            if self.dry_run:
                self.save('dry_run_pass', proposed_bytes=size)
                return
            if self.stopped():
                break
            self.idle()
            self.master(True)
            if self.stopped():
                self.master(False)
                break
            # Recheck after the redeploy/budget wait; observations must still be fresh.
            p = self.plan(pid)
            verify(p, self.catalog(p), self.windows, utcnow())
            require(budget_permits(self.recent_bytes(), size), 'budget changed before enable')
            self.idle()
            self.save('enabling', current_plan=pid, started_plan_sha256=p['plan_sha256'])
            if self.stopped():
                self.master(False)
                break
            self.api('POST', 'plans/' + str(pid) + '/control', {'mode': 'enabled',
                'plan_sha256': p['plan_sha256'], 'approval_hours': 24,
                'allow_long_approval': True, 'reason': REASON})
            self.monitor(pid)
        self.save('stopped')

    def fail(self, error):
        self.status.update(phase='failed', last_error=error)
        self.master(False, emergency=True)  # Retries with alerts until OFF is confirmed.
        self.alert('STOPPED: owner review required')
        try:
            if self.status['current_plan']:
                self.recovery(self.plan(self.status['current_plan']))
        except Exception:
            self.status['shutdown_or_recovery_failed'] = True
            self.alert('Recovery verification failed; owner review required')
        # Independent best-effort writes: neither can prevent OFF or owner notification.
        try:
            self.save('failed')
        except Exception:
            critical('failure state write failed')
        try:
            (self.root / 'STOP').touch(mode=0o600)
        except Exception:
            critical('STOP write failed')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--repo', type=Path, default=Path.home() / 'Build/stoarama')
    ap.add_argument('--state', type=Path, default=Path.home() / 'stoarama-cleanup-runner')
    ap.add_argument('--dry-run', action='store_true', help='prepare/verify only; still forces master OFF and sends daily notes')
    args = ap.parse_args()
    os.umask(0o077)
    runner = Runner(args.repo, args.state, args.dry_run)
    try:
        runner.initialize()
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: setattr(runner, 'stopping', True))
        runner.run()
        return 0
    except LockHeld:
        return 0  # Another process owns the lock; never touch its master switch.
    except Exception as exc:
        message = str(exc) if isinstance(exc, Halt) else 'unexpected runner failure; inspect code privately'
        runner.fail(runner.status.get('last_error') or message)
        return 1
    finally:
        if runner.lock is not None:
            runner.lock.close()


if __name__ == '__main__':
    sys.exit(main())
