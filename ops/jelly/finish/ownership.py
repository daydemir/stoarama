"""Read-only ownership proof shared by collection, plan selection and monitoring."""
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.request

from collector import timestamp, fresh, count, require, PLAN, PLAN_SHA, CLOSE_SHA

POLICY_SHA = '060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def validate_stop(raw, owner, now):
    stop = json.loads(raw)
    require(stop['schema'] == 'stoarama.cleanup-stop.v1'
            and stop['owner_id'] == owner['owner_id']
            and stop['reason'] == 'natural_boundary'
            and timestamp(stop['created_at']) <= now)


def owner_plan(status, owner):
    pid = status['current_plan'] or status.get('next_plan') or status['recovery_plan_id']
    require(type(pid) is int and (pid >= owner['initial_plan_id'] or pid == owner['baseline_plan_id']))
    return pid


def assemble_owned(status, owner, recovery, runner_report, api, db, now, running, selecting=False):
    require(owner['schema'] == 'stoarama.cleanup-owner.v1'
            and re.fullmatch('[a-f0-9-]{36}', owner['owner_id'])
            and status['owner_id'] == owner['owner_id'])
    require(owner['baseline_plan_id'] == PLAN and owner['baseline_plan_sha256'] == PLAN_SHA
            and owner['policy_sha256'] == POLICY_SHA)
    require(status['last_error'] is None and status['recovery_error'] is None
            and status['shutdown_or_recovery_failed'] is False)
    # A stopped owner is valid only at an idle, execution-free boundary. While
    # alive it owns the exclusive runner flock and publishes fresh progress.
    if running:
        fresh(status['updated_at'], now)
    else:
        require(status['phase'] in ('stopped', 'complete') and db['enabled_plans'] == [])
        if status['current_plan'] is not None:
            pending = db['plans'][str(status['current_plan'])]
            require(pending['mode'] == 'dry_run' and pending['issued'] == 0)
    pid = owner_plan(status, owner)
    plan = db['plans'][str(pid)]
    checkpoint = status['recovery_plan_id']
    settled = db['plans'][str(checkpoint)]
    require(api['id'] == pid and api['plan_sha256'] == plan['sha256']
            and api['master_deletion_enabled'] is True)
    require(plan['connection_id'] == 13 and plan['account_id'] == 47
            and plan['policy_sha256'] == owner['policy_sha256'] and not plan['allow_nas_only'])
    if checkpoint == PLAN:
        require(plan['sha256'] == owner['initial_plan_sha256'] and pid == owner['initial_plan_id'])
    if db['enabled_plans']:
        require(running and status['current_plan'] == pid
                and status['started_plan_sha256'] == plan['sha256'])
    require(db['enabled_plans'] in ([], [pid]) and db['enabled_batches'] == [])
    require(plan['mode'] in ('dry_run', 'enabled', 'paused', 'retired'))
    require(plan['uncertain'] == 0 and db['inventory_mismatches'] == 0
            and db['replay_failed'] == 0 and db['replay_pending'] == 0)
    require(db['version'] == 'r2-copy-v6' and db['boot'])
    fresh(db['cleanup_at'], now)
    local = db['r2_copy']
    require(count(local['mismatch_latched']) == 0)
    require(all(isinstance(k, str) and count(v) == v for k, v in local['holds_by_reason'].items()))
    if selecting:
        # Selection is not feeding evidence: after explicit D reconciliation it
        # identifies the DB-bound checkpoint so the real verifier can repair a
        # failed/missing/expired report. It never admits a tick or clears a latch.
        require(settled['sha256'] == status['recovery_history'][str(checkpoint)]['sha256']
                and settled['issued'] == status['recovery_history'][str(checkpoint)]['issued'])
        return {'recovery_plan_id': checkpoint}
    # A D failure must dominate even when the runner has a newer success.
    require('refresh_error' not in recovery and recovery['problems'] == 0
            and recovery['verified'] == recovery['delete_ever_issued'])
    history = status['recovery_history']
    prior = history[str(recovery['plan_id'])]
    require(prior['sha256'] == recovery['plan_sha256']
            and prior['issued'] == recovery['delete_ever_issued'])
    fresh(recovery['checked_at'], now, 7 * 3600)
    require(timestamp(recovery['checked_at']) > timestamp(db['plans'][str(recovery['plan_id'])]['newest_issued_at']))
    report = recovery if recovery['plan_id'] == checkpoint else runner_report
    require(report['plan_id'] == checkpoint and report['plan_sha256'] == settled['sha256']
            and report['problems'] == 0
            and count(report['delete_ever_issued']) == report['verified'] == settled['issued'])
    require(checkpoint == status['recovery_plan_id'] and settled['mode'] in ('retired', 'paused', 'enabled'))
    require(timestamp(report['checked_at']) > timestamp(settled['newest_issued_at']))
    fresh(report['checked_at'], now, 7 * 3600)
    return dict(updated_at=now.isoformat(), recovery_problems=0,
                recovery_checked_at=report['checked_at'], recovery_issued_items=report['delete_ever_issued'],
                recovery_verified_items=report['verified'], recovery_plan_id=checkpoint,
                recovery_plan_sha256=report['plan_sha256'], shutdown_or_recovery_failed=False,
                recovery_error=None, last_error=None, controller_held_error=None,
                local_copy_mismatch_latched=0, local_copy_checked_at=db['cleanup_at'],
                master_checked_at=now.isoformat(), known_enabled_plans=[pid], known_enabled_batches=[],
                master_deletion_enabled=api['master_deletion_enabled'], expected_deletion_enabled=True,
                cleanup_owner=dict(plan_id=pid, plan_sha256=plan['sha256']),
                holds_by_reason=local['holds_by_reason'])


def collect(root, repo, recovery_path, owner_sha256, now, plan_source=False):
    raw = (root / 'cleanup-owner.json').read_bytes()
    require(re.fullmatch('[a-f0-9]{64}', owner_sha256) and digest(raw) == owner_sha256)
    owner = json.loads(raw)
    require(owner['finish_state'] == str(recovery_path.parent))
    require(digest((root / 'incident-recovery-1440-20261117/closed-verified.json').read_bytes()) == CLOSE_SHA)
    with (root / 'ownership.lock').open('a') as snapshot:
        fcntl.flock(snapshot, fcntl.LOCK_SH)
        status = json.loads((root / 'status.json').read_bytes())
        try:
            stop = (root / 'STOP').read_bytes()
        except FileNotFoundError:
            stop = None
        if stop is not None:
            validate_stop(stop, owner, datetime.now(timezone.utc))
        with (root / 'lock').open('a') as execution:
            try:
                fcntl.flock(execution, fcntl.LOCK_EX | fcntl.LOCK_NB)
                running = False
            except BlockingIOError:
                running = True
        if running:
            require(type(status['runner_pid']) is int and status['runner_pid'] > 0)
            command = subprocess.run(['/bin/ps', '-p', str(status['runner_pid']), '-o', 'command='],
                                     capture_output=True, text=True, timeout=5, check=True).stdout
            require(str(repo / 'ops/jelly/cleanup-runner/runner.py') in command)
        admission = json.loads((recovery_path.parent / 'admission.json').read_bytes())
        require(admission['failed'] is False and (plan_source or admission['recovery_required'] is False))
        recovery = None if plan_source else json.loads(recovery_path.read_bytes())
        checkpoint = status['recovery_plan_id']
        runner_report = None if plan_source else json.loads((root / ('recovery-' + str(checkpoint) + '.json')).read_bytes())
        pid = owner_plan(status, owner)
        ids = sorted(set([pid, checkpoint] + [int(k) for k in status['recovery_history']]))
        require(all(type(i) is int and i > 0 for i in ids))
        req = urllib.request.Request('https://stoarama-api.onrender.com/api/v1/service/nas-cleanup/plans/' + str(pid),
                headers={'Authorization': 'Bearer ' + os.environ['SERVICE_TOKEN'], 'User-Agent': 'stoarama-operator/1'})
        with urllib.request.urlopen(req, timeout=15) as response:
            raw = response.read(32 * 1024 * 1024 + 1)
        require(len(raw) <= 32 * 1024 * 1024)
        api = json.loads(raw)
        import sys
        sys.path.insert(0, str(repo / 'backend/scripts/nas-cleanup'))
        from prepare_keep28_plans import pg_env
        query = """BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY;
SET LOCAL ROLE stoarama; SET LOCAL statement_timeout='10s'; SET LOCAL lock_timeout='2s';
SET LOCAL work_mem='16MB'; SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off;
DO $$BEGIN IF session_user<>'stoarama_20260821' OR current_user<>'stoarama' OR current_database()<>'stoarama'
THEN RAISE EXCEPTION 'identity'; END IF; END$$;
SELECT jsonb_build_object(
 'enabled_plans',(SELECT coalesce(jsonb_agg(id ORDER BY id),'[]') FROM nas_cleanup_plans WHERE mode='enabled'),
 'enabled_batches',(SELECT coalesce(jsonb_agg(id ORDER BY id),'[]') FROM nas_cleanup_batches WHERE mode='enabled'),
 'plans',(SELECT jsonb_object_agg(p.id,jsonb_build_object('sha256',p.plan_sha256,'mode',p.mode,
 'connection_id',p.connection_id,'account_id',p.account_id,'policy_sha256',p.report_sha256,'allow_nas_only',p.allow_nas_only,
 'issued',(SELECT count(*) FROM nas_cleanup_items WHERE plan_id=p.id AND delete_ever_issued),
 'newest_issued_at',(SELECT coalesce(max(updated_at),timestamptz '1970-01-01') FROM nas_cleanup_items WHERE plan_id=p.id AND delete_ever_issued),
 'uncertain',(SELECT count(*) FROM nas_cleanup_items WHERE plan_id=p.id AND
 (state IN ('unverified','conflict','missing','media_deleted_sidecar_failed') OR outcome ? 'contradictory_receipt'))))
 FROM nas_cleanup_plans p WHERE p.id IN (PLAN_IDS)),
 'version',client_version,'boot',client_boot_id,'cleanup_at',nas_cleanup_status_at,
 'inventory_mismatches',inventory_mismatches,'replay_failed',(nas_cleanup_status->>'replay_failed')::bigint,
 'replay_pending',(nas_cleanup_status->>'replay_pending')::bigint,'r2_copy',nas_cleanup_status->'r2_copy')
 FROM connections WHERE id=13; ROLLBACK;""".replace('PLAN_IDS', ','.join(str(i) for i in ids))
        result = subprocess.run(['psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=sqlstate'],
                                input=query, env=pg_env(), capture_output=True, text=True, timeout=25)
        require(result.returncode == 0)
        db = json.loads(result.stdout)
        for old_id, old in status['recovery_history'].items():
            fact = db['plans'][old_id]
            require(fact['sha256'] == old['sha256'] and fact['issued'] == old['issued'])
            if int(old_id) != checkpoint:
                require(fact['mode'] == 'retired')
        return assemble_owned(status, owner, recovery, runner_report, api, db,
                              datetime.now(timezone.utc), running, selecting=plan_source)
