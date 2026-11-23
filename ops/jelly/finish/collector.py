#!/usr/bin/env python3
"""Controller safety collector: strict held COPY mode or explicitly pinned cleanup ownership."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.request

PLAN = 1440
PLAN_SHA = '78b86990e0a45715deb9383226cff6ff5f01e6159f3d8eaf8b6d41bb39e38697'
HELD_ERROR = 'plan paused for a non-expiry reason'
CLOSE_SHA = '4c89d452abcb9f3ea4c9a138e8bf211b5a298b23ad204b265303de4bbedf3cb4'
NAS_CLIENT_VERSIONS = frozenset(('r2-copy-v6', 'r2-copy-v7-certified-stitch', 'r2-copy-v8-low-memory', 'r2-copy-v8-low-memory-cleanup'))
STOP = ("stoarama sole controller: intentional natural-boundary STOP\n"
        "operation=copy527-natural-boundary\n"
        "owner=1bd1e5c8-cde9-4155-affe-6ba81fd2ab16\n"
        "created_at=2026-11-17T07:06:33.347726+00:00\n"
        "wait_for_plan=1440\n"
        "deploy_commit=7a533130c9fe2ae260ea3c0b1c03e7ec51e48ff9\n")


def require(condition):
    if not condition:
        raise ValueError('evidence unavailable')


def timestamp(value):
    require(isinstance(value, str))
    # Python 3.9 accepts only 3/6 fractional digits; Go RFC3339Nano emits 1..9.
    # Truncate only this validation copy. The original canonical string is emitted.
    match = re.fullmatch(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})', value)
    require(match is not None)
    fraction = '.' + match[2][:6].ljust(6, '0') if match[2] else ''
    offset = '+00:00' if match[3] == 'Z' else match[3]
    result = datetime.fromisoformat(match[1] + fraction + offset)
    require(result.tzinfo is not None)
    return result


def count(value):
    require(type(value) is int and value >= 0)
    return value


def fresh(value, now, age=300):
    require(-30 <= (now - timestamp(value)).total_seconds() <= age)


def assemble(status, recovery, api, db, master_at, now):
    require(status['current_plan'] == PLAN and status['next_plan'] == 1441)
    require(api['id'] == PLAN and api['plan_sha256'] == PLAN_SHA and api['mode'] == 'retired')
    require(type(api['master_deletion_enabled']) is bool)
    require(db['plan_sha256'] == PLAN_SHA and db['enabled_plans'] == [] and db['enabled_batches'] == [])
    require(type(status['shutdown_or_recovery_failed']) is bool)
    require(status['recovery_error'] is None or isinstance(status['recovery_error'], str))
    require(status['last_error'] is None or isinstance(status['last_error'], str))
    require('refresh_error' not in recovery and recovery['plan_id'] == PLAN and recovery['plan_sha256'] == PLAN_SHA)
    issued = count(recovery['delete_ever_issued'])
    verified = count(recovery['verified'])
    problems = count(recovery['problems'])
    require(issued == db['issued'] == 1860)
    checked = timestamp(recovery['checked_at'])
    require(checked > timestamp(db['newest_issued_at']))
    fresh(recovery['checked_at'], now, 7 * 3600)
    require(db['version'] in NAS_CLIENT_VERSIONS and isinstance(db['boot'], str) and bool(db['boot']))
    fresh(db['cleanup_at'], now)
    local = db['r2_copy']
    require(isinstance(local, dict) and isinstance(local['holds_by_reason'], dict))
    mismatch = count(local['mismatch_latched'])
    holds = local['holds_by_reason']
    require(all(isinstance(k, str) and type(v) is int and v >= 0 for k, v in holds.items()))
    return dict(updated_at=now.isoformat(), recovery_problems=problems,
                recovery_checked_at=recovery['checked_at'], recovery_issued_items=issued,
                recovery_verified_items=verified, recovery_plan_id=PLAN, recovery_plan_sha256=PLAN_SHA,
                shutdown_or_recovery_failed=status['shutdown_or_recovery_failed'],
                recovery_error=status['recovery_error'], last_error=status['last_error'],
                controller_held_error=HELD_ERROR, local_copy_mismatch_latched=mismatch,
                local_copy_checked_at=db['cleanup_at'], master_checked_at=master_at,
                known_enabled_plans=[], known_enabled_batches=[],
                master_deletion_enabled=api['master_deletion_enabled'], expected_deletion_enabled=False,
                holds_by_reason=holds)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--recovery-report', type=Path)
    parser.add_argument('--plan-source', action='store_true')
    parser.add_argument('--owner-sha256')
    parser.add_argument('--state', type=Path, default=Path.home() / 'stoarama-finish-eae77803-v5-v3/state')
    args = parser.parse_args()
    require(args.plan_source != (args.recovery_report is not None))
    root = Path.home() / 'stoarama-cleanup-runner'
    repo = Path.home() / 'Build/stoarama/worktrees/stoarama-runner-3a0c3b23'
    if args.owner_sha256:
        from ownership import collect
        record = collect(root, repo, args.state / 'recovery.json', args.owner_sha256, datetime.now(timezone.utc), args.plan_source)
        print(json.dumps({'plan_id': record['recovery_plan_id']} if args.plan_source else record, sort_keys=True))
        return
    close = root / 'incident-recovery-1440-20261117/closed-verified.json'
    require(hashlib.sha256(close.read_bytes()).hexdigest() == CLOSE_SHA)
    require((root / 'STOP').read_text() == STOP)
    status_bytes = (root / 'status.json').read_bytes()
    status = json.loads(status_bytes)
    master_at = datetime.now(timezone.utc).isoformat()
    req = urllib.request.Request('https://stoarama-api.onrender.com/api/v1/service/nas-cleanup/plans/1440',
                                 headers={'Authorization': 'Bearer ' + os.environ['SERVICE_TOKEN'],
                                          'User-Agent': 'stoarama-operator/1'})
    with urllib.request.urlopen(req, timeout=15) as response:
        raw = response.read(32 * 1024 * 1024 + 1)
    require(len(raw) <= 32 * 1024 * 1024)
    api = json.loads(raw)
    sys.path.insert(0, str(repo / 'backend/scripts/nas-cleanup'))
    from prepare_keep28_plans import pg_env
    query = """BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY;
SET LOCAL ROLE stoarama;
SET LOCAL statement_timeout='10s';SET LOCAL lock_timeout='2s';SET LOCAL work_mem='16MB';
SET LOCAL max_parallel_workers_per_gather=0;SET LOCAL jit=off;
DO $$BEGIN IF session_user<>'stoarama_20260821' THEN RAISE EXCEPTION 'session identity';END IF;IF current_user<>'stoarama' THEN RAISE EXCEPTION 'role identity';END IF;IF current_database()<>'stoarama' THEN RAISE EXCEPTION 'database identity';END IF;END$$;
SELECT jsonb_build_object(
 'enabled_plans',(SELECT coalesce(jsonb_agg(id ORDER BY id),'[]') FROM nas_cleanup_plans WHERE mode='enabled'),
 'enabled_batches',(SELECT coalesce(jsonb_agg(id ORDER BY id),'[]') FROM nas_cleanup_batches WHERE mode='enabled'),
 'plan_sha256',(SELECT plan_sha256 FROM nas_cleanup_plans WHERE id=1440 AND connection_id=13),
 'issued',(SELECT count(*) FROM nas_cleanup_items WHERE plan_id=1440 AND delete_ever_issued),
 'newest_issued_at',(SELECT max(updated_at) FROM nas_cleanup_items WHERE plan_id=1440 AND delete_ever_issued),
 'version',client_version,'boot',client_boot_id,'cleanup_at',nas_cleanup_status_at,
 'r2_copy',nas_cleanup_status->'r2_copy') FROM connections WHERE id=13;
ROLLBACK;"""
    result = subprocess.run(['psql', '-XqAt', '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=sqlstate'],
                            input=query, env=pg_env(), capture_output=True, text=True, timeout=25)
    require(result.returncode == 0)
    db = json.loads(result.stdout)
    require((root / 'status.json').read_bytes() == status_bytes and (root / 'STOP').read_text() == STOP)
    if args.plan_source:
        require(status['current_plan'] == PLAN and status['next_plan'] == 1441)
        require(api['id'] == PLAN and api['plan_sha256'] == PLAN_SHA and api['mode'] == 'retired')
        require(api['master_deletion_enabled'] is False and db['enabled_plans'] == [] and db['enabled_batches'] == [])
        require(db['plan_sha256'] == PLAN_SHA and db['issued'] == 1860)
        require(status['recovery_error'] is None and status['shutdown_or_recovery_failed'] is False)
        require(status['last_error'] in (None, HELD_ERROR))
        print(json.dumps({'plan_id': PLAN}))
        return
    recovery = json.loads(args.recovery_report.read_bytes())
    record = assemble(status, recovery, api, db, master_at, datetime.now(timezone.utc))
    print(json.dumps(record, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('finish safety evidence unavailable', file=sys.stderr)
        raise SystemExit(1)
