#!/usr/bin/env python3
"""Run the canonical read-only verifier separately from the fast finish tick."""
import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess
from datetime import datetime
import re
import time

from tick import notify
from admission import (REFRESH_HOLD, TRANSIENT_EXIT, Attempt, Latched, Transient, TransientFailure,
                       publish, transient_record)
from cli_binding import bound_launcher

FIELDS = ('plan_id', 'plan_sha256', 'delete_ever_issued', 'verified', 'problems', 'checked_at')
# Three plan-source tries over about ten minutes inside one launchd run.
RETRY_DELAYS = (120, 360)


def validate_checked_at(value):
    # Go RFC3339Nano uses 1..9 fractional digits; Python 3.9 accepts only 3/6.
    # Normalize only this validation copy. The published canonical string stays
    # byte-for-byte unchanged, including its original nanosecond precision.
    if not isinstance(value, str):
        raise ValueError('invalid check time')
    match = re.fullmatch(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})', value)
    if match is None:
        raise ValueError('invalid check time')
    base, fraction, zone = match.groups()
    normalized = base + ('.' + fraction.ljust(6, '0')[:6] if fraction else '') + zone.replace('Z', '+00:00')
    datetime.fromisoformat(normalized)


def select_plan(source):
    try:
        result = subprocess.run([str(source)], capture_output=True, timeout=30)
    except subprocess.TimeoutExpired:
        raise TransientFailure(Transient.TIMEOUT) from None
    if result.returncode == TRANSIENT_EXIT:
        raise TransientFailure(transient_record(json.loads(result.stdout), 'transient'))
    result.check_returncode()
    record = json.loads(result.stdout)
    if not isinstance(record, dict):
        raise ValueError('invalid plan source')
    plan = record['plan_id']
    if type(plan) is not int or plan <= 0:
        raise ValueError('invalid plan')
    return plan


def select_plan_retrying(source):
    for delay in RETRY_DELAYS:
        try:
            return select_plan(source)
        except TransientFailure:
            time.sleep(delay)
    return select_plan(source)


def held_before(state):
    """Whether the previous refresh already held, so this episode was notified."""
    try:
        return REFRESH_HOLD in json.loads((state / 'recovery.json').read_bytes())
    except (OSError, ValueError, TypeError):
        return False


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--cli', required=True, type=Path)
    ap.add_argument('--state', required=True, type=Path)
    ap.add_argument('--plan-source', required=True, type=Path,
                    help='controller executable emitting {"plan_id": current/latest plan}')
    ap.add_argument('--notify-agent', required=True)
    args = ap.parse_args()
    reason = 'recovery_refresh_failed'
    attempt = None
    with ExitStack() as stack:
        try:
            args.state.mkdir(parents=True, exist_ok=True, mode=0o700)
            attempt = Attempt.start(args.state, 'recovery', stack)
            args.cli = bound_launcher(args.cli)
            plan = select_plan_retrying(args.plan_source)
            publish(attempt.path / 'plan.json', {'plan_id': plan})
            # Keep potentially credential-bearing stderr and raw problem detail
            # out of logs and the published record. The CLI owns checked_at.
            with (attempt.path / 'verifier.json').open('w+b') as output:
                os.chmod(output.name, 0o600)
                try:
                    result = subprocess.run([str(args.cli), 'nas-cleanup', 'verify-recovery', '--plan-id', str(plan)],
                                            stdout=output, stderr=subprocess.DEVNULL, timeout=1800)
                except subprocess.TimeoutExpired:
                    raise TransientFailure(Transient.TIMEOUT) from None
                output.seek(0)
                raw = output.read(2 * 1024 * 1024 + 1)
            publish(attempt.path / 'result.json', {'returncode': result.returncode})
            if len(raw) > 2 * 1024 * 1024:
                raise ValueError('oversize report')
            record = json.loads(raw)
            if not isinstance(record,dict):
                raise ValueError('invalid report shape')
            record = {key: record[key] for key in FIELDS}
            for key in ('plan_id','delete_ever_issued','verified','problems'):
                if type(record[key]) is not int or record[key]<0:
                    raise ValueError('invalid report counts')
            if not isinstance(record['plan_sha256'],str) or not re.fullmatch('[a-f0-9]{64}',record['plan_sha256']):
                raise ValueError('invalid report digest')
            validate_checked_at(record['checked_at'])
            if record['plan_id'] != plan or not record['checked_at']:
                raise ValueError('wrong report')
            passed = (result.returncode == 0 and record['problems'] == 0
                      and record['verified'] == record['delete_ever_issued'])
            if not passed and record['problems'] == 0:
                raise ValueError('verifier failed without problem report')
            publish(attempt.path / 'report.json', record)
            reason = '' if passed else 'cleanup_recovery_problems'
            if attempt.finish(reason, record):
                return 0
            notify(args.notify_agent, reason or 'recovery_refresh_failed')
            return 1
        except TransientFailure as failure:
            # Hold without latching: admission stays unlatched, the old report is
            # no longer consumable, and the next scheduled or kicked run retries.
            line = 'recovery_transient_hold ' + failure.kind.value
            first = not held_before(args.state)
            try:
                if attempt.finish('', {}, held=failure.kind):
                    if first:
                        notify(args.notify_agent, line)
                    else:
                        print('finish ' + line, flush=True)
                    return TRANSIENT_EXIT
            except Latched:
                notify(args.notify_agent, 'admission_publication_failed')
                return 1
            notify(args.notify_agent, 'evidence_unavailable')
            return 1
        except Latched:
            if attempt is not None and attempt.abandoned:
                notify(args.notify_agent, 'admission_publication_failed')
            print('finish admission_latched', flush=True)
            return 1
        except (OSError, RuntimeError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            reason = reason or 'recovery_refresh_failed'
            try:
                if attempt is not None:
                    attempt.finish(reason, {})
                publish(args.state / 'recovery.json', {'refresh_error': reason})
            except Latched:
                notify(args.notify_agent, 'admission_publication_failed')
                return 1
            except OSError:
                # Unlink needs no replacement allocation. A full disk must not leave
                # the previous success consumable after a failed verification.
                try:
                    (args.state / 'recovery.json').unlink(missing_ok=True)
                except OSError:
                    reason = 'recovery_invalidation_failed'
    notify(args.notify_agent, reason)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
