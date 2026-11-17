#!/usr/bin/env python3
"""Run the canonical read-only verifier separately from the fast finish tick."""
import argparse
from contextlib import ExitStack
import fcntl
import json
import os
from pathlib import Path
import subprocess
import tempfile
from datetime import datetime
import re

from tick import notify

FIELDS = ('plan_id', 'plan_sha256', 'delete_ever_issued', 'verified', 'problems', 'checked_at')


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


def publish(path, record):
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as output:
        temp = Path(output.name)
        try:
            json.dump(record, output)
            output.flush()
            os.fsync(output.fileno())
            output.close()
            os.replace(temp, path)
        finally:
            temp.unlink(missing_ok=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--cli', required=True, type=Path)
    ap.add_argument('--state', required=True, type=Path)
    ap.add_argument('--plan-source', required=True, type=Path,
                    help='controller executable emitting {"plan_id": current/latest plan}')
    ap.add_argument('--notify-agent', required=True)
    args = ap.parse_args()
    reason = 'recovery_refresh_failed'
    try:
        with ExitStack() as stack:
            args.state.mkdir(parents=True, exist_ok=True, mode=0o700)
            lock = stack.enter_context((args.state / 'recovery.lock').open('a'))
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 0
            source = subprocess.run([str(args.plan_source)], capture_output=True, timeout=30, check=True)
            source_record=json.loads(source.stdout)
            if not isinstance(source_record,dict):
                raise ValueError('invalid plan source')
            plan = source_record['plan_id']
            if type(plan) is not int or plan <= 0:
                raise ValueError('invalid plan')
            # Keep potentially credential-bearing stderr and raw problem detail
            # out of logs and the published record. The CLI owns checked_at.
            with tempfile.TemporaryFile() as output:
                result = subprocess.run([str(args.cli), 'nas-cleanup', 'verify-recovery', '--plan-id', str(plan)],
                                        stdout=output, stderr=subprocess.DEVNULL, timeout=1800)
                output.seek(0)
                raw = output.read(2 * 1024 * 1024 + 1)
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
            publish(args.state / 'recovery.json', record)
            if passed:
                publish(args.state / 'recovery-last-good.json', record)
                return 0
            reason = 'cleanup_recovery_problems'
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        try:
            publish(args.state / 'recovery.json', {'refresh_error': reason})
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
