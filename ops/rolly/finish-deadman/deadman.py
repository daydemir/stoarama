#!/usr/bin/env python3
"""Independent Rolly dead-man check for the Jelly finish tick.

Reads the tick's published status over SSH and alerts Deniz when the tick stops
or stops making progress. Read-only: it never touches Jelly state.
"""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile

SCHEMA = 'stoarama.finish-status.v1'
OK = 'ok'
UNREACHABLE = 'unreachable'
MISSING = 'missing'
MALFORMED = 'malformed'
FUTURE = 'future'
STALE = 'stale'
STOPPED = 'stopped'
CONDITIONS = (OK, UNREACHABLE, MISSING, MALFORMED, FUTURE, STALE, STOPPED)
# Closed set published by the tick; anything else is reported as unknown:<value>.
STOP_REASONS = ('evidence_unavailable', 'cleanup_recovery_problems', 'recovery_evidence_stale',
                'unknown_enabled_cleanup', 'copy_mismatch_latched', 'shutdown_or_recovery_failed',
                'deletion_master_mismatch', 'database_unavailable', 'manifest_import_failed',
                'tick_already_running', 'status_write_failed')
MALFORMED_JSON = 'not_json_object'
MALFORMED_SCHEMA = 'schema'
MALFORMED_UPDATED_AT = 'updated_at'
MALFORMED_STOP = 'stop_reason_type'
FUTURE_ALLOWANCE_SECONDS = 30
TIME = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})')


def parse_time(value):
    # Go RFC3339Nano uses 1..9 fractional digits; Python 3.9 accepts only 3/6.
    # Normalize a comparison copy only.
    match = TIME.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise ValueError('invalid time')
    base, fraction, zone = match.groups()
    normalized = base + ('.' + fraction.ljust(6, '0')[:6] if fraction else '') + zone.replace('Z', '+00:00')
    return datetime.fromisoformat(normalized)


def format_time(moment):
    return moment.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def absolute(value):
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError('must be absolute')
    return path


def moment(value):
    try:
        return parse_time(value)
    except ValueError:
        raise argparse.ArgumentTypeError('must be an RFC3339 timestamp')


def count(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return number


def fetch(target, status_path, ssh_timeout):
    """Return (condition, payload) for transport outcomes, else (None, payload)."""
    command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', target,
               'cat -- ' + shlex.quote(str(status_path))]
    try:
        result = subprocess.run(command, capture_output=True, timeout=ssh_timeout)
    except (OSError, subprocess.TimeoutExpired):
        # stderr stays unreported: it can carry host and path detail.
        return UNREACHABLE, b''
    if result.returncode == 255:
        return UNREACHABLE, b''
    if result.returncode:
        return MISSING, b''
    return None, result.stdout


def classify(payload, now, max_age):
    try:
        record = json.loads(payload)
    except ValueError:
        return MALFORMED, MALFORMED_JSON
    if not isinstance(record, dict):
        return MALFORMED, MALFORMED_JSON
    if record.get('schema') != SCHEMA:
        return MALFORMED, MALFORMED_SCHEMA
    try:
        updated = parse_time(record.get('updated_at'))
    except ValueError:
        return MALFORMED, MALFORMED_UPDATED_AT
    if updated - now > timedelta(seconds=FUTURE_ALLOWANCE_SECONDS):
        return FUTURE, ''
    if 'stop_reason' in record:
        value = record['stop_reason']
        if not isinstance(value, str):
            return MALFORMED, MALFORMED_STOP
        return STOPPED, value if value in STOP_REASONS else 'unknown:' + value
    if (now - updated).total_seconds() > max_age:
        return STALE, ''
    return OK, ''


def read_state(path):
    try:
        record = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return record if isinstance(record, dict) else {}


def write_state(path, record):
    with tempfile.NamedTemporaryFile('w', dir=str(path.parent), prefix='.deadman-', delete=False) as output:
        temp = Path(output.name)
        try:
            json.dump(record, output)
            output.flush()
            os.fsync(output.fileno())
            output.close()
            os.replace(str(temp), str(path))
        finally:
            temp.unlink(missing_ok=True)
    directory = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def notify(agent, message):
    try:
        subprocess.run(['darma', 'msg', 'send', agent, message],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=True)
    except (OSError, subprocess.SubprocessError):
        return False
    return True


def alert_text(condition, detail, since):
    return ('Stoarama finish dead-man: ' + condition + (' (' + detail + ')' if detail else '')
            + ' since ' + since + '; check Jelly finish status.')


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--ssh-target', required=True)
    ap.add_argument('--status-path', required=True, type=absolute, help='absolute status.json path on the remote host')
    ap.add_argument('--state', required=True, type=Path, help='existing local state directory')
    ap.add_argument('--notify-agent', required=True)
    ap.add_argument('--max-age-seconds', type=count, default=900)
    ap.add_argument('--remind-seconds', type=count, default=21600)
    ap.add_argument('--now', type=moment, help='current UTC time override for tests')
    ap.add_argument('--ssh-timeout', type=count, default=40, help=argparse.SUPPRESS)
    args = ap.parse_args()

    if not args.state.is_dir():
        print('deadman state_unavailable', flush=True)
        return 2

    now = args.now if args.now is not None else datetime.now(timezone.utc)
    state_path = args.state / 'deadman.json'
    previous = read_state(state_path)
    previous_condition = previous.get('condition')
    if previous_condition not in CONDITIONS:
        # An unreadable or foreign state file must not suppress an alert.
        previous_condition = None
    previous_detail = previous.get('detail')
    if not isinstance(previous_detail, str):
        previous_detail = ''
    unreachable_runs = previous.get('unreachable_runs')
    if type(unreachable_runs) is not int:
        unreachable_runs = 0
    last_alert = previous.get('last_alert_at')
    try:
        last_alert = parse_time(last_alert)
    except ValueError:
        last_alert = None
    since = previous.get('since')
    try:
        since = parse_time(since)
    except ValueError:
        since = None

    condition, payload = fetch(args.ssh_target, args.status_path, args.ssh_timeout)
    if condition is None:
        condition, detail = classify(payload, now, args.max_age_seconds)
    else:
        detail = ''
    unreachable_runs = unreachable_runs + 1 if condition == UNREACHABLE else 0

    changed = previous_condition != condition or previous_detail != detail
    previous_alerted = last_alert is not None
    if changed:
        # The alert timestamp belongs to the current condition only: a new
        # condition must not inherit the previous one's reminder window.
        last_alert = None
    if changed or since is None:
        since = now

    message = None
    if condition == OK:
        if previous_condition not in (None, OK) and previous_alerted:
            message = 'Stoarama finish dead-man: recovered (tick fresh) after ' + previous_condition + '.'
    elif changed or last_alert is None:
        # Also covers retrying a same-condition alert whose notification failed,
        # and the second consecutive unreachable run.
        if condition != UNREACHABLE or unreachable_runs >= 2:
            message = alert_text(condition, detail, format_time(since))
    elif last_alert is not None and (now - last_alert).total_seconds() >= args.remind_seconds:
        message = alert_text(condition, detail, format_time(since))

    failed = False
    if message is not None:
        failed = not notify(args.notify_agent, message)
        if not failed:
            last_alert = now

    # An OK run with a failed recovery notice keeps the previous non-OK record so
    # the next run retries the recovery message instead of going quiet.
    if condition != OK or not failed:
        try:
            write_state(state_path, {
                'condition': condition,
                'detail': detail,
                'since': format_time(since),
                'last_alert_at': None if condition == OK or last_alert is None else format_time(last_alert),
                'unreachable_runs': unreachable_runs,
            })
        except OSError:
            print('deadman state_unavailable', flush=True)
            return 2

    if failed:
        print('deadman notification_failed', flush=True)
        return 2
    print('deadman ' + condition + (' ' + detail if detail else ''), flush=True)
    return 0 if condition == OK else 1


if __name__ == '__main__':
    raise SystemExit(main())
