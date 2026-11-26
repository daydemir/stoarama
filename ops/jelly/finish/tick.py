#!/usr/bin/env python3
"""Controller-installed metadata feeder. Never controls cleanup or deletion."""
import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
import re
import threading
from pathlib import Path
import subprocess
from admission import TICK_HOLD, TRANSIENT_EXIT, Attempt, Latched, publish, transient_record
from cli_binding import bound_launcher

STOPS = {'evidence_unavailable', 'cleanup_recovery_problems', 'unknown_enabled_cleanup',
         'copy_mismatch_latched', 'shutdown_or_recovery_failed', 'deletion_master_mismatch',
         'database_unavailable', 'recovery_evidence_stale', 'manifest_import_failed', 'tick_already_running'}


STDERR_TAIL_BYTES = 8192
# A continuous pure-network hold longer than this latches (fail closed).
HOLD_DEADLINE_SECONDS = 2 * 3600


def hold_expired(path, now):
    """Start the continuous hold, or report whether it outlived the deadline.
    A start in the future (clock stepped back) also expires."""
    if not path.exists():
        publish(path, {'since': now.isoformat()})
        return False
    age = (now - datetime.fromisoformat(json.loads(path.read_text())['since'])).total_seconds()
    return not 0 <= age <= HOLD_DEADLINE_SECONDS


def run_cli(command, attempt, timeout=270):
    """Drain stderr without unbounded memory/disk; retain even on timeout."""
    tail = bytearray()
    truncated = False
    def drain(pipe):
        nonlocal truncated
        with pipe:
            while True:
                chunk = pipe.read(4096)
                if not chunk:
                    return
                tail.extend(chunk)
                if len(tail) > STDERR_TAIL_BYTES:
                    truncated = True
                    del tail[:-STDERR_TAIL_BYTES]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    reader = threading.Thread(target=drain, args=(process.stderr,), daemon=True)
    reader.start()
    try:
        return process.wait(timeout=timeout)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        reader.join()
        # Drop a partial first line so truncation cannot leave a URL credential
        # without its scheme. Redact URLs and known environment secrets locally.
        raw = bytes(tail)
        if truncated:
            raw = raw.partition(b'\n')[2]
        text = raw.decode('utf-8', errors='replace')
        text = re.sub(r'(?i)\b(?:postgres(?:ql)?|https?)://\S+', '[redacted-url]', text)
        for key, value in os.environ.items():
            if value and len(value) >= 4 and any(x in key.upper() for x in ('TOKEN', 'PASSWORD', 'SECRET', 'DATABASE_URL', 'API_KEY')):
                text = text.replace(value, '[redacted]')
        text = text.encode('utf-8')[-STDERR_TAIL_BYTES:].decode('utf-8', errors='ignore')
        publish(attempt / 'cli-stderr.json', {'tail': text, 'limit_bytes': STDERR_TAIL_BYTES,
                                             'truncated': truncated})


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--cli', required=True, type=Path)
    ap.add_argument('--state', required=True, type=Path)
    ap.add_argument('--safety-collector', required=True, type=Path,
                    help='controller executable; emits fresh safety JSON on stdout')
    ap.add_argument('--notify-agent', required=True, help='darma msg target configured by controller')
    ap.add_argument('--refresh-picks', type=Path, help='optional pinned B2 picks for guarded verify-only refresh')
    ap.add_argument('--expected-refresh-picks-sha256')
    args = ap.parse_args()
    if bool(args.refresh_picks) != bool(args.expected_refresh_picks_sha256):
        ap.error('refresh picks and digest must be supplied together')
    reason = 'dependency_failed'
    attempt = None
    with ExitStack() as stack:
        try:
            args.state.mkdir(mode=0o700, parents=True, exist_ok=True)
            attempt = Attempt.start(args.state, 'tick', stack)
            args.cli = bound_launcher(args.cli)
            temp = attempt.path
            safety, copy, status = (temp / name for name in ('safety.json', 'copy.json', 'status.json'))
            # No env files are loaded. Controller supplies auth; dependency stderr
            # is discarded because it may contain credential-bearing URLs.
            for command, path in (([str(args.safety_collector)], safety),
                                  ([str(args.cli), 'nas-r2-copy', 'status', '--json'], copy)):
                with path.open('wb') as output:
                    os.chmod(path, 0o600)
                    result = subprocess.run(command, stdout=output, stderr=subprocess.DEVNULL, timeout=90)
                if path.stat().st_size > 1024 * 1024:
                    raise ValueError('oversize evidence')
                if path == safety and result.returncode == TRANSIENT_EXIT:
                    # The collector obtained no data: nothing to feed or judge.
                    kind = transient_record(json.loads(safety.read_bytes()), 'transient')
                    reason = 'evidence_unavailable'
                    if hold_expired(args.state / TICK_HOLD, datetime.now(timezone.utc)):
                        (args.state / TICK_HOLD).unlink()  # The latch ends this episode.
                        raise ValueError('transient hold deadline passed')
                    if attempt.finish('', {}, held=True):
                        print('finish transient_hold ' + kind.value, flush=True)
                        return TRANSIENT_EXIT
                    notify(args.notify_agent, reason)
                    return 1
                if path == safety:
                    # Any non-transient collector outcome ends the hold episode.
                    (args.state / TICK_HOLD).unlink(missing_ok=True)
                result.check_returncode()
            command = [str(args.cli), 'finish', 'tick', '--safety', str(safety),
                       '--copy-status', str(copy), '--status', str(status)]
            if args.refresh_picks is not None:
                command += ['--refresh-picks', str(args.refresh_picks),
                            '--expected-refresh-picks-sha256', args.expected_refresh_picks_sha256]
            returncode = run_cli(command, attempt.path)
            publish(attempt.path / 'result.json', {'returncode': returncode})
            reason = 'current_status_unavailable'
            # This path did not exist before THIS invocation. Never report an old
            # reason if CLI startup/status publication failed. Same-filesystem
            # rename publishes the invocation's status atomically for the dead-man.
            current = json.loads(status.read_text())
            if not isinstance(current, dict):
                raise ValueError('invalid status')
            if current.get('stop_reason') in STOPS:
                reason = current['stop_reason']
            elif returncode or current.get('stop_reason'):
                reason = 'manifest_import_failed'
            else:
                if current.get('schema') != 'stoarama.finish-status.v1' or not isinstance(current.get('updated_at'), str):
                    raise ValueError('invalid status schema/time')
                reason = ''
            if attempt.finish(reason, current):
                return 0
            reason = reason or 'evidence_unavailable'
            notify(args.notify_agent, reason)
            return 1
        except Latched:
            if attempt is not None and attempt.abandoned:
                notify(args.notify_agent, 'admission_publication_failed')
            print('finish admission_latched', flush=True)
            return 1
        except (OSError, RuntimeError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            reason = reason or 'current_status_unavailable'
            if attempt is not None:
                try:
                    attempt.finish(reason, {})
                except (OSError, ValueError, Latched):
                    # The pre-subprocess active journal remains a durable stop.
                    pass
    notify(args.notify_agent, reason)
    return 1

def notify(agent, reason):
    message = 'Stoarama finish ' + reason + '; inspect finish status and controller safety evidence.'
    try:
        subprocess.run(['darma', 'msg', 'send', agent, message],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=True)
    except (OSError, subprocess.SubprocessError):
        print('finish notification_failed', flush=True)
    print('finish ' + reason, flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
