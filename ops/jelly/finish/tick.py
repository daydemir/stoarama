#!/usr/bin/env python3
"""Controller-installed metadata feeder. Never controls cleanup or deletion."""
import argparse
from contextlib import ExitStack
import fcntl
import json
import os
from pathlib import Path
import subprocess
from admission import Attempt, Latched, publish

STOPS = {'evidence_unavailable', 'cleanup_recovery_problems', 'unknown_enabled_cleanup',
         'copy_mismatch_latched', 'shutdown_or_recovery_failed', 'deletion_master_mismatch',
         'database_unavailable', 'recovery_evidence_stale', 'manifest_import_failed', 'tick_already_running'}


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
            lock = stack.enter_context((args.state / 'lock').open('a'))
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 1
            # State and lock initialization failures also reach the notifier.
            attempt = Attempt(args.state, 'tick')
            temp = attempt.path
            safety, copy, status = (temp / name for name in ('safety.json', 'copy.json', 'status.json'))
            # No env files are loaded. Controller supplies auth; dependency stderr
            # is discarded because it may contain credential-bearing URLs.
            for command, path in (([str(args.safety_collector)], safety),
                                  ([str(args.cli), 'nas-r2-copy', 'status', '--json'], copy)):
                with path.open('wb') as output:
                    os.chmod(path, 0o600)
                    subprocess.run(command, stdout=output, stderr=subprocess.DEVNULL,
                                   timeout=90, check=True)
                if path.stat().st_size > 1024 * 1024:
                    raise ValueError('oversize evidence')
            command = [str(args.cli), 'finish', 'tick', '--safety', str(safety),
                       '--copy-status', str(copy), '--status', str(status)]
            if args.refresh_picks is not None:
                command += ['--refresh-picks', str(args.refresh_picks),
                            '--expected-refresh-picks-sha256', args.expected_refresh_picks_sha256]
            result = subprocess.run(command,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=270)
            publish(attempt.path / 'result.json', {'returncode': result.returncode})
            reason = 'current_status_unavailable'
            # This path did not exist before THIS invocation. Never report an old
            # reason if CLI startup/status publication failed. Same-filesystem
            # rename publishes the invocation's status atomically for the dead-man.
            current = json.loads(status.read_text())
            if not isinstance(current, dict):
                raise ValueError('invalid status')
            if current.get('stop_reason') in STOPS:
                reason = current['stop_reason']
            elif result.returncode or current.get('stop_reason'):
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
            print('finish admission_latched', flush=True)
            return 1
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
            reason = reason or 'current_status_unavailable'
            if attempt is not None:
                try:
                    attempt.finish(reason, {})
                except (OSError, ValueError):
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
