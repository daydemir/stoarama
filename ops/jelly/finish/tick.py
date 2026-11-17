#!/usr/bin/env python3
"""Controller-installed metadata feeder. Never controls cleanup or deletion."""
import argparse
from contextlib import ExitStack
import fcntl
import json
import os
from pathlib import Path
import subprocess
import tempfile

STOPS = {'evidence_unavailable', 'cleanup_recovery_problems', 'unknown_enabled_cleanup',
         'copy_mismatch_latched', 'shutdown_or_recovery_failed', 'deletion_master_mismatch',
         'database_unavailable', 'manifest_import_failed', 'tick_already_running', 'listing_size_conflict'}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--cli', required=True, type=Path)
    ap.add_argument('--state', required=True, type=Path)
    ap.add_argument('--safety-collector', required=True, type=Path,
                    help='controller executable; emits fresh safety JSON on stdout')
    ap.add_argument('--notify-agent', required=True, help='darma msg target configured by controller')
    args = ap.parse_args()
    reason = 'dependency_failed'
    try:
        with ExitStack() as stack:
            args.state.mkdir(mode=0o700, parents=True, exist_ok=True)
            lock = stack.enter_context((args.state / 'lock').open('a'))
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 0
            # State and lock initialization failures also reach the notifier.
            temp = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix='.tick-', dir=args.state)))
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
            result = subprocess.run([str(args.cli), 'finish', 'tick', '--safety', str(safety),
                                     '--copy-status', str(copy), '--status', str(status)],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=270)
            reason = 'current_status_unavailable'
            # This path did not exist before THIS invocation. Never report an old
            # reason if CLI startup/status publication failed. Same-filesystem
            # rename publishes the invocation's status atomically for the dead-man.
            current = json.loads(status.read_text())
            if result.returncode and current.get('stop_reason') in STOPS:
                reason = current['stop_reason']
            os.replace(status, args.state / 'status.json')
            if result.returncode == 0:
                return 0
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    message = 'Stoarama finish ' + reason + '; inspect finish status and controller safety evidence.'
    try:
        subprocess.run(['darma', 'msg', 'send', args.notify_agent, message],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=True)
    except (OSError, subprocess.SubprocessError):
        print('finish notification_failed', flush=True)
    print('finish ' + reason, flush=True)
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
