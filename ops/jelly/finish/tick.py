#!/usr/bin/env python3
"""Controller-installed metadata feeder. Never controls cleanup or deletion."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--cli', required=True, type=Path)
    ap.add_argument('--state', required=True, type=Path)
    ap.add_argument('--safety-collector', required=True, type=Path,
                    help='controller executable; emits the fresh safety JSON on stdout')
    ap.add_argument('--notify-agent', required=True, help='darma msg target configured by controller')
    args = ap.parse_args()
    args.state.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (args.state / 'lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        # No environment file is loaded here. launchd/controller supplies auth.
        # Child stderr never enters logs/messages: dependency errors may contain URLs.
        reason = 'dependency_failed'
        try:
            with tempfile.TemporaryDirectory(prefix='finish-tick-') as temp:
                safety = Path(temp) / 'safety.json'
                copy = Path(temp) / 'copy.json'
                for command, path in (([str(args.safety_collector)], safety),
                                      ([str(args.cli), 'nas-r2-copy', 'status', '--json'], copy)):
                    with path.open('wb') as output:
                        os.chmod(path, 0o600)
                        subprocess.run(command, stdout=output, stderr=subprocess.DEVNULL,
                                       timeout=90, check=True)
                    if path.stat().st_size > 1024 * 1024:
                        raise ValueError('oversize evidence')
                result = subprocess.run([str(args.cli), 'finish', 'tick', '--safety', str(safety),
                                         '--copy-status', str(copy), '--status', str(args.state / 'status.json')],
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=270)
                if result.returncode == 0:
                    return 0
                reason = 'tick_stopped'
                status = json.loads((args.state / 'status.json').read_text())
                allowed = {'evidence_unavailable', 'cleanup_recovery_problems', 'unknown_enabled_cleanup',
                           'copy_mismatch_latched', 'shutdown_or_recovery_failed', 'deletion_master_mismatch',
                           'database_unavailable', 'manifest_import_failed', 'tick_already_running'}
                if status.get('stop_reason') in allowed:
                    reason = status['stop_reason']
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
        # Keep the last successful CLI timestamp on dependency failure so the
        # separate dead-man check can detect stale status. Alert contains only
        # fixed text, never untrusted errors or credential-bearing subprocess IO.
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
