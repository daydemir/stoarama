#!/usr/bin/env python3
"""External uptime probe for the public Global Street Scores delivery.

Each run GETs the delivery page and one metadata API response from outside
Render and records the result. Two consecutive failing runs alert through
`darma msg send <agent>`; an unchanged failure reminds every --remind-seconds,
and the first passing run after an alert sends one recovery. Exit 0 when up,
1 when down, 2 when the alert could not be sent (the next run retries).
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

PAGE = '/global-street-scores-delivery'
METADATA = '/api/v1/global-street-scores-delivery/metadata?path=%2Fpublic%2Fmetadata.json'


def check(origin, path, slow, is_metadata):
    """'ok', or why this endpoint failed: http_<code>, slow_<s>s, bad_json, unreachable."""
    start = time.monotonic()
    try:
        request = urllib.request.Request(origin + path, headers={'User-Agent': 'gss-uptime-probe'})
        with urllib.request.urlopen(request, timeout=max(30, 3 * slow)) as response:
            body = response.read()
    except urllib.error.HTTPError as e:
        return 'http_%d' % e.code
    except (OSError, ValueError):
        return 'unreachable'
    elapsed = time.monotonic() - start
    if is_metadata:
        try:
            if not isinstance(json.loads(body).get('streams'), list):
                return 'bad_json'
        except (ValueError, AttributeError):
            return 'bad_json'
    return 'slow_%ds' % elapsed if elapsed > slow else 'ok'


def write_state(path, record):
    with tempfile.NamedTemporaryFile('w', dir=str(path.parent), prefix='.probe-', delete=False) as output:
        json.dump(record, output)
    os.replace(output.name, str(path))


def notify(agent, message):
    try:
        subprocess.run(['darma', 'msg', 'send', agent, message],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=True)
    except (OSError, subprocess.SubprocessError):
        return False
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--state', required=True, type=Path, help='existing private state directory')
    ap.add_argument('--notify-agent', required=True, help='darma msg agent to alert')
    ap.add_argument('--origin', default='https://stoarama.com')
    ap.add_argument('--slow-seconds', type=float, default=10)
    ap.add_argument('--remind-seconds', type=int, default=6 * 3600)
    a = ap.parse_args()
    state_path = a.state / 'probe.json'
    try:
        previous = json.loads(state_path.read_text())
    except (OSError, ValueError):
        previous = {}
    now = int(time.time())
    results = {'page': check(a.origin, PAGE, a.slow_seconds, False), 'metadata': check(a.origin, METADATA, a.slow_seconds, True)}
    condition = ' '.join('%s=%s' % kv for kv in sorted(results.items()) if kv[1] != 'ok')
    stamp = datetime.datetime.fromtimestamp(now, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    alerted = previous.get('alerted') or {}
    record, message = {}, None
    if not condition:
        if alerted:
            message = 'GSS delivery recovered at %s (was down since %s: %s).' % (stamp, previous.get('since'), alerted.get('condition'))
    else:
        streak = previous.get('streak', 0) + 1
        record = {'since': previous.get('since') or stamp, 'streak': streak, 'alerted': alerted}
        due = streak >= 2 and (alerted.get('condition') != condition or now - alerted.get('at', 0) >= a.remind_seconds)
        if due:
            message = 'GSS delivery DOWN since %s: %s (%s). Researchers see this page; check Render stoarama-api.' % (record['since'], condition, a.origin + PAGE)
    if message:
        if not notify(a.notify_agent, message):
            write_state(state_path, record if condition else previous)
            print('probe %s notification_failed' % (condition or 'ok'))
            return 2
        if condition:
            record['alerted'] = {'condition': condition, 'at': now}
    write_state(state_path, record)
    print('probe ' + (condition or 'ok'))
    return 1 if condition else 0


if __name__ == '__main__':
    raise SystemExit(main())
