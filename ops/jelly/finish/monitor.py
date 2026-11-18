#!/usr/bin/env python3
"""One read-only ownership observation; no fixed-plan observer or background loop."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from ownership import collect


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--root', type=Path, required=True)
    ap.add_argument('--repo', type=Path, required=True)
    ap.add_argument('--state', type=Path, required=True)
    ap.add_argument('--owner-sha256', required=True)
    args = ap.parse_args()
    record = collect(args.root, args.repo, args.state / 'recovery.json', args.owner_sha256,
                     datetime.now(timezone.utc))
    status = json.loads((args.root / 'status.json').read_bytes())
    record.update(deleted_files=status['deleted_files'], deleted_bytes=status['deleted_bytes'],
                  current_plan=status['current_plan'], next_plan=status['next_plan'])
    print(json.dumps(record, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        raise SystemExit('cleanup ownership evidence unavailable')
