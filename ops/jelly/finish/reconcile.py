#!/usr/bin/env python3
"""Controller-only reconciliation of an exact stopped D admission journal."""
import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import uuid

from admission import LANES, binding, gate, load, publish


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--state', required=True, type=Path)
    ap.add_argument('--inspect', action='store_true')
    ap.add_argument('--expected-latch-sha256')
    ap.add_argument('--authority', type=Path, help='JSON: controller, authority, latch_sha256')
    ap.add_argument('--evidence', type=Path, help='JSON: latch_sha256, reconciled_attempts, queue_reconciled=true')
    args = ap.parse_args()
    if not args.inspect and not all((args.expected_latch_sha256, args.authority, args.evidence)):
        ap.error('clearing requires exact latch, authority and reconciliation evidence')
    try:
        with ExitStack() as stack:
            # Match scheduled admission's gate-before-lane-lock order. Even a
            # refused reconcile must not disguise an interrupted peer as live.
            stack.enter_context(gate(args.state))
            for filename in LANES.values():
                lock = stack.enter_context((args.state / filename).open('a'))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            record = load(args.state)
            digest = binding(args.state)
            if args.inspect:
                print(json.dumps({'latch_sha256': digest, 'journal': record}))
                return 0
            authority_bytes, evidence_bytes = args.authority.read_bytes(), args.evidence.read_bytes()
            authority, evidence = json.loads(authority_bytes), json.loads(evidence_bytes)
            if (digest != args.expected_latch_sha256 or authority['latch_sha256'] != digest
                    or evidence['latch_sha256'] != digest or not authority['controller'].strip()
                    or not authority['authority'].strip() or evidence['queue_reconciled'] is not True
                    or sorted(evidence['reconciled_attempts']) != sorted(record['active'].values())
                    or not (record['failed'] or record['active'])):
                raise ValueError('reconciliation binding mismatch')
            receipt = {'cleared_at': datetime.now(timezone.utc).isoformat(), 'uid': os.getuid(),
                       'controller': authority['controller'], 'authority': authority['authority'],
                       'latch_sha256': digest, 'journal': record,
                       'authority_sha256': hashlib.sha256(authority_bytes).hexdigest(),
                       'evidence_sha256': hashlib.sha256(evidence_bytes).hexdigest()}
            receipts = args.state / 'reconciliations'
            receipts.mkdir(mode=0o700, exist_ok=True)
            publish(receipts / (uuid.uuid4().hex + '.json'), receipt)
            # No evidence/status is restored or deleted. A real recovery must
            # succeed before tick admission; tick cannot relatch during that step.
            publish(args.state / 'admission.json', {'failed': False, 'active': {}, 'recovery_required': True})
            return 0
    except (OSError, ValueError, KeyError, TypeError):
        print('finish reconciliation_refused', flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
