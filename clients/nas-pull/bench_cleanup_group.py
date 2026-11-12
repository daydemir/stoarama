#!/usr/bin/env python3
"""Measure an eight-file group on real disposable files and FULL-WAL SQLite.

Compile test-support/count_fsync.c and preload it into a non-SIP Python:
  DYLD_INSERT_LIBRARIES=/tmp/count_fsync.dylib python3 bench_cleanup_group.py
Linux uses LD_PRELOAD=/tmp/count_fsync.so. --client can name a saved predecessor
stoarama_pull.py for an identical before/after run. No NAS/API access is made.
--sync-delay-ms optionally adds latency to each *real* sync, including SQLite's
C calls. Report this separately from native storage measurements.
"""
import argparse
import ctypes
import importlib.util
import json
import statistics
import threading
import time
from pathlib import Path
from unittest import mock

import test_cleanup_group as fixtures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', type=Path, default=Path(__file__).with_name('stoarama_pull.py'))
    parser.add_argument('--runs', type=int, default=5)
    parser.add_argument('--sync-delay-ms', type=float, default=0)
    parser.add_argument('--permit-delay-ms', type=float, default=0)
    parser.add_argument('--proof-delay-ms', type=float, default=0)
    parser.add_argument('--lease-delay-ms', type=float, default=0)
    args = parser.parse_args()
    if args.runs < 1 or not 0 <= args.sync_delay_ms < 1000:
        parser.error('positive runs and sync delay in [0,1000) required')
    spec = importlib.util.spec_from_file_location('measured_pull', args.client)
    pull = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pull)
    fixtures.pull = fixtures.fixtures.pull = pull
    library = ctypes.CDLL(None)
    library.cleanup_sync_begin.argtypes = [ctypes.c_long]
    library.cleanup_sync_end.restype = ctypes.c_long
    # Verify that preloading captures SQLite C syncs as well as Python fsyncs.
    import sqlite3
    import tempfile
    with tempfile.TemporaryDirectory() as scratch:
        db = sqlite3.connect(str(Path(scratch)/'probe.db'))
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        db.execute('CREATE TABLE probe(n)')
        library.cleanup_sync_begin(0)
        db.execute('INSERT INTO probe VALUES(1)')
        db.commit()
        assert library.cleanup_sync_end() > 0, 'preload did not intercept SQLite syncs'
        db.close()
    samples = []
    for _ in range(args.runs):
        case = fixtures.CleanupGroupTests('test_group_commits_intents_before_sequential_unlinks')
        case.setUp()
        try:
            tasks = case.group(8)
            # Warm lazy schema and ledger creation outside the timed steady group.
            pull.cleanup_local_headroom(case.inventory)
            pull.cleanup_execution_outcome(case.inventory, 999, 1, {})
            pull.cleanup_journal(case.inventory)
            pull.cleanup_quarantine_tables(case.inventory)
            case.inventory.db.commit()
            pull.cleanup_ledger_append_many(case.cfg, [{'event': 'benchmark_start'}])
            commits = []
            case.inventory.db.set_trace_callback(lambda sql: commits.append(sql) if sql == 'COMMIT' else None)
            proof, api = pull.CleanupFile.proof, case.api
            def delayed_proof(current):
                time.sleep(args.proof_delay_ms / 1000)
                return proof(current)
            def delayed_api(cfg, path, body, **kwargs):
                if path.endswith('/observe-batch'):
                    time.sleep(args.permit_delay_ms / 1000)
                return api(cfg, path, body, **kwargs)
            library.cleanup_sync_begin(round(args.sync_delay_ms * 1_000_000))
            started = time.monotonic()
            try:
                time.sleep(args.lease_delay_ms / 1000)
                with mock.patch.object(pull.CleanupFile, 'proof', delayed_proof), \
                        mock.patch.object(case, 'api', side_effect=delayed_api):
                    case.run_group(tasks)
            finally:
                elapsed = time.monotonic() - started
                syncs = library.cleanup_sync_end()
            assert not pull.cleanup_journal(case.inventory)
            assert all(not (case.cfg.output_dir/t['item']['relative_path']).exists() for t in tasks)
            samples.append(dict(seconds=round(elapsed, 6), fsyncs=syncs, sqlite_commits=len(commits)))
        finally:
            case.doCleanups()
    seconds = statistics.median(s['seconds'] for s in samples)
    print(json.dumps(dict(client=pull.CLEANUP_GROUP_VERSION, files_per_group=8,
        sync_delay_ms=args.sync_delay_ms, permit_delay_ms=args.permit_delay_ms,
        proof_delay_ms=args.proof_delay_ms, lease_delay_ms=args.lease_delay_ms,
        samples=samples, median_group_seconds=seconds,
        projected_files_per_minute=round(480/seconds, 2),
        fsyncs_per_file=statistics.median(s['fsyncs'] for s in samples)/8), indent=2))


if __name__ == '__main__':
    main()
