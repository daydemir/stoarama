#!/usr/bin/env python3
"""Measure the low-memory cleanup release on real disposable files.

This is a LOCAL FIXTURE measurement, never a NAS measurement. It stages an
eight-file group (plus optional sibling noise) on the local filesystem and runs
the actual reviewed group path: bounded parent enumeration, streamed proofs,
permit, unlink and receipts. Reported numbers are this process's Python
allocation peak (tracemalloc) and its current resident-set growth across the
run. Files are written in 1 MiB chunks, so the staging buffer is not part of the
measurement; page cache and kernel readahead are not client memory.

  python3 bench_cleanup_low_memory.py --file-mib 32 --siblings 2000 --runs 3

No NAS mount, API, credential or production path is touched.
"""
import argparse
import gc
import hashlib
import json
import os
import statistics
import threading
import time
import tracemalloc
from unittest import mock
from pathlib import Path

import test_cleanup_bulk as bulk_fixtures
import test_cleanup_group as group_fixtures

pull = group_fixtures.pull
CHUNK = 1024 * 1024


def resident_bytes():
    try:
        with open('/proc/self/statm', encoding='ascii') as status:
            return int(status.read().split()[1]) * os.sysconf('SC_PAGE_SIZE')
    except OSError:
        import subprocess
        return int(subprocess.run(['ps', '-o', 'rss=', '-p', str(os.getpid())],
                                  capture_output=True, text=True, check=True).stdout.strip()) * 1024


def stage_file(path, chunks, block):
    digest = hashlib.sha256()
    with open(path, 'wb') as output:
        for _ in range(chunks):
            output.write(block)
            digest.update(block)
    return digest.hexdigest()


def one_run(args):
    case = bulk_fixtures.CleanupBulkTests(
        'test_one_bulk_request_all_intents_before_unlink_all_syncs_before_success')
    case.version = pull.LOW_MEMORY_CLEANUP_CLIENT_VERSION
    case.setUp()
    try:
        template = case.group(1)[0]
        block = b'x' * CHUNK
        tasks = []
        for n in range(8):
            item = dict(template['item'])
            item['source_id'] = 500 + n
            item['relative_path'] = 'stream/day/representative-%02d.mp4' % n
            path = case.cfg.output_dir / item['relative_path']
            sidecar = Path(str(path) + '.stoarama.json')
            digest = stage_file(path, args.file_mib, block)
            sidecar.write_text(json.dumps(dict(schema_version=1, **dict(
                case.binding, clip_id=item['source_id'],
                relative_path=item['relative_path'],
                size_bytes=args.file_mib * CHUNK, sha256=digest))))
            item.update(size_bytes=args.file_mib * CHUNK, sha256=digest,
                        frozen_identity=pull.cleanup_identity(path.stat()),
                        frozen_sidecar_identity=pull.cleanup_identity(sidecar.stat()))
            tasks.append(dict(template, item_id=1000 + n, item=item))
        del block
        for n in range(args.siblings):
            (case.cfg.output_dir / 'stream/day' / ('retained-%05d.txt' % n)).write_bytes(b'keep')
        for task in tasks:
            with pull.CleanupFile(case.cfg, task['item'], threading.Event(), time.monotonic() + 60) as current:
                task['approved_proof'] = current.proof()
        gc.collect()
        baseline = resident_bytes()
        tracemalloc.start()
        started = time.monotonic()
        for task in tasks:
            case.run_group([task])
        elapsed = time.monotonic() - started
        python_peak, _ = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        growth = max(0, resident_bytes() - baseline)
        assert all(not (case.cfg.output_dir / task['item']['relative_path']).exists() for task in tasks)
        assert not pull.cleanup_journal(case.inventory)
        return dict(seconds=round(elapsed, 3), python_peak_bytes=python_peak,
                    rss_growth_bytes=growth, files=len(tasks), siblings=args.siblings)
    finally:
        case.doCleanups()


def release_group_size():
    with mock.patch.object(pull, 'CLIENT_VERSION', pull.LOW_MEMORY_CLEANUP_CLIENT_VERSION):
        return pull.cleanup_execution_group_size()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--file-mib', type=int, default=32)
    parser.add_argument('--siblings', type=int, default=2000)
    parser.add_argument('--runs', type=int, default=3)
    args = parser.parse_args()
    if args.file_mib < 1 or args.siblings < 0 or args.runs < 1:
        parser.error('positive file size and run count, non-negative siblings')
    samples = [one_run(args) for _ in range(args.runs)]
    print(json.dumps(dict(
        version=pull.LOW_MEMORY_CLEANUP_CLIENT_VERSION,
        measurement='local fixture, not a NAS measurement',
        file_mib=args.file_mib, samples=samples,
        median=dict(
            seconds=statistics.median(s['seconds'] for s in samples),
            python_peak_bytes=statistics.median(s['python_peak_bytes'] for s in samples),
            rss_growth_bytes=statistics.median(s['rss_growth_bytes'] for s in samples)),
        client_bounds=dict(dry_inflight=pull.CLEANUP_LOW_MEMORY_INFLIGHT,
                           dry_workers=pull.CLEANUP_LOW_MEMORY_WORKERS,
                           lookahead=pull.CLEANUP_LOW_MEMORY_LOOKAHEAD,
                           group=release_group_size(),
                           namespace_entries=pull.CLEANUP_NAMESPACE_MAX_ENTRIES,
                           namespace_encoded_bytes=pull.CLEANUP_NAMESPACE_MAX_ENCODED_BYTES,
                           stream_block_bytes=CHUNK,
                           sidecar_bytes=1024 * 1024)),
        indent=2))


if __name__ == '__main__':
    main()
