"""Low-memory cleanup release: exact admission, two-hash bound, bounded listing.

Real disposable files and FULL-WAL SQLite only; no NAS, API or credentials.
Execution uses the reviewed one-file fresh-permit group, like every release
since v85r-nas-proof-v2.2, run under the new immutable literal.
"""
import ast
import hashlib
import json
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

import test_cleanup as fixtures
import test_cleanup_bulk as bulk_fixtures
pull = fixtures.pull

VERSION = pull.LOW_MEMORY_CLEANUP_CLIENT_VERSION
COPY_ONLY = pull.LOW_MEMORY_COPY_CLIENT_VERSION


class LowMemoryReleaseTests(unittest.TestCase):
    setUp = fixtures.CleanupTests.setUp
    fixture_pins = fixtures.CleanupTests.fixture_pins

    def test_new_literal_is_in_the_reviewed_group_and_bulk_sets(self):
        self.assertIn(VERSION, pull.CLEANUP_BULK_VERSIONS)
        self.assertIn(VERSION, pull.CLEANUP_GROUP_VERSIONS)
        for unknown in (VERSION + '-extra', 'r2-copy-v8-low-memory-', 'r2-copy-v8',
                        'r2-copy-v8-low-memory-cleanup2', VERSION.upper()):
            self.assertNotIn(unknown, pull.CLEANUP_GROUP_VERSIONS)
            self.assertNotIn(unknown, pull.CLEANUP_BULK_VERSIONS)
        with mock.patch.object(pull, 'CLIENT_VERSION', VERSION):
            self.assertEqual(pull.cleanup_execution_group_size(), 1)
            self.assertEqual(pull.cleanup_inflight(), 2)
            self.assertEqual(pull.cleanup_dry_workers(), 2)
        # The deployed copy-only release still has no cleanup lane at all.
        self.assertNotIn(COPY_ONLY, pull.CLEANUP_GROUP_VERSIONS)
        self.assertNotIn(COPY_ONLY, pull.CLEANUP_BULK_VERSIONS)

    def test_existing_releases_keep_their_width_and_protocol_one(self):
        for version in sorted(pull.CLEANUP_V6_VERSIONS | {
                pull.CLEANUP_TRUTH_VERSION, pull.CLEANUP_CERTIFIED_STITCH_VERSION, 'r2-copy-v6'}):
            with self.subTest(version=version), mock.patch.object(pull, 'CLIENT_VERSION', version):
                self.assertEqual(pull.cleanup_inflight(), pull.CLEANUP_V2_INFLIGHT)
                self.assertEqual(pull.cleanup_dry_workers(), pull.CLEANUP_V2_WORKERS)
                pull.cleanup_require_v2()  # protocol 1 stays available

    def test_protocol_one_fallback_fails_closed_only_for_the_new_release(self):
        with mock.patch.object(pull, 'CLIENT_VERSION', VERSION):
            with self.assertRaises(pull.CleanupProtocolUnsupported):
                pull.cleanup_require_v2()
            with self.assertRaises(pull.CleanupProtocolUnsupported):
                pull.cleanup_batch(self.cfg, self.inventory, threading.Event(), None)
            with self.assertRaises(pull.CleanupProtocolUnsupported):
                pull.cleanup_once(self.cfg, self.inventory, threading.Event())
        self.assertTrue(self.path.exists())

    def test_loop_never_falls_back_to_protocol_one_for_the_new_release(self):
        waits, v1_calls = [], []
        class Stop:
            def is_set(self):
                return False
            def wait(self, seconds):
                waits.append(seconds)
                return len(waits) > 1
        def unsupported(*_args, **_kwargs):
            raise pull.CleanupProtocolUnsupported('cleanup protocol 2 unavailable (HTTP 400)')
        with mock.patch.object(pull, 'CLIENT_VERSION', VERSION), \
                mock.patch.object(pull, 'cleanup_batch_v2', side_effect=unsupported), \
                mock.patch.object(pull, 'cleanup_batch', side_effect=lambda *a, **k: v1_calls.append(a)) as v1, \
                mock.patch.object(pull, 'CleanupPreflight'):
            pull.cleanup_loop(self.cfg, self.inventory, Stop())
        self.assertFalse(v1.called, 'low-memory release fell back to protocol 1')
        self.assertEqual(v1_calls, [])
        self.assertEqual(waits[-1], pull.restore_wait(pull.CLEANUP_V2_FALLBACK_SEC), waits)
        self.assertNotIn(1, waits[1:], 'closed fallback used an error-loop short wait')

    def test_loop_keeps_the_existing_protocol_one_fallback(self):
        waits = []
        class Stop:
            def is_set(self):
                return False
            def wait(self, seconds):
                waits.append(seconds)
                return len(waits) > 2
        def unsupported(*_args, **_kwargs):
            raise pull.CleanupProtocolUnsupported('cleanup protocol 2 unavailable (HTTP 400)')
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_TRUTH_VERSION), \
                mock.patch.object(pull, 'cleanup_batch_v2', side_effect=unsupported), \
                mock.patch.object(pull, 'cleanup_batch', return_value=60) as v1, \
                mock.patch.object(pull, 'CleanupPreflight'):
            pull.cleanup_loop(self.cfg, self.inventory, Stop())
        self.assertTrue(v1.called, 'existing bulk releases lost their protocol-1 fallback')

    def test_both_low_memory_literals_refuse_stitch_authority_locally(self):
        for version in (VERSION, COPY_ONLY):
            with self.subTest(version=version), mock.patch.object(pull, 'CLIENT_VERSION', version):
                self.assertFalse(pull.StitchSwitches().local_enabled)
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.CLEANUP_CERTIFIED_STITCH_VERSION):
            self.assertTrue(pull.StitchSwitches().local_enabled)

    def test_certified_stitch_cleanup_target_is_refused(self):
        replacement = self.cfg.output_dir / 'stitched-output.mp4'
        replacement.write_bytes(b'certified output')
        output = dict(relative_path=replacement.name, size_bytes=replacement.stat().st_size,
                      sha256=hashlib.sha256(replacement.read_bytes()).hexdigest(),
                      identity=pull.cleanup_identity(replacement.stat()))
        self.item.update(permit_reason=pull.CLEANUP_CERTIFIED_STITCH_REASON,
                         stitch_certificate=dict(hour_id=1, attempt=1, output=output),
                         stitch_raw_sidecar=dict(size_bytes=self.sidecar.stat().st_size,
                                                 sha256=hashlib.sha256(self.sidecar.read_bytes()).hexdigest()))
        self.item.pop('frozen_identity')
        self.item.pop('frozen_sidecar_identity')
        for version in (VERSION, COPY_ONLY):
            with self.subTest(version=version), mock.patch.object(pull, 'CLIENT_VERSION', version):
                with self.assertRaisesRegex(RuntimeError, 'reviewed certified stitch'):
                    with pull.CleanupFile(self.cfg, self.item, threading.Event(), time.monotonic() + 20):
                        self.fail('low-memory release opened a stitch source')
        self.assertTrue(replacement.exists())


class LowMemoryListingTests(unittest.TestCase):
    """Bounded fd enumeration keeps the exact namespace digest and names."""

    setUp = fixtures.CleanupTests.setUp
    fixture_pins = fixtures.CleanupTests.fixture_pins

    def entries(self, version, cap_entries=pull.CLEANUP_NAMESPACE_MAX_ENTRIES,
                cap_bytes=pull.CLEANUP_NAMESPACE_MAX_ENCODED_BYTES):
        with mock.patch.object(pull, 'CLIENT_VERSION', version), \
                mock.patch.object(pull, 'CLEANUP_NAMESPACE_MAX_ENTRIES', cap_entries), \
                mock.patch.object(pull, 'CLEANUP_NAMESPACE_MAX_ENCODED_BYTES', cap_bytes):
            with pull.CleanupFile(self.cfg, self.item, threading.Event(), time.monotonic() + 60) as current:
                return current.list_parent()

    def test_scandir_and_listdir_produce_the_same_namespace(self):
        for n in range(25):
            (self.path.parent / ('retained-%03d.txt' % n)).write_bytes(b'keep')
        old = self.entries(pull.CLEANUP_TRUTH_VERSION)
        new = self.entries(VERSION)
        self.assertEqual(new, old)
        self.assertEqual(len(new), len(list(self.path.parent.iterdir())))
        self.assertEqual(pull.cleanup_namespace_sha256(new), pull.cleanup_namespace_sha256(old))

    def test_new_release_never_materializes_the_directory(self):
        (self.path.parent / 'retained.txt').write_bytes(b'keep')
        def forbidden(*_args, **_kwargs):
            raise AssertionError('low-memory release must not call os.listdir')
        with mock.patch.object(pull.os, 'listdir', side_effect=forbidden):
            entries = self.entries(VERSION)
        self.assertIn(b'retained.txt'.hex(), entries)

    def test_namespace_entry_cap_refuses_instead_of_growing(self):
        for n in range(9):
            (self.path.parent / ('retained-%03d.txt' % n)).write_bytes(b'keep')
        # media + sidecar + nine siblings = eleven entries.
        self.assertEqual(len(self.entries(VERSION, cap_entries=11)), 11)
        with self.assertRaisesRegex(pull.CleanupSourceDataError, 'namespace exceeds bound'):
            self.entries(VERSION, cap_entries=10)
        self.assertTrue(self.path.exists())

    def test_namespace_encoded_byte_cap_refuses_instead_of_growing(self):
        for n in range(4):
            (self.path.parent / ('retained-%03d.txt' % n)).write_bytes(b'keep')
        with self.assertRaisesRegex(pull.CleanupSourceDataError, 'namespace exceeds bound'):
            self.entries(VERSION, cap_bytes=8)
        self.assertTrue(self.path.exists())

    def test_deadline_is_checked_during_the_scan(self):
        for n in range(60):
            (self.path.parent / ('retained-%03d.txt' % n)).write_bytes(b'keep')
        calls, original = [0], time.monotonic
        def clock():
            calls[0] += 1
            return original() + (3600 if calls[0] > 30 else 0)
        with mock.patch.object(pull, 'CLIENT_VERSION', VERSION), \
                mock.patch.object(pull.time, 'monotonic', side_effect=clock):
            with pull.CleanupFile(self.cfg, self.item, threading.Event(), original() + 30) as current:
                with self.assertRaises(pull.CleanupInterrupted):
                    current.list_parent()
        self.assertTrue(self.path.exists())

    def test_oversized_namespace_refuses_before_any_unlink(self):
        for n in range(20):
            (self.path.parent / ('retained-%03d.txt' % n)).write_bytes(b'keep')
        with mock.patch.object(pull, 'CLIENT_VERSION', VERSION), \
                mock.patch.object(pull, 'CLEANUP_NAMESPACE_MAX_ENTRIES', 8), \
                mock.patch.object(pull.os, 'unlink', side_effect=AssertionError('oversized namespace unlinked')):
            with self.assertRaises(pull.CleanupSourceDataError):
                with pull.CleanupFile(self.cfg, self.item, threading.Event(), time.monotonic() + 60) as current:
                    current.proof()
        self.assertTrue(self.path.exists())
        self.assertTrue(self.sidecar.exists())


class LowMemoryGroupPathTests(unittest.TestCase):
    """The reviewed one-file fresh-permit group runs under the new literal."""

    setUp = bulk_fixtures.CleanupBulkTests.setUp
    group = bulk_fixtures.CleanupBulkTests.group
    fixture_pins = bulk_fixtures.CleanupBulkTests.fixture_pins
    api = bulk_fixtures.CleanupBulkTests.api
    run_group = bulk_fixtures.CleanupBulkTests.run_group
    phases = bulk_fixtures.CleanupBulkTests.phases
    version = VERSION

    def test_new_literal_deletes_each_file_under_its_own_permit(self):
        tasks = self.group(8)
        for task in tasks:
            self.run_group([task])
        self.assertFalse(self.phases())
        self.assertTrue(all(not (self.cfg.output_dir / task['item']['relative_path']).exists() for task in tasks))

    def test_new_literal_hashes_files_sequentially_within_the_group(self):
        tasks = self.group(8)
        original = pull.CleanupFile.proof
        active, peak, lock = [0], [0], threading.Lock()
        def proof(current):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            try:
                return original(current)
            finally:
                with lock:
                    active[0] -= 1
        with mock.patch.object(pull.CleanupFile, 'proof', proof):
            for task in tasks:
                self.run_group([task])
        self.assertEqual(peak[0], 1, 'group proof parallelism changed')

    def test_total_concurrent_hashes_stay_at_two(self):
        """Every lane's hash width, and the resulting total.

        The primary dry lane is two. The lookahead lane is one, and it may
        overlap exactly one one-file group proof, so
        group(1) + lookahead(1) = 2 as well. No lane, and no overlap of lanes,
        can produce a third concurrent hash."""
        with mock.patch.object(pull, 'CLIENT_VERSION', VERSION):
            primary = pull.cleanup_inflight()
            workers = pull.cleanup_dry_workers()
            lookahead = pull.cleanup_lookahead_workers()
            group = pull.cleanup_execution_group_size()
        self.assertEqual((primary, workers, lookahead, group), (2, 2, 1, 1))
        self.assertEqual(max(primary, lookahead + 1), 2)
        # The coordinator also sizes its shared executor by the same workers,
        # so a wide pool cannot be refilled past the lease window.
        self.assertLessEqual(workers, primary)
        # A one-file group retains one namespace snapshot, capped at 65536
        # entries and 4 MiB of hex-encoded names (the largest live continuous
        # folder had about 43k entries and 1.5 MB of hex on Nov 23); the
        # lookahead adds at most one more. Measured on Python 3.9 at the caps:
        # about 15 MiB retained and 21 MiB peak per proof, 35 MiB peak for two.
        self.assertEqual(pull.CLEANUP_NAMESPACE_MAX_ENCODED_BYTES, 4 * 1024 * 1024)
        self.assertEqual(pull.CLEANUP_NAMESPACE_MAX_ENTRIES, 65536)

    def test_lookahead_lane_hashes_one_file_at_a_time(self):
        """The real lookahead lane never runs two hashes at once."""
        templates = self.group(1)
        tasks = []
        for n in range(4):
            task = dict(templates[0], item_id=9000 + n)
            task.pop('approved_proof', None)
            task.pop('execution_batch', None)
            tasks.append(task)
        active, peak, lock = [0], [0], threading.Lock()
        original = pull.CleanupFile.proof
        def proof(current):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            try:
                time.sleep(0.02)
                return original(current)
            finally:
                with lock:
                    active[0] -= 1
        remaining = list(tasks)
        def api(cfg, path, body, **kwargs):
            if path.endswith('/lease'):
                if not remaining:
                    return dict(tasks=[], retry_after_sec=120)
                chosen, remaining[:] = remaining[:body['limit']], remaining[body['limit']:]
                return dict(tasks=chosen, retry_after_sec=0)
            if path.endswith('/observe-batch'):
                return dict(results=[fixtures.fixture_ack(o, o['item_id'], status=200,
                    action='would_delete', delete=False, recovery='r2_sha_verified')
                    for o in body['observations']])
            if path.endswith('/observe'):
                return fixtures.fixture_ack(body, int(path.split('/')[-2]), status=200,
                                            action='would_delete', delete=False,
                                            recovery='r2_sha_verified')
            raise AssertionError('unexpected dry call %s' % path)
        stop = threading.Event()
        api_client = pull.CleanupBatchAPI(self.cfg)
        active_flag = threading.Event()
        active_flag.set()
        try:
            with mock.patch.object(pull, 'CLIENT_VERSION', VERSION), \
                    mock.patch.object(pull, 'restore_api', side_effect=api), \
                    mock.patch.object(pull.CleanupFile, 'proof', proof):
                with pull.concurrent.futures.ThreadPoolExecutor(
                        max_workers=pull.cleanup_lookahead_workers()) as executor:
                    pull.cleanup_batch_v2(self.cfg, self.inventory, stop, executor, api_client,
                                          inflight=pull.cleanup_lookahead_workers(),
                                          task_limit=16, dry_run_only=True, active=active_flag)
        finally:
            api_client.close()
        self.assertGreaterEqual(peak[0], 1)
        self.assertEqual(peak[0], 1, 'lookahead hashed more than one file at a time')

    def test_release_source_still_parses_as_python_39(self):
        source = (Path(__file__).with_name('stoarama_pull.py')).read_bytes()
        ast.parse(source, feature_version=(3, 9))
        # The new literals must be plain module constants, not computed labels.
        tree = ast.parse(source)
        literal = {}
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in (
                            'LOW_MEMORY_COPY_CLIENT_VERSION', 'LOW_MEMORY_CLEANUP_CLIENT_VERSION'):
                        literal[target.id] = ast.literal_eval(node.value)
        self.assertEqual(literal, {'LOW_MEMORY_COPY_CLIENT_VERSION': COPY_ONLY,
                                   'LOW_MEMORY_CLEANUP_CLIENT_VERSION': VERSION})
        self.assertEqual(pull.CLIENT_VERSION, 'development')

    def test_lease_requests_keep_the_two_dry_hint(self):
        bodies = []
        def api(cfg, path, body):
            bodies.append(body)
            return dict(tasks=[])
        with mock.patch.object(pull, 'CLIENT_VERSION', VERSION), \
                mock.patch.object(pull, 'cleanup_api_retry', side_effect=api):
            pull.cleanup_lease_v2(self.cfg, 8, 8, 1 << 30)
        self.assertEqual(bodies[-1]['dry_run_limit'], 2)
        self.assertIs(bodies[-1]['execution_batch'], True)

class Keep20ReleaseGuardTests(unittest.TestCase):
    """The keep20 release guards with the real embedded 20-day pin, not keep28."""

    def refusal(self, start, end):
        item = dict(kind='raw', stream_id=2718, relative_path='recordings/326/continuous/1.mp4',
                    object_key='keep20-release-fixture', sidecar_required=True)
        proof = dict(sidecar=dict(binding=dict(clip_start_at=start, clip_end_at=end)))
        with mock.patch.object(pull, 'CLIENT_VERSION', pull.KEEP20_CLEANUP_CLIENT_VERSION):
            return pull.cleanup_guard_refusal(dict(item=item), proof)

    def test_keep20_literal_shares_low_memory_cleanup_behavior(self):
        self.assertIn(pull.KEEP20_CLEANUP_CLIENT_VERSION, pull.LOW_MEMORY_CLEANUP_VERSIONS)
        self.assertIn(pull.KEEP20_CLEANUP_CLIENT_VERSION, pull.CLEANUP_BULK_VERSIONS)

    def test_guard_uses_the_20_day_window(self):
        # Stream 2718 keeps [2026-07-13T07Z, 2026-08-02T07Z) under keep20; the
        # legacy keep28 window was [2026-07-09T07Z, 2026-08-06T07Z).
        self.assertIsNone(self.refusal('2026-07-10T12:00:00+00:00', '2026-07-10T12:01:00+00:00'))
        self.assertIn('overlaps', self.refusal('2026-07-20T12:00:00+00:00', '2026-07-20T12:01:00+00:00'))


if __name__ == '__main__':
    unittest.main()
