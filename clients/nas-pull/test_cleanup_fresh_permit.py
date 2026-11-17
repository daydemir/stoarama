"""Fresh per-file authority on real disposable files and durable journals."""
import concurrent.futures
import threading
import time
import unittest
from unittest import mock

import test_cleanup_bulk as fixtures
pull = fixtures.pull


class CleanupFreshPermitTests(unittest.TestCase):
    setUp = fixtures.CleanupBulkTests.setUp
    group = fixtures.CleanupBulkTests.group
    fixture_pins = fixtures.CleanupBulkTests.fixture_pins
    api = fixtures.CleanupBulkTests.api
    run_group = fixtures.CleanupBulkTests.run_group
    phases = fixtures.CleanupBulkTests.phases
    version = "v85r-nas-proof-v2.2"

    def test_eight_slow_files_each_get_fresh_authority(self):
        tasks = self.group(8)
        clock, unlink, api = time.monotonic, pull.os.unlink, self.api
        offset, permits = [0.0], []
        def remove(name, **kwargs):
            result = unlink(name, **kwargs)
            if name.endswith(".stoarama.json"):
                offset[0] += 3
            return result
        def observe(cfg, path, body, **kwargs):
            if path.endswith("/observe-batch"):
                self.assertEqual(len(body["observations"]), 1)
                permits.append(offset[0])
            return api(cfg, path, body, **kwargs)
        with mock.patch.object(pull.time, "monotonic", side_effect=lambda: clock()+offset[0]), \
                mock.patch.object(pull.os, "unlink", side_effect=remove), \
                mock.patch.object(self, "api", side_effect=observe):
            for task in tasks:
                self.run_group([task])
        self.assertEqual(permits, list(range(0, 24, 3)))
        self.assertFalse(self.phases())
        self.assertTrue(all(not (self.cfg.output_dir/t["item"]["relative_path"]).exists() for t in tasks))

    def test_lease_requests_only_one_execution_file(self):
        tasks, calls = self.group(1), []
        def lease(cfg, limit, files, size, **kwargs):
            self.assertEqual(files, 1)
            calls.append(files)
            return dict(tasks=tasks if len(calls)==1 else [], retry_after_sec=120)
        with mock.patch.object(pull, "CLIENT_VERSION", self.version), \
                mock.patch.object(pull, "cleanup_lease_v2", side_effect=lease), \
                mock.patch.object(pull, "restore_api", side_effect=self.api), \
                concurrent.futures.ThreadPoolExecutor(2) as executor:
            pull.cleanup_batch_v2(self.cfg, self.inventory, threading.Event(), executor, None)
        self.assertGreaterEqual(len(calls), 2)
        self.assertFalse(self.phases())

    def test_oversized_execution_roster_refused_before_journal_or_transport(self):
        tasks = self.group(2)
        with self.assertRaises(pull.CleanupResponseError):
            self.run_group(tasks)
        self.assertFalse(self.phases())
        self.assertTrue(all((self.cfg.output_dir/t["item"]["relative_path"]).exists() for t in tasks))


class R2CopyV4FreshPermitTests(CleanupFreshPermitTests):
    version = "r2-copy-v4"
