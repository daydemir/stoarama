import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import test_joined_download as joined_tests

pull = joined_tests.pull
RangeResponse = joined_tests.RangeResponse


class JoinedPollFenceTests(unittest.TestCase):
    def test_each_explicit_poll_allows_cleanup_before_the_poll_returns(self):
        helper = joined_tests.JoinedDownloadTests()
        # Two ranges visit first/range/range/verify/validate/publish.
        for fair_share in (False, True):
            for target in range(1, 7):
                with self.subTest(fair_share=fair_share, boundary=target), tempfile.TemporaryDirectory() as raw:
                    cfg = helper.config(Path(raw))
                    runtime = helper.runtime(cfg)
                    if fair_share:
                        runtime.apply_joined_protocol_response(helper.protocol_response(
                            1, pull.JOINED_FAIR_SHARE_MIN_GENERATION,
                        ))
                    content = b"joined polling" * 2
                    raw_item = helper.media_item(content)
                    item = pull.valid_joined_item(raw_item)
                    helper.install_manifest(cfg, item)
                    fence, stop = pull.NAS_MEDIA_FENCE, threading.Event()
                    waiting, release, executed = (threading.Event() for _ in range(3))
                    errors, polls = [], []
                    original_boundary = pull.joined_raw_priority_boundary
                    original_append = pull.append_joined_range

                    def boundary(*args):
                        self.assertEqual(fence.writers, 0)
                        polls.append(len(polls) + 1)
                        if len(polls) == target:
                            waiting.set()
                            if not release.wait(3):
                                raise AssertionError("poll was not released")
                        return original_boundary(*args)

                    def api(_cfg, _method, path, **_kwargs):
                        if path.startswith("/account/clips?"):
                            return {"clips": []}
                        if path == raw_item["download_path"]:
                            self.assertEqual(fence.writers, 1)
                            return helper.prepared(raw_item)
                        raise AssertionError(path)

                    def append(*args):
                        self.assertEqual(fence.writers, 1)
                        return original_append(*args)

                    def response(request):
                        start, end = map(int, request.get_header("Range").removeprefix("bytes=").split("-"))
                        return RangeResponse(content[start:end + 1], start, end, len(content))

                    def run(fn):
                        try:
                            fn()
                        except BaseException as exc:
                            errors.append(exc)

                    def cleanup():
                        lane = pull.CleanupExecutionLane(stop)
                        try:
                            lane.run(executed.set)
                        finally:
                            lane.close()

                    worker = threading.Thread(target=run, args=(lambda: pull.download_joined_item(
                        cfg, runtime, item, stop,
                    ),))
                    cleaner = threading.Thread(target=run, args=(cleanup,))
                    with mock.patch.object(pull, "joined_raw_priority_boundary", side_effect=boundary), \
                            mock.patch.object(pull, "request_json", side_effect=api), \
                            mock.patch.object(pull, "storage_status", return_value=helper.storage()), \
                            mock.patch.object(pull, "JOINED_RANGE_BYTES", len(content) // 2), \
                            mock.patch.object(pull, "append_joined_range", side_effect=append), \
                            mock.patch.object(pull, "open_joined_url", side_effect=response):
                        try:
                            worker.start()
                            self.assertTrue(waiting.wait(3), errors)
                            cleaner.start()
                            self.assertTrue(executed.wait(2), "cleanup waited for the slow poll")
                        finally:
                            release.set()
                            for thread in (worker, cleaner):
                                if thread.ident is not None:
                                    thread.join(4)
                    self.assertFalse(worker.is_alive() or cleaner.is_alive())
                    self.assertFalse(errors, errors)
                    self.assertEqual(len(polls), 6)
                    self.assertEqual(pull.joined_output_path(cfg, item).read_bytes(), content)
                    self.assertEqual((fence.writers, fence.cleaners), (0, 0))
                    self.assertIsNone(fence.execution_owner)

    def test_namespace_drift_during_poll_is_refused_without_repair(self):
        helper = joined_tests.JoinedDownloadTests()
        for change in ("inode", "size", "hardlink", "marker", "final", "directory"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as raw:
                cfg = helper.config(Path(raw))
                runtime = helper.runtime(cfg)
                item = pull.valid_joined_item(helper.media_item())
                directory_fd, final, part, marker_name = helper.names(cfg, item)
                marker = helper.marker(item)
                helper.write_entry(directory_fd, part, b"part")
                helper.write_entry(directory_fd, marker_name, marker)
                directory = pull.joined_output_path(cfg, item).parent
                changed = {}

                def mutate(*_args):
                    self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
                    if change == "inode":
                        (directory / part).rename(directory / "preserved-original")
                        (directory / part).write_bytes(b"part")
                    elif change == "size":
                        (directory / part).write_bytes(b"different bytes")
                    elif change == "hardlink":
                        os.link(directory / part, directory / "other-link")
                    elif change == "marker":
                        (directory / marker_name).write_bytes(b"x" * len(marker))
                    elif change == "final":
                        (directory / final).write_bytes(b"foreign final")
                    else:
                        directory.rename(directory.with_name(directory.name + "-moved"))
                        directory.mkdir()
                    changed["part"] = os.stat(part, dir_fd=directory_fd, follow_symlinks=False)

                try:
                    with pull.JoinedPollFence(cfg, runtime, item, threading.Event()) as owner, \
                            mock.patch.object(pull, "joined_raw_priority_boundary", side_effect=mutate), \
                            mock.patch.object(pull, "truncate_joined_part") as truncate, \
                            self.assertRaises(pull.ExistingFileMismatch):
                        owner.priority_boundary(directory_fd, (final, part, marker_name), marker)
                    truncate.assert_not_called()
                    after = os.stat(part, dir_fd=directory_fd, follow_symlinks=False)
                    self.assertEqual(pull.certification_identity(after), pull.certification_identity(changed["part"]))
                    self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
                finally:
                    os.close(directory_fd)

    def test_poll_error_reacquires_before_unwinding_and_preserves_nested_writer(self):
        helper = joined_tests.JoinedDownloadTests()
        with tempfile.TemporaryDirectory() as raw:
            cfg = helper.config(Path(raw))
            runtime = helper.runtime(cfg)
            item = pull.valid_joined_item(helper.media_item())
            directory_fd, final, part, marker_name = helper.names(cfg, item)
            marker = helper.marker(item)
            helper.write_entry(directory_fd, part, b"part")
            helper.write_entry(directory_fd, marker_name, marker)
            fence = pull.NAS_MEDIA_FENCE
            fence.acquire_delivery()
            try:
                with pull.JoinedPollFence(cfg, runtime, item, threading.Event()) as owner:
                    with mock.patch.object(pull, "joined_raw_priority_boundary", side_effect=RuntimeError("poll failed")), \
                            self.assertRaisesRegex(RuntimeError, "poll failed"):
                        owner.priority_boundary(directory_fd, (final, part, marker_name), marker)
                    self.assertTrue(owner.held)
                    self.assertEqual(fence.writers, 2)
                self.assertEqual(fence.writers, 1)
            finally:
                fence.release_delivery()
                os.close(directory_fd)

    def test_actual_media_work_still_excludes_cleanup(self):
        helper = joined_tests.JoinedDownloadTests()
        with tempfile.TemporaryDirectory() as raw:
            cfg = helper.config(Path(raw))
            runtime = helper.runtime(cfg)
            item = pull.valid_joined_item(helper.media_item())
            stop, entered, cleaned = threading.Event(), threading.Event(), threading.Event()
            errors = []

            def cleanup():
                lane = pull.CleanupExecutionLane(stop)
                try:
                    entered.set()
                    lane.run(cleaned.set)
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    lane.close()

            thread = threading.Thread(target=cleanup)
            with pull.JoinedPollFence(cfg, runtime, item, stop):
                thread.start()
                self.assertTrue(entered.wait(1))
                self.assertFalse(cleaned.wait(.05), "cleanup entered fenced media work")
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertTrue(cleaned.is_set())
            self.assertFalse(errors, errors)

    def test_failed_reacquisition_does_not_release_an_unowned_writer(self):
        helper = joined_tests.JoinedDownloadTests()
        with tempfile.TemporaryDirectory() as raw:
            cfg = helper.config(Path(raw))
            runtime = helper.runtime(cfg)
            item = pull.valid_joined_item(helper.media_item())
            fd, final, part, marker_name = helper.names(cfg, item)
            marker = helper.marker(item)
            helper.write_entry(fd, part, b"part")
            helper.write_entry(fd, marker_name, marker)
            try:
                with self.assertRaisesRegex(RuntimeError, "admission interrupted"):
                    with pull.JoinedPollFence(cfg, runtime, item, threading.Event()) as owner, \
                            mock.patch.object(pull, "joined_raw_priority_boundary"), \
                            mock.patch.object(pull.NAS_MEDIA_FENCE, "acquire_delivery", side_effect=RuntimeError("admission interrupted")):
                        owner.priority_boundary(fd, (final, part, marker_name), marker)
                self.assertEqual(pull.NAS_MEDIA_FENCE.writers, 0)
            finally:
                os.close(fd)


if __name__ == "__main__":
    unittest.main()
