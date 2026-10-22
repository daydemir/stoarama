import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('worker', pathlib.Path(__file__).with_name('capability-worker.py'))
worker = importlib.util.module_from_spec(spec)
with mock.patch.object(pathlib.Path, 'mkdir'):
    spec.loader.exec_module(worker)


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root_patch = mock.patch.object(worker, 'ROOT', pathlib.Path(self.directory.name))
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        worker.ACTIVE.clear()
        self.job = 'a' * 64
        self.owner = worker.job_root(self.job)
        self.attempt = self.owner / 'attempt-1'
        (self.attempt / 'staged').mkdir(parents=True)
        worker.write(self.owner / 'state.json', {'job_id': self.job, 'attempt': 1, 'state': 'running', 'started_at': 100})

    def state(self):
        return json.loads((self.owner / 'state.json').read_text())

    def test_restart_recovers_completed_result(self):
        worker.write(self.attempt / 'request.json', {'work': {'hour_id': 'frozen-hour'}})
        worker.write(self.attempt / 'staged' / 'result.json', {'manifest': {'hour_id': 'frozen-hour'}, 'ready_to_publish': False})
        worker.ACTIVE.add(self.job)
        with mock.patch.object(worker, 'live_runner', return_value=None):
            worker.recover_execution(self.job, 1, 200)
        self.assertEqual(self.state()['state'], 'staged')
        self.assertNotIn(self.job, worker.ACTIVE)

    def test_restart_without_runner_or_result_is_retryable(self):
        worker.ACTIVE.add(self.job)
        with mock.patch.object(worker, 'live_runner', return_value=None):
            worker.recover_execution(self.job, 1, 200)
        self.assertEqual(self.state()['failure'], 'worker_restarted')
        self.assertNotIn(self.job, worker.ACTIVE)

    def test_unknown_runner_state_keeps_worker_closed(self):
        worker.ACTIVE.add(self.job)
        with mock.patch.object(worker, 'live_runner', side_effect=RuntimeError('ambiguous runner')):
            worker.recover_execution(self.job, 1, 200)
        self.assertEqual(self.state()['failure'], 'worker_recovery_hold')
        self.assertIn(self.job, worker.ACTIVE)

    def test_recovery_hold_survives_second_restart(self):
        class ImmediateThread:
            def __init__(self, target, args, daemon):
                self.target, self.args = target, args
            def start(self):
                self.target(*self.args)
        with mock.patch.object(worker, 'live_runner', side_effect=RuntimeError('ambiguous runner')):
            worker.recover_execution(self.job, 1, 200)
            worker.ACTIVE.clear()  # Fresh process memory after another restart.
            with mock.patch.object(worker.threading, 'Thread', ImmediateThread):
                worker.recover_orphans()
        self.assertEqual(self.state()['failure'], 'worker_recovery_hold')
        self.assertIn(self.job, worker.ACTIVE)

    def test_export_restart_preserves_artifact_for_reconciliation(self):
        artifact = self.attempt / 'staged' / 'generated.mp4'
        artifact.write_bytes(b'keep-derived-artifact')
        worker.write(self.owner / 'state.json', {'job_id': self.job, 'attempt': 1, 'state': 'exporting'})
        worker.recover_orphans()
        self.assertEqual(self.state()['state'], 'export_failed')
        self.assertEqual(artifact.read_bytes(), b'keep-derived-artifact')


if __name__ == '__main__':
    unittest.main()
