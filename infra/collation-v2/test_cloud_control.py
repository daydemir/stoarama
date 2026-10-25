import hashlib
import fcntl
import importlib.util
import io
import json
import os
import pathlib
import sqlite3
import tarfile
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('cloud', pathlib.Path(__file__).with_name('cloud-control.py'))
cloud = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cloud)


class CloudControlTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name).resolve()
        for name in ('execution.sqlite3', 'structure-screen.sqlite3'):
            db = sqlite3.connect(self.root / name)
            db.execute('CREATE TABLE proof(id INT PRIMARY KEY, value TEXT)')
            db.execute("INSERT INTO proof VALUES(1,'preserved')")
            db.commit()
            db.close()

    def test_checkpoint_preserves_databases_and_excludes_credentials(self):
        (self.root / 'controller.env').write_text('PRIVATE_SECRET=must-not-export')
        (self.root / 'run-authority.json').write_text('{"additional_budget_usd":100}')
        body = cloud.checkpoint_bytes(self.root)
        with tarfile.open(fileobj=io.BytesIO(body), mode='r:gz') as archive:
            self.assertEqual(set(archive.getnames()), {'execution.sqlite3', 'structure-screen.sqlite3', 'run-authority.json'})
            for name in ('execution.sqlite3', 'structure-screen.sqlite3'):
                p = self.root / ('restored-' + name)
                p.write_bytes(archive.extractfile(name).read())
                db = sqlite3.connect(p)
                try:
                    self.assertEqual(db.execute('SELECT value FROM proof').fetchone()[0], 'preserved')
                    self.assertEqual(db.execute('PRAGMA quick_check').fetchone()[0], 'ok')
                finally:
                    db.close()

    def test_checkpoint_rejects_symlinked_metadata(self):
        (self.root / 'run-authority.json').symlink_to(self.root / 'outside.json')
        (self.root / 'outside.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'checkpoint_symlink'):
            cloud.checkpoint_bytes(self.root)

    def test_checkpoint_put_uncertainty_requires_full_readback_before_receipt(self):
        uploaded = []
        class Response(io.BytesIO):
            status = 200
        def opened(request, timeout):
            if isinstance(request, cloud.urllib.request.Request):
                self.assertEqual(request.get_header('If-none-match'), '*')
                uploaded.append(request.data)
                raise TimeoutError()
            return Response(uploaded[-1])
        with mock.patch.dict(os.environ, {'R2_BUCKET': 'bucket'}), \
                mock.patch.object(cloud.finite, 'presign', return_value={'url': 'https://example.invalid/checkpoint', 'headers': {'If-None-Match': '*'}}), \
                mock.patch.object(cloud.urllib.request, 'urlopen', side_effect=opened):
            cloud.archive_checkpoint(self.root)
        receipt = json.loads((self.root / 'checkpoint-receipt.json').read_text())
        self.assertEqual(receipt['sha256'], hashlib.sha256(uploaded[0]).hexdigest())
        self.assertTrue(receipt['full_readback_verified'])

    def test_corrupt_readback_does_not_write_success_receipt(self):
        class Response(io.BytesIO):
            status = 200
        with mock.patch.dict(os.environ, {'R2_BUCKET': 'bucket'}), \
                mock.patch.object(cloud.finite, 'presign', return_value={'url': 'https://example.invalid/checkpoint', 'headers': {}}), \
                mock.patch.object(cloud.urllib.request, 'urlopen', side_effect=[Response(), Response(b'corrupt')]):
            with self.assertRaisesRegex(ValueError, 'checkpoint_readback_differs'):
                cloud.archive_checkpoint(self.root)
        self.assertFalse((self.root / 'checkpoint-receipt.json').exists())

    def test_finite_deadline_and_operator_stop_do_not_depend_on_jelly(self):
        config = {'created_at_unix': 100, 'max_runtime_seconds': 3600}
        with mock.patch.object(cloud.time, 'time', return_value=3700):
            self.assertEqual(cloud.terminal(self.root, config), 'finite_controller_deadline')
        (self.root / 'STOP').write_text('{}')
        self.assertEqual(cloud.terminal(self.root, config), 'stop_requested')

    def test_backup_failure_pauses_every_worker_before_retry(self):
        workers = [{'id': str(i)} for i in range(4)]
        with mock.patch('sys.argv', ['cloud', '--run-root', str(self.root)]), \
                mock.patch.object(cloud, 'load_workers', return_value=workers), \
                mock.patch.object(cloud, 'preflight', return_value=(workers, {'created_at_unix': 100, 'max_runtime_seconds': 604800})), \
                mock.patch.object(cloud, 'terminal', return_value=None), \
                mock.patch.object(cloud.subprocess, 'Popen') as popen, \
                mock.patch.object(cloud, 'archive_checkpoint', side_effect=RuntimeError('network')), \
                mock.patch.object(cloud, 'stop_screen'), \
                mock.patch.object(cloud.finite, 'pause_for_control_storage') as pause, \
                mock.patch.object(cloud.finite, 'main') as dispatch:
            popen.return_value.poll.return_value = None
            with self.assertRaisesRegex(RuntimeError, 'network'):
                cloud.main()
            pause.assert_called_once_with(self.root, workers)
            dispatch.assert_not_called()

    def test_unconfirmed_terminal_pause_retries_on_restart(self):
        workers = [{'id': str(i)} for i in range(4)]
        with mock.patch.object(cloud.finite, 'pause_for_control_storage'), \
                mock.patch.object(cloud.finite, 'provider', side_effect=[RuntimeError('offline')] + [{'microvm': {'state': 'paused'}}] * 3):
            with self.assertRaisesRegex(RuntimeError, 'pause_unconfirmed'):
                cloud.pause_and_confirm(self.root, workers)
        with mock.patch.object(cloud.finite, 'pause_for_control_storage'), \
                mock.patch.object(cloud.finite, 'provider', return_value={'microvm': {'state': 'paused'}}):
            cloud.pause_and_confirm(self.root, workers)
        self.assertTrue(all(json.loads((self.root / 'cloud-stop-outcomes.json').read_text())['confirmed_paused'].values()))

    def test_scaling_roster_is_bounded_at_eight(self):
        for count in (4, 8):
            (self.root / 'execution-workers.json').write_text(json.dumps([{'id': str(x)} for x in range(count)]))
            self.assertEqual(len(cloud.load_workers(self.root)), count)
        (self.root / 'execution-workers.json').write_text(json.dumps([{'id': str(x)} for x in range(9)]))
        with self.assertRaisesRegex(ValueError, 'worker_roster'):
            cloud.load_workers(self.root)

    def test_interrupted_screen_batch_retains_completed_cursor(self):
        with mock.patch.dict(os.environ, {'BEST14_RUN_ROOT': str(self.root)}):
            spec = importlib.util.spec_from_file_location('screen', pathlib.Path(__file__).with_name('source-screen.py'))
            screen = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(screen)
        p = self.root / 'partial-screen.sqlite3'
        db = sqlite3.connect(p)
        db.execute('CREATE TABLE screened(clip_id INTEGER PRIMARY KEY,recording_id INTEGER,sha256 BLOB,status INTEGER,reason,reads INTEGER,observed_at REAL)')
        db.execute('CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT)')
        for i in range(1, 18):
            row = {'clip_id': i, 'recording_id': 1, 'sha256': 'ab'*32}
            screen.save_result(db, row, ('hold', 'range_transport_failure', 1), i*2, checkpoint=i%16==0)
        db.close()  # Simulate process interruption: the uncommitted 17th row rolls back.
        db = sqlite3.connect(p)
        try:
            self.assertEqual(db.execute('SELECT count(*) FROM screened').fetchone()[0], 16)
            self.assertEqual(dict(db.execute('SELECT key,value FROM meta'))['next_line'], '32')
        finally:
            db.close()

    def test_controller_retirement_needs_two_verified_archives_and_exact_identity(self):
        config = {'droplet_id': 123, 'name': 'controller'}
        with mock.patch.object(cloud.urllib.request, 'urlopen') as network:
            with self.assertRaises(FileNotFoundError):
                cloud.retire_controller(self.root, config)
            network.assert_not_called()
        (self.root / 'static-archive-receipt.json').write_text(json.dumps({'full_readback_verified': True, 'key': 'staging/best14-20261021/cloud-inputs/static.tar.gz'}))
        (self.root / 'checkpoint-receipt.json').write_text(json.dumps({'full_readback_verified': True, 'observed_at': cloud.time.time(), 'key': 'staging/best14-20261021/control-checkpoints/state.tar.gz'}))
        class Response(io.BytesIO):
            status = 204
        wrong = Response(json.dumps({'droplet': {'id': 123, 'name': 'other', 'tags': []}}).encode())
        with mock.patch.dict(os.environ, {'DIGITALOCEAN_TOKEN': 'test-only'}), \
                mock.patch.object(cloud.urllib.request, 'urlopen', return_value=wrong) as network:
            with self.assertRaisesRegex(ValueError, 'provider_identity'):
                cloud.retire_controller(self.root, config)
            self.assertEqual(network.call_count, 1)
        right = Response(json.dumps({'droplet': {'id': 123, 'name': 'controller', 'tags': ['best14-controller-20261024']}}).encode())
        with mock.patch.dict(os.environ, {'DIGITALOCEAN_TOKEN': 'test-only'}), \
                mock.patch.object(cloud.urllib.request, 'urlopen', side_effect=[right, Response()]) as network:
            cloud.retire_controller(self.root, config)
            self.assertEqual(network.call_args_list[-1].args[0].get_method(), 'DELETE')
            self.assertTrue(all('/droplets/123' in call.args[0].full_url for call in network.call_args_list))

    def test_snapshot_requires_stop_and_both_source_writer_locks(self):
        spec = importlib.util.spec_from_file_location('prepare', pathlib.Path(__file__).with_name('prepare-cloud-state.py'))
        prepare = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(prepare)
        for name in ('executor.lock', 'structure-screen.lock'):
            (self.root / name).write_bytes(b'')
        with mock.patch.object(prepare, '_copy_state_locked', return_value={'copied': True}) as copy:
            with self.assertRaisesRegex(ValueError, 'source_not_stopped'):
                prepare.copy_state(self.root, self.root, self.root / 'out', self.root, {})
            (self.root / 'STOP').write_text('{}')
            for name in ('executor.lock', 'structure-screen.lock'):
                with (self.root / name).open('r') as owner:
                    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    with self.assertRaisesRegex(ValueError, 'writer_active'):
                        prepare.copy_state(self.root, self.root, self.root / 'out', self.root, {})
            copy.assert_not_called()
            self.assertEqual(prepare.copy_state(self.root, self.root, self.root / 'out', self.root, {}), {'copied': True})
            copy.assert_called_once()

    def test_controller_lifetime_is_included_in_existing_budget(self):
        db = sqlite3.connect(self.root / 'execution.sqlite3')
        db.row_factory = sqlite3.Row
        try:
            db.execute('CREATE TABLE jobs(id TEXT, state TEXT, source_bytes INT, result TEXT, finished_at REAL,attempt INT)')
            db.execute('CREATE TABLE export_attempts(id TEXT,count INT)')
            db.execute('CREATE TABLE head_attempts(count INT)')
            worker = {'created_at': '2026-10-22T00:00:00Z', 'size': {'disk': 80}}
            now = 1792627200 + 3600
            (self.root / 'controller-reservation.json').write_text(json.dumps({'schema': 'best14-cloud-controller-v1','hourly_usd': .01786,'created_at_unix': now - 3600}))
            with mock.patch.object(cloud.finite.time, 'time', return_value=now):
                result = cloud.finite.budget(self.root, [worker], db)
            self.assertAlmostEqual(result['controller_upper_bound_usd'], 5.01786)
            self.assertGreater(result['compute_and_disk_upper_bound_usd'], 1.01786)
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
