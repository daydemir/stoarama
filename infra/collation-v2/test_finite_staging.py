import importlib.util
import io
import os
import pathlib
import sqlite3
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('finite', pathlib.Path(__file__).with_name('finite-staging.py'))
finite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(finite)


class ExecutorTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = pathlib.Path(self.directory.name)
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        self.db.execute('CREATE TABLE jobs(id TEXT PRIMARY KEY,path TEXT,state TEXT,recording_id INT,local_date TEXT,hour INT,source_bytes INT,source_files INT,nas_sources INT,attempt INT,worker_id TEXT,started_at REAL,finished_at REAL,result TEXT)')
        self.db.execute('CREATE TABLE deferred_jobs(id TEXT PRIMARY KEY,until REAL)')
        self.db.execute('CREATE TABLE export_attempts(id TEXT PRIMARY KEY,count INTEGER)')
        self.db.execute('CREATE TABLE head_attempts(count INTEGER)')
        self.db.execute('CREATE TABLE submission_attempts(id TEXT PRIMARY KEY,count INTEGER)')

    def test_confirmed_absent_submission_retries_same_identity(self):
        row = {'id': 'same-frozen-hash', 'attempt': 1}
        with mock.patch.object(finite, 'guest', side_effect=[finite.GuestStatus(404), {'active_jobs': 0, 'build_complete': True}]), \
                mock.patch.object(finite, 'read_work', return_value={'frozen': True}), \
                mock.patch.object(finite, 'submit') as submit:
            finite.reconcile(self.root, self.db, {'id': 'owner'}, row)
        submit.assert_called_once_with(self.db, {'id': 'owner'}, row, {'frozen': True}, media_attempt=1)

    def test_unknown_submission_is_not_repeated(self):
        with mock.patch.object(finite, 'guest', side_effect=RuntimeError('provider_transport_uncertain')), \
                mock.patch.object(finite, 'submit') as submit:
            with self.assertRaises(RuntimeError):
                finite.reconcile(self.root, self.db, {'id': 'owner'}, {'id': 'same-frozen-hash'})
        submit.assert_not_called()

    def test_capacity_rejection_refunds_attempt_and_defers(self):
        for code in (409, 503, 507):
            for prior_attempt in (0, 1):
                with self.subTest(code=code, prior_attempt=prior_attempt):
                    self.db.execute('DELETE FROM jobs')
                    self.db.execute('DELETE FROM submission_attempts')
                    self.db.execute('INSERT INTO jobs(id,state,attempt) VALUES(?,?,?)', ('job', 'queued' if prior_attempt == 0 else 'submitted', prior_attempt))
                    row = self.db.execute('SELECT * FROM jobs').fetchone()
                    work = {'clips': [{'clip_id': 1, 'bucket': 'b', 'object_key': 'k'}]}
                    with mock.patch.object(finite, 'head', return_value='etag'), \
                            mock.patch.object(finite, 'presign', return_value={}), \
                            mock.patch.object(finite, 'guest', side_effect=[{'active_jobs': 0, 'build_complete': True}, finite.GuestStatus(code)]):
                        self.assertFalse(finite.submit(self.db, {'id': 'owner'}, row, work, media_attempt=prior_attempt + 1))
                    after = self.db.execute('SELECT state,attempt FROM jobs').fetchone()
                    self.assertEqual(after['state'], row['state'])
                    self.assertEqual(after['attempt'], prior_attempt)
                    self.assertEqual(self.db.execute('SELECT count FROM submission_attempts').fetchone()[0], 0)

    def test_interrupted_media_retry_uses_next_attempt(self):
        row = {'id': 'frozen', 'attempt': 2}
        with mock.patch.object(finite, 'guest', return_value={'job_id': 'frozen', 'attempt': 2, 'state': 'failed', 'failure': 'worker_restarted'}), \
                mock.patch.object(finite, 'read_work', return_value={'frozen': True}), \
                mock.patch.object(finite, 'submit') as submit:
            finite.reconcile(self.root, self.db, {'id': 'owner'}, row)
        self.assertEqual(submit.call_args.kwargs['media_attempt'], 3)

    def test_waiting_prefix_does_not_starve_ready_job(self):
        screen = sqlite3.connect(self.root / 'structure-screen.sqlite3')
        screen.execute('CREATE TABLE screened(clip_id INT PRIMARY KEY,sha256 BLOB,status INTEGER)')
        screen.execute('INSERT INTO screened VALUES(?,?,?)', (201, bytes.fromhex('ab' * 32), 1))
        screen.commit()
        screen.close()
        for n in range(1, 202):
            self.db.execute('INSERT INTO jobs(id,state,recording_id,local_date,hour,source_bytes,source_files,nas_sources,attempt) VALUES(?,?,?,?,?,?,?,?,?)', (str(n), 'queued', n, '2026-10-08', 1, 1000, 1, 0, 0))
        self.db.commit()
        work = lambda row: {'clips': [{'clip_id': int(row['id']), 'sha256': 'ab' * 32}]}
        with mock.patch.object(finite, 'read_work', side_effect=work), mock.patch.object(finite, 'submit', return_value=True) as submit:
            finite.dispatch(self.root, self.db, {'id': 'owner'}, {'upper_bound_usd': 1})
        self.assertEqual(submit.call_args[0][2]['id'], '201')

    def test_head_wait_does_not_starve_later_ready_job(self):
        screen = sqlite3.connect(self.root / 'structure-screen.sqlite3')
        screen.execute('CREATE TABLE screened(clip_id INT PRIMARY KEY,sha256 TEXT,status TEXT)')
        screen.executemany("INSERT INTO screened VALUES(?,'sha','structure_present')", [(1,), (2,)])
        screen.commit()
        screen.close()
        for n in (1, 2):
            self.db.execute('INSERT INTO jobs(id,state,recording_id,local_date,hour,source_bytes,source_files,nas_sources,attempt) VALUES(?,?,?,?,?,?,?,?,?)', (str(n), 'queued', n, '2026-10-08', 1, 1000, 1, 0, 0))
        self.db.commit()
        with mock.patch.object(finite, 'read_work', side_effect=lambda row: {'clips': [{'clip_id': int(row['id']), 'sha256': 'sha'}]}), mock.patch.object(finite, 'submit', side_effect=[False, True]) as submit:
            finite.dispatch(self.root, self.db, {'id': 'owner'}, {'upper_bound_usd': 1})
        self.assertEqual([call.args[2]['id'] for call in submit.call_args_list], ['1', '2'])

    def test_budget_hard_stop_preserves_stuck_intent(self):
        self.db.execute("INSERT INTO jobs(id,state) VALUES('uncertain','submitted')")
        self.assertFalse(finite.must_pause(self.root, self.db, {'upper_bound_usd': 90}))
        self.assertTrue(finite.must_pause(self.root, self.db, {'upper_bound_usd': 94}))
        self.assertEqual(self.db.execute('SELECT state FROM jobs').fetchone()[0], 'submitted')

    def test_pause_failure_does_not_skip_remaining_workers(self):
        with mock.patch.object(finite, 'provider', side_effect=[RuntimeError('offline'), {}]) as provider:
            finite.pause_workers(self.root, [{'id': 'a'}, {'id': 'b'}], {'upper_bound_usd': 94})
        self.assertEqual([c.args[1] for c in provider.call_args_list], ['/a/pause', '/b/pause'])
        self.assertFalse((self.root / 'STOP').exists())

    def test_budget_accepts_provider_nanosecond_timestamp(self):
        worker = {'created_at': '2026-10-22T01:16:58.022515573Z', 'size': {'disk': 80}}
        with mock.patch.object(finite.time, 'time', return_value=1792635418):
            ledger = finite.budget(self.root, [worker], self.db)
        self.assertGreater(ledger['compute_and_disk_upper_bound_usd'], 0)

    def test_uncertain_export_retains_storage_reservation(self):
        worker = {'created_at': '2026-10-22T01:16:58.022515573Z', 'size': {'disk': 80}}
        with mock.patch.object(finite.time, 'time', return_value=1792635418):
            before = finite.budget(self.root, [worker], self.db)
            self.db.execute('INSERT INTO jobs(id,state,attempt,source_bytes) VALUES(?,?,?,?)', ('export', 'held_export', 1, finite.GIB))
            self.db.execute('INSERT INTO export_attempts VALUES(?,?)', ('export', 3))
            after = finite.budget(self.root, [worker], self.db)
        self.assertGreater(after['r2_storage_upper_bound_usd'], before['r2_storage_upper_bound_usd'])

    def test_archive_receipt_requires_complete_matching_readback(self):
        state = {'job_id': 'frozen-job', 'attempt': 1, 'result': {'manifest': {}},
                 'export_receipts': [{'bytes': 10}]}
        uploaded = []

        class Response(io.BytesIO):
            status = 200

        def opened(request, timeout):
            if request.get_method() == 'PUT':
                uploaded.append(request.data)
                self.assertEqual(request.get_header('If-none-match'), '*')
                return Response()
            return Response(uploaded[-1])

        with mock.patch.dict(os.environ, {'R2_BUCKET': 'bucket'}), \
                mock.patch.object(finite, 'presign', return_value={'url': 'https://example.invalid/receipt', 'headers': {'If-None-Match': '*'}}), \
                mock.patch.object(finite.urllib.request, 'urlopen', side_effect=opened):
            receipt = finite.archive_receipt(state)
            self.assertEqual(receipt['export_bytes'], 10)
            self.assertNotIn('result', receipt)
        with mock.patch.dict(os.environ, {'R2_BUCKET': 'bucket'}), \
                mock.patch.object(finite, 'presign', return_value={'url': 'https://example.invalid/receipt', 'headers': {}}), \
                mock.patch.object(finite.urllib.request, 'urlopen', side_effect=[Response(), Response(b'corrupted')]):
            with self.assertRaisesRegex(ValueError, 'readback_identity_differs'):
                finite.archive_receipt(state)


if __name__ == '__main__':
    unittest.main()
