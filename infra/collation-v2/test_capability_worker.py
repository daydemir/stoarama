import importlib.util
import json
import http.client
import threading
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
        worker.DRAINING=False
        worker.STOPPING=False
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



class PortableWorkerTest(RecoveryTest):
    def test_volume_missing_before_root_creation(self):
        target=pathlib.Path(self.directory.name)/'not-created'/'worker'
        with mock.patch.object(worker,'ROOT',target), mock.patch.object(worker,'MOUNT',str(target.parent)), mock.patch.object(worker,'UUID','test-uuid'), mock.patch.object(worker,'VOLUME_IDENTITY',None):
            with self.assertRaises(worker.VolumeUnavailable):worker.initialize_root()
        self.assertFalse(target.parent.exists())

    def test_remount_refuses_write_and_route(self):
        state=self.owner/'state.json';before=state.read_bytes()
        with mock.patch.object(worker,'MOUNT',str(worker.ROOT)), mock.patch.object(worker,'UUID','test'), mock.patch.object(worker,'VOLUME_IDENTITY',('100',1,2)), mock.patch.object(worker,'mounted_identity',return_value=('101',1,2)):
            with self.assertRaises(worker.VolumeUnavailable):worker.write(state,{'overwritten':True})
            server=worker.http.server.ThreadingHTTPServer(('127.0.0.1',0),worker.Handler)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            try:
                conn=http.client.HTTPConnection(*server.server_address)
                conn.request('GET','/ready');response=conn.getresponse()
                self.assertEqual(response.status,503);response.read();conn.close()
            finally:server.shutdown();server.server_close();thread.join()
        self.assertEqual(state.read_bytes(),before)
        self.assertFalse(state.with_suffix('.json.pending').exists())

    def test_drain_blocks_admission_and_preserves_job(self):
        (worker.ROOT/'build-complete').touch()
        artifact=self.attempt/'keep.bin';artifact.write_bytes(b'recoverable')
        work={'batch_id':'best14-20261021','clips':[{'size_bytes':1}]}
        job=worker.hashlib.sha256(json.dumps(work,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        payload={'job_id':job,'request':{'work':work,'timeout_seconds':1800,'sources':{'1':{}}}}
        with mock.patch.object(worker.os,'sync'):
            self.assertTrue(worker.begin_drain('drain')['drained'])
        server=worker.http.server.ThreadingHTTPServer(('127.0.0.1',0),worker.Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            conn=http.client.HTTPConnection(*server.server_address)
            conn.request('POST','/job',json.dumps(payload),{'Content-Type':'application/json'})
            response=conn.getresponse();self.assertEqual(response.status,409)
            self.assertEqual(json.loads(response.read())['error'],'worker_draining');conn.close()
        finally:server.shutdown();server.server_close();thread.join()
        self.assertFalse(worker.job_root(job).exists())
        self.assertEqual(artifact.read_bytes(),b'recoverable')
        self.assertEqual(json.loads((worker.ROOT/'drain.json').read_text())['mode'],'drain')

    def test_busy_drain_never_reports_synced(self):
        worker.ACTIVE.add(self.job)
        with mock.patch.object(worker.os,'sync') as sync:
            result=worker.begin_drain('drain')
        self.assertFalse(result['drained']);sync.assert_not_called()
        self.assertTrue(self.owner.exists());self.assertIn(self.job,worker.ACTIVE)

    def test_stop_never_signals_unknown_process(self):
        worker.ACTIVE.add(self.job)
        with mock.patch.object(worker,'live_runner',return_value=123),mock.patch.object(worker.os,'getpgid',return_value=999),mock.patch.object(worker.os,'killpg') as kill:
            with self.assertRaises(RuntimeError):worker.begin_drain('stop')
        kill.assert_not_called();self.assertIn(self.job,worker.ACTIVE)

    def test_stop_prevents_accepted_job_from_launching(self):
        worker.STOPPING=True;worker.DRAINING=True;worker.ACTIVE.add(self.job)
        with mock.patch.object(worker.subprocess,'Popen') as start:
            worker.execute(self.job,1)
        start.assert_not_called();self.assertEqual(self.state()['state'],'failed')
        self.assertTrue(self.attempt.exists())

    def release_request(self):
        server=worker.http.server.ThreadingHTTPServer(('127.0.0.1',0),worker.Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            conn=http.client.HTTPConnection(*server.server_address)
            conn.request('POST','/release',json.dumps({'job_id':self.job}))
            response=conn.getresponse();status=response.status;response.read();conn.close()
            return status
        finally:server.shutdown();server.server_close();thread.join()

    def release_fixture(self,state='exported',verified=True):
        key='joined/best14-20261021/test-owned.mp4'
        output=self.attempt/'staged'/'outputs'/key;output.parent.mkdir(parents=True);output.write_bytes(b'output')
        sibling=output.with_name('unlisted.mp4');sibling.write_bytes(b'keep')
        failed=self.attempt/'failed.bin';failed.write_bytes(b'failed-attempt')
        worker.write(self.owner/'state.json',{'job_id':self.job,'attempt':1,'state':state,'result':{'manifest':{'outputs':[{'object_key':key}]}},'export_receipts':[{'full_readback_verified':verified}]})
        return output,sibling,failed

    def test_guarded_verified_release_removes_only_listed_output(self):
        output,sibling,failed=self.release_fixture()
        with mock.patch.object(worker,'MOUNT','/retained'),mock.patch.object(worker,'require_volume'):
            self.assertEqual(self.release_request(),200)
        self.assertFalse(output.exists());self.assertTrue(sibling.exists());self.assertTrue(failed.exists())
        self.assertTrue(output.parent.is_dir());self.assertEqual(self.state()['state'],'released')

    def test_missing_readback_retains_all_media(self):
        output,sibling,failed=self.release_fixture(verified=False)
        self.assertEqual(self.release_request(),409)
        self.assertTrue(output.exists());self.assertTrue(sibling.exists());self.assertTrue(failed.exists())
        self.assertEqual(self.state()['state'],'exported')

    def test_failed_export_retains_all_media(self):
        output,sibling,failed=self.release_fixture(state='export_failed')
        self.assertEqual(self.release_request(),409)
        self.assertTrue(output.exists());self.assertTrue(sibling.exists());self.assertTrue(failed.exists())
        self.assertEqual(self.state()['state'],'export_failed')

    def test_configurable_root_and_endpoint_without_starting_worker(self):
        import os,subprocess,sys
        root=pathlib.Path(self.directory.name)/'portable-root'
        env=dict(os.environ,STOARAMA_WORKER_ROOT=str(root),STOARAMA_WORKER_PORT='8101',STOARAMA_WORKER_BIND='10.0.0.2',STOARAMA_WORKER_BINARY='/opt/pinned/collation-job')
        env.pop('STOARAMA_VOLUME_MOUNT',None);env.pop('STOARAMA_VOLUME_UUID',None)
        code="import runpy,json;w=runpy.run_path('capability-worker.py',run_name='config_test');print(json.dumps([str(w['ROOT']),w['BIND'],w['PORT'],str(w['BINARY'])]))"
        result=subprocess.check_output([sys.executable,'-c',code],cwd=pathlib.Path(__file__).parent,env=env,text=True)
        self.assertEqual(json.loads(result),[str(root),'10.0.0.2',8101,'/opt/pinned/collation-job'])
        self.assertTrue(root.is_dir())

    def test_default_configuration_preserves_legacy_mode(self):
        self.assertEqual(worker.BIND,'127.0.0.1');self.assertEqual(worker.PORT,8080)
        self.assertEqual(worker.MOUNT,'');self.assertEqual(worker.UUID,'')
        worker.require_volume()

if __name__ == '__main__':
    unittest.main()
