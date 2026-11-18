from test_runner import r, fixture, NOW, FINISH_ADMISSION
from pathlib import Path
from unittest.mock import patch
import json
import tempfile
import threading
from contextlib import ExitStack
import unittest


class ControllerOwnershipTests(unittest.TestCase):
    def job(self, root):
        job = r.Runner(Path('.'), Path(root))
        job.owner_validated = True
        job.admission = FINISH_ADMISSION
        job.finish_state = Path(root) / 'finish'
        job.finish_state.mkdir()
        (job.finish_state / 'admission.json').write_text(json.dumps(dict(failed=False, recovery_required=False, active={})))
        (job.finish_state / 'recovery.json').write_text(json.dumps(dict(plan_id=42, plan_sha256='b'*64, problems=0, verified=1, delete_ever_issued=1, checked_at=r.utcnow().isoformat())))
        plan, _ = fixture()
        job.status.update(current_plan=42, started_plan_sha256=plan['plan_sha256'], recovery_history={'42':dict(sha256='b'*64, issued=1)})
        return job, plan

    def test_on_is_read_only_and_requires_controller_master(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            job.active = lambda: dict(plans=[], batches=0)
            plan['master_deletion_enabled'] = False
            job.plan = lambda _: plan
            job.api = lambda *_: self.fail('ON cannot mutate')
            with self.assertRaises(r.Halt): job.master(True)
            plan['master_deletion_enabled'] = True
            job.master(True)

    def test_off_pauses_only_exact_owned_plan_once(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            plan.update(mode='enabled')
            plan['results'][0].update(state='unverified', delete_ever_issued=True)
            job.active = lambda: dict(plans=[42], batches=0)
            job.plan = lambda _: plan
            calls = []
            def pause(method, path, body):
                calls.append((method, path, body)); plan.update(mode='paused')
            job.api = pause
            job.master(False)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][1], 'plans/42/control')
            self.assertEqual(calls[0][2]['plan_sha256'], 'b'*64)

    def test_unknown_or_digest_mismatch_never_mutates(self):
        for plans, sha in (([99], 'b'*64), ([42], 'c'*64)):
            with tempfile.TemporaryDirectory() as root:
                job, plan = self.job(root)
                job.active = lambda: dict(plans=plans, batches=0)
                job.plan = lambda _: plan
                job.status['started_plan_sha256'] = sha
                job.api = lambda *_: self.fail('foreign work must remain untouched')
                with self.assertRaises(r.Halt): job.master(False)

    def test_latched_restart_preserves_status_stop_and_never_calls_control(self):
        for latch in ('last_error', 'recovery_error', 'shutdown_or_recovery_failed'):
            with tempfile.TemporaryDirectory() as root:
                job, _ = self.job(root)
                job.owner_validated = False
                job.status[latch] = True if latch.endswith('failed') else 'latched'
                status = json.dumps(job.status)
                (job.root / 'status.json').write_text(status)
                (job.root / 'STOP').write_bytes(b'owned STOP')
                job.alert = lambda _: None
                job.active = lambda: self.fail('latched restart is read only')
                with self.assertRaises(r.Halt): job.run()
                job.fail('restart refused')
                self.assertEqual((job.root / 'status.json').read_text(), status)
                self.assertEqual((job.root / 'STOP').read_bytes(), b'owned STOP')

    def test_real_recovery_publishes_checkpoint_without_clearing_ownership(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            plan['results'][0].update(state='deleted', delete_ever_issued=True)
            job.status['recovery_history'] = {}
            report = dict(plan_id=42, plan_sha256='b'*64, items=1, delete_ever_issued=1,
                          verified=1, problems=0, checked_at=NOW.isoformat())
            proc = type('Result', (), dict(returncode=0, stdout=json.dumps(report).encode()))()
            with patch.object(r.subprocess, 'run', return_value=proc), patch.object(r, 'utcnow', return_value=NOW):
                job.recovery(plan)
            status = json.loads((job.root / 'status.json').read_bytes())
            self.assertEqual(status['current_plan'], 42)
            self.assertEqual(status['recovery_plan_id'], 42)
            self.assertEqual(json.loads((job.root / 'recovery-42.json').read_bytes()), report)

    def test_actual_dryrun_and_on_share_controller_master(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            job.active = lambda: dict(plans=[], batches=0)
            job.plan = lambda _: plan
            self.assertEqual(job.await_dry(42), plan)
            job.master(True)
            plan['master_deletion_enabled'] = False
            with self.assertRaises(r.Halt): job.await_dry(42)

    def test_live_heartbeat_failure_takes_owned_stop_path(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            job.status.update(shutdown_or_recovery_failed=True, recovery_error='ownership heartbeat failed')
            job.plan = lambda _: plan
            with patch.object(job, 'master') as stop, patch.object(job, 'drain'), patch.object(job, 'recovery'), patch.object(job, 'alert'):
                with self.assertRaises(r.Halt): job.monitor(42)
                job.fail('live ownership failure requires shutdown')
                stop.assert_called_once_with(False, emergency=True)
            self.assertTrue(json.loads((job.root / 'status.json').read_bytes())['shutdown_or_recovery_failed'])

    def test_start_holds_snapshot_through_pid_publication(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            job.status['last_error'] = None
            path = job.root / 'status.json'
            path.write_text(json.dumps(job.status))
            def validate():
                job.status['runner_pid'] = r.os.getpid()
                with (job.root / 'ownership.lock').open('a') as observer:
                    with self.assertRaises(BlockingIOError):
                        r.fcntl.flock(observer, r.fcntl.LOCK_SH | r.fcntl.LOCK_NB)
            with patch.object(job, 'validate_owner', side_effect=validate):
                job.start()
            self.assertEqual(json.loads(path.read_bytes())['runner_pid'], r.os.getpid())
            job.lock.close()

    def test_pause_failure_is_reconciled_before_retry_and_never_abandoned(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            plan.update(mode='enabled')
            job.active = lambda: dict(plans=[42] if plan['mode'] == 'enabled' else [], batches=0)
            job.plan = lambda _: plan
            calls = []
            def pause(*args):
                calls.append(args)
                if len(calls) == 1: raise r.Halt('HTTP transport failed')
                plan['mode'] = 'paused'
            job.api = pause
            with patch.object(job, 'alert'), patch.object(r.time, 'sleep'):
                job.contain()
            self.assertEqual(plan['mode'], 'paused')
            self.assertEqual(len(calls), 2)
            self.assertTrue(job.status['shutdown_or_recovery_failed'])

    def test_d_failure_stops_monitor_and_prevents_enable(self):
        for name, record in [('admission.json', dict(failed=True, recovery_required=True)),
                             ('recovery.json', dict(problems=1, verified=0, delete_ever_issued=1)),
                             ('recovery.json', dict(refresh_error='failed'))]:
            with tempfile.TemporaryDirectory() as root:
                job, plan = self.job(root)
                (job.finish_state / name).write_text(json.dumps(record))
                job.active = lambda: dict(plans=[], batches=0)
                job.plan = lambda _: plan
                with self.assertRaises((r.Halt, KeyError)): job.monitor(42)
                with self.assertRaises((r.Halt, KeyError)): job.master(True)

    def test_auto_pause_between_active_and_get_preserves_reason(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            plan.update(mode='paused', approval_reason='auto-paused: approval expired; original')
            job.active = lambda: dict(plans=[42], batches=0)
            job.plan = lambda _: plan
            job.api = lambda *_: self.fail('already paused plan must not be rewritten')
            job.master(False)
            self.assertEqual(r.execution_state(plan), 'expired')

    def test_successor_cannot_enable_while_d_consumes_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            called, started, finished = threading.Event(), threading.Event(), threading.Event()
            job.api = lambda *args: called.set()
            result = []
            def enable():
                started.set()
                result.append(job.enable(plan))
                finished.set()
            with (job.finish_state / 'lock').open('a') as tick:
                r.fcntl.flock(tick, r.fcntl.LOCK_EX)
                thread = threading.Thread(target=enable)
                thread.start()
                self.assertTrue(started.wait(1))
                self.assertFalse(called.wait(0.05))
                r.fcntl.flock(tick, r.fcntl.LOCK_UN)
                self.assertTrue(finished.wait(1))
                thread.join()
            self.assertEqual(result, [True])
            self.assertTrue(called.is_set())

    def test_stop_while_waiting_for_d_prevents_successor(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            (job.root / 'STOP').write_text('owned natural stop')
            job.api = lambda *args: self.fail('STOP must prevent new enable')
            self.assertFalse(job.enable(plan))

    def test_abandoned_d_attempt_cannot_enable_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            with ExitStack() as stack:
                FINISH_ADMISSION.Attempt.start(job.finish_state, 'tick', stack)
            job.api = lambda *args: self.fail('abandoned D tick cannot authorize cleanup')
            with self.assertRaises(r.Halt): job.enable(plan)
            with self.assertRaises(r.Halt): job.finish_health()

    def test_live_recovery_can_coexist_but_orphan_directory_stops(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            with ExitStack() as stack:
                FINISH_ADMISSION.Attempt.start(job.finish_state, 'recovery', stack)
                job.finish_health()
                job.api = lambda *args: None
                self.assertTrue(job.enable(plan))
            with self.assertRaises(r.Halt): job.finish_health()

    def test_runner_has_no_render_capability(self):
        text = Path(r.__file__).read_text()
        self.assertNotIn('api.render.com', text)
        self.assertFalse(hasattr(r.Runner, 'render'))

if __name__ == '__main__': unittest.main()
