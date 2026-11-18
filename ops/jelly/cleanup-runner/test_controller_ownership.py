from test_runner import r, fixture, NOW
from pathlib import Path
from unittest.mock import patch
import json
import tempfile
import unittest


class ControllerOwnershipTests(unittest.TestCase):
    def job(self, root):
        job = r.Runner(Path('.'), Path(root))
        job.owner_validated = True
        plan, _ = fixture()
        job.status.update(current_plan=42, started_plan_sha256=plan['plan_sha256'])
        return job, plan

    def test_on_is_read_only_and_requires_controller_master(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            job.active = lambda: dict(plans=[], batches=0)
            job.plan = lambda _: plan
            job.api = lambda *_: self.fail('ON cannot mutate')
            with self.assertRaises(r.Halt): job.master(True)
            plan['master_deletion_enabled'] = True
            job.master(True)

    def test_off_pauses_only_exact_owned_plan_once(self):
        with tempfile.TemporaryDirectory() as root:
            job, plan = self.job(root)
            plan.update(mode='enabled')
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

    def test_runner_has_no_render_capability(self):
        text = Path(r.__file__).read_text()
        self.assertNotIn('api.render.com', text)
        self.assertFalse(hasattr(r.Runner, 'render'))

if __name__ == '__main__': unittest.main()
