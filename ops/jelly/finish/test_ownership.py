import copy
import json
import sys
import tempfile
from pathlib import Path
import unittest.mock
from unittest.mock import patch
from datetime import datetime, timezone, timedelta
import unittest
import ownership
from ownership import assemble_owned, parse_stop, validate_stop, POLICY_SHA, StopReason
from collector import PLAN, PLAN_SHA, timestamp

NOW = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)


def fixture():
    owner = dict(schema='stoarama.cleanup-owner.v1', owner_id='00000000-0000-0000-0000-000000000001',
                 baseline_plan_id=PLAN, baseline_plan_sha256=PLAN_SHA, initial_plan_id=1441,
                 initial_plan_sha256='b'*64, policy_sha256=POLICY_SHA)
    status = dict(current_plan=1441, next_plan=1442, started_plan_sha256='b'*64,
                  owner_id=owner['owner_id'], recovery_plan_id=1440, last_error=None,
                  recovery_error=None, shutdown_or_recovery_failed=False, lookahead_pending=False,
                  phase='enabling', updated_at=NOW.isoformat(),
                  recovery_history={'1440': {'sha256': PLAN_SHA, 'issued': 1860}})
    recovery = dict(plan_id=1440, plan_sha256=PLAN_SHA, delete_ever_issued=1860,
                    verified=1860, problems=0, checked_at=(NOW-timedelta(hours=1)).isoformat())
    plan = dict(sha256='b'*64, connection_id=13, account_id=47, policy_sha256=POLICY_SHA,
                allow_nas_only=False, mode='enabled', uncertain=0, issued=8, newest_issued_at=NOW.isoformat())
    db = dict(plans={'1441': plan, '1440': dict(plan, mode='retired', sha256=PLAN_SHA, issued=1860,
                 newest_issued_at=(NOW-timedelta(hours=2)).isoformat())}, enabled_plans=[1441], enabled_batches=[],
              inventory_mismatches=0, replay_failed=0, replay_pending=0, version='r2-copy-v6', boot='boot',
              cleanup_at=NOW.isoformat(), r2_copy=dict(mismatch_latched=0, holds_by_reason={}))
    api = dict(id=1441, plan_sha256='b'*64, master_deletion_enabled=True)
    return status, owner, recovery, copy.deepcopy(recovery), api, db


class OwnershipTests(unittest.TestCase):
    def test_failure_stop_has_valid_schema_but_never_allows_healthy_collection(self):
        _, owner, *_ = fixture()
        raw = json.dumps(dict(schema='stoarama.cleanup-stop.v1', owner_id=owner['owner_id'],
                              reason='failure', created_at=NOW.isoformat())).encode()
        self.assertEqual(parse_stop(raw, owner, NOW)['reason'], 'failure')
        with self.assertRaises(ValueError):
            validate_stop(raw, owner, NOW)

    def test_runner_finish_hold_is_an_idle_stop_but_failure_and_foreign_are_not(self):
        _, owner, *_ = fixture()
        stop = dict(schema='stoarama.cleanup-stop.v1', owner_id=owner['owner_id'],
                    reason='finish_admission_hold', created_at=NOW.isoformat())
        validate_stop(json.dumps(stop), owner, NOW)
        self.assertIs(parse_stop(json.dumps(stop), owner, NOW)['reason'], StopReason.FINISH_ADMISSION_HOLD)
        for change in (dict(owner_id='foreign'), dict(reason='failure'), dict(reason='finish_hold'),
                       dict(schema='stoarama.cleanup-stop.v0'), dict(created_at=(NOW+timedelta(seconds=1)).isoformat())):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_stop(json.dumps(dict(stop, **change)), owner, NOW)

    def test_plan_source_collects_a_held_idle_runner_after_reconcile(self):
        """refresh_recovery's plan source after a human reconcile: hold STOP, phase stopped, recovery_required."""
        now = datetime.now(timezone.utc)
        status, owner, *_ = fixture()
        status.update(current_plan=None, next_plan=None, phase='stopped', recovery_plan_id=1441,
                      recovery_history={'1440': {'sha256': PLAN_SHA, 'issued': 1860}, '1441': {'sha256': 'b'*64, 'issued': 8}})
        plan = fixture()[5]['plans']['1441']
        db = dict(fixture()[5], enabled_plans=[], cleanup_at=now.isoformat(),
                  plans={'1441': dict(plan, mode='retired'), '1440': dict(plan, mode='retired', sha256=PLAN_SHA, issued=1860)})
        api = json.dumps(dict(id=1441, plan_sha256='b'*64, master_deletion_enabled=True)).encode()
        for reason, accepted in (('finish_admission_hold', True), ('natural_boundary', True), ('failure', False)):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as tmp:
                root, state = Path(tmp) / 'runner', Path(tmp) / 'finish'
                (root / 'incident-recovery-1440-20261117').mkdir(parents=True)
                state.mkdir()
                close = root / 'incident-recovery-1440-20261117/closed-verified.json'
                close.write_bytes(b'closed')
                raw = json.dumps(dict(owner, finish_state=str(state))).encode()
                (root / 'cleanup-owner.json').write_bytes(raw)
                (root / 'status.json').write_text(json.dumps(status))
                (root / 'STOP').write_text(json.dumps(dict(schema='stoarama.cleanup-stop.v1', owner_id=owner['owner_id'],
                                                          reason=reason, created_at=now.isoformat())))
                (state / 'admission.json').write_text(json.dumps(dict(failed=False, recovery_required=True, active={})))
                response = unittest.mock.MagicMock()
                response.__enter__.return_value.read.return_value = api
                psql = unittest.mock.Mock(returncode=0, stdout=json.dumps(db))
                with patch.object(ownership, 'CLOSE_SHA', ownership.digest(b'closed')), \
                        patch.object(ownership.urllib.request, 'urlopen', return_value=response), \
                        patch.object(ownership.subprocess, 'run', return_value=psql), \
                        patch.dict(sys.modules, prepare_keep28_plans=unittest.mock.Mock(pg_env=dict)), \
                        patch.dict(ownership.os.environ, SERVICE_TOKEN='fixture'):
                    collect = lambda: ownership.collect(root, Path(tmp), state / 'recovery.json',
                                                        ownership.digest(raw), now, plan_source=True)
                    if accepted:
                        self.assertEqual(collect(), {'recovery_plan_id': 1441})
                    else:
                        with self.assertRaises(ValueError): collect()

    def collect(self, parts, running=True):
        return assemble_owned(*parts, now=NOW, running=running)

    def test_active_issuance_does_not_invalidate_settled_recovery(self):
        parts = fixture()
        for issued in (0, 8, 3492):
            parts[5]['plans']['1441']['issued'] = issued
            result = self.collect(parts)
            self.assertEqual(result['known_enabled_plans'], [1441])
            self.assertEqual(result['recovery_issued_items'], 1860)
            self.assertEqual(result['cleanup_owner']['plan_sha256'], 'b'*64)

    def test_certified_stitch_client_retains_ordinary_finish_ownership(self):
        parts = fixture()
        parts[5]['version'] = 'r2-copy-v7-certified-stitch'
        self.assertEqual(self.collect(parts)['known_enabled_plans'], [1441])
        parts[5]['version'] += '-unknown'
        with self.assertRaises(ValueError):
            self.collect(parts)

    def test_low_memory_cleanup_client_retains_ordinary_finish_ownership(self):
        for version in ('r2-copy-v8-low-memory', 'r2-copy-v8-low-memory-cleanup', 'r2-copy-v8-keep20-cleanup', 'r2-copy-v8-fast-copy', 'r2-copy-v8-tunable-copy', 'r2-copy-v8-tunable-day', 'r2-copy-v8-tunable-modes', 'r2-copy-v8-ke-day'):
            parts = fixture()
            parts[5]['version'] = version
            self.assertEqual(self.collect(parts)['known_enabled_plans'], [1441])
            parts[5]['version'] += '-extra'
            with self.assertRaises(ValueError):
                self.collect(parts)

    def test_dry_run_enable_pause_retire_transitions(self):
        for mode in ('dry_run', 'enabled', 'paused', 'retired'):
            parts = fixture()
            parts[5]['plans']['1441']['mode'] = mode
            parts[5]['enabled_plans'] = [1441] if mode == 'enabled' else []
            self.collect(parts)

    def test_checkpoint_promotion_accepts_only_bound_preceding_d_success(self):
        parts = fixture()
        status, owner, recovery, runner, api, db = parts
        status.update(recovery_plan_id=1441)
        status['recovery_history']['1441'] = dict(sha256='b'*64, issued=8)
        runner.update(plan_id=1441, plan_sha256='b'*64, delete_ever_issued=8, verified=8,
                      checked_at=NOW.isoformat())
        db['plans']['1441'].update(mode='retired', newest_issued_at=(NOW-timedelta(minutes=1)).isoformat())
        db['enabled_plans'] = []
        self.assertEqual(self.collect(parts)['recovery_plan_id'], 1441)
        recovery['plan_id'] = 1439  # Mere numeric age is never sufficient.
        with self.assertRaises((ValueError, KeyError)):
            self.collect(parts)

    def test_future_stale_failed_foreign_mismatch_and_uncertain_stop(self):
        mutations = [
            lambda p: p[0].update(owner_id='foreign'),
            lambda p: p[0].update(updated_at=(NOW+timedelta(seconds=31)).isoformat()),
            lambda p: p[0].update(updated_at=(NOW-timedelta(seconds=301)).isoformat()),
            lambda p: p[0].update(last_error='failure'),
            lambda p: p[0].update(recovery_error='failure', shutdown_or_recovery_failed=True),
            lambda p: p[0].update(started_plan_sha256='c'*64),
            lambda p: p[2].update(problems=1),
            lambda p: p[2].update(refresh_error='failure'),
            lambda p: p[2].update(checked_at=(NOW-timedelta(hours=8)).isoformat()),
            lambda p: p[5].update(enabled_plans=[1442]),
            lambda p: p[5].update(enabled_batches=[1]),
            lambda p: p[5].update(replay_failed=1),
            lambda p: p[5].update(inventory_mismatches=1),
            lambda p: p[5]['plans']['1441'].update(uncertain=1),
            lambda p: p[5]['r2_copy'].update(mismatch_latched=1),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                parts = fixture(); mutation(parts)
                with self.assertRaises((ValueError, KeyError)):
                    self.collect(parts)

    def test_plan_selection_can_repair_report_without_admitting_feed(self):
        parts = fixture()
        parts[2].update(refresh_error='failed')
        result = assemble_owned(*parts, now=NOW, running=True, selecting=True)
        self.assertEqual(result, {'recovery_plan_id': 1440})
        with self.assertRaises(ValueError): self.collect(parts)
        parts = list(parts); parts[2] = None; parts[3] = None
        self.assertEqual(assemble_owned(*parts, now=NOW, running=True, selecting=True), result)

    def test_natural_stop_is_exact_owner_bound_and_never_foreign(self):
        owner = fixture()[1]
        stop = dict(schema='stoarama.cleanup-stop.v1', owner_id=owner['owner_id'],
                    reason='natural_boundary', created_at=NOW.isoformat())
        validate_stop(json.dumps(stop), owner, NOW)
        for change in (dict(owner_id='foreign'), dict(reason='failure'),
                       dict(created_at=(NOW+timedelta(seconds=1)).isoformat())):
            with self.assertRaises(ValueError):
                validate_stop(json.dumps(dict(stop, **change)), owner, NOW)
        with self.assertRaises(ValueError): validate_stop('', owner, NOW)

    def test_natural_stop_run_collect_and_restart_preserves_pending_plan(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'cleanup-runner'))
        from test_runner import FakeRunner, r
        with tempfile.TemporaryDirectory() as root, patch.object(r, 'utcnow', return_value=NOW):
            job = FakeRunner(root)
            job.p['id'] = 1441
            job.p['results'][0]['updated_at'] = NOW.isoformat()
            job.picks = [1441, None]
            enable = job.enable
            def stop_before_enable(plan):
                (job.root / 'STOP').write_text(json.dumps(dict(schema='stoarama.cleanup-stop.v1',
                    owner_id=fixture()[1]['owner_id'], reason='natural_boundary', created_at=NOW.isoformat())))
                return enable(plan)
            job.enable = stop_before_enable
            job.run()
            self.assertEqual(job.status['current_plan'], 1441)
            self.assertEqual(job.status['phase'], 'stopped')
            self.assertNotIn('plans/1441/control', job.events)
            parts = fixture()
            parts[0].update(job.status)
            parts[5]['plans']['1441'].update(mode='dry_run', issued=0)
            parts[5]['enabled_plans'] = []
            self.collect(parts, running=False)
            (job.root / 'STOP').unlink()
            job.enable = enable
            job.run()
            self.assertIn('plans/1441/control', job.events)
            self.assertEqual(job.status['phase'], 'complete')

    def test_stopped_pending_plan_requires_dryrun_zero_issued(self):
        parts = fixture()
        parts[0]['phase'] = 'stopped'
        parts[5]['enabled_plans'] = []
        parts[5]['plans']['1441'].update(mode='dry_run', issued=0)
        self.collect(parts, running=False)
        parts[5]['plans']['1441']['issued'] = 1
        with self.assertRaises(ValueError): self.collect(parts, running=False)
        parts[5]['plans']['1441'].update(mode='paused', issued=0)
        with self.assertRaises(ValueError): self.collect(parts, running=False)

    def test_finished_day_run_is_an_idle_boundary(self):
        parts = fixture()
        parts[0]['phase'] = 'day_complete'
        parts[5]['enabled_plans'] = []
        parts[5]['plans']['1441'].update(mode='dry_run', issued=0)
        self.collect(parts, running=False)
        parts[0]['phase'] = 'day_running'
        with self.assertRaises(ValueError): self.collect(parts, running=False)

    def test_dead_owner_cannot_allow_enabled_plan(self):
        with self.assertRaises(ValueError): self.collect(fixture(), running=False)

    def test_nano_timestamps_normalize_on_python39(self):
        for fraction in ('1', '12345', '123456789'):
            self.assertEqual(timestamp('2026-11-18T12:00:00.'+fraction+'Z').tzinfo, timezone.utc)


if __name__ == '__main__': unittest.main()
