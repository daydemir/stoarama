import copy
from datetime import datetime, timezone, timedelta
import unittest
from ownership import assemble_owned, POLICY_SHA
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

    def test_dead_owner_cannot_allow_enabled_plan(self):
        with self.assertRaises(ValueError): self.collect(fixture(), running=False)

    def test_nano_timestamps_normalize_on_python39(self):
        for fraction in ('1', '12345', '123456789'):
            self.assertEqual(timestamp('2026-11-18T12:00:00.'+fraction+'Z').tzinfo, timezone.utc)


if __name__ == '__main__': unittest.main()
