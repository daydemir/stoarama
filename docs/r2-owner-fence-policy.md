# Owner-authorized fence and object cutoff

`owner_fence_policy_20261118_v1` replaces `non28_key_policy_v1` for both
`r2-inverse apply` and `apply-keep28`. It is owner authority, not proof that an
external PUT completed. The reviewed [owner record](records/r2-owner-fence-policy-20261118.md)
quotes Deniz verbatim and records the waived completion/access-log and former
canary/analytics/two-hour no-signing requirements. The authority record's
SHA-256 is pinned in the executor; a different file cannot replace it.

The existing outer evidence file and expected SHA remain mandatory. Its exact
bucket, endpoint, fence activation and deadline must match the locked live row;
the observation must be after that deadline and no later than database time.
The live activation truncated downward to a second must equal
`2026-11-12T14:28:47Z`. Both listing and final HEAD LastModified must be unchanged,
nonzero, strictly before that cutoff, before partition creation and before the
current database time. The full-bucket partition reports `last_modified_excluded`
objects/bytes across every listing entry, independently of its protection class;
these are not all otherwise deletable. The apply summary's `last_modified_held`
counts only new candidates in that lane's manifest class rejected while scanning,
plus attempted keys rejected at final HEAD; HEAD rejection still stops on error.
Neither counter is an additional class total. The seven-day moving object age
is removed.

Use `proof: owner_fence_policy_20261118_v1`, leave both
`accepted_put_completion_evidence` and `deployed_writer_coverage_evidence` empty,
and supply `fence_policy` with:

- `partition_sha256` and `summary_sha256`, matching the apply pins.
- `last_modified_before: 2026-11-12T14:28:47Z`.
- `owner_decision: {path, sha256}` pointing at the exact committed owner record.
- `residual_acceptance: owner_accepted_head_delete_overwrite_reappearance_v1`.

A PUT can still commit after final HEAD and before unconditional DELETE, or
reappear afterward. This acceptance names that race and does not infer terminal
provider evidence. Permanent intents, one wire attempt, no retries, fresh source
identity/epoch/holds and B2's fresh NAS size/hash custody still apply. The gate is
`r2_retirement_writes_blocked`, which requires the six live wire/capability
categories to be empty. Historical lifecycle rows in `writers_drained` are not
an executor gate; that diagnostic and its other consumers are unchanged.
Active recordings or pending/leased recording jobs separately veto the batch
and every subsequent authority check, under ROOT's recorder-replay rule.

Migration 0210 changes only the `stoarama` fence deadline to its transaction's
`now()`. It takes the existing wire/apply try-locks before disabling only the
transition trigger for that UPDATE, then re-enables it in the same transaction.
Busy locks abort with the old row and trigger intact. A fresh database without
the fence remains unfenced. This does not install or activate a new fence.

The controller deploys between batches and must read back the unchanged
activation/endpoint, `drain_not_before = schema_migrations.applied_at` for 0210,
enabled trigger, `r2_retirement_writes_blocked('stoarama', endpoint)` and the
separate `r2_retirement_writers_drained('stoarama')` diagnostic. Source merge is
not production deployment. The retired legacy `r2-retire` age/default remains
unchanged; the current inverse executors remain RAW-only.
