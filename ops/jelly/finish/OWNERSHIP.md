# Cleanup ownership handoff (preparation, no production authority)

Install only after D stage 5 and R2 partition preparation, with fresh ops review
and explicit controller GO. No API/schema change. The updated **CLI** is required.
The original collector's exact STOP / retired1440 / next1441 mode remains the
default. `--owner-sha256 <SHA>` explicitly selects the new mode; no mode is inferred
from missing STOP, failures, status edits or master state.

The controller writes an immutable `~/stoarama-cleanup-runner/cleanup-owner.json`:

```json
{
  "schema": "stoarama.cleanup-owner.v1",
  "owner_id": "<new canonical UUID>",
  "finish_state": "/Users/jelly/stoarama-finish-eae77803-v5-v3/state",
  "initial_plan_id": 1441,
  "initial_plan_sha256": "23b175d7b1822030b24998e0deeb0da40b852f34ab9926d3a25d8dda7426ff40",
  "baseline_plan_id": 1440,
  "baseline_plan_sha256": "78b86990e0a45715deb9383226cff6ff5f01e6159f3d8eaf8b6d41bb39e38697",
  "policy_sha256": "060aa7070c7909d78013415f83b76bd0c027a885caf91c67c8b709ac1f4b5811"
}
```

Pin its exact bytes in the collector/plan-source wrappers' `--owner-sha256`, the
monitor invocation, and `CLEANUP_OWNER_SHA256` in the runner's existing owner.env.
The manifest does not approve dry-run work. Revalidate current eligibility,
permanent incident holds, keep28, budgets, resources and the supported client
before activation. Never retry339081 or335808–335815, or their RAW sources;
never alter source holds, incident vetoes or0209; never delete the protected tools
prefix `experimental/v85-retired-WORK-custody/`.

With runner absent and its flock exclusively held, preserve old STOP/status and
canonical1440 report. Verify exact old status and STOP pins. Assert failure flags
are false/null, retired1440, actual recovery1860/1860/problems0, no enabled work,
no mismatches/replay problems. Preserve all counters, holds, history and notes.
Reconcile the *known held error only* in a reviewed atomic status reset:
current_plan=null, next_plan=1441, started_plan_sha256=null, last_error=null,
phase=stopped, lookahead_pending=false. Add owner_id from manifest,
recovery_plan_id=1440, recovery_plan_sha256=baseline digest, and
recovery_history={"1440":{"sha256":baseline digest,"issued":1860}}.
Copy the **actual canonical report bytes** to runner `recovery-1440.json`;
never restamp checked_at. Fsync files/directory, re-read, append reconciliation
note. Remove only the exact owned STOP and release the flock before kickstart.

The new runner has no Render capability. It requires the controller to establish
master ON **explicitly and separately**. The previous global-master-OFF-between-
batches contract becomes owned-plan-OFF (digest-bound pause and drain), with the
global switch remaining controller-owned. The controller's fresh ops review/GO
must cover that change; kickstart must never imply an env edit or deployment.

Ownership collection runs under a shared snapshot lock; runner status writes use
its exclusive side. The collector checks the runner's exclusive execution flock,
PID command and <=300s heartbeat, exact owner token, plan/catalog digest, global
absence of foreign enabled work, client/replay/inventory/mismatch evidence, D's
failure/admission state, and latest real settled recovery. Missing/future/stale or
failed evidence stops. Lookahead admission may be in progress only while the
healthy process owns the lock; interrupted admission still refuses restart.

Active per-file permits and R2 custody continue unchanged. A live plan's changing
issued set is distinct from its last settled recovery checkpoint. The runner
writes the real verifier report durably and atomically publishes its checkpoint
before releasing current_plan. The finish CLI binds that checkpoint to the DB's
unchanged issued count/newest update; it independently checks the owner ID/digest
against the DB. A D success for an explicitly recorded preceding checkpoint may
coexist with a newer runner checkpoint; the collector uses the newest checkpoint's
real report until D refreshes it. D failures always stop feeding, except a pure network failure that obtained
no data: it holds without latching or refreshing evidence (README "Transient holds"). After explicit D reconciliation, plan-source
selects only the DB-bound checkpoint without consuming the failed report, so a
real verifier can replace it. Selection is never feeding evidence. Older IDs outside the
recorded chain never count. D's refresh selects this same settled plan, so normal
active issuance cannot invalidate its report. No last-good fallback is read.

Install collector.py + ownership.py together in INSTALL. Existing collector-entry
and CLI identity/env wrappers remain. Add the owner SHA and explicit STATE to both
collector and plan-source wrapper invocations. Replace the old fixed1440 observer
with `monitor.py --root ROOT --repo REPO --state STATE --owner-sha256 SHA` via the
same credential/read-only wrapper. This is one observation, with nonzero exit on
failure; it never starts a background loop. V7's old observer invocation cannot
remain in the execution chain. Its historical COPY throughput baseline is retained
as historical evidence; do not reinterpret it as cleanup authority.

Rollback is stop-and-preserve: prevent successors with an owned STOP, let healthy
execution drain naturally, or request controller-approved digest-bound pause on
failure; inspect actual issued work and run real recovery. No Render call is made
by the runner. Never restore old status over new issuance, rewind checkpoints,
clear latches, retry held items, remove guards, or launch old wrappers against a
changed ownership state. The old COPY-only collector is reusable only after a new
review has rebound an actually idle held boundary. Preserve both sets of files
and all new reports; binary/file rollback alone is not operational rollback.

Startup acquires the snapshot lock before the execution flock and holds it
through restart-latch/manifest validation and durable PID publication. Rejected
starts preserve the prior bytes; collectors cannot see a new flock with old PID.
A live heartbeat failure raises a terminal execution check and takes the owned
pause/drain path; it is distinct from a preexisting restart latch.

The runner consumes the same explicit D admission/recovery state before startup,
each active poll and enable. D failures, missing reports, stale reports or identity
mismatches stop cleanup too. Emergency containment retains supervision until the
exact owned plan is confirmed stopped. After a lost pause response it re-reads the
active set and exact digest before another monotonic pause; it never retries item
execution or writes a foreign plan. Already-paused expiry reasons are preserved.

New enablement takes the **existing D tick state/lock** before its final STOP/D
health check and control POST. D already owns that lock before collecting safety
and through its last import/check. No successor can become enabled while D uses
an older ownership snapshot. The runner holds no snapshot lock while waiting;
its heartbeat remains writable, and its previous plan has already drained.

For an intentional natural boundary during owned execution, the controller writes
and hashes an explicit STOP JSON, retaining the actual owner UUID and timestamp:

```json
{"schema":"stoarama.cleanup-stop.v1","owner_id":"<manifest UUID>","reason":"natural_boundary","created_at":"<actual RFC3339 time>"}
```

This prevents successor enablement while healthy active execution drains; D and
monitoring continue. The runner's own `"reason":"finish_admission_hold"` STOP (same
schema and owner) is the other idle boundary collection and plan selection accept:
it marks a runner that refused startup finish health with no owned plan, never
latched itself, and releases that STOP itself once health is clean. Empty STOPs, the retired initial STOP text, foreign UUIDs,
other reasons and future timestamps fail closed. Failure STOPs never override D
or runner latches. The original STOP is removed only by the initial reviewed
handoff, not converted or reused. Controller journals preserve every actual byte.

A stopped runner may retain a selected plan and its pre-enable start digest only
when fresh DB proof shows that exact plan remains dry_run with zero issuance and
no enabled work. The collector accepts this explicit idle boundary, preserving
both prepared plans for restart; paused or issued current plans cannot claim it.

Before execution, the runner imports the existing installed admission module and
uses its gate/journal/interrupted proof. Abandoned active markers or orphan
attempts stop cleanup even if failed=false has not yet been published. New enable
checks that proof under the gate, acquires the tick lock nonblocking under the
same gate, then keeps the gate through the POST. Waiting for a live tick happens
outside both locks before revalidation. A live recovery remains permitted. The
module's observer parameter is Optional[str]; its D participant behavior is
unchanged. Install the hash-bound admission.py alongside collector dependencies.
