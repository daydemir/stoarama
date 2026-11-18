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
real report until D refreshes it. D failures always stop feeding; after explicit D reconciliation, plan-source
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
