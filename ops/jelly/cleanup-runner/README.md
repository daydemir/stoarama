# Supervised keep28 cleanup runner

PR-only implementation. Installing or starting this job is a separate operation
on Jelly. Build and test changes on Mooney; never build on Jelly. Python 3.9+
stdlib only; runtime tools are `psql`, Git, and the existing Jelly
`/Users/jelly/tmp-safeguards/stoaramactl` recovery verifier. Nothing here deletes
R2 objects or modifies database rows directly.

Owner approval (Deniz, 2026-11-11), verbatim conditions:

> delete everything on the NAS outside each stream's pinned keep28 period (raw clips and old stitched files), without asking per batch, provided every batch passes: (1) the server's dry run AND an independent check that every file is outside its stream's kept period; (2) a verified R2 copy for every file (R2 copies stay); (3) no protected or uncertain files; (4) the 5 TB/day cap, one plan at a time; deletion master switch OFF between batches; fixes deployed only between batches (never pause an enabled plan to deploy); a short daily progress note; any failed check stops the run and goes to the owner.

The runner:

- Takes an exclusive local lock. Refuses enabled foreign plans or batches.
  Persists its current plan before enable, so restart monitors/reconciles that
  plan before doing more work. Never calls the pause control.
- Forces the master OFF through a single-key Render env edit and an explicit
  redeploy of the **current live SHA**, never HEAD. Rejects concurrent deploys,
  waits for live, and checks the API master when a plan is selected. Repeats
  this OFF transition after completion or pause.
- Uses a complete fresh dry run or invokes `prepare_keep28_plans.py --submit`
  for one stream, at most one <=100 GB plan. Unique `k28auto-YYYYMMDD-HHMMSS`
  prefixes avoid immutable-label reuse. Admission rejection is a stop.
- Independently checks the pinned policy bytes at `origin/main` (no network
  fetch or checkout update), raw/collated catalog identities, all source clips,
  stream/window bounds, other reserved/ever-issued identities, complete result
  ordinals, verified R2 proof, zero issued items, and observations <20 hours old.
  Missing rows, unknown bounds, stale evidence and non-permitting states stop.
- Uses the backend's conservative rolling 24h **ever-issued** byte count, which
  includes uncertain receipts, for the 5 TB cap. Waits with master OFF when full;
  keeps the same plan. Rechecks all gates after ON redeploy before enable.
  Evidence that becomes stale during a long budget wait stops for owner review.
- Enables only the exact plan SHA with the dated standing approval and a 24h
  approval. Monitors for completion, expiry, anomalies and live-commit changes.
  OFF, drain outstanding receipts/leases, then recovery verification precede
  close. Only exact backend lease/approval expiry reasons may close never-issued
  remainder. Anomalies and uncertain issued receipts stop for the owner.
- Invokes the existing `verify-recovery` CLI because this API revision has no
  recovery route; checks plan identity, issued/verified counts and zero problems.
  This CLI GETs the report and HEAD-verifies every ever-issued R2 copy. It uses
  its existing operator user agent; the runner/preparer use `stoarama-operator/1`.
- Confirms no eligible, reserved, outside-window or uncertain cataloged NAS work
  remains before reporting complete. Inventory/ack entries are subtracted only
  by matching confirmed deletion identities. Unmapped inventory blocks completion.

## State and owner delivery

`~/stoarama-cleanup-runner/status.json` is atomically replaced; `runner.jsonl` is
append-only. Totals and per-stream confirmed deletion progress come from the DB,
so restart does not double-count. A short daily UTC progress email goes to
`CLEANUP_OWNER_EMAIL` via the existing Resend credentials; an initial accepted
email is required before enabling deletion. Errors email immediately, latch
`last_error`, create STOP, and attempt OFF + recovery independently. HTTP response
bodies, subprocess output and credential values never enter logs or emails.
If email/OFF/recovery fails, its failure flag remains in status for supervision.
A delivery acknowledgment means Resend accepted the email, not that it was read.

`STOP` or SIGTERM prevents new enables and naturally drains a currently enabled
plan to completion/expiry before OFF/recovery. It never pauses a plan to deploy.
The plist has no automatic restart after failure and no forced exit timeout.
Do not force-kill a draining runner. A process crash can leave the master ON until
restart; its persisted current plan and backend approval expiry bound recovery.

## Later installation on Jelly (not performed by this PR)

First verify no plan/batch is enabled and the master is OFF. Update the checkout
only between batches. Keep Jelly's credential files in place; never copy secrets
from Mooney. Ensure `psql` is in the plist PATH, Git has the reviewed `origin/main`,
and the specified existing recovery CLI is available. Set up private state:

```sh
umask 077
mkdir -p ~/stoarama-cleanup-runner
# Write owner.env with CLEANUP_OWNER_EMAIL=<Deniz's delivery address>.
# EMAIL_FROM and EMAIL_RESEND_API_KEY come from existing local/render.env.
chmod 700 ~/stoarama-cleanup-runner
chmod 600 ~/stoarama-cleanup-runner/owner.env
~/Build/stoarama/ops/jelly/cleanup-runner/run.sh --dry-run
```

**Dry-run still redeploys the live commit with master OFF, submits dry-run plans,
and emails progress.** It never switches ON or enables a plan. It is not an
offline test. Do not run it against production merely to validate this PR.
An enabled saved plan cannot be adopted in dry-run mode.

After reviewing dry-run status, remove its STOP only if present and explained;
install the job with the usual `ops/jelly/README.md` launchd procedure using
`com.stoarama.cleanup-runner.plist`. Do not run multiple copies from different
state directories or combine this job with manual enable/batch automation.
The backend's own one-plan/budget fences remain in force for concurrent operators.

For fixes, create STOP and wait until `phase=stopped`, no enabled plans/batches,
and master OFF before changing runner/server code or booting out the job. After
a failed check, review `last_error` and recovery/shutdown flags with Deniz; resolve
the cause, clear `last_error` in status, then remove STOP and restart. Never clear
the persisted `current_plan` to bypass recovery. No automatic retry clears a latch.

## Offline checks

```sh
python3 -m unittest discover -s ops/jelly/cleanup-runner -v
python3 -m unittest discover -s backend/scripts/nas-cleanup -q
plutil -lint ops/jelly/launchd/com.stoarama.cleanup-runner.plist
sh -n ops/jelly/cleanup-runner/run.sh
```

Protocol references: [Render single env edit](https://api-docs.render.com/reference/update-env-var),
[explicit commit redeploy](https://api-docs.render.com/reference/create-deploy),
[Resend email delivery](https://resend.com/docs/api-reference/emails/send-email).
