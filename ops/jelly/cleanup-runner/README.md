# Supervised keep28 cleanup runner

PR-only implementation. Installing or starting this job is a separate operation
on Jelly. Build and test changes on Mooney; never build on Jelly. Python 3.9+
stdlib only; runtime tools are `psql`, Git, and the existing Jelly
`/Users/jelly/tmp-safeguards/stoaramactl` recovery verifier. Nothing here deletes
R2 objects or modifies database rows directly.

Owner approval (Deniz, 2026-11-11), verbatim conditions:

> delete everything on the NAS outside each stream's pinned keep28 period (raw clips and old stitched files), without asking per batch, provided every batch passes: (1) the server's dry run AND an independent check that every file is outside its stream's kept period; (2) a verified R2 copy for every file (R2 copies stay); (3) no protected or uncertain files; (4) the 5 TB/day cap, one plan at a time; deletion master switch OFF between batches; fixes deployed only between batches (never pause an enabled plan to deploy); a short daily progress note; any failed check stops the run and goes to the owner.

The runner:

- Takes an exclusive local lock. On startup, first reads active execution.
  With **no enabled plan**, forces master OFF as a barrier before policy loading,
  email checks, progress, or other work (including a latched failure). With an
  enabled plan matching the persisted `current_plan` and `started_plan_sha256`,
  resumes monitoring **without toggling master** only if approval is still in the
  future, the plan is enabled,
  master is ON, no anomalous item states exist, and no failure is latched.
  An unknown/unhealthy enabled plan, enabled batch, or dry-run takeover forces
  OFF, alerts the owner, and stops. Startup errors also take this fail-safe path;
  unreadable or malformed ownership state is treated as an unknown enabled plan.
  Only actual lock contention exits without touching the master switch.
  Never pauses a healthy plan on restart:
  toggling OFF would expire its leases and force replanning. The start digest is
  durably recorded before the enable request, including recovery from a lost API
  response; a plan merely selected for dry run has no execution ownership marker.
- Forces master OFF through a single-key Render env edit, cancels any in-progress
  deploys, then explicitly redeploys the **current live SHA**, never HEAD. OFF
  bypasses the normal concurrent-deploy rejection and needs no successful disk
  write. Remembers its new OFF deploy in memory across retries and never cancels
  it while it is in progress, including after a polling timeout. Confirms that
  deploy (created after the OFF env edit) is live,
  configured env is false, and the live API reports master false; rechecks live
  deploy identity after that report. With no saved plan, reads the latest existing
  account-47/connection-13 plan report for this runtime check. Missing reports
  cannot confirm OFF. OFF and ON retry idempotent reads (deploy status/list,
  live identity, env, plan, and read-only queries) with at most 120 seconds of
  added backoff per read, in addition to existing I/O timeouts. Exhausted reads,
  failed/canceled/timed-out deploys, concurrent deploys, or a runtime master mismatch trigger loud stderr + owner email alerts
  and retries forever with 15-second exponential backoff capped at five minutes.
  A terminal failed OFF deploy or a confirmed verification mismatch requires a
  new OFF deploy; read failures alone resume the remembered deploy. ON read
  exhaustion propagates to the normal fail-safe OFF path.
  ON still rejects concurrent deploys. Repeats OFF after completion or pause.
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
  close. Re-reads and reclassifies the final report after recovery verification:
  only exact backend lease/approval expiry reasons may close never-issued
  remainder. A concurrent manual/non-expiry pause, anomalies, and uncertain issued
  receipts stop for the owner without closing or replanning.
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
email is required before enabling deletion. Failure handling first confirms OFF,
then alerts the owner, attempts recovery, and independently attempts state and
STOP writes. Disk-full or other write errors cannot skip shutdown or notification;
write failures also emit loud stderr messages. While OFF is unconfirmed, shutdown
keeps retrying and alerting rather than exiting. Email/recovery failure flags stay
in memory and are persisted if storage works; no disk write is required to alert.
HTTP response bodies, subprocess output and credential values never enter logs or
emails. Failed OFF/ON steps emit fixed step names to stderr and `runner.jsonl`
(best effort if storage is unavailable), including transient read retries.
A delivery acknowledgment means Resend accepted the email, not that it was read.

`STOP` or SIGTERM prevents new enables and naturally drains a currently enabled
plan to completion/expiry before OFF/recovery. It never pauses a plan to deploy.
The plist has no automatic restart after failure and no forced exit timeout.
Do not force-kill a draining runner. A process crash can leave the master ON until
restart; restart applies the active-plan policy above, and backend approval expiry
bounds deletion. Do not force-kill an OFF retry loop.

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
[in-progress deploy cancellation](https://api-docs.render.com/reference/cancel-deploy),
[Resend email delivery](https://resend.com/docs/api-reference/emails/send-email).
