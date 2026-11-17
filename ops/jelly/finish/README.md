# Finish tick (controller installs; no production changes here)

`stoaramactl finish tick --safety FILE --copy-status FILE --status FILE` feeds
keep28 raw metadata for account 47 / connection 13. It never changes cleanup,
master switches, NAS files, R2 objects or deletion budgets. A1's
`nas-r2-copy status --json` is required before installation. B and stitching are
not wired into D-min.

The queue target is 10,000 pending + leased rows, split into manifests of at most
5,000 items / 100 GB. Each tick imports at most 10,000 new rows and has a four
minute deadline. It selects likely-present files by exact path + size in the
newest completed connection-13 listing, then absent files. Conflicting listing
sizes are excluded for reconciliation. This listing is only a priority hint;
it confers no custody. Existing queue rows of every state are excluded. Source
validation, holds, path ambiguity and queue identity equality remain enforced by
the existing exact-manifest importer. A partial import can safely resume on the
next tick. An advisory lock excludes concurrent feeders. Present and absent tiers use
separate manifests, so the concurrent importer finishes the present tier first.
Existing queue IDs are preserved, so already queued absent items keep their
original order. The 10k-row bound may require more than two byte-capped manifests
for large files; the 100GB cap applies separately to every manifest.

The sole controller must supply an executable safety collector, run on every
launch. It emits this complete JSON (all fields required, no nulls):

```json
{
  "updated_at": "2026-11-17T12:00:00Z",
  "recovery_problems": 0,
  "recovery_checked_at": "2026-11-17T12:00:00Z",
  "recovery_issued_items": 0,
  "recovery_verified_items": 0,
  "shutdown_or_recovery_failed": false,
  "recovery_error": "",
  "known_enabled_plans": [],
  "known_enabled_batches": [],
  "master_deletion_enabled": false,
  "expected_deletion_enabled": false,
  "holds_by_reason": {}
}
```

`updated_at` is the observation time, never a copied file's mtime. The collector
must fail if any source is missing or cannot be verified. Read recovery problem
counts from the actual recovery verifier, and preserve its failure latch until
resolved. Bind its reports to plan ID/SHA, successful verifier exit, actual issued
count and verified count; cover all connection-13 plans with issued permits.
`recovery_checked_at` is the last successful reconciliation of those reports with
the current issued scope, never envelope creation time. The tick requires that
check within five minutes, verified count equal issued count, and an exact match
to the current DB connection-13 issued count. A new permit requires a new recovery
check. Do not silently discard a failed or superseded report. Read shutdown/recovery flags from the controller's durable record;
do not infer them from a running process. Confirm `master_deletion_enabled`
from the live API (not an env file or deployment request). Expected master and
known IDs come from the sole controller's actual execution ownership. The tick
queries DB enabled plans and batches and refuses IDs outside those sets, even
on other connections. An ON master without a known owned plan/batch is refused.
The safety collector owns the coherent/fresh observation of these facts.

`holds_by_reason` contains current COPY holds, from a fresh client heartbeat
`host.r2_copy` (use that heartbeat's own age, not the API response time). Normalize
only a COPY free-space hold to `free_space`. Memory, transport, sidecar conflicts,
maintenance and general resource holds retain their own keys. Never convert
historical cumulative counts into a current free-space hold. Emit zero/omit a
reason only with fresh evidence that the corresponding hold is not active.

Both safety and copy status timestamps must be no older than five minutes and
no more than 30 seconds in the future. Copy status is produced afresh by A1's
`stoaramactl nas-r2-copy status --json`, schema
`stoarama.nas-r2-copy-status.v1`. Its mismatch latch and durable copy statistics
are copied into finish status. Queue depth and `items_per_min` (verified in the
last five minutes divided by five, including idle time) are refreshed from DB.
On a safety stop, cleanup is always false. On a successful tick it is true only
for a positive current `free_space` hold. The controller runner alone acts on
this decision; an empty queue is not cleanup authority.

## Install packet

Sole controller, at a natural boundary after A1 is merged/deployed:

1. Build the merged CLI on Jelly using `darma storage check` then
   `darma run --slot -- go -C backend build -o <install>/stoaramactl ./cmd/stoaramactl`.
   Copy `tick.py` to the private install directory. Do not reuse or modify the
   cleanup runner's code/state/launchd job. Create a separate private state dir.
2. Provide the executable safety collector described above. Configure credentials
   non-interactively for the CLI and collector using existing controller auth;
   never put secrets in the plist, arguments, logs, or this repository. The wrapper
   deliberately loads no env file. Configure a working `darma msg` notification
   target with `--notify-agent`; it must route to Deniz.
3. Before installing launchd, run the wrapper once with absolute paths:
   `python3 <install>/tick.py --cli <install>/stoaramactl --state <state> --safety-collector <collector> --notify-agent <target>`.
   Verify exit 0, a current `<state>/status.json`, no `stop_reason`, queue growth
   toward 10k (or `candidates_exhausted`), copy metrics, and cleanup false unless a
   fresh free-space hold is active. Confirm cleanup/master/STOP were unchanged.
4. Render the plist template with XML-escaped absolute `__INSTALL__`, `__STATE__`,
   `__COLLECTOR__`, `__NOTIFY_AGENT__`. Run `plutil -lint`, copy the rendered plist
   to `~/Library/LaunchAgents/com.stoarama.finish-tick.plist`, then
   `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.stoarama.finish-tick.plist`.
   It runs immediately and every 120 seconds; launchd and a wrapper lock prevent
   overlap. Verify the next automatic timestamp. The independent Rolly dead-man
   check reads the output age and `stop_reason`; CLI status is replaced atomically.
5. To stop only the feeder:
   `launchctl bootout gui/$(id -u)/com.stoarama.finish-tick`.

## Stops

| `stop_reason` | Meaning / controller action |
|---|---|
| `evidence_unavailable` | Missing, malformed, incomplete, stale or future evidence. Repair collection; never stamp old facts as fresh. |
| `cleanup_recovery_problems` | Recovery verifier reports problems > 0. Resolve missing/conflicting R2 recovery evidence. |
| `unknown_enabled_cleanup` | DB enabled plan/batch not owned by the controller. Reconcile ownership; do not adopt it blindly. |
| `copy_mismatch_latched` | A1 reports durable checksum/final conflict. Reconcile the affected exact key; never clear to feed. |
| `shutdown_or_recovery_failed` | Failure latch or nonempty recovery_error. Controller resolves recovery/shutdown failure first. |
| `deletion_master_mismatch` | Live master differs from expected, or ON has no owned plan/batch. Reconcile actual runtime state. |
| `database_unavailable` | Bounded DB read/lock unavailable or deadline exceeded. Inspect privately; wrapper alerts. |
| `manifest_import_failed` | Manifest/source/hold/equality validation or import failed. Existing queue state survives; inspect source identity. |
| `tick_already_running` | Another feeder holds the DB lock. Check duplicate installation. |
| `status_write_failed` | CLI could not publish status; stderr only. Repair storage. Dead-man sees the old timestamp. |

Every CLI stop exits nonzero; wrapper alerts with `darma msg`. Dependency failure
also alerts and leaves the last CLI status age unchanged. Raw subprocess errors
are suppressed to keep credential-bearing URLs out of logs/notifications.
