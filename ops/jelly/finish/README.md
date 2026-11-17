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
launch. It emits this complete JSON (all fields required; only the three error fields may be null):

```json
{
  "updated_at": "2026-11-17T12:00:00Z",
  "recovery_problems": 0,
  "recovery_checked_at": "2026-11-17T12:00:00Z",
  "recovery_issued_items": 0,
  "recovery_verified_items": 0,
  "shutdown_or_recovery_failed": false,
  "recovery_plan_id": 1440,
  "recovery_plan_sha256": "<64-character exact plan digest>",
  "recovery_error": null,
  "last_error": null,
  "controller_held_error": null,
  "local_copy_mismatch_latched": 0,
  "local_copy_checked_at": "2026-11-17T12:00:00Z",
  "holds_checked_at": "2026-11-17T12:00:00Z",
  "master_checked_at": "2026-11-17T12:00:00Z",
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
resolved. Scope recovery to the runner's current or latest plan. Bind its verifier
report to `recovery_plan_id`, `recovery_plan_sha256`, issued/verified counts,
successful verifier exit and actual verification observation time
`recovery_checked_at`. The tick checks this plan's SHA and issued count in DB.
Verified must equal issued, problems must be zero, and checked_at must be within
five minutes. A new permit requires a fresh check. Historical plans are covered
by their verified close records; there is no global count aggregation or sealed
baseline (root decision, 2026-11-17).

`recovery_error` must be null and `shutdown_or_recovery_failed` false.
`last_error` must be null or exactly match `controller_held_error`, an explicit
known state the controller is deliberately holding. Do not designate unexpected
errors as holds. `known_enabled_plans` is empty or contains only that runner's
current plan; `known_enabled_batches` must be empty.

Read shutdown/recovery flags from the controller's durable record;
do not infer them from a running process. Confirm `master_deletion_enabled`
from the live API (not an env file or deployment request). Expected master and
known IDs come from the sole controller's actual execution ownership. The tick
queries DB enabled plans and batches and refuses IDs outside those sets, even
on other connections. An ON master without a known owned plan is refused. `master_checked_at` records
the actual live API check, independently of envelope creation.
The safety collector owns the coherent/fresh observation of these facts.

`holds_by_reason` contains current COPY holds, from a fresh client heartbeat
`cleanup.r2_copy` (persisted at `connections.nas_cleanup_status->'r2_copy'`,
with `nas_cleanup_status_at`) (use that heartbeat's own age, not the API response time). Normalize
only a COPY free-space hold to `free_space`. Memory, transport, sidecar conflicts,
maintenance and general resource holds retain their own keys. Never convert
historical cumulative counts into a current free-space hold. Emit zero/omit a
reason only with fresh evidence that the corresponding hold is not active.
`holds_checked_at` is the heartbeat observation time. `local_copy_mismatch_latched`
comes from A1's inventory SQLite latch, which can precede the server blocked-row
ACK. `local_copy_checked_at` records that independent observation time; do not
substitute the time a stale heartbeat was fetched. A positive local or server
latch stops the tick. All three COPY/master observation times must be fresh.

Both safety and copy status timestamps must be no older than five minutes and
no more than 30 seconds in the future. Copy status is produced afresh by A1's
`stoaramactl nas-r2-copy status --json`, schema
`stoarama.nas-r2-copy-status.v1`. Its mismatch latch and durable copy statistics
are copied into finish status. Queue depth and `items_per_min` (verified in the
last five minutes divided by five, including idle time) are refreshed from DB.
Listing size conflicts are reported by count and up to 50 source IDs when the
queueable candidates are exhausted; they produce a typed nonzero
`listing_size_conflict` stop so stranded objects cannot look complete.
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
| `cleanup_recovery_problems` | Current/latest plan recovery verifier reports problems > 0. Resolve missing/conflicting R2 recovery evidence. |
| `unknown_enabled_cleanup` | DB enabled plan/batch not owned by the controller. Reconcile ownership; do not adopt it blindly. |
| `copy_mismatch_latched` | A1 reports durable checksum/final conflict. Reconcile the affected exact key; never clear to feed. |
| `shutdown_or_recovery_failed` | Failure latch, non-null recovery_error or an unrecognized last_error. Controller resolves recovery/shutdown failure first. |
| `deletion_master_mismatch` | Live master differs from expected, or ON has no owned plan/batch. Reconcile actual runtime state. |
| `database_unavailable` | Bounded DB read/lock unavailable or deadline exceeded. Inspect privately; wrapper alerts. |
| `listing_size_conflict` | Remaining raw sources have a conflicting size in the newest listing. Reconcile reported source IDs; no absent-tier download is authorized. |
| `manifest_import_failed` | Manifest/source/hold/equality validation or import failed. Existing queue state survives; inspect source identity. |
| `tick_already_running` | Another feeder holds the DB lock. Check duplicate installation. |
| `status_write_failed` | CLI could not publish status; stderr only. Repair storage. Dead-man sees the old timestamp. |

Every CLI stop exits nonzero; wrapper alerts with `darma msg`. Dependency failure
also alerts and leaves the last CLI status age unchanged. State directory/lock failures also alert. A failed invocation without its own
status reports `current_status_unavailable`, never a prior stop reason.
Raw subprocess errors
are suppressed to keep credential-bearing URLs out of logs/notifications.
