# Finish tick (controller installs; no production changes here)

`stoaramactl finish tick --safety FILE --copy-status FILE --status FILE` feeds
keep28 raw metadata for account 47 / connection 13. It never changes cleanup,
master switches, NAS files, R2 objects or deletion budgets. A1's
`nas-r2-copy status --json` is required before installation. B and stitching are
not wired into D-min.

The queue target is 10,000 pending + leased rows, split into manifests of at most
5,000 items / 100 GB. Each tick imports at most 10,000 new rows and has a four
minute deadline. Root's latest catalog priority replaces the unavailable listing:

1. Raw with an exact stale inventory row (connection, clip, path, size, SHA and
   present state): verify-in-place first, with no age requirement for scheduling.
2. Sources without exact inventory whose pinned window ends before Sep 24, 2026.
3. Remaining sources without exact inventory (the 23 late-window streams).

Each tier is selected by stream then source ID; tiers use separate manifests.
The existing four-worker importer can reorder insertion/lease IDs within a tier;
selection and manifest order follow stream/ID, not a serial execution guarantee.
There is no listing query or fallback. Inventory is only a priority hint; the
worker still hashes and publishes fresh exact custody. Existing queue rows of
every state are excluded; verified and blocked work is never requeued. Live
source validation, holds, destination ambiguity and queue equality stay in the
existing exact-manifest importer. Partial imports resume idempotently. An advisory
lock excludes concurrent feeders. Existing queue IDs/order are preserved.

Tier 3 requires fresh storage data and at least 300 GB headroom above the 2 TB
floor. Root's conservative admission window reserves the full size of pending and
leased raw rows, plus every terminal raw row updated within T=180 seconds,
regardless of verified-in-place/downloaded/blocked label or committed bytes.
This also conservatively covers recently completed tiers 1/2 for at most T.
It needs no publication ledger, completion-label inference or sample timestamp.

Source: `clients/nas-pull/stoarama_pull.py:57-58` fixes the heartbeat at 30 seconds
and maximum cached storage sample age at 3×30=90 seconds. The typed late-budget
receipt limit is separately 90 seconds and T is twice that, 180 seconds. Terminal
debits use an inclusive `>=` cutoff, retaining writes exactly T old. A receipt at
most 90 seconds old plus sample age at most 90 seconds means an expired terminal
debit is already represented in free bytes. The cleanup valve's separate freshness
limit remains ten minutes. A 91-second-old receipt can therefore report a valve
decision while withholding late-tier admission.

Late budget subtracts these full-size reservations and the existing 16 MiB range
reserve from free bytes above the unchanged floor. Status includes typed
`late_tier_hold_reason`, `late_tier_budget_bytes` and `queue_reserved_bytes`.
A capacity hold is resumable, not exhaustion; tiers 1/2 can still feed. Imported
cohort counters explicitly label historical inventory, early unknown and late
unknown sources. None asserts a completed census or physical absence; COPY is
the actual measurement. Each manifest remains capped at 100 GB.

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
successful verifier exit and the canonical verifier's own `checked_at`, copied
unchanged into `recovery_checked_at`. The canonical CLI now records checked_at at
the start of its HEAD pass, so every HEAD occurs after that time. The tick checks
this plan's SHA, issued count and newest issued-item `updated_at` in DB.
Verified must equal issued, problems must be zero, and checked_at must be strictly
later than that newest timestamp. `updated_at` is a conservative upper bound on
permit issuance: a later receipt can also require a new verification. A changed
count or later issuance invalidates the report even while the count stays equal.
The tick never re-stamps cached evidence or re-HEADs objects inline. A real result
remains acceptable while its bound state is unchanged, up to seven hours (the
six-hour refresh cadence plus verification/scheduling margin). A binding or age
failure produces `recovery_evidence_stale`; the controller reruns the verifier. Historical plans are covered
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

`holds_by_reason` contains diagnostic COPY hold counters from the client heartbeat
`cleanup.r2_copy` (persisted at `connections.nas_cleanup_status->'r2_copy'`, with
`nas_cleanup_status_at`). These are boot-cumulative and never authorize cleanup.
`local_copy_mismatch_latched` comes from A1's durable inventory SQLite latch,
which can precede the server blocked-row ACK. `local_copy_checked_at` records
that independent observation time; do not substitute the time a stale heartbeat
was fetched. A positive local or server latch stops the tick. Local mismatch and
master observation timestamps must be fresh.

Root's latest valve decision (2026-11-17) supersedes hold-based interleaving:
read current `connections.nas_storage_free_bytes` and `nas_storage_reported_at`
directly. Recommend cleanup only if free bytes minus the unchanged 2 TB COPY
floor is strictly below the typed 300 GB headroom threshold. The storage heartbeat
must be no older than ten minutes (and no more than 30 seconds in the future).
Missing/stale storage evidence gives no recommendation and a visible reason;
it does not stop metadata feeding. Historical hold counters, memory holds,
transport holds and empty queues do not decide the valve. The controller alone
acts on the recommendation.

Both safety and copy status timestamps must be no older than five minutes and
no more than 30 seconds in the future. Copy status is produced afresh by A1's
`stoaramactl nas-r2-copy status --json`, schema
`stoarama.nas-r2-copy-status.v1`. Its mismatch latch and durable copy statistics
are copied into finish status. Queue depth and `items_per_min` (verified in the
last five minutes divided by five, including idle time) are refreshed from DB.
On a safety stop, cleanup is always false. On a successful tick it follows only
the current free-space headroom rule above. The controller runner alone acts on
this decision; an empty queue is not cleanup authority.

## Install packet

Sole controller, at a natural boundary after A1 is merged/deployed and a fresh
v4 heartbeat provides local mismatch/hold evidence:

1. Build the merged CLI on Jelly using `darma storage check` then
   `darma run --slot -- go -C backend build -o <install>/stoaramactl ./cmd/stoaramactl`.
   Copy `tick.py` and `refresh_recovery.py` to the private install directory. Do not reuse or modify the
   cleanup runner's code/state/launchd job. Create a separate private state dir.
2. Provide the executable safety collector described above. Configure credentials
   non-interactively for the CLI and collector using existing controller auth;
   never put secrets in the plist, arguments, logs, or this repository. The wrapper
   deliberately loads no env file. Configure a working `darma msg` notification
   target with `--notify-agent`; it must route to Deniz.
3. Provide a separate controller-owned executable `__PLAN_SOURCE__` emitting
   `{"plan_id":1440}` for the actual current/latest runner plan. It must fail on
   unknown ownership and use no inline verifier. Run the refresh once:
   `python3 <install>/refresh_recovery.py --cli <install>/stoaramactl --state <state> --plan-source <plan-source> --notify-agent <target>`.
   This runs the real read-only `nas-cleanup verify-recovery`, with its own
   30-minute timeout, and atomically writes `<state>/recovery.json`. It preserves
   the CLI's own checked_at and canonical identity/count fields; problem detail is
   redacted. Failure publishes a failed record; if replacement publication fails it unlinks
   the old canonical record, so it cannot remain consumable. Malformed JSON shapes
   also enter this failure path. It alerts and retains the last good
   result separately as `recovery-last-good.json` (never a fallback for collection).
   The safety collector reads the canonical record, maps `delete_ever_issued` to
   `recovery_issued_items` and copies `checked_at` without modification; it fails
   on `refresh_error`. It does not invoke the slow verifier in its 90s budget.
4. Before installing launchd, run the fast tick wrapper once with absolute paths:
   `python3 <install>/tick.py --cli <install>/stoaramactl --state <state> --safety-collector <collector> --notify-agent <target>`.
   Verify exit 0, a current `<state>/status.json`, no `stop_reason`, queue growth
   toward 10k (or `candidates_exhausted`), copy metrics, and the expected cleanup headroom decision (or a missing/stale reason). Confirm cleanup/master/STOP were unchanged.
5. Render both plist templates with XML-escaped absolute `__INSTALL__`, `__STATE__`,
   `__COLLECTOR__`, `__PLAN_SOURCE__`, `__NOTIFY_AGENT__`. Run `plutil -lint`, copy the rendered tick plist
   to `~/Library/LaunchAgents/com.stoarama.finish-tick.plist`, then
   `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.stoarama.finish-tick.plist`.
   It runs immediately and every 120 seconds; launchd and a wrapper lock prevent
   overlap. Verify the next automatic timestamp. The independent Rolly dead-man
   check reads the output age and `stop_reason`; CLI status is replaced atomically.
   Also install/bootstrap `com.stoarama.finish-recovery.plist` from its separate
   template: it runs immediately and every six hours, with a separate lock and
   timeout. It performs only canonical recovery HEAD verification, never cleanup
   or master control. Verify `recovery.json` has its own fresh checked_at.
6. To stop only the feeder:
   `launchctl bootout gui/$(id -u)/com.stoarama.finish-tick`.
   To stop its independent refresh, boot out `com.stoarama.finish-recovery`.

## Stops

| `stop_reason` | Meaning / controller action |
|---|---|
| `evidence_unavailable` | Missing, malformed, incomplete, stale or future evidence. Repair collection; never stamp old facts as fresh. |
| `recovery_evidence_stale` | Canonical result does not bind current/latest plan SHA, issued count and newest issued-item update, or exceeds the seven-hour refresh budget. Run the real refresh; never re-stamp. |
| `cleanup_recovery_problems` | Current/latest plan recovery verifier reports problems > 0. Resolve missing/conflicting R2 recovery evidence. |
| `unknown_enabled_cleanup` | DB enabled plan/batch not owned by the controller. Reconcile ownership; do not adopt it blindly. |
| `copy_mismatch_latched` | A1 reports durable checksum/final conflict. Reconcile the affected exact key; never clear to feed. |
| `shutdown_or_recovery_failed` | Failure latch, non-null recovery_error or an unrecognized last_error. Controller resolves recovery/shutdown failure first. |
| `deletion_master_mismatch` | Live master differs from expected, or ON has no owned plan/batch. Reconcile actual runtime state. |
| `database_unavailable` | Bounded DB read/lock unavailable or deadline exceeded. Inspect privately; wrapper alerts. |
| `manifest_import_failed` | Manifest/source/hold/equality validation or import failed. Existing queue state survives; inspect source identity. |
| `tick_already_running` | Another feeder holds the DB lock. Check duplicate installation. |
| `status_write_failed` | CLI could not publish status; stderr only. Repair storage. Dead-man sees the old timestamp. |

Every CLI stop exits nonzero; wrapper alerts with `darma msg`. Dependency failure
also alerts and leaves the last CLI status age unchanged. State directory/lock failures also alert. A failed invocation without its own
status reports `current_status_unavailable`, never a prior stop reason.
Raw subprocess errors
are suppressed to keep credential-bearing URLs out of logs/notifications.
