# Finish tick (controller installs; no production changes here)

`stoaramactl finish tick --safety FILE --copy-status FILE --status FILE` feeds
keep28 raw metadata for account 47 / connection 13. It never changes cleanup,
master switches, NAS files, R2 objects or deletion budgets. A1's
`nas-r2-copy status --json` and A1b's reviewed server-challenge storage (exact `r2-copy-v5`/`r2-copy-v6`)
boundary are required before installation. B and stitching are
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
every state are excluded from ordinary import; blocked work is never requeued.
Verified work is unchanged except for the explicit bounded verify-only refresh
lane described below. Live
source validation, holds, destination ambiguity and queue equality stay in the
existing exact-manifest importer. Partial imports remain stopped until explicit controller reconciliation. An advisory
lock excludes concurrent feeders. Existing queue IDs/order are preserved.

Tier 3 requires fresh storage data and at least 300 GB headroom above the 2 TB
floor. Root's conservative admission window reserves the full size of pending and
leased raw rows, plus every terminal raw row updated within T=180 seconds,
regardless of verified-in-place/downloaded/blocked label or committed bytes.
This also conservatively covers recently completed tiers 1/2 for at most T.
It needs no publication ledger, completion-label inference or sample timestamp.

Budget freshness is proven on the database clock, independent of network delay
or client lifetime. Every successful heartbeat stores a new random 64-hex nonce
and its server issuance time. Only exact v5/v6 responses expose the private
`storage_challenge_nonce`; legacy joined/proof envelopes omit it entirely.
V5/V6 extract and store it before passing the remaining response to the unchanged
strict control parser. Each client spends that nonce once, immediately before
sampling storage for the next request, then echoes `storage.challenge_nonce`. Without a nonce it
still sends fresh display-only storage; no budget is granted for that cycle.

Under the existing connection row lock, the storage UPDATE checks the exact
current nonce, exact v5/v6 literal and nonfuture issuance no older than 180 seconds.
It atomically saves that **matched old issuance** in `nas_storage_budget_at`, binds
the sample to its actual accepted version, and rotates the next challenge. Rotating or delivering a later
response cannot refresh an earlier sample. Missing, expired or mismatched nonces
update display storage normally but clear budget eligibility. A lost response
or replay therefore gives a normal no-budget cycle; the next response provides
a fresh challenge. Omitted storage preserves the earlier sample's proof/time;
unavailable or legacy storage clears it. Old rows start without any proof.

Migration `0203_nas_storage_client_binding.sql` stores the challenge and accepted
sample separately. The CLI requires an exact v5/v6 `nas_storage_client_version` via the shared
`api.SupportsNASStorageBudget` predicate
and `0 <= DB clock - nas_storage_budget_at <= T=180s`. T is shared server policy
(`api.NASStorageBudgetMaxAge`), with an inclusive terminal-debit cutoff. A delayed
complete request accepted at t200 with challenge t60 still has budget time t60;
a tick at t250 refuses it. There is no M/receipt arithmetic, HMAC secret, client
clock comparison or fallback. The child's 20-second watchdog limits wasted work;
it does not establish sample freshness. The separate cleanup valve requires the same proven sample, allowing its
server issuance time and display receipt to be up to ten minutes old. Holds never grant cleanup authority.

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
The sample must also have an exact v5/v6 proof whose server issuance is nonfuture
and no older than ten minutes; a fresh display receipt alone has no authority.
Missing/stale storage or challenge evidence gives no recommendation and a visible reason;
it does not stop metadata feeding. Historical hold counters, memory holds,
transport holds and empty queues do not decide the valve. The controller alone
acts on the recommendation.

Both safety and copy status timestamps must be no older than five minutes and
no more than 30 seconds in the future. Copy status is produced afresh by A1's
`stoaramactl nas-r2-copy status --json`, schema
`stoarama.nas-r2-copy-status.v1`. Its mismatch latch and durable copy statistics
are copied into finish status. The live admission queue depth is refreshed from DB; `items_per_min` is the sum
of A1 JSON's rolling-60-second verified-in-place and downloaded completion rates.
On a safety stop, cleanup is always false. On a successful tick it follows only
the current free-space headroom rule above. The controller runner alone acts on
this decision; an empty queue is not cleanup authority.

## Install packet

Sole controller, at a natural boundary after the combined D/A1b boundary review:
A1 + #526 and the reviewed storage-sample boundary remain prerequisites. Follow
[the canonical COPY/collated rollout](../../../docs/operations.md)
for the complete schema/index, API/CLI, all-caller quiescence and immutable v6
cutover order. Audit and retarget private collector/version pins **before v6
activation**, as required there; do not activate until its preactivation gates
are complete. After activation, verify two v6 heartbeats with
`nas_storage_client_version='r2-copy-v6'` bound to the preceding server challenge
time, then install D. A v4 observation packet is not late-budget eligible.

1. Build the merged CLI on Jelly using `darma storage check` then
   `darma run --slot -- go -C backend build -o <install>/stoaramactl ./cmd/stoaramactl`.
   Copy `tick.py`, `refresh_recovery.py`, `admission.py` and `reconcile.py` to the private install directory. Do not reuse or modify the
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
   Python 3.9 validates a normalized six-digit fractional copy of Go RFC3339Nano
   timestamps; the canonical string itself is preserved byte-for-byte.
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
also alerts and publishes a stop. State directory/lock failures also alert. A failed invocation without its own
status reports `current_status_unavailable`, never a prior stop reason.
Raw subprocess errors
are suppressed to keep credential-bearing URLs out of logs/notifications.

## Bounded keep28 custody refresh (PR2)

The sole controller may enable the optional refresh lane after deploying migration
0205, the new backend and capable NAS worker at its existing maintenance boundary:

```sh
"$STOARAMACTL" r2-inverse refresh on
# Add these to the existing tick.py/controller installation:
# --refresh-picks "$PICKS_FILE" --expected-refresh-picks-sha256 "$PICKS_SHA"
```

The default DB flag is off. Missing optional tick arguments preserve ordinary
feeding. Pausing B2 uses `r2-inverse refresh off`; D admits no new refresh and the
backend grants no new refresh leases. Existing guarded hashes may finish. Queued
or expired-leased refresh work remains retryable when enabled later. At most the
capped queue ages out; B2 independently refuses stale custody. There are no pacing
windows, revocation/adoption journals or new proof protocols.

This deliberately narrows “verified work is never requeued” only for exact,
unblocked keep28 RAW in account 47 / connection 13 / bucket stoarama whose matching
inventory expires within one hour. D allocates refresh at most half the current
total deficit **before** ordinary fill. Ordinary keeps the other half, including
odd spare slots and unused refresh capacity. Refresh is capped at 5,000 outstanding
rows inside the unchanged 10,000 total; that is 10m25s of B2's 8/s canary throughput,
below one consumer-hour. Purged sources are excluded before the catalog limit.
A persisted custody-time/source-ID cursor makes each bounded 10,000-row page
circular, so later ticks/restarts progress past rejected KE/held/changed rows.
The cursor advances before per-key work and grants no admission authority.
The refresh catalog read reuses the scoped 60s read-only candidate transaction;
pooled queries, writes and the four-minute tick deadline keep their existing
budgets. Each tick retains the existing 100 GB byte bound, admitting
the complete 64 GiB COPY domain. A row too large for the remaining allowance is
skipped so smaller following rows progress; it can fit a subsequent fresh batch.

Fresh storage proof, full-file capacity debits, memory/cgroup/load/free-space and
cleanup/stitching gates remain. KE streams/paths, delivery and dependency aliases,
incidents/live holds, A1 blocked keys, ambiguity, collated, tool prefix and any
retirement reservation/intent veto refresh. Exact key serialization handles both
admission orders. Refresh changes no source identity, removes no intent, and
publishes no freshness until the existing worker hashes stable media and proves
its sidecar. Inventory replay preserves verified_at and cannot extend expiry past
that timestamp+24h; B2 continues locking the exact complete proof through wire.

DB work mode `verify_existing` uses `refresh_pending`/`refresh_leased`, which legacy
lease and token SQL cannot select. Constraints prohibit ordinary pending/leased
states and RANGE grant fields for that mode. A combined four-live-lease DB guard
also covers old-server admissions. Ordinary shared-lock renewals do not upgrade
locks. New-server dispatch alternates modes when both have work; ordinary FIFO
is preserved. Old clients never get refresh tasks. On rollback, disable refresh
and retain its DB guards/states; never reset them into ordinary copy. A new worker
against an old server may hold on the unsupported capability field; rollback the
worker together with the server for ordinary work. No download fallback exists.

Verify-only workers open existing parents/media without creating either. Missing
media is durably `verify_missing`; media partials are not recovered or removed,
and no RANGE, download, quarantine or replacement occurs. Sidecar conflicts remain
protected. Worker and CLI status expose refresh completion separately from first
transfer throughput. Original completion, verified_at, downloaded/committed totals
and cumulative attempts are retained across refreshes; `custody_refreshed_at` and
`refresh_count` record successful refreshes. Recent-terminal full-file debits stay
conservative even for a zero-download hash.

The controller measures refresh queue age, actual hash rate and delay from fresh
custody to B2 removal. Repeated hashing and imperfect timing are accepted bounded
waste while enabled. No refresh counter or enable flag grants DELETE authority;
B2 still checks exact fresh locked custody, policy/holds, HEAD7d, mature0198 fence,
substantiated drain evidence, durable intent/ledger and its own single-use token.

## Durable admission and reconciliation

Both wrappers retain their existing lane flocks. A short shared `admission.lock`
serializes lane-lock acquisition, journal transitions and status publication, without serializing the
30-minute verifier with ordinary feeding. `admission.json` records an active
attempt **before any subprocess**. A failed or interrupted attempt stops both
lanes until controller reconciliation. Later launches exit nonzero, start no
subprocess and never overwrite failure evidence or publish success. An in-flight
sibling cannot publish success after the other lane latches a failure.

Each failed attempt retains its private `tick-<id>` / `recovery-<id>` directory:
input evidence, exact generated import manifests (fsynced before import),
original CLI status/verifier output, return code if available,
and failure reason. Raw verifier detail stays private, out of canonical status
and logs. Successful scratch attempts are removed. Existing failed status or
recovery evidence is archived under `legacy-<id>` and latches first installation;
absence of a new journal never erases a legacy failure. A full-disk failure to
record a result leaves the pre-subprocess active marker unresolved, blocking retry.

Only the sole controller, at a newly authorized boundary, may reconcile. First
stop admission and reconcile the actual attempt's entire committed queue subset,
including terminal rows, against pre-attempt identities and manifest/policy
hashes. Counters and queue-depth differences are insufficient. Preserve the
read-only extraction and actual before/after comparison as the evidence artifact;
never delete/requeue work or remove any hold to make it pass.

`python3 <install>/reconcile.py --state <state> --inspect` acquires **both** lane
flocks, refuses running work, and prints the exact `latch_sha256` and journal.
Bind two retained JSON artifacts to that digest:

- Authority: `controller` (identity), `authority` (new explicit root authority),
  `latch_sha256`.
- Evidence: `latch_sha256`, `queue_reconciled: true`, and `reconciled_attempts`
  listing every directory in the journal's `active` map. Include the actual
  queue extraction/comparison and its hashes in this artifact. The controller
  is responsible for reviewing that evidence; the command validates bindings,
  not production truth.

Then run `python3 <install>/reconcile.py --state <state>
--expected-latch-sha256 <digest> --authority <authority.json>
--evidence <evidence.json>` as one command. It refuses changed bytes or running
work, durably records controller, OS UID, time, full stopped journal and both
artifact SHA256s under `reconciliations/`, and clears admission only. All failed
attempts and canonical errors remain. The digest covers every active attempt's
evidence, so an in-flight outcome cannot be omitted from reconciliation.

Resumption is **recovery first**: ticks remain blocked without relatching until
one actual recovery invocation succeeds. Then normal feeding may resume, using
fresh evidence. Neither periodic launch can reconcile, restore last-good data,
or restamp the old recovery timestamp. No command here grants production GO.

The ordinary selection and live loader independently consult immutable incident
source IDs and roster identities/keys, including collated dependencies. Migration
`0209_nas_cleanup_permanent_holds.sql` adds unconditional DELETE/TRUNCATE rejection
on source holds. No legitimate removal lifecycle exists: releases use append-only
receipts. Incident roster/source views are immutable VALUES views from 0202 and
already reject DELETE/TRUNCATE. There is no data rewrite or role exemption.
