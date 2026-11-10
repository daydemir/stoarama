# NAS cleanup: preparation and explicit enablement

Operator commands use the existing service credential, matching the other
service control APIs. Account and NAS pull keys cannot create or enable plans.

The NAS app polls the backend over its existing authenticated outbound channel.
No SSH or new service is required. Preparing a plan never enables deletion.

`stoaramactl nas-cleanup prepare --input plan.json` accepts a label and up to 10,000
explicit `{kind,source_id}` pairs (and at most 100 GB of media). Supported media kinds are raw and collated;
unknown/legacy joined artifacts remain retained. The backend derives identities
from account 47/connection 13 catalog rows, requires their exact observed paths and
fingerprints in the complete audit, and checks the independently reviewed folder
policy. Import labels and manifests are immutable and idempotent.

The app opens the mounted root and each ancestor with directory FDs and
`O_NOFOLLOW`, rejects nested devices/hardlinks, verifies original fingerprints and
full SHA, and parses/binds raw sidecars including registered dates. It reports the
measured root/ancestors/file/sidecar identities and current parent membership.
Every outcome is durably journaled in the existing app-owned inventory database.
App replacement waits for the cleanup worker to checkpoint and stop.

The backend revalidates source identities, all 221 previously protected UTC windows (223 deduplicated
windows after the November2 refresh),31 whole-stream protections, all three current
registered/availability/quarantine rankings, current source/error references and
public delivery. Unknowns, changed metadata, shifted sensitivity windows and
unsupported content stay retained. Historical inventory MATCH or download ACKs
are never promoted into cleanup proof.

Report a plan with `stoaramactl nas-cleanup report --plan-id ID`. Results separately
show full R2 SHA verification, proven 404 absence, unchecked/failed recovery,
physical conflicts and policy holds. HEAD equality alone is not recovery proof:
every dry run streams the exact ETag generation and hashes it; execution may
then rely on a conditional HEAD bound to that verified generation (below).

To retry a transient recovery failure in a finished dry run, use the service-token
operator command:

```sh
stoaramactl nas-cleanup recheck --plan-id ID --expected-plan-sha256 SHA --reason "retry recovery timeout"
# Refresh all would_delete observations when the dry run is older than 24 hours:
stoaramactl nas-cleanup recheck --plan-id ID --expected-plan-sha256 SHA --reason "refresh expired dry run" --all
```

This calls `POST /api/v1/service/nas-cleanup/plans/{id}/recheck` with
`{expected_plan_sha256, reason, scope: "unverified"|"all"}`. The default
`unverified` scope resets only `would_delete` items whose recovery is neither
`r2_sha_verified` nor `r2_md5_etag_matched`, and `unverified` items. `all` also
resets verified `would_delete` items. Keep, conflict, missing, pending, leased,
and deletion states stay untouched. Reset items lose their previous lease,
proof and recovery evidence and return to `pending` for the normal NAS client
re-hash and server policy/recovery checks. Attempts remain monotonic to reject
stale observations; a separate baseline grants three new lease-expiry attempts.

Recheck requires the exact plan SHA, account 47 / connection 13, the current
policy, mode `dry_run`, and **no item with `delete_ever_issued`**. Paused plans
must first use `resume-dry-run` (which restores source holds); enabled and
retired plans are refused. It never changes plan mode, approval, `allow_nas_only`,
or permits. The required reason is recorded in the server's `nas cleanup recheck`
log line with the plan digest, scope and reset count; `approval_reason` is
unchanged. Pull keys cannot access this service route. Resetting timestamps
does not satisfy enable: every item must finish a fresh, permitting dry run.

Two independent gates remain OFF during preparation:

1. Backend `NAS_CLEANUP_DELETION_ENABLED` defaults false.
2. A plan defaults `dry_run`. Explicit `nas-cleanup enable` requires its exact
   `--expected-plan-sha256`, `--reason`, a complete matching dry run less than 24 hours
   old, and full R2 recovery (with ETag evidence) for every item. Approval
   expires after `--approval-hours` (default 2; 1..24, above 4 only with
   `--allow-long-approval`), or approve a whole batch of plans at once (below).

### Belt-and-braces safeguards (safety council, 2026-11)

All are code constants (raising one is a reviewed code change) and fail closed:

- **No NAS-only deletion.** `allow_nas_only` is rejected (API 400, DB check
  `nas_cleanup_plans_no_nas_only`); a permit is issued only for an item whose R2
  copy was streamed and SHA-verified. Re-introducing NAS-only deletion needs its
  own reviewed change.
- **One enabled plan** at a time; a plan above **100 GB** cannot be enabled; no
  permit is issued beyond a rolling **5 TB / 24 h** of issued deletions
  (raised from 3 TB by owner decision, 2026-11).
- **Auto-pause.** During execution any observation that is not a permit
  (unverified, conflict, missing, client error, policy/recovery mismatch,
  budget), an expired execution lease, or any receipt other than `deleted`
  pauses the plan.
- **NAS client guard** (independent of the server): pinned copy of the keep28
  selection and of `clients/nas-pull/nas_cleanup_folder_streams.json`, both
  SHA-checked at startup (a failed check refuses every deletion; other work
  continues). Before each unlink the client maps the folder to a stream, reads
  the interval from the physical sidecar (raw) or the output bounds confirmed
  by the dated path (collated), and refuses on keep-window overlap, unknown
  folder/bounds or a disagreeing server `stream_id`. Hard local caps: 400,000
  files / 5 TB per rolling 24 h (bytes bind first; raised from 50,000 / 3 TB
  by owner decision, 2026-11). Every unlink is bracketed by fsynced intent and
  result lines in `/state/cleanup-deletions.jsonl` (append-only). A media unlink
  whose sidecar step failed is receipted `media_deleted_sidecar_failed`.
- **R2 purge fence.** `nas-verified-purge` refuses to run (dry run included)
  while any cleanup plan is enabled and skips clips held by any non-retired plan.
- **Recovery check.** `stoaramactl nas-cleanup verify-recovery --plan-id ID`
  HEADs (read-only) the R2 copy of every item whose deletion was ever issued
  and reports any that are missing or differ in size (exit status 2).

Runbook notes:

- **An auto-paused plan that already deleted items cannot be re-enabled.**
  Enable requires every item to be a fresh `would_delete`, and resume-dry-run
  refuses plans with any issued deletion. Investigate the pause reason
  (`nas-cleanup report`, `approval_reason`), run `verify-recovery`, then close
  the paused plan as described below before preparing a new plan.
- **An expired approval pauses.** Once `approved_until` passes, no permit is
  issued; the next lease (or batch advance) sets the plan and its batch to
  `paused` (reason "auto-paused: approval expired") so it no longer blocks
  other enables or purges. Outstanding 30 s permits may still be receipted.
- **Deploy the API (and promote a NAS client) only while no cleanup item is
  `leased`**: pause any enabled plan first and let outstanding leases finish or
  expire. The client also refuses in dry runs (reported as `conflict`), so a
  plan the client disagrees with fails to enable instead of pausing mid-run.
- Migrations 0171 (metadata-only, NOT VALID) and 0172 (VALIDATE, which proves
  no legacy `allow_nas_only=true` row exists) run as separate transactions.

Enablement is a separate user decision. Do not change either gate while doing
preparation or dry runs. A later exact-file permit expires after 30 seconds. The app
fences its delivery writers and re-hashes/rechecks each approved file before its
final exact-name unlink. Directories and unlisted siblings are never removed.
Changing sibling membership after one file finishes does not change another
file's identity approval; that next file still measures a fresh stable parent.

A lost permit response is reconciled without executing deletion. An interrupted
or partially failed destructive attempt is reported `unverified`; it is never
retried blindly. A monotonic ever-issued flag preserves recovery holds even after
uncertain outcomes and pauses. DB triggers fence old purge binaries, source/date/
namespace changes and new joined/collation references. Successful NAS removal
retains its R2 recovery copy; this workflow does not authorize R2 deletion.
Source-reference fences end with approval and permit grace, so retained R2
sources can be used for future stitching. Dry runs do not prohibit new source
references; those references are rechecked before any deletion grant.

While approval/permits can be outstanding, public delivery serves the immutable
reviewed generation. A shared DB fence serializes public reads with enablement;
new generations are deferred until approval and the short permit grace end.
Existing reviewed public downloads remain available. A changed registry prevents
further cleanup permits until reviewed.

`nas-cleanup pause --plan-id ID --expected-plan-sha256 SHA --reason TEXT` stops
future grants. An already issued 30-second permit may finish. Recovery holds for any
ever-issued operation remain; pausing never asserts that NAS bytes survived.

To continue the never-issued remainder of a paused plan, use the service-token
control (works with the deletion master switch OFF):

```sh
stoaramactl nas-cleanup close --plan-id ID --expected-plan-sha256 SHA --reason "split never-issued remainder after timeout"
```

`POST /api/v1/service/nas-cleanup/plans/{id}/close` requires
`expected_plan_sha256` and a nonblank `reason` of at most 500 bytes. Under the
public, per-plan and row fences, it accepts only `paused` (or already `retired`
for idempotent retry), refuses any `delete_issued` item even with an expired
permit, and refuses a `leased` item with an unexpired lease. Let leases finish
or expire; never infer an unlink outcome from a timeout. It atomically reuses
the terminal `retired` mode, clears approval, records the reason and pauses any
enabled batch containing the plan. It cannot be resumed or enabled, including
by a previously approved batch. Repeating close preserves the first reason.
No file, item, manifest, receipt, attempt, proof, or hold row is deleted or reset.

Recovery retention deliberately stays **conservative and plan-wide**: if any
item ever received a deletion permit, every source/recovery hold of that plan
remains permanent, including after close. `nas_cleanup_clip_held` remains true
and the DB purge guards still refuse destruction of those R2 recovery identities.
This may retain extra R2 data for never-issued items; freeing that storage is
outside this control. A plan with no issued items uses existing retirement
semantics and releases its holds.

The shared `nas_cleanup_reserved_files` view makes never-issued files of retired
plans available to both `prepare_keep28_plans.py` and server admission. Issued
files remain excluded across policies and states, including after an uncertain
unlink, except for an audited unexecuted reservation release (below). Missing legacy item rows also stay
excluded. Other non-retired plans still reserve their files. Prepare with a
**new `--label-prefix`** because old labels and manifests are immutable. The new
plan starts `dry_run`/`pending`, requires a full new dry run with recovery
verification, and needs its own explicit owner approval; old approval does not
carry over. Reservation availability does not guarantee policy admission.

For an `unverified` ever-issued item with any possibility of unlink, leave it
uncertain and protected indefinitely. The narrow audited reservation release
below applies only to receipts that prove a recognized pre-unlink code path.
Close itself never asserts presence or deletion and never releases issued
reservations. Keep the client journal, ledger and report evidence.

The original 430 folder roots / 6.43 TB logical are policy candidates, not an executable
recursive command or guaranteed reclaimed storage. This workflow materializes a
narrower exact-file set. NAS-only treatment and the final selected batch are left
for explicit approval. Full kept 28-day transfer and decoded optimality are separate.

Bulk verification performance: parent fingerprints describe byte-exact entry
names, not unrelated sibling metadata. Full target/sidecar hashes, inode/stat/
hardlink checks, directory-FD confinement and final permit deadlines still apply.
Client namespace/hash/proof durations and backend policy/recovery durations are
reported separately. Dry-run ranking snapshots may be reused for at most five
minutes; enabled observations query live rankings or, after activation, the
live fingerprint of the replay's inputs (full replay on any difference). Cached preflight evidence
never authorizes a deletion. Both deletion gates remain OFF by default.

The November2 policy refresh covers all109 NAS streams in one read-only repeatable-read snapshot. All221 previous UTC intervals and all31 whole-stream holds remain; two newer intervals are added. Unassigned catalog records remain retained. Source snapshot and policy gzip fixtures are pinned by their full SHA256 values and monotonic-protection tests.

Full-manifest preparation uses a content-addressed folder index of the original
immutable audit. Its pinned root binds all2,943,864 observations, the original
audit SHA and reviewed folder ownership to3,918 fully verified shards. Admission
fetches only the required folders, checks every referenced shard hash, and
retains all original frozen-stat, sidecar, hardlink, owner and duplicate checks.
A missing, corrupt or mismatched index fails closed. This index grants no
deletion authority; both gates and fresh live checks remain unchanged.
The metadata-only producer is backend/scripts/nas-cleanup/build_frozen_index.py.

### Throughput (protocol 2, batch approval; 2026-11)

The pilot (plan 1388) ran at ~156 files/hour: per file a full best-14 replay
(~0.8-4 s), a full R2 re-download + SHA (~0.6 s), three serial round trips and,
dominating, ~60 s client back-off after each transient API error. None of the
gates above changed; the work each gate costs and the approval granularity did.

- **Plans** hold up to 10,000 items (still at most 100 GB). Each item row keeps
  a write-once copy of its manifest entry and size (derived in SQL from the
  stored manifest at admission), so leases, observations and the 24 h budget no
  longer detoast a multi-MB manifest per item.
- **Best-14 replay once per plan.** Enabling a plan runs the full live replay
  per stream and stores the winner together with a SHA-256 fingerprint of the
  replay's exact input rows (every valid clip interval, timezone and
  quarantine flag of the stream), computed in the same statement. Each
  execution observation recomputes only the fingerprint (once per batched
  request); equal fingerprint means the replay would return the same pinned
  winner. Any difference runs the full replay as before (a different winner
  keeps the file and auto-pauses).
- **R2 recovery without re-download.** Every dry run byte-verifies the copy
  (now via `If-Match` on the HEAD's ETag) and records `{object_key, etag,
  version_id, size, sha256}`. Execution issues a conditional HEAD (`If-Match`
  that ETag and the version id) and requires the same size/ETag/version and any
  stored `sha256` metadata to equal the catalog SHA. ETags are content
  derived, so a replaced object fails; 404 is NAS-only (no permit), 412 or any
  difference is a conflict and other errors are unchecked: all auto-pause.
  Such a check is recorded as `r2_etag_bound` (never `r2_sha_verified`, which
  stays reserved for full streamed hashes) and permits only during execution.
  Enable refuses any plan with an item lacking dry-run ETag evidence.
  Missing evidence (plans dry-run by older servers) falls back to the full
  re-download.
- **Dry-run recovery without downloading (2026-11, second pass).** The NAS
  client computes the media MD5 in the same read as its SHA-256 and, when the
  lease advertises `recovery_md5`, sends it with the observation. The server
  HEADs the R2 object and records `r2_md5_etag_matched` when the size matches
  and the ETag is a plain single-part MD5 equal to the client's MD5 (R2's
  single-part ETag is the MD5 of the stored bytes), any `sha256` metadata
  agrees, and the proof's SHA-256 equals the catalog SHA (R2 bytes == NAS bytes
  == catalog). Multipart ETags (`…-N`), a missing or different MD5 and any
  other doubt fall back to the full download + SHA-256 (`r2_sha_verified`). A
  random ~1% of matches are also fully downloaded as an audit; a disagreement
  is recorded as `conflict` and pauses the plan (dry-run plans too). Enable
  accepts `r2_sha_verified` or `r2_md5_etag_matched` with ETag evidence.
- **No R2 I/O under locks.** Both observation endpoints compute every R2
  check before their transaction (16 in parallel for dry runs; execution
  keeps its 15 s total budget). A transport 503 alone is not a no-commit
  certificate. Dry runs may retry; execution retains its journal and uses
  reconciliation-only recovery. (Before this, a large dry-run batch of big
  objects could exceed the proxy timeout; the client then fell back to serial
  single-item replays, each downloading inside its transaction.)
- **Protocol 2** (`lease` with `protocol: 2`, `observe-batch`, `result-batch`):
  up to 64 tasks per lease, observations and receipts batched up to 64 per
  request. Execution work is leased only within the client's remaining local
  24 h caps (sent with the lease) and the server's remaining 24 h budget
  (issued + already leased); a lease never mixes dry-run and execution tasks.
  Every request is retry-safe: a lease `request_id` returns the same leased
  tasks; an observation `request_id` returns the recorded decision, or for an
  identical request the one permit already issued with its original expiry
  (never a new or extended permit), and only while the plan is still enabled,
  approved and the master switch is on; receipts are idempotent. Batched and
  single observations take locks in one order: plan fence(s), then budget. The client retries
  502/503/504/429/timeouts with short back-off instead of pausing a minute.
- **Serialized execution.** The server permits one outstanding execution item
  per plan and requires `reconciliation_protocol:1` on leases and observations.
  Older clients receive dry-run work only. Protocol 2 requests at most one execution lease
  and drains its full observation/result lifecycle before leasing another.
  An execution mutex also serializes already-leased tasks and direct callers
  before opening files or starting the permit clock. Dry runs retain sixteen
  workers and up to 64 in-flight leases. Writer fences, confined descriptors,
  fresh proofs, namespace checks, local caps and ledger lines still apply.
- **Batch approval.** `prepare_keep28_plans.py --submit` writes
  `batch-<prefix>-s<stream>.json` (ordered plan ids/digests and
  `batch_sha256 = sha256("stoarama-nas-cleanup-batch-v1\npolicy <sha>\n" +
  "plan <id> <sha>\n"...)`). After the dry runs complete, the owner approves it
  once: `stoaramactl nas-cleanup enable-batch --batch-input FILE
  --expected-batch-sha256 SHA --reason TEXT [--approval-hours 2]` (above 4 h
  only with `--allow-long-approval`; `prepare --batch-by all` needs
  `--i-understand-large-batch`). The backend
  requires the master switch, the exact digest over the plans' current digests
  and policy, no other enabled plan or batch, and that every plan is a
  policy-current dry-run plan with no issued deletion that passes the per-plan
  gates now. It enables only the first plan. When a plan finishes cleanly
  (every item `deleted`) it is closed (`paused`, reason `batch-completed`, holds
  kept) and the next plan is enabled only after passing every per-plan gate
  again (fresh <=24 h dry run, verified R2 copies, <=100 GB, one enabled plan,
  master switch, approval not expiring within a minute). Any anomaly that
  pauses a plan also pauses its batch; a batch never re-enables a plan that
  stopped, and a paused batch cannot be re-enabled (approve the remainder as a
  new batch). `pause-batch --batch-id ID --expected-batch-sha256 SHA --reason`
  stops it; `batch-report --batch-id ID` shows per-plan states.
- The 24 h limits are code constants: server budget 5 TB, client 5 TB and
  400,000 files. At ~17 MB per raw clip 5 TB is ~290,000 files, so bytes bind
  first: about 4 hours at 20 files/s, then the pipeline waits for the window
  to roll (~16 TB total takes about 3.3 days).

Rollout: apply migration 0173 (additive; NOT VALID checks; one small partial
index), deploy the API while no item is `leased`, then stage/promote the NAS
client. Old clients receive only dry-run work against the new API; a new
client detects an old API (HTTP 400 on protocol 2) and stays on protocol 1 for
an hour before probing again.

Preflight is bounded to eight concurrent read-only tasks, with rolling refills
and at most64 leases per drain/replay cycle. Batch requests lease only free slots,
up to eight; legacy requests default to one task. One coordinator owns
receipt replay and drains every worker, including after a lease/worker failure,
before replay, replacement or serial execution. Under protocol 1 approved
destructive tasks are never dispatched concurrently; protocol 2 also drains
each execution lifecycle before leasing another. Independent observations share a plan read lock;
operator control still needs the exclusive parent lock. Public-registry evidence
may be cached for one minute only in dry runs; enabled grants always check live.

A verifier release can become stable when a fresh full physical proof receives
an exact matching nonexecuting backend acknowledgement with R2 SHA verification,
and this boot has passed authenticated heartbeat and fresh mount checks. This
does not clear legacy raw-feed errors or advance its progress. Replayed receipts,
empty leases and mere thread liveness cannot qualify a release. Promotion checks
the immutable version-specific manifest for the compiled CLIENT_VERSION and
installs only the captured verified source bytes,
not a mutable candidate pathname. In-process staging is serialized, and the
candidate path is retained so a concurrent external stager cannot lose its newer
release. A candidate mismatch or unavailable release metadata defers promotion without
blocking raw delivery. This also covers an external stager replacing the source
pathname during interpreter startup; the expected hash is not read from that path.
Optional heartbeat host.release
telemetry reports candidate/stable status through the existing connection API.

When a backend rollback rejects optional release telemetry or batch-limit fields
with HTTP400, the client retries once without only that optional field. All
authentication, source identities, bounds and other telemetry remain unchanged.
Other status codes and a second rejection use the ordinary retry/outage path.


### Receipt replay diagnostics and release

The NAS client bounds journal replay to 120 seconds before leasing other work.
It quarantines a receipt locally only after five definite failures spanning at
least one hour: HTTP 4xx except 408/429, or malformed/unprocessable responses.
Network errors, timeouts and server failures never count toward quarantine.
Quarantine preserves the current journal phase and bytes; it never deletes media.
Heartbeat cleanup status includes the total count, up to 100 item IDs and a
truncation flag. The server prefers items with fewer prior leases; cycles that
only skip unresolved receipts wait before trying again.

After resolving the underlying error, operators can request replay with:

```sh
stoaramactl nas-client release-quarantine --connection-id ID
```

The release generation remains in heartbeat responses across response loss and
old clients. The NAS records it durably after requeuing receipts. Conflicting
local journal rows preserve both copies for reconciliation. Replayed intents
and recovered permits produce unverified results; replay never executes deletes.

`stoaramactl nas-client logs --connection-id ID --since 1h --grep cleanup` reads
redacted diagnostics. Error-triggered uploads have a minimum 60-second gap,
including failed attempts. Server retention preserves seven days regardless of
upload count; the read endpoint filters and limits lines in PostgreSQL.


## Execution observation deadlines and refusal evidence

Enabled observations use one **15-second server deadline**, starting before account
authentication and covering connection/mode lookup, R2 recovery (including conditional
ETag-bound HEAD), policy checks, advisory/row locks, and transaction commit.
Both the single and batch endpoints use it. Expired authentication or recovery returns
HTTP 503 with `retryable=true` and no permit; timed-out recovery leaves the item
leased with no issued flag. Dry-run recovery retains its 40-second budget. An
enable racing a dry-run request is refused for retry under an execution context.

Execution clients send one observation directly per file, bypassing the dry-run
batch queue, retry loop, and request splitting. The request uses a 25-second
socket timeout; the independent **30-second monotonic age limit** still covers
the complete observation call and the final pre-unlink checks. Socket timeouts
are not a total network deadline (for example, a slowly streaming response can
outlast one), so a late response must still pass the age guard. Absolute permit
expiry, exact proof binding, the write fence, and local guards all remain
required. The age and absolute-expiry guards also run before each actual unlink.
A timeout/lost response retains the exact observation journal and follows
non-destructive reconciliation; it never prompts an immediate permit retry.

Deletion receipts add `observe_elapsed_ms`, `execution_elapsed_ms`, and
`unlink_attempted`. A pre-unlink refusal has `unlink_attempted=false` plus a
stable `refusal_reason`: `deadline_exceeded`, `proof_mismatch`, `permit_expired`,
`execution_disabled`, `local_guard`, or `pre_unlink_failure`. Proof/identity
acknowledgement mismatches retain the observation journal rather than trusting
an unbound permit for a receipt. Recovered observation permits are receipted as
`observation_replayed` with `unlink_attempted=false`; interrupted intent journals
remain uncertain. No elapsed duration is invented after a crash.

Every execution attempt also appends a JSON outcome to the client's inventory
SQLite table `cleanup_execution_outcomes` (`item_id`, `attempt`, `at`, `body`).
Receipt `execution_elapsed_ms` runs through the filesystem result; the local
outcome measures through the worker's return, including acknowledgement time.
Outcomes cover non-permit responses, transport errors, and acknowledgement
mismatches. `observe_elapsed_ms=null` means observation never started. Keeping
this separately preserves the immutable observation body used for replay.
New observation journals preserve proof key order as submitted: sorting the
proof before replay would change the byte SHA and prevent receipt acceptance.
Already-written journals with a different proof serialization remain subject
to the existing exact-binding checks; this change does not rewrite them.
If the state volume cannot store this diagnostic, the client logs that failure;
the existing durable journal and deletion ledger remain the safety evidence.

The server deliberately retains state `unverified` and pauses an enabled plan
after a refused **issued** permit, even when `unlink_attempted=false` proves this
client never called unlink. That flag distinguishes a refusal from uncertain
filesystem execution; it is not independent proof of current NAS presence and
does not erase granted authority. Issued identities and recovery holds stay
reserved. There is no automatic reset, new permit, or deletion retry. A fresh
inventory observation is still needed to independently prove current presence.

Deploy and verify the server before releasing this client: older servers reject
the optional receipt diagnostics as unknown JSON fields. Do not roll the server
back to a version without receipt support while this client or its journals are
active. Each legacy batch HTTP retry has its own server deadline; transport delay
and time across retries remain bounded by the client's independent 30-second
guard, not by the server's per-request timer.

Regression harness (local disposable PostgreSQL only):

```sh
cd backend
STOARAMA_TEST_DATABASE_URL=postgres://localhost/stoarama_test go test ./internal/api -run '^TestNASCleanup(ExecutionDeadline|NeverAttemptedReceipt|DryRunKeepsRecoveryBudget)' -count=1 -v
```

The harness uses the real Python worker, disposable media/sidecars and fake R2.
It runs both client protocols against normal HEADs, 35-second HEADs, and lost
permit responses, counts actual unlink calls, and checks database authority and
saved receipt timings. Short-budget tests also cover batch execution, lock
contention, dry-run budget preservation, and the enable race.

### Reconciliation-only observation recovery (plans 1390/1391)

Journal `observe` checkpoints use only
`POST /api/v1/account/connections/cleanup/{itemId}/reconcile` in both protocols.
The client owns the exact checkpoint through compare-and-swap; it must durably
replace `observe` with `intent` before any unlink. The server takes the same
plan fence as issuance, then item/plan row fences, so any original request must
commit or roll back before reconciliation. This endpoint performs no policy/R2
work and never returns deletion authority or a permit expiry.

If no permit was ever committed and the exact attempt is still leased/pending,
it releases the lease to `pending` and resets the lease-expiry baseline without
pausing the plan. Approved dry-run proof/recovery stay intact; a fresh lease
increments attempt and must pass all ordinary execution gates. Delayed original
observations and stale journal replays cannot affect that new attempt. An
immutable `nas_cleanup_reconciliations` row records the checkpoint and decision.

Already-issued authority remains protected: replay gets `reconcile`, never a
permit, then submits the existing explicit no-unlink refusal receipt. Interrupted
`intent` and durable `result` checkpoints only report outcomes. Unknowns remain
reserved. A server lacking the endpoint leaves the journal unresolved; the
client never falls back to a permit-capable endpoint. The compatible API and
client must both be present before a future separately approved execution.

### Permanent incident protection and server serialization

All 28 unverified incident items remain permanently reserved: 12 in plan 1390
and 16 in plan 1391. A restored stale journal can report `unlink_attempted:false`
after a real deletion, so neither that flag nor its diagnostic proves no unlink.
There is no command or endpoint to release ever-issued reservations. Closing a
plan preserves every issued reservation and all recovery holds.

Lease and observation requests must advertise `reconciliation_protocol:1` to
receive execution work or permits. Older clients receive dry-run work only,
including lease retries; replaying their ordinary observation cannot mint a first
permit. This capability means observation journal replay exclusively uses
`/reconcile`, never `/observe` or `/observe-batch`.

The server serializes lease selection and allows at most one outstanding
`leased` or `delete_issued` item per enabled plan, across all clients and request
sizes. A permit must receive its terminal receipt before the next execution
lease. Existing parallel leases are refused execution until reconciled. The
client execution lock and all existing gates, including the 15 s server
execution budget, remain in place.

Regression coverage includes 16 contending workers against PostgreSQL handlers
and the real client, with a 1.1 s policy query inside the plan lock per item.
Separate tests cover legacy capability refusal, concurrent leases, first-permit
replay refusal, 503-before-issuance, lost committed permit responses and replay
identity binding. No production action was performed.
