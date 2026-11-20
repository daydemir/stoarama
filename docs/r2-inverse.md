# Full-bucket R2 partition

`stoaramactl r2-inverse partition --picks /path/to/r2-picks.json --out /new/report/dir`

This command reads the entire configured bucket without a prefix filter. It
loads the catalog and protections in a PostgreSQL read-only repeatable-read
transaction, then writes `partition.jsonl` (every listed key, class and reason)
and `summary.json` (per-class object counts and bytes, exactly reconciled to the
listing total). It never calls DELETE or changes database rows, and requires no
writer fence or drain evidence. Only a complete successful report is usable;
partial JSONL without summary is an interrupted report. Output directories must
be new, so earlier evidence is never overwritten.

Protected classes: Keep Everything by stream or a known stream-folder path;
keep28 by pinned stream window (including every collated source); permanent
incident roster and dependent outputs; cleanup reservations and unretired or
ever-issued source holds; retirement reservations/receipts; pending restores;
joined source snapshots, snapshotting scopes and dry-run scopes; A1 `blocked` items by exact bucket/key regardless of
inventory; delivery keys and source/output dependencies; stitch tools under
`experimental/v85-retired-WORK-custody/`; unknown or ambiguous identities.
Delivery remains protected pending Deniz's answer. No hold is inferred released
from an exact NAS inventory record or a changed cleanup receipt.

Permanent incidents have one immutable `nas_cleanup_incident_roster` descriptor
view (53 direct identities, 25 exact R2 keys, 292 raw identities including the
1414 collated-source dependencies). The compatibility views
`nas_cleanup_incident_files` and
`nas_cleanup_incident_sources` preserve their OIDs and derive from it, independently
of mutable plan/receipt state. Kind namespaces and literal controller-supplied
R2 keys are preserved; no R2 sidecar identity is inferred. Legacy release/owner
checks and SQL reservation/output guards consume the same canonical roster.
Missing required tables/views or unreadable holds fail the command. All unknown
keys appear individually in JSONL and are ineligible for deletion. `deletable`
is a planning classification only; this report grants no wire authority.

The sole controller runs this read-only on Jelly after merge. Compare total
bucket bytes with the approximately 53 TB bill, inspect unknown counts, and
retain the report as durable evidence. No production partition was run by the
implementation agent. Collated outputs are held in this release, including outside-window outputs;
raw keys with multiple catalog identities remain unknown. The executor is a separate change and has additional
live fence, drain, intent, budget and provider checks.

## Raw-only executor (separate release)

The sole controller can run a bounded batch with this interface. D-min remains
a metadata feeder plus safety/space decision; it has no R2 apply hook. A later
reviewed integration can call this interface after the drain/canary gate:

```
stoaramactl r2-inverse apply \
  --manifest REPORT/partition.jsonl --expected-sha256 PARTITION_SHA \
  --summary REPORT/summary.json --expected-summary-sha256 SUMMARY_SHA \
  --picks PICKS --ledger PRIVATE_LEDGER \
  --drain-evidence DRAIN_JSON --expected-drain-evidence-sha256 DRAIN_SHA \
  --workers 16 --max-items 100 --max-bytes CANARY_BYTES \
  --budget-24h-bytes 0
```

The rolling budget defaults to unlimited (`--budget-24h-bytes 0`) for both inverse
and NAS-backed keep28 apply. A positive value sets an optional limit; negative
values are rejected. Byte accounting, including unresolved intents, remains
conservatively charged by minute over the preceding 24h in either mode. The
default 100-object limit is the canary size; subsequent tick calls may increase
`--max-items` and the required positive per-run byte bound.
Only raw keys with one certain catalog identity and no protection are eligible.
Collated, KE, keep28, tools, delivery, held and unknown keys are never deleted.
The partition and picks must match the pinned policy and be no older than 24h.
The tick can generate a new read-only partition when its current one expires.

Each new key commits an ordinary source reservation and a permanent key intent
before wire, charges its byte budget, and fsyncs a private append-only JSONL
ledger. The network phase's lock order, which an operator should read as the
runbook, is: the KE epoch row `FOR SHARE` first as the barrier, then the global
protection advisory locks in **shared** mode (137/1 and the two protection keys),
then the **exclusive** per-key COPY advisory lock for this object, then the clip
row `FOR UPDATE` (that shared clip row is the recording-clip lock a concurrent
COPY or hold admission also takes, so neither can interleave with this key).
Every lock is acquired, and the target data and keep-28 custody are read, under
the database step budget first; only then does the 30-second network deadline
start, and it covers the identity HEAD, the multipart inventory and the final
guard. The wire transaction commits, and rolls back, separately from the
reservation that came before it and from publication that follows. Keys hold only
shared global locks, so independent keys still run concurrently; only the per-key
COPY advisory and that key's clip row are exclusive. The startup multipart
inventory and the restart reconciliation HEAD are the two reads that run outside
the protection transaction: startup under the apply session's advisory lock before
any key reserves, reconciliation while held intents are read back and before any
new work. A common live delivery predicate includes content/path aliases
and current collated-output dependencies without relying on a refreshed source
cache; admission of those protections and output membership changes is fenced. It exercises the installed purge guards before DELETE, rolls
that probe back, HEAD-checks listing/catalog ETag, size and the fixed pre-fence LastModified cutoff, and
rechecks live protections, controller drain evidence and migration 0198 at the
wire. The private concrete executor verifier issues authority only after these checks
and independently commits a permanent one-issuance row. No exported approval
callback or generic R2 delete API is added. Repeated verification cannot remint
authority. Each key's network calls and database steps have separate bounds, so
waiting on a sibling's protection locks never spends the network budget. The
network phase (identity HEAD, multipart inspection, the DELETE and the confirming
HEAD) has a 30-second deadline from when the wire transaction holds its locks.
Each database step (reservation, wire connection, wire preparation, the wire commit, each
publication attempt) gets its own 2 minutes. A timeout retains the intent for
reconciliation. There is one DELETE attempt, no SDK replay or redirects. A DELETE
whose request was fully written but which got no answer by its deadline is a lost
response: only its key is held. Success requires
a confirming 404 for the exact key. Every read the job makes is safe to repeat,
so one bounded policy covers them all: a LIST page, a multipart inventory page,
the pre-DELETE identity HEAD and the confirming HEAD each retry in the executor's
own loop rather than the SDK's (which is disabled on these paths, so the caps are
real). Each gets at most 4 tries with jittered backoff of at most 2 seconds
between them, and each honors the provider's own floor: `Retry-After` as delta
seconds or an HTTP date, and R2's millisecond `X-Amz-Retry-After`. A floor past
what is left of that read's budget stops that read cleanly — the retry is never
sent early — while still publishing the provider's real floor on the shared gate.
A floor past the 30-second retry cap does not degrade to 30 seconds: it latches
the job's reads shut, because releasing a worker that is deep in a LIST or a long
read at the cap would walk it back into the same throttle. A header value is
clamped to 24 hours, so a hostile duration cannot overflow. The confirming HEAD
keeps its reviewed 22-second deadline inside the key's network deadline (and, for
keep-28, custody end); the pre-DELETE HEAD keeps the existing 30-second network
bound it already had; a LIST or multipart page attempt is finite on its own
(60 seconds) even when the command has no deadline — that bounds one page, never
the whole bucket listing — and a retried page keeps its
`ContinuationToken`/markers, so it never relists from the first page. The LIST
loop and the multipart eligibility and continuation validation are the existing
`r2.Client` code, reached through a required page function. Every read attempt
carries an explicit deadline and runs on its own fresh HTTP/1.1 connection with
keep-alives off and HTTP/2 refused, and no read follows a redirect, so an attempt
never costs an extra hop after a published cooldown and the attempt count is an
upper bound on physical requests — a callback can fail before its request reaches
the wire. Only timeouts, connection errors, 429, every 5xx
(including a proxy's 520-599) and throttling retry; a bare 503 with no
`Retry-After` retries under the same bounded jitter and publishes the same shared
minimum cooldown a bare 429 does, on an early or a terminal try alike. When both
`Retry-After` and `X-Amz-Retry-After` are present the longest valid floor wins, so
a short standard hint never undercuts R2's own ask, and an overflowing positive
decimal saturates at the clamp instead of reading as absent. The confirming HEAD follows
no redirect: a 404 from anywhere but the key cannot confirm it. A 200 means the
object remains: it fails at once. A pre-DELETE read failure issues no DELETE
attempt at all. A final confirming failure keeps a key whose DELETE was already
issued unconfirmed and held — never absent, and never resent; the job stops
through its existing clean abort path.

The standalone `r2-inverse partition` command reads through this same bounded
store, built from the identical archived bucket, endpoint and environment config,
so a throttled page retries in place during a partition exactly as it does inside
a job.

One rate limiter belongs to the job's single store and is shared by every worker
and every read type. A 429 — or any transient status carrying `Retry-After` — sets
a job-wide cooldown, so every reader waits for the same instant and the job does
not re-create the herd that caused the throttle. A reader that waited re-reads the
gate on every wake, so an extension published while it slept extends its wait;
readers the cooldown releases leave staggered inside one jitter window. A throttle
on the final permitted try still sets the cooldown before that reader stops, so the
job's subsequent new reads stay fenced. The cooldown ends early when the job's
context is cancelled.

Both the pre-DELETE identity HEAD and the multipart inventory run inside the
guarded wire transaction, so the cooldown wait on that path happens while the
key's shared protection locks are held. That is deliberate: the HEAD is the exact
identity and `LastModified` authority for the DELETE, and the inventory is the
provider-side veto that must be observed under the same guarded snapshot. It adds
no new bound — both live inside the wire transaction's existing network budget and
hold only shared locks — so the tradeoff is bounded sharing, not extended custody.

A publication step whose parent context ended first — the job cancelled or its
deadline passed before the step's own 2-minute bound — stops the job and holds
nothing; it is not an unpublished key. Only the step's own bound expiring, or
lock contention inside it, holds the key as `unpublished`.

Receipt and raw purged_at publication happens separately with exclusive admission
locks, without upgrading a network transaction's locks. It is bookkeeping for a
key a 404 already confirmed gone, so it retries up to 3 times when a step ran out
of its own 2-minute bound (waiting for a pool connection, a lock or a statement)
or lost to lock contention (55P03, 40P01, 40001). It disables the role's
`statement_timeout`, so a 57014 there can only be an explicit cancel, which stops
the job, as does any refusal or inconsistency. A failed publication COMMIT has an
unknown outcome, so it stops the job instead of being retried or held. If every
retry fails, the key is held exactly like a lost response: issued, unresolved,
reported in `unpublished`, never resent, and the batch's readback must name it
among the unresolved keys. Every failing key logs its step (`reserve`, `wire`,
`head`, `commit` or `publish`) and key.

A read error's `after N attempts` count is the number of SDK callback
invocations, not a wire count. A failure that never reaches the SDK — a gate that
is already latched, or a command that ended first — reports the operations it
actually invoked, so it says `after 0 attempts` and is not a physical request.
A callback that does run can still fail before its request reaches the wire (a
refused dial, a failed TLS handshake), so the logged count is an upper bound on
physical requests, never a lower one.

A batch whose only failures held keys continues the job even if it published
nothing, as when a lock convoy catches every in-flight key at once. Three such
batches in a row (`inverseJobStalledBatches`) stop the job, so a persistent outage
cannot spin.

An existing intent is never retransmitted, including a crash before wire. On
restart, an issued key whose object now answers 404 is reported as
`held_absent` and stays unresolved: the executor never reconciles it into a
receipt or a ledger absence, because an attempt row cannot prove which actor
removed the object. A present object stays held as `ambiguous_held` while
unrelated fresh keys can proceed. Preserve the ledger and
DB records. There is no automated intent expiry, removal, or ambiguous retry.
A damaged/partial local ledger fails closed; it is never truncated or repaired.

Drain evidence is a controller-reviewed, SHA-pinned JSON artifact, bound to the
exact bucket, endpoint and durable fence activation/deadline. It must include
`bucket`, `endpoint_sha256`, `fence_activated_at`, `drain_not_before`,
`observed_at`, `deployed_writer_coverage_evidence`, and
`proof`, and `accepted_put_completion_evidence`. Use
`proof: accepted_put_completion_v1` for substantiated terminal evidence. The observation is strictly after the real deadline. Completion evidence remains bound
to that immutable fence; it does not need a daily reapproval. References must substantiate
all deployed writer coverage and completion of externally accepted PUTs.
A string claiming completion or capability expiry does not establish that fact.
The implementation agent creates none of this production evidence.

Both executors accept the explicit `owner_fence_policy_20261118_v1` route in
[the owner fence policy contract](r2-owner-fence-policy.md). It replaces the old
B1-only key-policy route, pins the committed owner decision and this partition
and summary, and explicitly accepts the final HEAD/unconditional DELETE race
without claiming external PUT completion. Exact live fence binding, six live
wire/capability vetoes, source identity, holds and B2 NAS custody remain mandatory.

Controller canary gate after migration 0210 and fresh bound owner evidence:

1. Record baseline provider bucket bytes and object count using the controller's
   read-only metrics. Pin exactly 100 eligible raw keys and their total bytes.
2. Run one 100-object batch with an explicit per-run byte cap and `--budget-24h-bytes 0`. On any error,
   preserve evidence; reconcile existing intents by HEAD before continuing.
3. Require 100 distinct absent ledger keys = 100 DB absent events = 100 provider
   removals, and HEAD 404 for all 100. Provider bucket bytes must fall; account for
   metric publication delay explicitly. No double-counting reconciliations.
4. Require sustained >=8 objects/s measured by elapsed batch wall time. The CLI
   prints `deleted`, `reconciled`, `ambiguous_held`, `bytes`, `elapsed_seconds`,
   and `objects_per_second`. Synthetic-provider throughput is not this gate.
5. Only after every gate passes, configure larger controller-owned batches.
   Tick integration is a later change.
   Keep the same durable ledger across runs. Preserve reports outside temp.

Full-cardinality startup cost and memory remain unmeasured: each invocation reads
the manifest, private ledger and historical intent keys. The offline throughput
fixture includes 10,010 collated outputs but does not establish 4.17M-key scale.

Protection metadata changes take the epoch-row barrier with NOWAIT and can return
SQLSTATE 55P03 during an active batch; retry metadata admission in a new
transaction. They require READ COMMITTED so an earlier snapshot cannot miss a
committed retirement. Cleanup receipts/control do not take this barrier or a new
global lock. New cleanup recovery admission instead locks raw clip rows in order
and rejects a committed reservation; retired items cannot reactivate a hold.
Output IDs and source-hold identities cannot be rebound.

COPY reimport keeps its existing key-before-row order. Already-protected COPY
progress and unchanged verified rows take no new key lock. A clean verified row
gaining protection requires READ COMMITTED and tries the same key lock without
waiting; a busy key returns 55P03, and a committed reservation vetoes adoption.

The CLI pool has 20 connections. Workers default to 16 and may use at most the
pool capacity minus two (18 here), retaining the session and independent issuance
connections. Invalid capacity fails before any reservation or budget charge.

## Keep28 RAW with fresh NAS custody (B2)

`r2-inverse apply-keep28` takes the same bounded, SHA-pinned partition, picks,
ledger, drain and budget arguments as `apply`. Use a **separate B2 ledger**;
every B2 record contains `lane: keep28_raw_v1`. Both commands reject the other
lane's ledger before any append. B1 still refuses keep28. B2 admits only a unique
managed RAW identity overlapping pinned keep28, in account 47 / NAS connection
13, with a known non-KE owner choice. All existing KE stream/path, delivery,
incident, live hold, exact A1 blocked-key, purge, unknown, tool-prefix and
collated exclusions remain. A broad partition can label collated/ambiguous
keep28 keys; that classification cannot authorize them here.

Selection intersects the report with fresh exact inventory in `verified_at`,
source-ID order. Reservation and final authorization both use the existing
`r2RetireLockNASIdentity` lock: exact connection/source/path/size/SHA, present,
both verified and server-received timestamps within 24 hours, exact sidecar
path/size/SHA. The complete proof is persisted with the permanent B2 intent and
compared again under lock, including timestamps and sidecar. Old October 30
inventory does not qualify. The final inventory row stays locked through DELETE
and confirming HEAD404. Each PostgreSQL clock sample fixes custody expiry as
one absolute local monotonic instant, 2 seconds early (`keep28CustodyMargin`).
The authority keeps the running minimum across every sample, and every later
handoff (first request byte, socket write deadline, confirming HEAD) ends at it
or the existing 30-second cap, so neither a pause after sampling nor local
wall-clock skew can extend authority. Selection takes only keys whose custody
outlasts one network timeout plus the margin, so a key custody would skip uses
no item or byte allowance. Reservation repeats that check under lock: a key
whose custody shortened or aged out after selection is counted as
`custody_short` and skipped before any reservation, intent, budget charge or
ledger record, and fresh custody retries it. Selection admits only exact,
complete inventory evidence, so a legacy row without sidecar evidence is
ineligible rather than a batch failure. Reservation requires the identity
selection observed (source ID, catalog path, size, media SHA, sidecar digest
and size) before it considers custody age; any difference, or a changed
state, stops the batch. A job is done only when a clean batch selected
nothing. Job totals sum per-batch counts; held keys and `last_modified_held`
are snapshots of the latest batch that read its partition; a batch that failed
before reading it keeps the last measurement.

Custody is a 24-hour freshness policy on an existing, verified NAS copy, not a
physical deadline. The requirement is that custody is valid at the last
PostgreSQL sample with at least the 2-second margin to spare. The accepted
residuals, which have no data-loss consequence and get no further machinery:

- the provider executing an already-accepted DELETE after expiry, bounded by
  the 30-second network timeout;
- a PostgreSQL clock step larger than the margin after the last sample;
- a host suspend during which the monotonic clock stops.

Migration 0204 creates independent `r2_keep28_intents`, `r2_keep28_attempts`,
`r2_keep28_events` and a pending-source identity guard. Ordinary reservations
and the conservative byte budget remain shared protections. B2 issues its
single-use authority from its own lane tables and adds the custody proof and
custody deadline to every check, but sends over the same one-attempt first-wire
transport as B1: a fresh HTTP/1.1 connection per DELETE, the batch send gate on
the first request byte, and the same lost-response classification. The first
request byte is refused at or after the DELETE's deadline, which for B2 is never
later than custody expiry, and the socket write deadline is set to that instant.
A response counts as received when its first byte is read from the connection;
a failed DELETE is classified only after its connection is closed and any read
under way has returned. A B2 key whose DELETE lost its response stays held and
is never retransmitted. An
existing B2 intent is never retransmitted, even after a stop before wire.
Restart treats held B2 intents exactly as B1's: an issued key whose object is
gone is reported `held_absent`, an unissued one stops the run, and a present
object must still match and remains `ambiguous_held`. `r2-inverse job-keep28`
runs B2 batches with the same per-batch owner evidence and exact readback, over
the B2 tables, as `r2-inverse job` does for B1. Its batches default to the
100-object canary size, and it refuses to start without explicit `--max-bytes`
and `--budget-24h-bytes` (0 = unlimited). Both jobs refuse more workers than the
database pool allows before writing any partition or evidence.

## Job restart

A job's `--work` directory is lost with its one-off Render container. So after
writing and sealing a fresh partition, `job` and `job-keep28` store it, gzipped,
as the bucket endpoint's one reusable partition in `r2_inverse_partitions`
(migration 0226), replacing the previous one, and print its `partition_sha256` in
the `partition` step. It lives in PostgreSQL, not R2: under the writer fence every
new R2 write is refused, and an ungated operator PUT would break the drain the
executor relies on.

To restart without relisting the bucket, pass that digest as
`--reuse-partition HEX`. The job then restores the stored partition into its new
`--work` directory and stops unless all of these hold:

- the restored `partition.jsonl` has exactly that digest;
- its summary names the same bucket, endpoint and Keep-28 policy, and picks
  equal to the independently approved `--expected-picks-sha256`;
- it is fresh by the same 24-hour rule every batch applies ("partition is stale
  or future-dated", by database time);
- its external classification inputs are unchanged. The summary's
  `protection_sha256` digests the KE epoch and the exact owner keep prefixes, KE
  folders, external hold set, delivery holds and joined releases, read with the
  partition's own SQL. The job reads them again and requires an equal digest.
  Retirement reservations and receipts, the job's own progress, are left out:
  they would otherwise invalidate the partition after its first key, and the
  live gate refuses every reserved or retired key anyway.

The digest cannot prove the whole partition still classifies the same way. Catalog
fields the KE epoch does not version, such as clip spans and timezones, can change
without it. So the per-key live reclassification is load-bearing for safety:
each candidate's identity is reloaded and reclassified under lock at reserve and
again at the wire, and a key that is no longer eligible is refused. The reverse is
a completeness gap, not a safety one: a reused listing can omit a key that became
eligible after it, and the next fresh partition picks that key up. The reused
partition only proposes candidates: reserve's database checks, live holds,
custody and the wire HEAD identity check still run for every key, so a key held
after the reuse check is still refused.
On any refusal, run the job without `--reuse-partition` to take a fresh partition.

Deniz's November 17 order to delete group 3 that is on the NAS supersedes the
stitch-proof prerequisite **only for this keep28 RAW lane**. This new command
accepts no stitch-proof and never calls the legacy stitch-gated retirement
entry point. The old `r2-retire` command and its stitch-proof gate are unchanged.
Final HEAD identity, the strict pre-fence cutoff, the immutable 0198 fence with
its owner-adjusted 0210 deadline, and pinned owner evidence still apply.

Only the sole production controller may deploy and run this command. Use the
same 100-object canary gate, but reconcile **B2** DB events, B2 ledger and exact
provider removals separately from B1. Require HEAD404, bucket-byte reduction and
at least 8 objects/second by full batch wall time before larger batches.
Production readiness and throughput are not established by offline tests.

COPY ordinary import still does not re-lease verified rows. The separately
reviewed PR2 adds optional bounded verify-only refresh through D; see
[finish tick](../ops/jelly/finish/README.md#bounded-keep28-custody-refresh-pr2).
Its controller flag and queue status grant no deletion authority. No timestamp
refresh without hashing, manual reset, NAS scanner or reduced resource guard
is supplied. The recorded 1,158,889 RAW / 16.44 TB requires about 5.37 days at 150
verifications/minute or 2.01 days at 400/minute (~95 MB/s of hashing). Memory and
load can hold the worker entirely. Selection and startup memory at full
cardinality remain unmeasured: B2 retains the report's keep28 entries in memory,
and both lanes read their historical intents and ledger. Use bounded controller
batches and measure startup/RSS before scaling.
